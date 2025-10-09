///
/// The parquet-concat function receives S3 Event Notifications and concatenates parquet files
///
use aws_lambda_events::event::sqs::SqsEvent;
use aws_lambda_events::s3::S3EventRecord;
use lambda_runtime::tracing::{debug, error, info, trace};
use lambda_runtime::{Error, LambdaEvent, run, service_fn, tracing};
use parquet::arrow::async_reader::{
    ParquetObjectReader, ParquetRecordBatchStream, ParquetRecordBatchStreamBuilder,
};
use parquet::arrow::async_writer::{AsyncArrowWriter, ParquetObjectWriter};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

use oxbow_lambda_shared::*;
use oxbow_sqs::{ConsumerConfig, TimedConsumer};
use tokio_stream::StreamExt;

use deltalake::{ObjectStore, Path};
use std::alloc::System;
use std::env;
use std::iter::Iterator;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// The setting of the global allocator here with stats alloc is intended to allow the
/// parquet-concat
/// lambda to load as many files from S3 as it can tolerate given the memory available on the
/// system.
#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

/// The type of data file which parquet-concat can load
#[derive(Clone, Debug, PartialEq)]
enum RecordType {
    /// Parquet file
    Parquet,
    /// The file format is unknown and parquet-concat cannot handle it
    Unknown,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    deltalake::aws::register_handlers(None);
    tracing::init_default_subscriber();
    let _ = env::var("OUTPUT_PREFIX").expect("The `OUTPUT_PREFIX` must be set in the environment");
    let _ = env::var("INPUT_BUCKET").expect("The `INPUT_BUCKET` must be set in the environment");
    let _ = env::var("OUTPUT_BUCKET").expect("The `OUTPUT_BUCKET` must be set in the environment");
    info!("Starting parquet-concat");

    run(service_fn(function_handler)).await
}

/// This is the primary invocation point for the lambda and should do the heavy lifting
async fn function_handler(event: LambdaEvent<SqsEvent>) -> Result<(), Error> {
    let output_prefix = std::env::var("OUTPUT_PREFIX").expect("Failed to get `OUTPUT_PREFIX`");
    let input_bucket = std::env::var("INPUT_BUCKET").expect("Failed to get `INPUT_BUCKET`");
    let output_bucket = std::env::var("OUTPUT_BUCKET").expect("Failed to get `OUTPUT_BUCKET`");
    let input_prefix = std::env::var("INPUT_PREFIX").unwrap_or_default();
    debug!("Receiving event: {event:?}");
    let region = Region::new(GLOBAL);
    let mut records = extract_records_from(vec![event.payload])?;

    info!("Processing {} bucket notifications", records.len());
    let fn_start_since_epoch_ms: u128 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Failed to get a system time after unix epoch")
        .as_millis();
    // Millis to allow for consuming more messages
    let more_deadline_ms = ((event.context.deadline as u128) - fn_start_since_epoch_ms) / 2;

    let mut last_writer: Option<AsyncArrowWriter<ParquetObjectWriter>> = None;
    let mut last_dir: Option<String> = None;

    let config = aws_config::load_from_env().await;

    // Input store to fetch parquet files from S3
    let input_store: Arc<dyn ObjectStore> = Arc::new(
        object_store::aws::AmazonS3Builder::from_env()
            .with_bucket_name(&input_bucket)
            .build()
            .expect("Input object store failed to build"),
    );

    // Output store to write parquet files to S3
    let output_store: Arc<dyn ObjectStore> = Arc::new(
        object_store::aws::AmazonS3Builder::from_env()
            .with_bucket_name(&output_bucket)
            .build()
            .expect("Output object store failed to build"),
    );
    let consumer_config = ConsumerConfig {
        retrieval_max: 1,
        ..Default::default()
    };
    info!("TimedConsumer configuration being used: {consumer_config:?}");

    let mut consumer = TimedConsumer::new(
        consumer_config,
        &config,
        Duration::from_millis(
            more_deadline_ms
                .try_into()
                .expect("Failed to compute a 64-bit deadline"),
        ),
    );

    if let Ok(bytes_to_consume) = std::env::var("BUFFER_MORE_MBYTES_ALLOWED") {
        consumer.set_memory_limit(
            region,
            bytes_to_consume
                .parse::<usize>()
                .expect("BUFFER_MORE_MBYTES_ALLOWED must be parseable as a uint64")
                * 1024
                * 1024,
        );
    }

    loop {
        let next_up = consumer.next().await?;

        if let Some(batch) = next_up {
            info!(
                "Buffered an additional {} more messages from SQS",
                batch.len()
            );
            // When pulling messages separately, convert them to a Lambda SqsEvent like structure for
            // easier reuse of deserialization codec
            let event = SqsEvent {
                records: batch.into_iter().map(convert_from_sqs).collect(),
            };
            records.extend(extract_records_from(vec![event])?);
        }

        if records.is_empty() {
            trace!("No more records to process at the moment, see ya later!");
            break;
        }

        // Each iteration we'll drain the records to make sure everything is _gotten_ before
        // continuing
        process_records(
            records.drain(..),
            &input_store,
            &output_store,
            &output_prefix,
            &input_prefix,
            &mut last_writer,
            &mut last_dir,
        )
        .await?;
    }

    if let Some(writer) = last_writer {
        info!("Finalizing last writer");
        writer.close().await?;
    }
    debug!("Flushing consumer to wrap up the execution");
    consumer.flush().await?;

    Ok(())
}

/// Process a batch of S3 event records, concatenating parquet files
async fn process_records(
    records: impl Iterator<Item = S3EventRecord>,
    input_store: &Arc<dyn ObjectStore>,
    output_store: &Arc<dyn ObjectStore>,
    output_prefix: &str,
    input_prefix: &str,
    last_writer: &mut Option<AsyncArrowWriter<ParquetObjectWriter>>,
    last_dir: &mut Option<String>,
) -> Result<(), Error> {
    for file_record in records {
        match suffix_from_record(&file_record) {
            RecordType::Parquet => {
                debug!("Preparing to load file {file_record:?}");
                let location: Path = file_record.s3.object.url_decoded_key.unwrap().into();
                let object_reader = ParquetObjectReader::new(input_store.clone(), location.clone());
                let mut reader = ParquetRecordBatchStreamBuilder::new(object_reader)
                    .await?
                    .with_batch_size(10_000)
                    .build()?;

                let dir = directory_from_location(&location, input_prefix)?;

                if Some(dir.clone()) != *last_dir {
                    info!("Opening new writer for prefix {dir}");

                    let uuid = Uuid::new_v4();
                    let path = format!("{output_prefix}/{dir}/{uuid}.parquet",);
                    let sink = ParquetObjectWriter::new(output_store.clone(), path.into());

                    let props = WriterProperties::builder()
                        .set_compression(Compression::SNAPPY)
                        .build();

                    let mut new_writer = Some(AsyncArrowWriter::try_new(
                        sink,
                        reader.schema().clone(),
                        Some(props),
                    )?);
                    std::mem::swap(last_writer, &mut new_writer);
                    *last_dir = Some(dir);

                    if let Some(old_writer) = new_writer {
                        old_writer.close().await?;
                        info!("Flushed writer");
                    }
                }

                if let Some(writer) = last_writer {
                    let batches = copy_parquet(&mut reader, writer).await?;
                    info!("Deserialized {batches} batches");
                }
            }
            RecordType::Unknown => {
                error!(
                    "parquet-concat was invoked for a file with an unknown suffix! Ignoring: {file_record:?}"
                );
            }
        }
    }
    Ok(())
}

/// Extract the suffix from the given [S3EventRecord] for matching and data loading
fn suffix_from_record(record: &S3EventRecord) -> RecordType {
    if let Some(key) = record.s3.object.url_decoded_key.as_ref()
        && key.ends_with(".parquet")
    {
        return RecordType::Parquet;
    }
    RecordType::Unknown
}

// Drop prefix and filename, keep the middle.
// For example "exports/table1/ds=2025-10-09/file.parquet" with prefix "exports"
// returns "table1/ds=2025-10-09"
fn directory_from_location(location: &Path, input_prefix: &str) -> Result<String, Error> {
    let path_str = location.to_string();

    if !path_str.starts_with(input_prefix) {
        return Err(format!("{path_str} doesn't start with {input_prefix}").into());
    }

    let path_str = path_str[input_prefix.len()..].trim_start_matches('/');

    // Get the parent directory path
    let std_path = std::path::Path::new(&path_str);
    let parent = std_path
        .parent()
        .ok_or("Failed to get parent directory from location")?;

    let parent = parent.to_str().unwrap_or("");
    if parent.is_empty() {
        return Err("No parent directory found - path appears to be just a filename".into());
    }

    Ok(parent.to_string())
}

async fn copy_parquet(
    reader: &mut ParquetRecordBatchStream<ParquetObjectReader>,
    writer: &mut AsyncArrowWriter<ParquetObjectWriter>,
) -> Result<u64, Error> {
    let mut batches = 0;

    while let Some(batch) = reader.next().await {
        writer.write(&batch?).await?;
        batches += 1;
    }

    Ok(batches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_lambda_events::s3::{S3Entity, S3EventRecord, S3Object};
    use aws_lambda_events::sqs::SqsMessage;

    #[test]
    fn test_suffix_from_record() {
        let d = S3EventRecord::default();
        assert_eq!(suffix_from_record(&d), RecordType::Unknown);

        let parquet = S3EventRecord {
            s3: S3Entity {
                object: S3Object {
                    key: Some("some/prefix/fileA.parquet".into()),
                    url_decoded_key: Some("some/prefix/fileA.parquet".into()),
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Default::default()
        };
        assert_eq!(suffix_from_record(&parquet), RecordType::Parquet);
    }

    /// The [SqsMessage] and [oxbow_sqs::Message] structs are mostly identical except one comes
    /// from Lambda triggers and the other directly from SQS.
    ///
    /// This function makes sure that they can be interchanged
    #[test]
    fn test_sqs_event_compat() {
        let buf = std::fs::read_to_string("../../tests/data/s3-event-tables.json")
            .expect("Failed to read file");
        let message = oxbow_sqs::Message::builder().body(buf.clone()).build();

        let converted: SqsMessage = convert_from_sqs(message);
        assert_eq!(Some(buf), converted.body);
    }

    #[test]
    fn test_directory_from_location_with_prefix() {
        // Test case: "exports/table1/ds=2025-10-09/file", prefix="exports", dir="table1/ds=2025-10-09"
        let path = Path::from("exports/table1/ds=2025-10-09/file.parquet");
        assert_eq!(
            directory_from_location(&path, "exports").unwrap(),
            "table1/ds=2025-10-09"
        );

        // Test case: prefix="exports/", dir="table1/ds=2025-10-09"
        let path = Path::from("exports/table1/ds=2025-10-09/file.parquet");
        assert_eq!(
            directory_from_location(&path, "exports/").unwrap(),
            "table1/ds=2025-10-09"
        );

        // Test case: prefix="", dir="exports/table1/ds=2025-10-09"
        let path = Path::from("exports/table1/ds=2025-10-09/file.parquet");
        assert_eq!(
            directory_from_location(&path, "").unwrap(),
            "exports/table1/ds=2025-10-09"
        );

        // Test case: no prefix match
        let path = Path::from("other/table1/ds=2025-10-09/file.parquet");
        assert!(directory_from_location(&path, "exports").is_err(),);

        // Test case: empty path
        let path = Path::from("file.parquet");
        let result = directory_from_location(&path, "");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_concat_parquet_files() {
        use object_store::local::LocalFileSystem;
        use tempfile::TempDir;

        // Create temporary directory for output only
        let output_dir = TempDir::new().expect("Failed to create temp output dir");

        let input_store: Arc<dyn object_store::ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix("../../tests/data/hive").unwrap());
        let output_store: Arc<dyn object_store::ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(output_dir.path()).unwrap());

        let files = vec![
            "faker_products/1701800282197_0.parquet",
            "faker_products/1701800282197_0.parquet",
        ];
        let records = files.iter().map(|file| S3EventRecord {
            s3: S3Entity {
                object: S3Object {
                    key: Some(file.to_string()),
                    url_decoded_key: Some(file.to_string()),
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Default::default()
        });

        let mut last_writer: Option<AsyncArrowWriter<ParquetObjectWriter>> = None;
        let mut last_dir: Option<String> = None;
        let output_prefix = "output";

        process_records(
            records,
            &input_store,
            &output_store,
            output_prefix,
            "",
            &mut last_writer,
            &mut last_dir,
        )
        .await
        .expect("Failed to process records");

        // Close the writer
        if let Some(writer) = last_writer {
            writer.close().await.expect("Failed to close writer");
        }

        // Verify that an output file was created in the faker_products directory
        let output_files: Vec<_> = get_parquet_files_in_directory(
            &output_dir.path().join("output").join("faker_products"),
        );

        assert_eq!(
            output_files.len(),
            1,
            "Expected exactly one output parquet file"
        );

        let output_file = &output_files[0];
        let original_file_path =
            std::path::Path::new("../../tests/data/hive/faker_products/1701800282197_0.parquet");
        let original_rows = count_parquet_rows(original_file_path)
            .await
            .expect("Failed to count original rows");
        let output_rows = count_parquet_rows(output_file)
            .await
            .expect("Failed to count output rows");

        // The output should have twice as many rows as the original (since we processed it twice)
        assert_eq!(
            output_rows,
            original_rows * 2,
            "Expected output to have {} rows (2x original), but got {}",
            original_rows * 2,
            output_rows
        );
    }

    #[tokio::test]
    async fn test_concat_partitioned_parquet_files() {
        use object_store::local::LocalFileSystem;
        use tempfile::TempDir;

        let output_dir = TempDir::new().expect("Failed to create temp output dir");

        let input_store: Arc<dyn object_store::ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix("../../tests/data/hive").unwrap());
        let output_store: Arc<dyn object_store::ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(output_dir.path()).unwrap());

        let files = vec![
            "deltatbl-partitioned/c2=foo0/part-00000-2bcc9ff6-0551-4401-bd22-d361a60627e3.c000.snappy.parquet",
            "deltatbl-partitioned/c2=foo0/part-00001-ca647ee7-f1ad-4d70-bf02-5d1872324d6f.c000.snappy.parquet",
            "deltatbl-partitioned/c2=foo1/part-00001-1c702e73-89b5-465a-9c6a-25f7559cd150.c000.snappy.parquet",
            "deltatbl-partitioned/c2=foo1/part-00000-786c7455-9587-454f-9a4c-de0b22b62bbd.c000.snappy.parquet",
        ];

        let records = files.iter().map(|file| S3EventRecord {
            s3: S3Entity {
                object: S3Object {
                    key: Some(file.to_string()),
                    url_decoded_key: Some(file.to_string()),
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Default::default()
        });

        let mut last_writer: Option<AsyncArrowWriter<ParquetObjectWriter>> = None;
        let mut last_dir: Option<String> = None;
        let output_prefix = "output";

        process_records(
            records,
            &input_store,
            &output_store,
            output_prefix,
            "",
            &mut last_writer,
            &mut last_dir,
        )
        .await
        .expect("Failed to process records");

        if let Some(writer) = last_writer {
            writer.close().await.expect("Failed to close writer");
        }

        // Verify that output files were created in the correct partition directories
        let foo0_output_files: Vec<_> = get_parquet_files_in_directory(
            &output_dir
                .path()
                .join("output")
                .join("deltatbl-partitioned")
                .join("c2=foo0"),
        );

        let foo1_output_files = get_parquet_files_in_directory(
            &output_dir
                .path()
                .join("output")
                .join("deltatbl-partitioned")
                .join("c2=foo1"),
        );
        // Verify we have exactly one output file in each partition
        assert_eq!(
            foo0_output_files.len(),
            1,
            "Expected exactly one output parquet file in c2=foo0"
        );
        assert_eq!(
            foo1_output_files.len(),
            1,
            "Expected exactly one output parquet file in c2=foo1"
        );

        // Count rows in the original files and the concatenated outputs
        let foo0_original_files = [
            "../../tests/data/hive/deltatbl-partitioned/c2=foo0/part-00000-2bcc9ff6-0551-4401-bd22-d361a60627e3.c000.snappy.parquet",
            "../../tests/data/hive/deltatbl-partitioned/c2=foo0/part-00001-ca647ee7-f1ad-4d70-bf02-5d1872324d6f.c000.snappy.parquet",
        ];
        let foo1_original_files = [
            "../../tests/data/hive/deltatbl-partitioned/c2=foo1/part-00001-1c702e73-89b5-465a-9c6a-25f7559cd150.c000.snappy.parquet",
            "../../tests/data/hive/deltatbl-partitioned/c2=foo1/part-00000-786c7455-9587-454f-9a4c-de0b22b62bbd.c000.snappy.parquet",
        ];

        let mut foo0_original_rows = 0;
        for file_path in &foo0_original_files {
            let rows = count_parquet_rows(std::path::Path::new(file_path))
                .await
                .expect("Failed to count original rows");
            foo0_original_rows += rows;
        }

        let mut foo1_original_rows = 0;
        for file_path in &foo1_original_files {
            let rows = count_parquet_rows(std::path::Path::new(file_path))
                .await
                .expect("Failed to count original rows");
            foo1_original_rows += rows;
        }

        let foo0_output_rows = count_parquet_rows(&foo0_output_files[0])
            .await
            .expect("Failed to count foo0 output rows");
        let foo1_output_rows = count_parquet_rows(&foo1_output_files[0])
            .await
            .expect("Failed to count foo1 output rows");

        // Verify the output has the same number of rows as the sum of original files
        assert_eq!(
            foo0_output_rows, foo0_original_rows,
            "Expected c2=foo0 output to have {} rows, but got {}",
            foo0_original_rows, foo0_output_rows
        );
        assert_eq!(
            foo1_output_rows, foo1_original_rows,
            "Expected c2=foo1 output to have {} rows, but got {}",
            foo1_original_rows, foo1_output_rows
        );
    }

    async fn count_parquet_rows(file_path: &std::path::Path) -> Result<usize, Error> {
        use object_store::local::LocalFileSystem;
        use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;

        let store = LocalFileSystem::new_with_prefix(file_path.parent().unwrap())?;
        let file_name = file_path.file_name().unwrap().to_str().unwrap();
        let location: Path = file_name.into();
        let object_reader = ParquetObjectReader::new(Arc::new(store), location);
        let mut reader = ParquetRecordBatchStreamBuilder::new(object_reader)
            .await?
            .with_batch_size(10_000)
            .build()?;

        let mut total_rows = 0;
        while let Some(batch) = reader.next().await {
            total_rows += batch?.num_rows();
        }

        Ok(total_rows)
    }

    fn get_parquet_files_in_directory(directory_path: &std::path::Path) -> Vec<std::path::PathBuf> {
        std::fs::read_dir(directory_path)
            .expect(&format!(
                "Failed to read directory: {}",
                directory_path.display()
            ))
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                if path.extension().map_or(false, |ext| ext == "parquet") {
                    Some(path)
                } else {
                    None
                }
            })
            .collect()
    }
}
