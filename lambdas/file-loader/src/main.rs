///
/// The file-loader function receives S3 Event Notifications and reads a number of different file
/// formats for appending records to existing Delta Lake tables
///
use aws_lambda_events::event::sqs::SqsEvent;
use aws_lambda_events::s3::S3EventRecord;
use aws_lambda_events::sqs::SqsMessage;
use deltalake::DeltaResult;
use deltalake::arrow::datatypes::Schema as ArrowSchema;
use deltalake::arrow::json::reader::ReaderBuilder;
use deltalake::kernel::engine::arrow_conversion::TryIntoArrow;
use deltalake::writer::{DeltaWriter, record_batch::RecordBatchWriter};
use lambda_runtime::tracing::{debug, error, info, trace};
use lambda_runtime::{Error, LambdaEvent, run, service_fn, tracing};

use oxbow_lambda_shared::*;
use oxbow_sqs::{ConsumerConfig, TimedConsumer};

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use std::env;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// The type of data file which file-loader can load
#[derive(Clone, Debug, PartialEq)]
enum RecordType {
    /// Line-delimited JSON, e.g. .jsonl file
    Jsonl,
    /// The file format is unknown and file-loader cannot handle it
    Unknown,
}

/// Simple function for extracting the necessary [S3EventRecord] structs from a given [SqsEvent]
///
/// This utilizes the `UNWRAP_SNS_ENVELOPE` environment variable to handle SNS-encoded bucket
/// notifications
fn extract_records_from(
    events: impl IntoIterator<Item = SqsEvent>,
) -> DeltaResult<Vec<S3EventRecord>> {
    let mut records = vec![];
    for event in events {
        let pieces = match std::env::var("UNWRAP_SNS_ENVELOPE") {
            Ok(_) => s3_from_sns(event)?,
            Err(_) => s3_from_sqs(event)?,
        };
        records.extend(pieces);
    }
    Ok(records_with_url_decoded_keys(&records))
}

/// This is the primary invocation point for the lambda and should do the heavy lifting
async fn function_handler(event: LambdaEvent<SqsEvent>) -> Result<(), Error> {
    let table_uri = std::env::var("DELTA_TABLE_URI").expect("Failed to get `DELTA_TABLE_URI`");
    let fn_start = Instant::now();
    debug!("Receiving event: {event:?}");
    let mut records = extract_records_from(vec![event.payload])?;

    info!("Processing {} bucket notifications", records.len());
    let fn_start_since_epoch_ms: u128 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Failed to get a system time after unix epoch")
        .as_millis();
    // Millis to allow for consuming more messages
    let more_deadline_ms = ((event.context.deadline as u128) - fn_start_since_epoch_ms) / 2;

    let mut table = oxbow::lock::open_table(&table_uri)
        .await
        .expect("Failed to open the Delta table!");
    let mut writer = RecordBatchWriter::for_table(&table)?;

    let config = aws_config::load_from_env().await;
    let client = aws_sdk_s3::Client::new(&config);
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
    let mut bytes_consumed: usize = 0;

    loop {
        let next_up = consumer.next().await?;

        if let Some(batch) = next_up {
            debug!(
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
        for file_record in records.drain(..) {
            let key = file_record
                .s3
                .object
                .key
                .as_ref()
                .expect("Failed to extract key from record");
            match suffix_from_record(&file_record) {
                RecordType::Jsonl => {
                    debug!("Preparing to load filue {file_record:?}");
                    let mut response = client
                        .get_object()
                        .bucket(file_record.s3.bucket.name.clone().unwrap())
                        .key(file_record.s3.object.key.as_ref().unwrap())
                        .send()
                        .await?;
                    info!("Attempting to read bytes from {file_record:?}");
                    let schema: ArrowSchema = table.snapshot()?.schema().try_into_arrow()?;

                    let mut json = ReaderBuilder::new(schema.into()).build_decoder()?;
                    while let Some(bytes) = response.body.try_next().await? {
                        bytes_consumed += bytes.len();
                        json.decode(&bytes)?;
                    }
                    if let Some(batch) = json.flush()? {
                        debug!("Writing a batch with {} rows", batch.num_rows());
                        writer.write(oxbow::write::augment_with_ds(batch)?).await?;
                        debug!("Appended values from {key} to: {table:?}");
                    }
                }
                RecordType::Unknown => {
                    error!(
                        "file-loader was invoked for a file with an unknown suffix! Ignoring: {file_record:?}"
                    );
                }
            }
        }

        if let Ok(bytes_to_consume) = std::env::var("BUFFER_MORE_MBYTES_ALLOWED") {
            let mbytes_to_consume: usize = str::parse(&bytes_to_consume)
                .expect("BUFFER_MORE_BYTES_ALLOWED must be parseable as a uint64");

            info!(
                "Allocated {bytes_consumed} bytes thus far... I can only have {mbytes_to_consume}MB"
            );
            if bytes_consumed >= (mbytes_to_consume * 1024 * 1024) {
                info!("Finalizing after consuming {bytes_consumed} bytes of memory");
                break;
            }
        }
    }

    let version = writer.flush_and_commit(&mut table).await?;
    info!("Successfully flushed v{version} via append_batches to Delta table");
    debug!("Flushing consumer to wrap up the execution");
    consumer.flush().await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    deltalake::aws::register_handlers(None);
    tracing::init_default_subscriber();
    let _ =
        env::var("DELTA_TABLE_URI").expect("The `DELTA_TABLE_URI` must be set in the environment");
    info!("Starting file-loader");

    run(service_fn(function_handler)).await
}

/// Extract the suffix from the given [S3EventRecord] for matching and data loading
fn suffix_from_record(record: &S3EventRecord) -> RecordType {
    if let Some(key) = record.s3.object.key.as_ref() {
        if key.ends_with(".jsonl") || key.ends_with(".json") {
            return RecordType::Jsonl;
        }
    }
    if let Some(key) = record.s3.object.url_decoded_key.as_ref() {
        if key.ends_with(".jsonl") || key.ends_with(".json") {
            return RecordType::Jsonl;
        }
    }
    RecordType::Unknown
}

/// Convert an [oxbow_sqs::Message] into an [SqsMessage]
fn convert_from_sqs(message: oxbow_sqs::Message) -> SqsMessage {
    SqsMessage {
        message_id: message.message_id,
        receipt_handle: message.receipt_handle,
        body: message.body,
        md5_of_body: message.md5_of_body,
        md5_of_message_attributes: message.md5_of_message_attributes,
        // Translating message_attributes structs is an exercise for later
        //attributes: message.message_attributes.unwrap_or_default()
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_lambda_events::s3::{S3Entity, S3EventRecord, S3Object};
    use std::sync::Arc;

    /// This test is largely just for sanity checking the [ReaderBuilder] approach for
    /// deserializing line-delimited JSON
    #[test]
    fn test_json_deserialization() -> anyhow::Result<()> {
        use deltalake::arrow::array::RecordBatch;
        use deltalake::arrow::datatypes::*;
        use std::fs::File;
        use std::io::BufReader;
        let buf = File::open("../../tests/data/senators.jsonl")?;
        let reader = BufReader::new(buf);

        // Normally the schema would come from the Delta table but for this test, providing an
        // arrow schema manually
        let schema = Arc::new(Schema::new(vec![
            Field::new("current", DataType::Boolean, false),
            Field::new("description", DataType::Utf8, false),
            Field::new("party", DataType::Utf8, false),
        ]));

        let json = deltalake::arrow::json::ReaderBuilder::new(schema).build(reader)?;
        let batches: Vec<RecordBatch> = json.into_iter().map(|i| i.unwrap()).collect();

        deltalake::arrow::util::pretty::print_batches(&batches)?;

        Ok(())
    }

    #[test]
    fn test_suffix_from_record() {
        let d = S3EventRecord::default();
        assert_eq!(suffix_from_record(&d), RecordType::Unknown);

        let jsonl = S3EventRecord {
            s3: S3Entity {
                object: S3Object {
                    key: Some("some/prefix/fileA.jsonl".into()),
                    url_decoded_key: Some("some/prefix/fileA.jsonl".into()),
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Default::default()
        };
        assert_eq!(suffix_from_record(&jsonl), RecordType::Jsonl);
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
}
