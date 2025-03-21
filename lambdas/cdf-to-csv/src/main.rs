use aws_config;
use aws_lambda_events::event::sqs::SqsEvent;
use aws_lambda_events::s3::S3EventRecord;
use aws_sdk_s3;
use lambda_runtime::{run, service_fn, tracing, Error, LambdaEvent};
use oxbow_lambda_shared::*;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::record::RowAccessor;
use tracing::log::*;

mod mysql_value;
mod s3_upload;
use mysql_value::ToMysqlValue;

const CHANGE_TYPE_COLUMN: &str = "_change_type";
const INSERT: &str = "insert";
const UPDATE_POSTIMAGE: &str = "update_postimage";
const DELETE: &str = "delete";

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        // disable printing the name of the module in every log line.
        .with_target(false)
        // disabling time is handy because CloudWatch will add the ingestion time.
        .without_time()
        .init();

    info!("Starting cdf-to-csv");

    run(service_fn(function_handler)).await
}

async fn function_handler(event: LambdaEvent<SqsEvent>) -> Result<(), Error> {
    debug!("Receiving event: {:?}", event);

    let config = aws_config::load_defaults(aws_config::BehaviorVersion::v2024_03_28()).await;
    let client = aws_sdk_s3::Client::new(&config);

    let records = match std::env::var("UNWRAP_SNS_ENVELOPE") {
        Ok(_) => s3_from_sns(event.payload)?,
        Err(_) => s3_from_sqs(event.payload)?,
    };

    let records: Vec<S3EventRecord> = records_with_url_decoded_keys(&records);
    debug!("processing records: {records:?}");

    for record in records {
        match suffix_from_record(&record) {
            RecordType::Parquet => {
                process_parquet_file(
                    &client,
                    &record.s3.bucket.name.as_ref().unwrap(),
                    &record.s3.object.url_decoded_key.as_ref().unwrap(),
                )
                .await
                .map_err(|e| {
                    format!(
                        "error while processing s3://{}/{}: {:?}",
                        &record.s3.bucket.name.unwrap(),
                        &record.s3.object.url_decoded_key.unwrap(),
                        e
                    )
                })?;
            }
            RecordType::Unknown => {
                error!("cdf-to-csv was invoked for a file with an unknown extension! Ignoring: {record:?}");
            }
        }
    }
    Ok(())
}

async fn process_parquet_file(
    client: &aws_sdk_s3::Client,
    bucket: &String,
    key: &String,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Processing s3://{}/{}", bucket, key);

    let body = client
        .get_object()
        .bucket(bucket.clone())
        .key(key.clone())
        .send()
        .await
        .map_err(|e| format!("S3 get object: {:?}", e))?
        .body
        .collect()
        .await
        .map(|data| data.into_bytes())
        .map_err(|e| format!("S3 reading body: {:?}", e))?;

    let reader =
        SerializedFileReader::new(body).map_err(|e| format!("can't read parquet {:?}", e))?;

    let max_rows_per_file = std::env::var("CSV_MAX_LINES_PER_FILE")
        .unwrap_or_else(|_| "1000000".to_string())
        .parse::<usize>()
        .expect("CSV_MAX_LINES_PER_FILE must be a number");

    let bucket = std::env::var("CSV_OUTPUT_BUCKET").expect("CSV_OUTPUT_BUCKET not set");
    let update_prefix = std::env::var("CSV_OUTPUT_PREFIX").expect("CSV_OUTPUT_PREFIX not set");
    let delete_prefix = std::env::var("CSV_DELETE_PREFIX").ok();
    let basename = basename_from_key(key);

    let change_type_index =
        column_index(&reader, CHANGE_TYPE_COLUMN).ok_or_else(|| "_change_type column not")?;
    let primary_key_column = std::env::var("DELETE_PRIMARY_KEY")
        .ok()
        .and_then(|name| column_index(&reader, &name));

    let row_iter = reader
        .get_row_iter(None)
        .map_err(|e| format!("can't read row: {:?}", e))?;

    let mut updates = s3_upload::S3Upload::new(
        client,
        &bucket,
        &update_prefix,
        &basename,
        max_rows_per_file,
    );
    let mut deletes: Option<s3_upload::S3Upload> = delete_prefix.map(|prefix| {
        s3_upload::S3Upload::new(client, &bucket, &prefix, &basename, max_rows_per_file)
    });

    for row in row_iter {
        let row = row.map_err(|e| format!("failed reading row: {:?}", e))?;

        let change_type = row.get_string(change_type_index)?;
        if change_type == INSERT || change_type == UPDATE_POSTIMAGE {
            let fields = row
                .get_column_iter()
                .filter_map(|(name, field)| {
                    if name != CHANGE_TYPE_COLUMN {
                        Some(
                            field
                                .to_mysql_value()
                                .map_err(|e| format!("error encoding {}: {:?}", name, e)),
                        )
                    } else {
                        None
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;

            updates.write_record(fields).await?;
        } else if change_type == DELETE {
            match (primary_key_column, deletes.as_mut()) {
                (Some(primary_key_column), Some(ref mut deletes)) => {
                    let primary_key_value = row
                        .get_column_iter()
                        .skip(primary_key_column)
                        .next()
                        .ok_or_else(|| "cannot get primary key, row does not have enough columns")?
                        .1
                        .to_mysql_value()
                        .map_err(|e| {
                            format!("cannot convert primary key to mysql value: {:?}", e)
                        })?;
                    deletes.write_record(vec![primary_key_value]).await?;
                }
                _ => {}
            }
        }
    }

    updates.close().await?;
    if let Some(deletes) = deletes {
        deletes.close().await?;
    }

    Ok(())
}

fn column_index(parquet: &dyn FileReader, column_name: &str) -> Option<usize> {
    parquet
        .metadata()
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .enumerate()
        .find(|(_idx, column)| column.name() == column_name)
        .map(|(idx, _)| idx)
}

fn basename_from_key(key: &String) -> String {
    key.split('/')
        .last()
        .unwrap()
        .split('.')
        .next()
        .unwrap()
        .to_string()
}

#[derive(Debug, PartialEq)]
enum RecordType {
    Parquet,
    Unknown,
}

fn suffix_from_record(record: &S3EventRecord) -> RecordType {
    if let Some(key) = record.s3.object.key.as_ref() {
        if key.ends_with(".parquet") {
            return RecordType::Parquet;
        }
    }
    if let Some(key) = record.s3.object.url_decoded_key.as_ref() {
        if key.ends_with(".parquet") {
            return RecordType::Parquet;
        }
    }
    RecordType::Unknown
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_lambda_events::s3::{S3Entity, S3EventRecord, S3Object};

    #[test]
    fn test_suffix_from_record() {
        let d = S3EventRecord::default();
        assert_eq!(suffix_from_record(&d), RecordType::Unknown);

        let jsonl = S3EventRecord {
            s3: S3Entity {
                object: S3Object {
                    key: Some("some/prefix/fileA.jsonl".into()),
                    url_decoded_key: Some("some/prefix/fileA.parquet".into()),
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Default::default()
        };
        assert_eq!(suffix_from_record(&jsonl), RecordType::Parquet);
    }
}
