///
/// The file-loader function receives S3 Event Notifications and reads a number of different file
/// formats for appending records to existing Delta Lake tables
///
use aws_lambda_events::event::sqs::SqsEvent;
use aws_lambda_events::s3::S3EventRecord;
use lambda_runtime::{run, service_fn, tracing, Error, LambdaEvent};
use tracing::log::*;

use oxbow::write::*;
use oxbow_lambda_shared::*;

use std::env;

/// The type of data file which file-loader can load
#[derive(Clone, Debug, PartialEq)]
enum RecordType {
    /// Line-delimited JSON, e.g. .jsonl file
    Jsonl,
    /// The file format is unknown and file-loader cannot handle it
    Unknown,
}

/// This is the primary invocation point for the lambda and should do the heavy lifting
async fn function_handler(event: LambdaEvent<SqsEvent>) -> Result<(), Error> {
    let table_uri = std::env::var("DELTA_TABLE_URI").expect("Failed to get `DELTA_TABLE_URI`");
    debug!("Receiving event: {:?}", event);

    let records = match std::env::var("UNWRAP_SNS_ENVELOPE") {
        Ok(_) => s3_from_sns(event.payload)?,
        Err(_) => s3_from_sqs(event.payload)?,
    };
    let records: Vec<S3EventRecord> = records_with_url_decoded_keys(&records);
    debug!("processing records: {records:?}");

    let table = oxbow::lock::open_table(&table_uri)
        .await
        .expect("Failed to open the Delta table!");

    let config = aws_config::load_from_env().await;
    let client = aws_sdk_s3::Client::new(&config);

    let mut lines: Vec<String> = vec![];

    for file_record in records {
        match suffix_from_record(&file_record) {
            RecordType::Jsonl => {
                debug!("Preparing to load filue {file_record:?}");
                let response = client
                    .get_object()
                    .bucket(file_record.s3.bucket.name.clone().unwrap())
                    .key(file_record.s3.object.key.as_ref().unwrap())
                    .send()
                    .await?;
                debug!("Attempting to read bytes from {file_record:?}");
                let stream = response.body;
                let data = stream.collect().await.map(|data| data.into_bytes());
                // This is silly unnecessary but trying to get to splittable lines as quickly
                // and easily as possible
                let s = String::from_utf8(data.unwrap().into()).expect("File was not proper UTF8?");
                lines.extend(s.split("\n").map(|m| m.to_string()));
            }
            RecordType::Unknown => {
                error!("file-loader was invoked for a file with an unknown suffix! Ignoring: {file_record:?}");
            }
        }
    }

    let table = append_values(table, lines).await?;
    debug!("Appended values to: {table:?}");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    deltalake::aws::register_handlers(None);
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        // disable printing the name of the module in every log line.
        .with_target(false)
        // disabling time is handy because CloudWatch will add the ingestion time.
        .without_time()
        .init();

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
                    url_decoded_key: Some("some/prefix/fileA.jsonl".into()),
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Default::default()
        };
        assert_eq!(suffix_from_record(&jsonl), RecordType::Jsonl);
    }
}
