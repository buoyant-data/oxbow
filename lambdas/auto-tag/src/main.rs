use aws_lambda_events::event::s3::S3EventRecord;
use aws_lambda_events::event::sqs::SqsEvent;
use aws_sdk_s3::types::{Tag, Tagging};
use lambda_runtime::{Error, LambdaEvent, run, service_fn};
use tracing::log::*;

use std::collections::HashMap;

use oxbow_lambda_shared::*;

/// This is the main body for the function.
async fn function_handler(event: LambdaEvent<SqsEvent>) -> Result<(), Error> {
    let config = aws_config::load_from_env().await;
    let client = aws_sdk_s3::Client::new(&config);

    debug!("Receiving event: {:?}", event);
    let records = match std::env::var("UNWRAP_SNS_ENVELOPE") {
        Ok(_) => s3_from_sns(event.payload)?,
        Err(_) => s3_from_sqs(event.payload)?,
    };
    let records = records_with_url_decoded_keys(&records);
    let extensions = extensions_for(&records);

    for (locator, tag) in extensions.iter() {
        info!("Setting tag: fileext:{tag} for {locator:?}");
        client
            .put_object_tagging()
            .bucket(&locator.bucket)
            .key(&locator.key)
            .tagging(
                Tagging::builder()
                    .tag_set(Tag::builder().key("fileext").value(tag).build()?)
                    .build()?,
            )
            .send()
            .await?;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        // disable printing the name of the module in every log line.
        .with_target(false)
        // disabling time is handy because CloudWatch will add the ingestion time.
        .without_time()
        .init();
    info!("Starting auto-tag lambda");

    run(service_fn(function_handler)).await
}

/// Simmple struct for keeping track of a stored object's "location" of bucket/key
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct StoredObject {
    bucket: String,
    key: String,
}

/// Return a mapping of the extensions for each key in the [S3EventRecord] set
fn extensions_for(records: &[S3EventRecord]) -> HashMap<StoredObject, String> {
    use std::path::Path;

    let mut result = HashMap::new();

    for record in records {
        if let (Some(bucket), Some(key)) =
            (&record.s3.bucket.name, &record.s3.object.url_decoded_key)
        {
            let path = Path::new(key);
            if let Some(ext) = path.extension() {
                let stored = StoredObject {
                    bucket: bucket.clone(),
                    key: key.clone(),
                };
                result.insert(stored, ext.to_string_lossy().to_string());
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_lambda_events::event::s3::S3Event;

    #[test]
    fn test_extensions_for_events() {
        let buf = std::fs::read_to_string("../../tests/data/s3-event-multiple-urlencoded.json")
            .expect("Failed to read file");
        let event: S3Event = serde_json::from_str(&buf).expect("Failed to parse");
        assert_eq!(3, event.records.len());

        let urldecoded = records_with_url_decoded_keys(&event.records);
        let exts = extensions_for(&urldecoded);
        assert_eq!(exts.keys().len(), event.records.len());

        let locator = StoredObject {
            bucket: "example-bucket".into(),
            key: "some/first-prefix/some-file.parquet".into(),
        };

        assert_eq!(Some("parquet".to_string()), exts.get(&locator).cloned());

        let locator = StoredObject {
            bucket: "example-bucket".into(),
            key: "some/prefix/ds=2023-01-01/a.parquet".into(),
        };

        assert_eq!(Some("parquet".to_string()), exts.get(&locator).cloned());
    }
}
