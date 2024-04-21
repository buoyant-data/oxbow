///
/// The sqs-ingest function is for ingesting simple JSON payloads from SQS
/// and appending them to the configured Delta table
///
use aws_lambda_events::event::sqs::{SqsEvent, SqsMessage};
use lambda_runtime::{run, service_fn, tracing, Error, LambdaEvent};
use tracing::log::*;

use oxbow::write::*;

use std::env;

/// This is the primary invocation point for the lambda and should do the heavy lifting
async fn function_handler(event: LambdaEvent<SqsEvent>) -> Result<(), Error> {
    let table_uri = std::env::var("DELTA_TABLE_URI").expect("Failed to get `DELTA_TABLE_URI`");
    debug!("payload received: {:?}", event.payload.records);

    let jsonl = extract_json_from_records(&event.payload.records);
    debug!("jsonl generated: {jsonl}");

    if !jsonl.is_empty() {
        let table = oxbow::lock::open_table(&table_uri).await?;
        match append_values(table, &jsonl).await {
            Ok(_) => {}
            Err(e) => {
                error!("Failed to append the values to configured Delta table: {e:?}");
                return Err(Box::new(e));
            }
        }
    } else {
        error!("An empty payload was extracted which doesn't seem right!");
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

    let _ =
        env::var("DELTA_TABLE_URI").expect("The `DELTA_TABLE_URI` must be set in the environment");
    match env::var("DYNAMO_LOCK_TABLE_NAME") {
        Ok(_) => {}
        Err(_) => {
            warn!("sqs-ingest SHOULD have `DYNAMO_LOCK_TABLE_NAME` set to a valid name, and should have AWS_S3_LOCKING_PROVIDER=dynamodb set so that concurrent writes can be performed safely.");
        }
    }

    info!("Starting sqs-ingest");

    run(service_fn(function_handler)).await
}

/// Convert the `body` payloads from [SqsMessage] entities into JSONL
/// which can be passed into the [oxbow::write::append_values] function
fn extract_json_from_records(records: &[SqsMessage]) -> String {
    records
        .iter()
        .filter(|m| m.body.is_some())
        .map(|m| m.body.as_ref().unwrap().clone())
        .collect::<Vec<String>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_data() {
        let buf = r#"{
            "body" : "{\"key\" : \"value\"}"
        }"#;
        let message: SqsMessage = serde_json::from_str(buf).expect("Failed to deserialize");
        let messages = vec![
            message.clone(),
            message.clone(),
            message.clone(),
            SqsMessage::default(),
        ];

        let jsonl = extract_json_from_records(&messages);

        let expected = r#"{"key" : "value"}
{"key" : "value"}
{"key" : "value"}"#;
        assert_eq!(expected, jsonl);
    }
}
