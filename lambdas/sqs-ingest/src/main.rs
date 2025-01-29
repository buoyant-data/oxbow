///
/// The sqs-ingest function is for ingesting simple JSON payloads from SQS
/// and appending them to the configured Delta table
///
use aws_lambda_events::event::sqs::{SqsEvent, SqsMessage};
use lambda_runtime::{run, service_fn, tracing, Error, LambdaEvent};
use serde::Deserialize;
use tracing::log::*;

use oxbow::write::*;

use std::env;

/// This is the primary invocation point for the lambda and should do the heavy lifting
async fn function_handler(event: LambdaEvent<SqsEvent>) -> Result<(), Error> {
    let table_uri = std::env::var("DELTA_TABLE_URI").expect("Failed to get `DELTA_TABLE_URI`");
    debug!("payload received: {:?}", event.payload.records);

    let records = match std::env::var("UNWRAP_SNS_ENVELOPE") {
        Ok(_) => unwrap_sns_payload(&event.payload.records),
        Err(_) => event.payload.records,
    };

    let values = extract_json_from_records(&records);
    debug!("JSON pulled out: {values:?}");

    if !values.is_empty() {
        let table = oxbow::lock::open_table(&table_uri).await?;
        match append_values(table, values.as_slice()).await {
            Ok(table) => {
                debug!("Appended values to: {table:?}");
            }
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
    info!("Starting sqs-ingest");

    run(service_fn(function_handler)).await
}

/// Convert the `body` payloads from [SqsMessage] entities into JSONL
/// which can be passed into the [oxbow::write::append_values] function
fn extract_json_from_records(records: &[SqsMessage]) -> Vec<String> {
    records
        .iter()
        .filter(|m| m.body.is_some())
        .map(|m| m.body.as_ref().unwrap().clone())
        .collect::<Vec<String>>()
}

/// SNS cannot help but JSON encode all its payloads so sometimes we must unwrap it.
fn unwrap_sns_payload(records: &[SqsMessage]) -> Vec<SqsMessage> {
    let mut unpacked = vec![];
    for record in records {
        if let Some(body) = record.body.as_ref() {
            trace!("Attempting to unwrap the contents of nested JSON: {body}");
            let nested: SNSWrapper = serde_json::from_str(body).expect(
                "Failed to unpack SNS
messages, this could be a misconfiguration and there is no SNS envelope or raw_delivery has not
been set",
            );
            for body in nested.records {
                let message: SqsMessage = SqsMessage {
                    body: Some(serde_json::to_string(&body).expect("Failed to reserialize JSON")),
                    ..Default::default()
                };
                unpacked.push(message);
            }
        }
    }
    unpacked
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct SNSWrapper {
    records: Vec<serde_json::Value>,
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

        let values = extract_json_from_records(&messages);
        assert_eq!(values.len(), 3);

        let expected: Vec<String> = vec![
            r#"{"key" : "value"}"#.into(),
            r#"{"key" : "value"}"#.into(),
            r#"{"key" : "value"}"#.into(),
        ];
        assert_eq!(values, expected);
    }

    #[test]
    fn test_unwrap_sns() {
        // This is an example of what a full message can look like
        //let body = r#"{\"Records\":[{\"eventVersion\":\"2.1\",\"eventSource\":\"aws:s3\",\"awsRegion\":\"us-east-2\",\"eventTime\":\"2024-01-25T20:57:23.379Z\",\"eventName\":\"ObjectCreated:CompleteMultipartUpload\",\"userIdentity\":{\"principalId\":\"AWS:AROAU7FUYKEVYG4GF4IAV:s3-replication\"},\"requestParameters\":{\"sourceIPAddress\":\"10.0.153.194\"},\"responseElements\":{\"x-amz-request-id\":\"RYAX8R8CB6FF1MQN\",\"x-amz-id-2\":\"uLUt4C/TfjwvpObPlTnrWYjOIPH1YT1yJ8jZjqRyLIuTLOxGSkNgKc2Hd1/O7wTP2cd3u59lRtVYrU4ECizehRYw0NGNlL5b\"},\"s3\":{\"s3SchemaVersion\":\"1.0\",\"configurationId\":\"tf-s3-queue-20231207170751084400000001\",\"bucket\":{\"name\":\"scribd-data-warehouse-dev\",\"ownerIdentity\":{\"principalId\":\"A1FIHS1B0BWUTQ\"},\"arn\":\"arn:aws:s3:::scribd-data-warehouse-dev\"},\"object\":{\"key\":\"databases/airbyte/faker_users/ds%3D2024-01-25/1706216212007_0.parquet\",\"size\":143785,\"eTag\":\"67165dca52a1089d312e19c3ddf1e342-1\",\"versionId\":\"3v567TKlEQF5IBoeXrFBRiRX8vY.bY1m\",\"sequencer\":\"0065B2CB162FC1AC3B\"}}}]}"#;
        let body = r#"{"Records":[{"eventVersion":"2.1"}]}"#;
        let message: SqsMessage = SqsMessage {
            body: Some(body.to_string()),
            ..Default::default()
        };
        let event: SqsEvent = SqsEvent {
            records: vec![message],
        };
        let values = unwrap_sns_payload(&event.records);
        assert_eq!(values.len(), event.records.len());
        assert_eq!(Some(r#"{"eventVersion":"2.1"}"#), values[0].body.as_deref());
    }
}
