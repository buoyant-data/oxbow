///
/// The sqs-ingest function is for ingesting simple JSON payloads from SQS
/// and appending them to the configured Delta table
///
use aws_lambda_events::event::sqs::{SqsEvent, SqsMessage};
use aws_sdk_sqs::types::DeleteMessageBatchRequestEntry;
use lambda_runtime::{run, service_fn, tracing, Error, LambdaEvent};
use serde::Deserialize;
use tracing::log::*;

use oxbow::write::*;

use std::env;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// This is the primary invocation point for the lambda and should do the heavy lifting
async fn function_handler(event: LambdaEvent<SqsEvent>) -> Result<(), Error> {
    let table_uri = std::env::var("DELTA_TABLE_URI").expect("Failed to get `DELTA_TABLE_URI`");
    let fn_start_since_epoch_ms: u128 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Failed to get a system time after unix epoch")
        .as_millis();

    // Hold onto the instant that the function started in order to attempt to exit on time.
    let fn_start = Instant::now();
    trace!("payload received: {:?}", event.payload.records);

    let config = aws_config::from_env().load().await;
    let sqs_client = aws_sdk_sqs::Client::new(&config);
    // How many more messages should sqs-ingest try to consume?
    let mut more_count = 0;
    debug!(
        "Context deadline in milliseconds since epoch is: {}",
        event.context.deadline
    );
    // Millis to allow for consuming more messages
    let more_deadline_ms: u128 = ((event.context.deadline as u128) - fn_start_since_epoch_ms) / 2;
    // records should contain the raw deserialized JSON payload that was sent through SQS. It
    // should be "fit" for writing to Delta
    let mut records: Vec<String> = vec![];
    // Messages to delete from SQS if manually fetched
    //
    // This needs to be stored until after the successful write to Delta to ensure message
    // persistence
    let mut received = vec![];

    if let Ok(how_many_more) = std::env::var("BUFFER_MORE_MESSAGES") {
        more_count = how_many_more
            .parse()
            .expect("The value of BUFFER_MORE_MESSAGES cannot be coerced into an int :thinking:");
        debug!("sqs-ingest configured to consume an additional {more_count} messages from SQS");
        debug!("sqs-ingest will attempt to retrieve {more_count} messages in no more than {more_deadline_ms}ms to avoid timing out the function");
    }

    if more_count > 0 {
        if let Ok(queue_url) = std::env::var("BUFFER_MORE_QUEUE_URL") {
            let mut completed = false;
            let mut fetched = 0;

            while !completed {
                let visibility_timeout: i32 = (more_deadline_ms / 1000).try_into()?;
                debug!("Fetching things from SQS with a {visibility_timeout}s visibility timeout");

                let receive = sqs_client
                    .receive_message()
                    .max_number_of_messages(10)
                    // Set the visibility timeout to the timeout of the function to ensure that
                    // messages are not made visible befoore the function exits
                    .visibility_timeout(visibility_timeout)
                    // Enable a smol long poll to receive messages
                    .wait_time_seconds(1)
                    .queue_url(queue_url.clone())
                    .send()
                    .await;

                if receive.is_err() {
                    error!("Received {receive:?} from SQS, not buffering more messages");
                    break;
                }
                let receive = receive.unwrap();

                for message in receive.messages.clone().unwrap_or_default() {
                    received.push(
                        DeleteMessageBatchRequestEntry::builder()
                            .set_id(message.message_id)
                            .set_receipt_handle(message.receipt_handle)
                            .build()?,
                    );
                }

                if receive.messages.is_none() {
                    debug!("Empty receive off SQS, continuing on");
                    break;
                }

                records.append(&mut extract_json_from_sqs_direct(
                    receive.messages.unwrap_or_default(),
                ));

                fetched += 1;
                completed =
                    (fetched >= more_count) || (fn_start.elapsed().as_millis() >= more_deadline_ms);
            }
        } else {
            error!("The function cannot buffer more messages without a BUFFER_MORE_QUEUE_URL! Only writing messages that triggered the Lambda");
        }
    }

    // Add the messages that actually triggered this function invocation
    records.append(&mut extract_json_from_records(&event.payload.records));

    if !records.is_empty() {
        let table = oxbow::lock::open_table(&table_uri).await?;

        match append_values(table, records.as_slice()).await {
            Ok(table) => {
                debug!("Appended {} values to: {table:?}", records.len());
            }
            Err(e) => {
                error!("Failed to append the values to configured Delta table: {e:?}");
                return Err(Box::new(e));
            }
        }
    } else {
        error!("An empty payload was extracted which doesn't seem right!");
    }

    if let Ok(queue_url) = std::env::var("BUFFER_MORE_QUEUE_URL") {
        if !received.is_empty() {
            info!(
                "Marking {} messages from SQS which were buffered as deleted",
                received.len()
            );
            for batch in received.chunks(10) {
                let _ = sqs_client
                    .delete_message_batch()
                    .queue_url(queue_url.clone())
                    .set_entries(Some(batch.to_vec()))
                    .send()
                    .await?;
            }
        }
    }

    debug!(
        "sqs-ingest completed its work in {}ms",
        fn_start.elapsed().as_millis()
    );

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

/// Extract and deserialize the JSON from messages which were directly consumed from SQS rather
/// than those received via Lambda triggering.
///
/// This corresponds to the messages consumed from BUFFER_MORE_QUEUE_URL
fn extract_json_from_sqs_direct(messages: Vec<aws_sdk_sqs::types::Message>) -> Vec<String> {
    if inside_sns() {
        messages
            .iter()
            .filter(|m| m.body().is_some())
            .map(|m| m.body().as_ref().unwrap().to_string())
            .map(|b| {
                let value: SNSWrapper =
                    serde_json::from_str(&b).expect("Failed to deserialize SNS payload as JSON");
                value.to_vec()
            })
            .flatten()
            .collect::<Vec<String>>()
    } else {
        messages
            .iter()
            .filter(|m| m.body().is_some())
            .map(|m| m.body().as_ref().unwrap().to_string())
            .collect::<Vec<String>>()
    }
}

/// Convert the `body` payloads from [SqsMessage] entities into JSONL
/// which can be passed into the [oxbow::write::append_values] function
fn extract_json_from_records(records: &[SqsMessage]) -> Vec<String> {
    if inside_sns() {
        records
            .iter()
            .filter(|m| m.body.is_some())
            .map(|m| {
                let value: SNSWrapper = serde_json::from_str(m.body.as_ref().unwrap())
                    .expect("Failed to deserialize SNS payload as JSON");
                value.to_vec()
            })
            .flatten()
            .collect::<Vec<String>>()
    } else {
        records
            .iter()
            .filter(|m| m.body.is_some())
            .map(|m| m.body.as_ref().unwrap().clone())
            .collect::<Vec<String>>()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct SNSWrapper {
    records: Vec<serde_json::Value>,
}

impl SNSWrapper {
    /// to_vec() will handle converting all the deserialized JSON inside the wrapper back into
    /// strings for passing deeper into oxbow
    fn to_vec(&self) -> Vec<String> {
        self.records
            .iter()
            .map(|v| serde_json::to_string(&v).expect("Failed to reserialize SNS JSON"))
            .collect()
    }
}

/// Return true if the function expecs an SNS envelope
fn inside_sns() -> bool {
    std::env::var("UNWRAP_SNS_ENVELOPE").is_ok()
}

/// These tests are for the BufferMore functionality
#[cfg(test)]
mod buffer_more_tests {
    use super::*;

    use aws_sdk_sqs::types::Message;
    use serial_test::serial;

    #[serial]
    #[test]
    fn test_extract_direct() {
        let message = Message::builder().body("hello").build();

        let res = extract_json_from_sqs_direct(vec![message]);
        assert_eq!(res, vec!["hello".to_string()]);
    }

    #[serial]
    #[test]
    fn test_extract_direct_with_sns() {
        let body = r#"{"Records":[{"eventVersion":"2.1"}]}"#;
        let message = Message::builder().body(body).build();

        unsafe {
            std::env::set_var("UNWRAP_SNS_ENVELOPE", "true");
        }

        let res = extract_json_from_sqs_direct(vec![message]);

        unsafe {
            std::env::remove_var("UNWRAP_SNS_ENVELOPE");
        }
        assert_eq!(res, vec![r#"{"eventVersion":"2.1"}"#]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[serial]
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

    #[serial]
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

        unsafe {
            std::env::set_var("UNWRAP_SNS_ENVELOPE", "true");
        }

        let values = extract_json_from_records(&event.records);

        unsafe {
            std::env::remove_var("UNWRAP_SNS_ENVELOPE");
        }
        assert_eq!(values, vec![r#"{"eventVersion":"2.1"}"#]);
    }
}
