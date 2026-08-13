///
/// The sqs-ingest function is for ingesting simple JSON payloads from SQS
/// and appending them to the configured Delta table
///
use aws_lambda_events::event::sqs::{SqsEvent, SqsMessage};
use lambda_runtime::{Error, LambdaEvent, run, service_fn, tracing};
use serde::Deserialize;
use tracing::log::*;

use oxbow::write::*;
use oxbow_sqs::{ConsumerConfig, TimedConsumer};

use std::env;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::task::JoinSet;

/// This is the primary invocation point for the lambda and should do the heavy lifting
async fn function_handler(event: LambdaEvent<SqsEvent>) -> Result<(), Error> {
    let table_uri = std::env::var("DELTA_TABLE_URI").expect("Failed to get `DELTA_TABLE_URI`");
    let buffer_more = std::env::var("BUFFER_MORE_QUEUE_URL").is_ok();

    let fn_start_since_epoch_ms: u128 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Failed to get a system time after unix epoch")
        .as_millis();

    // Hold onto the instant that the function started in order to attempt to exit on time.
    let fn_start = Instant::now();
    trace!("payload received: {:?}", event.payload.records);

    let config = aws_config::from_env().load().await;
    // Millis to allow for consuming more messages
    let more_deadline_ms = ((event.context.deadline as u128) - fn_start_since_epoch_ms) / 2;
    let mut limit: Option<u64> = None;

    if let Ok(how_many_more) = std::env::var("BUFFER_MORE_MESSAGES") {
        limit =
            Some(how_many_more.parse().expect(
                "The value of BUFFER_MORE_MESSAGES cannot be coerced into an int :thinking:",
            ));
        debug!("sqs-ingest configured to consume an additional {how_many_more} messages from SQS");
        debug!(
            "sqs-ingest will attempt to retrieve {how_many_more} messages in no more than {more_deadline_ms}ms to avoid timing out the function"
        );
    }

    let mut consumer = TimedConsumer::new(
        ConsumerConfig::default(),
        &config,
        Duration::from_millis(
            more_deadline_ms
                .try_into()
                .expect("Failed to compute a 64-bit deadline"),
        ),
    );
    consumer.limit = limit;

    debug!(
        "Context deadline in milliseconds since epoch is: {}",
        event.context.deadline
    );
    // records should contain the raw deserialized JSON payload that was sent through SQS. It
    // should be "fit" for writing to Delta
    let mut records: Vec<String> = vec![];

    if buffer_more {
        while let Some(batch) = consumer.next().await? {
            // Use parallel processing for larger batches, sequential for small batches
            // This threshold can be tuned based on performance testing
            const PARALLEL_THRESHOLD: usize = 50;

            if batch.len() > PARALLEL_THRESHOLD {
                // Large batch: use parallel processing for better CPU utilization
                let batch_len = batch.len();
                debug!(
                    "Processing batch of {} messages using PARALLEL mode (threshold: {} messages)",
                    batch_len, PARALLEL_THRESHOLD
                );
                let parallel_results = process_messages_parallel(batch).await;
                debug!("Completed parallel processing of {} messages", batch_len);
                records.extend(parallel_results);
            } else {
                // Small batch: use sequential processing to avoid overhead
                let batch_len = batch.len();
                debug!(
                    "Processing batch of {} messages using SEQUENTIAL mode (below threshold of {} messages)",
                    batch_len, PARALLEL_THRESHOLD
                );
                records.extend(extract_json_from_sqs_direct(batch));
                debug!("Completed sequential processing of {} messages", batch_len);
            }
        }
    }

    // Add the messages that actually triggered this function invocation
    records.extend(extract_json_from_records(&event.payload.records));

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

    if buffer_more {
        debug!("more messages were buffered and we need to flush() to clean them up");
        consumer.flush().await?;
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
fn extract_json_from_sqs_direct(
    messages: Vec<aws_sdk_sqs::types::Message>,
) -> Box<dyn Iterator<Item = String>> {
    if inside_sns() {
        Box::new(
            messages
                .into_iter()
                .filter(|m| m.body().is_some())
                .map(|m| m.body().as_ref().unwrap().to_string())
                .flat_map(move |b| {
                    let value: SNSWrapper = serde_json::from_str(&b)
                        .expect("Failed to deserialize SNS payload as JSON");
                    value.into_iter()
                }),
        )
    } else {
        Box::new(
            messages
                .into_iter()
                .filter(|m| m.body().is_some())
                .map(|m| m.body().as_ref().unwrap().to_string()),
        )
    }
}

/// Convert the `body` payloads from [SqsMessage] entities into JSONL
/// which can be passed into the [oxbow::write::append_values] function
fn extract_json_from_records(records: &[SqsMessage]) -> Box<dyn Iterator<Item = String> + '_> {
    if inside_sns() {
        Box::new(
            records
                .iter()
                .filter(|m| m.body.is_some())
                .flat_map(move |m| {
                    let value: SNSWrapper = serde_json::from_str(m.body.as_ref().unwrap())
                        .expect("Failed to deserialize SNS payload as JSON");
                    value.into_iter()
                }),
        )
    } else {
        Box::new(
            records
                .iter()
                .filter(|m| m.body.is_some())
                .map(|m| m.body.as_ref().unwrap().clone()),
        )
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct SNSWrapper {
    records: Vec<serde_json::Value>,
}

// Use an optimal batch size for parallel processing
// Larger batches benefit more from parallelization
const PARALLEL_BATCH_SIZE: usize = 50;

/// Parallel JSON processing function using Tokio JoinSet
/// This function processes messages in parallel across multiple CPU cores
/// for improved throughput on CPU-bound JSON operations
async fn process_messages_parallel(messages: Vec<aws_sdk_sqs::types::Message>) -> Vec<String> {
    trace!(
        "Starting parallel JSON processing of {} messages (batch size: {})",
        messages.len(),
        PARALLEL_BATCH_SIZE
    );

    let mut join_set = JoinSet::new();

    // Split messages into chunks for parallel processing
    debug!(
        "Splitting {} messages into {} parallel chunks for CPU-bound JSON processing",
        messages.len(),
        messages.chunks(PARALLEL_BATCH_SIZE).count()
    );

    for chunk in messages.chunks(PARALLEL_BATCH_SIZE) {
        let chunk = chunk.to_vec();
        join_set.spawn(async move {
            let mut result = Vec::new();

            for message in chunk {
                if let Some(body) = message.body() {
                    if inside_sns() {
                        // SNS mode: parse wrapper and extract records
                        if let Ok(value) = serde_json::from_str::<SNSWrapper>(body) {
                            let sns_records = value.into_iter().collect::<Vec<String>>();
                            result.extend(sns_records);
                        }
                    } else {
                        // Direct mode: use body as-is
                        result.push(body.to_string());
                    }
                }
            }

            result
        });
    }

    // Collect results from all parallel tasks
    trace!("Collecting results from {} parallel tasks", join_set.len());

    let mut results = Vec::new();
    while let Some(res) = join_set.join_next().await {
        match res {
            Ok(batch_result) => {
                let batch_result_len = batch_result.len();
                results.extend(batch_result);
                trace!(
                    "Successfully collected {} records from parallel task",
                    batch_result_len
                );
            }
            Err(e) => {
                error!("Parallel task failed: {:?}", e);
            }
        }
    }

    debug!(
        "Completed parallel JSON processing of {} messages, produced {} records",
        messages.len(),
        results.len()
    );

    results
}

impl SNSWrapper {
    /// to_vec() will handle converting all the deserialized JSON inside the wrapper back into
    /// strings for passing deeper into oxbow
    fn into_iter(self) -> impl Iterator<Item = String> {
        self.records
            .into_iter()
            .map(|v| serde_json::to_string(&v).expect("Failed to reserialize SNS JSON"))
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

        let res: Vec<String> = extract_json_from_sqs_direct(vec![message]).collect();
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

        let res: Vec<String> = extract_json_from_sqs_direct(vec![message]).collect();

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

        let values: Vec<String> = extract_json_from_records(&messages).collect();
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

        let values: Vec<String> = extract_json_from_records(&event.records).collect();

        unsafe {
            std::env::remove_var("UNWRAP_SNS_ENVELOPE");
        }
        assert_eq!(values, vec![r#"{"eventVersion":"2.1"}"#]);
    }
}
