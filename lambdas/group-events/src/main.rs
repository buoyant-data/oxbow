use aws_lambda_events::event::sqs::SqsEvent;
use aws_lambda_events::s3::{S3Event, S3EventRecord};
use aws_sdk_sqs::types::SendMessageBatchRequestEntry;
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use tracing::log::*;
use uuid::Uuid;

use std::collections::HashMap;

use oxbow_lambda_shared::*;

/**
 * The `func` function is the main Lambda entrypoint and handles receiving the messages in order to
 * output them with a group IDs
 */
async fn func(event: LambdaEvent<SqsEvent>) -> Result<(), Error> {
    let config = aws_config::load_from_env().await;
    let client = aws_sdk_sqs::Client::new(&config);

    debug!("Receiving event: {:?}", event);
    let records = match std::env::var("UNWRAP_SNS_ENVELOPE") {
        Ok(_) => s3_from_sns(event.payload)?,
        Err(_) => s3_from_sqs(event.payload)?,
    };
    let segmented = segmented_by_prefix(&records)?;
    debug!("Segmented into the following keys: {:?}", segmented.keys());

    let queue_url = std::env::var("QUEUE_URL").expect("Failed to get the FIFO output queue");

    let mut entries: Vec<SendMessageBatchRequestEntry> = vec![];

    for (group_id, records) in segmented.iter() {
        // The group-id for the batch must only contain alphanumberic characters and punctuation
        // and be 1-128 characters. Computing the md5 hash here ensures that this group_id is
        // always compatible with SQS
        let group_id = md5::compute(group_id);
        info!(
            "Sending {} records with group_id: {:x}",
            records.len(),
            group_id
        );
        let uuid = Uuid::new_v4();
        let body = S3Event {
            records: records.to_vec(),
        };
        let entry = SendMessageBatchRequestEntry::builder()
            .id(uuid.simple().to_string())
            .message_body(serde_json::to_string(&body)?)
            .message_group_id(format!("{:x}", group_id))
            .build()?;
        entries.push(entry);
    }
    debug!("Ordered entries to send: {entries:?}");

    if !entries.is_empty() {
        info!("Relaying {} entries to {queue_url}", entries.len());
        let response = client
            .send_message_batch()
            .queue_url(queue_url.clone())
            .set_entries(Some(entries))
            .send()
            .await?;
        debug!("SQS response: {response:?}");
        info!("Successfully batched events into SQS FIFO at {queue_url}");
    } else {
        info!("No entries to send");
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
    info!("Starting events grouping lambda");
    let _ = std::env::var("QUEUE_URL")
        .expect("Must be configured with the `QUEUE_URL` for the FIFO output");

    run(service_fn(func)).await
}

/**
 * Take the given records and group them by the table path prefix
 *
 * The key of the resulting hashmap can be then used as the message id for pushing the messages
 * into a fifo queue
 */
fn segmented_by_prefix(
    records: &[S3EventRecord],
) -> Result<HashMap<String, Vec<S3EventRecord>>, Error> {
    let mut segments = HashMap::new();

    for record in records_with_url_decoded_keys(records) {
        if let Some(bucket) = &record.s3.bucket.name {
            let log_path = infer_log_path_from(record.s3.object.url_decoded_key.as_ref().unwrap());
            let key = format!("s3://{}/{}", bucket, log_path);

            if !segments.contains_key(&key) {
                segments.insert(key.clone(), vec![]);
            }
            if let Some(objects) = segments.get_mut(&key) {
                objects.push(record.clone());
            }
        }
    }
    Ok(segments)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_lambda_events::s3::S3Event;

    #[test]
    fn test_segment_events() {
        let buf = std::fs::read_to_string("../../tests/data/s3-event-multiple.json")
            .expect("Failed to read file");
        let event: S3Event = serde_json::from_str(&buf).expect("Failed to parse");
        assert_eq!(4, event.records.len());

        let fifos = segmented_by_prefix(&event.records).expect("Failed to segment");
        assert_eq!(
            2,
            fifos.keys().len(),
            "The segmented test file should have only two prefixes"
        );
    }
}
