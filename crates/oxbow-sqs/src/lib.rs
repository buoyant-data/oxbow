//!
//! The oxbow-sqs crate helps different lambda functions in the oxbow ecosystem use Amazon SQS as a
//! signaling mechanism without relying on the Lambda trigger to deliver all the data from SQS.
//!
//! This is helpful when applications need finer grained control on data retrieval or need to pull
//! in more than the 6MB limit of data that Lambda triggers provide for.
//!
//! From a design standpoint oxbow-sqs should allow functions to have a batch size of **1** and
//! then allow them to consume some optional number of messages until a time-based or other
//! threshold.
//!
//! This is outlined at a high level in [this blog post](https://www.buoyantdata.com/blog/2025-02-24-just-keep-buffering.html)

use aws_sdk_sqs::types::DeleteMessageBatchRequestEntry;
pub use aws_sdk_sqs::types::Message;
use tracing::log::*;
use url::Url;

use std::time::{Duration, Instant};

// How can I handle this:
//
// Perhaps I can create a function handler and just invoke the function handler with a constructed
// sqsevent for each one of the sqsevents that are retrieved in the buffer_more_messages?
//
// How can an application signal that it needs to be done?
//
// file loader needs to signal off of memory
// sqs-ingest probably needs to fgo off of time

/// Configuration for consuming messages from SQS
#[derive(Clone, Debug)]
pub struct ConsumerConfig {
    pub queue: Url,
    pub retrieval_max: i32,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        let url = Url::parse(
            &std::env::var("BUFFER_MORE_QUEUE_URL")
                .expect("Must set BUFFER_MORE_QUEUE_URL in the environment!"),
        )
        .expect("Failed to parse BUFFER_MORE_QUEUE_URL as a url");
        Self {
            queue: url,
            retrieval_max: 10,
        }
    }
}

/// A [TimedConsumer] helps consume from Amazon SQS up until a certain threshold of time, typically
/// used to stop consuming messages at a certain amount of the Lambda's runtime
#[derive(Clone, Debug)]
pub struct TimedConsumer {
    config: ConsumerConfig,
    /// The [Instant] when this consumer was created. This value is used to determine when to
    /// finalize based on the configured deadline
    start: Instant,
    consumer: aws_sdk_sqs::Client,
    /// Runtime deadline after which the timed consumer will enter the finalization phase and
    /// prepare to exit
    deadline: Duration,
    /// Limit to the total number of messages to receive. If this is not used, the consumer will
    /// only stop consuming once the deadline has been hit
    pub limit: Option<u64>,
    // Collection of needed to delete messages upon finalization
    receive_handles: Vec<DeleteMessageBatchRequestEntry>,
}

impl TimedConsumer {
    /// Initialize the [TimedConsumer]
    ///
    /// `config` is a provided [aws_config::SdkConfig] to be used for interacting with AWS services like SQS
    /// `deadline` is a number
    pub fn new(
        consumer_config: ConsumerConfig,
        sdk_config: &aws_config::SdkConfig,
        deadline: Duration,
    ) -> Self {
        let consumer = aws_sdk_sqs::Client::new(sdk_config);
        Self {
            deadline,
            consumer,
            config: consumer_config,
            start: Instant::now(),
            limit: None,
            receive_handles: vec![],
        }
    }

    pub async fn default() -> Self {
        let sdk_config = aws_config::from_env().load().await;
        // Setting a default of 5s because hey why not
        let deadline_str = std::env::var("OXBOW_SQS_DEADLINE_MS").unwrap_or("5000".into());
        let deadline: Duration = Duration::from_millis(
            deadline_str
                .parse()
                .expect("OXBOW_SQS_DEADLINE_MS must be parseable as an integer"),
        );
        Self::new(ConsumerConfig::default(), &sdk_config, deadline)
    }

    /// `next` can be used to consume messages from the configured SQS queue.
    ///
    /// This function will return `None` once there are no more messages to consume _or_ the
    /// deadline threshold has been reached
    pub async fn next(&mut self) -> anyhow::Result<Option<Vec<Message>>> {
        // Before this executes, check to see if we are approaching our deadline or limit
        //
        if let Some(limit) = self.limit.as_ref() {
            if self.receive_handles.len() >= (*limit as usize) {
                info!(
                    "The received messages has reached the limit of {:?}, ending the iteration",
                    self.limit
                );
                return Ok(None);
            }
        }

        if self.start.elapsed() >= self.deadline {
            debug!("The deadline has elapsed, returning from TimedConsumer");
            return Ok(None);
        }

        let visibility_timeout: i32 = self
            .deadline
            .as_secs()
            .try_into()
            .expect("Failed to convert deadline to seconds (i32)");

        let receive = self
            .consumer
            .receive_message()
            .max_number_of_messages(self.config.retrieval_max)
            // Set the visibility timeout to the timeout of the function to ensure that
            // messages are not made visible befoore the function exits
            .visibility_timeout(visibility_timeout)
            // Enable a smol long poll to receive messages
            .wait_time_seconds(1)
            .queue_url(self.config.queue.as_ref())
            .send()
            .await?;

        if let Some(messages) = receive.messages {
            debug!(
                "TimedConsumer received an additional {} messages",
                messages.len()
            );

            for message in &messages {
                self.receive_handles.push(
                    DeleteMessageBatchRequestEntry::builder()
                        .set_id(message.message_id.clone())
                        .set_receipt_handle(message.receipt_handle.clone())
                        .build()?,
                );
            }
            return Ok(Some(messages));
        } else {
            debug!("TimedConsumer received no additional messages, exiting");
        }

        Ok(None)
    }

    pub async fn flush(&mut self) -> anyhow::Result<()> {
        if self.receive_handles.is_empty() {
            debug!("Asked to flush an empty TimedConsumer, returning early");
            return Ok(());
        }
        debug!(
            "Marking {} messages in SQS as deleted",
            self.receive_handles.len()
        );

        // We can mark messages deleted 10 at a time
        for batch in self.receive_handles.chunks(10) {
            let _ = self
                .consumer
                .delete_message_batch()
                .queue_url(self.config.queue.as_ref())
                .set_entries(Some(batch.to_vec()))
                .send()
                .await?;
        }
        self.receive_handles = vec![];
        Ok(())
    }
}

impl std::ops::Drop for TimedConsumer {
    fn drop(&mut self) {
        if !self.receive_handles.is_empty() {
            error!("The TimedConsumer was not flushed before being dropped! This causes data duplication, you have to flush!");
        }
    }
}

#[cfg(test)]
mod tests {}
