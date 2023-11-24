use aws_lambda_events::event::sqs::SqsEvent;
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use tracing::log::*;

/**
 * The `func` function is the main Lambda entrypoint and handles receiving the messages in order to
 * output them with a group IDs
 */
async fn func(event: LambdaEvent<SqsEvent>) -> Result<(), Error> {
    debug!("Receiving event: {:?}", event);

    // Extract some useful information from the request

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

    run(service_fn(func)).await
}

#[cfg(test)]
mod tests {
    use super::*;
}
