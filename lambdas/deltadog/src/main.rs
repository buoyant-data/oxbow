use aws_lambda_events::event::sqs::SqsEvent;
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use tracing::log::*;

use oxbow_lambda_shared::*;

async fn function_handler(event: LambdaEvent<SqsEvent>) -> Result<(), Error> {
    debug!("Receiving event: {:?}", event);
    let records = s3_from_sqs(event.payload)?;
    debug!("processing records: {records:?}");
    let records = records_with_url_decoded_keys(&records);
    let by_table = objects_by_table(&records);

    if by_table.is_empty() {
        info!("No elligible events found, exiting early");
        return Ok(());
    }

    for table_name in by_table.keys() {
        match deltalake::open_table(table_name).await {
            Ok(table) => {
                debug!("Opened table {table:?} for metrics tracking");
            },
            Err(e) => {
                error!("Failed to open Delta table: {e:?}");
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        // disable printing the name of the module in every log line.
        .with_target(false)
        // disabling time is handy because CloudWatch will add the ingestion time.
        .without_time()
        .init();

    info!("Starting deltadog");
    run(service_fn(function_handler)).await
}
