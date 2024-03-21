/*
 * The lambda crate contains the Lambda specific implementation of oxbow.
 */

use aws_lambda_events::sqs::SqsEvent;
use deltalake::DeltaTableError;
use lambda_runtime::{service_fn, Error, LambdaEvent};
use serde_json::Value;
use tracing::log::*;
use url::Url;

use oxbow_lambda_shared::*;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        // disable printing the name of the module in every log line.
        .with_target(false)
        // disabling time is handy because CloudWatch will add the ingestion time.
        .without_time()
        .init();
    info!("Starting oxbow");
    info!("Starting the Lambda runtime");
    let func = service_fn(func);
    lambda_runtime::run(func)
        .await
        .expect("Failed while running the lambda handler");
    Ok(())
}

async fn func<'a>(event: LambdaEvent<SqsEvent>) -> Result<Value, Error> {
    debug!("Receiving event: {:?}", event);
    let records = match std::env::var("UNWRAP_SNS_ENVELOPE") {
        Ok(_) => s3_from_sns(event.payload)?,
        Err(_) => s3_from_sqs(event.payload)?,
    };
    debug!("processing records: {records:?}");
    let records = records_with_url_decoded_keys(&records);
    let by_table = objects_by_table(&records);

    if by_table.is_empty() {
        info!("No elligible events found, exiting early");
        return Ok("{}".into());
    }

    debug!("Grouped by table: {by_table:?}");

    for table_name in by_table.keys() {
        let location = Url::parse(table_name).expect("Failed to turn a table into a URL");
        debug!("Handling table: {:?}", location);
        let table_mods = by_table
            .get(table_name)
            .expect("Failed to get the files for a table, impossible!");
        let lock_client = oxbow::lock::client_for(table_name);
        let lock = oxbow::lock::acquire(table_name, &lock_client).await;

        match oxbow::lock::open_table(table_name).await {
            Ok(mut table) => {
                info!("Opened table to append: {:?}", table);

                match oxbow::append_to_table(table_mods.adds.as_slice(), &mut table).await {
                    Ok(version) => {
                        info!(
                            "Successfully appended version {} to table at {}",
                            version, location
                        );

                        if version % 10 == 0 {
                            info!("Creating a checkpoint for {}", location);
                            debug!("Reloading the table state to get the latest version");
                            let _ = table.load().await;
                            if table.version() == version {
                                match deltalake::checkpoints::create_checkpoint(&table).await {
                                    Ok(_) => info!("Successfully created checkpoint"),
                                    Err(e) => {
                                        error!("Failed to create checkpoint for {location}: {e:?}")
                                    }
                                }
                            } else {
                                error!("The table was reloaded to create a checkpoint but a new version already exists!");
                            }
                        }
                    }
                    Err(err) => {
                        error!("Failed to append to the table {}: {:?}", location, err);
                        let _ = oxbow::lock::release(lock, &lock_client).await;
                        return Err(Box::new(err));
                    }
                }

                if !table_mods.removes.is_empty() {
                    info!(
                        "{} Remove actions are expected in this operation",
                        table_mods.removes.len()
                    );
                    match oxbow::remove_from_table(table_mods.removes.as_slice(), &mut table).await
                    {
                        Ok(version) => {
                            info!(
                                "Successfully created version {} with remove actions",
                                version
                            );
                        }
                        Err(err) => {
                            error!(
                                "Failed to create removes on the table {}: {:?}",
                                location, err
                            );
                            let _ = oxbow::lock::release(lock, &lock_client).await;
                            return Err(Box::new(err));
                        }
                    }
                }
            }
            Err(DeltaTableError::NotATable(_e)) => {
                // create the table with our objects
                info!("Creating new Delta table at: {location}");
                let table =
                    oxbow::convert(table_name, Some(oxbow::lock::storage_options(table_name)))
                        .await;
                info!("Created table at: {location}");

                if table.is_err() {
                    error!("Failed to create new Delta table: {:?}", table);
                    // Propogate that error up so the function fails
                    let _ = table?;
                }
            }
            Err(source) => {
                let _ = oxbow::lock::release(lock, &lock_client).await;
                error!("Failed to open the Delta table for some reason: {source:?}");
                return Err(Box::new(source));
            }
        }

        let _ = oxbow::lock::release(lock, &lock_client).await;
    }

    Ok("[]".into())
}

#[cfg(test)]
mod tests {}
