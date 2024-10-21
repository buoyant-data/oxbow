///
/// The oxbow lambda crate contains the Lambda-specific handling of the Oxbow lambda
///
/// While most of the key logic does exist in the oxbow and oxbow-lambda-shared crates, this
/// function glues that into the Lambda runtime
use aws_lambda_events::sqs::SqsEvent;
use deltalake::DeltaTableError;
use lambda_runtime::{service_fn, Error, LambdaEvent};
use serde_json::Value;
use tracing::log::*;
use url::Url;

use oxbow_lambda_shared::*;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    deltalake::aws::register_handlers(None);
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        // disable printing the name of the module in every log line.
        .with_target(false)
        // disabling time is handy because CloudWatch will add the ingestion time.
        .without_time()
        .init();
    info!("Starting the Lambda runtime");
    info!("Starting oxbow");
    let func = service_fn(func);
    lambda_runtime::run(func)
        .await
        .expect("Failed while running the lambda handler");
    Ok(())
}

async fn func<'a>(event: LambdaEvent<SqsEvent>) -> Result<Value, Error> {
    debug!("Receiving event: {:?}", event);
    let can_evolve_schema: bool = std::env::var("SCHEMA_EVOLUTION").is_ok();
    if can_evolve_schema {
        info!("Schema evolution has been enabled based on the environment variable");
    }

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

        match oxbow::lock::open_table(table_name).await {
            Ok(mut table) => {
                info!("Opened table to append: {:?}", table);

                let actions = oxbow::actions_for(table_mods, &table, can_evolve_schema)
                    .await
                    .expect("Failed to generate actions for the table modifications");

                match oxbow::commit_to_table(&actions, &mut table).await {
                    Ok(version) => {
                        info!(
                            "Successfully appended version {} to table at {}",
                            version, location
                        );

                        if should_checkpoint(version) {
                            info!("Creating a checkpoint for {}", location);
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
                        return Err(Box::new(err));
                    }
                }
            }
            Err(DeltaTableError::NotATable(_e)) => {
                let lock = oxbow::lock::acquire(table_name, &lock_client).await;
                // create the table with our objects
                info!("Creating new Delta table at: {location}");
                let options = oxbow::lock::storage_options(table_name);
                let table = oxbow::convert(table_name, Some(options)).await;
                let _ = oxbow::lock::release(lock, &lock_client).await;

                // After releasing the lock, unpack the error
                if table.is_err() {
                    error!("Failed to create new Delta table: {:?}", table);
                    // Propogate that error up so the function fails
                    let _ = table?;
                } else {
                    info!("Created table at: {location}");
                }
            }
            Err(source) => {
                error!("Failed to open the Delta table for some reason: {source:?}");
                return Err(Box::new(source));
            }
        }
    }

    Ok("[]".into())
}

/// Determine whether the version qualitfies for checkpointing
fn should_checkpoint(version: i64) -> bool {
    (version > 0) && (version % 10 == 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_checkpoint() {
        assert!(should_checkpoint(10));
        assert!(should_checkpoint(100));
        assert!(!should_checkpoint(1));
        assert!(!should_checkpoint(11));
        assert!(!should_checkpoint(0));
    }
}
