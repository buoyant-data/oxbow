/*
 * The lambda crate contains the Lambda specific implementation of oxbow.
 */

use aws_lambda_events::sqs::SqsEvent;
use deltalake::DeltaTableError;
use dynamodb_lock::Region;
use lambda_runtime::{service_fn, Error, LambdaEvent};
use serde_json::Value;
use tracing::log::*;
use url::Url;

use oxbow_lambda_shared::*;

use std::collections::HashMap;

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
    let records = s3_from_sqs(event.payload)?;
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
        let files = by_table
            .get(table_name)
            .expect("Failed to get the files for a table, impossible!");
        let mut storage_options: HashMap<String, String> = HashMap::default();
        // Ensure that the DeltaTable we get back uses the table-name as a partition key
        // when locking in DynamoDb: <https://github.com/buoyant-data/oxbow/issues/9>
        //
        // Without this setting each Lambda invocation will use the same default key `delta-rs`
        // when locking in DynamoDb.
        storage_options.insert(
            "DYNAMO_LOCK_PARTITION_KEY_VALUE".into(),
            format!("{table_name}:delta"),
        );
        let lock_options = dynamodb_lock::DynamoDbOptions {
            lease_duration: 60,
            partition_key_value: table_name.into(),
            ..Default::default()
        };
        let lock_client = dynamodb_lock::DynamoDbLockClient::for_region(Region::default())
            .with_options(lock_options);
        let lock = acquire_lock(table_name, &lock_client).await;

        match deltalake::open_table_with_storage_options(&table_name, storage_options.clone()).await
        {
            Ok(mut table) => {
                info!("Opened table to append: {:?}", table);

                match oxbow::append_to_table(files, &mut table).await {
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
                        let _ = release_lock(lock, &lock_client).await;
                        return Err(Box::new(err));
                    }
                }
            }
            Err(DeltaTableError::NotATable(_e)) => {
                // create the table with our objects
                info!("Creating new Delta table at: {location}");
                let table = oxbow::convert(table_name, Some(storage_options)).await;
                info!("Created table at: {location}");

                if table.is_err() {
                    error!("Failed to create new Delta table: {:?}", table);
                    // Propogate that error up so the function fails
                    let _ = table?;
                }
            }
            Err(source) => {
                let _ = release_lock(lock, &lock_client).await;
                error!("Failed to open the Delta table for some reason: {source:?}");
                return Err(Box::new(source));
            }
        }

        let _ = release_lock(lock, &lock_client).await;
    }

    Ok("[]".into())
}

/**
 * Simple helper function to acquire a lock with DynamoDb
 *
 * This function will panic in the bizarre condition where the lock has expired upon acquisition
 */
async fn acquire_lock(
    key: &str,
    lock_client: &dynamodb_lock::DynamoDbLockClient,
) -> dynamodb_lock::LockItem {
    debug!("Attempting to retrieve a lock for {key:?}");
    let lock = lock_client
        .acquire_lock(Some(key))
        .await
        .expect("Failed to acquire a lock");
    debug!("Lock acquired");

    if lock.acquired_expired_lock {
        error!("Somehow oxbow acquired an already expired lock, failing");
        panic!("Failing in hopes of acquiring a fresh lock");
    }
    lock
}

/**
 * Helper function to release a given [dynamodb_lock::LockItem].
 *
 * Will return true if the lock was successfully released.
 */
async fn release_lock(
    lock: dynamodb_lock::LockItem,
    lock_client: &dynamodb_lock::DynamoDbLockClient,
) -> bool {
    if let Err(e) = lock_client.release_lock(&lock).await {
        error!("Failing to properly release the lock {:?}: {:?}", lock, e);
        return false;
    }
    true
}

#[cfg(test)]
mod tests {}
