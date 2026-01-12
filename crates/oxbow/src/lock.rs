use deltalake::DeltaTableBuilder;
///
/// THe lock module contains some simple helpers for handling table locks in DynamoDB. This is
/// something required of deltalake 0.16.x and earlier.
///
use dynamodb_lock::Region;
use tracing::log::*;
use url::Url;

use std::collections::HashMap;

///
///Wrapper aroudn [deltalake::open_table] which will open the table with the appropriate storage
///options needed for locking
pub async fn open_table(table_uri: &str) -> deltalake::DeltaResult<deltalake::DeltaTable> {
    let table_url = Url::parse(table_uri).expect("Fatal error trying to parse a table URL");
    DeltaTableBuilder::from_url(table_url)?
        .without_files()
        .with_storage_options(storage_options(table_uri))
        .load()
        .await
}

/// Default storage options for using with `deltalake` calls
pub fn storage_options(table_uri: &str) -> HashMap<String, String> {
    let mut storage_options: HashMap<String, String> = HashMap::default();
    // Ensure that the DeltaTable we get back uses the table-name as a partition key
    // when locking in DynamoDb: <https://github.com/buoyant-data/oxbow/issues/9>
    //
    // Without this setting each Lambda invocation will use the same default key `delta-rs`
    // when locking in DynamoDb.
    storage_options.insert(
        "DYNAMO_LOCK_PARTITION_KEY_VALUE".into(),
        format!("{table_uri}:delta"),
    );
    storage_options
}

///
/// Return a properly configured lock for the given `table_name`
pub fn client_for(table_name: &str) -> dynamodb_lock::DynamoDbLockClient {
    let lock_options = dynamodb_lock::DynamoDbOptions {
        lease_duration: 60,
        partition_key_value: table_name.into(),
        ..Default::default()
    };
    dynamodb_lock::DynamoDbLockClient::for_region(Region::default()).with_options(lock_options)
}

/**
 * Simple helper function to acquire a lock with DynamoDb
 *
 * This function will panic in the bizarre condition where the lock has expired upon acquisition
 */
pub async fn acquire(
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
pub async fn release(
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
mod tests {
    use super::*;

    use deltalake::DeltaResult;

    #[tokio::test]
    async fn test_open_table() -> DeltaResult<()> {
        let _table = open_table("memory://")
            .await
            .expect_err("Can't possibly load a table that doesn't exist!");
        Ok(())
    }
}
