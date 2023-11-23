/*
 * The lambda module contains the Lambda specific implementation of oxbox.
 *
 * This can be compiled with the `lambda` feature
 */

use aws_lambda_events::s3::{S3Event, S3EventRecord, S3Object};
use aws_lambda_events::sqs::SqsEvent;
use chrono::prelude::*;
use deltalake::{DeltaTableError, ObjectMeta, Path};
use dynamodb_lock::Region;
use lambda_runtime::{service_fn, Error, LambdaEvent};
use serde_json::Value;
use tracing::log::*;
use url::Url;

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
    let mut records = vec![];
    for record in event.payload.records.iter() {
        /* each record is an SqsMessage */
        if let Some(body) = &record.body {
            let _: S3Event = serde_json::from_str(body).expect("Failed to deserialize!");
            if let Ok(s3event) = serde_json::from_str::<S3Event>(body) {
                for s3record in s3event.records {
                    records.push(s3record.clone());
                }
            }
        }
    }

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
        .try_acquire_lock(Some(key))
        .await
        .expect("Failed to acquire a lock")
        .expect("Failed to acquire a lock, failing function");
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

/*
 * Group the objects from the notification based on the delta tables they should be added to.
 *
 * There's a possibility that an S3 bucket notification will have objects mixed in which should be
 * destined for different delta tables. Rather than re-opening/loading the table for each object as
 * we iterate the records, we can group them based on the delta table and then create the
 * appropriate transactions
 */
fn objects_by_table(records: &[S3EventRecord]) -> HashMap<String, Vec<ObjectMeta>> {
    let mut result = HashMap::new();

    for record in records.iter() {
        if let Some(bucket) = &record.s3.bucket.name {
            let log_path = infer_log_path_from(record.s3.object.url_decoded_key.as_ref().unwrap());
            let om = into_object_meta(&record.s3.object, Some(&log_path));

            let key = format!("s3://{}/{}", bucket, log_path);

            if !result.contains_key(&key) {
                result.insert(key.clone(), vec![]);
            }
            if let Some(objects) = result.get_mut(&key) {
                objects.push(om);
            }
        }
    }

    result
}

/*
 * Return wholly new [`S3EventRecord`] objects with their the [`S3Object`] `url_decoded_key`
 * properly filled in
 *
 * For whatever reason `aws_lambda_events` does not properly handle this
 */
fn records_with_url_decoded_keys(records: &[S3EventRecord]) -> Vec<S3EventRecord> {
    use urlencoding::decode;

    records
        .iter()
        .filter(|record| match &record.s3.object.key {
            None => true,
            Some(key) => !key.contains("_delta_log"),
        })
        .map(|record| {
            let mut replacement = record.clone();
            if let Some(key) = &replacement.s3.object.key {
                if let Ok(decoded_key) = decode(key) {
                    replacement.s3.object.url_decoded_key = Some(decoded_key.into_owned());
                }
            }
            replacement
        })
        .collect()
}

/*
 * Infer the log path from the given object path.
 *
 * The location of `_delta_log/` can technically be _anywhere_ but for convention's
 * sake oxbow will attempt to put the `_delta_log/` some place predictable to ensure that
 * `add` actions in the log can use relative file paths for newly added objects
 */
fn infer_log_path_from(path: &str) -> String {
    use std::path::{Component, Path};

    let mut root = vec![];

    for component in Path::new(path)
        .parent()
        .expect("Failed to get parent() of path")
        .components()
    {
        if let Component::Normal(os_str) = component {
            if let Some(segment) = os_str.to_str() {
                /*
                 * If a segment has what looks like a hive-style partition, bail and call that the root of
                 * the delta table
                 */
                if segment.find('=') >= Some(0) {
                    break;
                }
                root.push(segment);
            }
        }
    }
    root.join("/")
}

/*
 * Convert an [`S3Object`] into an [`ObjectMeta`] for use in the creation of Delta transactions
 *
 * This is a _lossy_ conversion since the two structs do not share the same set of information,
 * therefore this conversion is really only taking the path of the object and the size
 */
fn into_object_meta(s3object: &S3Object, prune_prefix: Option<&str>) -> ObjectMeta {
    let location = s3object.url_decoded_key.clone().unwrap_or("".to_string());

    let location = match prune_prefix {
        Some(prune) => Path::from(location.strip_prefix(prune).unwrap_or(&location)),
        None => Path::from(location),
    };
    ObjectMeta {
        size: s3object.size.unwrap_or(0) as usize,
        last_modified: Utc::now(),
        e_tag: None,
        location,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn infer_log_path_from_object() {
        let object = "some/path/to/a/prefix/alpha.parquet";
        let expected = "some/path/to/a/prefix";
        assert_eq!(expected, infer_log_path_from(object));
    }

    /*
     * It is valid to have a bucket totally dedicated to the delta table such that there is no
     * prefix
     */
    #[test]
    fn infer_log_path_from_object_at_root() {
        let object = "some.parquet";
        let expected = "";
        assert_eq!(expected, infer_log_path_from(object));
    }

    #[test]
    fn infer_log_path_from_hive_partitioned_object() {
        let object = "some/path/ds=2023-05-05/site=delta.io/beta.parquet";
        let expected = "some/path";
        assert_eq!(expected, infer_log_path_from(object));
    }

    #[test]
    fn s3event_object_to_objectmeta() {
        let s3object = S3Object {
            key: Some("some/path/to/a/prefix/alpha.parquet".into()),
            size: Some(1024),
            url_decoded_key: Some("some/path/to/a/prefix/alpha.parquet".into()),
            version_id: None,
            e_tag: None,
            sequencer: None,
        };

        let expected = deltalake::ObjectMeta {
            location: deltalake::Path::from("some/path/to/a/prefix/alpha.parquet"),
            last_modified: Utc::now(),
            size: 1024,
            e_tag: None,
        };

        let result = into_object_meta(&s3object, None);
        assert_eq!(expected.location, result.location);
        assert_eq!(expected.size, result.size);
    }

    #[test]
    fn into_object_meta_with_prefix() {
        let s3object = S3Object {
            key: Some("some/path/to/a/prefix/alpha.parquet".into()),
            size: Some(1024),
            url_decoded_key: Some("some/path/to/a/prefix/alpha.parquet".into()),
            version_id: None,
            e_tag: None,
            sequencer: None,
        };

        let expected = deltalake::ObjectMeta {
            location: deltalake::Path::from("alpha.parquet"),
            last_modified: Utc::now(),
            e_tag: None,
            size: 1024,
        };

        let result = into_object_meta(&s3object, Some("some/path/to/a/prefix"));
        assert_eq!(expected.location, result.location);
        assert_eq!(expected.size, result.size);
    }

    /*
     * The keys coming off of the S3Object will be url encodeed, and for hive style partitioning
     * that needs to be undone.
     *
     * In theory S3Object does have `url_decoded_key` but in production testing this was always
     * None.
     */
    #[test]
    fn into_object_meta_urlencoded() {
        let key = "databases/deltatbl-partitioned/c2%3Dfoo0/part-00000-2bcc9ff6-0551-4401-bd22-d361a60627e3.c000.snappy.parquet";
        let s3object = S3Object {
            key: Some(key.into()),
            size: Some(1024),
            url_decoded_key: Some("databases/deltatbl-partitioned/c2=foo0/part-00000-2bcc9ff6-0551-4401-bd22-d361a60627e3.c000.snappy.parquet".into()),
            version_id: None,
            e_tag: None,
            sequencer: None,
        };

        let expected = deltalake::ObjectMeta {
            location: deltalake::Path::from(
                "c2=foo0/part-00000-2bcc9ff6-0551-4401-bd22-d361a60627e3.c000.snappy.parquet",
            ),
            last_modified: Utc::now(),
            size: 1024,
            e_tag: None,
        };

        let result = into_object_meta(&s3object, Some("databases/deltatbl-partitioned"));
        assert_eq!(expected.location, result.location);
        assert_eq!(expected.size, result.size);
    }

    #[test]
    fn group_objects_to_tables() {
        let buf = std::fs::read_to_string("../../tests/data/s3-event-multiple.json")
            .expect("Failed to read file");
        let event: S3Event = serde_json::from_str(&buf).expect("Failed to parse");
        assert_eq!(4, event.records.len());

        let groupings = objects_by_table(&records_with_url_decoded_keys(&event.records));

        assert_eq!(2, groupings.keys().len());

        let table_one = groupings
            .get("s3://example-bucket/some/first-prefix")
            .expect("Failed to get the first table");
        assert_eq!(
            1,
            table_one.len(),
            "Shoulid only be one object in table one"
        );

        let table_two = groupings
            .get("s3://example-bucket/some/prefix")
            .expect("Failed to get the second table");
        assert_eq!(
            2,
            table_two.len(),
            "Shoulid only be two objects in table two"
        );
    }

    #[test]
    fn test_records_with_url_decoded_keys() {
        let buf = std::fs::read_to_string("../../tests/data/s3-event-multiple-urlencoded.json")
            .expect("Failed to read file");
        let event: S3Event = serde_json::from_str(&buf).expect("Failed to parse");
        assert_eq!(3, event.records.len());

        let records = records_with_url_decoded_keys(&event.records);
        assert_eq!(event.records.len(), records.len());
    }

    #[test]
    fn test_records_with_url_decoded_keys_checkpoint_parquets() {
        let buf = std::fs::read_to_string("../../tests/data/s3-event-multiple.json")
            .expect("Failed to read file");
        let event: S3Event = serde_json::from_str(&buf).expect("Failed to parse");
        assert_eq!(4, event.records.len());

        let records = records_with_url_decoded_keys(&event.records);
        // Thec checkpoint file should be removewd
        assert_eq!(3, records.len());
    }
}
