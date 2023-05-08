/*
 * The lambda module contains the Lambda specific implementation of oxbox.
 *
 * This can be compiled with the `lambda` feature
 */

use aws_lambda_events::s3::{S3Event, S3EventRecord, S3Object};
use chrono::prelude::*;
use deltalake::{ObjectMeta, Path};
use lambda_runtime::{service_fn, Error, LambdaEvent};
use log::*;
use serde_json::json;
use serde_json::Value;
use url::Url;

use std::collections::HashMap;

pub async fn main() -> Result<(), anyhow::Error> {
    info!("Starting the Lambda runtime");
    let func = service_fn(func);
    lambda_runtime::run(func)
        .await
        .expect("Failed while running the lambda handler");
    Ok(())
}

async fn func<'a>(event: LambdaEvent<S3Event>) -> Result<Value, Error> {
    debug!("Receiving event: {:?}", event);

    /*
     * The response variable will be spat out at the end of the Lambda, since this Lambda is
     * intended to be executed by S3 Bucket Notifications this doesn't really serve any useful
     * purposes outside of manual testing
     */
    let mut response: HashMap<Url, Vec<String>> = HashMap::new();
    let by_table = objects_by_table(&event.payload.records);

    for table in by_table.keys() {
        let location = Url::parse(&table).expect("Failed to turn a table into a URL");
        let files = by_table
            .get(table)
            .expect("Failed to get the files for a table, impossible!");
        // messages is just for sending responses out of the lambda
        let mut messages = vec![];

        if let Ok(mut table) = deltalake::open_table(&table).await {
            let result = oxbow::append_to_table(&files, &mut table).await;

            if result.is_err() {
                let message = format!("Failed to append to the table {}: {:?}", location, result);
                error!("{}", &message);
                messages.push(message);
            } else {
                messages.push(format!("Successfully appended to table at {}", location));
            }
        } else {
            // create the table with our objects
            info!("Creating new Delta table at: {}", location);
            let store = oxbow::object_store_for(&location);
            let table = oxbow::create_table_with(&files, store.clone()).await;

            if table.is_err() {
                let message = format!("Failed to create new Delta table: {:?}", table);
                error!("{}", &message);
                messages.push(message);
            } else {
                messages.push(format!("Successfully created table at {}", location));
            }
        }

        response.insert(location, messages);
    }

    Ok(json!(response))
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
            let log_path = infer_log_path_from(record.s3.object.key.as_ref().unwrap());
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
        match component {
            Component::Normal(os_str) => {
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
            _ => {}
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
    let location = s3object.key.clone().unwrap_or("".to_string());
    let location = match prune_prefix {
        Some(prune) => Path::from(location.strip_prefix(prune).unwrap_or(&location)),
        None => Path::from(location),
    };
    ObjectMeta {
        size: s3object.size.unwrap_or(0) as usize,
        last_modified: Utc::now(),
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
        assert_eq!(expected, infer_log_path_from(&object));
    }

    /*
     * It is valid to have a bucket totally dedicated to the delta table such that there is no
     * prefix
     */
    #[test]
    fn infer_log_path_from_object_at_root() {
        let object = "some.parquet";
        let expected = "";
        assert_eq!(expected, infer_log_path_from(&object));
    }

    #[test]
    fn infer_log_path_from_hive_partitioned_object() {
        let object = "some/path/ds=2023-05-05/site=delta.io/beta.parquet";
        let expected = "some/path";
        assert_eq!(expected, infer_log_path_from(&object));
    }

    #[test]
    fn s3event_object_to_objectmeta() {
        let s3object = S3Object {
            key: Some("some/path/to/a/prefix/alpha.parquet".into()),
            size: Some(1024),
            url_decoded_key: None,
            version_id: None,
            e_tag: None,
            sequencer: None,
        };

        let expected = deltalake::ObjectMeta {
            location: deltalake::Path::from("some/path/to/a/prefix/alpha.parquet"),
            last_modified: Utc::now(),
            size: 1024,
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
            url_decoded_key: None,
            version_id: None,
            e_tag: None,
            sequencer: None,
        };

        let expected = deltalake::ObjectMeta {
            location: deltalake::Path::from("alpha.parquet"),
            last_modified: Utc::now(),
            size: 1024,
        };

        let result = into_object_meta(&s3object, Some("some/path/to/a/prefix"));
        assert_eq!(expected.location, result.location);
        assert_eq!(expected.size, result.size);
    }

    #[test]
    fn group_objects_to_tables() {
        let buf = std::fs::read_to_string("tests/data/s3-event-multiple.json")
            .expect("Failed to read file");
        let event: S3Event = serde_json::from_str(&buf).expect("Failed to parse");
        assert_eq!(3, event.records.len());

        let groupings = objects_by_table(&event.records);

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
}
