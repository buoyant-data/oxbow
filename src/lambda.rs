/*
 * The lambda module contains the Lambda specific implementation of oxbox.
 *
 * This can be compiled with the `lambda` feature
 */

use aws_lambda_events::s3::S3Event;
use lambda_runtime::{service_fn, Error, LambdaEvent};
use log::*;
use serde_json::json;
use serde_json::Value;

pub async fn main() -> Result<(), anyhow::Error> {
    info!("Starting the Lambda runtime");
    let func = service_fn(func);
    lambda_runtime::run(func)
        .await
        .expect("Failed while running the lambda handler");
    Ok(())
}

async fn func<'a>(event: LambdaEvent<S3Event>) -> Result<Value, Error> {
    info!("Receiving event: {:?}", event);
    Ok(json!({
        "message": "",
    }))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deser_event() {
        let buf =
            std::fs::read_to_string("tests/data/s3-put-event.json").expect("Failed to read file");
        let _event: S3Event = serde_json::from_str(&buf).expect("Failed to parse");
    }

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

    #[ignore]
    #[test]
    fn tease_apart_events() {
        use std::collections::HashMap;

        let buf = std::fs::read_to_string("tests/data/s3-event-multiple.json")
            .expect("Failed to read file");
        let event: S3Event = serde_json::from_str(&buf).expect("Failed to parse");
        assert_eq!(2, event.records.len());
        let mut files: HashMap<String, Vec<String>> = HashMap::new();

        for record in event.records.iter() {
            let bucket = record.s3.bucket.name.clone().unwrap();
            if let Some(file) = &record.s3.object.key {
                if !files.contains_key(&bucket) {
                    files.insert(bucket.clone(), vec![]);
                }
                if let Some(v) = files.get_mut(&bucket) {
                    v.push(file.into());
                }
            }
        }

        assert!(false);
    }
}
