/*
 * The lambda module contains the Lambda specific implementation of oxbox.
 *
 * This can be compiled with the `lambda` feature
 */

use aws_lambda_events::s3::S3Event;
use log::*;
use lambda_runtime::{service_fn, Error, LambdaEvent};
use serde_json::json;
use serde_json::Value;

pub async fn main() -> Result<(), anyhow::Error> {
    info!("Starting the Lambda runtime");
    let func = service_fn(func);
    lambda_runtime::run(func).await.expect("Failed while running the lambda handler");
    Ok(())
}

async fn func<'a>(event: LambdaEvent<S3Event>) -> Result<Value, Error> {
    info!("Receiving event: {:?}", event);
    Ok(json!({
        "message": "",
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deser_event() {
        let buf = std::fs::read_to_string("tests/data/s3-put-event.json").expect("Failed to read file");
        let _event: S3Event = serde_json::from_str(&buf).expect("Failed to parse");
    }

    #[ignore]
    #[test]
    fn tease_apart_events() {
        use std::collections::HashMap;

        let buf = std::fs::read_to_string("tests/data/s3-event-multiple.json").expect("Failed to read file");
        let event: S3Event = serde_json::from_str(&buf).expect("Failed to parse");
        assert_eq!(2, event.records.len());
        let mut files: HashMap<String, Vec<String>> = HashMap::new();

        for record in event.records.iter() {
            let bucket = record.s3.bucket.name.clone().unwrap();
            if let Some(file) = &record.s3.object.key {
                if ! files.contains_key(&bucket) {
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
