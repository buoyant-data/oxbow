/**
 * oxbow-lambda-shared contains common helper functions and utilities for all oxbow related lambdas
 */
use aws_lambda_events::s3::{S3Event, S3EventRecord, S3Object};
use aws_lambda_events::sns::SnsMessage;
use aws_lambda_events::sqs::SqsEvent;
use chrono::prelude::*;
use deltalake::{DeltaResult, ObjectMeta, Path};
use tracing::log::*;

use std::collections::HashMap;

/**
 * Return wholly new [`S3EventRecord`] objects with their the [`S3Object`] `url_decoded_key`
 * properly filled in
 *
 * For whatever reason `aws_lambda_events` does not properly handle this
 */
pub fn records_with_url_decoded_keys(records: &[S3EventRecord]) -> Vec<S3EventRecord> {
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

/// Struct to keep track of the table modifications needing to be made based on
/// [S3EventRecord] objects .
#[derive(Debug, Clone, Default)]
pub struct TableMods {
    pub adds: Vec<ObjectMeta>,
    pub removes: Vec<ObjectMeta>,
}

/**
 * Group the objects from the notification based on the delta tables they should be added to.
 *
 * There's a possibility that an S3 bucket notification will have objects mixed in which should be
 * destined for different delta tables. Rather than re-opening/loading the table for each object as
 * we iterate the records, we can group them based on the delta table and then create the
 * appropriate transactions
 */
pub fn objects_by_table(records: &[S3EventRecord]) -> HashMap<String, TableMods> {
    let mut mods = HashMap::new();

    for record in records.iter() {
        if let Some(bucket) = &record.s3.bucket.name {
            let log_path = infer_log_path_from(record.s3.object.url_decoded_key.as_ref().unwrap());
            let om = into_object_meta(&record.s3.object, Some(&log_path));

            let key = format!("s3://{}/{}", bucket, log_path);

            if !mods.contains_key(&key) {
                mods.insert(key.clone(), TableMods::default());
            }
            if let Some(objects) = mods.get_mut(&key) {
                if let Some(event_name) = &record.event_name {
                    if event_name.starts_with("ObjectCreated") {
                        objects.adds.push(om);
                    } else if event_name == "ObjectRemoved:Delete" {
                        objects.removes.push(om);
                    }
                }
            }
        }
    }

    mods
}

/**
 * Infer the log path from the given object path.
 *
 * The location of `_delta_log/` can technically be _anywhere_ but for convention's
 * sake oxbow will attempt to put the `_delta_log/` some place predictable to ensure that
 * `add` actions in the log can use relative file paths for newly added objects
 */
pub fn infer_log_path_from(path: &str) -> String {
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

/// A simple structure to make deserializing test events for identification easier
///
/// See <fhttps://github.com/buoyant-data/oxbow/issues/8>
#[derive(serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct TestEvent {
    event: String,
}

/// Convert the given [aws_lambda_events::sqs::SqsEvent] to a collection of
///  [aws_lambda_events::s3::S3EventRecord] entities. This is mostly useful for handling S3 Bucket
///  Notifications which have been passed into SQS
///
///  In the case where the [aws_lambda_events::sqs::SqsEvent] contains an `s3:TestEvent` which is
///  fired when S3 Bucket Notifications are first enabled, the event will be ignored to avoid
///  errorsin the processing pipeline
pub fn s3_from_sqs(event: SqsEvent) -> DeltaResult<Vec<S3EventRecord>> {
    let mut records = vec![];
    for record in event.records.iter() {
        /* each record is an SqsMessage */
        if let Some(body) = &record.body {
            match serde_json::from_str::<S3Event>(body) {
                Ok(s3event) => {
                    for s3record in s3event.records {
                        records.push(s3record.clone());
                    }
                }
                Err(err) => {
                    // if we cannot deserialize and the event is an s3::TestEvent, then we should
                    // just return empty records.
                    let test_event = serde_json::from_str::<TestEvent>(body);
                    // Early exit with the original error if we cannot parse the JSON at all
                    if test_event.is_err() {
                        return Err(err.into());
                    }

                    // Ignore the error on deserialization if the event ends up being an S3
                    // TestEvent which is fired when bucket notifications are originally configured
                    if "s3:TestEvent" != test_event.unwrap().event {
                        return Err(err.into());
                    }
                }
            };
        }
    }
    Ok(records)
}

/// Convert SQS messages which are from an SNS subscription into the records format most consumers
/// are expection.
///
/// This function basically invokes [s3_from_sqs] after unwrapping the SNS envelope on the
/// [SqsEvent]
pub fn s3_from_sns(event: SqsEvent) -> DeltaResult<Vec<S3EventRecord>> {
    let mut records = vec![];

    for record in event.records.iter() {
        if let Some(body) = &record.body {
            match serde_json::from_str::<SnsMessage>(body) {
                Ok(sns_event) => {
                    if let Ok(s3_event) = serde_json::from_str::<S3Event>(&sns_event.message) {
                        for r in s3_event.records.into_iter() {
                            records.push(r);
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to deserialize an SnsMessage: {e:?}");
                }
            }
        }
    }
    Ok(records)
}

/**
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
    use aws_lambda_events::sqs::SqsMessage;

    #[test]
    fn test_s3_from_sns() {
        let body = r#"
        {"Type" : "Notification",
        "MessageId" : "blah",
        "TopicArn": "arn:blah",
        "Subject" : "blah",
        "Message" : "{\"Records\":[{\"eventVersion\":\"2.1\",\"eventSource\":\"aws:s3\",\"awsRegion\":\"us-east-2\",\"eventTime\":\"2024-01-25T20:57:23.379Z\",\"eventName\":\"ObjectCreated:CompleteMultipartUpload\",\"userIdentity\":{\"principalId\":\"AWS:AROAU7FUYKEVYG4GF4IAV:s3-replication\"},\"requestParameters\":{\"sourceIPAddress\":\"10.0.153.194\"},\"responseElements\":{\"x-amz-request-id\":\"RYAX8R8CB6FF1MQN\",\"x-amz-id-2\":\"uLUt4C/TfjwvpObPlTnrWYjOIPH1YT1yJ8jZjqRyLIuTLOxGSkNgKc2Hd1/O7wTP2cd3u59lRtVYrU4ECizehRYw0NGNlL5b\"},\"s3\":{\"s3SchemaVersion\":\"1.0\",\"configurationId\":\"tf-s3-queue-20231207170751084400000001\",\"bucket\":{\"name\":\"scribd-data-warehouse-dev\",\"ownerIdentity\":{\"principalId\":\"A1FIHS1B0BWUTQ\"},\"arn\":\"arn:aws:s3:::scribd-data-warehouse-dev\"},\"object\":{\"key\":\"databases/airbyte/faker_users/ds%3D2024-01-25/1706216212007_0.parquet\",\"size\":143785,\"eTag\":\"67165dca52a1089d312e19c3ddf1e342-1\",\"versionId\":\"3v567TKlEQF5IBoeXrFBRiRX8vY.bY1m\",\"sequencer\":\"0065B2CB162FC1AC3B\"}}}]}",
        "Timestamp" : "2024-01-25T20:57:24.649Z",
        "Signature" : "",
        "SignatureVersion" : "",
        "SigningCertUrl" : "",
        "UnsubscribeUrl" : ""
        }
        "#;
        let message: SqsMessage = SqsMessage {
            body: Some(body.to_string()),
            ..Default::default()
        };
        let event: SqsEvent = SqsEvent {
            records: vec![message],
        };
        let res = s3_from_sns(event);
        assert!(res.is_ok());
        assert_eq!(res.unwrap().len(), 1);
    }

    #[test]
    fn infer_log_path_from_object() {
        let object = "some/path/to/a/prefix/alpha.parquet";
        let expected = "some/path/to/a/prefix";
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

    /**
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

    /**
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
            table_one.adds.len(),
            "Shoulid only be one object in table one"
        );

        let table_two = groupings
            .get("s3://example-bucket/some/prefix")
            .expect("Failed to get the second table");
        assert_eq!(
            2,
            table_two.adds.len(),
            "Shoulid only be two objects in table two"
        );
    }

    #[test]
    fn test_s3_from_sqs() {
        let buf = std::fs::read_to_string("../../tests/data/s3-event-multiple.json")
            .expect("Failed to read file");
        let message = SqsMessage {
            body: Some(buf),
            ..Default::default()
        };
        let event = SqsEvent {
            records: vec![message],
        };

        let events = s3_from_sqs(event).expect("Failed to get events");
        assert_eq!(4, events.len(), "Unexpected number of entries");
    }

    #[test]
    fn test_s3_from_sqs_with_delete() {
        let buf = r#"{"Records":[{"eventVersion":"2.1","eventSource":"aws:s3","awsRegion":"us-west-2","eventTime":"2023-12-18T00:22:24.292Z","eventName":"ObjectRemoved:Delete","userIdentity":{"principalId":"A16S3A764ZBGJN"},"requestParameters":{"sourceIPAddress":"76.218.225.124"},"responseElements":{"x-amz-request-id":"CWK6W9YANZBH6SK4","x-amz-id-2":"H7P6nIKhchv9soZ4pnX0GsAj3zqqdrShFddk4kX9UpSbC2C5FL9XNvNtSxtTD1Nt0ZtTnREeZIMqO1IsSpkebocjUTRJkumh"},"s3":{"s3SchemaVersion":"1.0","configurationId":"test-delete","bucket":{"name":"oxbow-simple","ownerIdentity":{"principalId":"A16S3A764ZBGJN"},"arn":"arn:aws:s3:::oxbow-simple"},"object":{"key":"gcs-export/ds%3D2023-12-12/testing_oxbow-partitioned2_ds%3D2023-12-12_000000000000.parquet","sequencer":"00657F90C047858AE9"}}}]}"#;
        let message = SqsMessage {
            body: Some(buf.into()),
            ..Default::default()
        };
        let event = SqsEvent {
            records: vec![message],
        };

        let events = s3_from_sqs(event).expect("Failed to get events");
        assert_eq!(1, events.len(), "Unexpected number of entries");

        let records = records_with_url_decoded_keys(&events);
        let tables = objects_by_table(records.as_slice());
        if let Some(mods) = tables.get("s3://oxbow-simple/gcs-export") {
            assert_eq!(
                mods.removes.len(),
                1,
                "Should have recorded a removes table modification"
            );
        } else {
            assert!(false, "Failed to find the right key on {tables:?}");
        }
    }

    #[test]
    fn test_s3_from_sqs_with_invalid() {
        let message = SqsMessage {
            body: Some("This ain't no valid JSON".into()),
            ..Default::default()
        };
        let event = SqsEvent {
            records: vec![message],
        };

        let response = s3_from_sqs(event);
        assert!(
            response.is_err(),
            "Should have returned an error trying to deserialize"
        );
    }

    #[test]
    fn test_s3_from_sqs_with_test_event() {
        let buf = std::fs::read_to_string("../../tests/data/s3-test-event.json")
            .expect("Failed to read file");
        let message = SqsMessage {
            body: Some(buf),
            ..Default::default()
        };
        let event = SqsEvent {
            records: vec![message],
        };

        let response = s3_from_sqs(event);
        assert!(
            response.is_ok(),
            "Should have treated a test event like a no-op: {response:?}"
        );
    }
}
