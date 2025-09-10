//!
//! The trigger module contains the abstractions to make triggering these lambdas simpler
//!

use aws_lambda_events::s3::S3EventRecord;
use deltalake::DeltaResult;
use regex::Regex;
use std::collections::HashMap;
use std::sync::OnceLock;
use url::Url;

static TXN_LOG: OnceLock<Regex> = OnceLock::new();
static CDC: OnceLock<Regex> = OnceLock::new();
static DV: OnceLock<Regex> = OnceLock::new();

/// A [TableTrigger] is a struct which contains details about a [Delta](https://delta.io) table
/// which has had a file change inside of it, triggering the Lambda function in some way.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct TableTrigger {
    // [Url] which points to the **root** of the Delta table, i.e. the prefix above `_delta_log/`
    location: Url,
    changes: Vec<Change>,
}

impl TableTrigger {
    /// Return the number of changes in this trigger
    fn len(&self) -> usize {
        self.changes.len()
    }

    /// Create an empty [TableTrigger] for the given table location [Url]
    fn new(location: Url) -> Self {
        Self {
            location,
            changes: vec![],
        }
    }

    /// Return a reference to this [TableTrigger] changes
    pub fn changes(&self) -> &Vec<Change> {
        &self.changes
    }

    /// Return a reference to the [Url] of this [TableTrigger]
    pub fn location(&self) -> &Url {
        &self.location
    }

    /// Retrun the smallest transaction version in this trigger
    ///
    /// This can be helpful to understand what the lower end of the triggered version changes may
    /// be when they are grouped together
    pub fn smallest_version(&self) -> Option<i64> {
        if self.changes.is_empty() {
            return None;
        }
        self.changes
            .iter()
            .filter_map(|c| match c.what {
                ChangeType::TransactionLog { version } => Some(version),
                _ => None,
            })
            .min()
    }

    /// Return the largest transaction version in this trigger
    pub fn largest_version(&self) -> Option<i64> {
        if self.changes.is_empty() {
            return None;
        }
        self.changes
            .iter()
            .filter_map(|c| match c.what {
                ChangeType::TransactionLog { version } => Some(version),
                _ => None,
            })
            .max()
    }

    // Turn the `records` into a [Vec] of [TableTrigger] entries
    //
    // NOTE: This may panic if the [S3EventRecord] records don't have valid keys inside of them
    pub fn from_s3_records(records: &[S3EventRecord]) -> DeltaResult<Vec<Self>> {
        let mut triggers: HashMap<Url, TableTrigger> = HashMap::default();

        for record in records.iter() {
            let event_name = record
                .event_name
                .as_ref()
                .expect("No event name on the event, how!?");
            let key = match record.s3.object.url_decoded_key.as_ref() {
                Some(key) => key.clone(),
                None => record
                    .s3
                    .object
                    .key
                    .as_ref()
                    .expect("Invalid state!")
                    .clone(),
            };
            let (what, prefix) = ChangeType::from_key(key.as_ref());
            let how = Modification::from(event_name.as_str());

            let bucket = record
                .s3
                .bucket
                .name
                .as_ref()
                .expect("How does this event not have a bucket name associated?");
            let table = Url::parse(&format!("s3://{bucket}/{prefix}"))
                .expect("Unable to compute a table location");
            let change = Change {
                what,
                how,
                key,
                table: table.clone(),
            };

            if !triggers.contains_key(&table) {
                triggers.insert(table.clone(), TableTrigger::new(table));
            }

            if let Some(trigger) = triggers.get_mut(&change.table) {
                trigger.changes.push(change);
            }
        }
        Ok(triggers.into_values().collect())
    }
}

/// A [Change] represents the realized state of a change received by a Lambda triggered by S3
/// Bucket Notifications
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct Change {
    pub what: ChangeType,
    how: Modification,
    table: Url,
    key: String,
}

/// The [Modification] enum helps articulate the difference between a ObjectCreated and
/// ObjectDeleted type events
#[derive(Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd)]
pub enum Modification {
    /// Indicating a file creation
    Create,
    /// Indicating a file deletion
    Delete,
    #[default]
    Unknown,
}

impl From<&str> for Modification {
    fn from(name: &str) -> Self {
        if name.starts_with("ObjectCreated") {
            return Modification::Create;
        }
        if name.starts_with("ObjectRemoved") {
            return Modification::Delete;
        }
        Modification::default()
    }
}

/// The [ChangeType] enum helps distinguish the table modifications that are seen in trigger events
#[derive(Debug, Default, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum ChangeType {
    TransactionLog {
        version: i64,
    },
    ChangeDataFeed,
    DataFile,
    DeletionVector,
    #[default]
    Unknown,
}

impl ChangeType {
    fn from_key(path_thing: &str) -> (Self, String) {
        let txn_matcher = TXN_LOG.get_or_init(|| {
            Regex::new(r"(?P<root>.*)/_delta_log/(?P<v>\d{20})\.json$")
                .expect("Failed to compile transaction log matcher")
        });

        if let Some(captured) = txn_matcher.captures(path_thing) {
            let what = ChangeType::TransactionLog {
                version: captured["v"]
                    .to_string()
                    .parse()
                    .expect("Failed to get a proper int"),
            };
            return (what, captured["root"].to_string());
        }

        // This check before the .parquet check ensures that parquet files in change_data/
        // directory
        let cdc_matcher = CDC.get_or_init(|| {
            Regex::new(r"(?P<root>.*)/_change_data/(?P<cdf_file>.*)")
                .expect("Failed to compile CDF matcher")
        });
        if let Some(captured) = cdc_matcher.captures(path_thing) {
            return (Self::ChangeDataFeed, captured["root"].to_string());
        }

        if path_thing.ends_with(".parquet") {
            // Parse out the root properly
            return (Self::DataFile, String::new());
        }

        let dv_matcher = DV.get_or_init(|| {
            Regex::new(r"(?P<root>.*)/deletion_vector-(.*)\.bin")
                .expect("Failed to compile deletion vector matcher")
        });
        if let Some(captured) = dv_matcher.captures(path_thing) {
            return (Self::DeletionVector, captured["root"].to_string());
        }
        (Self::Unknown, String::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_lambda_events::s3::S3Event;

    #[test]
    fn test_changetype_detection() {
        for (txn, version) in [
            ("mytable/_delta_log/00000000000000000000.json", 0),
            ("/mytable/_delta_log/00000000000000000001.json", 1),
            ("foo/mytable/_delta_log/00000000000000000003.json", 3),
        ] {
            let (change, _) = ChangeType::from_key(txn);
            assert_eq!(ChangeType::TransactionLog { version }, change);
        }

        let (change, _) = ChangeType::from_key(
            "/mytable/_change_data/cdc-00000-924d9ac7-21a9-4121-b067-a0a6517aa8ed.c000.snappy.parquet",
        );
        assert_eq!(ChangeType::ChangeDataFeed, change);

        let (change, _) = ChangeType::from_key(
            "/mytable/part-00000-3935a07c-416b-4344-ad97-2a38342ee2fc.c000.snappy.parquet",
        );
        assert_eq!(ChangeType::DataFile, change);
        // Enable this when we care about whether the file is partitioned or not
        //assert_eq!(ChangeType::DataFile { partitioned: true }, "some/prefix/ds%3d2023-01-01/b.parquet".into());
        let (change, _) = ChangeType::from_key(
            "/mytable/deletion_vector-0c6cbaaf-5e04-4c9d-8959-1088814f58ef.bin",
        );
        assert_eq!(ChangeType::DeletionVector, change);
    }

    #[test]
    fn test_trigger_from_events() -> DeltaResult<()> {
        let buf = std::fs::read_to_string("../../tests/data/s3-event-tables.json")
            .expect("Failed to read file");
        let event: S3Event = serde_json::from_str(&buf).expect("Failed to parse");
        assert_eq!(3, event.records.len());

        let mut triggers = TableTrigger::from_s3_records(&event.records)
            .expect("Failed to process S3EventRecords");
        triggers.sort();
        assert_eq!(2, triggers.len());
        // The first table trigger should just have a data file change in it
        assert_eq!(1, triggers[0].len());
        // The second trigger has two changes colocated in it
        assert_eq!(2, triggers[1].len());
        Ok(())
    }

    #[test]
    fn test_identify_versions_changed() -> DeltaResult<()> {
        let buf = std::fs::read_to_string("../../tests/data/s3-event-tables.json")
            .expect("Failed to read file");
        let event: S3Event = serde_json::from_str(&buf).expect("Failed to parse");
        assert_eq!(3, event.records.len());

        let mut triggers = TableTrigger::from_s3_records(&event.records)
            .expect("Failed to process S3EventRecords");
        triggers.sort();

        // This batch has no transaction changes
        let first = &triggers[0];
        // This batch does
        let second = &triggers[1];

        assert_eq!(first.smallest_version(), None);
        assert_eq!(first.largest_version(), None);

        assert_eq!(second.smallest_version(), Some(1));
        assert_eq!(second.largest_version(), Some(1));

        Ok(())
    }

    #[test]
    fn test_modification() {
        for m in [
            "ObjectCreated:Put",
            "ObjectCreated:Copy",
            "ObjectCreated:CompleteMultipartUpload",
        ] {
            assert_eq!(Modification::Create, m.into());
        }
        for m in ["ObjectRemoved:Delete", "ObjectRemoved:DeleteMarkerCreated"] {
            assert_eq!(Modification::Delete, m.into());
        }

        assert_eq!(Modification::Unknown, "TestEvent".into());
    }
}
