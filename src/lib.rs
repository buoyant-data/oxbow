/*
 * The lib module contains the business logic of oxbow, regardless of the interface implementation
 */
use deltalake::action::*;
use deltalake::parquet::arrow::async_reader::{
    ParquetObjectReader, ParquetRecordBatchStreamBuilder,
};
use deltalake::partitions::DeltaTablePartition;
use deltalake::storage::DeltaObjectStore;
use deltalake::{DeltaResult, DeltaTable, ObjectMeta, ObjectStore, SchemaDataType, SchemaField};
use futures::StreamExt;
use tracing::log::*;
use url::Url;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/*
 * convert is the main function to be called by the CLI or other "one shot" executors which just
 * need to take a given location and convert it all at once
 */
pub async fn convert(location: &str) -> DeltaResult<DeltaTable> {
    match deltalake::open_table(&location).await {
        Err(e) => {
            debug!("No Delta table at {}: {:?}", location, e);
            /*
             * Parse the given location as a URL in a way that can be passed into
             * some delta APIs
             */
            let location = match Url::parse(location) {
                Ok(parsed) => parsed,
                Err(_) => {
                    let absolute = std::fs::canonicalize(location)
                        .expect("Failed to canonicalize table location");
                    Url::from_file_path(absolute)
                        .expect("Failed to parse the location as a file path")
                }
            };
            let store = object_store_for(&location);
            let files = discover_parquet_files(store.clone()).await?;
            debug!(
                "Files identified for turning into a delta table: {:?}",
                files
            );
            create_table_with(&files, store.clone()).await
        }
        Ok(table) => {
            warn!("There is already a Delta table at: {}", table);
            Ok(table)
        }
    }
}

/*
 * Create the ObjectStore for the given location
 */
pub fn object_store_for(location: &Url) -> Arc<DeltaObjectStore> {
    let options = HashMap::new();
    Arc::new(DeltaObjectStore::try_new(location.clone(), options).expect("Failed to make store"))
}

/*
 * Discover `.parquet` files which are present in the location
 */
pub async fn discover_parquet_files(
    store: Arc<DeltaObjectStore>,
) -> deltalake::DeltaResult<Vec<ObjectMeta>> {
    let mut result = vec![];
    let mut iter = store.list(None).await?;

    /*
     * NOTE: There is certainly some way to make this more compact
     */
    while let Some(path) = iter.next().await {
        // Result<ObjectMeta> has been yielded
        if let Ok(meta) = path {
            debug!("Discovered file: {:?}", meta);

            if let Some(ext) = meta.location.extension() {
                match ext {
                    "parquet" => {
                        result.push(meta);
                    }
                    &_ => {}
                }
            }
        }
    }
    Ok(result)
}

/*
 * Create a Delta table with the given series of files at the specified location
 */
pub async fn create_table_with(
    files: &Vec<ObjectMeta>,
    store: Arc<DeltaObjectStore>,
) -> DeltaResult<DeltaTable> {
    use deltalake::operations::create::CreateBuilder;
    use deltalake::schema::Schema;

    let partitions = partition_columns_from(files);
    let smallest = find_smallest_file(files)
        .expect("Failed to find the smallest parquet file, which is fatal");
    debug!(
        "Using the smallest parquet file for schema inference: {:?}",
        smallest.location
    );
    let file_reader = ParquetObjectReader::new(store.clone(), smallest.clone());
    let arrow_schema = ParquetRecordBatchStreamBuilder::new(file_reader)
        .await?
        .build()?
        .schema()
        .clone();
    debug!("Read schema from Parquet file: {:?}", arrow_schema);

    let schema: deltalake::schema::Schema = Schema::try_from(arrow_schema)
        .expect("Failed to convert the schema for creating the table");

    let mut columns = schema.get_fields().clone();
    for partition in &partitions {
        let field = SchemaField::new(
            partition.into(),
            SchemaDataType::primitive("string".into()),
            true,
            HashMap::new(),
        );
        columns.push(field);
    }

    /*
     * Create and persist the table
     */
    let actions = add_actions_for(files);
    CreateBuilder::new()
        .with_object_store(store.clone())
        .with_columns(columns)
        .with_partition_columns(partitions)
        .with_actions(actions)
        .with_save_mode(SaveMode::Ignore)
        .await
}

/*
 * Append the given files to an already existing and initialized Delta Table
 */
pub async fn append_to_table(files: &[ObjectMeta], table: &mut DeltaTable) -> DeltaResult<i64> {
    let actions = add_actions_for(files);

    deltalake::operations::transaction::commit(
        table.object_store().as_ref(),
        &actions,
        DeltaOperation::Write {
            mode: SaveMode::Append,
            partition_by: Some(partition_columns_from(files)),
            predicate: None,
        },
        table.get_state(),
        None,
    )
    .await
}

/*
 * Take an iterator of files and determine what looks like a partition column from it
 */
fn partition_columns_from(files: &[ObjectMeta]) -> Vec<String> {
    // The HashSet is only to prevent collecting redundant partitions
    let mut results = HashSet::new();

    /*
     * The nested iteration here is required to evaluate nested partitions
     */
    for file in files.iter() {
        let _ = file
            .location
            .as_ref()
            .split('/')
            .flat_map(DeltaTablePartition::try_from)
            .map(|p| results.insert(p.key.to_string()))
            .collect::<Vec<_>>();
    }

    results.into_iter().collect()
}

/*
 * Return all the partitions from the given path buf
 */
fn partitions_from(path_str: &str) -> Vec<DeltaTablePartition> {
    path_str
        .split('/')
        .flat_map(DeltaTablePartition::try_from)
        .collect()
}

/*
 * Provide a series of Add actions for the given ObjectMeta entries
 *
 * This is a critical translation layer between discovered parquet files and how those would be
 * represented inside of the log
 */
pub fn add_actions_for(files: &[ObjectMeta]) -> Vec<Action> {
    files
        .iter()
        .map(|om| Add {
            path: om.location.to_string(),
            size: om.size as i64,
            modification_time: om.last_modified.timestamp_millis(),
            data_change: true,
            deletion_vector: None,
            stats: None,
            stats_parsed: None,
            tags: None,
            partition_values: partitions_from(om.location.as_ref())
                .iter()
                .map(|p| (p.key.into(), Some(p.value.into())))
                .collect(),
            partition_values_parsed: None,
        })
        .map(Action::add)
        .collect()
}

/*
 * Return the smallest file from the given set of files.
 *
 * This can be useful to find the smallest possible parquet file to load from the set in order to
 * discern schema information
 */
fn find_smallest_file(files: &Vec<ObjectMeta>) -> Option<&ObjectMeta> {
    if files.is_empty() {
        return None;
    }

    let mut smallest = &files[0];
    for file in files.iter() {
        if file.size < smallest.size {
            smallest = file;
        }
    }

    Some(smallest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::hash::Hash;

    use chrono::prelude::Utc;
    use deltalake::Path;

    #[tokio::test]
    async fn discover_parquet_files_empty_dir() {
        let dir = tempfile::tempdir().expect("Failed to create a temporary directory");
        let url = Url::from_file_path(dir.path()).expect("Failed to parse local path");
        let store = object_store_for(&url);

        let files = discover_parquet_files(store.clone())
            .await
            .expect("Failed to discover parquet files");
        assert_eq!(files.len(), 0);
    }

    #[tokio::test]
    async fn discover_parquet_files_full_dir() {
        let path = std::fs::canonicalize("./tests/data/hive/deltatbl-non-partitioned")
            .expect("Failed to canonicalize");
        let url = Url::from_file_path(path).expect("Failed to parse local path");
        let store = object_store_for(&url);

        let files = discover_parquet_files(store.clone())
            .await
            .expect("Failed to discover parquet files");

        assert_eq!(files.len(), 2);
    }

    #[test]
    fn find_smallest_file_empty_set() {
        let files = vec![];
        assert_eq!(None, find_smallest_file(&files));
    }

    #[test]
    fn find_smallest_file_set() {
        let files = vec![
            ObjectMeta {
                location: Path::from("foo/large"),
                last_modified: Utc::now(),
                size: 4096,
                e_tag: None,
            },
            ObjectMeta {
                location: Path::from("foo/small"),
                last_modified: Utc::now(),
                size: 1024,
                e_tag: None,
            },
        ];

        let result = find_smallest_file(&files);
        assert!(result.is_some());

        if let Some(result) = result {
            let expected_path = Path::from("foo/small");
            assert_eq!(result.location, expected_path);
        }
    }

    #[test]
    fn add_actions_for_empty() {
        let result = add_actions_for(&vec![]);
        assert_eq!(0, result.len());
    }

    #[test]
    fn add_actions_for_not_empty() {
        let files = vec![ObjectMeta {
            location: Path::from(
                "part-00001-f2126b8d-1594-451b-9c89-c4c2481bfd93-c000.snappy.parquet",
            ),
            last_modified: Utc::now(),
            size: 689,
            e_tag: None,
        }];
        let result = add_actions_for(&files);
        assert_eq!(1, result.len());
    }

    #[test]
    fn partition_columns_from_empty() {
        let expected: Vec<String> = vec![];
        assert_eq!(expected, partition_columns_from(&vec![]));
    }

    #[test]
    fn partition_columns_from_non_partitioned() {
        let expected: Vec<String> = vec![];
        let files = vec![
            ObjectMeta {
                location: Path::from("foo/large"),
                last_modified: Utc::now(),
                size: 4096,
                e_tag: None,
            },
            ObjectMeta {
                location: Path::from("foo/small"),
                last_modified: Utc::now(),
                size: 1024,
                e_tag: None,
            },
        ];
        assert_eq!(expected, partition_columns_from(&files));
    }

    #[test]
    fn partition_columns_from_single_partition() {
        let expected: Vec<String> = vec!["c2".into()];

        let files = vec![
            ObjectMeta {
                location: Path::from("c2=foo1/large"),
                last_modified: Utc::now(),
                size: 4096,
                e_tag: None,
            },
            ObjectMeta {
                location: Path::from("c2=foo2/small"),
                last_modified: Utc::now(),
                e_tag: None,
                size: 1024,
            },
        ];
        assert_eq!(expected, partition_columns_from(&files));
    }

    fn assert_unordered_eq<T>(left: &[T], right: &[T])
    where
        T: Eq + Hash + std::fmt::Debug,
    {
        let left: HashSet<_> = left.iter().collect();
        let right: HashSet<_> = right.iter().collect();

        assert_eq!(left, right);
    }

    #[test]
    fn partition_columns_from_multiple_partition() {
        let expected: Vec<String> = vec!["c2".into(), "c3".into()];

        let files = vec![
            ObjectMeta {
                location: Path::from("c2=foo1/c3=bar1/large"),
                last_modified: Utc::now(),
                e_tag: None,
                size: 4096,
            },
            ObjectMeta {
                location: Path::from("c2=foo2/c3=bar2/small"),
                last_modified: Utc::now(),
                e_tag: None,
                size: 1024,
            },
        ];
        assert_unordered_eq(&expected, &partition_columns_from(&files));
    }

    /*
     * See <https://github.com/buoyant-data/oxbow/issues/2>
     */
    #[tokio::test]
    async fn create_schema_for_partitioned_path() {
        use fs_extra::{copy_items, dir::CopyOptions, remove_items};

        let path = std::fs::canonicalize("./tests/data/hive/deltatbl-partitioned")
            .expect("Failed to canonicalize");
        let dir = tempfile::tempdir().expect("Failed to create a temporary directory");

        let options = CopyOptions::new();
        copy_items(&vec![path.as_path()], dir.path(), &options).expect("Failed to copy items over");
        // Remove the tempdir's copied _delta_log/ since the test must recreate it
        remove_items(&vec![dir.path().join("_delta_log")])
            .expect("Failed to remove temp _delta_log/");

        let url = Url::from_file_path(dir.path()).expect("Failed to parse local path");
        let store = object_store_for(&url);

        let files = discover_parquet_files(store.clone())
            .await
            .expect("Failed to discover parquet files");
        assert_eq!(files.len(), 11);

        let parts = partition_columns_from(&files);
        assert_eq!(
            parts.len(),
            1,
            "Expected to find one partition in the files"
        );

        let table = create_table_with(&files, store.clone())
            .await
            .expect("Failed to create table");
        let schema = table.get_schema().expect("Failed to get schema");
        assert!(
            schema.get_field_with_name("c2").is_ok(),
            "The schema does not include the expected partition key `c2`"
        );
    }
}
