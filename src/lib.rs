/*
 * The lib module contains the business logic of oxbow, regardless of the interface implementation
 */
use deltalake::action::{Action, Add, SaveMode};
use deltalake::parquet::arrow::async_reader::{
    ParquetObjectReader, ParquetRecordBatchStreamBuilder,
};
use deltalake::storage::DeltaObjectStore;
use deltalake::{ObjectMeta, ObjectStore};
use futures::StreamExt;
use log::*;
use url::Url;

use std::collections::HashMap;
use std::sync::Arc;

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
) -> deltalake::DeltaResult<()> {
    use deltalake::operations::create::CreateBuilder;
    use deltalake::schema::Schema;

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

    /*
     * Create and persist the table
     */
    let actions = add_actions_for(&files);
    let table = CreateBuilder::new()
        .with_object_store(store.clone())
        .with_columns(schema.get_fields().clone())
        .with_actions(actions)
        .with_save_mode(SaveMode::Ignore)
        .await
        .expect("Failed to create CreateBuilder");

    Ok(())
}

/*
 * Provide a series of Add actions for the given ObjectMeta entries
 *
 * This is a critical translation layer between discovered parquet files and how those would be
 * represented inside of the log
 */
fn add_actions_for(files: &Vec<ObjectMeta>) -> Vec<Action> {
    files
        .iter()
        .map(|om| Add {
            path: om.location.to_string(),
            size: om.size as i64,
            modification_time: om.last_modified.timestamp_millis(),
            data_change: true,
            stats: None,
            stats_parsed: None,
            tags: None,
            partition_values: HashMap::default(),
            partition_values_parsed: None,
        })
        .map(|a| Action::add(a))
        .collect()
}

/*
 * Return the smallest file from the given set of files.
 *
 * This can be useful to find the smallest possible parquet file to load from the set in order to
 * discern schema information
 */
fn find_smallest_file(files: &Vec<ObjectMeta>) -> Option<&ObjectMeta> {
    if files.len() == 0 {
        return None;
    }

    let mut smallest = &files[0];
    for file in files.iter() {
        if file.size < smallest.size {
            smallest = &file;
        }
    }

    Some(smallest)
}

#[cfg(test)]
mod tests {
    use super::*;

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
            },
            ObjectMeta {
                location: Path::from("foo/small"),
                last_modified: Utc::now(),
                size: 1024,
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
        }];
        let result = add_actions_for(&files);
        assert_eq!(1, result.len());
    }
}
