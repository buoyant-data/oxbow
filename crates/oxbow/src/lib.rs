/*
 * The lib module contains the business logic of oxbow, regardless of the interface implementation
 */
use deltalake::arrow::datatypes::Schema as ArrowSchema;
use deltalake::parquet::arrow::async_reader::{
    ParquetObjectReader, ParquetRecordBatchStreamBuilder,
};
use deltalake::partitions::DeltaTablePartition;
use deltalake::protocol::*;
use deltalake::storage::DeltaObjectStore;
use deltalake::{DeltaResult, DeltaTable, ObjectMeta, ObjectStore, SchemaDataType, SchemaField};
use futures::StreamExt;
use tracing::log::*;
use url::Url;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/**
 * convert is the main function to be called by the CLI or other "one shot" executors which just
 * need to take a given location and convert it all at once
 */
pub async fn convert(
    location: &str,
    storage_options: Option<HashMap<String, String>>,
) -> DeltaResult<DeltaTable> {
    let table_result = match storage_options {
        Some(ref so) => deltalake::open_table_with_storage_options(&location, so.clone()).await,
        None => deltalake::open_table(&location).await,
    };

    match table_result {
        Err(e) => {
            info!("No Delta table at {}: {:?}", location, e);
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
            let store = object_store_for(&location, storage_options);
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

/**
 * Create the ObjectStore for the given location
 */
pub fn object_store_for(
    location: &Url,
    storage_options: Option<HashMap<String, String>>,
) -> Arc<DeltaObjectStore> {
    let options = match storage_options {
        None => HashMap::default(),
        Some(s) => s,
    };
    Arc::new(DeltaObjectStore::try_new(location.clone(), options).expect("Failed to make store"))
}

/**
 * Discover `.parquet` files which are present in the location
 */
pub async fn discover_parquet_files(
    store: Arc<DeltaObjectStore>,
) -> deltalake::DeltaResult<Vec<ObjectMeta>> {
    info!("Discovering parquet files for {store:?}");
    let mut result = vec![];
    let mut iter = store.list(None).await?;

    /*
     * NOTE: There is certainly some way to make this more compact
     */
    while let Some(path) = iter.next().await {
        // Result<ObjectMeta> has been yielded
        if let Ok(meta) = path {
            if let Some(ext) = meta.location.extension() {
                match ext {
                    "parquet" => {
                        if let Some(filename) = meta.location.filename() {
                            if !filename.ends_with(".checkpoint.parquet") {
                                debug!("Discovered file: {:?}", meta);
                                result.push(meta);
                            }
                        }
                    }
                    &_ => {}
                }
            }
        }
    }
    Ok(result)
}

/**
 * Create a Delta table with the given series of files at the specified location
 */
pub async fn create_table_with(
    files: &Vec<ObjectMeta>,
    store: Arc<DeltaObjectStore>,
) -> DeltaResult<DeltaTable> {
    use deltalake::operations::create::CreateBuilder;
    use deltalake::schema::Schema;

    let partitions = partition_columns_from(files);
    let smallest = find_smallest_file(files);
    if smallest.is_none() {
        let msg = "Failed to find the smallest parquet file, which is fatal";
        error!("{}", &msg);
        return Err(deltalake::DeltaTableError::Generic(msg.into()));
    }
    let smallest = smallest.unwrap();
    info!(
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

    let mut conversions: Vec<Arc<deltalake::arrow::datatypes::Field>> = vec![];

    for field in arrow_schema.fields().iter() {
        match field.data_type() {
            deltalake::arrow::datatypes::DataType::Timestamp(unit, tz) => match unit {
                deltalake::arrow::datatypes::TimeUnit::Millisecond => {
                    warn!("I have been asked to create a table with a Timestamp(millis) column ({}) that I cannot handle. Cowardly setting the Delta schema to pretend it is a Timestamp(micros)", field.name());
                    let field = deltalake::arrow::datatypes::Field::new(
                        field.name(),
                        deltalake::arrow::datatypes::DataType::Timestamp(
                            deltalake::arrow::datatypes::TimeUnit::Microsecond,
                            tz.clone(),
                        ),
                        field.is_nullable(),
                    );
                    conversions.push(Arc::new(field));
                }
                _ => conversions.push(field.clone()),
            },
            _ => conversions.push(field.clone()),
        }
    }

    let arrow_schema = ArrowSchema::new_with_metadata(conversions, arrow_schema.metadata.clone());

    let schema: deltalake::schema::Schema = Schema::try_from(&arrow_schema)
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

/**
 * Append the given files to an already existing and initialized Delta Table
 */
pub async fn append_to_table(files: &[ObjectMeta], table: &mut DeltaTable) -> DeltaResult<i64> {
    let existing_files = table.get_files();
    let new_files: Vec<ObjectMeta> = files
        .iter()
        .filter(|f| !existing_files.contains(&f.location))
        .cloned()
        .collect();

    if new_files.is_empty() {
        debug!("No new files to add on {table:?}, skipping a commit");
        return Ok(table.version());
    }

    let actions = add_actions_for(&new_files);

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

/**
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

/**
 * Return all the partitions from the given path buf
 */
fn partitions_from(path_str: &str) -> Vec<DeltaTablePartition> {
    path_str
        .split('/')
        .flat_map(DeltaTablePartition::try_from)
        .collect()
}

/**
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
                .into_iter()
                .map(|p| ((p.key), Some(p.value)))
                .collect(),
            partition_values_parsed: None,
        })
        .map(Action::add)
        .collect()
}

/**
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

    use chrono::prelude::Utc;
    use deltalake::Path;

    /*
     * test utilities to share between test cases
     */
    mod util {
        use std::collections::HashSet;
        use std::hash::Hash;
        use std::sync::Arc;

        use url::Url;

        use super::*;

        pub(crate) fn assert_unordered_eq<T>(left: &[T], right: &[T])
        where
            T: Eq + Hash + std::fmt::Debug,
        {
            let left: HashSet<_> = left.iter().collect();
            let right: HashSet<_> = right.iter().collect();

            assert_eq!(left, right);
        }

        #[allow(dead_code)]
        /**
         * Helper function to use when debugging to list temp and other directories recursively
         */
        fn list_directory(path: &std::path::Path) {
            if path.is_dir() {
                for entry in std::fs::read_dir(path).unwrap() {
                    let entry = entry.unwrap();
                    println!("{}", entry.path().display());
                    list_directory(&entry.path());
                }
            }
        }

        /**
         * Create a temporary directory filled with parquet files but no _delta_log
         * for testing.
         *
         * The [tempfile::TempDir] must be passed to the caller to ensure the destructor doesn't
         * delete the directory before it is used further in the test.
         */
        pub(crate) fn create_temp_path_with(
            fixture_table_path: &str,
        ) -> (tempfile::TempDir, Arc<DeltaObjectStore>) {
            use fs_extra::{copy_items, dir::CopyOptions, remove_items};

            let path = std::fs::canonicalize(fixture_table_path).expect("Failed to canonicalize");
            let dir = tempfile::tempdir().expect("Failed to create a temporary directory");

            let options = CopyOptions::new();
            let _ = copy_items(&[path.as_path()], dir.path(), &options)
                .expect("Failed to copy items over");
            // Remove the tempdir's copied _delta_log/ since the test must recreate it
            remove_items(&[dir.path().join("_delta_log")])
                .expect("Failed to remove temp _delta_log/");

            let url = Url::from_file_path(dir.path()).expect("Failed to parse local path");
            (dir, object_store_for(&url, None))
        }
    }

    #[tokio::test]
    async fn discover_parquet_files_empty_dir() {
        let dir = tempfile::tempdir().expect("Failed to create a temporary directory");
        let url = Url::from_file_path(dir.path()).expect("Failed to parse local path");
        let store = object_store_for(&url, None);

        let files = discover_parquet_files(store.clone())
            .await
            .expect("Failed to discover parquet files");
        assert_eq!(files.len(), 0);
    }

    #[tokio::test]
    async fn discover_parquet_files_full_dir() {
        let path = std::fs::canonicalize("../../tests/data/hive/deltatbl-non-partitioned")
            .expect("Failed to canonicalize");
        let url = Url::from_file_path(path).expect("Failed to parse local path");
        let store = object_store_for(&url, None);

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
        let result = add_actions_for(&[]);
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
        assert_eq!(expected, partition_columns_from(&[]));
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
        util::assert_unordered_eq(&expected, &partition_columns_from(&files));
    }

    /*
     * See <https://github.com/buoyant-data/oxbow/issues/2>
     */
    #[tokio::test]
    async fn create_schema_for_partitioned_path() {
        let (_tempdir, store) =
            util::create_temp_path_with("../../tests/data/hive/deltatbl-partitioned");
        let files = discover_parquet_files(store.clone())
            .await
            .expect("Failed to discover parquet files");
        assert_eq!(files.len(), 4, "No files discovered");

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

    /*
     * See <https://github.com/buoyant-data/oxbow/issues/5>
     */
    #[tokio::test]
    async fn create_table_without_files() {
        let (_tempdir, store) =
            util::create_temp_path_with("../../tests/data/hive/deltatbl-partitioned");

        let files = vec![];

        let result = create_table_with(&files, store.clone()).await;
        assert!(result.is_err());
    }

    /*
     * Attempt to create a table with a parquet file containing a timestamp column with millisecond
     * precision
     */
    #[tokio::test]
    async fn create_table_with_millis_timestamp() {
        let (_tempdir, store) = util::create_temp_path_with("../../tests/data/hive/faker_products");

        let files = discover_parquet_files(store.clone())
            .await
            .expect("Failed to discover parquet files");
        assert_eq!(files.len(), 1, "No files discovered");

        let result = create_table_with(&files, store.clone()).await;
        assert!(result.is_ok(), "Failed to create: {result:?}");
    }

    #[tokio::test]
    async fn attempt_to_convert_without_auth() {
        let region = std::env::var("AWS_REGION");
        std::env::set_var("AWS_REGION", "custom");

        let files: Vec<ObjectMeta> = vec![];
        let store = object_store_for(&Url::parse("s3://example/non-existent").unwrap(), None);
        let result = create_table_with(&files, store).await;
        println!("result from create_table_with: {:?}", result);
        assert!(result.is_err());

        if let Ok(region) = region {
            std::env::set_var("AWS_REGION", region);
        }
    }

    #[tokio::test]
    async fn test_avoid_discovering_checkpoints() {
        let test_dir =
            std::fs::canonicalize("../../tests/data/hive/deltatbl-non-partitioned-with-checkpoint")
                .expect("Failed to canonicalize");
        let url = Url::from_file_path(test_dir).expect("Failed to parse local path");
        let store = object_store_for(&url, None);

        let files = discover_parquet_files(store.clone())
            .await
            .expect("Failed to discover parquet files");
        assert_eq!(files.len(), 2, "No files discovered");
    }

    /*
     * Ensure that the append_to_table() function does not add redundant files already added to the
     * Delta Table
     *
     * <https://github.com/buoyant-data/oxbow/issues/3>
     */
    #[tokio::test]
    async fn test_avoiding_adding_duplicate_files() {
        let (_tempdir, store) =
            util::create_temp_path_with("../../tests/data/hive/deltatbl-partitioned");

        let files = discover_parquet_files(store.clone())
            .await
            .expect("Failed to discover parquet files");
        assert_eq!(files.len(), 4, "No files discovered");

        let mut table = create_table_with(&files, store.clone())
            .await
            .expect("Failed to create table");
        let schema = table.get_schema().expect("Failed to get schema");
        assert!(
            schema.get_field_with_name("c2").is_ok(),
            "The schema does not include the expected partition key `c2`"
        );
        assert_eq!(
            table.get_files().len(),
            4,
            "Did not find the right number of tables"
        );

        append_to_table(&files, &mut table)
            .await
            .expect("Failed to append files");
        table.load().await.expect("Failed to reload the table");
        assert_eq!(table.get_files().len(), 4, "Found redundant files!");
    }

    /*
     * Discovered an issue where append_to_table can create a new empty commit if no files are
     * provided. That's silly!
     */
    #[tokio::test]
    async fn test_avoid_appending_empty_list() {
        let (_tempdir, store) =
            util::create_temp_path_with("../../tests/data/hive/deltatbl-partitioned");

        let files = discover_parquet_files(store.clone())
            .await
            .expect("Failed to discover parquet files");
        assert_eq!(files.len(), 4, "No files discovered");

        let mut table = create_table_with(&files, store.clone())
            .await
            .expect("Failed to create table");
        let schema = table.get_schema().expect("Failed to get schema");
        assert!(
            schema.get_field_with_name("c2").is_ok(),
            "The schema does not include the expected partition key `c2`"
        );
        assert_eq!(0, table.version(), "Unexpected version");
        // Start with an empty set
        let files = vec![];

        append_to_table(&files, &mut table)
            .await
            .expect("Failed to append files");
        table.load().await.expect("Failed to reload the table");
        assert_eq!(
            0,
            table.version(),
            "Unexpected version, should not have created a commit"
        );
        assert_eq!(table.get_files().len(), 4, "Found redundant files!");
    }
}
