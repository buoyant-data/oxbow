///
/// The lib module contains the business logic of oxbow, regardless of the interface implementation
///
use deltalake::arrow::datatypes::Schema as ArrowSchema;
use deltalake::parquet::arrow::async_reader::{
    ParquetObjectReader, ParquetRecordBatchStreamBuilder,
};
use deltalake::parquet::file::metadata::ParquetMetaData;
use deltalake::partitions::DeltaTablePartition;
use deltalake::protocol::*;
use deltalake::storage::DeltaObjectStore;
use deltalake::{DeltaResult, DeltaTable, ObjectMeta, ObjectStore, SchemaDataType, SchemaField};
use futures::StreamExt;
use tracing::log::*;
use url::Url;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub mod lock;
pub mod write;

/// TableMods helps to keep track of files which should be added or removed in a given
/// invocation of Oxbow-based utilities.
///
/// Typically this struct is populated by S3 events which would indicate a file(s) has been added
/// or removed.
#[derive(Debug, Clone, Default)]
pub struct TableMods {
    pub adds: Vec<ObjectMeta>,
    pub removes: Vec<ObjectMeta>,
}

/// convert is the main function to be called by the CLI or other "one shot" executors which just
/// need to take a given location and convert it all at once
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

///
/// Create the [ObjectStore] for the given location
///
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

///
/// Discover `.parquet` files which are present in the location
///
/// **NOTE**: This will not _stop_ at any particular point and can list an entire
/// prefix. While it will not recursively list, it should only be used in cases
/// where the potential number of parquet files is quite small.
///
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
                            } else {
                                warn!("Was asked to discover parquet files on what appears to already be a table, and found checkpoint files: {filename}");
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

///
/// Create a Delta table with a given series of files at the specified location
///
pub async fn create_table_with(
    files: &[ObjectMeta],
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
        conversions.push(coerce_field(field.clone()));
    }

    let arrow_schema = ArrowSchema::new_with_metadata(conversions, arrow_schema.metadata.clone());

    let schema: deltalake::schema::Schema = Schema::try_from(&arrow_schema)
        .expect("Failed to convert the schema for creating the table");

    let mut columns = schema.get_fields().clone();

    for partition in &partitions {
        // Only add the partition if it does not already exist in the schema
        if schema.get_field_with_name(partition).is_err() {
            let field = SchemaField::new(
                partition.into(),
                SchemaDataType::primitive("string".into()),
                true,
                HashMap::new(),
            );
            columns.push(field);
        }
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

/// Commit the given [Action]s to the [DeltaTable]
pub async fn commit_to_table(actions: &Vec<Action>, table: &mut DeltaTable) -> DeltaResult<i64> {
    if actions.is_empty() {
        return Ok(table.version());
    }
    deltalake::operations::transaction::commit(
        table.object_store().as_ref(),
        actions,
        DeltaOperation::Update { predicate: None },
        table.get_state(),
        None,
    )
    .await
}

/// Generate the list of [Action]s based on the given [TableMods]
pub async fn actions_for(
    mods: &TableMods,
    table: &DeltaTable,
    evolve_schema: bool,
) -> DeltaResult<Vec<Action>> {
    let existing_files = table.get_files();
    let new_files: Vec<ObjectMeta> = mods
        .adds
        .iter()
        .filter(|f| !existing_files.contains(&f.location))
        .cloned()
        .collect();

    let adds = add_actions_for(&new_files);
    let removes = remove_actions_for(&mods.removes);
    let metadata = match evolve_schema {
        true => match metadata_actions_for(&new_files, table).await {
            Ok(axns) => axns,
            Err(err) => {
                error!("Attempted schema evolution but received an unhandled error!: {err:?}");
                vec![]
            }
        },
        false => vec![],
    };

    Ok([adds, removes, metadata].concat())
}

/// Return the new metadata actions if the files given can result in a schema migration
///
/// If nothing should happen then this will end up returning an empty set
async fn metadata_actions_for(
    files: &[ObjectMeta],
    table: &DeltaTable,
) -> DeltaResult<Vec<Action>> {
    if let Some(last_file) = files.last() {
        debug!("Attempting to evolve the schema for {table:?}");
        let table_schema = table.get_schema()?;
        // Cloning here to take an owned version of [DeltaTableMetaData] for later modification
        let mut table_metadata = table.get_metadata()?.clone();

        let file_schema =
            fetch_parquet_schema(table.object_store().clone(), last_file.clone()).await?;
        let mut new_schema: Vec<SchemaField> = table_schema.get_fields().clone();

        for file_field in file_schema.fields() {
            let name = file_field.name();
            // If the table's schema doesn't have the field, add to our new schema
            if table_schema.get_field_with_name(name).is_err() {
                debug!("Found a new column `{name}` which will be added");
                new_schema.push(SchemaField::new(
                    name.to_string(),
                    // These types can have timestmaps in them, so coerce them properly
                    coerce_field(file_field.clone()).data_type().try_into()?,
                    true,
                    HashMap::default(),
                ));
            }
        }

        if new_schema.len() > table_schema.get_fields().len() {
            let new_schema = deltalake::schema::SchemaTypeStruct::new(new_schema);
            table_metadata.schema = new_schema;
            table_metadata.created_time = Some(chrono::Utc::now().timestamp_millis());
            return Ok(vec![Action::metaData(table_metadata.try_into()?)]);
        }
    }
    Ok(vec![])
}

/// Take an iterator of files and determine what looks like a partition column from it
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

/// Return all the partitions from the given path buf
fn partitions_from(path_str: &str) -> Vec<DeltaTablePartition> {
    path_str
        .split('/')
        .flat_map(DeltaTablePartition::try_from)
        .collect()
}

/// Provide a series of Add actions for the given ObjectMeta entries
///
/// This is a critical translation layer between discovered parquet files and how those would be
/// represented inside of the log
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

/// Provide a series of Remove actions for the given [ObjectMeta] entries
pub fn remove_actions_for(files: &[ObjectMeta]) -> Vec<Action> {
    files
        .iter()
        .map(|om| Remove {
            path: om.location.to_string(),
            data_change: true,
            size: Some(om.size as i64),
            partition_values: Some(
                partitions_from(om.location.as_ref())
                    .into_iter()
                    .map(|p| ((p.key), Some(p.value)))
                    .collect(),
            ),
            ..Default::default()
        })
        .map(Action::remove)
        .collect()
}

/// Return the smallest file from the given set of files.
///
/// This can be useful to find the smallest possible parquet file to load from the set in order to
/// discern schema information
fn find_smallest_file(files: &[ObjectMeta]) -> Option<&ObjectMeta> {
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

/// Retrieve and return the schema of the given [ObjectMeta] parquet file based on
/// its metadata
pub async fn fetch_parquet_schema(
    store: Arc<dyn ObjectStore>,
    file: ObjectMeta,
) -> DeltaResult<ArrowSchema> {
    let metadata = load_parquet_metadata(store.clone(), file).await?;
    let schema = metadata.file_metadata().schema_descr();
    Ok(deltalake::parquet::arrow::parquet_to_arrow_schema(
        schema, None,
    )?)
}

/// Retrieve a the metadata for a given parquet file referenced by [ObjectMeta]
async fn load_parquet_metadata(
    store: Arc<dyn ObjectStore>,
    file: ObjectMeta,
) -> DeltaResult<Arc<ParquetMetaData>> {
    let reader = ParquetObjectReader::new(store.clone(), file);
    let builder = ParquetRecordBatchStreamBuilder::new(reader).await.unwrap();
    let metadata = builder.metadata();

    deltalake::parquet::schema::printer::print_parquet_metadata(&mut std::io::stdout(), metadata);
    Ok(metadata.clone())
}

fn coerce_field(
    field: deltalake::arrow::datatypes::FieldRef,
) -> deltalake::arrow::datatypes::FieldRef {
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
                return Arc::new(field);
            }
            _ => {}
        },
        _ => {}
    };
    field.clone()
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

        /// Return a simple test table for based on the fixtures
        ///
        /// The table that is returned cannot be reloaded since the tempdir is dropped
        pub(crate) async fn test_table() -> DeltaTable {
            let (_tempdir, store) =
                create_temp_path_with("../../tests/data/hive/deltatbl-partitioned");

            let files = discover_parquet_files(store.clone())
                .await
                .expect("Failed to discover parquet files");
            assert_eq!(files.len(), 4, "No files discovered");

            create_table_with(&files, store.clone())
                .await
                .expect("Failed to create table")
        }

        pub(crate) fn paths_to_objectmetas(slice: Vec<Path>) -> Vec<ObjectMeta> {
            slice
                .into_iter()
                .map(|location| ObjectMeta {
                    location,
                    last_modified: Utc::now(),
                    size: 1,
                    e_tag: None,
                })
                .collect()
        }

        pub(crate) fn assert_unordered_eq<T>(left: &[T], right: &[T])
        where
            T: Eq + Hash + std::fmt::Debug,
        {
            let left: HashSet<_> = left.iter().collect();
            let right: HashSet<_> = right.iter().collect();

            assert_eq!(left, right);
        }

        #[allow(dead_code)]
        /// Helper function to use when debugging to list temp and other directories recursively
        pub(crate) fn list_directory(path: &std::path::Path) {
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
    fn remove_actions_for_not_empty() {
        let files = vec![ObjectMeta {
            location: Path::from(
                "part-00001-f2126b8d-1594-451b-9c89-c4c2481bfd93-c000.snappy.parquet",
            ),
            last_modified: Utc::now(),
            size: 689,
            e_tag: None,
        }];
        let result = remove_actions_for(&files);
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
     * There are some cases where data will be laid out in a hive partition scheme but the parquet
     * files may not contain the partitioning information. When using EXPORT DATA from BigQuery it
     * will automatically insert the hive-style partition information into the parquet schema
     */
    #[tokio::test]
    async fn test_avoid_duplicate_partition_columns() {
        let (_tempdir, store) = util::create_temp_path_with("../../tests/data/hive/gcs-export");

        let files = discover_parquet_files(store.clone())
            .await
            .expect("Failed to discover parquet files");
        assert_eq!(files.len(), 2, "No files discovered");

        let table = create_table_with(&files, store.clone())
            .await
            .expect("Failed to create table");
        let schema = table.get_schema().expect("Failed to get schema");
        let fields: Vec<&str> = schema.get_fields().iter().map(|f| f.get_name()).collect();

        let mut uniq = HashSet::new();
        for field in &fields {
            uniq.insert(field);
        }
        assert_eq!(
            uniq.len(),
            fields.len(),
            "There were not unique fields, that probably means a `ds` column is doubled up"
        );
    }

    /// This test is mostly to validate an approach for reading and loading schema
    /// from a parquet file into an Arrow schema for evolution
    #[tokio::test]
    async fn test_load_parquet_metadata() -> DeltaResult<()> {
        let location = Path::from(
            "c2=foo1/part-00001-1c702e73-89b5-465a-9c6a-25f7559cd150.c000.snappy.parquet",
        );
        let url = Url::from_file_path(std::fs::canonicalize(
            "../../tests/data/hive/deltatbl-partitioned",
        )?)
        .expect("Failed to parse");
        let storage = object_store_for(&url, None);
        let meta = storage.head(&location).await.unwrap();

        let schema = fetch_parquet_schema(storage.clone(), meta)
            .await
            .expect("Failed to load parquet file schema");
        assert_eq!(
            schema.fields.len(),
            1,
            "Expected only to find one field on the Parquet file's schema"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_actions_for() -> DeltaResult<()> {
        let (_tempdir, store) =
            util::create_temp_path_with("../../tests/data/hive/deltatbl-partitioned");

        let files = discover_parquet_files(store.clone())
            .await
            .expect("Failed to discover parquet files");
        assert_eq!(files.len(), 4, "No files discovered");

        // Creating the table with one of the discovered files, so the remaining three should be
        // added later
        let table = create_table_with(&[files[0].clone()], store.clone())
            .await
            .expect("Failed to create table");

        let mods = TableMods {
            adds: files,
            ..Default::default()
        };

        let actions = actions_for(&mods, &table, false)
            .await
            .expect("Failed to curate actions");
        assert_eq!(
            actions.len(),
            3,
            "Expected an add action for every new file discovered"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_actions_for_with_redundant_files() {
        let table = util::test_table().await;
        let files = util::paths_to_objectmetas(table.get_files());
        let mods = TableMods {
            adds: files,
            ..Default::default()
        };

        let actions = actions_for(&mods, &table, false)
            .await
            .expect("Failed to curate actions");
        assert_eq!(
            actions.len(),
            0,
            "Expected no add actions for redundant files"
        );
    }

    #[tokio::test]
    async fn test_actions_for_with_removes() -> DeltaResult<()> {
        let table = util::test_table().await;
        let files = util::paths_to_objectmetas(table.get_files());
        let mods = TableMods {
            adds: vec![],
            removes: files,
        };

        let actions = actions_for(&mods, &table, false)
            .await
            .expect("Failed to curate actions");
        assert_eq!(
            actions.len(),
            4,
            "Expected an add action for every new file discovered"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_commit_with_no_actions() {
        let mut table = util::test_table().await;
        let initial_version = table.version();
        let result = commit_to_table(&vec![], &mut table).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), initial_version);
    }

    #[tokio::test]
    async fn test_commit_with_remove_actions() {
        let mut table = util::test_table().await;
        let initial_version = table.version();

        let files = util::paths_to_objectmetas(table.get_files());
        let mods = TableMods {
            adds: vec![],
            removes: files,
        };
        let actions = actions_for(&mods, &table, false)
            .await
            .expect("Failed to curate actions");
        assert_eq!(
            actions.len(),
            4,
            "Expected an add action for every new file discovered"
        );

        let result = commit_to_table(&actions, &mut table).await;
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            initial_version + 1,
            "Should have incremented the version"
        );
    }

    #[tokio::test]
    async fn test_commit_with_all_actions() {
        let (_tempdir, store) =
            util::create_temp_path_with("../../tests/data/hive/deltatbl-partitioned");
        let files = discover_parquet_files(store.clone())
            .await
            .expect("Failed to discover parquet files");
        assert_eq!(files.len(), 4, "No files discovered");
        // Creating the table with one of the discovered files, so the remaining three should be
        // added later
        let mut table = create_table_with(&[files[0].clone()], store.clone())
            .await
            .expect("Failed to create table");
        let initial_version = table.version();

        let mods = TableMods {
            adds: files.clone(),
            removes: vec![files[0].clone()],
        };
        let actions = actions_for(&mods, &table, false)
            .await
            .expect("Failed to curate actions");
        assert_eq!(
            actions.len(),
            4,
            "Expected an 3 add actions and 1 remove action"
        );

        let result = commit_to_table(&actions, &mut table).await;
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            initial_version + 1,
            "Should have incremented the version"
        );
        table.load().await.expect("Failed to reload table");
        assert_eq!(
            table.get_files().len(),
            3,
            "Expected to only find three files on the table at this state"
        );
    }

    #[tokio::test]
    async fn test_schema_evolution() {
        use deltalake::operations::create::CreateBuilder;

        let (_tempdir, store) =
            util::create_temp_path_with("../../tests/data/hive/deltatbl-partitioned");

        let table = CreateBuilder::new()
            .with_object_store(store.clone())
            .with_column(
                "party_time",
                SchemaDataType::primitive("string".into()),
                true,
                None,
            )
            .with_save_mode(SaveMode::Ignore)
            .await
            .expect("Failed to create a test table");

        let initial_version = table.version();
        assert_eq!(initial_version, 0);

        let adds = discover_parquet_files(store.clone())
            .await
            .expect("Failed to discover parquet files");
        assert_eq!(adds.len(), 4, "No files discovered");
        let mods = TableMods {
            adds,
            ..Default::default()
        };

        let actions = actions_for(&mods, &table, true)
            .await
            .expect("Failed to get actions");
        assert_eq!(
            actions.len(),
            5,
            "Expected 4 add actions and a metadata action"
        );
    }

    #[tokio::test]
    async fn test_schema_evolution_with_timestamps() {
        use fs_extra::{copy_items, dir::CopyOptions};
        let (tempdir, _store) =
            util::create_temp_path_with("../../tests/data/hive/deltatbl-partitioned");

        // The store that comes back is not properly prefixed to the delta table that this test
        // needs to work with
        let table_url = Url::from_file_path(tempdir.path().join("deltatbl-partitioned"))
            .expect("Failed to parse local path");
        let store = object_store_for(&table_url, None);

        let files = discover_parquet_files(store.clone())
            .await
            .expect("Failed to discover parquet files");
        assert_eq!(files.len(), 4, "No files discovered");

        let table = create_table_with(&files, store.clone())
            .await
            .expect("Failed to create table");

        let options = CopyOptions::new();
        // This file has some _alternative_ timestamps and can be used for testing :smile:
        let new_file =
            std::fs::canonicalize("../../tests/data/hive/faker_products/1701800282197_0.parquet")
                .expect("Failed to canonicalize");
        let _ =
            copy_items(&[new_file], store.root_uri(), &options).expect("Failed to copy items over");

        let files = vec![ObjectMeta {
            location: Path::from("1701800282197_0.parquet"),
            e_tag: None,
            size: 11351,
            last_modified: Utc::now(),
        }];
        let metadata = metadata_actions_for(&files, &table)
            .await
            .expect("Failed to generate metadata actions");
        assert_eq!(
            metadata.len(),
            1,
            "Execpted a scheam evolution metadata action"
        );
    }
}
