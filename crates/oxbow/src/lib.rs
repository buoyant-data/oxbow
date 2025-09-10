use deltalake::ObjectStore;
///
/// The lib module contains the business logic of oxbow, regardless of the interface implementation
///
use deltalake::arrow::datatypes::Schema as ArrowSchema;
use deltalake::kernel::models::{Schema, StructField};
use deltalake::kernel::*;
use deltalake::logstore::ObjectStoreRef;
use deltalake::logstore::{LogStoreRef, logstore_for};
use deltalake::operations::create::CreateBuilder;
use deltalake::parquet::arrow::async_reader::{
    ParquetObjectReader, ParquetRecordBatchStreamBuilder,
};
use deltalake::parquet::file::metadata::ParquetMetaData;
use deltalake::protocol::*;
use deltalake::{DeltaResult, DeltaTable, DeltaTableError, ObjectMeta};
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
    adds: HashMap<deltalake::Path, ObjectMeta>,
    removes: HashMap<deltalake::Path, ObjectMeta>,
}

impl TableMods {
    pub fn new(adds: &[ObjectMeta], removes: &[ObjectMeta]) -> Self {
        Self {
            adds: HashMap::from_iter(adds.iter().map(|o| (o.location.clone(), o.clone()))),
            removes: HashMap::from_iter(removes.iter().map(|o| (o.location.clone(), o.clone()))),
        }
    }

    pub fn adds(&self) -> Vec<&ObjectMeta> {
        self.adds.values().collect()
    }

    pub fn add(&mut self, add: ObjectMeta) -> bool {
        if self.adds.contains_key(&add.location) {
            return false;
        }
        self.adds.insert(add.location.clone(), add);
        true
    }

    pub fn removes(&self) -> Vec<&ObjectMeta> {
        self.removes.values().collect()
    }

    pub fn remove(&mut self, remove: ObjectMeta) -> bool {
        if self.removes.contains_key(&remove.location) {
            return false;
        }
        self.removes.insert(remove.location.clone(), remove);
        true
    }
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
            let store = logstore_for(location, storage_options.unwrap_or_default(), None)?;
            let files = discover_parquet_files(store.object_store(None).clone()).await?;
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
/// Discover `.parquet` files which are present in the location
///
/// **NOTE**: This will not _stop_ at any particular point and can list an entire
/// prefix. While it will not recursively list, it should only be used in cases
/// where the potential number of parquet files is quite small.
///
pub async fn discover_parquet_files(
    store: ObjectStoreRef,
) -> deltalake::DeltaResult<Vec<ObjectMeta>> {
    info!("Discovering parquet files for {store:?}");
    let mut result = vec![];
    let mut iter = store.list(None);

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
                                warn!(
                                    "Was asked to discover parquet files on what appears to already be a table, and found checkpoint files: {filename}"
                                );
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
    store: LogStoreRef,
) -> DeltaResult<DeltaTable> {
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
    let file_reader =
        ParquetObjectReader::new(store.object_store(None).clone(), smallest.location.clone());
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

    let schema = Schema::try_from(&arrow_schema)
        .expect("Failed to convert the schema for creating the table");

    let mut columns: Vec<StructField> = schema.fields().cloned().collect();

    for partition in &partitions {
        // Only add the partition if it does not already exist in the schema
        if schema.index_of(partition).is_none() {
            let field = StructField::new(partition, DataType::STRING, true);
            columns.push(field);
        }
    }

    /*
     * Create and persist the table
     */
    let actions = add_actions_for(files);

    debug!("Creating table with the log store: {store:?}");
    CreateBuilder::new()
        .with_log_store(store)
        .with_columns(columns)
        .with_partition_columns(partitions)
        .with_actions(actions)
        .with_save_mode(SaveMode::Ignore)
        .await
}

/// Commit the given [Action]s to the [DeltaTable]
pub async fn commit_to_table(actions: &[Action], table: &DeltaTable) -> DeltaResult<i64> {
    use deltalake::kernel::transaction::{CommitBuilder, CommitProperties};
    if actions.is_empty() {
        return Ok(table.version());
    }
    let commit = CommitProperties::default();
    let pre_commit = CommitBuilder::from(commit)
        .with_actions(actions.to_vec())
        .build(
            Some(table.snapshot()?),
            table.log_store(),
            DeltaOperation::Update { predicate: None },
        );

    Ok(pre_commit.await?.version())
}

/// Generate the list of [Action]s based on the given [TableMods]
pub async fn actions_for(
    mods: &TableMods,
    table: &DeltaTable,
    evolve_schema: bool,
) -> DeltaResult<Vec<Action>> {
    let existing_files: Vec<deltalake::Path> = table.get_files_iter()?.collect();
    let new_files: Vec<ObjectMeta> = mods
        .adds()
        .into_iter()
        .filter(|f| !existing_files.contains(&f.location))
        .cloned()
        .collect();

    let adds = add_actions_for(&new_files);
    let removes = remove_actions_for(
        &mods
            .removes()
            .into_iter()
            .cloned()
            .collect::<Vec<ObjectMeta>>(),
    );
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
        let table_metadata = table.metadata()?;

        let file_schema =
            fetch_parquet_schema(table.object_store().clone(), last_file.clone()).await?;
        let mut new_schema: Vec<StructField> = table_schema.fields().cloned().collect();

        for file_field in file_schema.fields() {
            let name = file_field.name();
            // If the table's schema doesn't have the field, add to our new schema
            if table_schema.index_of(name).is_none() {
                debug!("Found a new column `{name}` which will be added");
                let coerced = coerce_field(file_field.clone());
                new_schema.push(StructField::new(
                    name.to_string(),
                    // These types can have timestmaps in them, so coerce them properly
                    deltalake::kernel::DataType::try_from(coerced.data_type())?,
                    true,
                ));
            }
        }

        if new_schema.len() > table_schema.fields.len() {
            let new_schema = Schema::new(new_schema);
            let mut action = deltalake::kernel::Metadata::try_new(
                new_schema,
                table_metadata.partition_columns.clone(),
                table_metadata.configuration.clone(),
            )?;
            if let Some(name) = &table_metadata.name {
                action = action.with_name(name);
            }
            if let Some(description) = &table_metadata.description {
                action = action.with_description(description);
            }
            return Ok(vec![Action::Metadata(action)]);
        }
    }
    Ok(vec![])
}

/// Try to create a DeltaTable partition from a HivePartition string.
/// Returns a DeltaTableError if the string is not in the form of a HivePartition.
fn partition_try_from(partition: &str) -> Result<(String, Option<String>), DeltaTableError> {
    let partition_splitted: Vec<&str> = partition.split('=').collect();
    match partition_splitted {
        partition_splitted if partition_splitted.len() == 2 => Ok((
            partition_splitted[0].to_owned(),
            Some(partition_splitted[1].to_owned()),
        )),
        _ => Err(DeltaTableError::PartitionError {
            partition: partition.to_string(),
        }),
    }
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
            .flat_map(partition_try_from)
            .map(|(k, _v)| results.insert(k))
            .collect::<Vec<_>>();
    }

    results.into_iter().collect()
}

/// Return all the partitions from the given path buf
fn partitions_from(path_str: &str) -> HashMap<String, Option<String>> {
    path_str.split('/').flat_map(partition_try_from).collect()
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
            partition_values: partitions_from(om.location.as_ref()),
            ..Default::default()
        })
        .map(Action::Add)
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
            partition_values: Some(partitions_from(om.location.as_ref())),
            ..Default::default()
        })
        .map(Action::Remove)
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
    let reader = ParquetObjectReader::new(store.clone(), file.location.clone());
    let builder = ParquetRecordBatchStreamBuilder::new(reader).await.unwrap();
    let metadata = builder.metadata();

    deltalake::parquet::schema::printer::print_parquet_metadata(&mut std::io::stdout(), metadata);
    Ok(metadata.clone())
}

fn coerce_field(
    field: deltalake::arrow::datatypes::FieldRef,
) -> deltalake::arrow::datatypes::FieldRef {
    use deltalake::arrow::datatypes::*;
    match field.data_type() {
        DataType::Timestamp(unit, tz) => match unit {
            TimeUnit::Nanosecond => {
                warn!(
                    "Given a nanosecond precision which we will cowardly pretend is microseconds"
                );
                let field = Field::new(
                    field.name(),
                    DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
                    field.is_nullable(),
                );
                return Arc::new(field);
            }
            TimeUnit::Millisecond => {
                warn!(
                    "I have been asked to create a table with a Timestamp(millis) column ({}) that I cannot handle. Cowardly setting the Delta schema to pretend it is a Timestamp(micros)",
                    field.name()
                );
                let field = Field::new(
                    field.name(),
                    DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
                    field.is_nullable(),
                );
                return Arc::new(field);
            }
            _ => {}
        },
        DataType::List(field) => {
            let coerced = coerce_field(field.clone());
            let list_field = Field::new(field.name(), DataType::List(coerced), field.is_nullable());
            return Arc::new(list_field);
        },
        DataType::Struct(fields) => {
            let coerced: Vec<deltalake::arrow::datatypes::FieldRef> =
                fields.iter().map(|f| coerce_field(f.clone())).collect();
            let struct_field = Field::new(
                field.name(),
                DataType::Struct(coerced.iter().map(|f| f.as_ref().clone()).collect()),
                field.is_nullable(),
            );
            return Arc::new(struct_field);
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

        use url::Url;

        use super::*;

        /// Return a simple test table for based on the fixtures
        ///
        /// The table that is returned cannot be reloaded since the tempdir is dropped
        pub(crate) async fn test_table() -> (tempfile::TempDir, DeltaTable) {
            let (td, store) = create_temp_path_with("../../tests/data/hive/deltatbl-partitioned");

            let files = discover_parquet_files(store.object_store(None).clone())
                .await
                .expect("Failed to discover parquet files");
            assert_eq!(files.len(), 4, "No files discovered");

            (
                td,
                create_table_with(&files, store.clone())
                    .await
                    .expect("Failed to create table"),
            )
        }

        pub(crate) fn paths_to_objectmetas(
            slice: impl Iterator<Item = impl Into<deltalake::Path>>,
        ) -> Vec<ObjectMeta> {
            slice
                .into_iter()
                .map(|location| ObjectMeta {
                    location: location.into(),
                    last_modified: Utc::now(),
                    size: 1,
                    e_tag: None,
                    version: None,
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
        ) -> (tempfile::TempDir, LogStoreRef) {
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
            (
                dir,
                logstore_for(url, HashMap::<String, String>::default(), None)
                    .expect("Failed to get store"),
            )
        }
    }

    #[tokio::test]
    async fn discover_parquet_files_empty_dir() {
        let dir = tempfile::tempdir().expect("Failed to create a temporary directory");
        let url = Url::from_file_path(dir.path()).expect("Failed to parse local path");
        let store = logstore_for(url, HashMap::<String, String>::default(), None)
            .expect("Failed to get store");

        let files = discover_parquet_files(store.object_store(None).clone())
            .await
            .expect("Failed to discover parquet files");
        assert_eq!(files.len(), 0);
    }

    #[tokio::test]
    async fn discover_parquet_files_full_dir() {
        let path = std::fs::canonicalize("../../tests/data/hive/deltatbl-non-partitioned")
            .expect("Failed to canonicalize");
        let url = Url::from_file_path(path).expect("Failed to parse local path");
        let store = logstore_for(url, HashMap::<String, String>::default(), None)
            .expect("Failed to get store");

        let files = discover_parquet_files(store.object_store(None).clone())
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
                version: None,
            },
            ObjectMeta {
                location: Path::from("foo/small"),
                last_modified: Utc::now(),
                size: 1024,
                e_tag: None,
                version: None,
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
            version: None,
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
            version: None,
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
                version: None,
            },
            ObjectMeta {
                location: Path::from("foo/small"),
                last_modified: Utc::now(),
                size: 1024,
                e_tag: None,
                version: None,
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
                version: None,
            },
            ObjectMeta {
                location: Path::from("c2=foo2/small"),
                last_modified: Utc::now(),
                e_tag: None,
                size: 1024,
                version: None,
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
                version: None,
            },
            ObjectMeta {
                location: Path::from("c2=foo2/c3=bar2/small"),
                last_modified: Utc::now(),
                e_tag: None,
                size: 1024,
                version: None,
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
        let files = discover_parquet_files(store.object_store(None).clone())
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
            schema.index_of("c2").is_some(),
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

        let files = discover_parquet_files(store.object_store(None).clone())
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
        deltalake::aws::register_handlers(None);

        let files: Vec<ObjectMeta> = vec![];
        let store = logstore_for(
            Url::parse("s3://example/non-existent").unwrap(),
            HashMap::<String, String>::default(),
            None,
        )
        .expect("Failed to get store");
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
        let store = logstore_for(url, HashMap::<String, String>::default(), None)
            .expect("Failed to get store");

        let files = discover_parquet_files(store.object_store(None).clone())
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

        let files = discover_parquet_files(store.object_store(None).clone())
            .await
            .expect("Failed to discover parquet files");
        assert_eq!(files.len(), 2, "No files discovered");

        let table = create_table_with(&files, store.clone())
            .await
            .expect("Failed to create table");
        let schema = table.get_schema().expect("Failed to get schema");
        let fields: Vec<&str> = schema.fields().map(|f| f.name.as_ref()).collect();

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
        let storage = logstore_for(url, HashMap::<String, String>::default(), None)
            .expect("Failed to get store");
        let meta = storage.object_store(None).head(&location).await.unwrap();

        let schema = fetch_parquet_schema(storage.object_store(None).clone(), meta)
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

        let files = discover_parquet_files(store.object_store(None).clone())
            .await
            .expect("Failed to discover parquet files");
        assert_eq!(files.len(), 4, "No files discovered");

        // Creating the table with one of the discovered files, so the remaining three should be
        // added later
        let table = create_table_with(&[files[0].clone()], store.clone())
            .await
            .expect("Failed to create table");

        let mods = TableMods::new(&files, &vec![]);

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
        let (_tempdir, table) = util::test_table().await;
        let files = util::paths_to_objectmetas(table.get_files_iter().unwrap());
        let mods = TableMods::new(&files, &vec![]);

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
        let (_tempdir, table) = util::test_table().await;
        let files = util::paths_to_objectmetas(table.get_files_iter()?);
        let mods = TableMods::new(&vec![], &files);

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

    /// This test and the one with removes blow ensures that if oxbow is triggered with multiple
    /// versions of the same file in a single payload that it will not add redundant `add` or
    /// `remove` actions to the transaction log.
    ///
    /// While these not expressly forbidden by the Delta protocol, the Databricks runtime does fail
    /// to operate on these files correct, specifically in the optimize case
    #[tokio::test]
    async fn test_actions_for_with_redundant_adds() -> DeltaResult<()> {
        let (_tempdir, store) =
            util::create_temp_path_with("../../tests/data/hive/deltatbl-partitioned");

        let files = discover_parquet_files(store.object_store(None).clone())
            .await
            .expect("Failed to discover parquet files");
        assert_eq!(files.len(), 4, "No files discovered");

        // Creating the table with one of the discovered files, so the remaining three should be
        // added later
        let table = create_table_with(&[files[0].clone()], store.clone())
            .await
            .expect("Failed to create table");

        let mut redundant = files.clone();
        redundant.append(&mut files.clone());
        let mods = TableMods::new(&redundant, &[]);

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
    async fn test_actions_for_with_redundant_removes() -> DeltaResult<()> {
        let (_tempdir, table) = util::test_table().await;
        let files = util::paths_to_objectmetas(table.get_files_iter()?);
        let mut redundant_removes = files.clone();
        redundant_removes.append(&mut files.clone());
        let mods = TableMods::new(&vec![], &redundant_removes);

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
    async fn test_tablemods_uniqueness() -> DeltaResult<()> {
        let (_tempdir, table) = util::test_table().await;
        let files = util::paths_to_objectmetas(table.get_files_iter()?);
        let mut redundant_removes = files.clone();
        redundant_removes.append(&mut files.clone());
        let mods = TableMods::new(&vec![], &redundant_removes);

        assert_eq!(0, mods.adds().len());
        assert_eq!(4, mods.removes().len());

        let mods = TableMods::new(&redundant_removes, &vec![]);

        assert_eq!(4, mods.adds().len());
        assert_eq!(0, mods.removes().len());

        Ok(())
    }

    #[tokio::test]
    async fn test_commit_with_no_actions() {
        let (_tempdir, table) = util::test_table().await;
        let initial_version = table.version();
        let result = commit_to_table(&[], &table).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), initial_version);
    }

    #[tokio::test]
    async fn test_commit_to_table_make_checkpoint() -> DeltaResult<()> {
        let (_tempdir, store) =
            util::create_temp_path_with("../../tests/data/hive/deltatbl-partitioned");
        let files = discover_parquet_files(store.object_store(None).clone())
            .await
            .expect("Failed to discover parquet files");
        assert_eq!(files.len(), 4, "No files discovered");
        // Creating the table with one of the discovered files, so the remaining three should be
        // added later
        let mut table = create_table_with(&[files[0].clone()], store.clone())
            .await
            .expect("Failed to create table");
        let initial_version = table.version();
        assert_eq!(0, initial_version);

        let mods = TableMods::new(&files, &[files[0].clone()]);
        let actions = actions_for(&mods, &table, false)
            .await
            .expect("Failed to curate actions");

        for _ in 0..101 {
            let _ = commit_to_table(&actions, &table).await?;
            table.load().await?;
        }
        assert_eq!(table.version(), 101);

        if let Some(state) = table.state.as_ref() {
            // The default is expected to be 100
            assert_eq!(100, state.table_config().checkpoint_interval());
        }

        use deltalake::Path;
        let checkpoint = table
            .object_store()
            .head(&Path::from("_delta_log/_last_checkpoint"))
            .await?;

        assert_ne!(0, checkpoint.size);
        Ok(())
    }

    #[tokio::test]
    async fn test_commit_with_remove_actions() -> DeltaResult<()> {
        let (_tempdir, table) = util::test_table().await;
        let initial_version = table.version();

        let files = util::paths_to_objectmetas(table.get_files_iter()?);
        let mods = TableMods::new(&[], &files);
        let actions = actions_for(&mods, &table, false).await?;
        assert_eq!(
            actions.len(),
            4,
            "Expected an add action for every new file discovered"
        );

        let result = commit_to_table(&actions, &table).await?;
        assert_eq!(
            result,
            initial_version + 1,
            "Should have incremented the version"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_commit_with_all_actions() {
        let (_tempdir, store) =
            util::create_temp_path_with("../../tests/data/hive/deltatbl-partitioned");
        let files = discover_parquet_files(store.object_store(None).clone())
            .await
            .expect("Failed to discover parquet files");
        assert_eq!(files.len(), 4, "No files discovered");
        // Creating the table with one of the discovered files, so the remaining three should be
        // added later
        let mut table = create_table_with(&[files[0].clone()], store.clone())
            .await
            .expect("Failed to create table");
        let initial_version = table.version();
        assert_eq!(0, initial_version);

        let mods = TableMods::new(&files, &vec![files[0].clone()]);
        let actions = actions_for(&mods, &table, false)
            .await
            .expect("Failed to curate actions");
        assert_eq!(
            actions.len(),
            4,
            "Expected an 3 add actions and 1 remove action"
        );

        let result = commit_to_table(&actions, &mut table).await.unwrap();
        assert_eq!(
            result,
            initial_version + 1,
            "Should have incremented the version"
        );
        table.load().await.expect("Failed to reload table");
        assert_eq!(
            table.get_files_iter().unwrap().collect::<Vec<_>>().len(),
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
            .with_log_store(store.clone())
            .with_column("party_time", DataType::STRING, true, None)
            .with_save_mode(SaveMode::Ignore)
            .await
            .expect("Failed to create a test table");

        let initial_version = table.version();
        assert_eq!(initial_version, 0);

        let adds = discover_parquet_files(store.object_store(None).clone())
            .await
            .expect("Failed to discover parquet files");
        assert_eq!(adds.len(), 4, "No files discovered");
        let mods = TableMods::new(&adds, &[]);

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

        let table_path = tempdir.path().join("deltatbl-partitioned");
        // The store that comes back is not properly prefixed to the delta table that this test
        // needs to work with
        let table_url = Url::from_file_path(&table_path).expect("Failed to parse local path");
        let store = logstore_for(table_url, HashMap::<String, String>::default(), None)
            .expect("Failed to get object store");

        let files = discover_parquet_files(store.object_store(None).clone())
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
        let _ = copy_items(&[new_file], table_path, &options).expect("Failed to copy items over");

        let files = vec![ObjectMeta {
            location: Path::from("1701800282197_0.parquet"),
            e_tag: None,
            size: 11351,
            last_modified: Utc::now(),
            version: None,
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

    #[test]
    fn test_table_mods_add() {
        let mut mods = TableMods::default();
        let meta = ObjectMeta {
            location: Path::from("some.parquet"),
            e_tag: None,
            size: 11351,
            last_modified: Utc::now(),
            version: None,
        };
        let meta2 = ObjectMeta {
            location: Path::from("some.parquet"),
            e_tag: None,
            size: 11351,
            last_modified: Utc::now(),
            version: None,
        };

        assert!(mods.add(meta2), "Failed to add new ObjectMeta");
        assert!(
            !mods.add(meta),
            "Should have gotten false when adding a duplicate"
        );
        assert_eq!(mods.adds().len(), 1, "Why are there two? {mods:#?}");
    }

    #[test]
    fn test_coerce_field_struct() {
        use deltalake::arrow::datatypes::*;
        let field = Field::new(
            "meta",
            DataType::Struct(
                vec![
                    Field::new(
                        "timestamp_ns",
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        true,
                    ),
                    Field::new(
                        "timestamp_ms",
                        DataType::Timestamp(TimeUnit::Microsecond, None),
                        true,
                    ),
                    Field::new(
                        "timestamps",
                        DataType::List(Arc::new(Field::new(
                            "item",
                            DataType::Timestamp(TimeUnit::Nanosecond, None),
                            true,
                        ))),
                        true,
                    ),
                    Field::new("id", DataType::Int32, true),
                ]
                .into(),
            ),
            true,
        );

        let coerced = coerce_field(Arc::new(field));
        let formatted = format!("{}", coerced);

        assert!(
            formatted.contains("Timestamp(Microsecond"),
            "Expected to find a Timestamp(Microsecond) in the coerced schema, got: {formatted}"
        );
        assert!(
            !formatted.contains("Timestamp(Nanosecond"),
            "Expected to not find a Timestamp(Nanosecond) in the coerced schema, got: {formatted}"
        );
    }
}
