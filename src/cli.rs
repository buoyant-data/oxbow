/*
 * The CLI module contains all the necessary code for running oxbow in the command line.
 */

use futures::StreamExt;
use gumdrop::Options;
use log::*;
use url::Url;

use deltalake::Path;
use std::collections::HashMap;

/*
 * Flags is a structure for managing command linke parameters
 */
#[derive(Debug, Options)]
struct Flags {
    #[options(help = "print help message")]
    help: bool,
    #[options(help = "Table location, can also be set by TABLE_LOCATION")]
    table: Option<String>,
}

/*
 * Default implementation for Flags which is largely just used for testing
 */
impl Default for Flags {
    fn default() -> Self {
        Flags {
            help: false,
            table: Some("s3://test-bucket/table".into()),
        }
    }
}

/*
 * Main entrypoint for the command line
 */
pub async fn main() -> Result<(), anyhow::Error> {
    let flags = Flags::parse_args_default_or_exit();
    debug!("Options as read: {:?}", flags);
    let location = table_location(&flags)?;
    info!("Using the table location of: {:?}", location);

    match deltalake::open_table(&location).await {
        Err(e) => {
            debug!("No Delta table at {}: {:?}", location, e);
            /*
             * Parse the given location as a URL in a way that can be passed into
             * some delta APIs
             */
            let location = match Url::parse(&location) {
                Ok(parsed) => parsed,
                Err(_) => Url::from_file_path(&location)
                    .expect("Failed to parse the location as a file path"),
            };
            let _files = discover_parquet_files(&location).await?;
        }
        Ok(table) => {
            warn!("There is already a Delta table at: {}", table);
        }
    }

    Ok(())
}

/*
 * Discover `.parquet` files which are present in the location
 */
async fn discover_parquet_files(location: &Url) -> deltalake::DeltaResult<Vec<Path>> {
    use deltalake::storage::DeltaObjectStore;
    use deltalake::ObjectStore;

    let options = HashMap::new();
    let store = DeltaObjectStore::try_new(location.clone(), options).expect("Failed to make store");

    let mut result = vec![];
    let mut iter = store.list(None).await?;

    /*
     * NOTE: There is certainly some way to make this more compact
     */
    while let Some(path) = iter.next().await {
        // Result<ObjectMeta> has been yielded
        if let Ok(meta) = path {
            debug!("Discovered file: {:?}", meta.location);

            if let Some(ext) = meta.location.extension() {
                match ext {
                    "parquet" => {
                        result.push(meta.location);
                    }
                    &_ => {}
                }
            }
        }
    }
    Ok(result)
}

/*
 * Return the configured table location. If there is not one configured, this will panic the
 * process..
 */
fn table_location(flags: &Flags) -> Result<String, anyhow::Error> {
    match &flags.table {
        None => Ok(std::env::var("TABLE_LOCATION")?),
        Some(path) => Ok(path.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_location() {
        let flags = Flags::default();
        let location = table_location(&flags).expect("Failed to load table location");
        assert_eq!(location, "s3://test-bucket/table");
    }

    #[test]
    fn test_table_location_with_env() {
        let mut flags = Flags::default();
        flags.table = None;

        std::env::set_var("TABLE_LOCATION", "s3://test-bucket-from-env/table");

        let location = table_location(&flags).expect("Failed to load table location");
        assert_eq!(location, "s3://test-bucket-from-env/table");
    }

    #[tokio::test]
    async fn test_discover_parquet_files_empty_dir() {
        let dir = tempfile::tempdir().expect("Failed to create a temporary directory");
        let url = Url::from_file_path(dir.path()).expect("Failed to parse local path");
        let files = discover_parquet_files(&url)
            .await
            .expect("Failed to discover parquet files");
        assert_eq!(files.len(), 0);
    }

    #[tokio::test]
    async fn test_discover_parquet_files_full_dir() {
        let path = std::fs::canonicalize("./hive/deltatbl-non-partitioned")
            .expect("Failed to canonicalize");
        let url = Url::from_file_path(path).expect("Failed to parse local path");

        let files = discover_parquet_files(&url)
            .await
            .expect("Failed to discover parquet files");

        assert_eq!(files.len(), 2);
    }
}
