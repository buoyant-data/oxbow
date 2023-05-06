/*
 * The CLI module contains all the necessary code for running oxbow in the command line.
 */

use gumdrop::Options;
use log::*;
use url::Url;

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
                Err(_) => {
                    let absolute = std::fs::canonicalize(&location)
                        .expect("Failed to canonicalize table location");
                    Url::from_file_path(&absolute)
                        .expect("Failed to parse the location as a file path")
                }
            };
            let store = oxbow::object_store_for(&location);
            let files = oxbow::discover_parquet_files(store.clone()).await?;
            debug!(
                "Files identified for turning into a delta table: {:?}",
                files
            );
            oxbow::create_table_with(&files, store.clone())
                .await
                .expect("Failed to create the table!");
        }
        Ok(table) => {
            warn!("There is already a Delta table at: {}", table);
        }
    }

    Ok(())
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
}
