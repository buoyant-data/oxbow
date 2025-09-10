/*
 * The CLI module contains all the necessary code for running oxbow in the command line.
 */

use gumdrop::Options;
use tracing::log::*;

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
#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    info!("Starting oxbow");
    let flags = Flags::parse_args_default_or_exit();
    debug!("Options as read: {:?}", flags);
    let location = table_location(&flags)?;
    info!("Using the table location of: {:?}", location);

    oxbow::convert(&location, None)
        .await
        .expect("Failed to convert location");
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

        unsafe {
            std::env::set_var("TABLE_LOCATION", "s3://test-bucket-from-env/table");
        }

        let location = table_location(&flags).expect("Failed to load table location");
        assert_eq!(location, "s3://test-bucket-from-env/table");
    }
}
