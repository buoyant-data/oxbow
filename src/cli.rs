/*
 * The CLI module contains all the necessary code for running oxbow in the command line.
 */

use gumdrop::Options;

#[derive(Debug, Options)]
struct Flags {
    #[options(help = "print help message")]
    help: bool,
    #[options(help = "Table location, can also be set by DATALAKE_LOCATION")]
    table: Option<String>,
}

pub async fn main() -> Result<(), anyhow::Error> {
    Ok(())
}
