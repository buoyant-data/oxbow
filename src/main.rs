/*
 * The main module contains the entrypoint for oxbow regardless of what mode it operates under
 */

use log::*;

mod cli;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    #[cfg(debug_assertions)]
    pretty_env_logger::init();

    info!("Starting oxbow");

    #[cfg(feature = "cli")]
    cli::main().await
}
