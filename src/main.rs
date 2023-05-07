/*
 * The main module contains the entrypoint for oxbow regardless of what mode it operates under
 */

use log::*;

#[cfg(feature = "cli")]
mod cli;
#[cfg(feature = "lambda")]
mod lambda;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    pretty_env_logger::init();

    info!("Starting oxbow");

    if cfg!(feature = "lambda") {
        #[cfg(feature = "lambda")]
        lambda::main().await.expect("Failed to run lambda main");
    } else {
        #[cfg(feature = "cli")]
        cli::main().await.expect("Failed to run CLI main");
    }
    Ok(())
}
