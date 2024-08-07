///
/// The webhook lambda can receive JSONL formatted events and append them to a pre-configured Delta
/// table
use lambda_http::{run, service_fn, tracing, Body, Error, Request, Response};
use tracing::log::*;

use oxbow::write::*;

/// Main function handler which does the basics around HTTP management for the Lambda
async fn function_handler(event: Request) -> Result<Response<Body>, Error> {
    let table_uri = std::env::var("DELTA_TABLE_URI").expect("Failed to get `DELTA_TABLE_URI`");
    let secret = std::env::var("PRESHARED_WEBHOOK_SECRET")
        .expect("Failed to get `PRESHARED_WEBHOOK_SECRET`");

    if secret != event.headers()["Authorization"] {
        return Ok(Response::builder()
            .status(401)
            .body("Invalid authorization".into())
            .map_err(Box::new)?);
    }

    let response = match event.method() {
        &http::method::Method::POST => {
            debug!("Processing POST");
            let body = event.body();
            match body {
                lambda_http::Body::Text(buf) => {
                    debug!("Deserializing body text and appending to {table_uri}");
                    let table = oxbow::lock::open_table(&table_uri).await?;
                    debug!("{}", &buf);
                    match append_jsonl(table, buf).await {
                        Ok(_) => {}
                        Err(e) => {
                            error!("Failed to append the values to configured Delta table: {e:?}");
                            return Err(Box::new(e));
                        }
                    }
                }
                others => {
                    warn!("Unsupported body payload type: {others:?}");
                }
            }
            Response::builder()
                .status(200)
                .header("content-type", "application/json")
                .body("{}".into())
                .map_err(Box::new)?
        }
        others => {
            warn!("Received method I cannot support: {others:?}");
            Response::builder()
                .status(400)
                .body("I only speak POST".into())
                .map_err(Box::new)?
        }
    };
    Ok(response)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    deltalake::aws::register_handlers(None);
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        // disable printing the name of the module in every log line.
        .with_target(false)
        // disabling time is handy because CloudWatch will add the ingestion time.
        .without_time()
        .init();
    let _ = std::env::var("DELTA_TABLE_URI")
        .expect("THe `DELTA_TABLE_URI` environment must be set in the environment");

    info!("Starting webhook lambda handler");

    match std::env::var("DYNAMO_LOCK_TABLE_NAME") {
        Ok(_) => {}
        Err(_) => {
            warn!("sqs-ingest SHOULD have `DYNAMO_LOCK_TABLE_NAME` set to a valid name, and should have AWS_S3_LOCKING_PROVIDER=dynamodb set so that concurrent writes can be performed safely.");
        }
    }

    run(service_fn(function_handler)).await
}
