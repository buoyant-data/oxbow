use deltalake::arrow::datatypes::Schema as ArrowSchema;
///
/// The webhook lambda can receive JSONL formatted events and append them to a pre-configured Delta
/// table
use deltalake::arrow::json::reader::ReaderBuilder;
use deltalake::writer::{record_batch::RecordBatchWriter, DeltaWriter};
use deltalake::DeltaTable;
use lambda_http::{run, service_fn, tracing, Body, Error, Request, Response};
use tracing::log::*;

use std::io::Cursor;

/// Function responsible for opening the table and actually appending values to it
async fn append_values(mut table: DeltaTable, jsonl: &str) -> Result<DeltaTable, Error> {
    let cursor = Cursor::new(jsonl);
    let schema = table.get_schema()?;
    let schema = ArrowSchema::try_from(schema)?;
    let mut reader = ReaderBuilder::new(schema.into()).build(cursor).unwrap();

    let mut writer = RecordBatchWriter::for_table(&table)?;

    while let Some(Ok(batch)) = reader.next() {
        println!("batch: {batch:?}");
        writer.write(batch).await?;
    }

    writer.flush_and_commit(&mut table).await?;

    Ok(table)
}

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
                    debug!("Deserializing body text into a Value");
                    let table = oxbow::lock::open_table(&table_uri).await?;
                    append_values(table, &buf).await?;
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
                .body("I ony speak POST".into())
                .map_err(Box::new)?
        }
    };
    Ok(response)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
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
    run(service_fn(function_handler)).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_append_values() -> deltalake::DeltaResult<()> {
        use deltalake::schema::SchemaDataType;
        use deltalake::*;

        let table = DeltaOps::try_from_uri("memory://")
            .await?
            .create()
            .with_table_name("test")
            .with_column(
                "id",
                SchemaDataType::primitive("integer".into()),
                true,
                None,
            )
            .with_column(
                "name",
                SchemaDataType::primitive("string".into()),
                true,
                None,
            )
            .await?;

        let jsonl = r#"
            {"id" : 0, "name" : "Ben"}
            {"id" : 1, "name" : "Chris"}
        "#;
        let table = append_values(table, jsonl)
            .await
            .expect("Failed to do nothing");

        assert_eq!(table.version(), 1);
        Ok(())
    }
}
