///
/// The webhook lambda can receive JSONL formatted events and append them to a pre-configured Delta
/// table
use chrono::prelude::*;
use deltalake::arrow::array::RecordBatch;
use deltalake::arrow::datatypes::Schema as ArrowSchema;
use deltalake::arrow::json::reader::ReaderBuilder;
use deltalake::writer::{record_batch::RecordBatchWriter, DeltaWriter};
use deltalake::DeltaTable;
use lambda_http::{run, service_fn, tracing, Body, Error, Request, Response};
use tracing::log::*;

use std::io::Cursor;
use std::sync::Arc;

/// Function responsible for opening the table and actually appending values to it
async fn append_values(mut table: DeltaTable, jsonl: &str) -> Result<DeltaTable, Error> {
    let cursor = Cursor::new(jsonl);
    let schema = table.get_schema()?;
    let schema = ArrowSchema::try_from(schema)?;
    let mut reader = ReaderBuilder::new(schema.into()).build(cursor).unwrap();

    let mut writer = RecordBatchWriter::for_table(&table)?;

    while let Some(Ok(batch)) = reader.next() {
        let batch = augment_with_ds(&batch);
        debug!("Augmented: {batch:?}");
        writer.write(batch).await?;
    }

    let version = writer.flush_and_commit(&mut table).await?;
    info!("Successfully flushed v{version} to Delta table");
    // Reload the table to make sure we have the latest version to checkpoint
    let _ = table.load().await;
    if table.version() == version {
        match deltalake::checkpoints::create_checkpoint(&table).await {
            Ok(_) => info!("Successfully created checkpoint"),
            Err(e) => {
                error!("Failed to create checkpoint for {table:?}: {e:?}")
            }
        }
    } else {
        error!("The table was reloaded to create a checkpoint but a new version already exists!");
    }

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
                    debug!("Deserializing body text and appending to {table_uri}");
                    let table = oxbow::lock::open_table(&table_uri).await?;
                    debug!("{}", &buf);
                    match append_values(table, buf).await {
                        Ok(_) => {}
                        Err(e) => {
                            error!("Failed to append the values to configured Delta table: {e:?}");
                            return Err(e);
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

///
/// Augment the given [RecordBatch] with another column that represents `ds`, treated
/// as the date-stampe, e.g. `2024-01-01`.
///
fn augment_with_ds(batch: &RecordBatch) -> RecordBatch {
    use deltalake::arrow::array::{Array, StringArray};

    // If the schema doesn't have a `ds` then don't try to add one
    if batch.column_by_name("ds").is_none() {
        info!("The schema of the destination table doesn't have `ds` so not adding the value");
        return batch.clone();
    }

    let mut ds = vec![];
    let now = Utc::now();
    let datestamp = now.format("%Y-%m-%d").to_string();

    for _index in 0..batch.num_rows() {
        ds.push(datestamp.clone());
    }

    let mut columns: Vec<Arc<dyn Array>> = vec![];

    for field in &batch.schema().fields {
        if field.name() != "ds" {
            if let Some(column) = batch.column_by_name(field.name()) {
                columns.push(column.clone());
            }
        } else {
            columns.push(Arc::new(StringArray::from(ds.clone())));
        }
    }

    RecordBatch::try_new(batch.schema(), columns).expect("Failed to transpose `ds` onto batch")
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::*;

    async fn setup_test_table() -> DeltaResult<DeltaTable> {
        DeltaOps::try_from_uri("memory://")
            .await?
            .create()
            .with_table_name("test")
            .with_column(
                "id",
                SchemaDataType::primitive("integer".into()),
                true,
                None,
            )
            .with_column("ds", SchemaDataType::primitive("string".into()), true, None)
            .with_column(
                "name",
                SchemaDataType::primitive("string".into()),
                true,
                None,
            )
            .await
    }

    #[tokio::test]
    async fn test_append_values() -> DeltaResult<()> {
        let table = setup_test_table().await?;

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

    #[tokio::test]
    async fn test_augment_with_ds() -> DeltaResult<()> {
        let table = setup_test_table().await?;
        let jsonl = r#"
            {"id" : 0, "name" : "Ben"}
            {"id" : 1, "name" : "Chris"}
        "#;

        let cursor = Cursor::new(jsonl);
        let schema = table.get_schema()?;
        let schema = ArrowSchema::try_from(schema)?;
        let mut reader = ReaderBuilder::new(schema.into()).build(cursor).unwrap();

        while let Some(Ok(batch)) = reader.next() {
            let batch = augment_with_ds(&batch);
            println!("{:?}", batch);
            assert_ne!(None, batch.column_by_name("ds"));
        }
        Ok(())
    }
}
