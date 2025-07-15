use chrono::prelude::*;
use deltalake::arrow::array::RecordBatch;
use deltalake::arrow::datatypes::Schema as ArrowSchema;
use deltalake::arrow::error::ArrowError;
use deltalake::arrow::json::reader::ReaderBuilder;
use deltalake::kernel::engine::arrow_conversion::TryIntoArrow;
use deltalake::table::config::TablePropertiesExt;
use deltalake::writer::{DeltaWriter, record_batch::RecordBatchWriter};
use deltalake::{DeltaResult, DeltaTable};

use std::io::Cursor;
use std::sync::Arc;
use tracing::log::*;

///
/// Append an iterator which yields [RecordBatch] onto the given [DeltaTable]
///
/// This fnuciton will produce a single transactional commit onto the table
pub async fn append_batches(
    mut table: DeltaTable,
    batches: impl IntoIterator<Item = Result<RecordBatch, ArrowError>>,
) -> DeltaResult<DeltaTable> {
    let mut writer = RecordBatchWriter::for_table(&table)?;
    let mut written = false;

    trace!("Iterating through batches to write");
    for batch in batches {
        let batch = batch?;
        let batch = augment_with_ds(batch)?;
        debug!("Augmented: {batch:?}");
        writer.write(batch).await?;
        written = true;
    }

    if written {
        let version = writer.flush_and_commit(&mut table).await?;
        info!("Successfully flushed v{version} via append_batches to Delta table");
    } else {
        error!("Failed to write any data files! Cowardly avoiding a Delta commit");
    }

    Ok(table)
}

/// Function responsible for appending string values to an opened [DeltaTable]
pub async fn append_values(
    mut table: DeltaTable,
    values: impl IntoIterator<Item: AsRef<str>>,
) -> DeltaResult<DeltaTable> {
    let schema = table.get_schema()?;
    debug!("Attempting to append values with schema: {schema:?}");
    let schema: ArrowSchema = schema.try_into_arrow()?;

    let mut writer = RecordBatchWriter::for_table(&table)?;
    let mut written = false;

    for value in values {
        let cursor: Cursor<&str> = Cursor::new(value.as_ref());
        let reader = ReaderBuilder::new(schema.clone().into())
            .build(cursor)
            .unwrap();

        for res in reader {
            match res {
                Ok(batch) => {
                    let batch = augment_with_ds(batch)?;
                    debug!("Augmented: {batch:?}");
                    writer.write(batch).await?;
                    written = true;
                }
                Err(e) => {
                    error!("Failed to write a data file: {e:?}");
                    return Err(e.into());
                }
            }
        }
    }

    if written {
        let version = writer.flush_and_commit(&mut table).await?;
        info!("Successfully flushed v{version} to Delta table");
    } else {
        warn!("Failed to write any data files! Cowardly avoiding a Delta commit");
    }
    Ok(table)
}

/// Function responsible for appending values to an opened [DeltaTable]
pub async fn append_jsonl(table: DeltaTable, jsonl: &str) -> DeltaResult<DeltaTable> {
    append_values(table, jsonl.split("\n").map(|m| m.to_string())).await
}

///
/// Augment the given [RecordBatch] with another column that represents `ds`, treated
/// as the date-stampe, e.g. `2024-01-01`.
///
pub fn augment_with_ds(batch: RecordBatch) -> DeltaResult<RecordBatch> {
    use deltalake::arrow::array::{Array, StringArray};

    // If the schema doesn't have a `ds` then don't try to add one
    if batch.column_by_name("ds").is_none() {
        info!("The schema of the destination table doesn't have `ds` so not adding the value");
        return Ok(batch);
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

    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::kernel::DataType;
    use deltalake::*;
    use std::num::NonZero;

    async fn setup_test_table() -> DeltaResult<DeltaTable> {
        DeltaOps::try_from_uri("memory://")
            .await?
            .create()
            .with_table_name("test")
            .with_column("id", DataType::INTEGER, true, None)
            .with_column("ds", DataType::STRING, true, None)
            .with_column("name", DataType::STRING, true, None)
            .await
    }

    #[tokio::test]
    async fn test_append_batches() -> DeltaResult<()> {
        use std::fs::File;
        use std::io::BufReader;

        let table = DeltaOps::try_from_uri("memory://")
            .await?
            .create()
            .with_table_name("test")
            .with_column("current", DataType::BOOLEAN, true, None)
            .with_column("ds", DataType::STRING, true, None)
            .with_column("description", DataType::STRING, true, None)
            .await?;

        let buf = File::open("../../tests/data/senators.jsonl")?;
        let reader = BufReader::new(buf);
        let schema: ArrowSchema = table.snapshot()?.schema().try_into_arrow()?;

        let json = deltalake::arrow::json::ReaderBuilder::new(schema.into()).build(reader)?;

        let table = append_batches(table, json).await?;
        assert_eq!(table.version(), Some(1));
        Ok(())
    }

    #[tokio::test]
    async fn test_append_jsonl() -> DeltaResult<()> {
        let table = setup_test_table().await?;

        let jsonl = r#"
            {"id" : 0, "name" : "Ben"}
            {"id" : 1, "name" : "Chris"}
        "#;
        let table = append_jsonl(table, jsonl)
            .await
            .expect("Failed to do nothing");

        assert_eq!(table.version(), Some(1));
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
        let schema = table.snapshot()?.schema();
        let schema: ArrowSchema = schema.try_into_arrow()?;
        let mut reader = ReaderBuilder::new(schema.into()).build(cursor).unwrap();

        while let Some(Ok(batch)) = reader.next() {
            let batch = augment_with_ds(batch)?;
            println!("{batch:?}");
            assert_ne!(None, batch.column_by_name("ds"));
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_append_values() -> DeltaResult<()> {
        let table = setup_test_table().await?;

        let values: Vec<String> = vec![
            r#"{"id" : 0, "name" : "Ben"}"#.into(),
            r#"{"id" : 1, "name" : "Chris"}"#.into(),
        ];
        let table = append_values(table, values)
            .await
            .expect("Failed to do nothing");

        assert_eq!(table.version(), Some(1));
        Ok(())
    }

    #[tokio::test]
    async fn test_append_values_checkpoint() -> DeltaResult<()> {
        let mut table = setup_test_table().await?;

        for _ in 0..101 {
            let values: Vec<String> = vec![
                r#"{"id" : 0, "name" : "Ben"}"#.into(),
                r#"{"id" : 1, "name" : "Chris"}"#.into(),
            ];
            table = append_values(table, values)
                .await
                .expect("Failed to do nothing");
        }

        if let Some(state) = table.state.as_ref() {
            // The default is expected to be 100
            assert_eq!(
                NonZero::new(100).unwrap(),
                state.table_config().checkpoint_interval()
            );
        }

        use deltalake::Path;
        let checkpoint = table
            .object_store()
            .head(&Path::from("_delta_log/_last_checkpoint"))
            .await?;

        assert_ne!(0, checkpoint.size);
        assert_eq!(table.version(), Some(101));
        Ok(())
    }
}
