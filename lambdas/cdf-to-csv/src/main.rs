//!
//! This lambda function should be triggered by S3 Bucket Notifications on Delta tables and will
//! produce CSV files to ingest into Aurora for Change Data Feeds

use aws_lambda_events::event::sqs::SqsEvent;
use deltalake::datafusion::dataframe::DataFrameWriteOptions;
use deltalake::datafusion::prelude::*;
use deltalake::delta_datafusion::DeltaCdfTableProvider;
use deltalake::logstore::object_store::ObjectStore;
use deltalake::logstore::object_store::aws::AmazonS3Builder;
use deltalake::logstore::object_store::path::Path;
use deltalake::logstore::object_store::prefix::PrefixStore;
use deltalake::{DeltaOps, DeltaResult};
use lambda_runtime::{Error, LambdaEvent, run, service_fn, tracing};
use oxbow_lambda_shared::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::log::*;
use url::Url;

#[tokio::main]
async fn main() -> Result<(), Error> {
    deltalake::aws::register_handlers(None);
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        // disable printing the name of the module in every log line.
        .with_target(false)
        // disabling time is handy because CloudWatch will add the ingestion time.
        .without_time()
        .init();

    info!("Starting cdf-to-csv");
    run(service_fn(function_handler)).await
}

async fn function_handler(event: LambdaEvent<SqsEvent>) -> DeltaResult<(), Error> {
    debug!("Receiving event: {:?}", event);
    let destination = std::env::var("CSV_OUTPUT_URL").expect("CSV_OUTPUT_URL not set");

    let triggers = triggers_from(event.payload)?;
    debug!("Triggered by: {triggers:?}");

    for trigger in triggers {
        let mut should_process = false;
        // Since the file triggers should include some other piece of information, only bother
        // loading the table if there has been a transaction log modification
        for change in trigger.changes() {
            if let ChangeType::TransactionLog { version: _ } = change.what {
                should_process = true;
                break;
            }
        }

        if !should_process {
            continue;
        }

        if let (Some(mut min), Some(max)) = (trigger.smallest_version(), trigger.largest_version())
        {
            // Creating a new [SessionContext] for every trigger to make sure the namespace inside
            // the context is unique to a triggered table
            let ctx = SessionContext::new();
            let store: Arc<dyn ObjectStore> = Arc::new(
                AmazonS3Builder::from_env()
                    .with_url(&destination)
                    .build()
                    .expect("Failed to create an output object_store"),
            );

            let table = deltalake::open_table(trigger.location().as_str()).await?;
            info!(
                "Loaded a table for {} at version {:?}",
                trigger.location().as_str(),
                table.version()
            );

            // Min and max are the same when just one transaction is in the trigger, in that case,
            // make min N-1
            if min == max {
                min = max - 1;
            }

            // Always look at the last version
            let cdf = DeltaOps::from(table)
                .load_cdf()
                .with_starting_version(min)
                .with_ending_version(max);
            let provider = DeltaCdfTableProvider::try_new(cdf)?;
            ctx.register_table("cdf", Arc::new(provider))?;

            // The prefix used should be bespoke for every table trigger and contain the max
            // version processed to make sure there are no conflicts between invocations
            let prefix = format!("{}/{}", trigger.location().path(), max);
            let store: Arc<dyn ObjectStore> = Arc::new(PrefixStore::new(store, prefix));
            // Registering our destinations into the [SessionContext] so that write_csv() can write properly
            let insert_store = Arc::new(PrefixStore::new(store.clone(), "inserts"));
            let delete_store = Arc::new(PrefixStore::new(store.clone(), "deletes"));
            ctx.register_object_store(&Url::parse("cdfo://inserts").unwrap(), insert_store.clone());
            ctx.register_object_store(&Url::parse("cdfo://deletes").unwrap(), delete_store.clone());

            let inserts = retrieve_inserts(&ctx).await?;
            let deletes = retrieve_deletes(&ctx).await?;

            // write_csv will return a Vec,RecordBatch> which we can use for some rudimentary
            // statistics
            let inserts = inserts
                .write_csv("cdfo://inserts", DataFrameWriteOptions::default(), None)
                .await?;
            let deletes = deletes
                .write_csv("cdfo://deletes", DataFrameWriteOptions::default(), None)
                .await?;

            let completion = Completion {
                inserts: inserts.iter().map(|rb| rb.num_rows()).sum(),
                deletes: deletes.iter().map(|rb| rb.num_rows()).sum(),
            };

            mark_complete(store.clone(), &completion).await?;
        } else {
            warn!("Invoked but didn't find min/max trigger versions, something is fishy!");
        }
    }
    Ok(())
}

/// This function must be given an existing Apache Datafusiojn [SessionContext] and will produce
/// a simplistic dataframe with _only_ the changed data from the [DeltaCdfTableProvider].
///
/// That is to say, none of the underscore extra columns or metadata
///
/// NOTE: Right now this is only handling inserts/updates but **not** deletes. Deletes should be
/// handled somewhat differently
async fn retrieve_inserts(ctx: &SessionContext) -> DeltaResult<DataFrame> {
    let df = ctx
        .sql("SELECT * FROM cdf WHERE _change_type IN ('insert', 'update_postimage')")
        .await?;
    Ok(df.drop_columns(&[
        "_change_type",
        "_commit_version",
        "_commit_timestamp",
        "_change_version",
    ])?)
}

/// Compute the deletes from the change data feed associated with the [SessionContext]
async fn retrieve_deletes(ctx: &SessionContext) -> DeltaResult<DataFrame> {
    let df = ctx
        .sql("SELECT * FROM cdf WHERE _change_type IN ('delete')")
        .await?;
    Ok(df.drop_columns(&[
        "_change_type",
        "_commit_version",
        "_commit_timestamp",
        "_change_version",
    ])?)
}

/// Write a completion file to the given object store.
///
/// This is expected to be the prefix store associated with a werite
async fn mark_complete(store: Arc<dyn ObjectStore>, completion: &Completion) -> DeltaResult<()> {
    // Write a sentinel file once the writes have completed successfully
    store
        .put(
            &Path::from("cdf-completion.json"),
            serde_json::to_string(completion)
                .expect("Failed to serialize Completion")
                .into(),
        )
        .await?;
    Ok(())
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct Completion {
    inserts: usize,
    deletes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::logstore::object_store::memory::InMemory;
    use deltalake::logstore::object_store::path::Path;
    use futures::StreamExt;

    use deltalake::datafusion::{
        common::assert_batches_sorted_eq, dataframe::DataFrameWriteOptions,
    };
    use deltalake::operations::load_cdf::CdfLoadBuilder;

    async fn cdf_test_setup() -> DeltaResult<(SessionContext, CdfLoadBuilder)> {
        let table = deltalake::open_table("../../tests/data/hive/checkpoint-cdf-table/").await?;
        let cdf = DeltaOps::from(table).load_cdf().with_starting_version(3);
        let ctx = SessionContext::new();
        Ok((ctx, cdf))
    }

    #[tokio::test]
    async fn test_mark_complete() -> DeltaResult<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let completion = Completion {
            inserts: 1,
            deletes: 0,
        };
        mark_complete(store.clone(), &completion).await?;
        let _ = store.head(&Path::from("cdf-completion.json")).await?;

        let result = store.get(&Path::from("cdf-completion.json")).await?;
        let bytes = result.bytes().await?;
        let s = String::from_utf8(bytes.to_vec()).expect("Failed to convert buffer");
        let received: Completion = serde_json::from_str(&s)?;

        assert_eq!(completion, received);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_cdf_deletes() -> DeltaResult<()> {
        let (ctx, cdf) = cdf_test_setup().await?;
        let provider = DeltaCdfTableProvider::try_new(cdf)?;
        ctx.register_table("cdf", Arc::new(provider))?;
        let df = retrieve_deletes(&ctx).await?;

        assert_batches_sorted_eq!(
            [
                "+----+--------+------------+",
                "| id | name   | birthday   |",
                "+----+--------+------------+",
                "| 7  | Dennis | 2023-12-29 |",
                "+----+--------+------------+",
            ],
            &df.collect().await?
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_read_cdf() -> DeltaResult<()> {
        let (ctx, cdf) = cdf_test_setup().await?;
        let provider = DeltaCdfTableProvider::try_new(cdf)?;
        ctx.register_table("cdf", Arc::new(provider))?;
        let df = retrieve_inserts(&ctx).await?;

        assert_batches_sorted_eq!(
            [
                "+----+--------+------------+",
                "| id | name   | birthday   |",
                "+----+--------+------------+",
                "| 13 | Ryan   | 2023-12-22 |",
                "| 14 | Zach   | 2023-12-25 |",
                "| 12 | Nick   | 2023-12-29 |",
                "| 11 | Ossama | 2024-12-30 |",
                "| 12 | Ossama | 2024-12-30 |",
                "| 15 | Zach   | 2023-12-25 |",
                "| 14 | Ryan   | 2023-12-22 |",
                "| 13 | Nick   | 2023-12-29 |",
                "+----+--------+------------+",
            ],
            &df.collect().await?
        );
        Ok(())
    }

    /// This test will ensure that CSV output can be correctly written to the registered object
    /// store
    #[tokio::test]
    async fn test_write_csv() -> DeltaResult<()> {
        let (ctx, cdf) = cdf_test_setup().await?;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let insert_store = Arc::new(PrefixStore::new(store.clone(), "inserts"));
        let delete_store = Arc::new(PrefixStore::new(store.clone(), "deletes"));
        ctx.register_object_store(&Url::parse("cdfo://inserts").unwrap(), insert_store.clone());
        ctx.register_object_store(&Url::parse("cdfo://deletes").unwrap(), delete_store.clone());

        let mut stream = store.list(None);
        while let Some(Ok(_entry)) = stream.next().await {
            unreachable!("The InMemory object store should be empty before the test begins");
        }

        let provider = DeltaCdfTableProvider::try_new(cdf)?;
        ctx.register_table("cdf", Arc::new(provider))?;
        let change_data = retrieve_inserts(&ctx).await?;
        let deletes = retrieve_deletes(&ctx).await?;
        change_data
            .write_csv("cdfo://inserts", DataFrameWriteOptions::default(), None)
            .await?;
        deletes
            .write_csv("cdfo://deletes", DataFrameWriteOptions::default(), None)
            .await?;

        let mut stream = store.list(None);
        let mut insert_found = false;
        let mut delete_found = false;
        while let Some(Ok(entry)) = stream.next().await {
            println!("entry: {entry:?}");
            if entry.location.prefix_matches(&Path::from("deletes")) {
                delete_found = true;
            }
            if entry.location.prefix_matches(&Path::from("inserts")) {
                insert_found = true;
            }
        }

        assert!(delete_found, "No delete was found to be written");
        assert!(insert_found, "No insert was found to be written");
        Ok(())
    }
}
