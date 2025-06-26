//!
//! This lambda function should be triggered by S3 Bucket Notifications on Delta tables and will
//! produce CSV files to ingest into Aurora for Change Data Feeds

use aws_lambda_events::event::sqs::SqsEvent;
use deltalake::datafusion::dataframe::DataFrameWriteOptions;
use deltalake::datafusion::prelude::*;
use deltalake::delta_datafusion::DeltaCdfTableProvider;
use deltalake::{DeltaOps, DeltaResult};
use lambda_runtime::{run, service_fn, tracing, Error, LambdaEvent};
use object_store::prefix::PrefixStore;
use oxbow_lambda_shared::*;
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
            let store = object_store::aws::AmazonS3Builder::from_env()
                .with_url(&destination)
                .build()
                .expect("Failed to create an output object_store");

            let table = deltalake::open_table(trigger.location().as_str()).await?;
            info!(
                "Loaded a table for {} at version {}",
                trigger.location().as_str(),
                table.version()
            );

            // Min and max are the same when just one transaction is in the trigger, in that case,
            // make min N-1
            if min == max {
                min = max - 1;
            }

            // The prefix used should be bespoke for every table trigger and contain the max
            // version processed to make sure there are no conflicts between invocations
            let prefix = format!("{}/{}/inserts", trigger.location().path(), max);
            let store = Arc::new(PrefixStore::new(store, prefix));
            // Registering our destination into the [SessionContext] so that write_csv() can just use our
            // cdf output scheme for writing into S3
            ctx.register_object_store(
                &Url::parse("cdfo://").expect("Failed to parse internal URI scheme"),
                store.clone(),
            );

            // Always look at the last version
            let cdf = DeltaOps::from(table)
                .load_cdf()
                .with_starting_version(min)
                .with_ending_version(max);
            let provider = DeltaCdfTableProvider::try_new(cdf)?;

            let change_data = retrieve_change_data(&ctx, provider).await?;
            change_data
                .write_csv("cdfo://", DataFrameWriteOptions::default(), None)
                .await?;
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
async fn retrieve_change_data(
    ctx: &SessionContext,
    cdf_provider: DeltaCdfTableProvider,
) -> DeltaResult<DataFrame> {
    ctx.register_table("cdf", Arc::new(cdf_provider))?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use object_store::ObjectStore;

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
    async fn test_read_cdf() -> DeltaResult<()> {
        let (ctx, cdf) = cdf_test_setup().await?;
        let provider = DeltaCdfTableProvider::try_new(cdf)?;
        let df = retrieve_change_data(&ctx, provider).await?;

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
        let store = Arc::new(object_store::memory::InMemory::new());
        ctx.register_object_store(&Url::parse("cdfo://").unwrap(), store.clone());

        let mut stream = store.list(None);
        while let Some(Ok(_entry)) = stream.next().await {
            unreachable!("The InMemory object store should be empty before the test begins");
        }

        let provider = DeltaCdfTableProvider::try_new(cdf)?;
        let change_data = retrieve_change_data(&ctx, provider).await?;
        change_data
            .write_csv("cdfo://", DataFrameWriteOptions::default(), None)
            .await?;

        let mut stream = store.list(None);
        let mut output_found = false;
        while let Some(Ok(entry)) = stream.next().await {
            println!("entry: {entry:?}");
            output_found = true;
        }

        assert!(output_found, "Nothing was found in the prefix!");
        Ok(())
    }
}
