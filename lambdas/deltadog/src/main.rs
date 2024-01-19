use aws_lambda_events::event::sqs::SqsEvent;
use dipstick::*;
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use tracing::log::*;
use url::Url;

use oxbow_lambda_shared::*;

async fn function_handler(event: LambdaEvent<SqsEvent>) -> Result<(), Error> {
    let statsd = std::env::var("STATSD_HOST").unwrap_or("127.0.0.1:8125".into());
    let prefix = std::env::var("STATSD_PREFIX").unwrap_or(String::new());

    info!("Configured for statsd events on {statsd} with the prefix of \"{prefix}\"");
    let metrics = Statsd::send_to(statsd)?.named(prefix).metrics();

    debug!("Receiving event: {event:?}");
    let records = s3_from_sqs(event.payload)?;
    debug!("processing records: {records:?}");
    let records = records_with_url_decoded_keys(&records);
    let by_table = objects_by_table(&records);

    if by_table.is_empty() {
        info!("No elligible events found, exiting early");
        return Ok(());
    }

    for table_name in by_table.keys() {
        match deltalake::open_table(table_name).await {
            Ok(table) => {
                debug!("Opened table {table:?} for metrics tracking");
                let metric_name = url_to_metric_name(table_name);

                let version = metrics.gauge(&format!("deltadog.{metric_name}.version"));
                let table_version = table.version();
                metrics
                    .observe(version, move |_| {
                        table_version
                            .try_into()
                            .expect("Failed to convert i64 to isize")
                    })
                    .on_flush();
            }
            Err(e) => {
                error!("Failed to open Delta table: {e:?}");
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        // disable printing the name of the module in every log line.
        .with_target(false)
        // disabling time is handy because CloudWatch will add the ingestion time.
        .without_time()
        .init();

    info!("Starting deltadog");
    run(service_fn(function_handler)).await
}

/// Trying to convert the given url into a statsd compatible metric string
fn url_to_metric_name<S: Into<String>>(url: S) -> String {
    let url = Url::parse(&url.into())
        .expect("Failed to turn a URL into a metric because it's not a real URL!");

    let hostname: String = match url.host_str() {
        Some(hostname) => hostname.to_string(),
        None => String::new(),
    };
    format!(
        "{}{}",
        hostname.replace('-', ""),
        url.path().replace('-', "").replace('/', "_")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s3_url_to_metric_name() {
        let s = "s3://mybucket-name/databases/mylogs/my-table";
        let metric = "mybucketname_databases_mylogs_mytable";
        assert_eq!(url_to_metric_name(s), metric);
    }
}
