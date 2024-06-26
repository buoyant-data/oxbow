use aws_lambda_events::s3::S3EventRecord;
use aws_lambda_events::sqs::SqsEvent;
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use regex::Regex;
use tracing::log::*;

use oxbow_lambda_shared::*;

/// Environment variable which defines the regular express to process a database and table from the
/// key names
const GLUE_REGEX_ENV: &str = "GLUE_PATH_REGEX";
/// Environment variable of the Athena workgroup to use when creating external tables in the
/// catalog
const ATHENA_WORKGROUP_ENV: &str = "ATHENA_WORKGROUP";
/// Environment variable for the Data Source to use in Athena, this sohuld be a Glue catalog
/// configured Data Source for Athena
const ATHENA_DATA_SOURCE_ENV: &str = "ATHENA_DATA_SOURCE";
const QUERY_TIMEOUT_SECS: i32 = 60;

#[derive(Debug, PartialEq)]
struct GlueTable {
    name: String,
    database: String,
}

async fn function_handler(event: LambdaEvent<SqsEvent>) -> Result<(), Error> {
    info!("Invoking glue-create");
    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let glue = aws_sdk_glue::Client::new(&config);
    // The athena client is used for creating the table in the catalog with the
    // appropriate metadata.
    //
    // Rather than processing the Delta Table ourselves, Athena will:
    //
    //      Unlike traditional Hive tables, Delta Lake table metadata are inferred from
    //      the Delta Lake transaction log and synchronized directly to AWS Glue.
    //
    // <https://docs.aws.amazon.com/athena/latest/ug/delta-lake-tables.html>
    let athena = aws_sdk_athena::Client::new(&config);

    let _ = std::env::var(GLUE_REGEX_ENV)
        .expect("The Lambda must have GLUE_PATH_REGEX defined in the environment");
    let _ = std::env::var(ATHENA_WORKGROUP_ENV)
        .expect("The Lambda must have ATHENA_WORKGROUP defined in the environment");
    let _ = std::env::var(ATHENA_DATA_SOURCE_ENV)
        .expect("The Lambda must have ATHENA_DATA_SOURCE defined in the environment");

    let records = match std::env::var("UNWRAP_SNS_ENVELOPE") {
        Ok(_) => s3_from_sns(event.payload)?,
        Err(_) => s3_from_sqs(event.payload)?,
    };
    debug!("processing records: {records:?}");

    for record in records {
        process_record(record, &glue, &athena).await?;
    }

    Ok(())
}

async fn process_record(
    record: S3EventRecord,
    glue: &aws_sdk_glue::Client,
    athena: &aws_sdk_athena::Client,
) -> Result<(), Error> {
    debug!("Parsing keys out of {record:?}");
    let bucket = record
        .s3
        .bucket
        .name
        .expect("Failed to get bucket name out of payload");
    let key = record
        .s3
        .object
        .key
        .expect("Failed to get object key out of payload");

    let pattern = std::env::var(GLUE_REGEX_ENV)
        .expect("Must have GLUE_PATH_REGEX defined in the environment");
    let pattern = Regex::new(&pattern).expect("Failed to compile the defined GLUE_PATH_REGEX");

    // Map the key key to a GlueTable properly, currently only bucket notifications from the
    // aurora_raw/ space is supported.
    let glue_table = extract_table_from_key(&key, &pattern).expect(&format!(
        "Expected to be able to parse out a table name: {key:?}"
    ));

    match glue
        .get_table()
        .database_name(&glue_table.database)
        .name(&glue_table.name)
        .send()
        .await
    {
        Ok(_output) => {
            info!("The table {glue_table:?} already exists, nice.");
            Ok(())
        }
        Err(_e) => {
            info!("The table {glue_table:?} does not exist, creating it!");
            // Determine if the key is pointing to a delta table and then start loading
            if let Some(table_path) = key_is_delta_table(&key) {
                let uri = format!("s3://{bucket}/{table_path}");
                let work_group =
                    std::env::var(ATHENA_WORKGROUP_ENV).expect("Failed to get work group");
                let catalog = match std::env::var(ATHENA_DATA_SOURCE_ENV) {
                    Ok(val) => Some(val.to_string()),
                    Err(_) => None,
                };

                match glue.get_database().name(&glue_table.database).send().await {
                    Ok(_) => debug!("Database {} already exists. Nice", glue_table.database),
                    Err(error) => {
                        let get_db_error = error.into_service_error();
                        if get_db_error.is_entity_not_found_exception() {
                            debug!(
                                "Database {} doesn't exist. Creating...",
                                glue_table.database
                            );
                            let input = aws_sdk_glue::types::DatabaseInput::builder()
                                .name(&glue_table.database)
                                .build()?;

                            glue.create_database().database_input(input).send().await?;
                            debug!("Database {} created.", glue_table.database)
                        } else {
                            return Err(get_db_error.into());
                        }
                    }
                }

                let exec_context = aws_sdk_athena::types::QueryExecutionContext::builder()
                    .database(glue_table.database)
                    .set_catalog(catalog)
                    .build();

                let query = format!(
                    r#"CREATE EXTERNAL TABLE
                                        {0}
                                        LOCATION '{uri}'
                                        TBLPROPERTIES ('table_type' = 'DELTA')"#,
                    glue_table.name
                );
                info!("Triggering athena with: {query}");
                let result = athena
                    .start_query_execution()
                    .work_group(work_group)
                    .query_execution_context(exec_context)
                    .query_string(&query)
                    .send()
                    .await?;

                debug!("Query response: {result:?}");
                if let Some(query_execution_id) = result.query_execution_id {
                    debug!("Waiting for results from Athena");

                    for _i in 0..QUERY_TIMEOUT_SECS {
                        let execution = athena
                            .get_query_execution()
                            .query_execution_id(&query_execution_id)
                            .send()
                            .await?;

                        if let Some(execution) = execution.query_execution {
                            if let Some(status) = execution.status {
                                debug!("Current status of query execution: {status:?}");
                                match status.state {
                                    Some(aws_sdk_athena::types::QueryExecutionState::Succeeded) => {
                                        break;
                                    }
                                    Some(aws_sdk_athena::types::QueryExecutionState::Failed) => {
                                        error!("Query failed! {status:?}");
                                        return Err(
                                            "Failed to query Athena for some reason!".into()
                                        );
                                    }
                                    // Safely ignore Running, Queued, and other statuses
                                    _ => {}
                                }
                            }
                        }

                        debug!("Incompleted query, waiting a sec");
                        let _ = tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }

                Ok(())
            } else {
                error!("Invoked with a key that didn't match a delta table! {key}");
                Ok(())
            }
        }
    }
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

    run(service_fn(function_handler)).await
}

///
/// Use the `GLUE_TABLE_REGEX` to identify whether the key is a consistent with a table
///
fn extract_table_from_key(key: &str, regex: &Regex) -> Option<GlueTable> {
    if let Some(captured) = regex.captures(key) {
        let database = captured.name("database");
        let table = captured.name("table");
        if database.is_some() && table.is_some() {
            let database = database.unwrap().as_str();
            let name = table.unwrap().as_str();
            return Some(GlueTable {
                database: database.into(),
                name: name.into(),
            });
        }
    }
    None
}

/// Match the key against a regular expression of what a delta table looks like
/// This will make it easier to figure out whether the modified bucket is a Delta Table so we can
/// load it and look for it in the Glue catalog
///
/// Returns the key name for the delta table so you can open it
fn key_is_delta_table(key: &str) -> Option<String> {
    let regex = Regex::new(r#"(?P<root>.*)\/_delta_log\/.*\.json"#)
        .expect("Failed to compile the regular expression");

    if let Some(caps) = regex.captures(key) {
        if let Some(root) = caps.name("root") {
            return Some(root.as_str().into());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_invalid_table() {
        let key = "data-lake/foo.parquet";
        let pattern = Regex::new("").expect("Failed to compile regex");
        assert_eq!(None, extract_table_from_key(key, &pattern));
    }

    #[test]
    fn test_extract_valid_table() {
        let key = r#"data-lake/testdb/testtable/_delta_log/0000000000.json"#;
        let pattern = r#"data-lake\/(?P<database>\w+)\/(?P<table>\w+\.?\w+)\/_delta_log\/.*.json"#;
        let pattern = Regex::new(pattern).expect("Failed to compile regex");
        let result = extract_table_from_key(key, &pattern);
        assert!(result.is_some());
        let info = result.unwrap();
        assert_eq!(info.database, "testdb");
        assert_eq!(info.name, "testtable");
    }

    #[test]
    fn test_unrelated_key() {
        let key = "prefix/some/key/that/don/match.parquet";
        let result = key_is_delta_table(key);
        assert!(result.is_none());
    }

    #[test]
    fn test_matching_key() {
        let key = "prefix/testdb/testtable/_delta_log/00000000000000000000.json";
        let result = key_is_delta_table(key);
        assert!(result.is_some());
        if let Some(root) = result {
            assert_eq!(root, "prefix/testdb/testtable");
        }
    }
}
