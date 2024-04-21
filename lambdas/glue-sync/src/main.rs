///
/// The glue-sync Lambda is triggered off of S3 Event Notifications and can read a Delta table
/// and attempt to add additional columns to the Glue Data Catalog schema
///
use aws_lambda_events::event::sqs::SqsEvent;
use aws_sdk_glue::types::{Column, StorageDescriptor, Table, TableInput};
use deltalake::{DeltaTable, SchemaDataType};
use lambda_runtime::{run, service_fn, tracing, Error, LambdaEvent};
use regex::Regex;
use tracing::log::*;

use oxbow_lambda_shared::*;
use std::collections::HashMap;

/// Environment variable which defines the regular express to process a database and table from the
/// key names
const GLUE_REGEX_ENV: &str = "GLUE_PATH_REGEX";

/// Helper struct to carry around a glue table's
/// database and table name
#[derive(Debug, PartialEq)]
struct GlueTable {
    name: String,
    database: String,
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
    let _ = std::env::var(GLUE_REGEX_ENV)
        .expect("The Lambda must have GLUE_PATH_REGEX defined in the environment");

    info!("Starting the Lambda runtime");
    info!("Starting glue-sync");

    run(service_fn(function_handler)).await
}

/// Main lambda function handler
async fn function_handler(event: LambdaEvent<SqsEvent>) -> Result<(), Error> {
    let pattern = std::env::var(GLUE_REGEX_ENV)
        .expect("Must have GLUE_PATH_REGEX defined in the environment");
    let pattern = Regex::new(&pattern).expect("Failed to compile the defined GLUE_PATH_REGEX");

    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let client = aws_sdk_glue::Client::new(&config);

    debug!("Receiving event: {:?}", event);

    let records = match std::env::var("UNWRAP_SNS_ENVELOPE") {
        Ok(_) => s3_from_sns(event.payload)?,
        Err(_) => s3_from_sqs(event.payload)?,
    };
    debug!("processing records: {records:?}");

    for record in records {
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

        // Map the key key to a GlueTable properly, currently only bucket notifications from the
        // aurora_raw/ space is supported.
        let parsed_table = extract_table_from_key(&key, &pattern).expect(&format!(
            "Expected to be able to parse out a table name: {key:?}"
        ));

        if let Some(glue_table) =
            fetch_table(&parsed_table.database, &parsed_table.name, &client).await
        {
            if let Some(table_path) = key_is_delta_table(&key) {
                debug!("loading table from {table_path}");
                let uri = format!("s3://{bucket}/{table_path}");
                let delta_table = deltalake::open_table(&uri).await?;

                if let Some(descriptor) = storage_descriptor_from(&delta_table, &glue_table) {
                    let parameters = match glue_table.parameters {
                        None => None,
                        Some(ref original) => {
                            let mut updated = original.clone();
                            updated.insert(
                                "delta.lastUpdateVersion".into(),
                                delta_table.version().to_string(),
                            );
                            Some(updated)
                        }
                    };
                    let update = client
                        .update_table()
                        .database_name(&parsed_table.database)
                        .table_input(
                            TableInput::builder()
                                .set_description(glue_table.description)
                                .name(glue_table.name)
                                .set_owner(glue_table.owner)
                                .set_parameters(parameters)
                                .set_partition_keys(glue_table.partition_keys)
                                .set_retention(Some(glue_table.retention))
                                .set_table_type(glue_table.table_type)
                                .set_target_table(glue_table.target_table)
                                .storage_descriptor(descriptor)
                                .build()
                                .expect("Failed to build table input"),
                        )
                        .send()
                        .await
                        .expect("Failed to update table");
                    info!(
                        "Update table for {}.{} sent: {update:?}",
                        parsed_table.database, parsed_table.name
                    );
                } else {
                    debug!("No new StorageDescriptor created, nothing to do!");
                }
            }
        } else {
            info!(
                "The table {:?} did not exist in Glue already, skipping",
                parsed_table
            );
        }
    }

    Ok(())
}

///
/// Compute the new [StorageDescriptor] for the given [DeltaTable]
///
fn storage_descriptor_from(
    delta_table: &DeltaTable,
    glue_table: &Table,
) -> Option<StorageDescriptor> {
    if let Some(descriptor) = glue_table.storage_descriptor() {
        if let Some(cols) = &descriptor.columns {
            let delta_columns_for_glue = glue_columns_for(delta_table);
            let m: HashMap<String, Option<String>> =
                HashMap::from_iter(cols.iter().map(|c| (c.name.clone(), c.r#type.clone())));
            let delta_columns_for_glue: Vec<Column> = delta_columns_for_glue
                .into_iter()
                .filter(|c| !m.contains_key(&c.name))
                .collect();

            if delta_columns_for_glue.is_empty() {
                debug!("There are no columns to add to glue, bailing");
                return None;
            }
            let descriptor = StorageDescriptor::builder()
                .set_location(descriptor.location.clone())
                .set_input_format(descriptor.input_format.clone())
                .set_output_format(descriptor.output_format.clone())
                .set_bucket_columns(descriptor.bucket_columns.clone())
                .set_serde_info(descriptor.serde_info.clone())
                .set_sort_columns(descriptor.sort_columns.clone())
                .set_parameters(descriptor.parameters.clone())
                .set_additional_locations(descriptor.additional_locations.clone())
                .set_columns(Some([cols.clone(), delta_columns_for_glue].concat()))
                .build();
            debug!("Computed descriptor: {descriptor:?}");
            return Some(descriptor);
        }
    }
    None
}

/// Utility function to attempt to fetch a table from Glue with the provided [aws_sdk_glue::Client]
async fn fetch_table(database: &str, table: &str, client: &aws_sdk_glue::Client) -> Option<Table> {
    let response = client
        .get_table()
        .database_name(database)
        .name(table)
        .send()
        .await;

    match response {
        Ok(output) => {
            return output.table;
        }
        Err(err) => match err {
            aws_sdk_glue::error::SdkError::ServiceError(e) => match e.err() {
                aws_sdk_glue::operation::get_table::GetTableError::EntityNotFoundException(_e) => {
                    return None;
                }
                inner => {
                    error!("Unexpected error while looking up {database}.{table}: {inner:?}");
                }
            },
            _ => {
                error!("Unexpected fail oh no");
            }
        },
    }
    None
}

/// Return the primitive type mapping for glue
///
/// This is a separate function to accommodate reuse for arrays and structs
fn glue_type_for(delta_type_name: &str) -> String {
    match delta_type_name {
        "integer" => "int".into(),
        "long" => "bigint".into(),
        "short" => "smallint".into(),
        "byte" => "char".into(),
        "date" => "string".into(),
        _ => delta_type_name.into(),
    }
}

/// Convert a given [SchemaDataType] to the Glue type equivalent
///
/// This calls itself recursively to handle both primitives and structs/maps
fn to_glue_type(data_type: &SchemaDataType) -> String {
    match data_type {
        SchemaDataType::primitive(name) => glue_type_for(name),
        SchemaDataType::r#struct(s) => {
            format!(
                "struct<{}>",
                s.get_fields()
                    .iter()
                    .map(|f| format!("{}:{}", f.get_name(), to_glue_type(f.get_type())))
                    .collect::<Vec<String>>()
                    .join(",")
            )
        }
        SchemaDataType::map(m) => {
            format!(
                "map<{},{}>",
                to_glue_type(m.get_key_type()),
                to_glue_type(m.get_value_type())
            )
        }
        _ => {
            // Default type which hopefully doesn't cause too many issues with unknown types
            "string".into()
        }
    }
}

/// Generate the [Column] for Glue [aws_sdk_glue::types::StorageDescriptor] based on the provided
/// [DeltaTable]
fn glue_columns_for(table: &DeltaTable) -> Vec<Column> {
    if let Ok(schema) = table.get_schema() {
        return schema
            .get_fields()
            .iter()
            .map(|field| {
                Column::builder()
                    .name(field.get_name())
                    .r#type(to_glue_type(field.get_type()))
                    .build()
                    .expect("Failed to build column from Delta schema!")
            })
            .collect();
    }
    vec![]
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

    #[tokio::test]
    async fn test_glue_columns() {
        let table = "../../tests/data/hive/deltatbl-non-partitioned";
        let table = deltalake::open_table(&table)
            .await
            .expect("Failed to open table");

        let columns = glue_columns_for(&table);
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "c1");
        assert_eq!(columns[1].name, "c2");
    }

    /// This function is mostly used as a manual integration test since there are a lot of moving
    /// parts to validate with AWS Glue Data Catalog and it's very very eventually consistent
    #[ignore]
    #[tokio::test]
    async fn test_basic_integration() {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            // disable printing the name of the module in every log line.
            .with_target(false)
            // disabling time is handy because CloudWatch will add the ingestion time.
            .without_time()
            .init();

        let buf = r#"
{
  "Records": [
    {
      "eventVersion": "2.0",
      "eventSource": "aws:s3",
      "awsRegion": "us-east-1",
      "eventTime": "1970-01-01T00:00:00.000Z",
      "eventName": "ObjectCreated:Put",
      "userIdentity": {
        "principalId": "EXAMPLE"
      },
      "requestParameters": {
        "sourceIPAddress": "127.0.0.1"
      },
      "responseElements": {
        "x-amz-request-id": "EXAMPLE123456789",
        "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
      },
      "s3": {
        "s3SchemaVersion": "1.0",
        "configurationId": "testConfigRule",
        "bucket": {
          "name": "oxbow-simple",
          "ownerIdentity": {
            "principalId": "EXAMPLE"
          },
          "arn": "arn:aws:s3:::example-bucket"
        },
        "object": {
          "key": "evolution/cincotest/_delta_log/0000.json",
          "size": 1024,
          "eTag": "0123456789abcdef0123456789abcdef",
          "sequencer": "0A1B2C3D4E5F678901"
        }
      }
    }
  ]
}
        "#;

        use aws_lambda_events::sqs::SqsMessage;
        let message: SqsMessage = SqsMessage {
            body: Some(buf.into()),
            ..Default::default()
        };
        let event = SqsEvent {
            records: vec![message],
        };
        let lambda_event = LambdaEvent::new(event, lambda_runtime::Context::default());
        std::env::set_var(
            GLUE_REGEX_ENV,
            r#"(?P<database>\w+)\/(?P<table>\w+)\/_delta_log\/.*.json"#,
        );
        let result = function_handler(lambda_event).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_extract_invalid_table() {
        let key = "data-lake/foo.parquet";
        let pattern = Regex::new("").expect("Failed to compile regex");
        assert_eq!(None, extract_table_from_key(key, &pattern));
    }

    #[test]
    fn test_extract_valid_table() {
        let key = r#"data-lake/test/my_big_long_table_name/_delta_log/0000000000.json"#;
        let pattern = r#"data-lake\/(?P<database>\w+)\/(?P<table>\w+\.?\w+)\/_delta_log\/.*.json"#;
        let pattern = Regex::new(pattern).expect("Failed to compile regex");
        let result = extract_table_from_key(key, &pattern);
        assert!(result.is_some());

        let info = result.unwrap();
        assert_eq!(info.database, "test");
        assert_eq!(info.name, "my_big_long_table_name");
    }

    #[test]
    fn test_unrelated_key() {
        let key = "beep/some/key/that/don/match.parquet";
        let result = key_is_delta_table(key);
        assert!(result.is_none());
    }

    #[test]
    fn test_matching_key() {
        let key = "data-lake/some-other-prefix/test/my_big_long_table_name/_delta_log/00000000000000000000.json";
        let result = key_is_delta_table(key);
        assert!(result.is_some());
        if let Some(root) = result {
            assert_eq!(
                root,
                "data-lake/some-other-prefix/test/my_big_long_table_name"
            );
        }
    }
}
