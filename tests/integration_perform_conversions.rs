/*
 * This integration test file will take a number of directories from tests/data and perform the
 * conversions using the CLI on them to ensure consistency
 */
use log::*;
use tempfile::*;

use std::path::Path;

/* Just test all the dang tables to make sure they convert properly */
#[ignore]
#[tokio::test]
async fn test_all_tables() -> Result<(), anyhow::Error> {
    let tables = Path::new(&std::env::var("CARGO_MANIFEST_DIR")?).join("tests/data/hive");
    for path in std::fs::read_dir(tables)? {
        if let Ok(dir) = path {
            let dir = dir.path();
            let dir_str = dir.to_str().expect("Failed to make dir_str");
            let table_str = setup(&dir_str).await?;

            let table = oxbow::convert(&table_str).await;
            assert!(
                table.is_ok(),
                "Failure on convesion for {}: {:?}",
                table_str,
                table
            );
            assert!(
                table?.get_files().len() > 0,
                "Expected four files converted for {}",
                dir_str
            );
        }
    }
    Ok(())
}

/*
 * Ensure that a non-partitioned table will properly be created
 */
#[tokio::test]
async fn non_partitioned_table() -> Result<(), anyhow::Error> {
    let table_str = setup("tests/data/hive/deltatbl-non-partitioned/").await?;

    // Perform the actual conversion on the tempdir
    let table = oxbow::convert(&table_str).await;
    assert!(table.is_ok(), "Failed to convert table: {:?}", table);
    assert_eq!(
        2,
        table?.get_files().len(),
        "Expected only two files converted"
    );

    let table = deltalake::open_table(&table_str)
        .await
        .expect("Failed to re-load the table after conversion");
    assert_eq!(
        2,
        table.get_files().len(),
        "Expected only two files when reloading"
    );
    Ok(())
}

/*
 * Ensure that a partitioned table gets set up with the partitions correctly accounted for
 */
#[tokio::test]
async fn partitioned_table() -> Result<(), anyhow::Error> {
    let original_table = "tests/data/hive/deltatbl-partitioned";
    let table_str = setup(original_table).await?;
    let original_table = deltalake::open_table(&original_table).await?;
    let mut expected_partitions: Vec<String> = vec![];
    for partition in original_table.get_partition_values() {
        // partition is a HashMap<String, Option<String>
        for (key, value) in partition.iter() {
            if let Some(value) = value {
                expected_partitions.push(format!("{}={}", key, value));
            }
        }
    }
    expected_partitions.sort();

    let table = oxbow::convert(&table_str).await?;
    assert_eq!(4, table.get_files().len(), "Expected four files converted");
    let mut found_partitions: Vec<String> = vec![];
    for partition in table.get_partition_values() {
        // partition is a HashMap<String, Option<String>
        for (key, value) in partition.iter() {
            if let Some(value) = value {
                found_partitions.push(format!("{}={}", key, value));
            }
        }
    }
    found_partitions.sort();

    assert_eq!(&expected_partitions, &found_partitions);
    Ok(())
}

/*
 * Set up a temporary directory with a test data set and no _delta_log
 */
async fn setup(test_table: &str) -> Result<String, anyhow::Error> {
    let dir = tempdir()?.into_path();
    let data_path = Path::new(&std::env::var("CARGO_MANIFEST_DIR")?).join(test_table);
    assert!(data_path.exists());
    assert!(data_path.is_dir());

    debug!("Populating the temp dir: {:?} from {:?}", dir, data_path);
    let options = fs_extra::dir::CopyOptions::default().content_only(true);
    fs_extra::dir::copy(&data_path, &dir, &options)?;
    /*
     * After copying, the _delta_log needs t0 be removed to ensure the conversion can happen
     */
    fs_extra::dir::remove(dir.join("_delta_log")).expect("Failed to remove test _delta_log");

    let table_str = dir.to_str().expect("Failed to convert path to string");
    let not_yet = deltalake::open_table(&table_str).await;
    assert!(
        not_yet.is_err(),
        "Expected the directory to not yet be a delta table"
    );
    Ok(table_str.into())
}
