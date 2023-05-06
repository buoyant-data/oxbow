/*
 * This integration test file will take a number of directories from tests/data and perform the
 * conversions using the CLI on them to ensure consistency
 */
use log::*;
use tempfile::*;

use std::path::Path;

#[tokio::test]
async fn non_partitioned_table() -> Result<(), anyhow::Error> {
    let dir = tempdir()?.into_path();
    let data_path = Path::new(&std::env::var("CARGO_MANIFEST_DIR")?)
        .join("tests/data/hive/deltatbl-non-partitioned/");
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
