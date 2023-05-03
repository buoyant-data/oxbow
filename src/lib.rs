/*
 * The lib module contains the business logic of oxbow, regardless of the interface implementation
 */
use deltalake::Path;
use futures::StreamExt;
use log::*;
use url::Url;

use std::collections::HashMap;

/*
 * Discover `.parquet` files which are present in the location
 */
pub async fn discover_parquet_files(location: &Url) -> deltalake::DeltaResult<Vec<Path>> {
    use deltalake::storage::DeltaObjectStore;
    use deltalake::ObjectStore;

    let options = HashMap::new();
    let store = DeltaObjectStore::try_new(location.clone(), options).expect("Failed to make store");

    let mut result = vec![];
    let mut iter = store.list(None).await?;

    /*
     * NOTE: There is certainly some way to make this more compact
     */
    while let Some(path) = iter.next().await {
        // Result<ObjectMeta> has been yielded
        if let Ok(meta) = path {
            debug!("Discovered file: {:?}", meta.location);

            if let Some(ext) = meta.location.extension() {
                match ext {
                    "parquet" => {
                        result.push(meta.location);
                    }
                    &_ => {}
                }
            }
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_discover_parquet_files_empty_dir() {
        let dir = tempfile::tempdir().expect("Failed to create a temporary directory");
        let url = Url::from_file_path(dir.path()).expect("Failed to parse local path");
        let files = discover_parquet_files(&url)
            .await
            .expect("Failed to discover parquet files");
        assert_eq!(files.len(), 0);
    }

    #[tokio::test]
    async fn test_discover_parquet_files_full_dir() {
        let path = std::fs::canonicalize("./hive/deltatbl-non-partitioned")
            .expect("Failed to canonicalize");
        let url = Url::from_file_path(path).expect("Failed to parse local path");

        let files = discover_parquet_files(&url)
            .await
            .expect("Failed to discover parquet files");

        assert_eq!(files.len(), 2);
    }
}
