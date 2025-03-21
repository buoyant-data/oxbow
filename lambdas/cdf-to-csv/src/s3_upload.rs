use csv;
use tracing::log::*;

pub(crate) struct S3Upload<'a> {
    client: &'a aws_sdk_s3::Client,
    prefix: String,
    bucket: String,
    basename: String,

    max_rows_per_file: usize,
    single_file: Option<bool>,
    file_count: usize,

    csv: csv::Writer<Vec<u8>>,
    rows: usize,
}

impl<'a> S3Upload<'a> {
    pub(crate) fn new(
        client: &'a aws_sdk_s3::Client,
        bucket: &String,
        prefix: &String,
        basename: &String,
        max_rows_per_file: usize,
    ) -> Self {
        S3Upload {
            client,
            bucket: bucket.clone(),
            prefix: prefix.clone(),
            basename: basename.clone(),

            max_rows_per_file,
            single_file: None,
            file_count: 0,

            csv: csv::Writer::from_writer(vec![]),
            rows: 0,
        }
    }

    pub(crate) async fn write_record(
        &mut self,
        record: Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.csv.write_record(record)?;
        self.rows += 1;
        if self.rows >= self.max_rows_per_file {
            info!("Creating a part");
            let csv = std::mem::replace(&mut self.csv, csv::Writer::from_writer(vec![]));
            self.upload(csv.into_inner()?, true).await?;
            self.rows = 0;
        }
        Ok(())
    }

    pub(crate) async fn close(mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.rows > 0 {
            let csv = std::mem::replace(&mut self.csv, csv::Writer::from_writer(vec![]));
            self.upload(csv.into_inner()?, false).await?;
        }
        Ok(())
    }

    async fn upload(
        &mut self,
        data: Vec<u8>,
        multipart: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if self.single_file.is_none() || multipart {
            self.single_file = Some(!multipart);
        }

        let key;
        if self.single_file.unwrap_or(false) {
            key = format!("{}/{}.csv", self.prefix, self.basename);
        } else {
            key = format!("{}/{}.{}.csv", self.prefix, self.basename, self.file_count);
        }

        self.file_count += 1;

        info!("Uploading to s3://{}/{}", self.bucket, key);

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(data.into())
            .send()
            .await?;

        Ok(())
    }
}
