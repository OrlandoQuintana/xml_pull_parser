use std::io::Cursor;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use object_store::{aws::AmazonS3Builder, path::Path as ObjectPath, ObjectStore};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

/// Asynchronously encodes an Arrow [`RecordBatch`] into Parquet bytes
/// and uploads it to a MinIO (or any S3-compatible) bucket using environment config.
///
/// This function is async and non-blocking, running safely under Tokio.
///
/// # Arguments
/// * `batch`     – The Arrow record batch to write.
/// * `s3_path`   – The S3-style key (e.g. "table/date=2025-11-05/part-0001.parquet").
/// * `bucket`    – Target MinIO bucket.
/// * `endpoint`  – MinIO endpoint (http:// or https://).
/// * `access_key` / `secret_key` – Credentials.
/// * `region`    – AWS-style region (any string for MinIO).
pub async fn write_parquet_to_minio(
    batch: &RecordBatch,
    s3_path: &str,
    bucket: &str,
    endpoint: &str,
    access_key: &str,
    secret_key: &str,
    region: &str,
) -> anyhow::Result<()> {
    // ────────────────────────────────────────────────────────────────
    // 1. Build the S3/MinIO client.
    //    The builder automatically picks up AWS_CA_BUNDLE from env if set.
    // ────────────────────────────────────────────────────────────────
    let store = Arc::new(
        AmazonS3Builder::new()
            .with_access_key_id(access_key)
            .with_secret_access_key(secret_key)
            .with_endpoint(endpoint)
            .with_region(region)
            .with_bucket_name(bucket)
            .with_allow_http(endpoint.starts_with("http://")) // auto-detect HTTP vs HTTPS
            .build()?,
    );

    // ────────────────────────────────────────────────────────────────
    // 2. Encode the Arrow batch into Parquet format in memory.
    //    We use a Cursor<Vec<u8>> so ArrowWriter can treat it like a file.
    // ────────────────────────────────────────────────────────────────
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();

    let mut buffer = Cursor::new(Vec::new());
    {
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props))?;
        writer.write(batch)?;
        writer.close()?;
    }

    let bytes = buffer.into_inner();

    // ────────────────────────────────────────────────────────────────
    // 3. Upload the Parquet bytes asynchronously to MinIO.
    //    The call is fully async and non-blocking under Tokio.
    // ────────────────────────────────────────────────────────────────
    store
        .put(&ObjectPath::from(s3_path), bytes.into())
        .await?;

    println!("🪣 Uploaded to s3://{}/{}", bucket, s3_path);

    Ok(())
}