use std::io::Cursor;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use object_store::{aws::AmazonS3Builder, path::Path as ObjectPath, ObjectStore};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

/// Asynchronously encodes an Arrow [`RecordBatch`] into Parquet bytes
/// and uploads it to a MinIO (or any S3-compatible) bucket.
///
/// # Arguments
/// * `batch`  – The in-memory Arrow record batch you want to write.
/// * `s3_path` – The key (object path) inside the bucket, e.g.
///                `"table/date=2025-11-05/part-0001.parquet"`.
/// * `bucket` – The name of the target MinIO bucket.
///
/// # Returns
/// * `Ok(())` if the upload succeeded.
/// * An `anyhow::Error` if something went wrong.
///
/// # Example
/// ```ignore
/// write_parquet_to_minio(
///     &batch,
///     "table/date=2025-11-05/part-0001.parquet",
///     "my-bucket"
/// ).await?;
/// ```
pub async fn write_parquet_to_minio(
    batch: &RecordBatch,
    s3_path: &str,
    bucket: &str,
) -> anyhow::Result<()> {
    // ────────────────────────────────────────────────────────────────
    // 1. Build an S3 client configured for MinIO
    //    object_store handles S3-compatible APIs like MinIO when you
    //    specify a custom endpoint and allow HTTP (if not using TLS).
    // ────────────────────────────────────────────────────────────────
    let store = Arc::new(
        AmazonS3Builder::new()
            .with_access_key_id("minioadmin")              // your MinIO access key
            .with_secret_access_key("minioadmin")           // your MinIO secret key
            .with_endpoint("http://127.0.0.1:9000")         // MinIO endpoint
            .with_region("us-east-1")                       // region value can be arbitrary for MinIO
            .with_bucket_name(bucket)                       // target bucket
            .with_allow_http(true)                          // allow non-HTTPS for local setups
            .build()?,                                      // build the object store client
    );

    // ────────────────────────────────────────────────────────────────
    // 2. Encode the Arrow batch into Parquet format in memory
    //    We use a Cursor<Vec<u8>> so ArrowWriter can treat it like a file.
    // ────────────────────────────────────────────────────────────────
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY) // efficient, widely supported
        .build();

    let mut buffer = Cursor::new(Vec::new());

    {
        // Create a Parquet writer and write one or more batches
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props))?;
        writer.write(batch)?;   // serialize the batch into Parquet
        writer.close()?;        // finalize and flush metadata
    }

    // Extract the finished byte vector from the cursor
    let bytes = buffer.into_inner();

    // ────────────────────────────────────────────────────────────────
    // 3. Upload the Parquet bytes asynchronously to MinIO
    //    S3 "directories" are virtual, so you can just use prefixes
    //    like `table/date=.../part-...parquet` — no mkdir needed.
    // ────────────────────────────────────────────────────────────────
    store
        .put(&ObjectPath::from(s3_path), bytes.into())
        .await?; // async, non-blocking upload via reqwest

    // ────────────────────────────────────────────────────────────────
    // 4. Done!  Return success so callers can await and continue.
    // ────────────────────────────────────────────────────────────────
    Ok(())
}