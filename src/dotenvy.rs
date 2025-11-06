use dotenvy::dotenv;
use std::env;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load .env file into std::env
    dotenv().ok();

    // Read values from environment
    let endpoint = env::var("AWS_ENDPOINT_URL")?;
    let access_key = env::var("AWS_ACCESS_KEY_ID")?;
    let secret_key = env::var("AWS_SECRET_ACCESS_KEY")?;
    let region = env::var("AWS_REGION")?;
    let bucket = env::var("MINIO_BUCKET")?;

    // Now pass them to your write function
    let batch = /* your RecordBatch */;
    write_parquet_to_minio(
        &batch,
        "table/date=2025-11-05/part-0001.parquet",
        &bucket,
        &endpoint,
        &access_key,
        &secret_key,
        &region,
    )
    .await?;

    println!("✅ Parquet uploaded using .env config");
    Ok(())
}