use deltalake::{DeltaOps, DeltaTableMetaData, action::Add};
use std::{collections::HashMap, time::SystemTime};
use walkdir::WalkDir;

#[tokio::main(flavor = "current_thread")]
async fn main() -> deltalake::DeltaResult<()> {
    // Root directory that already contains partitioned Parquet files:
    // data/year=2025/month=05/day=05/part-*.parquet
    let table_path = "data";

    // 1) Get a DeltaOps handle for that path (creates _delta_log/ if needed)
    let mut ops = DeltaOps::try_from_uri(table_path).await?;

    // 2) Pick ANY representative parquet to infer the schema
    //    (you can choose a file you know exists)
    let sample_file = "data/year=2025/month=05/day=05/example.parquet";
    let schema = deltalake::parquet::schema::infer_schema_from_file(sample_file).await?;

    // 3) Partition columns must match your folder names (key=value)
    let partition_cols = vec!["year".to_string(), "month".to_string(), "day".to_string()];

    let metadata = DeltaTableMetaData::new(
        Some("ETL Delta Table".to_string()),
        Some("Created from existing Parquet files".to_string()),
        None,
        schema.clone(),
        partition_cols,
        HashMap::new(),
    );

    // 4) Scan all parquet files and build Add actions
    let mut add_actions = Vec::new();

    for entry in WalkDir::new(table_path)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().map(|x| x == "parquet").unwrap_or(false))
    {
        let file_path = entry.path();

        // relative path inside the table root, forward slashes
        let rel_path = file_path.strip_prefix(table_path).unwrap();
        let rel_str = rel_path.to_string_lossy().replace('\\', "/");

        // extract partition values from parent dirs: key=value
        let mut partitions = HashMap::new();
        if let Some(parent) = rel_path.parent() {
            for comp in parent.components() {
                let s = comp.as_os_str().to_string_lossy();
                if let Some((k, v)) = s.split_once('=') {
                    partitions.insert(k.to_string(), v.to_string());
                }
            }
        }

        let size = entry.metadata().map(|m| m.len() as i64).unwrap_or(0);
        let mod_time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        add_actions.push(Add {
            path: rel_str,                  // relative path, no leading slash
            size,                           // file size in bytes
            partition_values: partitions,   // {"year":"2025","month":"05","day":"05"}
            modification_time: mod_time,    // ms since epoch
            data_change: false,             // initial import, not a "data change" op
            stats: None,                    // (optional) you can precompute stats if desired
            tags: None,
            deletion_vector: None,
        });
    }

    // 5) Perform the initial create() commit (version 0)
    ops.create()
        .with_meta_data(metadata)
        .with_actions(add_actions)
        .await?;

    println!("✅ Delta table initialized at: {table_path}");
    Ok(())
}