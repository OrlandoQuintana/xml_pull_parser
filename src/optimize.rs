use deltalake::DeltaTable;
use deltalake::optimize::{OptimizeBuilder, OptimizeType};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let table_path = "s3://mybucket/mytable";

    println!("Loading table...");
    let mut table = DeltaTable::open_table(table_path).await?;

    println!("Running OPTIMIZE ZORDER...");
    let optimize_result = OptimizeBuilder::new(table.object_store(), table.table_uri())
        .with_type(OptimizeType::ZOrder(vec![
            "h3_res3".to_string(),
            "date_id".to_string(),
        ]))
        .with_max_concurrent_tasks(16) // adjust based on your CPUs
        .await?;

    println!("OPTIMIZE complete!");
    println!("Files added:   {}", optimize_result.metrics.added_files);
    println!("Files removed: {}", optimize_result.metrics.removed_files);

    // Refresh in-memory state
    table.update().await?;

    println!("Table updated.");
    Ok(())
}