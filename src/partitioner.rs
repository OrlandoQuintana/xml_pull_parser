use std::collections::HashMap;

#[derive(Debug, Clone)]
struct FlatRecord {
    // Partition columns
    id: Option<i32>,
    name: Option<String>,
    value: Option<f64>,
    category: Option<String>,
    tag: Option<String>,
    meta_created: Option<String>,
    meta_updated: Option<String>,

    // Reading
    reading_timestamp: Option<String>,
    reading_sensor: Option<String>,
    reading_value: Option<f64>,

    // Measurement + Method
    measurement_type: Option<String>,
    measurement_data: Option<String>,
    method_max: Option<u64>,
    method_min: Option<u64>,
    method_hash: Option<f64>,

    // Location + Region + RegionID
    lat: Option<f64>,
    lon: Option<f64>,
    h3_cell: Option<String>,
    country_code: Option<String>,
    region_hash: Option<String>,
    region_id: Option<String>,

    // Statistics + subcomponents
    stat_mean: Option<u64>,
    stat_mode: Option<u64>,
    stat_range: Option<u64>,

    physics_velocity: Option<f64>,
    physics_acceleration: Option<f64>,
    physics_mass: Option<f64>,

    history_previous: Option<String>,
    history_trend: Option<String>,

    business_revenue: Option<f64>,
    business_outlook: Option<String>,
}

/// ─────────────────────────────────────────────────────────────
///  Partition Vec<FlatRecord> by (category, tag, country_code, h3_cell)
/// ─────────────────────────────────────────────────────────────
///
/// Each unique combination of `(category, tag, country_code, h3_cell)`
/// becomes one key in the resulting HashMap.
/// The value is a Vec<FlatRecord> containing all records belonging
/// to that partition.
///
/// This is exactly how you’d pre-bucket data before converting to Arrow
/// and writing to Parquet.
fn partition_records(
    records: Vec<FlatRecord>,
) -> HashMap<(String, String, String, String), Vec<FlatRecord>> {
    let mut partitions: HashMap<(String, String, String, String), Vec<FlatRecord>> =
        HashMap::with_capacity(records.len() / 8); // heuristic pre-allocation

    for rec in records {
        // Build a unique key tuple for this record.
        // Use "unknown" when Option is None so you don’t collapse valid partitions.
        let key = (
            rec.category.clone().unwrap_or_else(|| "unknown".to_string()),
            rec.tag.clone().unwrap_or_else(|| "unknown".to_string()),
            rec.country_code.clone().unwrap_or_else(|| "unknown".to_string()),
            rec.h3_cell.clone().unwrap_or_else(|| "unknown".to_string()),
        );

        // Push the record into its partition (creates Vec if missing)
        partitions.entry(key).or_default().push(rec);
    }

    partitions
}