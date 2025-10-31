use std::collections::HashMap;

#[derive(Debug, Clone)]
struct FlatRecord {
    year: Option<u16>,
    month: Option<u8>,
    day: Option<u8>,
    h3_cell: Option<String>,
    // ... all your other 80+ fields ...
}

/// Partitions a vector of FlatRecord structs into a HashMap.
/// Each unique (year, month, day, h3_cell) combination becomes one key,
/// and its corresponding Vec<FlatRecord> contains all rows belonging to that partition.
fn partition_records(
    records: Vec<FlatRecord>,
) -> HashMap<(u16, u8, u8, String), Vec<FlatRecord>> {
    let mut partitions: HashMap<(u16, u8, u8, String), Vec<FlatRecord>> = HashMap::new();

    for rec in records {
        // Build a unique key tuple for this record.
        // We use `unwrap_or_default()` to avoid None values crashing the grouping logic.
        let key = (
            rec.year.unwrap_or_default(),
            rec.month.unwrap_or_default(),
            rec.day.unwrap_or_default(),
            rec.h3_cell.clone().unwrap_or_else(|| "unknown".to_string()),
        );

        // Push the record into its partition (automatically creates Vec if missing)
        partitions.entry(key).or_default().push(rec);
    }

    partitions
}