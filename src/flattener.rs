use std::fmt;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::time::Instant;
use std::sync::Mutex;
use rand::Rng;
use std::sync::Arc;
use dashmap::DashMap;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;

#[derive(Debug, Clone, Default)]
struct Business {
    revenue: Option<f64>,
    outlook: Option<String>
}

impl Business {
    fn flatten(&self) -> Vec<FlatRecord> {
        vec![FlatRecord {
            business_revenue: self.revenue,
            business_outlook: self.outlook.clone(),
            ..Default::default()
        }]
    }
}

#[derive(Debug, Clone, Default)]
struct History {
    previous: Option<String>,
    trend: Option<String>
}

impl History {
    fn flatten(&self) -> Vec<FlatRecord> {
        vec![FlatRecord {
            history_previous: self.previous.clone(),
            history_trend: self.trend.clone(),
            ..Default::default()
        }]
    }
}

#[derive(Debug, Clone, Default)]
struct Physics {
    velocity: Option<f64>,
    acceleration: Option<f64>,
    mass: Option<f64>
}
    
impl Physics {
    fn flatten(&self) -> Vec<FlatRecord> {
        vec![FlatRecord {
            physics_velocity: self.velocity,
            physics_acceleration: self.acceleration,
            physics_mass: self.mass,
            ..Default::default()
        }]
    }
}

#[derive(Debug, Clone, Default)]
struct Stat { 
    mean: Option<u64>,
    mode: Option<u64>,
    range: Option<u64>,
    physics: Option<Vec<Physics>>,
    history: Option<Vec<History>>,
    business: Option<Vec<Business>>
}


impl Stat {
    fn flatten(&self) -> Vec<FlatRecord> {
        let mut out = Vec::new();
        
        if let Some(physics_list) = &self.physics {
            for physics in physics_list {
                for flat_physics in physics.flatten() {
                    let mut row = flat_physics;
                    row.stat_mean = self.mean;
                    row.stat_mode = self.mode;
                    row.stat_range = self.range;
                    out.push(row);
                }
            }
        }

        if let Some(history_list) = &self.history {
            for history in history_list {
                for flat_history in history.flatten() {
                    let mut row = flat_history;
                    row.stat_mean = self.mean;
                    row.stat_mode = self.mode;
                    row.stat_range = self.range;
                    out.push(row);
                }
            }
        }

        if let Some(business_list) = &self.business {
            for business in business_list {
                for flat_business in business.flatten() {
                    let mut row = flat_business;
                    row.stat_mean = self.mean;
                    row.stat_mode = self.mode;
                    row.stat_range = self.range;
                    out.push(row);
                }
            }
        }

        out
    }

}

#[derive(Debug, Clone, Default)]
struct RegionID {
    region_hash: Option<String>,
    region_id: Option<String>,
}

// ─────────────────────────────────────────────────────────────
//  FLATTENING THE LOWEST-LEVEL STRUCT (RegionID)
// ─────────────────────────────────────────────────────────────

// This struct has no children — just scalar fields.
// Flattening it is trivial: we just turn it into one "partial row."
impl RegionID {
    fn flatten(&self) -> Vec<FlatRecord> {
        // Always return a Vec<FlatRecord>, even though this struct
        // produces only one row — this keeps the interface consistent
        // across all flatten() implementations.
        vec![FlatRecord {
            // Copy (clone) our scalar fields into the new flat row.
            // These are Option<String>, so cloning is cheap — mostly just pointer copies.
            region_hash: self.region_hash.clone(),
            region_id: self.region_id.clone(),

            // Fill every other column in FlatRecord with default values (usually None).
            // This ensures that the resulting struct is "complete"
            // and can later be augmented by parent flatten() calls.
            ..Default::default()
        }]
    }
}

#[derive(Debug, Clone, Default)]
struct Region {
    h3_cell: Option<String>,
    country_code: Option<String>,
    region_ID: Option<Vec<RegionID>>,
}
// ─────────────────────────────────────────────────────────────
//  FLATTENING THE PARENT STRUCT (Region)
// ─────────────────────────────────────────────────────────────

// Region has its own scalar fields (h3_cell, country_code)
// and may contain multiple RegionIDs under region_ID (Vec<RegionID>).
// This function flattens the Region and all its RegionIDs into flat rows.
impl Region {
    fn flatten(&self) -> Vec<FlatRecord> {
        // We'll store the output rows here.
        // Each row corresponds to one "Region × RegionID" combination.
        let mut out = Vec::new();

        // Clone the child list (Vec<RegionID>) if present; otherwise use an empty Vec.
        // This prevents Option-handling boilerplate later and ensures the loop always works.
        let ids = self.region_ID.clone().unwrap_or_default();

        // ─────────────────────────────────────────────────────
        // CASE 1: No children (no RegionIDs)
        // ─────────────────────────────────────────────────────
        // If this Region has no RegionIDs, we still want to preserve it in the flattened data.
        // So we emit ONE row containing only the Region-level fields.
        if ids.is_empty() {
            out.push(FlatRecord {
                // Copy the Region-level scalar fields into the row.
                h3_cell: self.h3_cell.clone(),
                country_code: self.country_code.clone(),

                // Leave all other columns empty (default None).
                ..Default::default()
            });
        } 
        // ─────────────────────────────────────────────────────
        // CASE 2: Has children (one or more RegionIDs)
        // ─────────────────────────────────────────────────────
        else {
            // Loop through each RegionID under this Region.
            for id in ids {
                // Flatten the child RegionID into one or more FlatRecords.
                // (In this specific example, RegionID::flatten() always returns a Vec of length 1.)
                for id_row in id.flatten() {
                    // Take ownership of the flattened row so we can modify it.
                    let mut row = id_row;

                    // Fill in the parent (Region) context fields for this row.
                    // Each RegionID row "inherits" the Region's h3_cell and country_code.
                    row.h3_cell = self.h3_cell.clone();
                    row.country_code = self.country_code.clone();

                    // Add the enriched row to the flattened output list.
                    out.push(row);
                }
            }
        }

        // Return all rows produced for this Region.
        // Each FlatRecord now contains both RegionID-level and Region-level data.
        out
    }
}
    
#[derive(Debug, Clone, Default)]
struct Location {
    lat: Option<f64>,
    lon: Option<f64>,
    region: Option<Vec<Region>>
}
impl Location {
    fn flatten(&self) -> Vec<FlatRecord> {
        let mut out = Vec::new();
        let regions = self.region.clone().unwrap_or_default();

        if regions.is_empty() {
            out.push(FlatRecord {
                lat: self.lat,
                lon: self.lon,
                ..Default::default()
            });
        } else {
            for reg in regions {
                for reg_row in reg.flatten() {
                    let mut row = reg_row;
                    row.lat = self.lat;
                    row.lon = self.lon;
                    out.push(row);
                }
            }
        }

        out
    }
}

    
#[derive(Debug, Clone, Default)]
struct Method {
    max: Option<u64>,
    min: Option<u64>,
    hash: Option<f64>,
}

// ─────────────────────────────────────────────────────────────
//  FLATTENING THE LOWEST-LEVEL STRUCT (Method)
// ─────────────────────────────────────────────────────────────
//
// This struct is a leaf — it contains only scalar fields and no nested lists.
// Flattening it is therefore straightforward: we produce one FlatRecord
// representing this Method and fill in the fields it owns.
//
impl Method {
    fn flatten(&self) -> Vec<FlatRecord> {
        vec![FlatRecord {
            // Directly copy scalar fields into their corresponding columns
            method_max: self.max,
            method_min: self.min,
            method_hash: self.hash,

            // Fill remaining columns with defaults (None)
            ..Default::default()
        }]
    }
}

#[derive(Debug, Clone, Default)]
struct Measurement {
    r#type: Option<String>,
    data: Option<String>,
    method: Option<Vec<Method>>,
}

// ─────────────────────────────────────────────────────────────
//  FLATTENING THE PARENT STRUCT (Measurement)
// ─────────────────────────────────────────────────────────────
//
// Measurement contains scalar fields (type, data)
// and may contain a list of Methods (Vec<Method>).
// The flatten process will merge the parent fields into
// each of its child Method rows.
//
impl Measurement {
    fn flatten(&self) -> Vec<FlatRecord> {
        // Vector to accumulate output rows
        let mut out = Vec::new();

        // Safely access the list of Methods; if missing, use empty Vec.
        // This way we can always iterate without worrying about Options.
        let methods = self.method.clone().unwrap_or_default();

        // ─────────────────────────────────────────────────────
        // CASE 1: No child methods
        // ─────────────────────────────────────────────────────
        // Even if no methods are present, we still want to represent
        // this Measurement's scalar fields in the output.
        if methods.is_empty() {
            out.push(FlatRecord {
                measurement_type: self.r#type.clone(),
                measurement_data: self.data.clone(),
                ..Default::default()
            });
        } 
        // ─────────────────────────────────────────────────────
        // CASE 2: One or more child methods
        // ─────────────────────────────────────────────────────
        else {
            for m in methods {
                // Flatten each Method into a Vec<FlatRecord>.
                // Each will have method_max, method_min, method_hash populated.
                for m_row in m.flatten() {
                    let mut row = m_row;

                    // Merge Measurement-level context into the row.
                    row.measurement_type = self.r#type.clone();
                    row.measurement_data = self.data.clone();

                    // Push the combined row to our output vector.
                    out.push(row);
                }
            }
        }

        // Return all flattened rows produced by this Measurement.
        // Each row now represents one Measurement×Method combination.
        out
    }
}

#[derive(Debug, Clone, Default)]
struct Reading {
    timestamp: Option<String>,
    sensor: Option<String>,
    value: Option<f64>,
}

// ─────────────────────────────────────────────────────────────
//  FLATTENING THE LOWEST-LEVEL STRUCT (Reading)
// ─────────────────────────────────────────────────────────────
//
// Reading is a pure "leaf" struct — it has no nested lists or
// sub-objects. Its flattening step just converts itself into a
// single FlatRecord row containing its own scalar fields.
//
impl Reading {
    fn flatten(&self) -> Vec<FlatRecord> {
        // Always return a Vec<FlatRecord> for consistency with other
        // flatten() functions in the hierarchy, even if it’s only 1 row.
        vec![FlatRecord {
            // Copy the scalar fields directly into their corresponding
            // columns in the flat record.
            reading_timestamp: self.timestamp.clone(),
            reading_sensor: self.sensor.clone(),
            reading_value: self.value,

            // Fill every other field with default values (usually None)
            // so the struct stays valid and complete for merging with
            // higher-level flatten() calls (like Record::flatten()).
            ..Default::default()
        }]
    }
}


#[derive(Debug, Clone, Default)]
struct Record {
    id: Option<Vec<i32>>,
    name: Option<Vec<String>>,
    value: Option<Vec<u64>>,
    category: Option<Vec<String>>,
    tags: Option<Vec<String>>,
    meta_created: Option<Vec<String>>,
    meta_updated: Option<Vec<String>>,
    readings: Option<Vec<Reading>>,
    measurements: Option<Vec<Measurement>>,
    locations: Option<Vec<Location>>,
    statistics: Option<Vec<Stat>>
}

impl Record {
    fn flatten(&self) -> Vec<FlatRecord> {
        let mut out = Vec::new();

        if let Some(readings) = &self.readings {
            for reading in readings {
                for flat_reading in reading.flatten() {
                    let mut row = flat_reading;
                    row.id = self.id.as_ref().and_then(|v| v.first().copied());
                    row.name = self.name.as_ref().and_then(|v| v.first().cloned());
                    row.value = self.value.as_ref().and_then(|v| v.first().copied());
                    row.category = self.category.as_ref().and_then(|v| v.first().cloned());
                    row.meta_created = self.meta_created.as_ref().and_then(|v| v.first().cloned());
                    row.meta_updated = self.meta_updated.as_ref().and_then(|v| v.first().cloned());
                    out.push(row);

                }
            }
        }

        if let Some(measurements) = &self.measurements {
            for measurement in measurements {
                for flat_measurement in measurement.flatten() {
                    let mut row = flat_measurement;
                    row.id = self.id.as_ref().and_then(|v| v.first().copied());
                    row.name = self.name.as_ref().and_then(|v| v.first().cloned());
                    row.value = self.value.as_ref().and_then(|v| v.first().copied());
                    row.category = self.category.as_ref().and_then(|v| v.first().cloned());
                    row.meta_created = self.meta_created.as_ref().and_then(|v| v.first().cloned());
                    row.meta_updated = self.meta_updated.as_ref().and_then(|v| v.first().cloned());
                    out.push(row);
                }
            }
        }

        if let Some(locations) = &self.locations {
            for location in locations {
                for flat_location in location.flatten() {
                    let mut row = flat_location;
                    row.id = self.id.as_ref().and_then(|v| v.first().copied());
                    row.name = self.name.as_ref().and_then(|v| v.first().cloned());
                    row.value = self.value.as_ref().and_then(|v| v.first().copied());
                    row.category = self.category.as_ref().and_then(|v| v.first().cloned());
                    row.meta_created = self.meta_created.as_ref().and_then(|v| v.first().cloned());
                    row.meta_updated = self.meta_updated.as_ref().and_then(|v| v.first().cloned());
                    out.push(row);
                }
            }
        }

        if let Some(statistics) = &self.statistics {
            for stat in statistics {
                for flat_stat in stat.flatten() {
                    let mut row = flat_stat;
                    row.id = self.id.as_ref().and_then(|v| v.first().copied());
                    row.name = self.name.as_ref().and_then(|v| v.first().cloned());
                    row.value = self.value.as_ref().and_then(|v| v.first().copied());
                    row.category = self.category.as_ref().and_then(|v| v.first().cloned());
                    row.meta_created = self.meta_created.as_ref().and_then(|v| v.first().cloned());
                    row.meta_updated = self.meta_updated.as_ref().and_then(|v| v.first().cloned());
                    out.push(row);
                }
            }
        }

        if let Some(tags) = &self.tags {
            for tag in tags {
                let mut row = FlatRecord {
                    tag: Some(tag.clone()),
                    ..Default::default()
                };
                row.id = self.id.as_ref().and_then(|v| v.first().copied());
                row.name = self.name.as_ref().and_then(|v| v.first().cloned());
                row.value = self.value.as_ref().and_then(|v| v.first().copied());
                row.category = self.category.as_ref().and_then(|v| v.first().cloned());
                row.meta_created = self.meta_created.as_ref().and_then(|v| v.first().cloned());
                row.meta_updated = self.meta_updated.as_ref().and_then(|v| v.first().cloned());
                out.push(row);
            }
        }
        

        if out.is_empty() {
            out.push(FlatRecord {
                id: self.id.as_ref().and_then(|v| v.first().copied()),
                name: self.name.as_ref().and_then(|v| v.first().cloned()),
                category: self.category.as_ref().and_then(|v| v.first().cloned()),
                meta_created: self.meta_created.as_ref().and_then(|v| v.first().cloned()),
                meta_updated: self.meta_updated.as_ref().and_then(|v| v.first().cloned()),
                ..Default::default()
            });
        }

        out

    }

}
    
#[derive(Debug, Clone, Default)]
struct FlatRecord {
    id: Option<i32>,
    name: Option<String>,
    value: Option<u64>,
    category: Option<String>,
    tag: Option<String>,
    meta_created: Option<String>,
    meta_updated: Option<String>,

    // Flattened reading
    reading_timestamp: Option<String>,
    reading_sensor: Option<String>,
    reading_value: Option<f64>,

    // Flattened measurement + method
    measurement_type: Option<String>,
    measurement_data: Option<String>,
    method_max: Option<u64>,
    method_min: Option<u64>,
    method_hash: Option<f64>,

    // Flattened location + region + region_id
    lat: Option<f64>,
    lon: Option<f64>,
    h3_cell: Option<String>,
    country_code: Option<String>,
    region_hash: Option<String>,
    region_id: Option<String>,
    
    // Flattened Stat + Physics + History + Business
    stat_mean: Option<u64>,
    stat_mode: Option<u64>,
    stat_range: Option<u64>,
    
    physics_velocity: Option<f64>,
    physics_acceleration: Option<f64>,
    physics_mass: Option<f64>,
    
    history_previous: Option<String>,
    history_trend: Option<String>,
    
    business_revenue: Option<f64>,
    business_outlook: Option<String>
}

// Optional pretty-printing to make it easier to read when printed.
impl fmt::Display for FlatRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "FlatRecord(id={:?}, reading={:?}, measurement={:?}, location=({:?},{:?}), stat_mean={:?}, physics_velocity={:?}, business_revenue={:?})",
            self.id,
            self.reading_value,
            self.measurement_type,
            self.lat,
            self.lon,
            self.stat_mean,
            self.physics_velocity,
            self.business_revenue
        )
    }
}

fn to_arrow(rows: &[FlatRecord]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::UInt64, true),
        Field::new("category", DataType::Utf8, true),
        Field::new("tag", DataType::Utf8, true),
        Field::new("meta_created", DataType::Utf8, true),
        Field::new("meta_updated", DataType::Utf8, true),

        Field::new("reading_timestamp", DataType::Utf8, true),
        Field::new("reading_sensor", DataType::Utf8, true),
        Field::new("reading_value", DataType::Float64, true),

        Field::new("measurement_type", DataType::Utf8, true),
        Field::new("measurement_data", DataType::Utf8, true),
        Field::new("method_max", DataType::UInt64, true),
        Field::new("method_min", DataType::UInt64, true),
        Field::new("method_hash", DataType::Float64, true),

        Field::new("lat", DataType::Float64, true),
        Field::new("lon", DataType::Float64, true),
        Field::new("h3_cell", DataType::Utf8, true),
        Field::new("country_code", DataType::Utf8, true),
        Field::new("region_hash", DataType::Utf8, true),
        Field::new("region_id", DataType::Utf8, true),

        Field::new("stat_mean", DataType::UInt64, true),
        Field::new("stat_mode", DataType::UInt64, true),
        Field::new("stat_range", DataType::UInt64, true),

        Field::new("physics_velocity", DataType::Float64, true),
        Field::new("physics_acceleration", DataType::Float64, true),
        Field::new("physics_mass", DataType::Float64, true),

        Field::new("history_previous", DataType::Utf8, true),
        Field::new("history_trend", DataType::Utf8, true),

        Field::new("business_revenue", DataType::Float64, true),
        Field::new("business_outlook", DataType::Utf8, true),
    ]));

    // --- Helper macro for string columns ---
    macro_rules! build_string {
        ($name:ident) => {{
            let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 8);
            for r in rows {
                match &r.$name {
                    Some(v) => b.append_value(v),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }};
    }

    // --- Helper macro for numeric columns ---
    macro_rules! build_prim {
        ($name:ident, $builder:ty) => {{
            let mut b = <$builder>::with_capacity(rows.len());
            for r in rows {
                match r.$name {
                    Some(v) => b.append_value(v),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }};
    }

    let arrays: Vec<ArrayRef> = vec![
        build_prim!(id, Int32Builder),
        build_string!(name),
        build_prim!(value, UInt64Builder),
        build_string!(category),
        build_string!(tag),
        build_string!(meta_created),
        build_string!(meta_updated),

        build_string!(reading_timestamp),
        build_string!(reading_sensor),
        build_prim!(reading_value, Float64Builder),

        build_string!(measurement_type),
        build_string!(measurement_data),
        build_prim!(method_max, UInt64Builder),
        build_prim!(method_min, UInt64Builder),
        build_prim!(method_hash, Float64Builder),

        build_prim!(lat, Float64Builder),
        build_prim!(lon, Float64Builder),
        build_string!(h3_cell),
        build_string!(country_code),
        build_string!(region_hash),
        build_string!(region_id),

        build_prim!(stat_mean, UInt64Builder),
        build_prim!(stat_mode, UInt64Builder),
        build_prim!(stat_range, UInt64Builder),

        build_prim!(physics_velocity, Float64Builder),
        build_prim!(physics_acceleration, Float64Builder),
        build_prim!(physics_mass, Float64Builder),

        build_string!(history_previous),
        build_string!(history_trend),

        build_prim!(business_revenue, Float64Builder),
        build_string!(business_outlook),
    ];

    RecordBatch::try_new(schema, arrays).unwrap()
}
fn write_parquet(batch: &RecordBatch, path: &str) {
    let file = File::create(path).unwrap();

    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
}

type PartitionKey = (Option<i32>, Option<String>, Option<String>);


fn main() {
    let num_records = 50_000;
    let threads = 8;
    let flush_threshold = 2_000_000;
    let start = Instant::now();

    // Shared concurrent hashmap (Arc so all threads can access)
    let partitions: Arc<DashMap<PartitionKey, Vec<FlatRecord>>> = Arc::new(DashMap::new());

    // Rayon pool
    let pool = ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()
        .unwrap();

    

    // 🧩 Producer: parse + flatten in parallel
    pool.install(|| {
        (0..num_records).into_par_iter().for_each(|_| {

            let record = Record {
                id: Some(vec![42]),
                name: Some(vec!["SensorClusterA".to_string()]),
                value: Some(vec![1]),
                category: Some(vec!["Temperature".to_string()]),
                tags: Some(vec!["tag1".into(), "tag2".into(), "tag3".into()]),
                meta_created: Some(vec!["2025-11-01T00:00Z".into()]),
                meta_updated: Some(vec!["2025-11-01T12:00Z".into()]),

                readings: Some(
                    (0..15)
                        .map(|i| Reading {
                            timestamp: Some(format!("2025-11-01T00:{:02}:00Z", i)),
                            sensor: Some(format!("therm-{:02}", i)),
                            value: Some(20.0 + i as f64),
                        })
                        .collect(),
                ),

                measurements: Some(
                    (0..15)
                        .map(|i| Measurement {
                            r#type: Some(format!("MType{}", i)),
                            data: Some(format!("MData{}", i)),
                            method: Some(
                                (0..2)
                                    .map(|j| Method {
                                        max: Some(100 + j as u64),
                                        min: Some(j as u64),
                                        hash: Some(0.5 * j as f64),
                                    })
                                    .collect(),
                            ),
                        })
                        .collect(),
                ),

                locations: Some(
                    (0..15)
                        .map(|i| Location {
                            lat: Some(40.0 + i as f64 * 0.1),
                            lon: Some(-105.0 - i as f64 * 0.1),
                            region: Some(vec![Region {
                                h3_cell: Some(format!("8f28308208{}", i)),
                                country_code: Some("US".into()),
                                region_ID: Some(vec![RegionID {
                                    region_hash: Some(format!("hash{}", i)),
                                    region_id: Some(format!("RID{}", i)),
                                }]),
                            }]),
                        })
                        .collect(),
                ),

                statistics: Some(
                    (0..15)
                        .map(|i| Stat {
                            mean: Some(10 + i),
                            mode: Some(20 + i),
                            range: Some(5 + i),
                            physics: Some(
                                (0..3)
                                    .map(|j| Physics {
                                        velocity: Some(1.0 + j as f64),
                                        acceleration: Some(0.1 * j as f64),
                                        mass: Some(5.0 + j as f64),
                                    })
                                    .collect(),
                            ),
                            history: Some(
                                (0..2)
                                    .map(|j| History {
                                        previous: Some(format!("prev{}", j)),
                                        trend: Some(format!("trend{}", j)),
                                    })
                                    .collect(),
                            ),
                            business: Some(
                                (0..2)
                                    .map(|j| Business {
                                        revenue: Some(1000.0 + j as f64 * 500.0),
                                        outlook: Some(format!("outlook{}", j)),
                                    })
                                    .collect(),
                            ),
                        })
                        .collect(),
                ),
            };


            let flat = record.flatten();

            // Access shared DashMap
            for row in flat {
                let key = (
                    row.id,
                    row.name.clone(),
                    row.category.clone()
                );

                // Get mutable reference to Vec<FlatRecord> in map
                let mut entry = partitions.entry(key.clone()).or_default();
                entry.push(row);

                // If this partition is "big enough", flush it
                if entry.len() >= flush_threshold {
                    // Take ownership of rows by swapping with empty vec
                    let mut rng_2 = rand::rng();
                    let rand_ending: u64 = rng_2.random_range(1000..=9999);
                    
                    let flushed = std::mem::take(&mut *entry);
                    drop(entry); // release DashMap lock
                    
                    let batch = to_arrow(&flushed);
                    let filename = format!("{}_{}_{}_{}.parquet", key.0.unwrap(), key.1.unwrap(), key.2.unwrap(), rand_ending);
                    write_parquet(&batch, &filename);
                }
            }
        });
    });
    
    
    // Final flush of remaining data
    for mut entry in partitions.iter_mut() {
        if !entry.is_empty() {
            let key = entry.key().clone();
            let flushed = std::mem::take(&mut *entry);
            //println!(
            //    "🧹 Final flush for {:?} ({} rows)",
            //    key,
            //    flushed.len()
            //);
            let mut rng_2 = rand::rng();
            let rand_ending: u64 = rng_2.random_range(1000..=9999);
            let batch = to_arrow(&flushed);
            let filename = format!("{}_{}_{}_{}.parquet", key.0.unwrap(), key.1.unwrap(), key.2.unwrap(), rand_ending);
            write_parquet(&batch, &filename);
        }
    }

    let elapsed = start.elapsed();
    println!(
        "🏁 Completed in {:.2}s ({:.1} recs/sec)",
        elapsed.as_secs_f64(),
        num_records as f64 / elapsed.as_secs_f64()
    );
}
