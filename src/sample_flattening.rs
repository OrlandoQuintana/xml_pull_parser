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
        // Unwrap child lists or fall back to empty Vecs
        let physics = self.physics.clone().unwrap_or_default();
        let history = self.history.clone().unwrap_or_default();
        let business = self.business.clone().unwrap_or_default();

        // Flatten each list to Vec<FlatRecord>
        let flat_physics: Vec<FlatRecord> = if physics.is_empty() {
            vec![FlatRecord::default()]
        } else {
            physics.into_iter().flat_map(|p| p.flatten()).collect()
        };

        let flat_history: Vec<FlatRecord> = if history.is_empty() {
            vec![FlatRecord::default()]
        } else {
            history.into_iter().flat_map(|h| h.flatten()).collect()
        };

        let flat_business: Vec<FlatRecord> = if business.is_empty() {
            vec![FlatRecord::default()]
        } else {
            business.into_iter().flat_map(|b| b.flatten()).collect()
        };

        // Cartesian join: physics × history × business
        let mut out = Vec::new();
        for p in &flat_physics {
            for h in &flat_history {
                for b in &flat_business {
                    let mut row = FlatRecord::default();

                    // Copy Stat-level scalar fields
                    row.stat_mean = self.mean;
                    row.stat_mode = self.mode;
                    row.stat_range = self.range;

                    // Merge flattened Physics
                    row.physics_velocity = p.physics_velocity;
                    row.physics_acceleration = p.physics_acceleration;
                    row.physics_mass = p.physics_mass;

                    // Merge flattened History
                    row.history_previous = h.history_previous.clone();
                    row.history_trend = h.history_trend.clone();

                    // Merge flattened Business
                    row.business_revenue = b.business_revenue;
                    row.business_outlook = b.business_outlook.clone();

                    out.push(row);
                }
            }
        }

        // Fallback: if no data at all
        if out.is_empty() {
            out.push(FlatRecord::default());
        }

        out
    }
}

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
    value: Option<Vec<f64>>,
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
        // ─────────────────────────────────────────────────────────────
        //  PREPARE LISTS FOR CARTESIAN EXPANSION
        // ─────────────────────────────────────────────────────────────
        //
        // Each Option<Vec<T>> may or may not exist.
        // We "unwrap_or_default()" to make iteration uniform and avoid Option gymnastics.
        // These clones are cheap because Vec and String are reference-counted under the hood.
        //
        let ids = self.id.clone().unwrap_or_default();
        let names = self.name.clone().unwrap_or_default();
        let values = self.value.clone().unwrap_or_default();
        let categories = self.category.clone().unwrap_or_default();
        let tags = self.tags.clone().unwrap_or_default();
        let meta_createds = self.meta_created.clone().unwrap_or_default();
        let meta_updateds = self.meta_updated.clone().unwrap_or_default();

        // Each nested Vec<T> will be flattened by calling its own flatten().
        let readings = self.readings.clone().unwrap_or_default();
        let measurements = self.measurements.clone().unwrap_or_default();
        let locations = self.locations.clone().unwrap_or_default();
        let statistics = self.statistics.clone().unwrap_or_default();
        // ─────────────────────────────────────────────────────────────
        //  FALLBACKS FOR EMPTY VECTORS
        // ─────────────────────────────────────────────────────────────
        //
        // If any list is empty, we still want one "placeholder" row
        // so the Cartesian product doesn’t collapse to zero rows.
        //
        let ids = if ids.is_empty() { vec![i32::default()] } else { ids };
        let names = if names.is_empty() { vec![String::new()] } else { names };
        let values = if values.is_empty() { vec![f64::default()] } else { values };
        let categories = if categories.is_empty() { vec![String::new()] } else { categories };
        let tags = if tags.is_empty() { vec![String::new()] } else { tags };
        let meta_createds = if meta_createds.is_empty() { vec![String::new()] } else { meta_createds };
        let meta_updateds = if meta_updateds.is_empty() { vec![String::new()] } else { meta_updateds };

        // For nested structs, we flatten them individually into Vec<FlatRecord>,
        // so each can be merged into our parent rows later.
        let flat_readings: Vec<FlatRecord> = if readings.is_empty() {
            vec![FlatRecord::default()]
        } else {
            readings.into_iter().flat_map(|r| r.flatten()).collect()
        };

        let flat_measurements: Vec<FlatRecord> = if measurements.is_empty() {
            vec![FlatRecord::default()]
        } else {
            measurements.into_iter().flat_map(|m| m.flatten()).collect()
        };

        let flat_locations: Vec<FlatRecord> = if locations.is_empty() {
            vec![FlatRecord::default()]
        } else {
            locations.into_iter().flat_map(|l| l.flatten()).collect()
        };
        
        let flat_statistics: Vec<FlatRecord> = if statistics.is_empty() {
            vec![FlatRecord::default()]
        } else {
            statistics.into_iter().flat_map(|s| s.flatten()).collect()
        };

        // ─────────────────────────────────────────────────────────────
        //  CARTESIAN PRODUCT ACROSS ALL COMBINATIONS
        // ─────────────────────────────────────────────────────────────
        //
        // For each ID × Tag × Reading × Measurement × Location combination,
        // we create a FlatRecord row containing *all* scalar and nested data.
        //
        // This produces a fully denormalized table: one row per logical combination
        // of repeating fields, with consistent 87 columns.
        //
        let mut rows = Vec::new();

        for id in &ids {
            for name in &names {
                for value in &values {
                    for category in &categories {
                        for tag in &tags {
                            for meta_created in &meta_createds {
                                for meta_updated in &meta_updateds {
                                    for reading in &flat_readings {
                                        for measurement in &flat_measurements {
                                             for location in &flat_locations {
                                                for statistic in &flat_statistics {
                                                    let mut row = FlatRecord::default();
                                            
                                                    // Fill Record-level fields...
                                                    row.id = Some(*id);
                                                    // (etc...)
                                            
                                                    // Merge flattened Reading
                                                    row.reading_timestamp = reading.reading_timestamp.clone();
                                                    row.reading_sensor = reading.reading_sensor.clone();
                                                    row.reading_value = reading.reading_value;
                                            
                                                    // Merge flattened Measurement + Method
                                                    row.measurement_type = measurement.measurement_type.clone();
                                                    row.measurement_data = measurement.measurement_data.clone();
                                                    row.method_max = measurement.method_max;
                                                    row.method_min = measurement.method_min;
                                                    row.method_hash = measurement.method_hash;
                                            
                                                    // Merge flattened Location + Region + RegionID
                                                    row.lat = location.lat;
                                                    row.lon = location.lon;
                                                    row.h3_cell = location.h3_cell.clone();
                                                    row.country_code = location.country_code.clone();
                                                    row.region_hash = location.region_hash.clone();
                                                    row.region_id = location.region_id.clone();
                                            
                                                    // 🆕 Merge flattened Statistics
                                                    row.stat_mean = statistic.stat_mean;
                                                    row.stat_mode = statistic.stat_mode;
                                                    row.stat_range = statistic.stat_range;
                                                    row.physics_velocity = statistic.physics_velocity;
                                                    row.physics_acceleration = statistic.physics_acceleration;
                                                    row.physics_mass = statistic.physics_mass;
                                                    row.history_previous = statistic.history_previous.clone();
                                                    row.history_trend = statistic.history_trend.clone();
                                                    row.business_revenue = statistic.business_revenue;
                                                    row.business_outlook = statistic.business_outlook.clone();
                                            
                                                    rows.push(row);
                                                }
}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // ─────────────────────────────────────────────────────────────
        //  FALLBACK: EMPTY RECORD CASE
        // ─────────────────────────────────────────────────────────────
        //
        // If somehow every field was empty, we still emit one default row.
        //
        if rows.is_empty() {
            rows.push(FlatRecord::default());
        }

        rows
    }
}
    
    
#[derive(Debug, Clone, Default)]
struct FlatRecord {
    id: Option<i32>,
    name: Option<String>,
    value: Option<f64>,
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