use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

fn flat_records_to_arrow(records: &[FlatRecord]) -> RecordBatch {
    let len = records.len();

    // ─────────────────────────────────────────────
    // Build Arrow array builders for every column
    // ─────────────────────────────────────────────
    let mut id_b = Int32Builder::new(len);
    let mut name_b = StringBuilder::new(len);
    let mut value_b = Float64Builder::new(len);
    let mut category_b = StringBuilder::new(len);

    // Partition columns (example)
    let mut year_b = UInt16Builder::new(len);
    let mut month_b = UInt8Builder::new(len);
    let mut day_b = UInt8Builder::new(len);
    let mut h3_cell_b = StringBuilder::new(len);

    // Reading
    let mut reading_timestamp_b = StringBuilder::new(len);
    let mut reading_sensor_b = StringBuilder::new(len);
    let mut reading_value_b = Float64Builder::new(len);

    // Measurement + Method
    let mut measurement_type_b = StringBuilder::new(len);
    let mut measurement_data_b = StringBuilder::new(len);
    let mut method_max_b = UInt64Builder::new(len);
    let mut method_min_b = UInt64Builder::new(len);
    let mut method_hash_b = Float64Builder::new(len);

    // Location + Region + RegionID
    let mut lat_b = Float64Builder::new(len);
    let mut lon_b = Float64Builder::new(len);
    let mut country_code_b = StringBuilder::new(len);
    let mut region_hash_b = StringBuilder::new(len);
    let mut region_id_b = StringBuilder::new(len);

    // Statistics + subcomponents
    let mut stat_mean_b = UInt64Builder::new(len);
    let mut stat_mode_b = UInt64Builder::new(len);
    let mut stat_range_b = UInt64Builder::new(len);

    let mut physics_velocity_b = Float64Builder::new(len);
    let mut physics_acceleration_b = Float64Builder::new(len);
    let mut physics_mass_b = Float64Builder::new(len);

    let mut history_previous_b = StringBuilder::new(len);
    let mut history_trend_b = StringBuilder::new(len);

    let mut business_revenue_b = Float64Builder::new(len);
    let mut business_outlook_b = StringBuilder::new(len);

    // ─────────────────────────────────────────────
    // Fill builders from the Vec<FlatRecord>
    // ─────────────────────────────────────────────
    for rec in records {
        id_b.append_option(rec.id).unwrap();
        name_b.append_option(rec.name.as_deref()).unwrap();
        value_b.append_option(rec.value).unwrap();
        category_b.append_option(rec.category.as_deref()).unwrap();

        year_b.append_option(rec.year).unwrap_or(());
        month_b.append_option(rec.month).unwrap_or(());
        day_b.append_option(rec.day).unwrap_or(());
        h3_cell_b.append_option(rec.h3_cell.as_deref()).unwrap();

        reading_timestamp_b.append_option(rec.reading_timestamp.as_deref()).unwrap();
        reading_sensor_b.append_option(rec.reading_sensor.as_deref()).unwrap();
        reading_value_b.append_option(rec.reading_value).unwrap();

        measurement_type_b.append_option(rec.measurement_type.as_deref()).unwrap();
        measurement_data_b.append_option(rec.measurement_data.as_deref()).unwrap();
        method_max_b.append_option(rec.method_max).unwrap();
        method_min_b.append_option(rec.method_min).unwrap();
        method_hash_b.append_option(rec.method_hash).unwrap();

        lat_b.append_option(rec.lat).unwrap();
        lon_b.append_option(rec.lon).unwrap();
        country_code_b.append_option(rec.country_code.as_deref()).unwrap();
        region_hash_b.append_option(rec.region_hash.as_deref()).unwrap();
        region_id_b.append_option(rec.region_id.as_deref()).unwrap();

        stat_mean_b.append_option(rec.stat_mean).unwrap();
        stat_mode_b.append_option(rec.stat_mode).unwrap();
        stat_range_b.append_option(rec.stat_range).unwrap();

        physics_velocity_b.append_option(rec.physics_velocity).unwrap();
        physics_acceleration_b.append_option(rec.physics_acceleration).unwrap();
        physics_mass_b.append_option(rec.physics_mass).unwrap();

        history_previous_b.append_option(rec.history_previous.as_deref()).unwrap();
        history_trend_b.append_option(rec.history_trend.as_deref()).unwrap();

        business_revenue_b.append_option(rec.business_revenue).unwrap();
        business_outlook_b.append_option(rec.business_outlook.as_deref()).unwrap();
    }

    // ─────────────────────────────────────────────
    // Define Arrow schema
    // ─────────────────────────────────────────────
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
        Field::new("category", DataType::Utf8, true),

        Field::new("year", DataType::UInt16, true),
        Field::new("month", DataType::UInt8, true),
        Field::new("day", DataType::UInt8, true),
        Field::new("h3_cell", DataType::Utf8, true),

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

    // ─────────────────────────────────────────────
    // Combine all arrays into an Arrow RecordBatch
    // ─────────────────────────────────────────────
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_b.finish()),
            Arc::new(name_b.finish()),
            Arc::new(value_b.finish()),
            Arc::new(category_b.finish()),

            Arc::new(year_b.finish()),
            Arc::new(month_b.finish()),
            Arc::new(day_b.finish()),
            Arc::new(h3_cell_b.finish()),

            Arc::new(reading_timestamp_b.finish()),
            Arc::new(reading_sensor_b.finish()),
            Arc::new(reading_value_b.finish()),

            Arc::new(measurement_type_b.finish()),
            Arc::new(measurement_data_b.finish()),
            Arc::new(method_max_b.finish()),
            Arc::new(method_min_b.finish()),
            Arc::new(method_hash_b.finish()),

            Arc::new(lat_b.finish()),
            Arc::new(lon_b.finish()),
            Arc::new(country_code_b.finish()),
            Arc::new(region_hash_b.finish()),
            Arc::new(region_id_b.finish()),

            Arc::new(stat_mean_b.finish()),
            Arc::new(stat_mode_b.finish()),
            Arc::new(stat_range_b.finish()),

            Arc::new(physics_velocity_b.finish()),
            Arc::new(physics_acceleration_b.finish()),
            Arc::new(physics_mass_b.finish()),

            Arc::new(history_previous_b.finish()),
            Arc::new(history_trend_b.finish()),

            Arc::new(business_revenue_b.finish()),
            Arc::new(business_outlook_b.finish()),
        ],
    )
    .unwrap()
}