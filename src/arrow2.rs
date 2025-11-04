use arrow2::array::*;
use arrow2::datatypes::*;
use arrow2::record_batch::RecordBatch;
use std::fs::File;
use std::sync::Arc;
use parquet2::write::*;
use parquet2::schema::types::*;
use parquet2::compression::CompressionOptions;
use arrow2::io::parquet::write as arrow_parquet;

const WRITE_OPTIONS: WriteOptions = WriteOptions {
    write_statistics: true,
    compression: CompressionOptions::Snappy,
    version: Version::V2,
    data_pagesize_limit: None,
};

fn write_parquet2(batch: &arrow2::record_batch::RecordBatch, path: &str) {
    let file = File::create(path).expect("Failed to create parquet file");

    let arrow_schema = batch.schema();
    let parquet_schema = arrow_parquet::to_parquet_schema(arrow_schema.as_ref());

    let row_group = arrow_parquet::to_row_group(batch, WRITE_OPTIONS)
        .expect("Failed to convert RecordBatch to RowGroup");

    let mut writer = FileWriter::try_new(
        file,
        parquet_schema,
        WRITE_OPTIONS,
    ).expect("Failed to create parquet2 FileWriter");

    writer.write(row_group).expect("Write failed");
    writer.end(None).expect("Failed to finalize parquet file");
}


fn to_arrow2(rows: &[FlatRecord]) -> RecordBatch {
    // ─────────────────────────────────────────────────────────────
    // Schema definition (nearly identical API to arrow)
    // ─────────────────────────────────────────────────────────────
    let schema = Arc::new(Schema::from(vec![
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

    // ─────────────────────────────────────────────────────────────
    // Macros to build columns (arrow2 uses MutableArrays)
    // ─────────────────────────────────────────────────────────────

    macro_rules! build_utf8 {
        ($name:ident) => {{
            let mut col = MutableUtf8Array::<i32>::with_capacity(rows.len());
            for r in rows {
                match &r.$name {
                    Some(v) => col.push(Some(v.as_str())),
                    None => col.push_null(),
                }
            }
            Arc::new(col.into_boxed()) as Arc<dyn Array>
        }};
    }

    macro_rules! build_prim {
        ($name:ident, $ty:ty) => {{
            let mut col = MutablePrimitiveArray::<$ty>::with_capacity(rows.len());
            for r in rows {
                match r.$name {
                    Some(v) => col.push(Some(v)),
                    None => col.push_null(),
                }
            }
            Arc::new(col.into_boxed()) as Arc<dyn Array>
        }};
    }

    // ─────────────────────────────────────────────────────────────
    // Construct all columns
    // ─────────────────────────────────────────────────────────────
    let arrays: Vec<Arc<dyn Array>> = vec![
        build_prim!(id, i32),
        build_utf8!(name),
        build_prim!(value, u64),
        build_utf8!(category),
        build_utf8!(tag),
        build_utf8!(meta_created),
        build_utf8!(meta_updated),

        build_utf8!(reading_timestamp),
        build_utf8!(reading_sensor),
        build_prim!(reading_value, f64),

        build_utf8!(measurement_type),
        build_utf8!(measurement_data),
        build_prim!(method_max, u64),
        build_prim!(method_min, u64),
        build_prim!(method_hash, f64),

        build_prim!(lat, f64),
        build_prim!(lon, f64),
        build_utf8!(h3_cell),
        build_utf8!(country_code),
        build_utf8!(region_hash),
        build_utf8!(region_id),

        build_prim!(stat_mean, u64),
        build_prim!(stat_mode, u64),
        build_prim!(stat_range, u64),

        build_prim!(physics_velocity, f64),
        build_prim!(physics_acceleration, f64),
        build_prim!(physics_mass, f64),

        build_utf8!(history_previous),
        build_utf8!(history_trend),

        build_prim!(business_revenue, f64),
        build_utf8!(business_outlook),
    ];

    // ─────────────────────────────────────────────────────────────
    // Build final RecordBatch
    // ─────────────────────────────────────────────────────────────
    RecordBatch::try_new(schema, arrays).unwrap()
}