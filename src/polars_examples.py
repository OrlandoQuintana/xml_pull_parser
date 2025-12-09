import polars as pl

###############################################################
# CONFIGURATION
###############################################################

PARQUET_PATH = "s3://bucket/path/**/*.parquet"

# Define rolling window sizes
WINDOWS = ["1h", "6h", "12h", "24h"]

# Output folder
OUTPUT_PATH = "s3://bucket/features/"

###############################################################
# GLOBAL LAZY SCAN
###############################################################
lf = pl.scan_parquet(PARQUET_PATH)

###############################################################
# STEP 0: BUILD GLOBAL MEASUREMENT VOCABULARY
###############################################################
print("Extracting global measurement vocabulary…")

global_vocab = (
    lf.filter(pl.col("measurement_type").is_not_null())
      .select("measurement_type")
      .unique()
      .collect()
)

measurement_types = global_vocab["measurement_type"].to_list()
print(f"Found {len(measurement_types)} measurement types.")


###############################################################
# STEP 1: GET ALL UNIQUE H3 CELLS (res3)
###############################################################
print("Extracting all unique H3 cells…")

unique_cells_df = (
    lf.select(pl.col("h3_res3"))
      .drop_nulls()
      .unique()
      .collect()
)

cells = unique_cells_df["h3_res3"].to_list()
print(f"Found {len(cells)} unique cells.")


###############################################################
# PROCESS EACH CELL
###############################################################

for target_cell in cells:

    print(f"\n\n==============================")
    print(f"Processing cell: {target_cell}")
    print("==============================\n")

    ###############################################################
    # STEP 2: Find all obs_ids that have ANY weight in this cell
    ###############################################################
    obs_ids_lf = (
        lf.filter(pl.col("h3_res3") == target_cell)
          .select("obs_id")
          .unique()
    )

    ###############################################################
    # STEP 3: Load all rows for these obs_ids
    ###############################################################
    df = (
        lf.filter(pl.col("obs_id").is_in(obs_ids_lf))
          .collect()
    )
    
    df = df.with_columns([
        pl.col("timestamp")
          .str.to_datetime()
          .dt.replace_time_zone("UTC")
    ])
    
    df = df.sort("timestamp")

    if df.is_empty():
        print(f"Cell {target_cell} has no data — skipping.")
        continue


    ###############################################################
    # STEP 4: Build loc_df (candidate locations)
    ###############################################################
    loc_df = (
        df.filter(pl.col("location_weight").is_not_null())
          .select(["h3_res3", "obs_id", "location_weight"])
          .unique()
          .group_by("h3_res3")
          .agg([
              pl.col("obs_id").list().alias("obs_ids"),
              pl.col("location_weight").list().alias("location_weights"),
          ])
    )

    ###############################################################
    # STEP 5: Build meas_df (candidate measurement types)
    ###############################################################
    meas_df = (
        df.filter(pl.col("measurement_weight").is_not_null())
          .select(["obs_id", "measurement_type", "measurement_weight"])
          .unique()
          .group_by("obs_id")
          .agg([
              pl.col("measurement_type").list().alias("measurement_types"),
              pl.col("measurement_weight").list().alias("measurement_weights"),
          ])
    )

    ###############################################################
    # STEP 6: Explode location ambiguity
    ###############################################################
    loc_expanded = (
        loc_df
          .explode(["obs_ids", "location_weights"])
          .rename({
              "obs_ids": "obs_id",
              "location_weights": "location_weight"
          })
    )

    ###############################################################
    # STEP 7: Join with measurement ambiguity
    ###############################################################
    joined = loc_expanded.join(meas_df, on="obs_id", how="left")

    ###############################################################
    # STEP 8: Explode measurement lists → atomic rows
    ###############################################################
    joined_expanded = joined.explode(["measurement_types", "measurement_weights"])

    ###############################################################
    # STEP 9: Compute PMHT-style weighted contribution
    ###############################################################
    final = joined_expanded.with_columns([
        (pl.col("location_weight") * pl.col("measurement_weights"))
            .alias("signal_weight")
    ])

    ###############################################################
    # STEP 10: Pivot into wide format
    ###############################################################
    pivoted = final.pivot(
        index="timestamp",
        columns="measurement_type",
        values="signal_weight",
        aggregate_function="sum"
    ).sort("timestamp")

    ###############################################################
    # STEP 10B: Resample timestamps to exact 1-hour grid
    ###############################################################
    pivoted = (
        pivoted
            .set_sorted("timestamp")
            .upsample(time_column="timestamp", every="1h")
            .fill_null(0.0)
    )

    ###############################################################
    # STEP 11: Ensure full measurement vocabulary exists
    ###############################################################
    for mt in measurement_types:
        if mt not in pivoted.columns:
            pivoted = pivoted.with_columns(pl.lit(0.0).alias(mt))

    ###############################################################
    # STEP 12: Rolling-window feature generation
    ###############################################################
    rolling = pivoted

    for win in WINDOWS:
        # Sum window
        rolling = rolling.with_columns([
            pl.col(measurement_types)
              .rolling_sum(win)
              .suffix(f"__sum_{win}")
        ])

        # Count (presence window)
        rolling = rolling.with_columns([
            (pl.col(measurement_types) > 0)
               .cast(pl.Int32)
               .rolling_sum(win)
               .suffix(f"__count_{win}")
        ])

    ###############################################################
    # STEP 13: Save feature table for this cell
    ###############################################################
    output_file = f"{OUTPUT_PATH}/h3={target_cell}/features.parquet"
    rolling.write_parquet(output_file)

    print(f"✔ Finished cell {target_cell} → {output_file}")