import polars as pl

###############################################################
# CONFIGURATION
###############################################################

PARQUET_PATH = "s3://bucket/path/**/*.parquet"

# Define your time windows
WINDOWS = ["1h", "6h", "12h", "24h"]

# Output folder for feature tables
OUTPUT_PATH = "s3://bucket/features/"


###############################################################
# LAZY SCAN ENTIRE DATASET (DO NOT LOAD EVERYTHING)
###############################################################
lf = pl.scan_parquet(PARQUET_PATH)


###############################################################
# STEP 0: GET ALL UNIQUE H3 CELLS (res3)
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
    # STEP 1: Find all obs_ids that contribute ANY weight to this cell
    ###############################################################
    obs_ids_lf = (
        lf.filter(pl.col("h3_res3") == target_cell)
          .select("obs_id")
          .unique()
    )

    ###############################################################
    # STEP 2: Load ALL rows (locations + measurements) for these obs_ids
    ###############################################################
    df = (
        lf.filter(pl.col("obs_id").is_in(obs_ids_lf))
          .collect()
          .sort("timestamp")
    )

    if df.is_empty():
        print(f"Cell {target_cell} has no data — skipping.")
        continue


    ###############################################################
    # STEP 3: Build loc_df = candidate locations grouped by h3 cell
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
    # STEP 4: Build meas_df = measurement candidates grouped by obs_id
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
    # STEP 5: Expand location ambiguity (explode)
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
    # STEP 6: Join location + measurement ambiguity
    ###############################################################
    joined = loc_expanded.join(meas_df, on="obs_id", how="left")


    ###############################################################
    # STEP 7: Explode measurement lists (→ atomic rows)
    ###############################################################
    joined_expanded = joined.explode(["measurement_types", "measurement_weights"])


    ###############################################################
    # STEP 8: Compute PMHT-style weighted signal contribution
    ###############################################################
    final = joined_expanded.with_columns([
        (pl.col("location_weight") * pl.col("measurement_weights"))
            .alias("signal_weight")
    ])


    ###############################################################
    # STEP 9: Pivot into wide format (one column per measurement type)
    ###############################################################
    pivoted = final.pivot(
        index="timestamp",
        columns="measurement_type",
        values="signal_weight",
        aggregate_function="sum"
    ).sort("timestamp")


    ###############################################################
    # STEP 10: Rolling-window feature creation
    ###############################################################
    rolling = pivoted

    for win in WINDOWS:
        rolling = rolling.with_columns([
            pl.all().exclude("timestamp")
                  .rolling_sum(win)
                  .suffix(f"__sum_{win}")
        ])

    ###############################################################
    # STEP 11: Save as Parquet / Delta partitioned by H3 cell
    ###############################################################
    output_file = f"{OUTPUT_PATH}/h3={target_cell}/features.parquet"
    rolling.write_parquet(output_file)

    print(f"✔ Finished cell {target_cell} → {output_file}")