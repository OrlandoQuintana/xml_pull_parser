import polars as pl

###############################################################
# CONFIGURATION
###############################################################

# The target H3 cell whose features we are constructing
target_cell = "your_h3_cell_here"

# Path to all your Delta table parquet files
PARQUET_PATH = "s3://bucket/path/**/*.parquet"

###############################################################
# LAZY SCAN OF ENTIRE DATASET
###############################################################
# We use lazy scanning so we do NOT read all 2B rows into memory.
lf = pl.scan_parquet(PARQUET_PATH)


###############################################################
# STEP 1: Find all obs_ids that contribute ANY weight to this H3 cell
###############################################################
obs_ids_lf = (
    lf.filter(pl.col("h3_res3") == target_cell)
      .select("obs_id")
      .unique()
)

# This is a lazy dataframe of unique obs_ids.
# We will use this to pull both location rows and measurement rows.


###############################################################
# STEP 2: Load ALL rows (location + measurement candidates)
#         for these observation IDs.
###############################################################
df = (
    lf.filter(pl.col("obs_id").is_in(obs_ids_lf))
      .collect()                         # now load into memory
      .sort("timestamp")                 # important for rolling windows later
)

# df now contains both:
#   - rows describing candidate locations
#   - rows describing candidate measurement types
#   - with shared obs_id linking them


###############################################################
# STEP 3: Build loc_df = location candidates grouped by cell
###############################################################
# We keep only the columns required for location ambiguity resolution.
loc_df = (
    df.filter(pl.col("location_weight").is_not_null())
      .select(["h3_res3", "obs_id", "location_weight"])
      .unique()     # remove duplicates caused by deeper nested lists
      .group_by("h3_res3")
      .agg([
          pl.col("obs_id").list().alias("obs_ids"),
          pl.col("location_weight").list().alias("location_weights"),
      ])
)

# loc_df now: one row per H3 cell, with lists of (obs_ids, location_weights)


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

# meas_df now: one row per observation, with lists of measurement types + weights.


###############################################################
# STEP 5: Expand location ambiguity (explode lists)
###############################################################
loc_expanded = (
    loc_df
      .explode(["obs_ids", "location_weights"])
      .rename({
          "obs_ids": "obs_id",
          "location_weights": "location_weight"
      })
)

# loc_expanded: one row per (cell, obs_id) with a single location_weight.


###############################################################
# STEP 6: Attach measurement ambiguity via join on obs_id
###############################################################
joined = loc_expanded.join(meas_df, on="obs_id", how="left")

# joined: each row has:
#   - h3_cell
#   - obs_id
#   - location_weight
#   - measurement_types (list)
#   - measurement_weights (list)


###############################################################
# STEP 7: Expand measurement ambiguity (explode measurement lists)
###############################################################
joined_expanded = joined.explode(["measurement_types", "measurement_weights"])

# joined_expanded now: atomic rows
#   h3_cell | obs_id | measurement_type | location_weight | measurement_weight


###############################################################
# STEP 8: Compute the PMHT-style weighted contribution of each measurement
###############################################################
final = joined_expanded.with_columns([
    (pl.col("location_weight") * pl.col("measurement_weights"))
        .alias("signal_weight")
])

# final now has:
#   timestamp | obs_id | h3_cell | measurement_type | signal_weight
#
# And this is the complete ambiguity-resolved atomic building block we need.


###############################################################
# STEP 9: Pivot into wide format (one column per measurement type)
###############################################################
pivoted = final.pivot(
    index="timestamp",
    columns="measurement_type",
    values="signal_weight",
    aggregate_function="sum"      # sum duplicate contributions at same timestamp
).sort("timestamp")

# pivoted is now:
# timestamp | temp | fan_speed | weld | pressure | rpm | ...
# exactly the structure XGBoost wants.


###############################################################
# STEP 10: Compute rolling-window aggregates (feature windows)
###############################################################
# Example feature windows:
WINDOWS = ["1h", "6h", "12h", "24h"]

rolling_features = pivoted

for win in WINDOWS:
    rolling_features = rolling_features.with_columns([
        pl.all().exclude("timestamp")
              .rolling_sum(win)
              .suffix(f"__sum_{win}")
    ])


###############################################################
# DONE — rolling_features is now YOUR FEATURE TABLE for this H3 cell
###############################################################
print(rolling_features)