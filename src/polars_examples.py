# 1. Filter → group → list locations per cell
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

# 2. Filter → group → list measurement types per observation
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

# 3. Explode locations
loc_expanded = (
    loc_df
      .explode(["obs_ids", "location_weights"])
      .rename({"obs_ids": "obs_id"})
)

# 4. Join with measurements
joined = loc_expanded.join(meas_df, on="obs_id", how="left")

# 5. Explode measurement lists
joined_expanded = joined.explode(["measurement_types", "measurement_weights"])

# 6. Compute effective contribution
final = joined_expanded.with_columns([
    (pl.col("location_weight") * pl.col("measurement_weights"))
        .alias("signal_weight")
])