from __future__ import annotations

import polars as pl
from dataclasses import dataclass
from typing import Iterable, Optional


# ----------------------------
# Config
# ----------------------------

@dataclass
class OpsParams:
    # ---- Short-term (local) behavior windows ----
    roll_mean_win: int = 24
    roll_std_win: int = 24
    roll_max_win: int = 6
    ewma_alpha: float = 0.30

    # ---- Long-term (nominal) baseline ----
    # windows are in OBSERVATIONS (rows), not real hours
    long_mean_win_obs: int = 24 * 30
    long_std_win_obs: int = 24 * 30
    long_ewma_alpha: float = 0.02

    # ---- Z-score stabilization ----
    std_floor_short: float = 1e-3
    std_floor_long: float = 1e-3
    z_clip_short: float = 5.0
    z_clip_long: float = 5.0

    # ---- Multi-horizon base weights ----
    w_1hr: float = 0.55
    w_6hr: float = 0.25
    w_12hr: float = 0.20

    # ---- Dynamic weights ----
    # IMPORTANT: these weights assume you use bounded dynamics (tanh/clipped deltas), not raw z
    w_z_short: float = 0.05
    w_z_long: float = 0.05
    w_delta: float = 0.05
    w_rollmax: float = 0.05

    # ---- Optional: gate short-term dynamics until segment has enough points ----
    min_seg_points: int = 6


# ----------------------------
# I/O + join
# ----------------------------

def load_pred_parquet(path: str, horizon: str) -> pl.DataFrame:
    """
    Input columns: h3_cell (str), timestamp (datetime), label (f32), prediction (f32)
    Output columns: h3_cell, timestamp, label_<horizon>, pred_<horizon>
    """
    df = pl.read_parquet(path)

    required = {"h3_cell", "timestamp", "label", "prediction"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{path} missing columns: {missing}")

    if df.schema.get("timestamp") == pl.Utf8:
        df = df.with_columns(pl.col("timestamp").str.to_datetime(time_unit="ms"))

    df = df.rename({
        "label": f"label_{horizon}",
        "prediction": f"pred_{horizon}",
    })

    return df.select(["h3_cell", "timestamp", f"label_{horizon}", f"pred_{horizon}"])


def join_horizons(df1: pl.DataFrame, df6: pl.DataFrame, df12: pl.DataFrame) -> pl.DataFrame:
    df = df1.join(df6, on=["h3_cell", "timestamp"], how="outer")
    df = df.join(df12, on=["h3_cell", "timestamp"], how="outer")
    return df


# ----------------------------
# Diagnostics (highly recommended)
# ----------------------------

def print_feature_quantiles(df: pl.DataFrame, cols: list[str], name: str = "features") -> None:
    """
    Print quick min/p01/p50/p99/max for selected columns.
    If z-scores are exploding, you’ll see it immediately here.
    """
    q = df.select(
        [
            pl.col(c).min().alias(f"{c}_min") for c in cols
        ] + [
            pl.col(c).quantile(0.01).alias(f"{c}_p01") for c in cols
        ] + [
            pl.col(c).median().alias(f"{c}_p50") for c in cols
        ] + [
            pl.col(c).quantile(0.99).alias(f"{c}_p99") for c in cols
        ] + [
            pl.col(c).max().alias(f"{c}_max") for c in cols
        ]
    )
    print(f"\n=== Quantiles: {name} ===")
    # Use to_dicts for a one-line print
    print(q.to_dicts()[0])


# ----------------------------
# Long-term baseline features (persist across gaps)
# ----------------------------

def add_long_baseline_features(df: pl.DataFrame, params: OpsParams) -> pl.DataFrame:
    df = df.sort(["h3_cell", "timestamp"])
    p1 = pl.col("pred_1hr")

    df = df.with_columns([
        p1.ewm_mean(alpha=params.long_ewma_alpha, adjust=False, ignore_nulls=True)
          .over("h3_cell")
          .alias("ewma_long_1hr"),

        p1.rolling_mean(window_size=params.long_mean_win_obs, min_periods=50)
          .over("h3_cell")
          .alias("rmean_long_1hr"),

        p1.rolling_std(window_size=params.long_std_win_obs, min_periods=50)
          .over("h3_cell")
          .alias("rstd_long_1hr"),
    ])

    # Stable z_long: floor std, then clip, then tanh to bound to (-1,1)
    df = df.with_columns([
        (
            ((p1 - pl.col("rmean_long_1hr")) / pl.col("rstd_long_1hr").clip_min(params.std_floor_long))
            .clip(-params.z_clip_long, params.z_clip_long)
        ).alias("z_long_raw_1hr"),

        pl.col("z_long_raw_1hr").tanh().alias("z_long_1hr"),
    ])

    return df


# ----------------------------
# Gap segmentation + short-term features (gap-safe)
# ----------------------------

def add_gap_segments(df: pl.DataFrame) -> pl.DataFrame:
    df = df.sort(["h3_cell", "timestamp"])

    df = df.with_columns(
        pl.col("timestamp")
          .diff()
          .over("h3_cell")
          .dt.total_hours()
          .alias("dt_hours")
    ).with_columns(
        pl.when(pl.col("dt_hours").is_null())
          .then(1)
          .when(pl.col("dt_hours") > 1)
          .then(1)
          .otherwise(0)
          .alias("new_seg")
    ).with_columns(
        pl.col("new_seg").cum_sum().over("h3_cell").alias("seg_id")
    )

    # segment position and segment length (used for gating)
    df = df.with_columns([
        pl.int_range(0, pl.len()).over(["h3_cell", "seg_id"]).alias("seg_pos"),
        pl.len().over(["h3_cell", "seg_id"]).alias("seg_len"),
    ])

    return df


def add_temporal_features_gap_aware(df: pl.DataFrame, params: OpsParams) -> pl.DataFrame:
    df = add_gap_segments(df)
    g = ["h3_cell", "seg_id"]

    p1 = pl.col("pred_1hr")
    p6 = pl.col("pred_6hr")
    p12 = pl.col("pred_12hr")

    def ewm(col: pl.Expr) -> pl.Expr:
        return col.ewm_mean(alpha=params.ewma_alpha, adjust=False, ignore_nulls=True)

    df = df.with_columns([
        # Deltas within contiguous segments
        (p1 - p1.shift(1)).over(g).alias("d1_1hr_raw"),
        (p6 - p6.shift(1)).over(g).alias("d1_6hr_raw"),
        (p12 - p12.shift(1)).over(g).alias("d1_12hr_raw"),

        # Short-term EWMAs within contiguous segments
        ewm(p1).over(g).alias("ewma_1hr"),
        ewm(p6).over(g).alias("ewma_6hr"),
        ewm(p12).over(g).alias("ewma_12hr"),

        # Rolling max within contiguous segments
        p1.rolling_max(window_size=params.roll_max_win, min_periods=1).over(g).alias("rmax_1hr"),

        # Rolling mean/std for short-term baseline within contiguous segments
        p1.rolling_mean(window_size=params.roll_mean_win, min_periods=5).over(g).alias("rmean_1hr"),
        p1.rolling_std(window_size=params.roll_std_win, min_periods=5).over(g).alias("rstd_1hr"),
    ])

    # Stable z_short: floor std, clip, tanh
    df = df.with_columns([
        (
            ((p1 - pl.col("rmean_1hr")) / pl.col("rstd_1hr").clip_min(params.std_floor_short))
            .clip(-params.z_clip_short, params.z_clip_short)
        ).alias("z_short_raw_1hr"),

        pl.col("z_short_raw_1hr").tanh().alias("z_short_1hr"),
    ])

    # Gap-safe delta: null at segment starts, then bound it
    df = df.with_columns([
        pl.when(pl.col("new_seg") == 1)
          .then(pl.lit(None))
          .otherwise(pl.col("d1_1hr_raw"))
          .alias("d1_1hr_raw")
    ])

    # Bound delta to (-1, 1) via tanh scaling.
    # Note: delta is typically small; tanh keeps it from ever dominating.
    df = df.with_columns([
        pl.col("d1_1hr_raw").fill_null(0.0).tanh().alias("d1_1hr"),
    ])

    # Optional gating: if segment is too short, don't trust short-term dynamics yet.
    # We set z_short/delta/rmax to 0 until seg_pos >= min_seg_points.
    df = df.with_columns([
        pl.when(pl.col("seg_pos") >= params.min_seg_points)
          .then(pl.col("z_short_1hr"))
          .otherwise(pl.lit(0.0))
          .alias("z_short_1hr"),

        pl.when(pl.col("seg_pos") >= params.min_seg_points)
          .then(pl.col("d1_1hr"))
          .otherwise(pl.lit(0.0))
          .alias("d1_1hr"),

        pl.when(pl.col("seg_pos") >= params.min_seg_points)
          .then(pl.col("rmax_1hr"))
          .otherwise(pl.lit(0.0))
          .alias("rmax_1hr"),
    ])

    return df


# ----------------------------
# Operational score
# ----------------------------

def add_ops_score(df: pl.DataFrame, params: OpsParams) -> pl.DataFrame:
    base = (
        params.w_1hr * pl.col("pred_1hr").fill_null(0.0) +
        params.w_6hr * pl.col("pred_6hr").fill_null(0.0) +
        params.w_12hr * pl.col("pred_12hr").fill_null(0.0)
    )

    # Note: z_short_1hr and z_long_1hr are already bounded to (-1,1).
    # d1_1hr is bounded to (-1,1).
    # rmax_1hr is in (0,1) but we treat it as a small booster.
    dyn = (
        params.w_z_long * pl.col("z_long_1hr").fill_null(0.0) +
        params.w_z_short * pl.col("z_short_1hr").fill_null(0.0) +
        params.w_delta * pl.col("d1_1hr").fill_null(0.0) +
        params.w_rollmax * pl.col("rmax_1hr").fill_null(0.0)
    )

    return df.with_columns([
        (base + dyn).alias("ops_score_raw"),
        (base + dyn).alias("ops_score"),
    ])


# ----------------------------
# Per-hour metrics (optional)
# ----------------------------

def per_hour_metrics(
    df: pl.DataFrame,
    score_col: str,
    label_col: str,
    top_frac: float,
    only_event_hours: bool = True,
) -> pl.DataFrame:
    if not (0 < top_frac <= 1.0):
        raise ValueError("top_frac must be in (0,1].")

    base = df.group_by("timestamp").agg([
        pl.len().alias("n_cells"),
        pl.col(label_col).sum().alias("n_pos"),
    ])

    if only_event_hours:
        base = base.filter(pl.col("n_pos") > 0)

    ranked = (
        df.join(base.select(["timestamp", "n_cells", "n_pos"]), on="timestamp", how="inner")
          .with_columns([
              pl.col(score_col).rank("ordinal", descending=True).over("timestamp").alias("rank"),
              (pl.col("n_cells") * top_frac).ceil().cast(pl.Int64).alias("k"),
          ])
          .with_columns([
              (pl.col("rank") <= pl.col("k")).alias("in_topk")
          ])
    )

    metrics = ranked.group_by("timestamp").agg([
        pl.col("n_cells").first(),
        pl.col("n_pos").first(),
        (pl.col(label_col) * pl.col("in_topk").cast(pl.Int64)).sum().alias("tp_at_k"),
        pl.col("in_topk").sum().alias("k_count"),
    ]).with_columns([
        (pl.col("tp_at_k") / pl.col("n_pos")).alias("recall_at_k"),
        (pl.col("tp_at_k") / pl.col("k_count")).alias("precision_at_k"),
    ]).sort("timestamp")

    return metrics


# ----------------------------
# Main pipeline
# ----------------------------

def build_ops_outputs(
    pred_1hr_path: str,
    pred_6hr_path: str,
    pred_12hr_path: str,
    out_enriched_path: str,
    params: Optional[OpsParams] = None,
    metrics_top_fracs: Iterable[float] = (0.001, 0.01, 0.05),
    label_for_metrics: str = "label_1hr",
    print_debug_quantiles: bool = True,
) -> None:
    """
    Writes one merged enriched parquet:
      h3_cell, timestamp,
      label_*, pred_*,
      baseline/dynamics features,
      ops_score
    """
    params = params or OpsParams()

    df1 = load_pred_parquet(pred_1hr_path, "1hr")
    df6 = load_pred_parquet(pred_6hr_path, "6hr")
    df12 = load_pred_parquet(pred_12hr_path, "12hr")

    df = join_horizons(df1, df6, df12)

    # Fill nulls defensively (outer join may introduce nulls if any file is missing rows)
    df = df.with_columns([
        pl.col("pred_1hr").fill_null(0.0),
        pl.col("pred_6hr").fill_null(0.0),
        pl.col("pred_12hr").fill_null(0.0),
        pl.col("label_1hr").fill_null(0.0),
        pl.col("label_6hr").fill_null(0.0),
        pl.col("label_12hr").fill_null(0.0),
    ]).sort(["h3_cell", "timestamp"])

    # Long-term baseline carries across gaps
    df = add_long_baseline_features(df, params)

    # Short-term dynamics are gap-safe
    df = add_temporal_features_gap_aware(df, params)

    # Debug: show ranges BEFORE building ops score
    if print_debug_quantiles:
        print_feature_quantiles(
            df,
            cols=[
                "pred_1hr",
                "z_long_raw_1hr", "z_long_1hr",
                "z_short_raw_1hr", "z_short_1hr",
                "d1_1hr_raw", "d1_1hr",
                "rmax_1hr",
            ],
            name="pre-ops",
        )

    # Combine into ops score
    df = add_ops_score(df, params)

    if print_debug_quantiles:
        print_feature_quantiles(
            df,
            cols=["pred_1hr", "ops_score"],
            name="ops_score",
        )

    # Save enriched predictions table (ALL columns)
    df.write_parquet(out_enriched_path)

    # Save per-hour metrics
    for frac in metrics_top_fracs:
        m = per_hour_metrics(
            df,
            score_col="ops_score",
            label_col=label_for_metrics,
            top_frac=frac,
            only_event_hours=True,
        )
        m.write_parquet(out_enriched_path.replace(".parquet", f".metrics_top{frac:.4f}.parquet"))


if __name__ == "__main__":
    build_ops_outputs(
        pred_1hr_path="/data/preds/test_preds_1hr.parquet",
        pred_6hr_path="/data/preds/test_preds_6hr.parquet",
        pred_12hr_path="/data/preds/test_preds_12hr.parquet",
        out_enriched_path="/data/preds/test_preds_ops_full.parquet",
        params=OpsParams(
            roll_mean_win=24,
            roll_std_win=24,
            roll_max_win=6,
            ewma_alpha=0.30,
            long_mean_win_obs=24 * 30,
            long_std_win_obs=24 * 30,
            long_ewma_alpha=0.02,
            std_floor_short=1e-3,
            std_floor_long=1e-3,
            z_clip_short=5.0,
            z_clip_long=5.0,
            w_1hr=0.55,
            w_6hr=0.25,
            w_12hr=0.20,
            w_z_short=0.05,
            w_z_long=0.05,
            w_delta=0.05,
            w_rollmax=0.05,
            min_seg_points=6,
        ),
        metrics_top_fracs=(0.001, 0.01, 0.05),
        label_for_metrics="label_1hr",
        print_debug_quantiles=True,
    )