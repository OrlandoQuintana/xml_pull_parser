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
    # Interpreted as "hours" only when your data is hourly-contiguous.
    # These are computed within contiguous segments (resets on gaps).
    roll_mean_win: int = 24
    roll_std_win: int = 24
    roll_max_win: int = 6

    # EWMA smoothing factor (0..1) for short-term smoothing within segments
    ewma_alpha: float = 0.30

    # ---- Long-term (nominal) baseline ----
    # These DO NOT reset on gaps. They are computed over the full per-cell history.
    # Note: These windows are in OBSERVATIONS (rows), not true hours, unless you densify.
    long_mean_win_obs: int = 24 * 30   # ~30 days if data is mostly hourly when present
    long_std_win_obs: int = 24 * 30
    long_ewma_alpha: float = 0.02      # slow-moving baseline mean

    # ---- Multi-horizon base weights ----
    w_1hr: float = 0.55
    w_6hr: float = 0.25
    w_12hr: float = 0.20

    # ---- Dynamic weights ----
    # Short-term "surprise" within contiguous segments
    w_z_short: float = 0.15
    # Long-term "surprise" vs nominal behavior across full history
    w_z_long: float = 0.15
    # Short-term trend (gap-safe)
    w_delta: float = 0.10
    # Short-term persistence
    w_rollmax: float = 0.10


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

    # Parse timestamp if needed (no-op if already Datetime)
    if df.schema.get("timestamp") == pl.Utf8:
        df = df.with_columns(pl.col("timestamp").str.to_datetime(time_unit="ms"))

    df = df.rename({
        "label": f"label_{horizon}",
        "prediction": f"pred_{horizon}",
    })

    return df.select(["h3_cell", "timestamp", f"label_{horizon}", f"pred_{horizon}"])


def join_horizons(df1: pl.DataFrame, df6: pl.DataFrame, df12: pl.DataFrame) -> pl.DataFrame:
    # Outer join is safest; assumes same dataset but avoids silent drops if one file is missing rows
    df = df1.join(df6, on=["h3_cell", "timestamp"], how="outer")
    df = df.join(df12, on=["h3_cell", "timestamp"], how="outer")
    return df


# ----------------------------
# Long-term baseline features (DO NOT reset on gaps)
# ----------------------------

def add_long_baseline_features(df: pl.DataFrame, params: OpsParams) -> pl.DataFrame:
    """
    Adds long-term baseline features per cell over the entire history.
    These carry across gaps and give you "nominal behavior" continuity.

    Adds:
      ewma_long_1hr: slow EWMA mean of pred_1hr over full cell history
      rmean_long_1hr / rstd_long_1hr: long rolling mean/std (OBSERVATION-based window)
      z_long_1hr: surprise vs long rolling baseline
    """
    df = df.sort(["h3_cell", "timestamp"])
    p1 = pl.col("pred_1hr")

    df = df.with_columns([
        # Slow baseline EWMA (does not reset on gaps)
        p1.ewm_mean(alpha=params.long_ewma_alpha, adjust=False, ignore_nulls=True)
          .over("h3_cell")
          .alias("ewma_long_1hr"),

        # Long rolling mean/std baseline (window is in rows/observations)
        p1.rolling_mean(window_size=params.long_mean_win_obs, min_periods=50)
          .over("h3_cell")
          .alias("rmean_long_1hr"),

        p1.rolling_std(window_size=params.long_std_win_obs, min_periods=50)
          .over("h3_cell")
          .alias("rstd_long_1hr"),
    ])

    df = df.with_columns([
        pl.when(pl.col("rstd_long_1hr") > 1e-12)
          .then((p1 - pl.col("rmean_long_1hr")) / pl.col("rstd_long_1hr"))
          .otherwise(pl.lit(0.0))
          .alias("z_long_1hr")
    ])

    return df


# ----------------------------
# Gap segmentation + short-term features (gap-safe)
# ----------------------------

def add_gap_segments(df: pl.DataFrame) -> pl.DataFrame:
    """
    Adds:
      dt_hours: hours since previous sample within cell
      new_seg: 1 if first row or gap > 1 hour else 0
      seg_id: cumulative segment id within each cell
    """
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

    return df


def add_temporal_features_gap_aware(df: pl.DataFrame, params: OpsParams) -> pl.DataFrame:
    """
    Computes short-term per-cell temporal features, resetting across timestamp gaps > 1 hour.
    These are "local dynamics" and should not be trusted across missing-day jumps.

    Adds:
      d1_*: short-term deltas within segments (gap-safe)
      ewma_*: short-term EWMA within segments
      rmax_1hr: short rolling max within segments
      rmean_1hr/rstd_1hr: short rolling mean/std within segments
      z_short_1hr: surprise vs short baseline within segments
    """
    df = add_gap_segments(df)
    g = ["h3_cell", "seg_id"]

    p1 = pl.col("pred_1hr")
    p6 = pl.col("pred_6hr")
    p12 = pl.col("pred_12hr")

    def ewm(col: pl.Expr) -> pl.Expr:
        return col.ewm_mean(alpha=params.ewma_alpha, adjust=False, ignore_nulls=True)

    df = df.with_columns([
        # Deltas (trend) within contiguous segments
        (p1 - p1.shift(1)).over(g).alias("d1_1hr"),
        (p6 - p6.shift(1)).over(g).alias("d1_6hr"),
        (p12 - p12.shift(1)).over(g).alias("d1_12hr"),

        # EWMA smoothed scores (short-term) within contiguous segments
        ewm(p1).over(g).alias("ewma_1hr"),
        ewm(p6).over(g).alias("ewma_6hr"),
        ewm(p12).over(g).alias("ewma_12hr"),

        # Rolling max (recent peak) within contiguous segments
        p1.rolling_max(window_size=params.roll_max_win, min_periods=1).over(g).alias("rmax_1hr"),

        # Rolling mean/std for short-term z-score within segments
        p1.rolling_mean(window_size=params.roll_mean_win, min_periods=5).over(g).alias("rmean_1hr"),
        p1.rolling_std(window_size=params.roll_std_win, min_periods=5).over(g).alias("rstd_1hr"),
    ])

    df = df.with_columns([
        pl.when(pl.col("rstd_1hr") > 1e-12)
          .then((p1 - pl.col("rmean_1hr")) / pl.col("rstd_1hr"))
          .otherwise(pl.lit(0.0))
          .alias("z_short_1hr")
    ])

    # Null delta at segment boundaries so a missing-day jump doesn't look like a 1-hour change
    df = df.with_columns([
        pl.when(pl.col("new_seg") == 1)
          .then(pl.lit(None))
          .otherwise(pl.col("d1_1hr"))
          .alias("d1_1hr")
    ])

    return df


# ----------------------------
# Operational score
# ----------------------------

def add_ops_score(df: pl.DataFrame, params: OpsParams) -> pl.DataFrame:
    """
    Produces ops_score from:
      - multi-horizon base signal
      - long-term surprise vs nominal (persists across gaps)
      - short-term dynamics (gap-safe)
    """
    base = (
        params.w_1hr * pl.col("pred_1hr").fill_null(0.0) +
        params.w_6hr * pl.col("pred_6hr").fill_null(0.0) +
        params.w_12hr * pl.col("pred_12hr").fill_null(0.0)
    )

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
    """
    For each timestamp:
      - rank all cells by score_col
      - take top K = ceil(top_frac * num_cells)
      - compute Recall@K and Precision@K
    If only_event_hours=True, restrict to hours where n_pos > 0.
    """
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
    label_for_metrics: str = "label_1hr",  # usually you care about imminent (1hr) events operationally
) -> None:
    """
    Produces a single merged/enriched parquet that includes:
      h3_cell, timestamp,
      label_*, pred_* for all horizons,
      long baseline features, short dynamics features,
      ops_score

    Also optionally writes per-hour metrics parquets.
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
    ])

    # Ensure stable ordering for time features
    df = df.sort(["h3_cell", "timestamp"])

    # Long-term baseline carries across gaps (nominal behavior memory)
    df = add_long_baseline_features(df, params)

    # Short-term dynamics are gap-safe and reset across missing periods
    df = add_temporal_features_gap_aware(df, params)

    # Combine into ops_score
    df = add_ops_score(df, params)

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
    # Example usage
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

            w_1hr=0.55,
            w_6hr=0.25,
            w_12hr=0.20,
            w_z_short=0.15,
            w_z_long=0.15,
            w_delta=0.10,
            w_rollmax=0.10,
        ),
        metrics_top_fracs=(0.001, 0.01, 0.05),
        label_for_metrics="label_1hr",
    )