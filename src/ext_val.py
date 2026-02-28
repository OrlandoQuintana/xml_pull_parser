from __future__ import annotations

import polars as pl
from dataclasses import dataclass
from typing import Iterable, Optional


# ----------------------------
# Config
# ----------------------------

@dataclass
class OpsParams:
    # rolling windows measured in HOURS of contiguous data (resets across gaps > 1 hour)
    roll_mean_win: int = 24
    roll_std_win: int = 24
    roll_max_win: int = 6

    # EWMA smoothing factor (0..1). Higher = more weight on recent.
    ewma_alpha: float = 0.30

    # Multi-horizon base weights
    w_1hr: float = 0.55
    w_6hr: float = 0.25
    w_12hr: float = 0.20

    # Dynamics weights (based on 1hr score)
    w_z: float = 0.15
    w_delta: float = 0.10
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
# Gap-aware temporal features
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
    Computes per-cell temporal features, but resets across timestamp gaps > 1 hour.
    Features are computed within (h3_cell, seg_id) groups.
    """
    df = add_gap_segments(df)
    g = ["h3_cell", "seg_id"]

    p1 = pl.col("pred_1hr")
    p6 = pl.col("pred_6hr")
    p12 = pl.col("pred_12hr")

    def ewm(col: pl.Expr) -> pl.Expr:
        return col.ewm_mean(alpha=params.ewma_alpha, adjust=False, ignore_nulls=True)

    df = df.with_columns([
        # Deltas (trend)
        (p1 - p1.shift(1)).over(g).alias("d1_1hr"),
        (p6 - p6.shift(1)).over(g).alias("d1_6hr"),
        (p12 - p12.shift(1)).over(g).alias("d1_12hr"),

        # EWMA smoothed scores
        ewm(p1).over(g).alias("ewma_1hr"),
        ewm(p6).over(g).alias("ewma_6hr"),
        ewm(p12).over(g).alias("ewma_12hr"),

        # Rolling max (recent peak)
        p1.rolling_max(window_size=params.roll_max_win, min_periods=1).over(g).alias("rmax_1hr"),

        # Rolling mean/std for z-score "surprise"
        p1.rolling_mean(window_size=params.roll_mean_win, min_periods=5).over(g).alias("rmean_1hr"),
        p1.rolling_std(window_size=params.roll_std_win, min_periods=5).over(g).alias("rstd_1hr"),
    ])

    df = df.with_columns([
        pl.when(pl.col("rstd_1hr") > 1e-12)
          .then((p1 - pl.col("rmean_1hr")) / pl.col("rstd_1hr"))
          .otherwise(pl.lit(0.0))
          .alias("z_1hr")
    ])

    # Optional: null delta at segment boundaries (so a multi-day jump doesn't look like a 1-hour delta)
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
    Produces ops_score from multi-horizon + 1hr dynamics.
    """
    base = (
        params.w_1hr * pl.col("pred_1hr").fill_null(0.0) +
        params.w_6hr * pl.col("pred_6hr").fill_null(0.0) +
        params.w_12hr * pl.col("pred_12hr").fill_null(0.0)
    )

    dyn = (
        params.w_z * pl.col("z_1hr").fill_null(0.0) +
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
    params = params or OpsParams()

    df1 = load_pred_parquet(pred_1hr_path, "1hr")
    df6 = load_pred_parquet(pred_6hr_path, "6hr")
    df12 = load_pred_parquet(pred_12hr_path, "12hr")

    df = join_horizons(df1, df6, df12)

    # Fill nulls defensively
    df = df.with_columns([
        pl.col("pred_1hr").fill_null(0.0),
        pl.col("pred_6hr").fill_null(0.0),
        pl.col("pred_12hr").fill_null(0.0),
        pl.col("label_1hr").fill_null(0.0),
        pl.col("label_6hr").fill_null(0.0),
        pl.col("label_12hr").fill_null(0.0),
    ])

    df = add_temporal_features_gap_aware(df, params)
    df = add_ops_score(df, params)

    # Save enriched predictions
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
        out_enriched_path="/data/preds/test_preds_ops.parquet",
        params=OpsParams(
            roll_mean_win=24,
            roll_std_win=24,
            roll_max_win=6,
            ewma_alpha=0.30,
            w_1hr=0.55,
            w_6hr=0.25,
            w_12hr=0.20,
            w_z=0.15,
            w_delta=0.10,
            w_rollmax=0.10,
        ),
        metrics_top_fracs=(0.001, 0.01, 0.05),
        label_for_metrics="label_1hr",
    )