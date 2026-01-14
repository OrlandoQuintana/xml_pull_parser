import datetime as dt
import polars as pl
import xgboost as xgb


#############################################
# CONFIG
#############################################

FEATURE_PREFIX = "s3://bucket/feature_store"
LABEL_COL = "label_exclusive_6h"

CELLS_TO_TRAIN = [
    "831c6ffffffffff",
    "831c2fffffffffff",
    "831c0fffffffffff",
    # ...
]

# TIME SPLITS
TRAIN_START = dt.datetime(2024, 1, 1)
TRAIN_END   = dt.datetime(2024, 5, 1)

VAL_START   = dt.datetime(2024, 5, 1)
VAL_END     = dt.datetime(2024, 6, 1)

TEST_START  = dt.datetime(2024, 6, 1)
TEST_END    = dt.datetime(2024, 7, 1)



#############################################
# HELPERS
#############################################

def load_parquet_from_s3(cell_id: str) -> pl.DataFrame:
    """
    Loads a full parquet from S3 into memory.
    """
    uri = f"{FEATURE_PREFIX}/h3={cell_id}/features.parquet"
    print(f"📥 Loading parquet: {uri}")
    return pl.read_parquet(uri)


def time_slice(df: pl.DataFrame, start, end) -> pl.DataFrame:
    """
    Keep only rows where timestamp ∈ [start, end).
    """
    return df.filter(
        (pl.col("timestamp") >= start) &
        (pl.col("timestamp") < end)
    )


def select_training_columns(df: pl.DataFrame) -> pl.DataFrame:
    """
    Keep: label + all sum/count feature columns.
    """
    lbl = LABEL_COL
    sums = [c for c in df.columns if "__sum_" in c]
    cnts = [c for c in df.columns if "__count_" in c]
    return df.select([lbl, *sums, *cnts])



#############################################
# QUANTILE DMATRIX BUILDERS (STREAMING)
#############################################

def batch_generator(cell_ids, start, end):
    """
    Generator that yields (X_batch, y_batch) NumPy arrays.

    Why this matters:
    • QuantileDMatrix builds a quantile sketch over all data
      without ever loading the entire dataset in memory.
    • Each "batch" is one full parquet.
    """
    for cell in cell_ids:
        df = load_parquet_from_s3(cell)

        # Time range filtering
        df = time_slice(df, start, end)
        if df.is_empty():
            print(f"  ⚠️ Cell {cell} produced no rows in this time window")
            continue

        # Select the training columns
        df = select_training_columns(df)

        # Convert → NumPy
        y = df[LABEL_COL].to_numpy(dtype="float32")
        X = df.drop(LABEL_COL).to_numpy(dtype="float32")

        print(f"  ➜ Yielding batch: {X.shape} rows × {X.shape[1]} cols")
        yield X, y



def build_quantile_dmatrix(cell_ids, start, end):
    """
    Creates a QuantileDMatrix using streaming batches.
    This is the correct XGBoost structure for massive datasets.
    """
    return xgb.QuantileDMatrix(
        batch_generator(cell_ids, start, end)
    )



#############################################
# DATASET BUILDER
#############################################

def build_datasets():
    print("Building TRAIN dataset (QuantileDMatrix)")
    dtrain = build_quantile_dmatrix(
        CELLS_TO_TRAIN,
        start=TRAIN_START,
        end=TRAIN_END,
    )

    print("Building VAL dataset (QuantileDMatrix)")
    dval = build_quantile_dmatrix(
        CELLS_TO_TRAIN,
        start=VAL_START,
        end=VAL_END,
    )

    print("Building TEST dataset (QuantileDMatrix)")
    dtest = build_quantile_dmatrix(
        CELLS_TO_TRAIN,
        start=TEST_START,
        end=TEST_END,
    )

    return dtrain, dval, dtest



#############################################
# TRAINING
#############################################

def train_xgboost():
    dtrain, dval, dtest = build_datasets()

    params = {
        "max_depth": 8,
        "eta": 0.05,
        "subsample": 0.8,
        "colsample_bytree": 0.5,
        "objective": "binary:logistic",

        "eval_metric": "aucpr",

        # REQUIRED for QuantileDMatrix
        "tree_method": "hist",      # or "gpu_hist" if you have a GPU
        "max_bin": 256,             # lower memory footprint of histograms
    }

    print("Training XGBoost (QuantileDMatrix)...")
    bst = xgb.train(
        params=params,
        dtrain=dtrain,
        num_boost_round=2000,
        evals=[(dtrain, "train"), (dval, "val")],
        early_stopping_rounds=50,
    )

    bst.save_model("event_pred.json")
    print("💾 Model saved to event_pred.json")

    return bst, dtest



#############################################
# EVALUATION (PR-AUC + ROC-AUC + FEATURE IMPORTANCE)
#############################################

def evaluate(bst, dtest, top_n=25):
    """
    Evaluate XGBoost model:
      • Computes PR-AUC, ROC-AUC
      • Computes event rate baseline
      • Estimates PR-AUC lift over random chance
      • Ranks features by gain importance
    """
    from sklearn.metrics import average_precision_score, roc_auc_score
    import numpy as np

    preds = bst.predict(dtest)   # XGBoost streams-from-disk
    labels = dtest.get_label()

    # Compute metrics
    prauc = average_precision_score(labels, preds)
    rocauc = roc_auc_score(labels, preds)
    event_rate = labels.mean()

    print("\n=============================")
    print("         FINAL TEST METRICS")
    print("=============================")
    print(f"Event rate (baseline PR-AUC): {event_rate:.6f}")
    print(f"PR-AUC:                       {prauc:.6f}")
    print(f"ROC-AUC:                      {rocauc:.6f}")
    print(f"PR-AUC lift over baseline:    {prauc / event_rate:.2f}x")
    print("=============================\n")

    # ------------------------------
    # Feature importance ranking
    # ------------------------------
    print("\n=============================")
    print(f" TOP {top_n} FEATURE IMPORTANCES (gain)")
    print("=============================")

    importance_dict = bst.get_score(importance_type="gain")

    ranked = sorted(
        importance_dict.items(),
        key=lambda x: x[1],
        reverse=True
    )

    for feat, score in ranked[:top_n]:
        print(f"{feat:40s} gain={score:.6f}")

    print("=============================\n")

    return {
        "prauc": prauc,
        "rocauc": rocauc,
        "event_rate": event_rate,
        "feature_importance": ranked,
    }



#############################################
# MAIN
#############################################

if __name__ == "__main__":
    bst, dtest = train_xgboost()
    results = evaluate(bst, dtest)o