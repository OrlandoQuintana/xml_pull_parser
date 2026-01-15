import datetime as dt
import polars as pl
import xgboost as xgb
import os

import os

os.environ["FSSPEC_S3_CACHE_TYPE"] = "bytes"
os.environ["FSSPEC_S3_CACHE_DIR"] = "/mnt/data/s3_cache"
os.environ["FSSPEC_S3_CACHE_MAXSIZE"] = "15TB"  # or however large you want


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
    uri = f"{FEATURE_PREFIX}/h3={cell_id}/features.parquet"
    print(f"📥 Loading parquet: {uri}")
    return pl.read_parquet(uri)

def time_slice(df, start, end):
    return df.filter(
        (pl.col("timestamp") >= start) &
        (pl.col("timestamp") < end)
    )

def select_training_columns(df):
    lbl = LABEL_COL
    sums = [c for c in df.columns if "__sum_" in c]
    cnts = [c for c in df.columns if "__count_" in c]
    return df.select([lbl, *sums, *cnts])


#############################################
# XGBOOST ITERATOR FOR QUANTILEDMATRIX
#############################################

class ParquetIter(xgb.core.DataIter):
    def __init__(self, cell_ids, start, end):
        super().__init__()
        self.cell_ids = cell_ids
        self.start = start
        self.end = end
        self._set_initial_state()

    def _set_initial_state(self):
        self._gen = self._make_generator()
        self._at_beginning = True
        self._seen_batch = False

    def reset(self):
        # Called by XGBoost before EVERY pass
        self._set_initial_state()

    def _make_generator(self):
        for cell_id in self.cell_ids:
            df = load_parquet_from_s3(cell_id)
            df = time_slice(df, self.start, self.end)

            if df.is_empty():
                continue

            df = select_training_columns(df)

            # --- Sort features ---
            feature_cols = sorted([c for c in df.columns if c != LABEL_COL])
            df = df.select([LABEL_COL, *feature_cols])

            y = df[LABEL_COL].to_numpy("float32")
            X = df.drop(LABEL_COL).to_numpy("float32")

            print(f"Yielding batch: {cell_id}: {X.shape}")
            yield X, y

    def next(self, input_data):
        try:
            X, y = next(self._gen)
            self._seen_batch = True
            input_data(data=X, label=y)
            return 0     # batch OK
        except StopIteration:
            if not self._seen_batch:
                # XGBoost will error unless we raise here
                raise RuntimeError("Iterator yielded no batches!")
            return 1     # signal end of this epoch


def build_quantile_dmatrix(cell_ids, start, end):
    return xgb.QuantileDMatrix(
        ParquetIter(cell_ids, start, end)
    )


#############################################
# DATASET BUILDER
#############################################

def build_datasets():
    print("Building TRAIN dataset (QuantileDMatrix)")
    dtrain = build_quantile_dmatrix(CELLS_TO_TRAIN, TRAIN_START, TRAIN_END)

    print("Building VAL dataset (QuantileDMatrix)")
    dval = xgb.QuantileDMatrix(
        ParquetIter(CELLS_TO_TRAIN, VAL_START, VAL_END),
        ref=dtrain
    )

    print("Building TEST dataset (QuantileDMatrix)")
    dtest = xgb.QuantileDMatrix(
        ParquetIter(CELLS_TO_TRAIN, TEST_START, TEST_END),
        ref=dtrain
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

        "tree_method": "hist",   # required for QuantileDMatrix
        "max_bin": 256,
    }

    print("Training XGBoost (QuantileDMatrix)...")
    bst = xgb.train(
        params=params,
        dtrain=dtrain,
        evals=[(dtrain, "train"), (dval, "val")],
        num_boost_round=2000,
        early_stopping_rounds=50,
    )

    bst.save_model("event_pred.json")
    print("💾 Model saved to event_pred.json")
    return bst, dtest


#############################################
# EVALUATION
#############################################

def evaluate(bst, dtest, top_n=25):
    from sklearn.metrics import average_precision_score, roc_auc_score

    preds = bst.predict(dtest)
    labels = dtest.get_label()

    prauc = average_precision_score(labels, preds)
    rocauc = roc_auc_score(labels, preds)
    event_rate = labels.mean()

    print("\n=============================")
    print("         FINAL TEST METRICS")
    print("=============================")
    print(f"Event rate (baseline PR-AUC): {event_rate:.6f}")
    print(f"PR-AUC:                       {prauc:.6f}")
    print(f"ROC-AUC:                      {rocauc:.6f}")
    print(f"PR-AUC lift over baseline:    {prauc/event_rate:.2f}x")
    print("=============================\n")

    print("\n=============================")
    print(f" TOP {top_n} FEATURE IMPORTANCES (gain)")
    print("=============================")

    ranked = sorted(
        bst.get_score(importance_type="gain").items(),
        key=lambda x: x[1],
        reverse=True
    )

    for feat, score in ranked[:top_n]:
        print(f"{feat:40s} gain={score:.6f}")

    return ranked


#############################################
# MAIN
#############################################

if __name__ == "__main__":
    bst, dtest = train_xgboost()
    evaluate(bst, dtest)