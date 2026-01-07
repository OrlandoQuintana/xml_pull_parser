from concurrent.futures import ProcessPoolExecutor
import polars as pl

TRUTH = None  # global per process


def init_worker(truth_path):
    global TRUTH
    TRUTH = pl.read_parquet(truth_path)


def process_cell(cell_path):
    global TRUTH

    df = pl.read_parquet(cell_path)


    return cell_path


if __name__ == "__main__":
    cell_paths = [...]                  # list of parquet paths (strings)
    truth_path = "s3://bucket/truth.parquet"

    with ProcessPoolExecutor(
        max_workers=4,
        initializer=init_worker,
        initargs=(truth_path,),
    ) as pool:
        list(pool.map(process_cell, cell_paths))