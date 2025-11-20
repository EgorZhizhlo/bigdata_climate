"""Файл выполняет describe() по сырым CSV через распределённый Dask."""

from __future__ import annotations

import argparse
from pathlib import Path

import dask.dataframe as dd
from dask.distributed import Client

from flows.settings import DATA_PROCESSED_DIR, DATA_RAW_DIR


def run_job(sample_fraction: float) -> Path:
    csv_files = sorted(DATA_RAW_DIR.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found under {DATA_RAW_DIR}")

    client = Client(address="tcp://dask-scheduler:8786")
    ddf = dd.read_csv(
        [str(path) for path in csv_files],
        assume_missing=True,
        dtype="object",
        blocksize="64MB",
    )

    if sample_fraction < 1.0:
        ddf = ddf.sample(frac=sample_fraction, random_state=42)

    summary = (
        ddf.describe(include="all")
        .compute()
        .reset_index()
        .rename(columns={"index": "stat"})
    )

    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    output = DATA_PROCESSED_DIR / "dask_describe.csv"
    summary.to_csv(output, index=False)
    client.close()
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run sample Dask metrics job.")
    parser.add_argument(
        "--sample-frac",
        type=float,
        default=0.1,
        help="Fraction of rows to sample before computing describe().",
    )
    args = parser.parse_args()
    result_path = run_job(args.sample_frac)
    print(f"Wrote summary to {result_path}")
