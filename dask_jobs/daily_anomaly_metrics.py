"""Вычисляет суточные агрегаты и флаги аномалий температуры через Dask."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import dask.dataframe as dd
import numpy as np
from dask.distributed import Client

from flows.etl_core import _pick_column, _read_location_metadata, _sanitize_column
from flows.settings import DATA_PROCESSED_DIR, DATA_RAW_DIR

WORKER_DATA_RAW_DIR = Path(os.getenv("DASK_WORKER_DATA_RAW_DIR", "/data_raw"))


def _load_timeseries() -> dd.DataFrame:
    csv_files = sorted(DATA_RAW_DIR.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found under {DATA_RAW_DIR}")

    parts: list[dd.DataFrame] = []
    for path in csv_files:
        _, skiprows = _read_location_metadata(path)
        worker_path = WORKER_DATA_RAW_DIR / path.name
        timeseries = dd.read_csv(
            str(worker_path),
            assume_missing=True,
            dtype="object",
            blocksize="64MB",
            skiprows=skiprows,
        )
        timeseries["source_file"] = path.name
        parts.append(timeseries)
    if not parts:
        raise RuntimeError("No timeseries sections were parsed from CSV files.")

    ddf = dd.concat(parts, axis=0, interleave_partitions=True)
    ddf = ddf.rename(columns=_sanitize_column)
    ddf["location_id"] = dd.to_numeric(ddf["location_id"], errors="coerce")
    ddf["source_file"] = ddf["source_file"].astype("object")
    return ddf


def run_job(z_threshold: float) -> Path:
    client = Client(address="tcp://dask-scheduler:8786")
    try:
        ddf = _load_timeseries()

        columns = set(ddf.columns)
        time_column = _pick_column(columns, "time", "timestamp")
        temperature_column = _pick_column(
            columns,
            "temperature_2m",
            "temperature_2m_degc",
            "temperature",
        )
        precipitation_column = _pick_column(
            columns,
            "precipitation",
            "precipitation_mm",
        )
        if not time_column or not temperature_column or not precipitation_column:
            raise ValueError("Required columns are missing in source CSV data.")

        ddf["observed_at"] = dd.to_datetime(ddf[time_column], errors="coerce")
        ddf["period_start"] = ddf["observed_at"].dt.floor("1D")
        ddf[temperature_column] = dd.to_numeric(ddf[temperature_column], errors="coerce")
        ddf[precipitation_column] = dd.to_numeric(
            ddf[precipitation_column], errors="coerce"
        )

        grouped = ddf.groupby(["location_id", "period_start"]).agg(
            {
                temperature_column: ["mean", "max", "min"],
                precipitation_column: "sum",
            }
        )
        grouped = grouped.reset_index()
        grouped.columns = [
            "_".join(filter(None, map(str, column))).strip("_")
            for column in grouped.columns
        ]
        temp_mean_col = f"{temperature_column}_mean"
        temp_max_col = f"{temperature_column}_max"
        temp_min_col = f"{temperature_column}_min"
        precip_sum_col = f"{precipitation_column}_sum"

        location_stats = (
            grouped.groupby("location_id")[temp_mean_col]
            .agg(["mean", "std"])
            .reset_index()
            .rename(
                columns={
                    "mean": "location_temp_mean",
                    "std": "location_temp_std",
                }
            )
        )
        enriched = grouped.merge(location_stats, on="location_id", how="left")
        enriched["location_temp_std"] = enriched["location_temp_std"].replace(
            0.0, np.nan
        )
        enriched["temperature_zscore"] = (
            enriched[temp_mean_col] - enriched["location_temp_mean"]
        ) / enriched["location_temp_std"]
        enriched["temperature_zscore"] = enriched["temperature_zscore"].fillna(0.0)
        enriched["is_temperature_anomaly"] = (
            enriched["temperature_zscore"].abs() >= z_threshold
        )

        result = enriched.rename(
            columns={
                temp_mean_col: "avg_temperature",
                temp_max_col: "max_temperature",
                temp_min_col: "min_temperature",
                precip_sum_col: "total_precipitation",
            }
        )

        DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        output = DATA_PROCESSED_DIR / "daily_temperature_anomalies.parquet"
        result.compute().to_parquet(output, index=False)
        return output
    finally:
        client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Compute daily temperature anomalies with Dask."
    )
    parser.add_argument(
        "--z-threshold",
        type=float,
        default=2.5,
        help="Absolute z-score threshold to flag anomalies.",
    )
    args = parser.parse_args()
    output_path = run_job(args.z_threshold)
    print(f"Wrote anomaly metrics to {output_path}")
