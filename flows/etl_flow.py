"""Упрощённый ETL, который ищет CSV, прогоняет их через Dask и загружает результат."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, List

import dask.dataframe as dd
import pandas as pd
from prefect import flow, get_run_logger, task
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from .settings import DATA_PROCESSED_DIR, DATA_RAW_DIR, POSTGRES_DSN


def _sanitize_column(name: str) -> str:
    cleaned = (
        name.lower()
        .strip()
        .replace(" ", "_")
        .replace("(", "")
        .replace(")", "")
        .replace("°", "deg")
        .replace("%", "pct")
        .replace("/", "_")
    )
    return cleaned


def _pick_column(columns: Iterable[str], *candidates: str) -> str | None:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return None


@task(name="Discover raw CSV files")
def discover_csv_files() -> List[Path]:
    files = sorted(DATA_RAW_DIR.glob("*.csv"))
    logger = get_run_logger()
    if not files:
        logger.warning("No CSV files found in %s", DATA_RAW_DIR)
    else:
        logger.info("Discovered %s CSV files", len(files))
    return files


@task(name="Transform with Dask")
def transform_data(file_paths: List[Path]) -> pd.DataFrame:
    logger = get_run_logger()
    if not file_paths:
        logger.warning("Skipping transform step because no files were provided.")
        return pd.DataFrame()

    ddf = dd.read_csv(
        [str(path) for path in file_paths],
        dtype="object",
        assume_missing=True,
        blocksize="64MB",
    )
    ddf = ddf.rename(columns=_sanitize_column)
    columns = set(ddf.columns)

    time_column = _pick_column(columns, "time", "timestamp")
    if not time_column:
        logger.warning("Missing time column, unable to build metrics.")
        return pd.DataFrame()

    ddf["observed_at"] = dd.to_datetime(ddf[time_column], errors="coerce")

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

    if not temperature_column and not precipitation_column:
        logger.warning(
            "Did not find temperature or precipitation columns, returning empty metrics."
        )
        return pd.DataFrame()

    agg_map = {}
    rename_map = {}
    if temperature_column:
        ddf[temperature_column] = dd.to_numeric(ddf[temperature_column], errors="coerce")
        agg_map[temperature_column] = "mean"
        rename_map[temperature_column] = "avg_temperature"
    if precipitation_column:
        ddf[precipitation_column] = dd.to_numeric(
            ddf[precipitation_column], errors="coerce"
        )
        agg_map[precipitation_column] = "sum"
        rename_map[precipitation_column] = "total_precipitation"

    if not agg_map:
        logger.warning("No numeric columns available for aggregation.")
        return pd.DataFrame()

    ddf["period_start"] = ddf["observed_at"].dt.floor("1D")

    metrics_ddf = ddf.groupby("period_start").agg(agg_map)
    metrics_df = metrics_ddf.rename(columns=rename_map).reset_index().compute()
    metrics_df["period_level"] = "day"
    logger.info("Produced %s aggregated rows", len(metrics_df))
    return metrics_df


@task(name="Load metrics")
def load_metrics(df: pd.DataFrame) -> None:
    logger = get_run_logger()
    if df.empty:
        logger.warning("No metrics to load.")
        return

    try:
        engine = create_engine(POSTGRES_DSN, future=True)
        with engine.begin() as conn:
            df.to_sql(
                "climate_metrics_staging",
                con=conn,
                schema="climate",
                if_exists="replace",
                index=False,
            )
        logger.info("Loaded metrics into PostgreSQL staging table.")
    except SQLAlchemyError as exc:
        logger.warning(
            "Failed to write to PostgreSQL (%s). Writing parquet snapshot instead.", exc
        )
        output_path = DATA_PROCESSED_DIR / "climate_metrics_preview.parquet"
        df.to_parquet(output_path, index=False)
        logger.info("Wrote parquet fallback to %s", output_path)


@flow(name="Climate CSV ETL")
def climate_csv_flow() -> None:
    files = discover_csv_files()
    metrics = transform_data(files)
    load_metrics(metrics)


if __name__ == "__main__":
    climate_csv_flow()
