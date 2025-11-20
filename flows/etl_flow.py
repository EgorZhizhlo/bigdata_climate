"""Упрощённый ETL, который ищет CSV, прогоняет их через Dask и загружает результат."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import List

import dask.dataframe as dd
import pandas as pd
from prefect import flow, get_run_logger, task
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from .etl_core import TransformArtifacts, compute_transform_artifacts
from .settings import DATA_PROCESSED_DIR, DATA_RAW_DIR, POSTGRES_DSN

SKIP_RAW_LOAD = os.getenv("SKIP_RAW_LOAD", "false").lower() in ("1", "true", "yes")


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
def transform_data(file_paths: List[Path]) -> TransformArtifacts:
    logger = get_run_logger()
    return compute_transform_artifacts(file_paths, logger=logger)


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


@task(name="Load locations")
def load_locations(df: pd.DataFrame) -> dict[str, int]:
    logger = get_run_logger()
    if df.empty:
        logger.warning("No locations to load.")
        return {}

    columns = [
        "raw_location_hash",
        "latitude",
        "longitude",
        "elevation",
        "timezone",
        "timezone_abbreviation",
    ]
    prepared = df.reindex(columns=columns)
    prepared = prepared.dropna(subset=["raw_location_hash"]).drop_duplicates(
        subset=["raw_location_hash"]
    )
    prepared = prepared.where(pd.notnull(prepared), None)
    records = prepared.to_dict("records")
    if not records:
        logger.warning("No unique locations after preprocessing.")
        return {}

    mapping: dict[str, int] = {}
    stmt = text(
        """
        INSERT INTO climate.locations (
            latitude,
            longitude,
            elevation,
            timezone,
            timezone_abbreviation,
            raw_location_hash
        )
        VALUES (
            :latitude,
            :longitude,
            :elevation,
            :timezone,
            :timezone_abbreviation,
            :raw_location_hash
        )
        ON CONFLICT (latitude, longitude) DO UPDATE SET
            elevation = EXCLUDED.elevation,
            timezone = EXCLUDED.timezone,
            timezone_abbreviation = EXCLUDED.timezone_abbreviation,
            raw_location_hash = EXCLUDED.raw_location_hash
        RETURNING location_id, raw_location_hash
        """
    )
    try:
        engine = create_engine(POSTGRES_DSN, future=True)
        with engine.begin() as conn:
            for record in records:
                result = conn.execute(stmt, record)
                location_id, location_hash = result.one()
                mapping[location_hash] = location_id
    except SQLAlchemyError as exc:
        logger.warning("Failed to write locations (%s).", exc)
        return {}

    logger.info("Upserted %s unique locations.", len(mapping))
    return mapping


@task(name="Load raw climate")
def load_raw_climate(
    ddf: dd.DataFrame, location_mapping: dict[str, int], payload_columns: List[str]
) -> None:
    logger = get_run_logger()
    if SKIP_RAW_LOAD:
        logger.info("SKIP_RAW_LOAD=1: skipping raw climate load.")
        return
    if not location_mapping:
        logger.warning("Skipping raw climate load because no location mapping is available.")
        return
    if ddf is None or len(ddf.columns) == 0:
        logger.warning("Skipping raw climate load because dataset is empty.")
        return

    partitions = ddf.to_delayed()
    if not partitions:
        logger.warning("No partitions to process for raw climate load.")
        return

    inserted = 0
    engine = create_engine(POSTGRES_DSN, future=True)
    insert_stmt = text(
        """
        INSERT INTO climate.raw_climate (location_id, observed_at, payload, source_file)
        VALUES (:location_id, :observed_at, CAST(:payload AS JSONB), :source_file)
        """
    )
    for delayed_partition in partitions:
        pdf = delayed_partition.compute()
        if pdf.empty:
            continue
        pdf = pdf.copy()
        pdf["location_id"] = pdf["raw_location_hash"].map(location_mapping)
        pdf = pdf.dropna(subset=["location_id", "observed_at"])
        if pdf.empty:
            continue
        pdf = pdf.where(pd.notnull(pdf), None)
        records = []
        for _, row in pdf.iterrows():
            payload = {
                column: row[column]
                for column in payload_columns
                if column in row and row[column] is not None
            }
            records.append(
                {
                    "location_id": int(row["location_id"]),
                    "observed_at": row["observed_at"],
                    "payload": json.dumps(payload),
                    "source_file": row["source_file"],
                }
            )
        if not records:
            continue
        try:
            with engine.begin() as conn:
                conn.execute(insert_stmt, records)
        except SQLAlchemyError as exc:
            logger.warning("Failed to write raw climate partition (%s).", exc)
            fallback = DATA_PROCESSED_DIR / "raw_climate_fallback.parquet"
            pdf.to_parquet(fallback, index=False)
            logger.info("Wrote raw fallback to %s", fallback)
            return
        inserted += len(records)
    logger.info("Loaded %s raw observations into PostgreSQL.", inserted)


@flow(name="Climate CSV ETL", validate_parameters=False)
def climate_csv_flow() -> None:
    files = discover_csv_files()
    artifacts = transform_data(files)
    locations_map = load_locations(artifacts.locations)
    load_metrics(artifacts.metrics)
    load_raw_climate(artifacts.raw_timeseries, locations_map, artifacts.payload_columns)


if __name__ == "__main__":
    climate_csv_flow()
