"""Core helpers for Prefect ETL flow (independent of Prefect decorators)."""

from __future__ import annotations

import csv
import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Tuple

import dask.dataframe as dd
import pandas as pd


@dataclass
class TransformArtifacts:
    metrics: pd.DataFrame
    locations: pd.DataFrame
    raw_timeseries: dd.DataFrame
    payload_columns: List[str]


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


def _read_location_metadata(csv_path: Path) -> Tuple[pd.DataFrame, int]:
    rows = []
    with csv_path.open(newline="") as handle:
        reader = csv.reader(handle)
        try:
            header = next(reader)
        except StopIteration:
            return pd.DataFrame(), 0
        for row in reader:
            if not row:
                break
            rows.append(row)
    if not rows:
        return pd.DataFrame(), 0
    df = pd.DataFrame(rows, columns=[_sanitize_column(name) for name in header])
    df["source_file"] = csv_path.name
    for column in ("location_id", "latitude", "longitude", "elevation", "utc_offset_seconds"):
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    if "latitude" in df.columns and "longitude" in df.columns:
        def _hash_row(row: pd.Series) -> str | None:
            lat = row.get("latitude")
            lon = row.get("longitude")
            if pd.isna(lat) or pd.isna(lon):
                return None
            key = f"{lat:.6f}:{lon:.6f}"
            return hashlib.sha1(key.encode()).hexdigest()
        df["raw_location_hash"] = df.apply(_hash_row, axis=1)
    else:
        df["raw_location_hash"] = None
    skiprows = len(rows) + 2
    return df, skiprows


def _empty_artifacts() -> TransformArtifacts:
    empty_pdf = pd.DataFrame()
    empty_ddf = dd.from_pandas(pd.DataFrame(), npartitions=1)
    return TransformArtifacts(empty_pdf, empty_pdf, empty_ddf, [])


def compute_transform_artifacts(
    file_paths: List[Path], logger: logging.Logger | None = None
) -> TransformArtifacts:
    if not file_paths:
        if logger:
            logger.warning("Skipping transform step because no files were provided.")
        return _empty_artifacts()

    timeseries_parts = []
    location_frames: List[pd.DataFrame] = []
    for path in file_paths:
        location_df, skiprows = _read_location_metadata(path)
        if location_df.empty:
            if logger:
                logger.warning("File %s does not contain location metadata, skipping.", path)
            continue
        location_frames.append(location_df)
        timeseries_part = dd.read_csv(
            str(path),
            skiprows=skiprows,
            dtype="object",
            assume_missing=True,
            blocksize="64MB",
        )
        timeseries_part["source_file"] = path.name
        timeseries_parts.append(timeseries_part)

    if not timeseries_parts:
        if logger:
            logger.warning("No usable data found in provided CSV files.")
        return _empty_artifacts()

    ddf = dd.concat(timeseries_parts, axis=0, interleave_partitions=True)
    ddf = ddf.rename(columns=_sanitize_column)
    if "location_id" in ddf.columns:
        ddf["location_id"] = dd.to_numeric(ddf["location_id"], errors="coerce")
    else:
        ddf["location_id"] = None
        if logger:
            logger.info("No location_id column in timeseries; will rely on coordinates.")
    ddf["source_file"] = ddf["source_file"].astype("object")

    location_df = (
        pd.concat(location_frames, ignore_index=True)
        if location_frames
        else pd.DataFrame()
    )
    if location_df.empty:
        if logger:
            logger.warning("Location metadata frame is empty after parsing CSV files.")
        return _empty_artifacts()
    location_df = location_df.dropna(subset=["raw_location_hash"]).drop_duplicates(
        subset=["raw_location_hash"]
    )

    if "location_id" not in location_df.columns:
        location_df["location_id"] = None
    location_ddf = dd.from_pandas(location_df, npartitions=1)
    ddf = ddf.merge(location_ddf, on=["source_file", "location_id"], how="left")
    columns = set(ddf.columns)

    time_column = _pick_column(columns, "time", "timestamp")
    if not time_column:
        if logger:
            logger.warning("Missing time column, unable to build metrics.")
        return _empty_artifacts()

    ddf["observed_at"] = dd.to_datetime(ddf[time_column], errors="coerce")

    location_columns: dict[str, str] = {}
    for canonical, candidates in {
        "location_id": ("location_id", "station_id"),
        "latitude": ("latitude", "lat"),
        "longitude": ("longitude", "lon", "lng"),
        "elevation": ("elevation", "elev_m", "elev"),
        "timezone": ("timezone", "tz"),
        "timezone_abbreviation": (
            "timezone_abbreviation",
            "tz_abbreviation",
            "tz_abbrev",
        ),
    }.items():
        column_name = _pick_column(columns, *candidates)
        if column_name:
            location_columns[canonical] = column_name

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
        if logger:
            logger.warning(
                "Did not find temperature or precipitation columns, returning empty metrics."
            )
        return _empty_artifacts()

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

    for canonical in ("latitude", "longitude", "elevation"):
        column_name = location_columns.get(canonical)
        if column_name:
            ddf[column_name] = dd.to_numeric(ddf[column_name], errors="coerce")

    if not agg_map:
        if logger:
            logger.warning("No numeric columns available for aggregation.")
        return _empty_artifacts()

    ddf["period_start"] = ddf["observed_at"].dt.floor("1D")

    group_keys = ["period_start"] + list(location_columns.values())
    if len(group_keys) == 1 and logger:
        logger.warning(
            "Missing location metadata (latitude/longitude/location_id); "
            "metrics will be aggregated for all sites together."
        )

    metrics_ddf = ddf.groupby(group_keys).agg(agg_map)
    rename_location_map = {
        column: canonical
        for canonical, column in location_columns.items()
        if column != canonical
    }
    metrics_df = (
        metrics_ddf.reset_index()
        .rename(columns={**rename_map, **rename_location_map})
        .compute()
    )
    metrics_df["period_level"] = "day"
    if logger:
        logger.info("Produced %s aggregated rows", len(metrics_df))
    payload_exclude = {
        "source_file",
        "raw_location_hash",
        "observed_at",
        "period_start",
    }
    payload_exclude.update(location_columns.values())
    payload_columns = [col for col in ddf.columns if col not in payload_exclude]
    raw_columns = ["source_file", "raw_location_hash", "observed_at"] + payload_columns
    raw_timeseries = ddf[raw_columns]
    return TransformArtifacts(metrics_df, location_df, raw_timeseries, payload_columns)


__all__ = [
    "TransformArtifacts",
    "_read_location_metadata",
    "_sanitize_column",
    "_pick_column",
    "compute_transform_artifacts",
]
