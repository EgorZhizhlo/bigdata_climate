from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from flows.etl_core import (  # noqa: E402
    TransformArtifacts,
    _read_location_metadata,
    compute_transform_artifacts,
)


def _write_sample_csv(tmp_path: Path) -> Path:
    content = "\n".join(
        [
            (
                "location_id,latitude,longitude,elevation,utc_offset_seconds,"
                "timezone,timezone_abbreviation"
            ),
            "0,10.0,20.0,100.0,0,UTC,UTC",
            "1,30.0,40.0,200.0,0,UTC,UTC",
            "",
            "location_id,time,temperature_2m,precipitation",
            "0,2024-01-01T00:00,10.0,1.0",
            "0,2024-01-01T01:00,12.0,0.0",
            "1,2024-01-01T00:00,5.0,0.5",
        ]
    )
    path = tmp_path / "sample.csv"
    path.write_text(content)
    return path


def test_read_location_metadata_parses_header_and_hash(tmp_path: Path) -> None:
    csv_path = _write_sample_csv(tmp_path)

    df, skiprows = _read_location_metadata(csv_path)

    assert skiprows == 4  # две строки метаданных + заголовок + пустая строка
    assert set(df.columns) >= {"location_id", "latitude", "longitude", "raw_location_hash"}
    assert df["raw_location_hash"].notna().all()
    assert df["location_id"].tolist() == [0.0, 1.0]


def test_transform_data_returns_metrics_and_raw(tmp_path: Path) -> None:
    csv_path = _write_sample_csv(tmp_path)

    artifacts = compute_transform_artifacts([csv_path])

    assert isinstance(artifacts, TransformArtifacts)
    metrics = artifacts.metrics.sort_values(["latitude", "longitude"]).reset_index(drop=True)
    assert len(metrics) == 2

    first_row = metrics.iloc[0]
    assert pytest.approx(first_row["avg_temperature"], 0.01) == 11.0
    assert pytest.approx(first_row["total_precipitation"], 0.01) == 1.0

    raw_pdf = artifacts.raw_timeseries.compute()
    assert {"temperature_2m", "precipitation"}.issubset(raw_pdf.columns)
    assert len(raw_pdf) == 3
    assert set(artifacts.payload_columns) >= {"temperature_2m", "precipitation"}
