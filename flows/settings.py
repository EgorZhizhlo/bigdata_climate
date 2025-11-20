# Shared configuration helpers for Prefect flows.
"""Этот файл хранит все пути и строки подключения, чтобы потоки Prefect не искали их по всему проекту."""

from __future__ import annotations

import os
from pathlib import Path


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


DATA_RAW_DIR = Path(os.getenv("DATA_RAW_DIR", project_root() / "data_raw"))
DATA_PROCESSED_DIR = Path(
    os.getenv("DATA_PROCESSED_DIR", project_root() / "data_processed")
)
POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN",
    "postgresql+psycopg2://climate:climate@postgres:5432/climate_db",
)

DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
