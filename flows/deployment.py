"""Скрипт создаёт Prefect Deployment для суточного запуска climate_csv_flow."""

from __future__ import annotations

import os
from datetime import timedelta

from prefect.deployments import Deployment
from prefect.server.schemas.schedules import IntervalSchedule

from .etl_flow import climate_csv_flow


def build_and_apply_deployment() -> None:
    deployment_name = os.getenv("PREFECT_DEPLOYMENT_NAME", "climate-csv-daily")
    work_queue = os.getenv("PREFECT_FLOW_QUEUE", "default")
    interval_hours = int(os.getenv("PREFECT_DEPLOYMENT_INTERVAL_HOURS", "24"))
    schedule = IntervalSchedule(interval=timedelta(hours=interval_hours))

    deployment = Deployment.build_from_flow(
        flow=climate_csv_flow,
        name=deployment_name,
        work_queue_name=work_queue,
        schedule=schedule,
        tags=["climate", "etl", "dask"],
        description="Daily ingestion of raw climate CSV files with Prefect and Dask.",
    )
    deployment.apply()


if __name__ == "__main__":
    build_and_apply_deployment()
