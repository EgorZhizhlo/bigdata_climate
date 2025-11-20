"""Streamlit dashboard для визуализации климатических метрик."""

from __future__ import annotations

import os
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN",
    "postgresql+psycopg2://climate:climate@localhost:5432/climate_db",
)
METRICS_TABLE = os.getenv(
    "CLIMATE_METRICS_TABLE",
    "climate.climate_metrics_staging",
)


@st.cache_data(ttl=600)
def load_metrics() -> pd.DataFrame:
    engine = create_engine(POSTGRES_DSN, future=True)
    query = text(
        f"""
        SELECT
            period_start,
            period_level,
            latitude,
            longitude,
            avg_temperature,
            total_precipitation
        FROM {METRICS_TABLE}
        ORDER BY period_start
        """
    )
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, parse_dates=["period_start"])
    except SQLAlchemyError as exc:
        st.error(f"Не удалось загрузить данные: {exc}")
        return pd.DataFrame()
    return df


def format_location(row: pd.Series) -> str:
    lat = row["latitude"]
    lon = row["longitude"]
    return f"{lat:.3f}, {lon:.3f}"


def main() -> None:
    st.set_page_config(page_title="Climate Metrics Dashboard", layout="wide")
    st.title("Климатические метрики")
    st.caption(
        "Данные поступают из таблицы "
        f"`{METRICS_TABLE}` и обновляются при каждом запуске Prefect flow."
    )

    df = load_metrics()
    if df.empty:
        st.warning("Таблица с метриками пуста или недоступна.")
        st.stop()

    df = df.dropna(subset=["latitude", "longitude"]).copy()
    df["location_label"] = df.apply(format_location, axis=1)
    location_options = sorted(df["location_label"].unique())
    selected_location = st.selectbox(
        "Выбери локацию (широта, долгота)",
        location_options,
    )
    location_df = df[df["location_label"] == selected_location]

    col1, col2, col3 = st.columns(3)
    col1.metric(
        "Средняя температура (последний день)",
        f"{location_df['avg_temperature'].iloc[-1]:.1f} °C",
    )
    col2.metric(
        "Суммарные осадки (последний день)",
        f"{location_df['total_precipitation'].iloc[-1]:.1f} мм",
    )
    col3.metric("Количество наблюдений", f"{len(location_df)}")

    temp_fig = px.line(
        location_df,
        x="period_start",
        y="avg_temperature",
        title="Динамика средней температуры",
        labels={"period_start": "Дата", "avg_temperature": "Температура (°C)"},
    )
    precip_fig = px.bar(
        location_df,
        x="period_start",
        y="total_precipitation",
        title="Дневные суммарные осадки",
        labels={"period_start": "Дата", "total_precipitation": "Осадки (мм)"},
    )

    st.plotly_chart(temp_fig, use_container_width=True)
    st.plotly_chart(precip_fig, use_container_width=True)

    with st.expander("Сырые данные"):
        st.dataframe(location_df.sort_values("period_start", ascending=False))


if __name__ == "__main__":
    main()
