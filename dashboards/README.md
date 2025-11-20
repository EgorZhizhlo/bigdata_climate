# Dashboards

Place Grafana / Dash / Streamlit apps here. Each dashboard should contain:

- `README.md` with launch instructions
- `requirements.txt` or `environment.yml` (if extra libs needed)
- Source code/notebooks

Dashboards consume processed data stored in PostgreSQL (`climate_metrics` schema) or parquet
artifacts under `data_processed/`.

## Streamlit dashboard

Примерный сценарий визуализации располагается в `streamlit_dashboard.py`. Он подключается
к PostgreSQL и отображает:

* линию средней температуры по выбранной локации;
* столбцы суммарных осадков;
* ключевые показатели последнего дня.

### Запуск

1. Убедись, что Prefect flow сформировал таблицу с метриками (`climate.climate_metrics_staging`
   или `climate.climate_metrics`) и `.env` содержит корректный `POSTGRES_DSN`.
2. Активируй виртуальное окружение и установи зависимости (`pip install -r requirements.txt`).
3. Выполни:

   ```bash
   streamlit run dashboards/streamlit_dashboard.py
   ```

4. В интерфейсе выбери локацию (широта/долгота) и изучай графики температуры и осадков.
