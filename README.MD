# Big Data Climate Analysis Project

## Краткое описание
Система для обработки климатических CSV через связку Prefect + Dask + PostgreSQL с визуализацией в Streamlit. Вся инфраструктура разворачивается локально через Docker Compose; отдельная виртуализация или глобальные Python-зависимости не требуются.

## Архитектура
- Prefect Server + Prefect Agent — оркестрация ETL.
- Dask (scheduler + 3 workers) — параллельная обработка CSV.
- PostgreSQL + pgAdmin — хранилище и администрирование.
- Streamlit — визуализация агрегированных метрик.
- JupyterLab — интерактивный анализ.
- Общие тома: `data_raw/` (входные CSV), `data_processed/` (выходные артефакты).

## Структура репозитория
```
bigdata_climate/
├─ data_raw/                # сырые CSV (монтируются в контейнеры)
├─ data_processed/          # результаты Dask/ETL
├─ dashboards/              # Streamlit дашборд
├─ dask_jobs/               # Dask скрипты (анализ аномалий и пр.)
├─ db/schema.sql            # создание схемы climate и таблиц
├─ flows/                   # Prefect flows и вспомогательные модули
├─ tests/                   # pytest и flake8 обёртка
├─ docker-compose.yml
├─ Dockerfile
├─ .env.docker              # окружение для контейнеров
└─ requirements.txt
```

## Зависимости
- Docker и docker compose.
- Образ собирается на Python 3.10, основные версии: pandas 2.1.4, dask 2023.9.3, prefect 2.20.x.

## Подготовка окружения
1. Проверьте, что в `data_raw/` лежат нужные CSV (две секции: метаданные локаций и временные ряды).
2. При необходимости скорректируйте `POSTGRES_DSN`, `PREFECT_API_URL`, пути данных в `.env.docker`.

## Запуск через Docker Compose (полный цикл)
1. Собрать образ и поднять сервисы:
   ```bash
   docker compose --env-file .env.docker build
   docker compose --env-file .env.docker up -d --force-recreate
   ```
2. Применить схему БД (один раз):
   ```bash
   docker compose exec -T postgres psql -U climate -d climate_db < db/schema.sql
   ```
3. Зарегистрировать Prefect deployment:
   ```bash
   docker compose exec app python -m flows.deployment
   ```
4. Выполнить ETL вручную (для первичной загрузки):
   ```bash
   docker compose exec app python -m flows.etl_flow
   ```
5. Проверить, что метрики появились:
   ```bash
   docker compose exec postgres psql -U climate -d climate_db -c "select count(*) from climate.climate_metrics_staging;"
   ```
6. Открыть дашборд: http://localhost:8501 (Streamlit читает `climate.climate_metrics_staging`).

Сервисы по портам: Prefect UI — 4200, Streamlit — 8501, pgAdmin — 5055, JupyterLab — 8888 (без токена).

## Быстрый режим (пропустить загрузку raw)
Если вставка сырых наблюдений не нужна (нужны только агрегаты для дашборда), установите в `.env.docker`:
```
SKIP_RAW_LOAD=1
```
Перезапустите compose и прогоните шаги 3–6 выше. Порядок задач в потоке: локации → метрики → raw; при `SKIP_RAW_LOAD=1` шаг raw пропускается, метрики доступны сразу.

## Ручные команды и задачи
- Разовый анализ аномалий Dask:  
  `docker compose exec app python -m dask_jobs.daily_anomaly_metrics --z-threshold 2.5`
- Запуск deployment по расписанию вручную:  
  `docker compose exec app prefect deployment run climate-csv-daily`

## Формат данных и таблицы
- CSV: секция метаданных локаций (локальный `location_id`, координаты) + секция временных рядов.
- Уникальность локаций — по паре `(latitude, longitude)`, хэш `raw_location_hash`.
- Таблицы:
  - `climate.locations` — справочник локаций.
  - `climate.raw_climate` — сырые наблюдения (JSONB payload).
  - `climate.climate_metrics` и `climate.climate_metrics_staging` — агрегированные показатели.

## Тестирование и проверка стиля
```bash
docker compose exec app pytest              # запустить все тесты
docker compose exec app pytest tests/test_etl_flow.py
docker compose exec app pytest tests/test_flake8.py  # вызывает flake8
```

## Файлы результатов
- `data_processed/climate_metrics_preview.parquet` — fallback, если запись в PostgreSQL недоступна.
- `data_processed/daily_temperature_anomalies.parquet` — результат `dask_jobs/daily_anomaly_metrics.py`.
- PostgreSQL: проверяйте через pgAdmin или `psql` внутри контейнера `postgres`.

## Типичные команды для обслуживания
- Просмотр логов приложения: `docker compose logs -f app`
- Остановка инфраструктуры: `docker compose down`
- Полная пересборка без кэша: `docker compose --env-file .env.docker build --no-cache && docker compose --env-file .env.docker up -d --force-recreate`

## Примечания по совместимости
- Образ фиксирован на Python 3.10 и dask 2023.9.3 из-за проблем с эксперимен-тальным планировщиком dask-expr на более новых связках pandas/dask.
- Перед повторной загрузкой данных убедитесь, что схема `climate` создана (скрипт `db/schema.sql`).
