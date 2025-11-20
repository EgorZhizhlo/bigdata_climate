# Jupyter Notebooks

This directory stores exploration notebooks that connect to services running inside
`docker-compose`. Keep raw CSVs outside notebooks (already mounted via `/data_raw`).

Recommended workflow:

1. Start the stack: `docker-compose up -d jupyter postgres pgadmin prefect-server dask-scheduler dask-worker-1 dask-worker-2 dask-worker-3`.
2. Open http://localhost:8888 and create notebooks inside `/home/jovyan/work`.
3. Use environment variables (see `.env.example`) to connect to PostgreSQL or Prefect.

Use `.ipynb_checkpoints/` for autosaves only (ignored by git).
