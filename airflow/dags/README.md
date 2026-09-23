# Airflow DAGs — owned by Member C, calling into everyone's scripts

- bootstrap_dag.py — one-time full load (bulk, all 3 sources) → Bronze → Silver → Gold
- monthly_incremental_dag.py — parametrized by month, runs each person's
  bronze_load_*.py + dbt run for silver/gold, then updates the control table.

Each source-owner is responsible for making sure their own script works when
called with the batch/month parameter — Member C just wires the calls together.
