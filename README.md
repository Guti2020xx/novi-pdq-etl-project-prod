###Oil and Gas ETL

This repo ingests two Texas RRC PDQ `.dsv` files (delimited by `}`) into Postgres, transforms with PySpark (local mode), orchestrates with Airflow, and models raw → staging → curated with basic DQ checks and idempotent monthly loads.

## What you get

- Dockerized Airflow (webserver + scheduler) and Postgres (metadata + DWH in separate DBs)
- OAdminer DB UI (http://localhost:8081)
- Airflow DAG: `pdq_etl_dag`
  - `extract_raw_operator` → raw.operator_cycle (JSONB)
  - `extract_raw_lease` → raw.lease_cycle (JSONB)
  - `spark_transform_operator` → staging.operator_monthly
  - `spark_transform_lease` → staging.lease_monthly
  - `spark_model_curated` → dims/facts in curated schema
  - DQ checks for non-negatives, uniqueness, and operator vs lease rollup logging
