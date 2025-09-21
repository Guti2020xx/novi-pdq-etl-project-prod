# dags/pdq_etl_dag.py
import os
import io
import re
import json
from datetime import datetime, timedelta
from psycopg2.extras import Json
import gc


import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import logging
import contextlib


# ---------------- Config ----------------
RAW_DATA_DIR = os.environ.get("RAW_DATA_DIR", "/opt/airflow/data/raw")
OPERATOR_FILE = os.path.join(RAW_DATA_DIR, "OG_OPERATOR_CYCLE_DATA_TABLE.dsv")
LEASE_FILE = os.path.join(RAW_DATA_DIR, "OG_LEASE_CYCLE_DATA_TABLE.dsv")

# One env var powers both SQLAlchemy (extract) and psycopg2 (checks)
# Accepts either postgresql+psycopg2://... or postgresql://...
DB_URL_SQLA = os.environ.get("AIRFLOW_CONN_DWH_POSTGRES")

# Spark JDBC (already set by docker-compose)
JDBC_URL = os.environ["DWH_JDBC_URL"]
JDBC_USER = os.environ["DWH_JDBC_USER"]
JDBC_PASSWORD = os.environ["DWH_JDBC_PASSWORD"]

SPARK_BASE_CMD = (
    "spark-submit --master local[*] "
    "--packages org.postgresql:postgresql:42.7.3 "
)

default_args = {
    "owner": "airflow",
    "retries": 0,
    "depends_on_past": False,
}

# Helpers 
def _parse_pg_env():
    """
    Parse AIRFLOW_CONN_DWH_POSTGRES into psycopg2 params.
    Supports:
      postgresql+psycopg2://user:pass@host:port/db
      postgresql://user:pass@host:port/db
    """
    uri = DB_URL_SQLA
    if not uri:
        raise RuntimeError("AIRFLOW_CONN_DWH_POSTGRES env var is missing.")
    m = re.match(r"postgresql\+psycopg2://([^:]+):([^@]+)@([^:]+):(\d+)/(.*)", uri)
    if not m:
        m = re.match(r"postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.*)", uri)
    if not m:
        raise RuntimeError(f"Bad AIRFLOW_CONN_DWH_POSTGRES URI: {uri}")
    user, pwd, host, port, db = m.groups()
    return dict(user=user, password=pwd, host=host, port=int(port), dbname=db)

def _sqlalchemy_engine():
    if not DB_URL_SQLA:
        raise RuntimeError("AIRFLOW_CONN_DWH_POSTGRES env var is missing.")
    return create_engine(DB_URL_SQLA)

def wait_for_db_fn(**_):
    params = _parse_pg_env()
    conn = psycopg2.connect(**params)
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
    conn.close()

def _yyyymm_from_row(row: dict) -> int:
    for k in ["CYCLE_YEAR_MONTH", "CYCLE_YEAR_MONTH_NO", "CYCLE_YR_MO"]:
        if k in row and str(row[k]).strip():
            try:
                return int(str(row[k]).strip())
            except Exception:
                pass
    y = int(str(row.get("CYCLE_YEAR", "")).strip() or 0)
    m = int(str(row.get("CYCLE_MONTH", "")).strip() or 0)
    if y and m:
        return y * 100 + m
    raise ValueError("Unable to determine yyyymm from row")

def _derive_yyyymm_vectorized(df: pd.DataFrame) -> pd.Series:
    # Prefer the pre-combined columns first
    for col in ["CYCLE_YEAR_MONTH", "CYCLE_YEAR_MONTH_NO", "CYCLE_YR_MO"]:
        if col in df.columns:
            s = df[col].str.strip()
            s = pd.to_numeric(s, errors="coerce")
            if s.notna().any():
                return s.astype("Int64")

    # If only YEAR + MONTH are available
    if "CYCLE_YEAR" in df.columns and "CYCLE_MONTH" in df.columns:
        y = pd.to_numeric(df["CYCLE_YEAR"].str.strip(), errors="coerce")
        m = pd.to_numeric(df["CYCLE_MONTH"].str.strip(), errors="coerce")
        return (y * 100 + m).astype("Int64")

    # Fallback: call row-wise (slower, but safe)
    return df.apply(lambda r: _yyyymm_from_row(r.to_dict()), axis=1)

from psycopg2.extras import Json

import os
import json
import pandas as pd
from sqlalchemy import text
# from sqlalchemy.dialects.postgresql import JSON as json


def _derive_yyyymm_vectorized(df: pd.DataFrame) -> pd.Series:
    """
    Try to derive yyyymm using vectorized ops.
    Falls back to row-wise _yyyymm_from_row if necessary.
    """
    # Pre-combined columns
    for col in ["CYCLE_YEAR_MONTH", "CYCLE_YEAR_MONTH_NO", "CYCLE_YR_MO"]:
        if col in df.columns:
            s = df[col].astype(str).str.strip()
            s = pd.to_numeric(s, errors="coerce")
            if s.notna().any():
                return s.astype("Int64")

    # Separate year + month
    if "CYCLE_YEAR" in df.columns and "CYCLE_MONTH" in df.columns:
        y = pd.to_numeric(df["CYCLE_YEAR"].astype(str).str.strip(), errors="coerce")
        m = pd.to_numeric(df["CYCLE_MONTH"].astype(str).str.strip(), errors="coerce")
        return (y * 100 + m).astype("Int64")

    # Fallback: row-wise
    return df.apply(lambda r: _yyyymm_from_row(r.to_dict()), axis=1)


def _df_to_json_records(df: pd.DataFrame) -> pd.Series:
    """
    Convert dataframe rows to JSON strings, treating null-like values as None.
    Faster than df.apply row-wise json.dumps.
    """
    records = df.drop(columns="__yyyymm").to_dict(orient="records")
    return pd.Series(
        (
            json.dumps({
                k: (None if str(v).strip() in ("", "NULL", "null", "NaN", "nan") else str(v).strip())
                for k, v in rec.items()
            })
            for rec in records
        ),
        index=df.index,
    )


import io
import psycopg2
import json
import pandas as pd
from sqlalchemy import text


def extract_dsv_to_raw(table: str, input_path: str, yyyymm: int, **_):
    """
    Extract a .dsv file into the raw schema.
    - Reads in chunks (default 100k rows) to avoid OOM
    - Uses vectorized yyyymm extraction when possible
    - Deletes existing month slice before inserting
    - Bulk inserts each chunk with psycopg2.Json
    - Skips any rows before 2000-01 (yyyymm < 200001)
    """
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"File not found: {input_path}")

    engine = _sqlalchemy_engine()

    # Clear this month's slice first
    with engine.begin() as conn:
        conn.execute(
            text(f"DELETE FROM raw.{table} WHERE yyyymm = :y"),
            {"y": int(yyyymm)},
        )

    #used 100k chunk size to try and process file faster
    chunksize = 1000000  
    total_inserted = 0

    for i, chunk in enumerate(pd.read_csv(
        input_path,
        sep="}",
        engine="python",
        dtype=str,
        keep_default_na=False,
        na_values=["", "NULL", "null", "NaN", "nan"],
        quoting=3,  # treat quotes literally
        chunksize=chunksize,
    )):
        print(f"[extract:{table}] Processing chunk {i} with {len(chunk)} rows")

        # Clean column names
        chunk.columns = [c.strip() for c in chunk.columns]

        # Vectorized __yyyymm
        chunk["__yyyymm"] = _derive_yyyymm_vectorized(chunk)

        # Drop invalid + pre-2000 data
        chunk = chunk[chunk["__yyyymm"].notna()]
        chunk = chunk[chunk["__yyyymm"].astype(int) >= 200001]

        # Filter only this slice
        filtered = chunk[
            chunk["__yyyymm"].astype(int) == int(yyyymm)
        ]

        if filtered.empty:
            continue

        # Build payload
        payload = pd.DataFrame({
            "yyyymm": filtered["__yyyymm"].astype(int),
            "raw": _df_to_json_records(filtered),
        })

        # Build rows for bulk insert
        rows = [
            {"y": int(rec.yyyymm), "r": Json(json.loads(rec.raw))}
            for rec in payload.itertuples(index=False)
        ]

        # Bulk insert in one DB call
        with engine.begin() as conn:
            conn.execute(
                text(f"INSERT INTO raw.{table} (yyyymm, raw) VALUES (:y, :r)"),
                rows
            )

        total_inserted += len(rows)
        print(f"[extract:{table}] Finished chunk {i}, inserted {len(rows)} rows. "
              f"Total so far: {total_inserted}")
        del chunk
        del filtered
        del payload
        del rows
        gc.collect()

    print(f"[extract:{table}] done. Inserted {total_inserted} rows for yyyymm={yyyymm}")



def dq_rollup_log_fn(yyyymm: int, **_):
    params = _parse_pg_env() #includes district and field level roll ups
    sql = """   
        WITH lease_by_op AS (
        SELECT operator_no,
                SUM(COALESCE(oil_bbl,0)) AS oil_bbl,
                SUM(COALESCE(gas_mcf,0)) AS gas_mcf,
                SUM(COALESCE(cond_bbl,0)) AS cond_bbl,
                SUM(COALESCE(csgd_mcf,0)) AS csgd_mcf
        FROM staging.lease_monthly
        WHERE yyyymm = %s
        GROUP BY operator_no
        ),
        lease_by_dist AS (
        SELECT district_no,
                SUM(COALESCE(oil_bbl,0)) AS oil_bbl,
                SUM(COALESCE(gas_mcf,0)) AS gas_mcf,
                SUM(COALESCE(cond_bbl,0)) AS cond_bbl,
                SUM(COALESCE(csgd_mcf,0)) AS csgd_mcf
        FROM staging.lease_monthly
        WHERE yyyymm = %s
        GROUP BY district_no
        ),
        lease_by_field AS (
        SELECT field_no,
                SUM(COALESCE(oil_bbl,0)) AS oil_bbl,
                SUM(COALESCE(gas_mcf,0)) AS gas_mcf,
                SUM(COALESCE(cond_bbl,0)) AS cond_bbl,
                SUM(COALESCE(csgd_mcf,0)) AS csgd_mcf
        FROM staging.lease_monthly
        WHERE yyyymm = %s
        GROUP BY field_no
        ),
        op_rollup AS (
        SELECT operator_no, yyyymm,
                SUM(oil_bbl) AS oil_bbl,
                SUM(gas_mcf) AS gas_mcf,
                SUM(cond_bbl) AS cond_bbl,
                SUM(csgd_mcf) AS csgd_mcf
        FROM staging.operator_monthly
        WHERE yyyymm = %s
        GROUP BY operator_no, yyyymm
        ),
        dist_rollup AS (
        SELECT district_no, yyyymm,
                SUM(oil_bbl) AS oil_bbl,
                SUM(gas_mcf) AS gas_mcf,
                SUM(cond_bbl) AS cond_bbl,
                SUM(csgd_mcf) AS csgd_mcf
        FROM staging.operator_monthly
        WHERE yyyymm = %s
        GROUP BY district_no, yyyymm
        ),
        field_rollup AS (
        SELECT field_no, yyyymm,
                SUM(oil_bbl) AS oil_bbl,
                SUM(gas_mcf) AS gas_mcf,
                SUM(cond_bbl) AS cond_bbl,
                SUM(csgd_mcf) AS csgd_mcf
        FROM staging.operator_monthly
        WHERE yyyymm = %s
        GROUP BY field_no, yyyymm
        )
        SELECT 'operator_vs_lease' AS check_type,
            o.operator_no,
            o.oil_bbl AS op_oil, l.oil_bbl AS lease_oil,
            o.gas_mcf AS op_gas, l.gas_mcf AS lease_gas,
            o.cond_bbl AS op_cond, l.cond_bbl AS lease_cond,
            o.csgd_mcf AS op_csgd, l.csgd_mcf AS lease_csgd
        FROM op_rollup o
        LEFT JOIN lease_by_op l USING (operator_no)
        WHERE (
        ABS(COALESCE(o.oil_bbl,0) - COALESCE(l.oil_bbl,0)) > 0.5 OR
        ABS(COALESCE(o.gas_mcf,0) - COALESCE(l.gas_mcf,0)) > 0.5 OR
        ABS(COALESCE(o.cond_bbl,0) - COALESCE(l.cond_bbl,0)) > 0.5 OR
        ABS(COALESCE(o.csgd_mcf,0) - COALESCE(l.csgd_mcf,0)) > 0.5
        )

        UNION ALL

        SELECT 'district_rollup' AS check_type,
            o.district_no,
            o.oil_bbl AS op_oil, l.oil_bbl AS lease_oil,
            o.gas_mcf AS op_gas, l.gas_mcf AS lease_gas,
            o.cond_bbl AS op_cond, l.cond_bbl AS lease_cond,
            o.csgd_mcf AS op_csgd, l.csgd_mcf AS lease_csgd
        FROM dist_rollup o
        LEFT JOIN lease_by_dist l USING (district_no)
        WHERE (
        ABS(COALESCE(o.oil_bbl,0) - COALESCE(l.oil_bbl,0)) > 0.5 OR
        ABS(COALESCE(o.gas_mcf,0) - COALESCE(l.gas_mcf,0)) > 0.5 OR
        ABS(COALESCE(o.cond_bbl,0) - COALESCE(l.cond_bbl,0)) > 0.5 OR
        ABS(COALESCE(o.csgd_mcf,0) - COALESCE(l.csgd_mcf,0)) > 0.5
        )

        UNION ALL

        SELECT 'field_rollup' AS check_type,
            o.field_no,
            o.oil_bbl AS op_oil, l.oil_bbl AS lease_oil,
            o.gas_mcf AS op_gas, l.gas_mcf AS lease_gas,
            o.cond_bbl AS op_cond, l.cond_bbl AS lease_cond,
            o.csgd_mcf AS op_csgd, l.csgd_mcf AS lease_csgd
        FROM field_rollup o
        LEFT JOIN lease_by_field l USING (field_no)
        WHERE (
        ABS(COALESCE(o.oil_bbl,0) - COALESCE(l.oil_bbl,0)) > 0.5 OR
        ABS(COALESCE(o.gas_mcf,0) - COALESCE(l.gas_mcf,0)) > 0.5 OR
        ABS(COALESCE(o.cond_bbl,0) - COALESCE(l.cond_bbl,0)) > 0.5 OR
        ABS(COALESCE(o.csgd_mcf,0) - COALESCE(l.csgd_mcf,0)) > 0.5
        )
        ORDER BY check_type;
        """

    conn = psycopg2.connect(**params)
    with conn:
        with conn.cursor() as cur: # 6 times called now
            cur.execute(sql, (int(yyyymm), int(yyyymm), int(yyyymm), int(yyyymm), int(yyyymm), int(yyyymm),))
            rows = cur.fetchall()
            if rows:
                print(f"[DQ] Found {len(rows)} operator vs lease rollup mismatches for {yyyymm}:")
                for r in rows[:50]:
                    print("  MISMATCH:", r)
            else:
                print(f"[DQ] No rollup mismatches for {yyyymm}.")
    conn.close()

def dq_non_negative_fn(yyyymm: int, **_):
    params = _parse_pg_env()
    conn = psycopg2.connect(**params)
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
              SELECT COUNT(*) FROM staging.operator_monthly
              WHERE yyyymm=%s AND (oil_bbl<0 OR gas_mcf<0 OR cond_bbl<0 OR csgd_mcf<0);
            """, (int(yyyymm),))
            if cur.fetchone()[0] > 0:
                raise RuntimeError(f"Negative volumes in staging.operator_monthly for {yyyymm}")
            cur.execute("""
              SELECT COUNT(*) FROM staging.lease_monthly
              WHERE yyyymm=%s AND (oil_bbl<0 OR gas_mcf<0 OR cond_bbl<0 OR csgd_mcf<0);
            """, (int(yyyymm),))
            if cur.fetchone()[0] > 0:
                raise RuntimeError(f"Negative volumes in staging.lease_monthly for {yyyymm}")
    conn.close()

def dq_uniques_fn(yyyymm: int, **_):
    params = _parse_pg_env()
    conn = psycopg2.connect(**params)
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
              SELECT COUNT(*) FROM (
                SELECT operator_no, yyyymm, COUNT(*) c
                FROM staging.operator_monthly
                WHERE yyyymm=%s
                GROUP BY operator_no, yyyymm
                HAVING COUNT(*) > 1
              ) t;
            """, (int(yyyymm),))
            if cur.fetchone()[0] > 0:
                raise RuntimeError(f"Duplicates in staging.operator_monthly for {yyyymm}")
            cur.execute("""
              SELECT COUNT(*) FROM (
                SELECT lease_key, yyyymm, COUNT(*) c
                FROM staging.lease_monthly
                WHERE yyyymm=%s
                GROUP BY lease_key, yyyymm
                HAVING COUNT(*) > 1
              ) t2;
            """, (int(yyyymm),))
            if cur.fetchone()[0] > 0:
                raise RuntimeError(f"Duplicates in staging.lease_monthly for {yyyymm}")
    conn.close()

# ---------------- DAG ----------------
with DAG(
    dag_id="pdq_etl_dag",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={"yyyymm": 202401},
    dagrun_timeout=timedelta(hours=4),
    description="Novi-style Layer 1–2 ETL for RRC PDQ (raw -> staging -> curated)",
) as dag:

    wait_for_db = PythonOperator(
        task_id="wait_for_db",
        python_callable=wait_for_db_fn,
    )

    def _get_yyyymm(context):
        return int(context["dag_run"].conf.get("yyyymm", context["params"]["yyyymm"]))

    extract_raw_operator = PythonOperator(
        task_id="extract_raw_operator",
        python_callable=lambda **ctx: extract_dsv_to_raw(
            table="operator_cycle",
            input_path=OPERATOR_FILE,
            yyyymm=_get_yyyymm(ctx),
        ),
    )

    extract_raw_lease = PythonOperator(
        task_id="extract_raw_lease",
        python_callable=lambda **ctx: extract_dsv_to_raw(
            table="lease_cycle",
            input_path=LEASE_FILE,
            yyyymm=_get_yyyymm(ctx),
        ),
    )

    spark_transform_operator = BashOperator(
    task_id="spark_transform_operator",
    bash_command=SPARK_BASE_CMD +
    " /opt/airflow/spark_jobs/transform_operator.py "
    " --yyyymm {{ dag_run.conf.get('yyyymm', params.yyyymm) }} "
    f" --jdbc-url '{JDBC_URL}' "
    f" --jdbc-user '{JDBC_USER}' "
    f" --jdbc-password '{JDBC_PASSWORD}' ")

    spark_transform_lease = BashOperator(
        task_id="spark_transform_lease",
        bash_command=SPARK_BASE_CMD +
        " /opt/airflow/spark_jobs/transform_lease.py "
        " --yyyymm {{ dag_run.conf.get('yyyymm', params.yyyymm) }} "
        f" --jdbc-url '{JDBC_URL}' "
        f" --jdbc-user '{JDBC_USER}' "
        f" --jdbc-password '{JDBC_PASSWORD}' "
    )

    spark_model_curated = BashOperator(
        task_id="spark_model_curated",
        bash_command=SPARK_BASE_CMD +
        " /opt/airflow/spark_jobs/model_curated.py "
        " --yyyymm {{ dag_run.conf.get('yyyymm', params.yyyymm) }} "
        f" --jdbc-url '{JDBC_URL}' "
        f" --jdbc-user '{JDBC_USER}' "
        f" --jdbc-password '{JDBC_PASSWORD}' "
        )
    

    dq_non_negative = PythonOperator(
        task_id="dq_non_negative",
        python_callable=lambda **ctx: dq_non_negative_fn(_get_yyyymm(ctx)),
    )

    dq_uniques = PythonOperator(
        task_id="dq_uniques",
        python_callable=lambda **ctx: dq_uniques_fn(_get_yyyymm(ctx)),
    )

    dq_rollup_log = PythonOperator(
        task_id="dq_rollup_log",
        python_callable=lambda **ctx: dq_rollup_log_fn(_get_yyyymm(ctx)),
    )

    wait_for_db >> [extract_raw_operator, extract_raw_lease]
    [extract_raw_operator, extract_raw_lease] >> spark_transform_operator >> spark_transform_lease >> spark_model_curated
    spark_model_curated >> [dq_non_negative, dq_uniques] >> dq_rollup_log
