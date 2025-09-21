import argparse
import re
import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession

LOAD_TABLES = {
    "op":    "staging._dim_op_load",
    "dist":  "staging._dim_dist_load",
    "field": "staging._dim_field_load",
    "lease": "staging._dim_lease_load",
}

def build_spark(app_name: str):
    return (SparkSession.builder
            .appName(app_name)
            .getOrCreate())

def _parse_jdbc(jdbc_url: str):
    m = re.match(r"jdbc:postgresql://([^:/]+)(?::(\d+))?/([^?]+)", jdbc_url)
    if not m:
        raise ValueError(f"Bad JDBC URL: {jdbc_url}")
    host, port, db = m.groups()
    return host, int(port or 5432), db

def _connect_pg(jdbc_url: str, user: str, password: str):
    host, port, db = _parse_jdbc(jdbc_url)
    return psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password)

def _ensure_load_tables(jdbc_url, user, password):
    """Create persistent load tables if they don't exist."""
    ddl = [
        f"""CREATE SCHEMA IF NOT EXISTS staging;""",
        f"""CREATE TABLE IF NOT EXISTS {LOAD_TABLES["op"]} (
                operator_no INTEGER,
                operator_name TEXT
            );""",
        f"""CREATE TABLE IF NOT EXISTS {LOAD_TABLES["dist"]} (
                district_no INTEGER
            );""",
        f"""CREATE TABLE IF NOT EXISTS {LOAD_TABLES["field"]} (
                field_no INTEGER
            );""",
        f"""CREATE TABLE IF NOT EXISTS {LOAD_TABLES["lease"]} (
                lease_key TEXT,
                operator_no INTEGER,
                district_no INTEGER,
                field_no INTEGER,
                lease_no INTEGER,
                lease_name TEXT
            );"""
    ]
    conn = _connect_pg(jdbc_url, user, password)
    with conn, conn.cursor() as cur:
        for stmt in ddl:
            cur.execute(stmt)
    conn.close()

def _truncate_load_tables(jdbc_url, user, password):
    conn = _connect_pg(jdbc_url, user, password)
    with conn, conn.cursor() as cur:
        for t in LOAD_TABLES.values():
            cur.execute(f"TRUNCATE {t};")
    conn.close()

def delete_month(jdbc_url, user, password, table, yyyymm):
    conn = _connect_pg(jdbc_url, user, password)
    with conn, conn.cursor() as cur:
        cur.execute(f"DELETE FROM {table} WHERE yyyymm = %s", (yyyymm,))
    conn.close()

def upsert_dims(jdbc_url, user, password):
    conn = _connect_pg(jdbc_url, user, password)
    with conn, conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO curated.dim_operator(operator_no, operator_name)
            SELECT operator_no, operator_name FROM {LOAD_TABLES["op"]}
            ON CONFLICT (operator_no)
            DO UPDATE SET operator_name = EXCLUDED.operator_name;
        """)
        cur.execute(f"""
            INSERT INTO curated.dim_district(district_no)
            SELECT district_no FROM {LOAD_TABLES["dist"]}
            ON CONFLICT (district_no) DO NOTHING;
        """)
        cur.execute(f"""
            INSERT INTO curated.dim_field(field_no)
            SELECT field_no FROM {LOAD_TABLES["field"]}
            ON CONFLICT (field_no) DO NOTHING;
        """)
        cur.execute(f"""
            INSERT INTO curated.dim_lease(lease_key, operator_no, district_no, field_no, lease_no, lease_name)
            SELECT lease_key, operator_no, district_no, field_no, lease_no, lease_name FROM {LOAD_TABLES["lease"]}
            ON CONFLICT (lease_key) DO UPDATE
              SET operator_no = EXCLUDED.operator_no,
                  district_no = EXCLUDED.district_no,
                  field_no = EXCLUDED.field_no,
                  lease_no = EXCLUDED.lease_no,
                  lease_name = EXCLUDED.lease_name;
        """)
    conn.close()

def main(yyyymm: int, jdbc_url: str, user: str, password: str):
    spark = build_spark("model_curated")

    # Make sure our persistent load tables exist and are empty
    _ensure_load_tables(jdbc_url, user, password)
    _truncate_load_tables(jdbc_url, user, password)

    # Read staging slices
    op = (spark.read.format("jdbc")
          .option("url", jdbc_url)
          .option("dbtable", f"(SELECT * FROM staging.operator_monthly WHERE yyyymm = {yyyymm}) x")
          .option("user", user).option("password", password)
          .option("driver", "org.postgresql.Driver").load())

    lease = (spark.read.format("jdbc")
          .option("url", jdbc_url)
          .option("dbtable", f"(SELECT * FROM staging.lease_monthly WHERE yyyymm = {yyyymm}) x")
          .option("user", user).option("password", password)
          .option("driver", "org.postgresql.Driver").load())

    # ---------- Load dim staging (persistent tables) ----------
    # dim_operator load
    op.select("operator_no", "operator_name") \
      .dropDuplicates(["operator_no"]) \
      .write.mode("append").format("jdbc") \
      .option("url", jdbc_url).option("dbtable", LOAD_TABLES["op"]) \
      .option("user", user).option("password", password) \
      .option("driver", "org.postgresql.Driver").save()

    # dim_district load
    lease.select("district_no").dropna().dropDuplicates(["district_no"]) \
      .write.mode("append").format("jdbc") \
      .option("url", jdbc_url).option("dbtable", LOAD_TABLES["dist"]) \
      .option("user", user).option("password", password) \
      .option("driver", "org.postgresql.Driver").save()

    # dim_field load
    lease.select("field_no").dropna().dropDuplicates(["field_no"]) \
      .write.mode("append").format("jdbc") \
      .option("url", jdbc_url).option("dbtable", LOAD_TABLES["field"]) \
      .option("user", user).option("password", password) \
      .option("driver", "org.postgresql.Driver").save()

    # dim_lease load
    lease.select("lease_key", "operator_no", "district_no", "field_no", "lease_no", "lease_name") \
      .dropDuplicates(["lease_key"]) \
      .write.mode("append").format("jdbc") \
      .option("url", jdbc_url).option("dbtable", LOAD_TABLES["lease"]) \
      .option("user", user).option("password", password) \
      .option("driver", "org.postgresql.Driver").save()

    # Upsert to curated dims
    upsert_dims(jdbc_url, user, password)

    # ---------- Facts (idempotent) ----------
    for tbl in ["curated.fact_operator_monthly", "curated.fact_lease_monthly"]:
        delete_month(jdbc_url, user, password, tbl, yyyymm)

    op.select("operator_no", "yyyymm", "oil_bbl", "gas_mcf", "cond_bbl", "csgd_mcf") \
      .write.mode("append").format("jdbc") \
      .option("url", jdbc_url).option("dbtable", "curated.fact_operator_monthly") \
      .option("user", user).option("password", password) \
      .option("driver", "org.postgresql.Driver").save()

    lease.select("lease_key", "operator_no", "district_no", "field_no", "yyyymm", "oil_bbl", "gas_mcf", "cond_bbl", "csgd_mcf") \
      .write.mode("append").format("jdbc") \
      .option("url", jdbc_url).option("dbtable", "curated.fact_lease_monthly") \
      .option("user", user).option("password", password) \
      .option("driver", "org.postgresql.Driver").save()

    spark.stop()

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--yyyymm", required=True, type=int)
    ap.add_argument("--jdbc-url", required=True)
    ap.add_argument("--jdbc-user", required=True)
    ap.add_argument("--jdbc-password", required=True)
    args = ap.parse_args()
    main(args.yyyymm, args.jdbc_url, args.jdbc_user, args.jdbc_password)
