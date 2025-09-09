import argparse
import re
import psycopg2
from pyspark.sql import SparkSession, functions as F, types as T


def build_spark(app_name: str):
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


def _connect_pg(jdbc_url: str, user: str, password: str):
    m = re.match(r"jdbc:postgresql://([^:/]+)(?::(\d+))?/([^?]+)", jdbc_url)
    if not m:
        raise ValueError(f"Bad JDBC URL: {jdbc_url}")
    host, port, db = m.groups()
    port = port or 5432
    return psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password)


def delete_month(jdbc_url: str, user: str, password: str, table: str, yyyymm: int):
    conn = _connect_pg(jdbc_url, user, password)
    with conn, conn.cursor() as cur:
        cur.execute(f"DELETE FROM {table} WHERE yyyymm = %s", (yyyymm,))
    conn.close()


def to_int_col(col):
    # robust int cast for stringy numbers
    return F.when(
        (F.col(col).isNull()) | (F.trim(F.col(col)) == ""),
        F.lit(None).cast("int")
    ).otherwise(F.col(col).cast("int"))


def to_num_col(col):
    # robust numeric cast -> double with empty/null -> 0.0
    return F.when(
        (F.col(col).isNull()) | (F.trim(F.col(col)) == ""),
        F.lit(0.0)
    ).otherwise(F.col(col).cast("double"))


def main(yyyymm: int, jdbc_url: str, user: str, password: str):
    spark = build_spark("transform_lease")
    props = {"user": user, "password": password, "driver": "org.postgresql.Driver"}

    # 1) Read raw JSON rows for this month
    raw_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", f"(SELECT yyyymm, raw::text AS raw FROM raw.lease_cycle WHERE yyyymm = {yyyymm}) x")
        .option("user", user).option("password", password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    # 2) Parse JSON
    schema = T.StructType([
        T.StructField("OPERATOR_NO", T.StringType()),
        T.StructField("DISTRICT_NO", T.StringType()),
        T.StructField("FIELD_NO", T.StringType()),
        T.StructField("LEASE_NO", T.StringType()),
        T.StructField("LEASE_NAME", T.StringType()),
        T.StructField("CYCLE_YEAR", T.StringType()),
        T.StructField("CYCLE_MONTH", T.StringType()),
        T.StructField("CYCLE_YEAR_MONTH", T.StringType()),
        # volume variants
        T.StructField("OIL_PROD_VOL", T.StringType()),
        T.StructField("GAS_PROD_VOL", T.StringType()),
        T.StructField("COND_PROD_VOL", T.StringType()),
        T.StructField("CSGD_PROD_VOL", T.StringType()),
        T.StructField("LEASE_OIL_PROD_VOL", T.StringType()),
        T.StructField("LEASE_GAS_PROD_VOL", T.StringType()),
        T.StructField("LEASE_COND_PROD_VOL", T.StringType()),
        T.StructField("LEASE_CSGD_PROD_VOL", T.StringType()),
    ])

    parsed = raw_df.select("yyyymm", F.from_json("raw", schema).alias("j")).select("yyyymm", "j.*")

    # 3) Normalize types & compute lease_key / yyyymm
    y_col = to_int_col("CYCLE_YEAR")
    m_col = to_int_col("CYCLE_MONTH")
    yyyymm_col = F.when(F.col("yyyymm").isNotNull(), F.col("yyyymm")) \
                   .otherwise(y_col * F.lit(100) + m_col)

    oil = F.coalesce(to_num_col("OIL_PROD_VOL"),  to_num_col("LEASE_OIL_PROD_VOL"))
    gas = F.coalesce(to_num_col("GAS_PROD_VOL"),  to_num_col("LEASE_GAS_PROD_VOL"))
    cond = F.coalesce(to_num_col("COND_PROD_VOL"), to_num_col("LEASE_COND_PROD_VOL"))
    csgd = F.coalesce(to_num_col("CSGD_PROD_VOL"), to_num_col("LEASE_CSGD_PROD_VOL"))

    wide = parsed.select(
        F.coalesce(to_int_col("OPERATOR_NO"), F.lit(0)).alias("operator_no"),
        to_int_col("DISTRICT_NO").alias("district_no"),
        to_int_col("FIELD_NO").alias("field_no"),
        to_int_col("LEASE_NO").alias("lease_no"),
        F.col("LEASE_NAME").alias("lease_name"),
        yyyymm_col.alias("yyyymm"),
        oil.alias("oil_bbl"),
        gas.alias("gas_mcf"),
        cond.alias("cond_bbl"),
        csgd.alias("csgd_mcf")
    ).withColumn(
        "lease_key",
        F.concat_ws("-", F.col("district_no").cast("string"), F.col("lease_no").cast("string"))
    ).where(F.col("yyyymm") == F.lit(int(yyyymm)))

    # (optional) quick diagnostic: how many duplicate rows per (lease_key, yyyymm)?
    dupes = (wide.groupBy("lease_key", "yyyymm").count()
                  .where(F.col("count") > 1)
                  .orderBy(F.desc("count")))
    dupes_count = dupes.count()
    print(f"[transform_lease] duplicate (lease_key,yyyymm) groups in source for {yyyymm}: {dupes_count}")
    if dupes_count > 0:
        dupes.show(15, truncate=False)

    # 4) **Aggregate** to enforce one row per (lease_key, yyyymm)
    agg = (wide
           .groupBy("lease_key", "yyyymm")
           .agg(
               F.first("operator_no", ignorenulls=True).alias("operator_no"),
               F.first("district_no", ignorenulls=True).alias("district_no"),
               F.first("field_no", ignorenulls=True).alias("field_no"),
               F.first("lease_no", ignorenulls=True).alias("lease_no"),
               F.first("lease_name", ignorenulls=True).alias("lease_name"),
               F.sum("oil_bbl").alias("oil_bbl"),
               F.sum("gas_mcf").alias("gas_mcf"),
               F.sum("cond_bbl").alias("cond_bbl"),
               F.sum("csgd_mcf").alias("csgd_mcf"),
           )
    )

    # 5) Idempotency: delete this month before insert
    delete_month(jdbc_url, user, password, "staging.lease_monthly", yyyymm)

    # 6) Write
    (agg.select("operator_no", "district_no", "field_no", "lease_no", "lease_name",
                "yyyymm", "oil_bbl", "gas_mcf", "cond_bbl", "csgd_mcf", "lease_key")
        .write
        .mode("append")
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "staging.lease_monthly")
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .save()
    )

    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--yyyymm", required=True, type=int)
    ap.add_argument("--jdbc-url", required=True)
    ap.add_argument("--jdbc-user", required=True)
    ap.add_argument("--jdbc-password", required=True)
    args = ap.parse_args()
    main(args.yyyymm, args.jdbc_url, args.jdbc_user, args.jdbc_password)
