import argparse
import re
import psycopg2
from pyspark.sql import SparkSession, functions as F, types as T


def build_spark(app_name: str):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.extraClassPath", "")
        .getOrCreate()
    )


def delete_month(jdbc_url: str, user: str, password: str, yyyymm: int):
    """
    Run DELETE FROM staging.operator_monthly WHERE yyyymm = %s
    using psycopg2 (since Spark JDBC doesn't support deletes).
    """
    m = re.match(r"jdbc:postgresql://([^:/]+)(?::(\d+))?/([^?]+)", jdbc_url)
    if not m:
        raise ValueError(f"Bad JDBC URL: {jdbc_url}")
    host, port, db = m.groups()
    port = port or 5432
    conn = psycopg2.connect(
        host=host, port=port, dbname=db, user=user, password=password
    )
    with conn, conn.cursor() as cur:
        cur.execute("DELETE FROM staging.operator_monthly WHERE yyyymm = %s", (yyyymm,))
    conn.close()


def main(yyyymm: int, jdbc_url: str, user: str, password: str):
    spark = build_spark("transform_operator")

    # Delete existing month before inserting new
    delete_month(jdbc_url, user, password, yyyymm)

    # Read raw JSON rows for the month
    raw_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option(
            "dbtable",
            f"(SELECT yyyymm, raw::text AS raw FROM raw.operator_cycle WHERE yyyymm = {yyyymm}) as x",
        )
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    # Define expected schema (tolerant)
    schema = T.StructType([
        T.StructField("OPERATOR_NO", T.StringType()),
        T.StructField("OPERATOR_NAME", T.StringType()),
        T.StructField("CYCLE_YEAR", T.StringType()),
        T.StructField("CYCLE_MONTH", T.StringType()),
        T.StructField("CYCLE_YEAR_MONTH", T.StringType()),
        T.StructField("OPER_OIL_PROD_VOL", T.StringType()),
        T.StructField("OPER_GAS_PROD_VOL", T.StringType()),
        T.StructField("OPER_COND_PROD_VOL", T.StringType()),
        T.StructField("OPER_CSGD_PROD_VOL", T.StringType()),
    ])

    parsed = raw_df.select(
        "yyyymm",
        F.from_json("raw", schema).alias("j")
    ).select(
        "yyyymm",
        "j.*"
    )

    # Cast helpers
    to_int = F.udf(lambda x: int(x) if x and x.strip().isdigit() else None, T.IntegerType())

    def to_num(col):
        return (
            F.when(F.col(col).isNull() | (F.trim(F.col(col)) == ""), F.lit(0.0))
            .otherwise(F.col(col).cast("double"))
        )

    # Compute yyyymm if missing
    yyyymm_col = F.when(F.col("yyyymm").isNotNull(), F.col("yyyymm")) \
        .otherwise((to_int("CYCLE_YEAR") * 100) + to_int("CYCLE_MONTH"))

    out = parsed.select(
        F.coalesce(to_int("OPERATOR_NO"), F.lit(0)).alias("operator_no"),
        F.col("OPERATOR_NAME").alias("operator_name"),
        yyyymm_col.alias("yyyymm"),
        to_num("OPER_OIL_PROD_VOL").alias("oil_bbl"),
        to_num("OPER_GAS_PROD_VOL").alias("gas_mcf"),
        to_num("OPER_COND_PROD_VOL").alias("cond_bbl"),
        to_num("OPER_CSGD_PROD_VOL").alias("csgd_mcf"),
    ).where(F.col("yyyymm") == F.lit(int(yyyymm)))

    # Write into staging
    out.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "staging.operator_monthly") \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .save()

    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--yyyymm", required=True, type=int)
    ap.add_argument("--jdbc-url", required=True)
    ap.add_argument("--jdbc-user", required=True)
    ap.add_argument("--jdbc-password", required=True)
    args = ap.parse_args()
    main(args.yyyymm, args.jdbc_url, args.jdbc_user, args.jdbc_password)
