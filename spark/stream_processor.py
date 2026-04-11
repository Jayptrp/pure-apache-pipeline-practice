"""
POS Stream Processor — Stage 2 (Parquet) → Stage 3 (Iceberg)

Consumes the raw_pos_transactions Kafka topic using Spark Structured
Streaming, validates and casts types, filters out failed payments,
and writes clean records to an Apache Iceberg table.

Stage 2 milestone: output format is "parquet" written to /opt/warehouse/stage2_output
Stage 3 milestone: output format is "iceberg" written to local.db.pos_transactions

Switch between stages by toggling WRITE_MODE below (or via env var).
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, size, expr
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC     = os.environ.get("KAFKA_TOPIC", "raw_pos_transactions")
DRIVER_MEMORY   = os.environ.get("SPARK_DRIVER_MEMORY", "2g")
WAREHOUSE_PATH  = os.environ.get("WAREHOUSE_PATH", "/opt/warehouse")
WRITE_MODE      = os.environ.get("WRITE_MODE", "iceberg")  # "parquet" | "iceberg"

CHECKPOINT_PARQUET = "/opt/checkpoints/stage2"
CHECKPOINT_ICEBERG = "/opt/checkpoints/iceberg"
PARQUET_OUTPUT     = f"{WAREHOUSE_PATH}/stage2_output"
ICEBERG_TABLE      = "local.db.pos_transactions"

# ---------------------------------------------------------------------------
# JSON schema — must exactly match the FR-1 payload from pos_generator.py
# Defined explicitly; never use schema_of_json() inference (nondeterministic)
# ---------------------------------------------------------------------------
ITEM_SCHEMA = StructType([
    StructField("sku",        StringType(), True),
    StructField("name",       StringType(), True),
    StructField("category",   StringType(), True),
    StructField("quantity",   IntegerType(), True),
    StructField("unit_price", DoubleType(),  True),
])

TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id",  StringType(),        True),
    StructField("store_id",        StringType(),        True),
    StructField("register_id",     StringType(),        True),
    StructField("timestamp",       StringType(),        True),  # cast to Timestamp below
    StructField("items",           ArrayType(ITEM_SCHEMA), True),
    StructField("subtotal",        DoubleType(),        True),
    StructField("tax",             DoubleType(),        True),
    StructField("total",           DoubleType(),        True),
    StructField("payment_method",  StringType(),        True),
    StructField("payment_status",  StringType(),        True),
    StructField("customer_id",     StringType(),        True),
])


def build_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("POSStreamProcessor")
        .master("local[2]")  # 2 threads — not local[*]; avoids CPU starvation under mem pressure
        .config("spark.driver.memory", DRIVER_MEMORY)
        .config("spark.driver.memoryOverhead", "512m")
        .config("spark.sql.shuffle.partitions", "4")  # default 200 creates tiny files per microbatch
    )

    if WRITE_MODE == "iceberg":
        builder = (
            builder
            .config("spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.local",
                    "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", WAREHOUSE_PATH)
        )

    return builder.getOrCreate()


def create_iceberg_table(spark: SparkSession) -> None:
    """
    Create the Iceberg table once at startup if it does not exist.
    Uses hidden partitioning (days + store_id) — a key Iceberg differentiator:
    files are partitioned without exposing partition columns in the schema.
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_TABLE} (
            transaction_id          STRING,
            store_id                STRING,
            register_id             STRING,
            transaction_timestamp   TIMESTAMP,
            subtotal                DOUBLE,
            tax                     DOUBLE,
            total                   DOUBLE,
            payment_method          STRING,
            payment_status          STRING,
            customer_id             STRING,
            item_count              INT
        )
        USING iceberg
        PARTITIONED BY (days(transaction_timestamp), store_id)
    """)
    print(f"[INFO] Iceberg table '{ICEBERG_TABLE}' ready.", flush=True)


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    if WRITE_MODE == "iceberg":
        create_iceberg_table(spark)

    # -----------------------------------------------------------------------
    # 1. Read from Kafka
    # -----------------------------------------------------------------------
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")  # safe if Kafka topic offsets are compacted
        .load()
    )

    # -----------------------------------------------------------------------
    # 2. Parse JSON and cast types
    # -----------------------------------------------------------------------
    parsed_df = (
        raw_df
        .select(from_json(col("value").cast("string"), TRANSACTION_SCHEMA).alias("d"))
        .select("d.*")
        .withColumnRenamed("timestamp", "transaction_timestamp")
        .withColumn("transaction_timestamp",
                    col("transaction_timestamp").cast(TimestampType()))
        .withColumn("item_count", size(col("items")))
        # Drop the nested items array from the flat output (stored implicitly in item_count)
        .drop("items")
    )

    # -----------------------------------------------------------------------
    # 3. Filter — allowlist valid statuses; excludes "failed" and any nulls
    # -----------------------------------------------------------------------
    clean_df = parsed_df.filter(col("payment_status").isin("completed", "refunded"))

    # -----------------------------------------------------------------------
    # 4. Write sink
    # -----------------------------------------------------------------------
    if WRITE_MODE == "parquet":
        print(f"[INFO] Writing Parquet to {PARQUET_OUTPUT} (Stage 2 mode)", flush=True)
        query = (
            clean_df.writeStream
            .format("parquet")
            .option("path", PARQUET_OUTPUT)
            .option("checkpointLocation", CHECKPOINT_PARQUET)
            .outputMode("append")
            .trigger(processingTime="10 seconds")
            .start()
        )

    elif WRITE_MODE == "iceberg":
        print(f"[INFO] Writing Iceberg to {ICEBERG_TABLE} (Stage 3 mode)", flush=True)
        query = (
            clean_df.writeStream
            .format("iceberg")
            .outputMode("append")
            .option("checkpointLocation", CHECKPOINT_ICEBERG)
            .trigger(processingTime="10 seconds")
            .toTable(ICEBERG_TABLE)  # use toTable(), not .option("path", ...)
        )

    else:
        print(f"[ERROR] Unknown WRITE_MODE: {WRITE_MODE}", file=sys.stderr)
        sys.exit(1)

    print("[INFO] Stream processor running. Awaiting termination...", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
