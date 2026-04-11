"""
Iceberg Verification Script — Stage 3

Run this manually inside the Spark container (or via docker exec) after the
streaming job has produced at least one microbatch to the Iceberg table.
Demonstrates: SQL queries, snapshot history, time-travel, and ACID operations.

Usage (from host):
    docker exec spark python /opt/spark/work-dir/verify_iceberg.py

Expected output: query results printed to stdout for README screenshots.
"""

import os

from pyspark.sql import SparkSession

WAREHOUSE_PATH = os.environ.get("WAREHOUSE_PATH", "/opt/warehouse")
ICEBERG_TABLE  = "local.db.pos_transactions"


def main():
    spark = (
        SparkSession.builder
        .appName("IcebergVerify")
        .master("local[1]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", WAREHOUSE_PATH)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    sep = "-" * 60

    # -----------------------------------------------------------------------
    # 1. Basic row count and recent records
    # -----------------------------------------------------------------------
    print(f"\n{sep}\n1. Row count\n{sep}")
    spark.sql(f"SELECT COUNT(*) AS total_rows FROM {ICEBERG_TABLE}").show()

    print(f"\n{sep}\n2. Latest 5 transactions\n{sep}")
    spark.sql(f"""
        SELECT transaction_id, store_id, total, payment_status, transaction_timestamp
        FROM {ICEBERG_TABLE}
        ORDER BY transaction_timestamp DESC
        LIMIT 5
    """).show(truncate=False)

    # -----------------------------------------------------------------------
    # 2. Aggregate — revenue by store and payment method
    # -----------------------------------------------------------------------
    print(f"\n{sep}\n3. Revenue by store\n{sep}")
    spark.sql(f"""
        SELECT store_id,
               COUNT(*)          AS transactions,
               ROUND(SUM(total), 2) AS total_revenue
        FROM {ICEBERG_TABLE}
        GROUP BY store_id
        ORDER BY total_revenue DESC
    """).show()

    print(f"\n{sep}\n4. Transactions by payment method\n{sep}")
    spark.sql(f"""
        SELECT payment_method, COUNT(*) AS cnt
        FROM {ICEBERG_TABLE}
        GROUP BY payment_method
        ORDER BY cnt DESC
    """).show()

    # -----------------------------------------------------------------------
    # 3. Iceberg snapshot history (time-travel prerequisite)
    # -----------------------------------------------------------------------
    print(f"\n{sep}\n5. Snapshot history\n{sep}")
    snapshots_df = spark.sql(f"SELECT * FROM {ICEBERG_TABLE}.snapshots")
    snapshots_df.show(truncate=False)
    snapshot_count = snapshots_df.count()

    # -----------------------------------------------------------------------
    # 4. Time-travel — query the first snapshot
    # -----------------------------------------------------------------------
    if snapshot_count >= 2:
        first_snapshot_id = snapshots_df.orderBy("committed_at").first()["snapshot_id"]
        print(f"\n{sep}\n6. Time-travel: first snapshot (id={first_snapshot_id})\n{sep}")
        spark.sql(f"""
            SELECT transaction_id, total, transaction_timestamp
            FROM {ICEBERG_TABLE} VERSION AS OF {first_snapshot_id}
            LIMIT 5
        """).show(truncate=False)
    else:
        print(f"\n[INFO] Only {snapshot_count} snapshot(s) so far — "
              "run more microbatches to demonstrate time-travel.")

    # -----------------------------------------------------------------------
    # 5. ACID operations — DELETE refunded transactions
    # -----------------------------------------------------------------------
    print(f"\n{sep}\n7. ACID DELETE: remove refunded transactions\n{sep}")
    before = spark.sql(f"SELECT COUNT(*) AS cnt FROM {ICEBERG_TABLE} WHERE payment_status='refunded'").collect()[0]["cnt"]
    print(f"   Refunded rows before DELETE: {before}")

    spark.sql(f"DELETE FROM {ICEBERG_TABLE} WHERE payment_status = 'refunded'")

    after = spark.sql(f"SELECT COUNT(*) AS cnt FROM {ICEBERG_TABLE} WHERE payment_status='refunded'").collect()[0]["cnt"]
    print(f"   Refunded rows after DELETE:  {after}")
    print(f"   ACID DELETE {'PASSED' if after == 0 else 'FAILED'}")

    # -----------------------------------------------------------------------
    # 6. Data quality assertions
    # -----------------------------------------------------------------------
    print(f"\n{sep}\n8. Data quality checks\n{sep}")
    null_ids = spark.sql(f"SELECT COUNT(*) AS cnt FROM {ICEBERG_TABLE} WHERE transaction_id IS NULL").collect()[0]["cnt"]
    failed   = spark.sql(f"SELECT COUNT(*) AS cnt FROM {ICEBERG_TABLE} WHERE payment_status = 'failed'").collect()[0]["cnt"]
    total    = spark.sql(f"SELECT COUNT(*) AS cnt FROM {ICEBERG_TABLE}").collect()[0]["cnt"]

    print(f"   Total rows:           {total}")
    print(f"   Null transaction_ids: {null_ids}  ({'PASS' if null_ids == 0 else 'FAIL'})")
    print(f"   Failed payments:      {failed}   ({'PASS' if failed == 0 else 'FAIL'})")

    spark.stop()
    print(f"\n{sep}\nVerification complete.\n{sep}\n")


if __name__ == "__main__":
    main()
