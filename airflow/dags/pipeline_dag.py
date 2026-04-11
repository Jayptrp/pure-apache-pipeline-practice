"""
POS Pipeline DAG — Stage 4

Orchestrates the Spark Structured Streaming job and runs batch data
quality checks on the Iceberg table.

Design note on streaming + Airflow:
    Spark Structured Streaming jobs run indefinitely and do not return an
    exit code. Airflow expects tasks to start, do work, and exit. The pattern
    used here:
      Task 1 (start_spark_streaming): launches spark-submit via subprocess.Popen,
        writes the PID to a file, and succeeds immediately. The streaming job
        continues running in the background.
      Task 2 (data_quality_check): waits one microbatch interval, then runs a
        lightweight SparkSession to assert data quality on the Iceberg table.

    In production this job would be submitted to a YARN/K8s cluster and tracked
    by application ID, not a local PID. This local pattern is documented
    explicitly as a trade-off in the README Design Decisions section.

Airflow 3.x notes:
    - Import from airflow.sdk, not airflow.models (legacy still works but dated)
    - airflow db init → airflow db migrate
    - TaskFlow API (@dag / @task decorators) is the idiomatic 3.x style
"""

import os
import signal
import subprocess
import time
from datetime import datetime, timedelta

from airflow.sdk import dag, task

WAREHOUSE_PATH    = os.environ.get("WAREHOUSE_PATH", "/opt/warehouse")
ICEBERG_TABLE     = "local.db.pos_transactions"
SPARK_SUBMIT      = "/opt/spark/bin/spark-submit"
STREAM_SCRIPT     = "/opt/spark/work-dir/stream_processor.py"
PID_FILE          = "/opt/airflow/spark_job.pid"
MICROBATCH_WAIT_S = 15  # seconds — wait for one 10s microbatch + margin


@dag(
    dag_id="pos_pipeline",
    schedule=None,           # Manual trigger — appropriate for a streaming job
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["pos", "streaming", "iceberg"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    doc_md="""
### POS Streaming Pipeline

Starts the Kafka → Spark → Iceberg streaming job and validates data quality.

**Tasks:**
1. `start_spark_streaming` — launches `spark-submit` in the background,
   records PID. Idempotent: skips launch if the process is already running.
2. `data_quality_check` — waits one microbatch, then asserts:
   - Row count > 0
   - No null `transaction_id` values
   - No `failed` payment records (should be filtered by Spark)

**Design trade-off:** Airflow is not designed to manage long-running streaming
jobs. The PID-file pattern is the correct local-dev solution. In production,
use `SparkSubmitOperator` against a YARN/K8s cluster and track by app ID.
    """,
)
def pos_pipeline():

    @task()
    def start_spark_streaming() -> int:
        """
        Launch the Spark streaming job as a background process.
        Writes the PID to a file for idempotency and health monitoring.
        This task succeeds immediately after spawning the process.
        """
        # Idempotency check — skip if the process is already running
        if os.path.exists(PID_FILE):
            with open(PID_FILE) as f:
                existing_pid = int(f.read().strip())
            try:
                os.kill(existing_pid, 0)  # signal 0 = existence check, no signal sent
                print(f"[INFO] Spark streaming job already running (PID {existing_pid}). Skipping launch.")
                return existing_pid
            except ProcessLookupError:
                print("[INFO] Stale PID file found. Launching new job.")

        proc = subprocess.Popen(
            [
                SPARK_SUBMIT,
                "--master", "local[2]",
                STREAM_SCRIPT,
            ],
            env={
                **os.environ,
                "WRITE_MODE": "iceberg",
                "WAREHOUSE_PATH": WAREHOUSE_PATH,
            },
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        os.makedirs(os.path.dirname(PID_FILE), exist_ok=True)
        with open(PID_FILE, "w") as f:
            f.write(str(proc.pid))

        print(f"[INFO] Spark streaming job launched (PID {proc.pid}).")
        return proc.pid

    @task()
    def data_quality_check(spark_pid: int) -> dict:
        """
        Wait for one microbatch, then assert data quality on the Iceberg table.
        Runs a lightweight SparkSession (local[1], 1g) separate from the
        streaming job's JVM — safe because they are different OS processes.
        Calls spark.stop() immediately to free resources.
        """
        from pyspark.sql import SparkSession

        print(f"[INFO] Waiting {MICROBATCH_WAIT_S}s for first microbatch (streaming PID {spark_pid})...")
        time.sleep(MICROBATCH_WAIT_S)

        spark = (
            SparkSession.builder
            .appName("POSDQCheck")
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

        try:
            row_count = spark.sql(
                f"SELECT COUNT(*) AS cnt FROM {ICEBERG_TABLE}"
            ).collect()[0]["cnt"]

            null_txn_ids = spark.sql(
                f"SELECT COUNT(*) AS cnt FROM {ICEBERG_TABLE} WHERE transaction_id IS NULL"
            ).collect()[0]["cnt"]

            failed_payments = spark.sql(
                f"SELECT COUNT(*) AS cnt FROM {ICEBERG_TABLE} WHERE payment_status = 'failed'"
            ).collect()[0]["cnt"]

        finally:
            spark.stop()  # release resources immediately

        # Assertions — any failure raises AssertionError which Airflow marks as task failure
        assert row_count > 0, \
            f"DQ FAIL: Iceberg table '{ICEBERG_TABLE}' has 0 rows after {MICROBATCH_WAIT_S}s."
        assert null_txn_ids == 0, \
            f"DQ FAIL: {null_txn_ids} null transaction_id(s) found."
        assert failed_payments == 0, \
            f"DQ FAIL: {failed_payments} 'failed' payment(s) found — filter in stream_processor.py is broken."

        result = {
            "row_count": row_count,
            "null_transaction_ids": null_txn_ids,
            "failed_payments": failed_payments,
        }
        print(f"[INFO] DQ PASS: {result}")
        return result

    # Task dependency: DQ check receives the PID from the launch task
    pid = start_spark_streaming()
    data_quality_check(pid)


pos_pipeline_dag = pos_pipeline()
