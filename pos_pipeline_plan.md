
**PURE-APACHE POS**
**STREAMING PIPELINE**

Project Plan & Technical Specification
Development Environment: VSCode + WSL (Ubuntu)
April 2026

**New Graduate Portfolio Project**
Data Platform Engineering Track

# 1. Business Requirements Document (BRD)
## 1.1 Project Title
Pure-Apache POS Streaming Pipeline
## 1.2 Objective
Architect a fault-tolerant, open-source data pipeline using the Apache ecosystem to ingest and process simulated Point of Sale (POS) transactions at a steady, manageable rate of 5–10 transactions per second (TPS). The system runs entirely on a local development machine using VSCode with WSL (Ubuntu), demonstrating production-grade architectural thinking without requiring cloud infrastructure.
## 1.3 Business Value
- Demonstrate the ability to decouple data generation from data processing using distributed messaging (Apache Kafka).
- Establish a reliable foundation for analytics and downstream forecasting models using open-source table formats (Apache Iceberg).
- Prove end-to-end pipeline orchestration capability using Apache Airflow.
- Showcase the complete skill set expected of a Data Platform Engineer: event streaming, stream processing, lakehouse storage, and workflow orchestration.
## 1.4 Target Audience
Hiring managers and technical interviewers evaluating candidates for Data Platform Engineer, Data Engineer, and Analytics Engineer roles.
## 1.5 Constraints
- Runs on a single local machine (WSL2 on Windows) with a minimum of 16 GB RAM.
- All components must be containerized via Docker Compose for reproducibility.
- Throughput is intentionally throttled to 5–10 TPS to prioritize architectural correctness over volume.
- Zero cloud cost. The entire stack runs locally.

# 2. Technology Stack & Version Matrix
All versions are the latest stable releases as of April 2026. Using current versions signals to hiring managers that the candidate stays up to date with the ecosystem.
**Component**
**Version**
**Role in Pipeline**
**Apache Kafka**
**4.x (KRaft)**
**Event broker. Runs in KRaft mode (no ZooKeeper). Queues and distributes POS transaction events.**
**Apache Spark**
**4.0.2 / PySpark**
**Stream processor. Consumes Kafka stream, cleans data, writes to Iceberg tables.**
**Apache Iceberg**
**1.8.x**
**Table format. Provides ACID-compliant SQL queries on Parquet files in the data lake.**
**Apache Parquet**
**(via Spark)**
**Columnar file format. The underlying storage format for Iceberg tables.**
**Apache Airflow**
**3.2.x**
**Orchestrator. Manages lifecycle of Spark jobs and batch data quality checks.**
**Docker Compose**
**v2+**
**Container orchestration. Spins up all services in a reproducible, isolated environment.**
**Python**
**3.12+**
**POS generator script, PySpark jobs, Airflow DAGs.**
**WSL2 (Ubuntu)**
**24.04 LTS**
**Linux environment on Windows. Runs Docker natively with full kernel support.**

**Critical Note on Kafka: **Apache Kafka 4.0 (released March 2025) completely removed ZooKeeper support. The project now runs exclusively in KRaft (Kafka Raft) mode, which simplifies deployment to a single process handling both broker and controller roles. Any tutorial or guide that still references ZooKeeper is outdated. This project uses KRaft mode exclusively.

# 3. Development Environment Setup
## 3.1 Why VSCode + WSL Instead of Codespaces
The original plan suggested GitHub Codespaces. While Codespaces is excellent for lightweight development, this project’s multi-container Docker stack (Kafka + Spark + Airflow + Postgres) demands approximately 10–14 GB of RAM when running simultaneously. The Codespaces free tier provides only a 2-core / 8 GB machine with 60 hours of runtime per month. That is not enough for iterative development on a resource-heavy pipeline. Running locally on WSL eliminates these constraints entirely.
## 3.2 Prerequisites
- **Windows 10/11 with WSL2 enabled** — run wsl --install in PowerShell to set up Ubuntu 24.04 LTS.
- **Docker Desktop for Windows** with the WSL2 backend integration enabled. This lets Docker containers run natively inside your WSL Linux kernel.
- **VSCode** with the “Remote - WSL” extension installed. This allows VSCode to open projects directly inside your WSL filesystem.
- **Minimum 16 GB RAM** (recommended). Allocate at least 10 GB to WSL via .wslconfig.
- **Python 3.12+** and **Java 17+** installed inside WSL (required by PySpark).
## 3.3 WSL Memory Configuration
Create or edit C:\Users\<YourName>\.wslconfig to allocate resources to WSL:
[wsl2]
memory=10GB
processors=4
swap=4GB
## 3.4 Project Directory Structure
**pos-streaming-pipeline/**
├── docker-compose.yml
├── .env
├── generator/
│   ├── pos_generator.py
│   └── requirements.txt
├── spark/
│   ├── Dockerfile
│   └── stream_processor.py
├── airflow/
│   ├── Dockerfile
│   └── dags/
│       └── pipeline_dag.py
├── warehouse/
├── checkpoints/
└── README.md


# 4. Functional Requirements (FR)
Each functional requirement is mapped to a specific Apache technology and a measurable acceptance criterion.
## FR-1: POS Transaction Generation
**Attribute**
**Detail**
**Description**
**The system shall simulate continuous JSON payloads representing POS transactions at a throttled rate of 5–10 events per second.**
**Technology**
**Python script (pos_generator.py) using kafka-python-ng or confluent-kafka library.**
**Acceptance Criteria**
**The generator produces valid JSON with all required schema fields. The rate stays within 5–10 TPS as measured by counting Kafka topic offsets over a 60-second window.**

**JSON Payload Schema:**
  {
    "transaction_id": "uuid-v4",
    "store_id": "STORE-001",
    "register_id": "REG-03",
    "timestamp": "2026-04-09T14:32:01.123Z",
    "items": [
      {
        "sku": "SKU-10042",
        "name": "Organic Whole Milk 1L",
        "category": "Dairy",
        "quantity": 2,
        "unit_price": 4.99
      }
    ],
    "subtotal": 9.98,
    "tax": 0.70,
    "total": 10.68,
    "payment_method": "credit_card",
    "payment_status": "completed",
    "customer_id": "CUST-8827 | null"
  }

## FR-2: Event Ingestion via Apache Kafka
**Attribute**
**Detail**
**Description**
**The system shall utilize Apache Kafka (KRaft mode, no ZooKeeper) to queue and distribute the transaction events via a topic named raw_pos_transactions.**
**Technology**
**Apache Kafka 4.x in KRaft combined mode (single container acting as both broker and controller).**
**Acceptance Criteria**
**Messages are persisted in the Kafka topic and can be consumed independently by multiple consumers. The broker starts and accepts connections within 30 seconds of container startup.**

## FR-3: Stream Processing via Apache Spark
**Attribute**
**Detail**
**Description**
**Apache Spark (Structured Streaming) shall consume the live Kafka stream, parse JSON payloads, cast data types, and filter out failed payment events before writing to storage.**
**Technology**
**Apache Spark 4.0.2 (PySpark) running in local mode with memory capped at 2 GB.**
**Acceptance Criteria**
**Stream processing begins within 10 seconds of Kafka producing messages. All data type casts succeed (price as double, timestamp as TimestampType). Failed payments (payment_status = "failed") are excluded from output.**

## FR-4: Lakehouse Storage via Apache Iceberg + Parquet
**Attribute**
**Detail**
**Description**
**Processed data shall be saved using the Apache Iceberg table format on top of Apache Parquet files, enabling ACID-compliant SQL queries (including UPDATE and DELETE) on the data lake.**
**Technology**
**Apache Iceberg 1.8.x with a Hadoop catalog, writing Parquet files to a local warehouse/ directory.**
**Acceptance Criteria**
**Data is queryable via Spark SQL. Time-travel queries return snapshots of previous data states. An UPDATE or DELETE operation executes successfully on the Iceberg table.**

## FR-5: Workflow Orchestration via Apache Airflow
**Attribute**
**Detail**
**Description**
**Apache Airflow shall manage the lifecycle of the Spark streaming job and run batch data quality checks on the Iceberg table.**
**Technology**
**Apache Airflow 3.2.x with LocalExecutor (no Celery, no Redis, no Flower).**
**Acceptance Criteria**
**The DAG is visible in the Airflow UI at localhost:8080. The Spark job task transitions to "running" and the data quality check task succeeds when records exist in the Iceberg table.**


# 5. Non-Functional Requirements (NFR)
**ID**
**Requirement**
**Detail**
**NFR-1**
**Reproducibility**
**A fresh clone of the repository must start the entire pipeline with a single "docker compose up" command (after initial image pulls).**
**NFR-2**
**Resource Limits**
**Total Docker memory consumption must stay under 12 GB. Each service has explicit memory limits in docker-compose.yml.**
**NFR-3**
**Fault Tolerance**
**If the Spark job crashes, Kafka retains all unprocessed messages. On restart, Spark resumes from its last checkpoint without data loss.**
**NFR-4**
**Observability**
**Airflow UI provides DAG run status and task logs. Kafka topic lag is observable via kafka-consumer-groups.sh CLI.**
**NFR-5**
**Documentation**
**README.md includes architecture diagram (Mermaid or image), setup instructions, demo screenshots, and a description of design decisions.**

# 6. Development Stages
Build the pipeline incrementally. Each stage produces a working, testable component before moving to the next. Do not attempt to run all services simultaneously until Stage 5.
## Stage 1: The Event Broker (Apache Kafka in KRaft Mode)
**Goal: **Set up the messaging backbone and start producing realistic POS events.
**Estimated Time: **2–3 hours.
### Action Items
- Initialize a Git repository inside your WSL home directory. Open it in VSCode via the Remote-WSL extension.
- Create a docker-compose.yml with a single Kafka service using the apache/kafka image (KRaft mode, combined broker+controller). Set a memory limit of 2 GB.
- Write pos_generator.py. Use time.sleep(0.1) to enforce the 5–10 TPS rate. Produce JSON payloads (matching the FR-1 schema) to a Kafka topic named raw_pos_transactions.
- Verify by running a console consumer: kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic raw_pos_transactions
### Milestone Check
✅ Kafka broker starts in KRaft mode without ZooKeeper. Messages appear in the consumer. Generator sustains 5–10 TPS.
## Stage 2: The Stream Processor (Apache Spark)
**Goal: **Consume Kafka events in real time and write clean, typed data to disk.
**Estimated Time: **3–4 hours.
### Action Items
- Add a Spark service to docker-compose.yml using the official Spark 4.0.2 image. Set memory limit to 3 GB.
- Write stream_processor.py using PySpark Structured Streaming. Subscribe to the Kafka topic, parse the JSON, cast data types (price → DoubleType, timestamp → TimestampType), and filter out payment_status = "failed" events.
- Initially write output as plain Parquet to a local directory with checkpointing enabled. This validates the streaming pipeline before adding Iceberg.
- Test: Stop the Spark job, restart it, and confirm it resumes from the checkpoint without reprocessing old data.
### Milestone Check
✅ Spark consumes from Kafka. Parquet files appear in the output directory. Checkpoint recovery works.
## Stage 3: The Open Data Lake (Apache Iceberg)
**Goal: **Upgrade storage from raw Parquet files to an Iceberg-managed table with ACID semantics.
**Estimated Time: **2–3 hours.
### Action Items
- Add the Iceberg Spark runtime JAR to your Spark container’s classpath. Configure a Hadoop-based Iceberg catalog in your SparkSession pointing to the local warehouse/ directory.
- Modify stream_processor.py to write the stream output to an Iceberg table instead of raw Parquet.
- After some data accumulates, open a PySpark shell and run Spark SQL queries against the Iceberg table. Test time-travel by querying a previous snapshot. Execute an UPDATE or DELETE to confirm ACID operations work.
- Capture screenshots of SQL query results for the README.
### Milestone Check
✅ Iceberg table is created and queryable. Time-travel works. UPDATE/DELETE operations succeed.
## Stage 4: The Orchestrator (Apache Airflow)
**Goal: **Automate and monitor the pipeline lifecycle through a DAG.
**Estimated Time: **3–4 hours.
### Action Items
- Add Airflow services (webserver, scheduler, Postgres metadata DB) to docker-compose.yml. Use LocalExecutor. Strip all unnecessary services (worker, flower, redis, triggerer). Set total Airflow memory limit to 2 GB.
- Write a DAG (pipeline_dag.py) with at least two tasks: (1) submit/start the Spark streaming job, and (2) run a data quality check query on the Iceberg table (e.g., assert row count > 0, assert no null transaction_ids).
- Verify the DAG renders correctly in the Airflow UI (localhost:8080). Trigger a manual run and confirm both tasks succeed.
### Milestone Check
✅ Airflow UI shows the DAG. Tasks execute and transition to “success.” Data quality check passes.
## Stage 5: Integration & Documentation
**Goal: **Run the full stack end-to-end and create portfolio-ready documentation.
**Estimated Time: **2–3 hours.
### Action Items
- Run docker compose up with all services. Verify the generator produces events, Kafka brokers them, Spark processes them into Iceberg, and Airflow orchestrates the workflow.
- Create an architecture diagram (Mermaid flowchart or a clean draw.io export) showing the data flow: Generator → Kafka → Spark → Iceberg/Parquet, with Airflow as the orchestration layer.
- Write a comprehensive README.md covering: project overview, architecture diagram, tech stack with versions, setup instructions, screenshots of Airflow UI and SQL query results, and design decisions.
- Push the completed repository to GitHub. Pin it to your profile.
### Milestone Check
✅ The entire pipeline runs from a single docker compose up. The GitHub repo has a polished README with an architecture diagram and demo screenshots.

# 7. Resource Budget (Docker Memory Limits)
These limits ensure the full stack fits within a 16 GB machine with 10 GB allocated to WSL. Each service has an explicit memory cap in docker-compose.yml to prevent any single component from starving the others.
**Service**
**Memory Limit**
**Notes**
**Kafka (KRaft)**
**2 GB**
**Single combined broker+controller node**
**Spark (Driver + Executor)**
**3 GB**
**Local mode, single JVM**
**Airflow Webserver**
**1 GB**
**Serves the UI only**
**Airflow Scheduler**
**1 GB**
**LocalExecutor, no workers**
**PostgreSQL (Airflow DB)**
**0.5 GB**
**Metadata database only**
**Python Generator**
**0.25 GB**
**Lightweight script**
**TOTAL**
**~7.75 GB**
**Fits within 10 GB WSL allocation with headroom**

# 8. Risks and Mitigations
**Risk**
**Impact**
**Mitigation**
**Spark OOM on low-memory machines**
**Stream processing crashes**
**Cap spark.driver.memory at 2g. Use micro-batch triggers with a 10-second interval.**
**Kafka + Spark + Airflow RAM contention**
**Containers killed by Docker**
**Develop each stage independently (Stages 1–4). Only run all together in Stage 5.**
**Outdated tutorials referencing ZooKeeper**
**Configuration errors and wasted time**
**Only follow Kafka 4.x documentation. Use the apache/kafka Docker image with KRaft env vars.**
**Iceberg catalog misconfiguration**
**Tables not persisted or not queryable**
**Use the Hadoop catalog with a local path. Test SQL queries in a PySpark shell before integrating with streaming.**
**Airflow 3.x breaking changes from 2.x guides**
**DAG import errors**
**Use the official Airflow 3.2 docker-compose reference. Test DAG syntax with "airflow dags test" CLI.**

# 9. Next Steps
With this plan validated and documented, the recommended order of execution is:
- **Step 1: **Set up WSL2, Docker Desktop, and VSCode with Remote-WSL.
- **Step 2: **Draft the exact JSON schema for the POS generator (FR-1) and begin Stage 1.
- **Step 3: **Work through Stages 2–5 sequentially, committing and pushing after each stage.


**Estimated Total Build Time: **12–17 hours across all stages. This is a weekend project that produces a portfolio-ready, interview-worthy artifact.