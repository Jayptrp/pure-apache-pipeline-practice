# Apache Spark 4.0.2 with Python 3 — official Apache image (not bitnami)
FROM apache/spark:4.0.2-python3

USER root

# ---------------------------------------------------------------------------
# Pre-bake JARs into $SPARK_HOME/jars/ to avoid network calls at job start
#
# Stage 2 — Kafka connector and its two required transitive dependencies:
#   spark-sql-kafka-0-10_2.13   Spark's built-in Kafka source/sink
#   kafka-clients               Transitive dep; missing = ClassNotFoundException
#   commons-pool2               Transitive dep; missing = ClassNotFoundException
#
# Stage 3 — Iceberg runtime (added below):
#   iceberg-spark-runtime-4.0_2.13   _2.13 = Scala 2.13 (required for Spark 4.0)
#                                    Using _2.12 causes binary incompatibility
# ---------------------------------------------------------------------------

ARG MAVEN=https://repo1.maven.org/maven2

# Kafka connector JARs (Stage 2)
# spark-sql-kafka-0-10        — Kafka source/sink
# spark-token-provider-kafka-0-10 — required companion (KafkaConfigUpdater lives here)
# kafka-clients               — transitive dep
# commons-pool2               — transitive dep
RUN wget -q "${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.2/spark-sql-kafka-0-10_2.13-4.0.2.jar" \
         -O "${SPARK_HOME}/jars/spark-sql-kafka-0-10_2.13-4.0.2.jar" && \
    wget -q "${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.2/spark-token-provider-kafka-0-10_2.13-4.0.2.jar" \
         -O "${SPARK_HOME}/jars/spark-token-provider-kafka-0-10_2.13-4.0.2.jar" && \
    wget -q "${MAVEN}/org/apache/kafka/kafka-clients/3.9.0/kafka-clients-3.9.0.jar" \
         -O "${SPARK_HOME}/jars/kafka-clients-3.9.0.jar" && \
    wget -q "${MAVEN}/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar" \
         -O "${SPARK_HOME}/jars/commons-pool2-2.12.0.jar"

# Iceberg runtime JAR (Stage 3)
# iceberg-spark-runtime-4.0_2.13 first appeared in Iceberg 1.10.0
RUN wget -q "${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.10.1/iceberg-spark-runtime-4.0_2.13-1.10.1.jar" \
         -O "${SPARK_HOME}/jars/iceberg-spark-runtime-4.0_2.13-1.10.1.jar"

USER spark
