# Apache Airflow 3.2.0 with Python 3.12
FROM apache/airflow:3.2.0-python3.12

# Install the Apache Spark provider for Airflow 3.x and PySpark for the DQ task.
# Check PyPI for the latest version compatible with Airflow 3.2.x before building:
#   pip index versions apache-airflow-providers-apache-spark
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark>=5.0.0 \
    pyspark==4.0.2
