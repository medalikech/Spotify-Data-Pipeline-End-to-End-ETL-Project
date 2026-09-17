FROM apache/airflow:3.3.1

USER root

# Install Java (required by PySpark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# Install our pipeline's Python dependencies
RUN pip install --no-cache-dir \
    boto3 \
    pandas \
    pyspark \
    snowflake-connector-python
    python-dotenv