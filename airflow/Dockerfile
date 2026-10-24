FROM apache/airflow:2.10.2-python3.11

ARG ICEBERG_VERSION=1.12.0
ARG POSTGRES_JDBC_VERSION=42.7.14
ARG MAVEN_BASE_URL=https://maven.aliyun.com/repository/central

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
RUN set -eux; \
    mkdir -p /opt/airflow/extra-jars; \
    curl --fail --location --retry 5 --retry-delay 3 \
      "${MAVEN_BASE_URL}/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar" \
      --output "/opt/airflow/extra-jars/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar"; \
    echo "9e395e373a9a99643bf413ca1f74fa0c034887b4  /opt/airflow/extra-jars/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar" | sha1sum -c -; \
    curl --fail --location --retry 5 --retry-delay 3 \
      "${MAVEN_BASE_URL}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
      --output "/opt/airflow/extra-jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
    echo "64d5012217035a60924feb633636b744d318eb0a  /opt/airflow/extra-jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" | sha1sum -c -; \
    curl --fail --location --retry 5 --retry-delay 3 \
      "${MAVEN_BASE_URL}/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
      --output "/opt/airflow/extra-jars/postgresql-${POSTGRES_JDBC_VERSION}.jar"; \
    echo "0e7c94ce80cb1dd9875ff44ffe6a40a8aaa3eefd  /opt/airflow/extra-jars/postgresql-${POSTGRES_JDBC_VERSION}.jar" | sha1sum -c -
USER airflow
RUN pip install --no-cache-dir "pyspark==4.1.2"
