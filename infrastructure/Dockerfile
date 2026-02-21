FROM apache/airflow:2.8.1-python3.10

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install --no-cache-dir \
    pyspark==3.5.2 \
    great_expectations \
    kafka-python
