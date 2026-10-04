FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*


RUN pip install --no-cache-dir \
    pyspark==3.5.3 \
    "ray[default,data]==2.40.0" \
    pandas \
    pyarrow \
    psutil

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PATH=$PATH:/usr/local/lib/python3.11/site-packages/pyspark/bin
ENV PYSPARK_PYTHON=python3

WORKDIR /workspace