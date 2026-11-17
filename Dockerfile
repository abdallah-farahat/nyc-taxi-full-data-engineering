# Base image: small Debian with Python 3.11
FROM python:3.11-slim-bookworm

# Spark runs on the JVM, so we need Java. "headless" = no GUI parts, smaller image.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Pinned versions: Spark 3.5.x and Delta 3.2.x are designed to work together.
RUN pip install --no-cache-dir pyspark==3.5.3 delta-spark==3.2.1

# Start Spark once while building the image, so the Delta jar files are downloaded
# and cached inside the image. Later runs start fast and do not need the internet.
RUN python -c "\
from pyspark.sql import SparkSession; \
from delta import configure_spark_with_delta_pip; \
b = SparkSession.builder.master('local[1]'); \
configure_spark_with_delta_pip(b).getOrCreate().stop()"

# Our code will live here (mounted from your PC by docker-compose)
WORKDIR /app