FROM python:3.11-slim-bullseye AS python-runtime

FROM apache/spark:3.5.3

USER root
COPY --from=python-runtime /usr/local /usr/local
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*
RUN python3.11 -m pip install --no-cache-dir --no-deps "delta-spark==3.2.0" && \
    python3.11 -m pip install --no-cache-dir "pydantic==2.9.*" "pydantic-settings==2.5.*" "boto3==1.35.*"

COPY docker/spark/jars-list.txt /tmp/jars-list.txt
RUN set -eux; \
    jar_dir=/opt/spark/jars; \
    while read -r coord; do \
        case "$coord" in \#*|"") continue;; esac; \
        group="$(echo "$coord" | awk -F: '{print $1}' | tr . /)"; \
        artifact="$(echo "$coord" | awk -F: '{print $2}')"; \
        version="$(echo "$coord" | awk -F: '{print $3}')"; \
        curl -fsSL "https://repo1.maven.org/maven2/${group}/${artifact}/${version}/${artifact}-${version}.jar" \
            -o "${jar_dir}/${artifact}-${version}.jar"; \
    done < /tmp/jars-list.txt

COPY pipelines /opt/app/pipelines
COPY jobs /opt/app/jobs
ENV PYTHONPATH=/opt/app:/opt/spark/python:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.11 \
    PYSPARK_PYTHON=/usr/local/bin/python3.11 \
    SPARK_HOME=/opt/spark
USER spark
