FROM apache/spark:3.5.3 AS spark-runtime

FROM apache/airflow:2.10.3-python3.11

USER root
COPY --from=spark-runtime /opt/spark /opt/spark
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl openjdk-17-jre-headless && \
    rm -rf /var/lib/apt/lists/*

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

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_HOME=/opt/spark \
    PATH="/opt/spark/bin:${PATH}" \
    PYTHONPATH=/opt/airflow:/opt/app:/opt/spark/python:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip

COPY docker/airflow/requirements.txt /requirements.txt
USER airflow
RUN pip install --no-cache-dir --no-deps -r /requirements.txt

USER root
COPY dags /opt/airflow/dags
COPY jobs /opt/app/jobs
COPY pipelines /opt/app/pipelines
USER airflow
