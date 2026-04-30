FROM apache/airflow:2.10.3-python3.11

COPY docker/airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
COPY dags /opt/airflow/dags
