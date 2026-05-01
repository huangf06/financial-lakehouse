FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/app
WORKDIR /app
RUN pip install --no-cache-dir \
    boto3==1.35.* \
    prometheus-client==0.21.1 \
    pydantic==2.9.* \
    pydantic-settings==2.5.*
COPY metrics_publisher /app/metrics_publisher
COPY pipelines /app/pipelines
CMD ["python", "-m", "metrics_publisher.publisher"]
