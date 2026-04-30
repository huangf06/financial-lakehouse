FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/app
WORKDIR /app
RUN pip install --no-cache-dir prometheus-client==0.21.1 fastapi==0.115.5 uvicorn==0.32.1
COPY metrics_publisher /app/metrics_publisher
CMD ["python", "-m", "metrics_publisher.publisher"]
