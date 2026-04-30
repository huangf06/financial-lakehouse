FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/app
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY docker/producer/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY producers /app/producers
COPY pipelines /app/pipelines
CMD ["python", "-m", "producers.binance"]
