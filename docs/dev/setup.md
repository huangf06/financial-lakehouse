# Developer Setup

```bash
uv sync --all-extras
make test-unit
docker compose up -d
```

Local services:

- MinIO: http://localhost:9001
- Spark master UI: http://localhost:8080
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000
