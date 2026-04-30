#!/bin/bash
set -e
docker compose down -v
docker compose up -d minio minio-init
docker compose logs minio-init
