#!/bin/sh
set -e
mc alias set local http://minio:9000 minioadmin minioadmin
for bucket in lakehouse lakehouse-meta lakehouse-archive; do
    mc ls "local/$bucket" >/dev/null 2>&1 || mc mb "local/$bucket"
done
echo "MinIO bootstrap complete"
