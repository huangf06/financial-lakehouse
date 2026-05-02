#!/bin/sh
set -e
mc alias set local http://minio:9000 "${S3_ACCESS_KEY:-minioadmin}" "${S3_SECRET_KEY:-minioadmin}"
for bucket in lakehouse lakehouse-meta lakehouse-archive; do
    mc ls "local/$bucket" >/dev/null 2>&1 || mc mb "local/$bucket"
done
echo "MinIO bootstrap complete"
