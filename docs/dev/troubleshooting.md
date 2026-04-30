# Troubleshooting

- If MinIO buckets are missing, run `./scripts/minio-init.sh` inside the `minio-init` container or restart `docker compose up minio-init`.
- If Spark cannot read `s3a://` paths, confirm the Hadoop AWS JARs are present in the Spark image.
- If Airflow DAGs do not import, verify the Spark provider package is installed in the Airflow image.
