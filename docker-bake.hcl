group "default" {
  targets = ["spark", "producer", "metrics"]
}

target "spark" {
  context = "."
  dockerfile = "docker/spark/Dockerfile"
  tags = ["financial-lakehouse-spark:dev"]
}

target "producer" {
  context = "."
  dockerfile = "docker/producer/Dockerfile"
  tags = ["financial-lakehouse-producer:dev"]
}

target "metrics" {
  context = "."
  dockerfile = "docker/metrics-publisher/Dockerfile"
  tags = ["financial-lakehouse-metrics:dev"]
}
