# Architecture Overview

The local MVP uses a medallion lakehouse: producers write atomically rotated JSONL files into
landing storage; Spark Structured Streaming reads them into Bronze Delta with checkpoint
recovery; Silver normalizes records and splits quality failures into quarantine tables; Gold
builds analytical aggregates; Airflow coordinates Silver, Gold, replay, optimization, and
vacuum jobs.

### Data Flow

```mermaid
flowchart LR
    subgraph Producers
        BIN[producer-binance<br/>WS]
        ALP[producer-alpaca<br/>REST poll]
        REP[ReplayProducer<br/>Parquet -&gt; JSONL]
    end
    subgraph Landing[MinIO landing/]
        L1[landing/binance/]
        L2[landing/alpaca/]
        L3[landing/replay/binance/]
    end
    subgraph Bronze[Bronze Delta]
        B[Structured Streaming<br/>+ checkpoint]
    end
    subgraph Silver[Silver Delta]
        S[Quality split<br/>valid + quarantine]
    end
    subgraph Gold[Gold Delta]
        G[Daily volume<br/>Market quality<br/>Bars 5m/1h/1d]
    end

    BIN -->|atomic JSONL| L1
    ALP -->|atomic JSONL| L2
    REP -->|atomic JSONL| L3
    L1 --> B
    L2 --> B
    L3 --> B
    B --> S
    S --> G
```

### Compose Topology

```mermaid
flowchart TB
    subgraph Storage
        MIN[MinIO<br/>S3-compatible<br/>:9000/:9001]
        INIT[minio-init<br/>bucket bootstrap]
    end
    subgraph Producers
        PB[producer-binance]
        PA[producer-alpaca<br/>profile=live]
    end
    subgraph SparkCluster[Spark Standalone]
        SM[spark-master<br/>:7077/:8080]
        SW1[spark-worker-1]
        SW2[spark-worker-2]
        SBR[spark-bronze<br/>profile=streaming]
    end
    subgraph Orchestration
        PG[(Postgres<br/>Airflow metadata)]
        AW[airflow-webserver<br/>:8081]
        AS[airflow-scheduler]
    end
    subgraph Observability
        MP[metrics-publisher<br/>:9100]
        PROM[Prometheus<br/>:9090]
        GRAF[Grafana<br/>:3000]
    end

    PB --> MIN
    PA --> MIN
    INIT --> MIN
    SBR --> SM
    SM --- SW1
    SM --- SW2
    SBR --> MIN
    AW --> PG
    AS --> PG
    AS -.spark-submit.-> SM
    MP --> MIN
    PROM --> MP
    PROM --> SM
    GRAF --> PROM
```

### Airflow Dataset Dependencies

```mermaid
flowchart LR
    SP[silver_pipeline<br/>schedule: every 15m]
    GA[gold_aggregations<br/>dataset-triggered]
    QR[quarantine_replay<br/>manual]
    OH[optimize_hot<br/>schedule: hourly]
    OZ[optimize_zorder_nightly<br/>schedule: nightly]
    VN[vacuum_nightly<br/>schedule: nightly]

    SP -->|silver_trades dataset| GA
    SP -->|silver_bars dataset| GA
    OH -.-> VN
    OZ -.-> VN
```

Diagram sources live under `docs/architecture/diagrams/`. See the Evidence Map in `README.md`
for the code paths backing each resume claim.
