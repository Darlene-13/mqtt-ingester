# MQTT INGESTER
This is a high throughput MQTT telemetry ingester written in Go. It subscribes to sensor topics, decodes a fixed binary frame, batches rows and writes them to Timescale DB with COPY.

The goal is to: Sustain atleast 100writes/s with large headroom (design goal 10x to 50x), never block MQTT network loop and fail in ways that are visible and recoverable.


## Contents
1. [Architecture](#architecture)
2. [Quick start](#quick-start)
3. [Configuration](#configuration)
4. [Wire format](#wire-format)
5. [Database schema](#database-schema)
6. [Benchmarking](#benchmarking)
7. [Operations and observability](#operations-and-observability)
8. [Failure behavior](#failure-behavior)
9. [Design decisions](#design-decisions)
10. [Tuning](#tuning)
11. [Troubleshooting](#troubleshooting)
12. [Roadmap](#roadmap)



## Architecture
```mermaid

flowchart TD
    ESP32["ESP32 Devices<br/>68-byte frame"] 
    MQTT["MQTT Broker<br/>Mosquitto / EMQX"]
    INGEST["Go MQTT Ingestor"]
    DB[("TimescaleDB")]

    ESP32 -->|"PUBLISH · QoS 1"| MQTT
    MQTT -->|"Deliver messages"| INGEST
    INGEST -->|"COPY · pgx CopyFrom"| DB

    subgraph PIPELINE["Inside the Go Ingestor"]
        direction TB

        LOOP["Paho Network Loop"]
        CALLBACK["Callback: Submit(payload)<br/>O(1) · Non-blocking"]
        CHANNEL[["Buffered Channel<br/>Capacity: 4096<br/>Bounded shock absorber"]]

        LOOP --> CALLBACK
        CALLBACK --> CHANNEL

        CHANNEL --> W1["Worker 1<br/>Decode → Local Batch"]
        CHANNEL --> W2["Worker 2<br/>Decode → Local Batch"]
        CHANNEL --> W3["Worker 3<br/>Decode → Local Batch"]
        CHANNEL --> W4["Worker 4<br/>Decode → Local Batch"]

        W1 --> FLUSH["Batch Flush Coordinator"]
        W2 --> FLUSH
        W3 --> FLUSH
        W4 --> FLUSH

        FLUSH -->|"500 rows OR 250 ms"| COPY["pgx CopyFrom"]
    end

    INGEST -.-> LOOP
    COPY --> DB

    classDef device fill:#dbeafe,stroke:#2563eb,color:#1e3a8a
    classDef broker fill:#fef3c7,stroke:#d97706,color:#78350f
    classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d
    classDef storage fill:#ede9fe,stroke:#7c3aed,color:#4c1d95

    class ESP32 device
    class MQTT broker
    class W1,W2,W3,W4,CHANNEL worker
    class DB storage

```

Three stages run at very different speeds (network in microseconds, decode in nanoseconds, database in milliseconds with spikes). Each boundary exists so a slow stage cannot stall a fast one.

### Folder Structure

```
tsingest/
  cmd/ingestor/main.go       entrypoint: wiring, MQTT client, shutdown
  cmd/simulator/main.go      load generator
  internal/model/reading.go  Reading struct and binary codec
  internal/ingest/pipeline.go  channel, workers, batching, retry
  internal/store/writer.go   pgx CopyFrom writer
  db/init/001_schema.sql     schema, compression, retention, rollup
  mosquitto/mosquitto.conf   local broker config
  docker-compose.yml         local TimescaleDB + Mosquitto
  Makefile                   common commands
```

## Quick start

### Prerequisites

- Go 1.22 or newer (`go version`)
- Docker Engine with the Compose plugin (`docker compose version`)
  Installing Docker on Ubuntu or Debian:

```bash
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
newgrp docker
docker compose version
```

No local Postgres or TimescaleDB install is needed. Both run in containers.

### Run it

```bash
cp .env.example .env
make up         
go mod tidy     
make run        
make sim         
make rate        
```

The ingestor prints one stats line per second:

```
written/s=100 total=3000 recv=3000 dropped=0 failed=0 decodeErr=0 queue=0
```

### Reset everything

```bash
make reset       
make up         
```

The schema script only runs on an empty data volume, so after editing `db/init/001_schema.sql` you need `make reset`.

## Configuration

Environment variables, read from `.env` by the Makefile.

| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | none (required) | pgx connection string |
| `MQTT_BROKER` | none (required) | Broker URL, for example `tcp://localhost:1883` |
| `MQTT_CLIENT_ID` | none (required) | Must be unique per replica |
| `MQTT_TOPIC` | `telemetry/+/data` | Subscription filter. Use `$share/ingest/telemetry/+/data` for replicas |

Pipeline settings (compiled in `cmd/ingestor/main.go`, candidates for env vars):

| Setting | Value | Meaning |
|---|---|---|
| Workers | 4 | Roughly the number of DB connections, not CPU cores |
| QueueSize | 4096 | About 40 s of buffering at 100 msg/s |
| BatchSize | 500 | Rows per COPY |
| FlushEvery | 250 ms | Upper bound on added latency when traffic is low |
| EnqueueWait | 5 ms | How long a callback waits on a full queue before dropping |

## Wire format

Fixed 68-byte little-endian frame, published to `telemetry/{device_id}/data` with QoS 1.

| Offset | Size | Field | Type |
|---|---|---|---|
| 0 | 4 | device_id | uint32 |
| 4 | 8 | timestamp (microseconds since epoch) | int64 |
| 12 | 4 | temperature | float32 |
| 16 | 4 | humidity | float32 |
| 20 | 12 | accelerometer x, y, z | 3 x float32 |
| 32 | 12 | gyroscope x, y, z | 3 x float32 |
| 44 | 8 | latitude | float64 |
| 52 | 8 | longitude | float64 |
| 60 | 4 | altitude | float32 |
| 64 | 4 | speed | float32 |

Latitude and longitude are float64 because float32 loses about a metre of precision. Frames with any other length are counted in `decodeErr` and skipped.

On the ESP32 side this maps to a packed C struct, so no serialization library is needed.

## Database schema

TimescaleDB hypertable `telemetry`, chunked by day.

- One index: `(device_id, time DESC)`. The default time-only index is disabled to protect write throughput.
- Compression after 3 days, segmented by `device_id`.
- Retention of 90 days.
- Continuous aggregate `telemetry_1m` (1 minute rollup per device) for dashboards.
- The `ingestor` role runs with `synchronous_commit = off`.
  Compression and policy syntax differs slightly between Timescale versions (newer ones call it "columnstore"). The compose file pins `2.17.2-pg16` so the init script is known to work. Check the docs before upgrading.

## Benchmarking

The simulator runs N devices, each with its own MQTT connection, publishing at a fixed rate.

| Command | Load | Purpose |
|---|---|---|
| `make sim` | 100 msg/s | Meets the target |
| `make sim-1k` | 1,000 msg/s | Headroom check |
| `make sim-5k` | 5,000 msg/s | Find the first bottleneck |

### Pass criteria for the 100 msg/s target

- `written/s` sustained at 100 or more
- `dropped=0` and `failed=0` in the ingestor log
- Queue depth stays near 0
- p99 publish-to-PUBACK latency under about 50 ms in the simulator output
- Row count in the database matches the simulator's `sent` count (`make count`)

## Benchmark results

Environment: single Linux laptop running the ingestor, simulator, Mosquitto and TimescaleDB together (Docker for the last two). Config: 4 workers, batch 500, flush 250 ms, queue 4096. Results are relative, not absolute, because everything shares one machine.

### Publisher side (simulator)

| Target (msg/s) | Devices | Sent | Achieved (msg/s) | Connect failures | PUBACK p50 | p95 | p99 | max |
|---|---|---|---|---|---|---|---|---|
| 100 | 100 | 5,897 | 98.3 | 0 | 290 µs | 582 µs | 1.19 ms | 3.7 ms |
| 1,000 | 100 | 59,889 | 998.1 | 0 | 207 µs | 354 µs | 622 µs | 6.3 ms |
| 5,000 | 500 | 299,172 | 4,983.2 | 0 | 555 µs | 1.67 ms | 3.48 ms | 27.6 ms |
| 10,000 | 1,000 | 597,174 | 9,947.2 | 0 | 1.34 ms | 9.32 ms | 24.3 ms | 164 ms |
| 25,000* | 2,500 | 606,966 | 10,109.4 | 1,484 | 398 µs | 1.41 ms | 4.32 ms | 129 ms |
| 50,000* | 5,000 | 604,984 | 10,073.2 | 3,984 | 399 µs | 1.27 ms | 4.83 ms | 425 ms |
| 75,000* | 7,500 | 577,347 | 9,606.6 | 6,484 | 2.18 ms | 13.8 ms | 62.1 ms | 2.9 s |
| 100,000* | 10,000 | 554,317 | 9,232.3 | 8,984 | 575 µs | 11.8 ms | 95.9 ms | 3.6 s |

\* Simulator limited. The shell's open-file limit (1024) capped connections at 1,016, so these runs never reached their target rate. The ingestor was not the bottleneck.

### Ingestor side

| Run | Rows written | Matches sent | Dropped | Failed | Decode errors | Peak written/s | Max queue depth |
|---|---|---|---|---|---|---|---|
| 100 msg/s | 5,897 | Yes | 0 | 0 | 0 | 101 | 0 |
| 1,000 msg/s | 59,889 | Yes | 0 | 0 | 0 | 1,183 | 3 |
| 5,000 msg/s | 299,172 | Yes | 0 | 0 | 0 | 5,005 | 20 |
| 10,000 msg/s | 597,174 | Yes | 0 | 0 | 0 | 10,810 | 0 |
| 25k run* | 606,966 | Yes | 0 | 0 | 0 | 11,000 | 0 |
| 50k run* | 604,984 | Yes | 0 | 0 | 0 | 10,500 | 0 |
| 75k run* | 577,347 | Yes | 0 | 0 | 0 | 13,000 | 0 |
| 100k run* | 554,317 | Yes | 0 | 0 | 0 | 22,000 (burst) | 0 |

### Takeaways

- The 100 msg/s target is met with roughly 100x headroom: zero drops, zero failures, zero loss across about 3.8M messages.
- Sustained throughput was verified up to about 10,000 msg/s with the queue staying at or near 0. A burst of 22,000 rows/s was absorbed with no drops.
- The true ingestor ceiling has not been found yet. Higher runs were limited by simulator connections, not by the ingestor.
- Written/s moves in steps of 500 at high rates because rows are counted when each 500-row batch commits.


