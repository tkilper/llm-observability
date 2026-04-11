# LLM Observability Pipeline

An end-to-end **streaming data pipeline** for real-time LLM API call observability — built with Python, Kafka, Apache Flink, PostgreSQL, and Grafana. Fully containerized with Docker Compose.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Docker Network                               │
│                                                                      │
│  ┌──────────────┐     ┌──────────────────────────────────────────┐  │
│  │ LLM Producer │────▶│ Kafka                                    │  │
│  │  (simulator) │     │  ├─ llm.events.raw        (3 partitions) │  │
│  └──────────────┘     │  ├─ llm.events.anomalies  (1 partition)  │  │
│                        │  └─ llm.events.dlq        (1 partition)  │  │
│  ┌──────────────┐     └──────────┬───────────────────────────────┘  │
│  │   LiteLLM    │────────────────┘                                  │
│  │ (live mode)  │     ┌──────────▼───────────────────────────────┐  │
│  └──────────────┘     │ Apache Flink                              │  │
│                        │  ├─ event_sink.py      raw → Postgres    │  │
│                        │  ├─ cost_rollups.py    hourly rollups    │  │
│                        │  └─ anomaly_detection  spike detection   │  │
│                        └──────────┬───────────────────────────────┘  │
│                                   │                                   │
│                        ┌──────────▼───────────────────────────────┐  │
│                        │ PostgreSQL                                │  │
│                        │  ├─ llm_events                           │  │
│                        │  ├─ cost_rollups_hourly                  │  │
│                        │  └─ anomaly_events                       │  │
│                        └──────────┬───────────────────────────────┘  │
│                                   │                                   │
│                        ┌──────────▼───────────────────────────────┐  │
│                        │ Grafana Dashboard                         │  │
│                        │  ├─ Live event feed                      │  │
│                        │  ├─ Cost by model / user                 │  │
│                        │  ├─ Latency percentiles                  │  │
│                        │  └─ Anomaly alert panel                  │  │
│                        └──────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

## Tech Stack

| Layer | Technology |
|---|---|
| Event streaming | Apache Kafka (Confluent) |
| Stream processing | Apache Flink 1.18 (PyFlink) |
| Storage | PostgreSQL 15 |
| Dashboarding | Grafana 10 |
| LLM proxy | LiteLLM (optional) |
| Language | Python 3.11 |
| Infrastructure | Docker Compose |

## Services & Ports

| Service | URL | Description |
|---|---|---|
| Kafka UI | http://localhost:8080 | Browse topics and messages |
| Flink UI | http://localhost:8081 | Monitor running jobs |
| Grafana | http://localhost:3000 | Live observability dashboard |
| PostgreSQL | localhost:5432 | Event and aggregate storage |
| LiteLLM | http://localhost:4000 | LLM proxy (live mode only) |

## Quick Start

**Prerequisites:** Docker Desktop

```bash
# 1. Clone and configure
git clone https://github.com/<your-username>/llm-observability.git
cd llm-observability
cp .env.example .env

# 2. Start the full stack (simulator mode — no API keys needed)
docker compose up -d

# 3. Open the dashboard
open http://localhost:3000   # admin / admin
```

The simulator starts producing ~2 LLM call events per second across 6 models
and 20 users, with periodic anomaly bursts injected automatically.

### Live mode (real LLM calls via LiteLLM)

```bash
# Add your API keys to .env, then:
docker compose --profile live up -d
```

## Kafka Topics

| Topic | Partitions | Description |
|---|---|---|
| `llm.events.raw` | 3 | Every LLM call event from producer or LiteLLM |
| `llm.events.anomalies` | 1 | Anomalies detected by Flink |
| `llm.events.dlq` | 1 | Dead-letter queue for malformed events |

## Flink Jobs

| Job | API | Description |
|---|---|---|
| `event_sink.py` | Table SQL | Writes raw events from Kafka → `llm_events` |
| `cost_rollups.py` | Flink SQL | Tumbling 1-hour windows → `cost_rollups_hourly` |
| `anomaly_detection.py` | DataStream | Z-score detection on latency + error rate |

## Anomaly Detection

The Flink anomaly detection job uses a **5-minute tumbling window** and flags:

- **Latency spike** — p95 latency exceeds 3× the rolling baseline (`warning`) or 6× (`critical`)
- **Error surge** — error rate in window exceeds 15% (`warning`) or 40% (`critical`)
- **Cost spike** — window cost exceeds 2× rolling average

## Project Structure

```
llm-observability/
├── docker-compose.yml
├── .env.example
├── producer/               # Simulated LLM call generator
│   ├── producer.py
│   ├── Dockerfile
│   └── requirements.txt
├── flink/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── jobs/
│       ├── event_sink.py
│       ├── cost_rollups.py
│       ├── anomaly_detection.py
│       └── submit_jobs.sh
├── postgres/
│   └── init.sql
├── grafana/
│   └── provisioning/
│       ├── datasources/
│       └── dashboards/
└── litellm/
    └── config.yaml
```
