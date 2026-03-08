# Livestream Assistant Agent

A scalable event-driven AI Agent pipeline — from raw Amazon review data to real-time AI insights using Docker, Kafka, ksqlDB, and OpenAI.

## Table of Contents

- [Setup and Run](#setup-and-run)
- [Architecture](#architecture)
  - [System Overview](#system-overview)
  - [Data Flow](#data-flow)
  - [Components](#components)
  - [Technologies](#technologies)

---

## Setup and Run

### Prerequisites

- Docker & Docker Compose
- Python 3.10+
- [uv](https://github.com/astral-sh/uv) (Python package manager)
- OpenAI API key (optional, for LLM insights)

### Quick Start

#### 1. Initial Setup

```bash
make setup
```

This will:
- Create `.env` and `.env.docker` from example files
- Install Python dependencies via `uv sync`

After running, edit the `.env` and `.env.docker` files to configure:
- `OPENAI_API_KEY` - Your OpenAI API key
- Database credentials (defaults work for local development)

#### 2. Start Infrastructure

```bash
make full-pipeline
```

This starts in order:
- Kafka cluster (3 brokers + Schema Registry + Kafka UI)
- CDC pipeline (PostgreSQL + Kafka Connect + Debezium)

#### 3. Start API Services

Open **two separate terminals**:

```bash
# Terminal 1: CDC Producer API (port 8001)
make api-cdc-start

# Terminal 2: Feature & Sentiment API (port 8000)
make api-start
```

#### 4. Start Streaming Pipeline

```bash
make streaming-pipeline
```

This starts:
- ksqlDB server
- Initializes ksqlDB streams and tables
- Streaming enrichment service
- LLM insight extraction service

### Useful Commands

| Command | Description |
|---------|-------------|
| `make status` | Show status of all containers |
| `make health` | Check health of all services |
| `make logs` | Show logs for all containers |
| `make logs-streaming` | Show streaming service logs |
| `make full-down` | Stop all services |
| `make clean` | Stop containers and remove orphans |
| `make clean-volumes` | Remove all data (WARNING: destructive) |

### Service URLs

| Service | URL |
|---------|-----|
| Kafka UI | http://localhost:8080 |
| Schema Registry | http://localhost:8081 |
| Debezium UI | http://localhost:8082 |
| Kafka Connect | http://localhost:8083 |
| ksqlDB | http://localhost:8088 |
| Feature API | http://localhost:8000 |
| CDC API | http://localhost:8001 |

---

## Architecture

### System Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         LIVESTREAM ASSISTANT AGENT                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│  │   Parquet   │───▶│  PostgreSQL │───▶│    Kafka    │───▶│   ksqlDB    │  │
│  │    Data     │    │    (CDC)    │    │   Cluster   │    │ Aggregation │  │
│  └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘  │
│                            │                  │                  │          │
│                            │                  │                  │          │
│                            ▼                  ▼                  ▼          │
│                     ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│                     │  Debezium   │    │ Enrichment  │    │ LLM Insight │  │
│                     │  Connector  │    │   Service   │    │   Service   │  │
│                     └─────────────┘    └──────┬──────┘    └─────────────┘  │
│                                               │                             │
│                                               ▼                             │
│                                        ┌─────────────┐                      │
│                                        │  Redis +    │                      │
│                                        │  SparkNLP   │                      │
│                                        └─────────────┘                      │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
PHASE 1: DATA INGESTION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Parquet Files (Amazon Reviews)
  └── reviewerID, reviewText
          │
          ▼
  CDC Producer API (POST /api/cdc/produce)
          │
          ▼
  PostgreSQL (cdc-postgresql:5432)
  └── Table: comment_events
      └── comment_id, user_id, comments, event_timestamp


PHASE 2: CHANGE DATA CAPTURE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  PostgreSQL WAL (wal_level=logical)
          │
          ▼
  Kafka Connect + Debezium
          │
          ▼
  Kafka Topic: tracking_postgres_cdc.public.comment_events
  └── Format: Avro (Debezium envelope)


PHASE 3: STREAM ENRICHMENT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Enrichment Service (Python)
  └── Consumes: tracking_postgres_cdc.public.comment_events
          │
          ├──▶ Redis: Gender lookup by user_id
          │
          └──▶ SparkNLP: Sentiment analysis
          │
          ▼
  Kafka Topic: streaming.enriched_events
  └── comment_id, user_id, comments, gender, sentiment, timestamp


PHASE 4: REAL-TIME AGGREGATION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  ksqlDB Stream Processing
  └── 1-minute tumbling window aggregation
          │
          ▼
  Kafka Topic: streaming.combined_stats
  └── total_count, male_count, female_count
      positive_count, negative_count


PHASE 5: AI INSIGHTS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  LLM Insight Service (Python)
  └── Consumes: streaming.combined_stats
          │
          ▼
  OpenAI GPT-4o
  └── Generates summary + recommendations
          │
          ▼
  Kafka Topic: streaming.llm_insights
```

### Components

#### CDC Pipeline

| Component | Description | Port |
|-----------|-------------|------|
| **PostgreSQL** | Source database with logical replication | 5432 |
| **Kafka Connect** | Runs Debezium connectors | 8083 |
| **Debezium** | Captures database changes in real-time | - |
| **Debezium UI** | Connector management interface | 8082 |

#### Kafka Cluster

| Component | Description | Port |
|-----------|-------------|------|
| **Kafka Brokers** | 3-node KRaft cluster | 19092, 29092, 39092 |
| **Schema Registry** | Avro schema management | 8081 |
| **Kafka UI** | Cluster monitoring interface | 8080 |

#### API Services

| Service | Description | Port |
|---------|-------------|------|
| **Feature & Sentiment API** | Gender lookup (Redis) + Sentiment analysis (SparkNLP) | 8000 |
| **CDC Producer API** | Triggers CDC events into PostgreSQL | 8001 |

#### Streaming Services

| Service | Input Topic | Output Topic |
|---------|-------------|--------------|
| **Enrichment** | `tracking_postgres_cdc.public.comment_events` | `streaming.enriched_events` |
| **ksqlDB** | `streaming.enriched_events` | `streaming.combined_stats` |
| **LLM Insight** | `streaming.combined_stats` | `streaming.llm_insights` |

#### Feature Store

| Component | Description | Port |
|-----------|-------------|------|
| **Redis** | User gender lookup cache | 6379 |

### Technologies

| Category | Technologies |
|----------|--------------|
| **Runtime** | Python 3.10+ |
| **API Framework** | FastAPI, Uvicorn |
| **Message Broker** | Apache Kafka (KRaft mode) |
| **CDC** | Debezium, Kafka Connect |
| **Stream Processing** | ksqlDB |
| **Databases** | PostgreSQL 15, Redis |
| **ML/NLP** | SparkNLP, PySpark |
| **LLM** | OpenAI GPT-4o |
| **Schema Management** | Apache Avro, Confluent Schema Registry |
| **Data Processing** | Parquet, PyArrow, Pandas |
| **ORM** | SQLAlchemy |
| **Package Management** | uv |
| **Containerization** | Docker, Docker Compose |

### Project Structure

```
Livestream-Assistant-Agent/
├── main.py                     # Feature & Sentiment API entry (port 8000)
├── main_cdc.py                 # CDC Producer API entry (port 8001)
├── docker-compose.yaml         # Main compose with profiles
├── Makefile                    # Build and run commands
│
├── src/
│   ├── api/                    # API layer
│   │   ├── server.py           # Feature & Sentiment API
│   │   ├── cdc_server.py       # CDC Producer API
│   │   └── routers.py          # API routes
│   │
│   ├── streaming/              # Stream processing
│   │   ├── main.py             # Enrichment service entry
│   │   ├── llm_main.py         # LLM insight service entry
│   │   ├── enrichment/         # Enrichment processor
│   │   ├── llm_insight/        # LLM insight processor
│   │   └── ksql/               # ksqlDB SQL scripts
│   │
│   ├── cdc_producer/           # CDC data ingestion
│   ├── agent/                  # ML/Agent logic
│   ├── kafka_connect/          # Kafka Connect Dockerfile
│   └── common/                 # Shared utilities
│
├── docker/                     # Service Dockerfiles
│   ├── cdc_producer/
│   ├── streaming/
│   └── streaming-llm/
│
└── tests/                      # Unit and integration tests
```

### Docker Compose Profiles

| Profile | Services Included |
|---------|-------------------|
| `kafka` | Kafka cluster (3 brokers), Schema Registry, Kafka UI |
| `cdc` | PostgreSQL, Kafka Connect, Debezium UI, CDC Producer |
| `streaming` | ksqlDB, Enrichment service, LLM Insight service |
| `dev` | Single Kafka broker, Redis (lightweight development) |
| `full` | All services |

---

## License
Apache 2.0

