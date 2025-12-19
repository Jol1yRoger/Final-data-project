# Lichess TV Streaming + Batch Data Pipeline

## Project Overview
This project implements a complete end-to-end data pipeline for collecting, cleaning, storing, and analyzing **frequently updating real-world chess data** from **Lichess TV**.

The pipeline combines:
- **Streaming ingestion** (API → Kafka)
- **Batch processing** (hourly cleaning + daily analytics)

The workflow is orchestrated using **Apache Airflow**.  
**Kafka** is used as a message broker for streaming events, and **SQLite** is used for storage and analytics.

---

## Team
- **Member 1:** Smambayev Zhusup — 22B030443  
- **Member 2:** Em Igor — 22B030612  
- **Member 3:** Kenesbek Asylmurat — 22B030376  

---

## Project Goal
The main goal of the project is to demonstrate the ability to:

- Collect real-world data from an external API
- Simulate streaming ingestion using Kafka
- Clean and normalize raw data
- Store structured data in a database (SQLite)
- Perform **daily analytical aggregation**
- Orchestrate all steps using **Airflow DAGs**

The pipeline strictly follows the assignment requirements and consists of **three Airflow jobs (DAGs)**.

---

## Data Source
**API used:** Lichess TV feed  
**Endpoint:** `/api/tv/feed`  
**Format:** NDJSON (newline-delimited JSON stream)

### Why this API
- **Frequently updated:** live featured games update continuously
- **Structured JSON events:** consistent fields per event
- **Stable and documented:** Lichess public API
- **Meaningful data:** players, ratings, clocks, FEN position, last move
- Works well for streaming: each NDJSON line = one Kafka message

---

## Architecture Overview
### High-level data flow
`Lichess TV API → DAG 1: Continuous Ingestion → Kafka (lichess_raw_events) → DAG 2: Hourly Cleaning & Storage → SQLite (events) → DAG 3: Daily Analytics → SQLite (daily_summary)`

Each stage is implemented as a separate Airflow DAG to keep the pipeline modular and clear.

---

## DAGs Description

### DAG 1: Continuous Ingestion (API → Kafka)
- **DAG name:** `dag1_continuous_ingestion`
- **Type:** pseudo-streaming / long-running (manual trigger)
- **Purpose:** connect to the Lichess TV NDJSON stream and publish **raw JSON** messages to Kafka
- **Kafka topic:** `lichess_raw_events`
- **Task:** `ingest_lichess_to_kafka`

**Important note:**  
No cleaning or validation is performed at this stage. The goal is to preserve raw events as received from the API.

---

### DAG 2: Hourly Cleaning & Storage (Kafka → SQLite)
- **DAG name:** `dag2_hourly_cleaning`
- **Schedule:** `@hourly`
- **Purpose:** consume raw messages from Kafka, clean/validate them, and store in SQLite
- **Target table:** `events`
- **Tasks:**
  - `initialize_database`
  - `clean_and_store_events`

#### Main cleaning steps (summary)
- Parse JSON events
- Drop invalid/incomplete records (missing `game_id`, missing players, etc.)
- Convert types (ratings/clocks → integers)
- Normalize usernames (lowercase)
- Basic validation (rating range, non-negative clocks, basic FEN structure)
- Deduplicate using SQLite UNIQUE constraint
- Insert cleaned records into SQLite

---

### DAG 3: Daily Analytics (SQLite → SQLite)
- **DAG name:** `dag3_daily_analytics`
- **Schedule:** `@daily`
- **Purpose:** compute daily aggregated analytics from cleaned event data
- **Source table:** `events`
- **Target table:** `daily_summary`
- **Task:** `compute_daily_analytics`

#### Computed metrics (from the code)
- Total number of games
- Average white rating / average black rating
- Min rating / max rating
- Average “duration” estimate (based on remaining clocks when available)
- Total titled players (GM/IM/etc.)
- Rating distribution buckets:
  - 1000–1500
  - 1500–2000
  - 2000–2500
  - 2500+
- Most common “opening” proxy (derived from early `last_move` pattern)

---

## Storage Layer (SQLite)

### Database
- **File:** `data/lichess.db`

### Tables

#### `events` (cleaned event-level data)
Stores one row per cleaned game event.
- Unique constraint: `UNIQUE(game_id, ingested_at)`
- Indexes: `idx_game_id`, `idx_processed_at`

Main fields include:
- game_id, event_type, orientation
- white_player/black_player, ratings, titles, remaining seconds
- fen, last_move
- clocks (white_clock, black_clock)
- timestamps (ingested_at, processed_at)

#### `daily_summary` (daily analytics)
Stores one row per date (`summary_date` is UNIQUE).

Includes:
- summary_date
- total_games
- avg_white_rating, avg_black_rating
- min_rating, max_rating
- avg_game_duration_seconds
- total_titled_players
- rating distribution counters
- most_common_opening
- created_at

---

## Project Structure

```text
.
├── Screenshots/
│   ├── all dags.png
│   ├── dag1 audit log.png
│   ├── dag1.png
│   ├── dag11.png
│   ├── dag2 audit log.png
│   ├── dag2.png
│   ├── dag22.png
│   ├── dag3 audit log.png
│   ├── dag3.png
│   ├── dag33.png
│   └── lichesdb log.png
├── dags/
│   ├── dag1_continuous_ingestion.py
│   ├── dag2_hourly_cleaning.py
│   └── dag3_daily_analytics.py
├── report/
│   └── Report.pdf
├── data/
│   └── lichess.db
├── schema/
│   └── init_db.sql
├── utils/
│   ├── cleaning_rules.py
│   ├── db_utils.py
│   └── kafka_utils.py
└── requirements.txt
```
**Notes:**
- `dag11.png`, `dag22.png`, `dag33.png` are duplicates. For submission, keep the clearest ones.

---

## How to Run the Project

### 1) Create virtual environment
```bash
python -m venv venv
source venv/bin/activate   # Linux/Mac
# venv\Scripts\activate  # Windows
```

### 2) Install dependencies
```bash
pip install -r requirements.txt
```

### 3) Start Kafka
Run Kafka + Zookeeper (or Kafka KRaft).  
You can use Docker or a local Kafka installation.

**Create topic (example):**
```bash
kafka-topics.sh --create   --topic lichess_raw_events   --bootstrap-server localhost:9092   --partitions 1   --replication-factor 1
```

### 4) Start Airflow
Run Airflow (Docker or local). Open the UI:
- `http://localhost:8080`

### 5) Enable and run DAGs
In Airflow UI:
1. Enable **dag2_hourly_cleaning** and **dag3_daily_analytics**
2. Trigger **dag1_continuous_ingestion** manually (stream ingestion)
3. Trigger **dag2_hourly_cleaning** (or wait for hourly schedule)
4. Trigger **dag3_daily_analytics** (or wait for daily schedule)

---

## Verification (View Data)

### Using SQLite CLI
```bash
sqlite3 data/lichess.db
```

```sql
.tables

SELECT COUNT(*) FROM events;
SELECT * FROM events LIMIT 3;

SELECT COUNT(*) FROM daily_summary;
SELECT * FROM daily_summary LIMIT 3;
```

Expected results:
- Kafka receives real-time Lichess events
- Cleaned events are stored in SQLite `events`
- Daily analytics are stored in SQLite `daily_summary`
- All DAGs show **green (success)** status in Airflow

---

## Evidence (Screenshots)
All evidence screenshots are stored in `Screenshots/`:
- Airflow DAGs overview (`all dags.png`)
- Audit logs showing successful runs (`dag1/2/3 audit log.png`)
- SQLite proof (`lichesdb log.png`)

---
##  Screenshots (Evidence for Report)
All evidence is in `Screenshots/`:
- `all dags.png` — Airflow page with all DAGs
- `dag1 audit log.png` — DAG1 audit log (trigger/running)
- `dag2 audit log.png` — DAG2 success log
- `dag3 audit log.png` — DAG3 success log
- `lichesdb log.png` — SQLite proof (COUNT, LIMIT, daily_summary)

- 
## Limitations & Future Improvements
- SQLite is good for a course project but not for large-scale production systems
- API stream may require better backoff/reconnect logic under network issues
- Analytics can be extended (openings classification, player trend analysis, etc.)
- The pipeline can be upgraded to PostgreSQL and/or cloud storage

---


