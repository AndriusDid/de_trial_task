# Google Trends Pipeline

An Apache Airflow pipeline that fetches Google Trends data for configurable search terms via SerpApi, validates the results, and persists them to CSV.

## Project Structure

```
mediatech-de-trial-task/
├── dags/
│   └── daily_search_dag.py      # Airflow DAG definition
├── mediatech_de_trial_task/     # Core Python package
│   ├── csv_writer.py            # CSV output with deduplication
│   ├── date_range.py            # Dynamic date range computation
│   ├── normalize.py             # SerpApi response flattening
│   ├── serpapi_retry.py         # Retry wrapper with exponential backoff
│   ├── types.py                 # TypedDict definitions for type safety
│   └── validation.py            # Data quality checks
├── tests/                       # Unit tests
├── docker-compose.yaml
├── Dockerfile
└── requirements.txt
```

## Prerequisites

- Docker and Docker Compose
- A [SerpApi](https://serpapi.com/) API key

## Getting Started

### 1. Configure environment variables

Create a `.env` file in the project root (do not forget to add airflow 
username and password). You may modify the `example.env` for this purpose.

### 2. Build and start

```bash
docker compose build
docker compose up -d
```

This starts three services:

| Service              | Description                      | Access              |
|----------------------|----------------------------------|---------------------|
| `airflow-webserver`  | Airflow UI                       | http://localhost:8080 |
| `airflow-scheduler`  | Runs scheduled DAGs              | -                   |
| `postgres`           | Airflow metadata database        | -                   |

### 3. Set Airflow Variables

The DAG reads its configuration from three Airflow Variables. Set them via the CLI after the services are up:

```bash
docker compose exec airflow-webserver airflow variables set search_terms '["vpn", "antivirus", "ad blocker", "password manager"]'
docker compose exec airflow-webserver airflow variables set location 'US'
docker compose exec airflow-webserver airflow variables set serpapi_key '<your-serpapi-key>'
```

Or set them through the Airflow UI at **Admin > Variables**.

| Variable        | Type       | Example                                                  |
|-----------------|------------|----------------------------------------------------------|
| `search_terms`  | JSON list  | `["vpn", "antivirus", "ad blocker", "password manager"]` |
| `location`      | String     | `US`                                                     |
| `serpapi_key`   | String     | `your-serpapi-api-key`                                   |

### 4. Trigger the DAG

1. Open http://localhost:8080 and log in with your user credentials.
2. Enable the **google_trends_dag** DAG.
3. Trigger it manually or wait for the daily schedule.

Output CSV is written to `output/trends.csv`.

### 5. Run tests

```bash
docker compose exec airflow-scheduler python -m pytest tests/ -v
```

### 6. Stop

```bash
docker compose down
```

Add `-v` to also remove the Postgres data volume:

```bash
docker compose down -v
```

## Pipeline Overview

The DAG (`google_trends_dag`) runs daily with three tasks:

1. **fetch_google_trends** -- Calls SerpApi for each search term with automatic retries (exponential backoff, up to 5 attempts) on transient failures.
2. **process_trends_data** -- Flattens responses into records, runs validation (schema checks, search-term coverage, null detection, date range gap analysis), and writes to CSV.
3. **summarize_results** -- Logs aggregate statistics (mean, max, min) per search term.

---

## Design Document: Future Extensions

### Data storage

The pipeline currently writes to a single CSV file. For production use:

- **Database sink** -- Write to PostgreSQL or a cloud data warehouse (BigQuery, Redshift) instead of CSV. This enables efficient querying, indexing, and concurrent access.
- **Partitioned storage** -- Partition output by date and region (e.g., Parquet files on S3) to support large-scale historical analysis without reprocessing.

### Observability

- **Alerting** -- Integrate Airflow email/Slack notifications on validation failures or task errors, so data quality issues are caught immediately rather than discovered downstream.
- **Metrics export** -- Push pipeline metrics (record counts, validation pass/fail rates, API latency) to a monitoring system (Prometheus/Grafana) for dashboarding and trend tracking.

### Scalability

- **Parallel fetching** -- Fetch search terms concurrently using Airflow dynamic task mapping (`expand()`) instead of a sequential loop. This reduces end-to-end latency proportionally to the number of terms.
- **Incremental loading** -- Track the last successfully fetched date range and only request new data, reducing API calls and avoiding duplicate processing.

### Configuration

- **Multi-region support** -- Accept a list of regions and generate per-region DAG runs or task groups for comparative analysis.
- **Airflow Connections for API key** -- Store the SerpApi key in an Airflow Connection (instead of a Variable) to leverage built-in secret backends and encryption.

### Data quality

- **Historical trend validation** -- Compare new data against historical baselines to detect anomalous spikes or drops that may indicate API issues or real-world events.
- **Schema versioning** -- Version the `TrendRecord` schema so downstream consumers can handle format changes gracefully during migrations.

### Testing

- **Integration tests** -- Add tests that run against the real SerpApi (gated behind a flag or separate CI stage) to catch API contract changes early.
- **Snapshot tests** -- Record known-good API responses as fixtures and validate that normalization output remains stable across code changes.
