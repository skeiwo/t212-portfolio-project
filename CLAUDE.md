# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A trading analytics ETL pipeline that extracts data from the Trading212 broker API and external sources, loads to BigQuery, and transforms with dbt into fact/dimension tables for portfolio analytics.

## Common Commands

```bash
# Run the full ETL pipeline
python orchestration/flows/pipeline.py

# Install dbt packages (after cloning or adding new packages)
cd transform && dbt deps

# Run dbt tests
cd transform && dbt test

# Run dbt models for a specific layer
cd transform && dbt build --select stg
cd transform && dbt build --select int
cd transform && dbt build --select marts

# Run a specific dbt model
cd transform && dbt run --select stg_orders_history
```

## Setup

```bash
pip install -r requirements.txt
cd transform && dbt deps
```

Requires a `.env` file with:
- `T212_API_KEY` / `T212_API_SECRET` / `T212_BASE_URL` — Trading212 API credentials
- `GCP_CREDENTIALS` — path to GCP service account JSON keyfile
- `GCP_PROJECT` — BigQuery project ID

## Architecture

**Data flow:** Trading212 API + Yahoo Finance + Frankfurter FX API → BigQuery raw → dbt → mart tables

### Orchestration (`orchestration/`)

- `flows/pipeline.py` — single Prefect flow `etl_pipeline_flow()` that submits 6 extract tasks in parallel, then loads all results to BigQuery, then runs dbt build per layer
- `tasks/extract.py` — 6 Prefect task functions; each fetches a data source, appends `extract_timestamp`/`record_id`/`payload` columns, handles pagination and T212 rate-limit (429 + `retry-after` header)
- `tasks/load.py` — single `load_to_db()` task; writes JSON rows to BigQuery with `WRITE_TRUNCATE`
- `utils.py` — `_get_logger()` returns Prefect run logger when inside a flow, else stdlib logger

### Transformation (`transform/`)

Three-layer medallion architecture in BigQuery:

| Layer | Materialization | Schema | Purpose |
|-------|----------------|--------|---------|
| `stg/` (7 models) | view | `t212_stg` | Flatten raw JSON payloads with `json_value()` |
| `int/` (3 models) | table | `t212_int` | Business logic: dedup, weighted-avg price, stock split adjustments |
| `marts/` (4 models) | table | `t212_marts` | Fact (`fct_*`) and dimension (`dim_*`) tables |

**Key intermediate transforms:**
- `int_orders_filled` — weighted avg price `SUM(price*qty)/SUM(qty)`, split-adjusted quantity via `exp(sum(ln(ratio_old/ratio_new)))`
- `int_positions_current` — latest position snapshot per ticker using `row_number() OVER (PARTITION BY ticker ORDER BY extract_timestamp DESC)`
- `int_dividends_dedup` — latest dividend record per reference

**dbt schema tests** are defined in `_stg.yml` and `_marts.yml` (not_null, unique, foreign key relationships).

**Seed data:** `transform/seeds/raw_stock_splits.csv` — 16 historical split events loaded to `t212_raw` schema.

**dbt packages:** `dbt_utils` used for `generate_surrogate_key()`.

### dbt + Prefect integration

`pipeline.py` uses `PrefectDbtRunner` (from `prefect-dbt`) to invoke dbt build inside the flow. The runner is invoked three times with explicit `--select stg`, `--select int`, `--select marts` to enforce layer ordering.
