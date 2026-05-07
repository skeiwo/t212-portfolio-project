# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A personal trading analytics ETL pipeline that extracts data from the Trading 212 broker API and Yahoo Finance, loads to BigQuery, and transforms with dbt into a star schema for portfolio analytics. Built as a portfolio piece demonstrating modern data engineering practices: medallion architecture, Prefect orchestration, dbt modeling with tests, and dimensional modeling.

## Common Commands

```bash
# Run the full ETL pipeline (note: module-style invocation)
python -m orchestration.flows.pipeline

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
- `T212_API_KEY` — Trading 212 API key
- `T212_BASE_URL` — Trading 212 base URL (live or demo)
- `GCP_CREDENTIALS` — path to GCP service account JSON keyfile
- `GCP_PROJECT` — BigQuery project ID

`profiles.yml` lives in `transform/` but is gitignored. The GCP service account JSON is also gitignored and lives outside the repo.

## Architecture

**Data flow:** Trading 212 API + Yahoo Finance + exchange rates API → BigQuery raw → dbt staging → intermediate → marts (star schema)

### Orchestration (`orchestration/`)

- `flows/pipeline.py` — single Prefect flow `etl_pipeline_flow()` that:
  1. Submits 6 extract tasks in parallel via `.submit()`
  2. Submits 6 load tasks in parallel, each implicitly awaiting its corresponding extract via Prefect's future-as-argument pattern
  3. Explicitly waits for all load futures via `f.wait()` before running dbt
  4. Runs dbt with a single `PrefectDbtRunner.invoke(["build", "--select", "stg", "int", "marts"])` call — `dbt build` runs `seed → run → test` in dependency order across all selected layers
- `tasks/extract.py` — 6 Prefect task functions for: open positions, orders history, dividends, exchange rates, tradable stocks, Yahoo historical prices
  - Each returns `list[dict]` with shape `{extract_timestamp, record_id, payload, ...}` (additional columns vary per source)
  - Trading 212 endpoints handle pagination via `nextPagePath` and 429 rate limits via `Retry-After` header
  - Yahoo prices uses `yfinance` with `auto_adjust=False` and a 5-year lookback (`LOOKBACK_DAYS = 1825`); fetches only prices, no company info or corporate actions yet
- `tasks/load.py` — single `load_to_db()` task; writes JSON rows to BigQuery via `load_table_from_json` with `WRITE_TRUNCATE` (configurable via `write_mode` parameter)
- `utils.py` — `_get_logger()` returns Prefect's `get_run_logger()` when inside a flow context, falls back to stdlib logger when called standalone

### Transformation (`transform/`)

Three-layer medallion architecture in BigQuery, with separate schemas per layer:

| Layer | Folder | Materialization | Schema | Purpose |
|-------|--------|----------------|--------|---------|
| Staging | `stg/` (7 models) | view | `t212_stg` | Flatten raw JSON via `json_value()`, cast types, `nullif(..., '')` for empty strings |
| Intermediate | `int/` (3 models) | table | `t212_int` | Dedup, derive missing fields, apply split adjustments — pure prep work, no metadata enrichment |
| Marts | `marts/` (4 models) | table | `t212_marts` | Star schema: `dim_instruments` + 3 fact tables, joined via `isin` |

**Bronze tables** (`t212_raw` schema, populated by `load_to_db`):
- `raw_open_positions`, `raw_orders_history`, `raw_dividends`, `raw_exchange_rates`, `raw_tradable_stocks`, `raw_historical_prices`
- All bronze tables currently use `WRITE_TRUNCATE` (full reload each run); future plan to revisit per-table strategy
- Seed: `raw_stock_splits` (manually maintained CSV)

### Key design decisions

**Medallion roles:**
- Bronze stores raw JSON payloads in a `payload` column alongside metadata (`extract_timestamp`, `record_id`, sometimes a typed timestamp like `record_created_at`)
- Staging unpacks the JSON, casts types, normalizes nulls
- Intermediate does business logic only — no metadata enrichment (this happens in marts)
- Marts produces the final star schema

**School A architecture (Kimball-style):**
- Dimensions live in `marts/`, not in `intermediate/`
- Facts join to dimensions at query time in Metabase, not denormalized in dbt
- Calculation inputs (stock splits, exchange rates) ARE used in intermediate where they affect the math; descriptive metadata (instrument names, types) is reserved for dim tables

**Naming conventions in staging:**
- snake_case columns, no `AS` keyword in aliases, single space between expression and alias

**Stock split adjustment math:**
- Splits seed encodes ratios as `ratio_old:ratio_new` reading as the announcement (e.g., NVDA 10-for-1 forward split = `ratio_old=10, ratio_new=1`)
- Multiplier formula: `ratio_old / ratio_new` (NVDA = 10)
- Cumulative product across multiple splits via `exp(sum(ln(ratio)))` trick (BigQuery has no native `PRODUCT()` aggregate)
- Adjustment applies only to splits where `split_date > order.filled_at`
- `int_orders_filled` keeps both raw and `_split_adjusted` versions of quantity and price

**Key intermediate transforms:**
- `int_orders_filled`:
  - Filters `status = 'FILLED'` AND `fill_type != 'STOCK_SPLIT'` (T212 represents corporate actions as pseudo-orders; filter them, use the seed instead)
  - Aggregation-based dedup (`group by order_id`) rather than row_number — necessary because T212 splits some ETF orders into multiple fill events that should be combined back into a single logical order
  - Weighted average fill price: `sum(price * qty) / sum(qty)` (and same for fx_rate)
  - Derives `filled_value_eur` from `coalesce(filled_value, abs(filled_quantity) * fill_price)` because T212 doesn't populate `filled_value` for sells (which are typically quantity-based)
  - Joins splits on `isin` (not ticker) — ISIN is more stable and globally unique
  - Uses `net_value` as the canonical EUR cash impact (always populated for fills)
- `int_positions_current` — latest position snapshot per ticker via `row_number() OVER (PARTITION BY ticker ORDER BY extract_timestamp DESC)`. With current truncate-loading this is a no-op, but kept for forward-compatibility with append-only bronze
- `int_dividends_dedup` — latest dividend record per `reference` UUID

**`dim_instruments` filtering:**
- Filtered to ISINs that appear in any fact table (orders ∪ positions ∪ dividends), not the full 15k+ tradable universe
- Sourced from `union distinct` across the three intermediate models, then inner joined to deduped `stg_tradable_stocks`

**Fact table conventions:**
- All EUR-denominated columns suffixed with `_eur` (`net_value_eur`, `total_cost_eur`, `unrealized_pnl_eur`)
- `isin` is the canonical join key to `dim_instruments` everywhere
- Primary keys: `order_id` for orders, `isin` for positions, `dividend_id` (renamed from `reference`) for dividends
- Lineage column `extract_timestamp` retained on facts for debugging

### dbt details

- **Project file:** `transform/dbt_project.yml`
- **Profile:** `t212` (defined in gitignored `transform/profiles.yml`)
- **Model paths:** `models/stg/`, `models/int/`, `models/marts/`
- **Seeds:** `transform/seeds/raw_stock_splits.csv` — manually maintained list of historical splits (small enough to be reasonable; splits are rare events)
- **Sources YAML:** `models/stg/_stg.yml` declares `t212_raw` source group with all raw tables
- **Tests:** `not_null` and `unique` on natural keys across staging models; `not_null` + `unique` + `relationships` to `dim_instruments` on facts. Future work: `accepted_values` tests on categorical columns (status, side, dividend_type, etc.)
- **Packages:** `dbt_utils` used for `generate_surrogate_key()` in seed-driven models

### dbt + Prefect integration

`pipeline.py` uses `PrefectDbtRunner` from `prefect_dbt` (the modern API; `prefect_dbt.cli.DbtCoreOperation` is deprecated). The runner is configured via `PrefectDbtSettings(project_dir="transform/", profiles_dir="transform/")` and called once per flow run with `dbt build --select stg int marts`. By default the runner raises on dbt failures, which propagates to a failed Prefect flow run.

## Open follow-ups

- Expand staging-layer dbt tests with `accepted_values` for categorical columns
- Final architecture decision on bronze ingestion strategy (currently truncate-and-replace for all tables; potential per-table mix of incremental + append-only + dedup-downstream)
- Deploy to Raspberry Pi via Docker (Path B: containerized for portfolio polish, simple `docker run` from cron for execution)
- README.md for the repo (currently absent — biggest portfolio-piece gap)