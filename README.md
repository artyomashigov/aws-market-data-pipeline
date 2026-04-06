# Serverless Market Data Pipeline on AWS

End-to-end serverless data pipeline that ingests stock market data from an external API, stores raw data in Amazon S3, transforms it with AWS Glue, exposes curated data through Amazon Athena, and visualizes the final dataset in Power BI.

This project reflects production-style cloud data engineering architecture including ingestion, transformation, orchestration, automation, and reporting.

---

## Table of Contents

- [Project Goal](#project-goal)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Data Scope](#data-scope)
- [S3 Data Lake Structure](#s3-data-lake-structure)
- [Pipeline Stages](#pipeline-stages)
- [Automation](#automation)
- [Key Problems Solved](#key-problems-solved)
- [Example Athena Queries](#example-athena-queries)
- [Dashboard Overview](#dashboard-overview)
- [Repository Structure](#repository-structure)

---

## Project Goal

Build a fully automated daily pipeline that:

- Pulls stock market data from a financial API
- Lands raw JSON responses in Amazon S3
- Transforms nested data into clean structured Parquet files
- Makes the final dataset queryable via Amazon Athena
- Feeds a Power BI dashboard for reporting and analysis

---

## Architecture

```text
Amazon EventBridge Scheduler
        ↓
AWS Step Functions (orchestration)
        ↓
AWS Lambda (API ingestion)
        ↓
Amazon S3 — raw layer (JSON)
        ↓
AWS Glue ETL Job
        ↓
Amazon S3 — curated layer (Parquet)
        ↓
AWS Glue Crawler / Data Catalog
        ↓
Amazon Athena (SQL query layer)
        ↓
Power BI Dashboard
```

---

## Tech Stack

| Service | Purpose |
|---|---|
| AWS Lambda | Pull data from API and write raw JSON to S3 |
| Amazon S3 | Data lake, raw and curated storage layers |
| AWS Glue ETL | Transform and flatten nested JSON into Parquet |
| AWS Glue Crawler | Detect schema and register tables in Data Catalog |
| Amazon Athena | SQL query engine on top of S3 |
| AWS Step Functions | Orchestrate pipeline stages in sequence |
| Amazon EventBridge | Schedule daily pipeline execution |
| Power BI | Dashboard and visualization layer |
| Financial Modeling Prep API | Source of stock price and company data |

---

## Data Scope

The pipeline tracks the following stock symbols:

- AAPL
- AMZN
- GOOGL
- MSFT
- NVDA

For each symbol, the pipeline collects:

- Daily stock price (open, high, low, close, volume)
- Latest market quote snapshot
- Company profile (sector, industry, market cap, exchange)

---

## S3 Data Lake Structure

```
s3://artyom-market-pipeline-dev/
├── raw/
│   ├── prices/
│   │   └── extract_date=YYYY-MM-DD/
│   │       └── symbol=AAPL/
│   │           └── prices.json
│   ├── quotes/
│   │   └── extract_date=YYYY-MM-DD/
│   │       └── symbol=AAPL/
│   │           └── quote.json
│   └── profiles/
│       └── extract_date=YYYY-MM-DD/
│           └── symbol=AAPL/
│               └── profile.json
├── curated/
│   └── prices/
│       └── part-00000.parquet
├── athena-results/
└── scripts/
```

Partition-style paths (`extract_date=`, `symbol=`) were used from the start to enable clean filtering in Athena and future compatibility with partition pruning.

---

## Pipeline Stages

### 1. Ingestion — AWS Lambda

- Lambda reads configuration from environment variables (`BUCKET_NAME`, `FMP_API_KEY`, `SYMBOLS`)
- Calls the Financial Modeling Prep API for each symbol
- Wraps each response in a metadata envelope (symbol, dataset, extracted_at, source)
- Writes raw JSON files to S3 under dated partition paths
- Each daily run creates new files — raw history is preserved, not overwritten

### 2. Transformation — AWS Glue ETL

- Reads raw nested JSON from the Glue Data Catalog
- Explodes nested arrays using PySpark `explode()`
- Resolves mixed numeric types (struct with `double` and `int` fields) using `coalesce()`
- Casts all fields to consistent types
- Writes clean output to the curated S3 layer in Parquet format using `overwrite` mode

**Output schema:**

| Column | Type |
|---|---|
| symbol | string |
| extract_date | string |
| date | string |
| close | double |
| volume | bigint |

### 3. Cataloging — AWS Glue Crawler

- Scans the curated S3 path after each ETL run
- Registers or updates the `curated_prices` table in the Glue Data Catalog
- Makes the latest data immediately available to Athena

### 4. Querying — Amazon Athena

- Used to validate raw and curated data at each stage
- Supports SQL queries directly on S3 data
- Query results are stored in the `athena-results/` prefix

### 5. Orchestration — AWS Step Functions

The pipeline stages run in sequence via a Step Functions state machine:

```
Lambda ingestion → Glue ETL job (.sync) → Glue Crawler
```

The Glue ETL step uses `.sync` integration, meaning Step Functions waits for the job to finish before starting the crawler.

### 6. Scheduling — Amazon EventBridge

- EventBridge Scheduler triggers the Step Functions state machine daily at 6:00 PM
- No manual steps are required once the pipeline is deployed

---

## Automation

The full pipeline is automated end-to-end:

```
6:00 PM daily
    → EventBridge Scheduler
    → Step Functions state machine
        → Lambda (ingest raw data)
        → Glue ETL job (transform to curated Parquet)
        → Glue Crawler (refresh Data Catalog)
```

The curated dataset is rebuilt with `overwrite` mode on each run, which prevents duplicate rows in the final analytics table.

---

## Key Problems Solved

These are real implementation challenges encountered during the build:

- **IAM permissions** — configured least-privilege policies for Lambda (S3 write to `raw/`), Glue (S3 read/write to `curated/`), and Step Functions (invoke Lambda, start Glue job, start crawler)
- **Schema mismatch** — separated prices, quotes, and profiles into distinct S3 paths to avoid Athena `HIVE_PARTITION_SCHEMA_MISMATCH` errors caused by mixing different JSON structures
- **Nested data** — flattened nested JSON arrays from the API response using PySpark `explode()` in Glue
- **Struct type in close column** — Glue inferred `close` as a struct (`{double, int}`) due to mixed source types; resolved using `coalesce(col("item.close.double"), col("item.close.int")).cast("double")`
- **Duplicate column conflict** — Glue Crawler created both a column and a partition key named `symbol`; resolved by removing the non-partition duplicate from the table schema
- **Symbol compatibility** — `GOOG` was not available on the free API plan; replaced with `GOOGL`
- **Duplicate rows** — prevented by using `overwrite` mode in the Glue ETL job so the curated layer is always a clean full rebuild
- **Step Functions permissions** — added `glue:StartCrawler` inline policy to the Step Functions execution role separately from the Glue job permissions
- **ODBC setup on Mac** — manually registered the Simba Athena ODBC driver and created DSN config via terminal (`~/odbcinst.ini` and `~/odbc.ini`) since the iODBC GUI failed to register the driver

---

## Example Athena Queries

**Query curated clean data:**

```sql
SELECT *
FROM curated_prices
LIMIT 20;
```

**Filter by symbol:**

```sql
SELECT symbol, date, close, volume
FROM curated_prices
WHERE symbol = 'AAPL'
ORDER BY date DESC
LIMIT 30;
```

**Query raw nested data (before ETL):**

```sql
SELECT
    symbol,
    extract_date,
    d.date,
    d.close,
    d.volume
FROM prices_prices
CROSS JOIN UNNEST(data) AS t(d)
LIMIT 20;
```

---

## Dashboard Overview

The final Power BI dashboard was built from the curated Athena dataset and includes:

- **Latest price KPIs** — current closing price per symbol
- **Price trend (last 90 days)** — line chart by symbol
- **Trading volume comparison** — bar chart across all 5 symbols

> Screenshot: `screenshots/dashboard.png`

---

## Repository Structure

```
aws-market-data-pipeline/
├── README.md
├── lambda/
│   └── lambda_function.py
├── glue/
│   └── prices_etl.py
├── sql/
│   └── queries.sql
├── config/
│   └── example_env.txt
└── screenshots/
    └── dashboard.png
```

---

## Author

**Artyom Ashigov**

AWS-based serverless data pipeline project covering ingestion, transformation, orchestration, and analytics.
