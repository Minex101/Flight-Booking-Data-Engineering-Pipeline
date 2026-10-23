# ✈️ Flight Bookings Data Engineering Pipeline

An end-to-end, metadata-driven data pipeline on Databricks that ingests flight booking data (bookings, flights, airports, passengers), processes it through a Bronze → Silver → Gold medallion architecture, and serves it as a star schema ready for analytics.

Built entirely on the Databricks Lakehouse: Auto Loader for ingestion, Lakeflow Declarative Pipelines for Silver, parameterized PySpark notebooks for a reusable Gold layer, Delta Lake at every layer, all governed by Unity Catalog and orchestrated with Databricks Jobs.

## 🛠️ Tech Stack
 
| Category        | Tools                                                             |
| --------------- | ----------------------------------------------------------------- |
| **Platform**    | Databricks · Unity Catalog · UC Volumes                           |
| **Processing**  | PySpark · Spark Structured Streaming · Spark SQL                  |
| **Ingestion**   | Auto Loader (`cloudFiles`)                                        |
| **Pipelines**   | Lakeflow Declarative Pipelines (`pyspark.pipelines`) · Auto CDC   |
| **Storage**     | Delta Lake (`MERGE`, `DeltaTable`)                                |
| **Orchestration** | Databricks Jobs (For Each task, task values)                    |
| **Languages**   | Python · SQL                                                      |
 
---

# 🏗️ Architecture

# 1. Orchestration - Databricks Workflow Job
 
The whole medallion chain runs as a single **Databricks Job** made of dependent tasks:
 
- A **Parameters** task defines the list of sources and publishes it with `dbutils.jobs.taskValues.set(key="output_key", value=src_array)`.
- A **For Each** task loops over that list and runs the **Bronze** notebook once per source (`src_folder` = `bookings`, `airports`, `customers`, `flights`).
- The **Silver** Lakeflow Declarative Pipeline runs once Bronze is complete.
- **GOLD_Dims** runs per dimension with different widget parameters, followed by **GOLD_Fact** to handle the gold layer.
- Each task only runs after its upstream task succeeds, so any failure stops the chain.

# 2. 🥉 Bronze - Dynamic Incremental Ingestion
 
A single **Bronze** notebook, parameterized by `src_folder`, handles every source:
 
- **Auto Loader** (`cloudFiles`) incrementally picks up only new files on each run.
- **Idempotency**: each source has its own checkpoint, so every file is processed exactly once and reruns are safe.
- **Schema handling**: schema is inferred and tracked per source, with malformed records captured in `_rescued_data`.
- Output is written as Delta to `/Volumes/databricksete/bronze/bronzevolume/<source>/data/`.

# 3. 🥈 Silver - Lakeflow Declarative Pipelines
 
`silver_transformations.py` uses **`pyspark.pipelines` (`dp`)** to declare the Silver layer as streaming tables, so Databricks manages dependencies, checkpoints and incremental processing (UPSERTS).
 
**Bookings** (`stage_bookings` → `transform_bookings` → `silver_bookings`)
- Streams from Bronze into a staging table, then a view casts `amount` to `Double`, converts `booking_date` to a date, adds `modifiedDate` and drops `_rescued_data`.
- **Data quality expectations** with `@dp.expect_all_or_drop`: rows with a NULL `booking_id` or `passenger_id` are dropped and tracked in the pipeline's data quality metrics.

**Flights, Passengers, Airports** (`trans_*` → `silver_*`)
- Each source is streamed into a transform table with a `modifiedDate` column.
- **Auto CDC** with `dp.create_auto_cdc_flow` upserts into the Silver table as **SCD Type 1**, keyed on the business key and sequenced by `modifiedDate`, so the latest version of each record always wins and out-of-order data is handled.
| Silver table        | Business key   | Strategy          |
| ------------------- | -------------- | ----------------- |
| `silver_bookings`   | `booking_id`   | Append + DQ rules |
| `silver_flights`    | `flight_id`    | Auto CDC · SCD 1  |
| `silver_passengers` | `passenger_id` | Auto CDC · SCD 1  |
| `silver_airports`   | `airport_id`   | Auto CDC · SCD 1  |
