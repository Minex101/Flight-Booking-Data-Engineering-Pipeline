# ✈️ Flight Bookings Data Engineering Pipeline

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Apache Spark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD4?style=for-the-badge&logoColor=white)
![Unity Catalog](https://img.shields.io/badge/Unity%20Catalog-1B3139?style=for-the-badge&logo=databricks&logoColor=white)
![Auto Loader](https://img.shields.io/badge/Auto%20Loader-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Lakeflow Declarative Pipelines](https://img.shields.io/badge/Lakeflow%20Declarative%20Pipelines-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Databricks Jobs](https://img.shields.io/badge/Databricks%20Jobs-1B3139?style=for-the-badge&logo=databricks&logoColor=white)
![Structured Streaming](https://img.shields.io/badge/Structured%20Streaming-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logoColor=white)

An end-to-end, **metadata-driven** lakehouse pipeline built entirely on **Databricks**. It ingests flight booking data (bookings, flights, airports, passengers), processes it through a **Bronze → Silver → Gold** medallion architecture with **Delta Lake** at every layer, and serves it as a **star schema** for analytics, all governed by **Unity Catalog**.

# 🛠️ Tech Stack
 
| Category        | Tools                                                             |
| --------------- | ----------------------------------------------------------------- |
| **Platform**    | Databricks · Unity Catalog · UC Volumes                           |
| **Processing**  | PySpark · Spark Structured Streaming · Spark SQL                  |
| **Ingestion**   | Auto Loader (`cloudFiles`)                                        |
| **Pipelines**   | Lakeflow Declarative Pipelines (`pyspark.pipelines`) · Auto CDC   |
| **Storage**     | Delta Lake (`MERGE`, `DeltaTable`)                                |
| **Orchestration** | Databricks Jobs (For Each task, task values)                    |
| **Languages**   | Python · SQL                                                      |

# 🏗️ Architecture

The whole medallion chain runs as **one Databricks Job** on **Serverless** compute. Each task starts only after the upstream task succeeds, so any failure stops the chain.

<img width="1352" height="164" alt="image" src="https://github.com/user-attachments/assets/5c69b72a-d05a-4cf1-8e19-24cdefdf04b7" />

The job runs five tasks in sequence, as shown in the pipeline above:

| Step | Task | What it does |
| ---- | ---- | ------------ |
| 1 | **Parameters** | Publishes the source list (`bookings`, `airports`, `customers`, `flights`) as a job task value |
| 2 | **Bronze_Autoloader** *(For Each)* | Loops over that list and runs the `Bronze` notebook once per source |
| 3 | **Silver_Transformations** | Triggers the `silver_transformations` Lakeflow Declarative Pipeline |
| 4 | **Gold_Dimension_Tables** *(For Each)* | Loops over the dimension configs and runs the `GOLD_Dims` notebook once per dimension |
| 5 | **Gold_Fact_Tables** | Runs `GOLD_Fact` to build `FactBookings` from the finished dimensions |
 
The two **For Each** loops make the job metadata-driven: adding a new source or dimension is a config change, not new code. All objects live in the `databricksete` catalog across `bronze`, `silver` and `gold` schemas.

## 1. 🥉 Bronze - Dynamic Incremental Ingestion
 
A single **Bronze** notebook, parameterized by `src_folder`, handles every source:
 
- **Auto Loader** (`cloudFiles`) incrementally picks up only new files on each run.
- **Idempotency**: each source has its own checkpoint, so every file is processed exactly once and reruns are safe.
- **Schema handling**: schema is inferred and tracked per source, with malformed records captured in `_rescued_data`.
- Output is written as Delta to `/Volumes/databricksete/bronze/bronzevolume/<source>/data/`.

## 2. 🥈 Silver - Lakeflow Declarative Pipelines
 
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

## 3. 🎖️ Gold - Reusable Star Schema
 
**`GOLD_Dims`**: one widget-parameterized notebook builds every SCD Type 1 dimension (`DimFlights`, `DimAirports`, `DimPassengers`) from its business keys, CDC column and source/target names.
- Loads incrementally from `max(modifiedDate)` in the target, with a full load on first run and an optional **backdated refresh**.
- Splits rows into updates vs inserts with a left join on business keys. New rows get **surrogate keys** (`max key + 1 + monotonically_increasing_id()`) and `create_date` / `update_date` audit columns.
- Delta **`MERGE`** on the surrogate key, updating only when incoming data is newer.
**`GOLD_Fact`**: builds `FactBookings` from a **config-driven, dynamically generated SQL query**.
- Each dimension in the config adds its surrogate key and a `LEFT JOIN` on business keys, with composite keys supported.
- Same incremental, backdated-refresh and "newer wins" `MERGE` logic, matched on the dimension keys + `booking_date`.

| Table           | Type      | Grain / Key                                                               |
| --------------- | --------- | ------------------------------------------------------------------------- |
| `FactBookings`  | Fact      | `DimPassengersKey`, `DimFlightsKey`, `DimAirportsKey`, `booking_date` · `amount` |
| `DimPassengers` | Dimension | `DimPassengersKey` → `passenger_id`                                       |
| `DimFlights`    | Dimension | `DimFlightsKey` → `flight_id`                                             |
| `DimAirports`   | Dimension | `DimAirportsKey` → `airport_id`                                           |

## ⚡ How It Works (TL;DR)
 
A Databricks Job loops over the sources → Auto Loader incrementally lands new files in Bronze → a Lakeflow Declarative Pipeline applies data quality rules and Auto CDC (SCD 1) in Silver → reusable Gold notebooks build surrogate-keyed dimensions and the `FactBookings` star schema with Delta `MERGE`.
