# Scalable Data Quality and Observability Platform for Large-Scale Data Pipelines

*NYC Yellow Taxi case study*

[Open the Streamlit dashboard](https://nyc-taxi-dashboard-zhnnxcwp33eetghgs9ipim.streamlit.app/) | [NYC TLC trip data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) | [Official Yellow Taxi data dictionary (PDF)](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)

This project uses **3,336,716 NYC Yellow Taxi records** to show how recorded trips can be replayed, checked for quality, and summarized for investigation. The upstream notebook uses Kafka, PySpark Structured Streaming, and Delta Lake. This repository contains the **Silver-to-Gold aggregation code and Streamlit dashboard**, not the upstream notebook or the original trip records.

## How the NYC Taxi data was used

The project has two separate paths:

```text
Notebook: August 2026 Parquet -> Kafka JSON replay -> Spark microbatches -> Silver / Quarantine Delta
Dashboard: separately supplied quality-enriched snapshot -> Gold aggregates -> Streamlit
```

### Apache Kafka: replaying historical trips

- **Producer and payload.** Spark first reads the August 2026 Yellow Taxi Parquet file. The notebook uses a 100-row smoke test on `taxi-trips` and a separate full replay on `taxi-trips-full`. For the full replay, each source row becomes a JSON message value. Null fields are serialized explicitly so missing passenger counts and request sources remain visible to validation. Kafka is transporting historical trips, not collecting live taxi events.
- **Partitioning and offsets.** The notebook documents `taxi-trips-full` with three Kafka partitions and replication factor one on a local broker. Replication factor one means this setup does not demonstrate broker failover. The producer's `repartition(4)` changes Spark's write-side parallelism; it does not create four Kafka partitions. Kafka offsets identify positions within a partition, not unique taxi-trip IDs or a global ordering across the topic.
- **Replay and reconciliation.** For a fresh query, `startingOffsets=earliest` starts from the earliest available offsets. A resumed query uses its checkpoint instead. The notebook compares the sum of per-partition offset ranges with the output row counts and checks final lag. This checks whether record counts reconcile for the run; it does not prove that every individual trip was written exactly once.

### PySpark Structured Streaming: parsing and validating microbatches

- **Schema and execution.** Spark decodes the Kafka message value and parses it against an explicit 21-field schema. Pickup and drop-off are parsed as timestamps without a timezone; zone and code fields use integral types, and distance and monetary amounts use doubles. The validation callback sees the parsed taxi fields rather than the Kafka message envelope. `availableNow=True` processes the available backlog in microbatches and then stops. It is not a continuously running production stream.
- **Batch sizing.** The baseline caps consumption at 2,000 Kafka offsets per trigger. The comparison starts from the earliest available offsets with a fresh checkpoint, a 5,000-offset cap, and separate output paths. A cap limits offset intake per trigger; it is not a guaranteed batch size or a throughput target. The two settings demonstrate different batch counts, not a proven speedup.
- **Validation and traceability.** `foreachBatch` calls the quality logic for each microbatch. It derives `trip_duration_minutes`, evaluates DQ01-DQ10 independently, and carries the rule labels alongside `duplicate_count`, `dq_status`, and `kafka_batch_id`. A `FAIL` anywhere takes precedence over `WARNING`; rows with neither are `PASS`. The batch ID identifies the Spark microbatch, not a Kafka partition/offset pair.
- **Duplicate scope.** DQ07 compares all original parsed fields **within the current microbatch** and flags every row in an exact duplicate group. It does not detect duplicates across microbatches. Changing batch boundaries can change which records this rule compares; the notebook and dashboard failure totals should not be attributed to DQ07 without a row-level comparison.

### Delta Lake: storing, routing, and compacting results

| Output | What is written |
| --- | --- |
| **Silver Delta** | `PASS` and `WARNING` rows, with the parsed trip fields, derived duration, rule results, overall status, and batch ID. Warnings remain available for review. |
| **Quarantine Delta** | Rows with any hard failure, retaining the rule results that explain why each row was routed there. |
| **Streaming checkpoint** | Per-query progress used by Spark to resume consumption. The replay experiment uses its own checkpoint so it can start independently. |

Silver and Quarantine are written as **two separate append operations**. Delta's transaction log identifies the active Parquet files for each table, so a Delta reader uses the table snapshot rather than treating every file on disk as current. The notebook does not demonstrate a single transaction spanning both tables or an application-level idempotency guard for retried `foreachBatch` writes. Checkpointing alone does not establish exactly-once delivery for these writes.

Repeated microbatch appends can create many small files. The notebook reads the routed Delta outputs and rewrites them to separate experimental locations with Spark `coalesce(4)` for Silver and `coalesce(2)` for Quarantine. It compares file counts, storage, and row totals before and after the rewrite. This is not native Delta `OPTIMIZE`; the measured results and their limits are listed below.

### From the evaluation snapshot to Streamlit

[`silver_data.py`](silver_data.py) reads a separately supplied, quality-enriched Delta or Parquet snapshot containing all three statuses. PyArrow and DuckDB produce [`gold/trips.parquet`](gold/trips.parquet) for trip and rule metrics, [`gold/zones.parquet`](gold/zones.parquet) for pickup and drop-off counts, and [`gold/manifest.json`](gold/manifest.json) for schema and provenance. Streamlit reads those files, not Kafka or the notebook's routed Delta outputs. It recounts existing rule labels and lets readers compare dates, vendors, payment codes, request sources, statuses, and zones. It does not rerun the Spark rules or export individual trips.

| Evidence | Row counts |
| --- | --- |
| **Notebook's routed Delta outputs** | Both saved validation runs show **3,328,863 Silver** (`PASS` or `WARNING`) + **7,853 Quarantine** (`FAIL`) = **3,336,716**. |
| **Published dashboard evaluation snapshot** | **3,313,587 PASS**, **15,262 WARNING-only**, and **7,867 with at least one FAIL** = **3,336,716**. |

These counts come from different artifacts. The dashboard's evaluation snapshot retains failed rows, unlike the notebook's routed Silver table. Equal total row counts do not establish that the artifacts have identical labels. The 14-row difference between the two failure totals is not attributed to a specific rule without a row-level comparison.

## Dataset and field dictionary

The [published manifest](gold/manifest.json) records the schema and actual pickup timestamp range, including dates outside the nominal August 2026 input month. The field meanings below come from the TLC's March 18, 2025 dictionary. Code lists are dictionary definitions, not necessarily values observed in the snapshot.

| Field(s) | Meaning and use |
| --- | --- |
| `VendorID` | Trip-record provider: `1` Creative Mobile Technologies, `2` Curb Mobility, `6` Myle Technologies, `7` Helix. Used in domain checks and vendor comparisons. |
| `tpep_pickup_datetime`, `tpep_dropoff_datetime` | Times when the meter was engaged and disengaged. Used for completeness, timestamp order, duration, and pickup date/hour trends. |
| `PULocationID`, `DOLocationID` | TLC Taxi Zone IDs at meter start and end. Used in location checks and zone counts, not as geographic coordinates. |
| `passenger_count`, `trip_distance` | Recorded passenger count and metered miles. Used in source-aware checks and distance plausibility checks, respectively. |
| `RatecodeID` | Final rate code: `1` standard, `2` JFK, `3` Newark, `4` Nassau or Westchester, `5` negotiated fare, `6` group ride, `99` null/unknown. |
| `store_and_fwd_flag` | `Y` means the vehicle stored the record before sending it; `N` means it did not. |
| `payment_type` | `0` Flex Fare, `1` credit card, `2` cash, `3` no charge, `4` dispute, `5` unknown, `6` voided trip. Used in domain checks and payment breakdowns. |
| `fare_amount`, `extra`, `mta_tax`, `tolls_amount` | Metered fare, miscellaneous extras, metered-rate tax, and tolls. |
| `tip_amount`, `total_amount` | Recorded card tips and the total recorded passenger charge. Neither includes cash tips. |
| `improvement_surcharge`, `congestion_surcharge` | Trip-start improvement surcharge and New York State congestion surcharge. |
| `airport_fee` | LaGuardia or JFK airport pickup charge. The notebook and supplied snapshot use `Airport_fee`. |
| `cbd_congestion_fee` | Per-trip charge for the MTA's Congestion Relief Zone, effective January 5, 2025. Distinct from `congestion_surcharge`. |

`request_source` is present in the notebook's input schema but is not defined in this edition of the TLC PDF. For DQ06 and DQ09, the notebook treats null `request_source` as the standard-source cohort. `trip_duration_minutes`, `duplicate_count`, the ten rule outputs, and `dq_status` are derived, not original dictionary fields. `kafka_batch_id` belongs to the notebook's streaming outputs; it is not a field in the published dashboard snapshot's source schema.

## Quality checks and dashboard results

The conditions below describe the notebook's rule logic. The counts come from the **dashboard evaluation snapshot**, not from an independent rerun of the notebook.

| Rule | What the notebook checks | Pass | Warning | Fail |
| --- | --- | ---: | ---: | ---: |
| DQ01 | Fail if either meter timestamp or either zone ID is missing. | 3,336,716 | 0 | 0 |
| DQ02 | Fail if drop-off is earlier than pickup. | 3,336,713 | 0 | 3 |
| DQ03 | Fail for negative duration; warn above 360 minutes. | 3,336,062 | 651 | 3 |
| DQ04 | Fail below 0 or above 1,000 miles; warn above 100 through 1,000 miles. | 3,336,581 | 95 | 40 |
| DQ05 | Check vendor, rate, payment, and store-and-forward domains; vendor and payment codes cannot be null. | 3,336,716 | 0 | 0 |
| DQ06 | Fail if both request source and passenger count are null. | 3,336,384 | 0 | 332 |
| DQ07 | Fail rows with an exact duplicate across the original parsed fields within the same microbatch. | 3,336,702 | 0 | 14 |
| DQ08 | Fail if either zone ID is outside 1-265; DQ01 handles missing IDs. | 3,336,716 | 0 | 0 |
| DQ09 | Fail zero-passenger rows when request source is null and both zone IDs are present. | 3,329,238 | 0 | 7,478 |
| DQ10 | Warn if any monitored fare, tax, tip, toll, surcharge, fee, or total amount is negative. | 3,322,187 | 14,529 | 0 |

The dashboard snapshot has **7,870 failed checks** and **15,275 warning checks**. These are counts of rule outcomes, not unique trips. DQ09 accounts for **95.0% of failed checks** (0.2241% of evaluated records), so zero-passenger standard-source trips are the first review priority. DQ10 accounts for **14,529 warnings** (0.4354% of evaluated records); negative values can represent refunds or adjustments, not necessarily invalid transactions or financial losses.

## Streamlit dashboard

| Tab | What you can do |
| --- | --- |
| **Executive & actions** | See headline metrics and a ranked review queue with suggested reviewers and next steps. |
| **Trip trends** | Zoom into trip and financial timelines and explore weekday and pickup-hour heatmaps. |
| **Data quality** | Compare rule outcomes, use the Pareto chart to find the largest failure contributors, and compare rates by vendor, payment code, or request source. |
| **Breakdowns & locations** | Explore linked vendor charts and compare pickup and drop-off counts by zone. |
| **Finance** | Compare recorded amounts, finite tip totals, amount coverage, and negative-payment warnings. |
| **Provenance** | Inspect the source schema, date range, and any mismatch between row status and rule labels. |

Sidebar filters for pickup date, vendor, payment code, request source, and quality status apply to the metrics and downloads. Chart selections affect only their linked visuals. Users can download aggregate CSVs for review actions, daily metrics, rule counts, and financial summaries. There is no individual-trip viewer or download.

## Reported storage and streaming experiments

| Experiment | Result | Limitation |
| --- | --- | --- |
| Delta file compaction | The report records a Spark `coalesce(4)` / `coalesce(2)` rewrite from 3,669 combined Parquet files to 6, and storage from 253.24 MB to 85.39 MB. The rewrite took 296.20 seconds and retained 3,336,716 records. | This was not native Delta `OPTIMIZE`; query speed was not benchmarked. |
| Kafka offset cap | The report records a reduction from 1,669 to 668 microbatches when increasing the cap from 2,000 to 5,000 offsets. Notebook outputs reconcile 3,336,716 records for both routed runs; the 5,000-offset run reports 7,780.88 seconds. | No baseline end-to-end runtime was recorded, so fewer microbatches do not establish a runtime speedup. |

These measurements describe the upstream experiments, not dashboard performance. Rule pass rates are not F1 scores; F1 requires ground-truth labels and true-positive, false-positive, and false-negative counts.

## Run locally

From the repository root, with Python 3.12 or newer and an activated virtual environment:

```powershell
python -m pip install -r requirements.txt
python -m streamlit run streamlit_app.py
```

The bundled `gold/` files are enough to run the dashboard. You do not need Kafka, Spark, or the original Silver files to view it.

## Refresh the hosted snapshot

1. Correct the upstream data and re-evaluate affected quality labels or derived fields. Use a quality-enriched evaluation snapshot that includes all statuses if the dashboard should show the full failure population; the notebook's routed Silver output alone excludes `FAIL` rows.
2. Build a new Gold snapshot outside the source directory:

   ```powershell
   python silver_data.py --silver "C:\path\to\quality_enriched_delta" --gold ".\gold_next"
   ```

   The builder also accepts a DQ-enriched Parquet file or a flat folder of matching Parquet files. Keep `_delta_log` for Delta input so only active files are read. Raw TLC Parquet without the required fields, derived duration, and quality labels is not sufficient input. The builder refuses to overwrite an existing output directory.
3. Validate the row count, rule totals, and provenance. Publish only the three reviewed Gold files together in one commit to `gold/`.

Streamlit Community Cloud uses `268912san/data-quality-observability-platform`, branch `main`, entry point `streamlit_app.py`. No application secrets are needed. Editing a local file or refreshing the browser does not rebuild Gold. The dashboard has no live Kafka feed, scheduled refresh, or automated alerting.

**Public-data note:** The repository and dashboard expose aggregates, not raw Silver data. Small groups can still appear in aggregates, so this is not an anonymization guarantee. Vendor comparisons are investigation leads, not evidence of cause. Recorded amounts are not profit, and trip counts by zone do not measure vehicle availability.

## Sources

- Project Kafka/PySpark/Delta notebook supplied for documentation review: ingestion, schema, rule logic, routing, and saved reconciliation outputs.
- Project technical report, *Scalable Data Quality and Observability Platform for Large-Scale Data Pipelines*: reported storage and microbatch experiments.
- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and [Yellow Taxi Trip Records data dictionary (March 18, 2025)](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf): source field descriptions.
- [Published Gold manifest](gold/manifest.json): source schema and provenance for the dashboard snapshot.
