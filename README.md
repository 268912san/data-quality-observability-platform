# Scalable Data Quality and Observability Platform for Large-Scale Data Pipelines

*NYC Yellow Taxi case study*

[Open the Streamlit dashboard](https://nyc-taxi-dashboard-zhnnxcwp33eetghgs9ipim.streamlit.app/) | [NYC TLC trip data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) | [Official Yellow Taxi data dictionary (PDF)](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)

This project uses **3,336,716 NYC Yellow Taxi records** to show how data-quality checks can guide investigation. The upstream workflow described in the technical report uses Kafka, PySpark Structured Streaming, and Delta Lake. This repository contains the **Silver-to-Gold aggregation code and Streamlit dashboard**. It does not include the upstream streaming pipeline or the original trip records.

## How the pipeline fits together

1. **Ingest and validate.** The report describes replaying taxi Parquet records through Kafka, parsing the messages in PySpark, deriving trip duration, and assigning ten rule outcomes (`PASS`, `WARNING`, or `FAIL`). The upstream workflow uses checkpoints and writes Silver and Quarantine Delta outputs.
2. **Build Gold.** [`silver_data.py`](silver_data.py) reads a quality-enriched Delta or Parquet snapshot. It uses PyArrow and DuckDB to produce [`gold/trips.parquet`](gold/trips.parquet) for trip and rule summaries, [`gold/zones.parquet`](gold/zones.parquet) for pickup and drop-off counts, and [`gold/manifest.json`](gold/manifest.json) for schema and provenance.
3. **Explore in Streamlit.** The hosted app reads those three Gold files. It recounts existing rule labels; it does not rerun the Spark rules, edit source records, or generate a Quarantine export.

**Which records are on the dashboard?** The supplied snapshot behind this app is a quality-enriched evaluation snapshot with all three outcomes, not the accepted-only Silver output described in part of the report. A failed row in the dashboard is not proof that it was written to a separate Quarantine table.

## Dataset and field dictionary

The case study uses the supplied quality-enriched snapshot associated with the August 2026 Yellow Taxi input. The [published manifest](gold/manifest.json) records the actual source schema and timestamp range, including out-of-period dates present in the input. The field meanings below come from the TLC's March 18, 2025 dictionary; code lists describe the dictionary, not necessarily every value observed in this snapshot.

| Field(s) | What they mean |
| --- | --- |
| `VendorID` | Provider of the trip record: `1` Creative Mobile Technologies, `2` Curb Mobility, `6` Myle Technologies, `7` Helix. |
| `tpep_pickup_datetime`, `tpep_dropoff_datetime` | Times when the taximeter was turned on and off. |
| `PULocationID`, `DOLocationID` | TLC Taxi Zone IDs at meter start and end. These are zone codes, not coordinates. |
| `passenger_count`, `trip_distance` | Recorded number of passengers and metered trip distance in miles. |
| `RatecodeID` | Final rate code: `1` standard, `2` JFK, `3` Newark, `4` Nassau or Westchester, `5` negotiated fare, `6` group ride, `99` null/unknown. |
| `store_and_fwd_flag` | `Y` means the record was stored in the vehicle before sending; `N` means it was not. |
| `payment_type` | `0` Flex Fare, `1` credit card, `2` cash, `3` no charge, `4` dispute, `5` unknown, `6` voided trip. |
| `fare_amount`, `extra`, `mta_tax`, `tolls_amount` | Metered fare, miscellaneous extras, metered-rate tax, and total tolls paid, respectively. |
| `tip_amount`, `total_amount` | The tip field is populated for card tips and excludes cash tips. Total amount is the recorded charge to the passenger and also excludes cash tips. |
| `improvement_surcharge`, `congestion_surcharge` | Improvement surcharge assessed at the start of the trip and New York State congestion surcharge, respectively. |
| `airport_fee` | Airport pickup charge for LaGuardia or JFK. The supplied snapshot uses the column name `Airport_fee`. |
| `cbd_congestion_fee` | Per-trip charge for the MTA's Congestion Relief Zone, effective January 5, 2025. This is distinct from `congestion_surcharge`. |

**Additional snapshot fields:** `request_source` is present in the supplied data but not defined in this edition of the TLC PDF. The project uses null `request_source` for the standard-source cohort. `trip_duration_minutes` is derived from the meter timestamps. The `DQ01` through `DQ10` result columns and `dq_status` carry quality labels; these are not original fields from the TLC dictionary.

## Quality checks and results

| Rule | Check | Pass | Warning | Fail |
| --- | --- | ---: | ---: | ---: |
| DQ01 | Required timestamps and location IDs | 3,336,716 | 0 | 0 |
| DQ02 | Drop-off is not earlier than pickup | 3,336,713 | 0 | 3 |
| DQ03 | Trip duration is plausible | 3,336,062 | 651 | 3 |
| DQ04 | Trip distance is plausible | 3,336,581 | 95 | 40 |
| DQ05 | Configured codes are in their allowed domains | 3,336,716 | 0 | 0 |
| DQ06 | Required fields for standard-source trips | 3,336,384 | 0 | 332 |
| DQ07 | Exact duplicate detection | 3,336,702 | 0 | 14 |
| DQ08 | Valid pickup and drop-off location codes | 3,336,716 | 0 | 0 |
| DQ09 | Passenger-count validity for standard-source trips | 3,329,238 | 0 | 7,478 |
| DQ10 | Review negative financial values | 3,322,187 | 14,529 | 0 |

**Row-level outcomes:** 3,313,587 PASS, 15,262 warning-only, and 7,867 with at least one FAIL. The ten rules returned **7,870 failed checks** and **15,275 warning checks**; check totals can exceed affected-row totals because one trip can trigger multiple rules. The report also describes reconciling published records with Kafka offsets and Delta outputs, and checking that compaction preserved the row count.

- **Investigate passenger counts first.** DQ09 accounts for 95.0% of failed checks and flags 0.2241% of evaluated records. Start with zero-passenger standard-source trips and compare by vendor before deciding whether zero represents missing or invalid data.
- **Reconcile negative amounts rather than rejecting them automatically.** DQ10 warns on 0.4354% of evaluated records. Refunds and adjustments can be legitimate; a warning does not establish fraud or financial loss.

## Streamlit dashboard

| Tab | What you can do |
| --- | --- |
| **Executive & actions** | See headline metrics and a ranked review queue with suggested reviewers and next steps. |
| **Trip trends** | Zoom into trip and financial timelines and explore weekday and pickup-hour heatmaps. |
| **Data quality** | Compare rule outcomes, use a Pareto chart to find the largest failure contributors, and compare rates across vendors, payment codes, or request sources. |
| **Breakdowns & locations** | Explore linked vendor charts and compare pickup and drop-off counts by zone. |
| **Finance** | Compare recorded amounts, finite tip totals, amount coverage, and negative-payment warnings. |
| **Provenance** | Inspect the source schema, date range, and any mismatch between row status and rule labels. |

Sidebar filters for pickup date, vendor, payment code, request source, and quality status apply to the metrics and downloads. Chart selections affect only their linked visuals. Users can download aggregate CSVs for review actions, daily metrics, rule counts, and financial summaries. There is no individual-trip viewer or download.

## Reported storage and streaming experiments

| Experiment | What the report measured | What it does not show |
| --- | --- | --- |
| Delta file compaction | A Spark `coalesce(4)` / `coalesce(2)` rewrite reduced the combined Parquet count from 3,669 to 6 and reported storage from 253.24 MB to 85.39 MB. The rewrite took 296.20 seconds and retained all 3,336,716 records. | This was not native Delta `OPTIMIZE`; query speed was not benchmarked. |
| Kafka offset cap | Raising `maxOffsetsPerTrigger` from 2,000 to 5,000 reduced the microbatch count from 1,669 to 668. Both runs processed 3,336,716 records and ended with zero reported lag. | The baseline end-to-end runtime was not recorded, so this is evidence of fewer microbatches, not a verified runtime speedup. |

These measurements describe the upstream experiments, not the hosted app. Rule pass rates are not F1 scores; F1 would require ground-truth labels and true-positive, false-positive, and false-negative counts.

## Run locally

From the repository root, with Python 3.12 or newer and an activated virtual environment:

```powershell
python -m pip install -r requirements.txt
python -m streamlit run streamlit_app.py
```

The bundled `gold/` files are enough to run the dashboard. You do not need Kafka, Spark, or the original Silver files to view it.

## Refresh the hosted snapshot

1. Correct the upstream data and re-evaluate any affected rule labels or derived fields.
2. Build a new Gold snapshot outside the source directory:

   ```powershell
   python silver_data.py --silver "C:\path\to\silver\taxi_trips_dq" --gold ".\gold_next"
   ```

   The builder also accepts a DQ-enriched Parquet file or a flat folder of matching Parquet files. Keep `_delta_log` for Delta input so only active files are read. Raw TLC Parquet without derived duration and quality labels is not sufficient input. The builder refuses to overwrite an existing output directory.
3. Validate the row count, rule totals, and provenance. Publish only the three reviewed Gold files together in one commit to `gold/`.

Streamlit Community Cloud is configured for `268912san/data-quality-observability-platform`, branch `main`, entry point `streamlit_app.py`. No application secrets are needed. The app is snapshot-based: changing a local file or refreshing the browser does not rebuild Gold, and there is no live Kafka feed, scheduled refresh, or automated alerting in the app.

**Public-data note:** The repository and dashboard expose aggregates, not raw Silver data. Small groups can still appear in aggregates, so this is not an anonymization guarantee. Vendor comparisons are investigation leads, not evidence of cause. Recorded amounts are not profit or proven losses, and trip counts by zone do not measure vehicle availability.

## Sources

- Project technical report, *Scalable Data Quality and Observability Platform for Large-Scale Data Pipelines*: upstream design, rule outcomes, reconciliation, and experiments.
- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and [Yellow Taxi Trip Records data dictionary (March 18, 2025)](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf): source field descriptions.
- [Published Gold manifest](gold/manifest.json): source schema and provenance for the dashboard snapshot.
