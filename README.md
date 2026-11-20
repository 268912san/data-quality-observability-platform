# NYC Taxi dashboard

Interactive trip and data-quality analytics built with Streamlit, using reviewed Gold aggregates only.

## Dashboard sections

- **Executive & actions:** ranked DQ investigation queue, affected checks, suggested reviewers, vendor concentration and an observed demand review prompt.
- **Trip trends:** recorded trip volume, zoomable financial and trip metrics, weekday/hour heatmaps, and normalized demand per observed weekday date.
- **Data quality:** supplied row outcomes, DQ01-DQ10 counts, failure Pareto, daily rule-label rates and vendor/source/payment risk plots with a minimum cohort size.
- **Breakdowns & locations:** normalized weekday demand, pickup/drop-off imbalance, linked vendor comparisons, source/payment distributions and busiest TLC location IDs.
- **Finance:** recorded finite tips, total-amount coverage, negative-payment rule warnings and financial breakdowns by payment/vendor/source and supplied quality status.
- **Provenance:** source snapshot identity and schema, with explicit limitations.

Sidebar filters affect all analysis and downloads. Chart-level selections affect only linked visualizations. Clearing a multiselect selects zero trips.

## Run locally

```text
python -m pip install -r requirements.txt
python -m streamlit run streamlit_app.py
```

## Streamlit Community Cloud

Deploy repository `268912san/nyc-taxi-dashboard`, branch `main`, entry point `streamlit_app.py`, using Python 3.12 or newer. No application secrets are needed. Public access permits viewing without a visitor login.

## Data and interpretation

The bundled Gold snapshot aggregates 3,336,716 supplied Silver rows at Delta version 0. Silver records, the original ZIP and local environments are not included. Aggregates can include small cohorts and are not an anonymization guarantee.

DQ labels were supplied with Silver. The app recounts them rather than rerunning or certifying the original rule logic. Failed checks can overlap on one trip. Failure-rate comparisons are unadjusted for trip mix; recommendations are investigation prompts, not causal findings, assigned tickets or automatic cleaning actions.

Recorded amounts are not recognized revenue, profit or proven losses. Negative financial values can represent refunds or corrections. DQ10 counts do not equal trips with a negative total amount. Tip sums are not average tip percentages; cash tips may not be recorded.

Demand normalization uses dates observed in the selected cohort, not an assumed complete calendar. Location imbalance is grouped by trip pickup date and does not establish real-time vehicle availability or shortages. No forecasting or unmatched previous-period comparison is claimed.

The snapshot is not a live taxi feed. No raw-record viewer or raw-record download is provided. Keep credentials and company files out of this public repository.

Source: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

Free hosting can sleep and has resource limits; no uptime guarantee is implied.
