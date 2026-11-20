# NYC Taxi dashboard

Interactive trip and data-quality analytics built with Streamlit.

## Run locally

```text
python -m pip install -r requirements.txt
python -m streamlit run streamlit_app.py
```

## Streamlit Community Cloud

Deploy repository `268912san/nyc-taxi-dashboard`, branch `main`, entry point `streamlit_app.py`. Select Python 3.12 or newer in Advanced settings. No application secrets are needed. Configure public app access for viewing without a login.

## Data and provenance

The bundled Gold snapshot aggregates 3,336,716 supplied Silver rows at Delta version 0. Silver records, the original ZIP and local environments are not included. The public dataset contains aggregate amounts and counts, including small cohorts; it is not claimed to be anonymized or differentially private.

DQ outcomes were supplied with the Silver table. The dashboard recounts them; it does not rerun or certify the original rule logic. Rows with FAIL labels remain represented. A trip can fail several rules, so failed checks differ from unique failed rows. Negative amounts and outliers are not silently removed.

Sidebar filters apply to all charts and downloads. Chart-level selections only affect their linked visualizations. Updates require replacing the Gold snapshot; this app is not a live taxi feed.

Source: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

A public deployment and this repository are viewable by anyone. Do not add private data, credentials or company files. Free hosting can sleep and has resource limits; no uptime guarantee is implied.
