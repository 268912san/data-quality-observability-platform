"""Interactive charts read Gold aggregates, not Silver records or the original file."""

import json
from datetime import date
from pathlib import Path

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

from silver_data import FILTER_COLUMNS, query_gold
from insight_views import analyze, render_executive, render_finance, render_operations, render_quality


STATUS_COLORS = alt.Scale(
    domain=["PASS", "WARNING", "FAIL"], range=["#16856b", "#e5a42b", "#d75454"]
)
# Bump when cached aggregate result fields or their meaning change.
AGGREGATE_QUERY_REVISION = 2


@st.cache_data(max_entries=32, show_spinner="Querying Silver-derived aggregates...")
def load_results(
    gold: str,
    fingerprint: tuple[int, ...],
    date_range: tuple[str, str] | None,
    selections: dict[str, list[str | None]],
    query_revision: int,
) -> dict:
    return query_gold(Path(gold), date_range, selections)


def number(value: object, decimals: int = 0, prefix: str = "") -> str:
    if pd.isna(value):
        return "N/A"
    return f"{prefix}{value:,.{decimals}f}"


def bars(frame: pd.DataFrame, label: str, title: str) -> alt.Chart:
    data = frame.copy()
    data[label] = data[label].map(lambda value: "(missing)" if pd.isna(value) else str(value))
    return (
        alt.Chart(data)
        .mark_bar(cornerRadiusEnd=3, color="#227f91")
        .encode(
            x=alt.X("trips:Q", title="Trips"),
            y=alt.Y(f"{label}:N", sort="-x", title=title),
            tooltip=[alt.Tooltip(f"{label}:N", title=title), alt.Tooltip("trips:Q", format=",")],
        )
    )


def render(gold: Path) -> None:
    st.title("NYC Yellow Taxi - Silver Analytics")
    st.caption("Interactive trip and quality metrics computed from the supplied Silver snapshot.")
    try:
        paths = [gold / name for name in ("manifest.json", "trips.parquet", "zones.parquet")]
        fingerprint = tuple(path.stat().st_mtime_ns for path in paths)
        manifest = json.loads(paths[0].read_text(encoding="utf-8"))
        if manifest["format_version"] != 1:
            raise ValueError("Unsupported Gold manifest version")
        with st.sidebar:
            st.header("Filter trips")
            all_dates = st.checkbox("Include all pickup dates", value=True)
            date_range = None
            if not all_dates:
                if manifest["pickup_min"] is None or manifest["pickup_max"] is None:
                    st.error("No non-null pickup dates are available.")
                    st.stop()
                minimum = date.fromisoformat(manifest["pickup_min"][:10])
                maximum = date.fromisoformat(manifest["pickup_max"][:10])
                selected_dates = st.date_input(
                    "Pickup date range", value=(minimum, maximum),
                    min_value=minimum, max_value=maximum,
                )
                if len(selected_dates) != 2:
                    st.info("Select a start and end date.")
                    st.stop()
                date_range = tuple(value.isoformat() for value in selected_dates)
            labels = {
                "vendor": "Vendor ID", "payment": "Payment code",
                "source": "Request source", "status": "Quality status",
            }
            selections = {}
            for column in FILTER_COLUMNS:
                options = manifest["options"][column]
                selections[column] = st.multiselect(
                    labels[column], options, default=options,
                    format_func=lambda value, column=column: (
                        "Standard (request_source IS NULL)"
                        if value is None and column == "source"
                        else "(missing)" if value is None else str(value)
                    ),
                    key=f"filter_{column}",
                )
            st.caption("Clearing a filter selects zero trips. Dates use recorded pickup time, not UTC ingestion time.")
            with st.expander("Can I share this dashboard?"):
                st.write(
                    "A localhost URL works only on the computer running the app. A public "
                    "HTTPS deployment can be shared with anyone without visitor login. "
                    "Keep Silver private and restrict access if the audience is limited."
                )
                st.write(
                    "You can also share downloaded aggregate CSVs or chart PNGs. These "
                    "are snapshots, not a live dashboard. Public aggregates can contain "
                    "small cohorts; they are not claimed to be anonymized."
                )
        results = load_results(
            str(gold), fingerprint, date_range, selections, AGGREGATE_QUERY_REVISION
        )
    except (OSError, ValueError, KeyError, duckdb.Error) as error:
        st.error(f"Cannot load Silver dashboard aggregates: {error}")
        st.stop()

    totals = results["totals"]
    rows = int(totals["trips"])
    snapshot = (
        f"Delta v{manifest['delta_version']}"
        if manifest.get("source_format", "delta") == "delta" else "Parquet snapshot"
    )
    st.caption(
        f"Silver {snapshot} | {manifest['active_files']} active files | "
        f"{manifest['rows']:,} source rows | Gold computed {manifest['computed_at_utc']}"
    )
    st.info(
        "The supplied Silver table is quality-enriched, not failure-free. Existing DQ labels "
        "are preserved; the dashboard recounts them but does not rerun or certify rule logic."
    )
    if manifest["status_rule_disagreement_rows"]:
        st.warning(
            f"{manifest['status_rule_disagreement_rows']:,} source rows have dq_status "
            "inconsistent with their rule labels. Status filters use supplied dq_status; "
            "failed-row metrics use any DQ01-DQ10 FAIL."
        )
    first = st.columns(4)
    first[0].metric("Filtered trips", number(rows))
    first[1].metric("Total recorded amount", number(totals["total_amount"], 2, "$"))
    first[2].metric("Average recorded amount", number(totals["average_amount"], 2, "$"))
    first[3].metric("Average distance (miles)", number(totals["average_distance"], 2))
    second = st.columns(4)
    second[0].metric("Rows with any rule FAIL", number(totals["rows_with_rule_fail"]))
    second[1].metric("Warning-only rows", number(totals["warning_only_rows"]))
    second[2].metric(
        "Rule-clean row rate",
        f"{100 * (rows - totals['rows_with_rule_fail']) / rows:.2f}%" if rows else "N/A",
    )
    second[3].metric("Average duration (minutes)", number(totals["average_duration"], 2))
    st.caption(
        "Amounts include supplied negative values (for example refunds); averages use non-null "
        "finite values and retain outliers. Rule-clean includes warning-only rows. "
        "Failed rows are not a quarantine export."
    )
    if rows == 0:
        st.warning("No trips match these filters. Adjust the sidebar selections.")

    analysis = analyze(results)
    st.html(Path(__file__).with_name("dashboard_tabs.css"))
    st.subheader("Explore the dashboard")
    st.caption("Click a section tab below to switch views. The filled green tab is the current section.")
    executive, trends, quality, breakdowns, finance, provenance = st.tabs(
        ["Executive & actions", "Trip trends", "Data quality", "Breakdowns & locations",
         "Finance", "Provenance"]
    )
    with executive:
        render_executive(results, analysis)
    with trends:
        st.subheader("Trip volume over time")
        grain = st.selectbox("Trend granularity", ["Day", "Month"], index=1)
        daily = results["daily"].copy()
        if not daily.empty:
            daily["period"] = daily["pickup_date"].dt.strftime(
                "%Y-%m-%d" if grain == "Day" else "%Y-%m"
            ).fillna("(missing date)")
            grouped = daily.groupby(["period", "status"], as_index=False)["trips"].sum()
            chart = alt.Chart(grouped).mark_bar().encode(
                x=alt.X("period:N", sort="ascending", title="Recorded pickup period"),
                y=alt.Y("trips:Q", title="Trips"),
                color=alt.Color("status:N", scale=STATUS_COLORS, title="Quality status"),
                tooltip=["period:N", "status:N", alt.Tooltip("trips:Q", format=",")],
            )
            st.altair_chart(chart, width="stretch")
            st.caption(
                "Only periods present in the selection are shown; gaps are not filled. "
                "All-dates mode includes anomalous pickup dates."
            )
        st.subheader("Explore financial and trip metrics")
        trend_metric = st.selectbox(
            "Trend metric",
            ["Recorded amount ($)", "Average amount ($)", "Average distance (miles)", "Average duration (minutes)"],
            key="trend_metric",
        )
        if not daily.empty:
            numeric = [
                "amount_sum", "amount_count", "distance_sum", "distance_count",
                "duration_sum", "duration_count",
            ]
            timeline = daily.loc[daily["pickup_date"].notna()].copy()
            timeline["period"] = timeline["pickup_date"].dt.to_period(
                "D" if grain == "Day" else "M"
            ).dt.to_timestamp()
            timeline = timeline.groupby("period", as_index=False)[numeric].sum(min_count=1)
            if trend_metric == "Recorded amount ($)":
                timeline["value"] = timeline["amount_sum"]
            else:
                component = {
                    "Average amount ($)": "amount",
                    "Average distance (miles)": "distance",
                    "Average duration (minutes)": "duration",
                }[trend_metric]
                denominator = timeline[f"{component}_count"].replace(0, float("nan"))
                timeline["value"] = timeline[f"{component}_sum"] / denominator
            st.altair_chart(
                alt.Chart(timeline).mark_line(point=True, color="#227f91").encode(
                    x=alt.X("period:T", title="Recorded pickup period"),
                    y=alt.Y("value:Q", title=trend_metric, scale=alt.Scale(zero=False)),
                    tooltip=[
                        alt.Tooltip("period:T", title="Period"),
                        alt.Tooltip("value:Q", title=trend_metric, format=",.2f"),
                    ],
                ).interactive(),
                width="stretch",
            )
            st.caption(
                "Scroll to zoom; drag to pan; double-click to reset. Averages are weighted "
                "by contributing records. Null pickup dates are excluded from this timeline."
            )

        st.subheader("Day-of-week and hour heatmap")
        heatmap_metric = st.selectbox(
            "Heatmap metric", ["Trips", "Average trips / observed weekday date",
                               "Recorded amount ($)", "Rule-fail rate (%)"],
            key="heatmap_metric",
        )
        if not results["heatmap"].empty:
            field, scheme = {
                "Trips": ("trips", "blues"),
                "Average trips / observed weekday date": ("trips_per_observed_date", "blues"),
                "Recorded amount ($)": ("total_amount", "redblue"),
                "Rule-fail rate (%)": ("fail_percent", "oranges"),
            }[heatmap_metric]
            heatmap = results["heatmap"].copy()
            weekdays = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            heatmap["day"] = heatmap["weekday"].map(dict(enumerate(weekdays, start=1)))
            cell = alt.selection_point(name="demand_cell", fields=["day", "pickup_hour"])
            st.altair_chart(
                alt.Chart(heatmap).mark_rect(cornerRadius=2).encode(
                    x=alt.X("pickup_hour:O", title="Recorded pickup hour"),
                    y=alt.Y("day:N", sort=weekdays, title=None),
                    color=alt.Color(f"{field}:Q", title=heatmap_metric, scale=alt.Scale(scheme=scheme)),
                    opacity=alt.condition(cell, alt.value(1), alt.value(0.3)),
                    tooltip=[
                        alt.Tooltip("day:N", title="Weekday"), alt.Tooltip("pickup_hour:O", title="Hour"),
                        alt.Tooltip("trips:Q", format=","),
                        alt.Tooltip("total_amount:Q", title="Recorded amount ($)", format=",.2f"),
                        alt.Tooltip("fail_percent:Q", title="Rule-fail rate (%)", format=".2f"),
                    ],
                ).add_params(cell),
                width="stretch",
            )
            st.caption(
                "Click a cell to highlight it; Shift-click adds cells; double-click clears. "
                "Trip totals and amounts span selected dates. The average-trip metric divides "
                "by observed dates for that weekday, including observed dates with no trips at that hour. "
                "Missing pickup dates/hours are excluded; empty cells are not filled."
            )
        st.subheader("Pickup hour")
        if not results["hourly"].empty:
            st.altair_chart(
                alt.Chart(results["hourly"]).mark_bar(color="#227f91").encode(
                    x=alt.X("pickup_hour:O", title="Recorded pickup hour"),
                    y=alt.Y("trips:Q", title="Trips"),
                    tooltip=["pickup_hour:O", alt.Tooltip("trips:Q", format=",")],
                ),
                width="stretch",
            )
        st.download_button(
            "Download filtered daily aggregates",
            results["daily"].to_csv(index=False),
            file_name="taxi-daily-aggregates.csv", mime="text/csv",
        )

    with quality:
        st.subheader("Row outcomes")
        if not results["status"].empty:
            st.altair_chart(
                alt.Chart(results["status"]).mark_arc(innerRadius=65).encode(
                    theta=alt.Theta("trips:Q"),
                    color=alt.Color("status:N", scale=STATUS_COLORS),
                    tooltip=["status:N", alt.Tooltip("trips:Q", format=",")],
                ),
                width="stretch",
            )
        rules = results["rules"].copy()
        rules["fail_percent"] = 100 * rules["fail"] / rows if rows else None
        st.subheader("DQ01-DQ10 outcomes in this selection")
        outcome = st.selectbox("Show rules with", ["Any outcome", "Failures", "Warnings"])
        shown = rules
        if outcome == "Failures":
            shown = rules.loc[rules["fail"] > 0]
        elif outcome == "Warnings":
            shown = rules.loc[rules["warning"] > 0]
        st.dataframe(shown, hide_index=True, width="stretch")
        checks = rules.melt(id_vars="rule", value_vars=["warning", "fail"], var_name="outcome", value_name="checks")
        st.altair_chart(
            alt.Chart(checks).mark_bar().encode(
                x=alt.X("rule:N", title="Rule"), y=alt.Y("checks:Q", title="Flagged checks"),
                color=alt.Color(
                    "outcome:N", scale=alt.Scale(domain=["warning", "fail"], range=["#e5a42b", "#d75454"])
                ),
                tooltip=["rule:N", "outcome:N", alt.Tooltip("checks:Q", format=",")],
            ),
            width="stretch",
        )
        st.caption(
            f"Failed checks: {number(rules['fail'].sum())}. Unique rows with any rule FAIL: "
            f"{number(totals['rows_with_rule_fail'])}. One trip can fail multiple rules."
        )
        st.download_button(
            "Download filtered rule counts", rules.to_csv(index=False),
            file_name="taxi-dq-aggregates.csv", mime="text/csv",
        )
        render_quality(results, analysis)

    with breakdowns:
        render_operations(results, analysis)
        st.subheader("Linked vendor comparison")
        vendors = results["distributions"]["vendor"].copy()
        if not vendors.empty:
            vendors["vendor"] = vendors["vendor"].map(
                lambda value: "(missing)" if pd.isna(value) else str(value)
            )
            vendor = alt.selection_point(name="vendor_comparison", fields=["vendor"])
            bubbles = alt.Chart(vendors).mark_circle().encode(
                x=alt.X("average_distance:Q", title="Average distance (miles)", scale=alt.Scale(zero=False)),
                y=alt.Y("average_amount:Q", title="Average recorded amount ($)", scale=alt.Scale(zero=False)),
                size=alt.Size("trips:Q", title="Trips", scale=alt.Scale(range=[100, 1800])),
                color=alt.Color("vendor:N", title="Vendor ID"),
                opacity=alt.condition(vendor, alt.value(0.85), alt.value(0.2)),
                tooltip=[
                    alt.Tooltip("vendor:N", title="Vendor ID"),
                    alt.Tooltip("trips:Q", format=","),
                    alt.Tooltip("average_amount:Q", title="Average amount ($)", format=",.2f"),
                    alt.Tooltip("average_distance:Q", title="Average distance (miles)", format=".2f"),
                    alt.Tooltip("fail_percent:Q", title="Rule-fail rate (%)", format=".2f"),
                ],
            ).add_params(vendor)
            vendor_amounts = alt.Chart(vendors).mark_bar().encode(
                x=alt.X("vendor:N", title="Vendor ID"),
                y=alt.Y("total_amount:Q", title="Total recorded amount ($)"),
                color=alt.Color("vendor:N", title="Vendor ID"),
                tooltip=[
                    alt.Tooltip("vendor:N", title="Vendor ID"),
                    alt.Tooltip("total_amount:Q", title="Recorded amount ($)", format=",.2f"),
                ],
            ).transform_filter(vendor)
            st.altair_chart(
                alt.hconcat(bubbles, vendor_amounts).resolve_scale(color="shared"),
                width="stretch",
            )
            st.caption(
                "Click a bubble to filter the linked amount chart; Shift-click compares "
                "multiple vendors; double-click resets. Bubble size represents trip count. "
                "Chart highlighting does not change sidebar filters or KPI totals. "
                "Vendors with missing averages cannot be plotted as bubbles."
            )
        for column, label in (("vendor", "Vendor ID"), ("payment", "Payment code"), ("source", "Request source")):
            frame = results["distributions"][column]
            if not frame.empty:
                st.subheader(f"Trips by {label.lower()}")
                st.altair_chart(bars(frame, column, label), width="stretch")
        st.subheader("Busiest location IDs")
        direction = st.radio("Location direction", ["Pickup", "Drop-off"], horizontal=True)
        top_n = st.slider("Number of locations", min_value=5, max_value=30, value=10)
        locations = results["zones"].loc[results["zones"]["direction"] == direction].head(top_n)
        if not locations.empty:
            st.altair_chart(bars(locations, "location_id", "TLC location ID"), width="stretch")
            st.dataframe(locations, hide_index=True, width="stretch")
        st.caption("Location IDs are supplied TLC codes. No unverified zone names or coordinates are invented.")

    with finance:
        render_finance(results, analysis)

    with provenance:
        st.write(
            "Data path: ZIP import or local Silver snapshot -> computed Gold "
            "aggregates -> interactive dashboard. The web app reads only Gold aggregates."
        )
        st.write("Source archive SHA-256:", manifest["archive_sha256"] or "Not recorded")
        st.write("Status/rule disagreements:", manifest["status_rule_disagreement_rows"])
        st.write("Rows missing pickup time:", manifest["pickup_min"], "to", manifest["pickup_max"])
        st.dataframe(pd.DataFrame(manifest["columns"]), hide_index=True, width="stretch")
        st.caption(
            "Silver rows have not been deleted, corrected or reclassified. Kafka, a "
            "quarantine export, alerts and rule re-evaluation are not implemented here."
        )
