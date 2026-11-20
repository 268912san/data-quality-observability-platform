"""Evidence-based summaries derived only from the dashboard's filtered aggregates."""

import altair as alt
import pandas as pd
import streamlit as st


RULE_GUIDANCE = {
    "DQ01": ("Critical fields", "Source ingestion", "Inspect missing pickup/drop-off times and locations; correct source mapping before accepting affected records."),
    "DQ02": ("Timestamp order", "Source ingestion", "Trace trips whose drop-off precedes pickup. Check timestamp parsing and source clocks; do not swap times automatically."),
    "DQ03": ("Trip duration", "Data quality", "Review negative or unusually long durations with timestamp checks. Validate thresholds before excluding trips."),
    "DQ04": ("Trip distance", "Data quality", "Investigate extreme distances by vendor and source. Validate units and meter data; do not cap values without approval."),
    "DQ05": ("Allowed codes", "Data contracts", "Compare flagged codes against the applicable TLC dictionary and source-specific allowed domains."),
    "DQ06": ("Source completeness", "Source onboarding", "Profile required fields within standard-source trips separately from dispatched trips. Confirm the original completeness policy."),
    "DQ07": ("Exact duplicates", "Pipeline engineering", "Check ingestion retries using original record fields. Confirm whether all copies or only excess copies should fail before deduplication."),
    "DQ08": ("Location validity", "Reference data", "Validate pickup and drop-off codes against the current TLC zone domain, including documented special codes."),
    "DQ09": ("Passenger count", "Source onboarding", "Investigate zero-passenger standard trips by source and vendor. Confirm zero semantics; do not impute passenger counts."),
    "DQ10": ("Negative payments", "Finance review", "Reconcile flagged financial values with refunds and adjustments. A negative-value warning alone does not establish fraud or loss."),
}
WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def labeled(frame: pd.DataFrame, column: str) -> pd.DataFrame:
    result = frame.copy()
    result[column] = result[column].map(
        lambda value: "Standard (NULL)" if pd.isna(value) and column == "source"
        else "(missing)" if pd.isna(value) else str(value)
    )
    return result


def analyze(results: dict) -> dict:
    rules = results["rules"].copy()
    rows = int(results["totals"]["trips"])
    actions = []
    for outcome in ("fail", "warning"):
        nonzero = rules.loc[rules[outcome] > 0].sort_values(
            [outcome, "rule"], ascending=[False, True]
        )
        for item in nonzero.to_dict("records"):
            title, owner, action = RULE_GUIDANCE[item["rule"]]
            actions.append({
                "Rule": item["rule"], "Issue": title, "Outcome": outcome.upper(),
                "Checks": int(item[outcome]),
                "Selected trips (%)": 100 * item[outcome] / rows if rows else None,
                "Suggested reviewer": owner, "Recommended next step": action,
            })
    action_frame = pd.DataFrame(actions, columns=[
        "Rule", "Issue", "Outcome", "Checks", "Selected trips (%)",
        "Suggested reviewer", "Recommended next step",
    ])
    pareto = rules.loc[rules["fail"] > 0, ["rule", "fail"]].sort_values(
        ["fail", "rule"], ascending=[False, True]
    ).copy()
    if not pareto.empty:
        pareto["cumulative_percent"] = 100 * pareto["fail"].cumsum() / pareto["fail"].sum()
        pareto["issue"] = pareto["rule"].map(lambda rule: RULE_GUIDANCE[rule][0])

    daily = results["daily"].copy()
    if daily.empty:
        timeline = pd.DataFrame(columns=[
            "pickup_date", "trips", "failed_rows", "warning_only_rows",
            "amount_sum", "amount_count", "tip_sum", "fail_percent", "warning_percent",
        ])
    else:
        timeline = daily.groupby("pickup_date", dropna=False, as_index=False)[[
            "trips", "failed_rows", "warning_only_rows", "amount_sum", "amount_count", "tip_sum",
        ]].sum(min_count=1).sort_values("pickup_date")
        timeline["fail_percent"] = 100 * timeline["failed_rows"] / timeline["trips"]
        timeline["warning_percent"] = 100 * timeline["warning_only_rows"] / timeline["trips"]
    dated = timeline.loc[timeline["pickup_date"].notna()].copy()
    if dated.empty:
        weekday = pd.DataFrame(columns=["weekday", "trips", "observed_dates", "trips_per_date"])
    else:
        dated["weekday"] = dated["pickup_date"].dt.day_of_week + 1
        weekday = dated.groupby("weekday", as_index=False).agg(
            trips=("trips", "sum"), observed_dates=("pickup_date", "nunique")
        )
        weekday["trips_per_date"] = weekday["trips"] / weekday["observed_dates"]
        weekday["day"] = weekday["weekday"].map(dict(enumerate(WEEKDAYS, 1)))

    zones = results["zones"]
    if zones.empty:
        balance = pd.DataFrame(columns=["location_id", "Pickup", "Drop-off", "net_pickups"])
    else:
        balance = zones.groupby(
            ["location_id", "direction"], dropna=False
        )["trips"].sum().unstack("direction", fill_value=0).reindex(
            columns=["Pickup", "Drop-off"], fill_value=0
        ).reset_index()
        balance["net_pickups"] = balance["Pickup"] - balance["Drop-off"]
        balance = balance.loc[
            balance["net_pickups"].abs().sort_values(ascending=False).index
        ].copy()
    return {
        "actions": action_frame, "pareto": pareto, "timeline": timeline,
        "weekday": weekday, "location_balance": balance,
    }


def render_executive(results: dict, analysis: dict) -> None:
    totals = results["totals"]
    rows = int(totals["trips"])
    st.subheader("What deserves attention first?")
    if not rows:
        st.info("Select trips to generate evidence-based insights and recommended review actions.")
        return
    actions = analysis["actions"]
    failed_checks = results["rules"]["fail"].sum()
    a, b, c = st.columns(3)
    a.metric("Fully PASS by rule labels", f"{rows - totals['rows_with_rule_fail'] - totals['warning_only_rows']:,.0f}")
    b.metric("Failed checks", f"{failed_checks:,.0f}")
    c.metric("Warning checks", f"{results['rules']['warning'].sum():,.0f}")
    if failed_checks:
        dominant = analysis["pareto"].iloc[0]
        st.warning(
            f"Start with {dominant['rule']} ({dominant['issue']}): "
            f"{dominant['fail']:,.0f} failures, "
            f"{100 * dominant['fail'] / failed_checks:.1f}% of failed checks in this selection. "
            "This is a concentration of checks, not a count of additional unique failed trips."
        )
    else:
        st.success("No rule FAIL labels in this selection. Review warnings and source coverage before calling the data clean.")
    if not actions.empty:
        st.table(actions.head(5).style.format({"Selected trips (%)": "{:.3f}"}))
        st.download_button(
            "Download review action queue", actions.to_csv(index=False),
            file_name="taxi-review-actions.csv", mime="text/csv",
        )
        st.caption(
            "Ranked by FAIL before WARNING, then affected checks. Suggested reviewers and "
            "next steps are recommendations, not assigned tickets or proven root causes. "
            "A record can appear under multiple actions."
        )
    vendors = results["distributions"]["vendor"]
    if totals["rows_with_rule_fail"] and not vendors.empty:
        largest = labeled(vendors, "vendor").sort_values(
            ["failed_rows", "trips"], ascending=False
        ).iloc[0]
        st.info(
            f"Source investigation: vendor {largest['vendor']} contributes "
            f"{largest['failed_rows']:,.0f} of the "
            f"{totals['rows_with_rule_fail']:,.0f} rows with any rule FAIL "
            f"({100 * largest['failed_rows'] / totals['rows_with_rule_fail']:.1f}%). "
            "Review that feed and its trip mix first; this concentration does not prove a vendor caused the failures."
        )
    heatmap = results["heatmap"]
    if not heatmap.empty:
        peak = heatmap.sort_values(
            ["trips_per_observed_date", "trips"], ascending=False
        ).iloc[0]
        st.info(
            f"Operations review: {WEEKDAYS[int(peak['weekday']) - 1]} "
            f"{int(peak['pickup_hour']):02}:00 has the highest selected average hourly volume "
            f"({peak['trips_per_observed_date']:,.0f} trips across "
            f"{int(peak['observed_dates'])} observed weekday dates). "
            "Compare several representative weeks before changing staffing; this is not a forecast."
        )
    st.caption(
        "Insights recompute for the current sidebar selection. A PASS-only or vendor-specific "
        "selection is not an overall pipeline assessment. No previous-period comparison is "
        "claimed from an unmatched or incomplete reporting period."
    )


def render_quality(results: dict, analysis: dict) -> None:
    st.subheader("Which rules explain most failed checks?")
    pareto = analysis["pareto"]
    if not pareto.empty:
        order = pareto["rule"].tolist()
        base = alt.Chart(pareto).encode(x=alt.X("rule:N", sort=order, title="Rule"))
        counts = base.mark_bar(color="#d75454").encode(
            y=alt.Y("fail:Q", title="Failed checks"),
            tooltip=["rule:N", "issue:N", alt.Tooltip("fail:Q", format=",")],
        )
        cumulative = base.mark_line(point=True, color="#183c57").encode(
            y=alt.Y("cumulative_percent:Q", title="Cumulative share (%)", scale=alt.Scale(domain=[0, 100])),
            tooltip=["rule:N", alt.Tooltip("cumulative_percent:Q", format=".1f")],
        )
        st.altair_chart(
            alt.layer(counts, cumulative).resolve_scale(y="independent"), width="stretch"
        )
    else:
        st.info("No failed checks to rank for this selection.")
    st.subheader("Quality-rate trend")
    timeline = analysis["timeline"].loc[analysis["timeline"]["pickup_date"].notna()].copy()
    if not timeline.empty:
        timeline["day"] = timeline["pickup_date"].dt.strftime("%Y-%m-%d")
        chart = timeline.melt(
            id_vars=["day", "trips"], value_vars=["fail_percent", "warning_percent"],
            var_name="measure", value_name="percent",
        )
        st.altair_chart(
            alt.Chart(chart).mark_line(point=True).encode(
                x=alt.X("day:N", sort="ascending", title="Observed pickup date"),
                y=alt.Y("percent:Q", title="Share of selected trips (%)"),
                color=alt.Color("measure:N", title="Rule-label outcome"),
                tooltip=["day:N", "measure:N", alt.Tooltip("percent:Q", format=".3f"), alt.Tooltip("trips:Q", format=",")],
            ),
            width="stretch",
        )
        st.caption("Failure means any rule FAIL; warning-only means no FAIL and at least one WARNING. Missing dates and dates with no selected trips are not plotted.")

    st.subheader("Cohort risk comparison")
    dimension = st.selectbox("Compare quality by", ["vendor", "source", "payment"], key="risk_dimension")
    minimum = st.number_input(
        "Minimum trips for rate comparisons", min_value=1, value=1000, step=1,
        key="risk_minimum",
    )
    frame = labeled(results["distributions"][dimension], dimension)
    qualifying = frame.loc[frame["trips"] >= minimum].copy()
    st.caption(
        "Small cohorts are excluded from this comparison only, not from KPIs. "
        "Rates are unadjusted for trip mix and do not establish vendor performance or causality."
    )
    if qualifying.empty:
        st.info("No cohort meets the minimum trip count. Lower the comparison threshold or broaden the filters.")
    else:
        st.altair_chart(
            alt.Chart(qualifying).mark_circle().encode(
                x=alt.X("trips:Q", title="Selected trips"),
                y=alt.Y("fail_percent:Q", title="Rows with any rule FAIL (%)"),
                size=alt.Size("failed_rows:Q", title="Failed rows", scale=alt.Scale(range=[100, 1600])),
                color=alt.Color(f"{dimension}:N"),
                tooltip=[
                    alt.Tooltip(f"{dimension}:N"), alt.Tooltip("trips:Q", format=","),
                    alt.Tooltip("failed_rows:Q", format=","),
                    alt.Tooltip("fail_percent:Q", format=".3f"),
                ],
            ),
            width="stretch",
        )
        st.dataframe(
            qualifying[[dimension, "trips", "failed_rows", "warning_only_rows", "fail_percent"]]
            .sort_values(["failed_rows", "trips"], ascending=False),
            hide_index=True, width="stretch",
        )


def render_operations(results: dict, analysis: dict) -> None:
    st.subheader("Demand per observed weekday date")
    weekday = analysis["weekday"]
    if not weekday.empty:
        st.altair_chart(
            alt.Chart(weekday).mark_bar(color="#227f91").encode(
                x=alt.X("day:N", sort=WEEKDAYS, title="Recorded pickup weekday"),
                y=alt.Y("trips_per_date:Q", title="Trips / observed weekday date"),
                tooltip=["day:N", alt.Tooltip("trips:Q", format=","),
                         "observed_dates:Q", alt.Tooltip("trips_per_date:Q", format=",.1f")],
            ),
            width="stretch",
        )
    st.caption("Normalized by distinct pickup dates present for that weekday in the selection. Entirely absent dates are not assumed to be zero-demand days.")
    st.subheader("Pickup versus drop-off imbalance")
    balance = labeled(analysis["location_balance"], "location_id").head(15)
    if not balance.empty:
        st.altair_chart(
            alt.Chart(balance).mark_bar().encode(
                x=alt.X("net_pickups:Q", title="Pickups minus drop-offs"),
                y=alt.Y("location_id:N", sort=balance["location_id"].tolist(), title="TLC location ID"),
                color=alt.condition(alt.datum.net_pickups > 0, alt.value("#227f91"), alt.value("#e5a42b")),
                tooltip=["location_id:N", alt.Tooltip("Pickup:Q", format=","),
                         alt.Tooltip("Drop-off:Q", format=","), alt.Tooltip("net_pickups:Q", format=",")],
            ),
            width="stretch",
        )
    st.caption(
        "The 15 largest absolute count imbalances are shown. Both ends are grouped by "
        "the trip's pickup date. This is not an origin-destination map, real-time vehicle "
        "availability or proof of a shortage. Review actual routes and fleet supply before repositioning."
    )


def render_finance(results: dict, analysis: dict) -> None:
    totals = results["totals"]
    st.subheader("Recorded financial indicators")
    rows = int(totals["trips"])
    a, b, c = st.columns(3)
    tips = totals["total_tips"]
    a.metric("Sum of recorded finite tips", "N/A" if pd.isna(tips) else f"${tips:,.2f}")
    a.caption("Not an average tip percentage; cash tips may not be recorded.")
    count = totals["amount_count"]
    b.metric("Finite total-amount coverage", f"{100 * count / rows:.2f}%" if rows else "N/A")
    negatives = results["rules"].set_index("rule").loc["DQ10", "warning"]
    c.metric("Negative-payment rule warnings", f"{negatives:,.0f}")
    st.caption(
        "Recorded amounts are not recognized revenue, profit or a measure of financial loss. "
        "DQ10 covers the supplier's monitored financial fields; it does not mean every "
        "flagged trip has a negative total amount. Refunds and corrections need reconciliation."
    )
    dimension = st.selectbox("Financial breakdown", ["payment", "vendor", "source"], key="finance_dimension")
    frame = labeled(results["distributions"][dimension], dimension)
    if not frame.empty:
        st.altair_chart(
            alt.Chart(frame).mark_bar(color="#16856b").encode(
                x=alt.X("total_amount:Q", title="Net recorded amount ($)"),
                y=alt.Y(f"{dimension}:N", sort="-x", title=dimension.title()),
                tooltip=[alt.Tooltip(f"{dimension}:N"), alt.Tooltip("trips:Q", format=","),
                         alt.Tooltip("total_amount:Q", format=",.2f"),
                         alt.Tooltip("total_tips:Q", format=",.2f"),
                         alt.Tooltip("average_amount:Q", format=",.2f")],
            ),
            width="stretch",
        )
        st.dataframe(
            frame[[dimension, "trips", "total_amount", "average_amount", "total_tips", "amount_count"]],
            hide_index=True, width="stretch",
        )
        st.download_button(
            "Download filtered financial breakdown", frame.to_csv(index=False),
            file_name="taxi-financial-aggregates.csv", mime="text/csv",
        )
    st.subheader("Recorded amount by supplied quality status")
    daily = results["daily"]
    if not daily.empty:
        by_status = daily.groupby("status", as_index=False).agg(
            amount_sum=("amount_sum", lambda values: values.sum(min_count=1)),
            trips=("trips", "sum"),
        )
        st.altair_chart(
            alt.Chart(by_status).mark_bar().encode(
                x=alt.X("status:N", title="Supplied quality status"),
                y=alt.Y("amount_sum:Q", title="Net recorded amount ($)"),
                color="status:N",
                tooltip=["status:N", alt.Tooltip("trips:Q", format=","),
                         alt.Tooltip("amount_sum:Q", format=",.2f")],
            ),
            width="stretch",
        )
    st.caption("Amounts on FAIL rows are associated with supplied status labels, not proven losses, refunds or recoverable money.")
