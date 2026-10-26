import os
from datetime import UTC, date, datetime, timedelta

import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

import console

CATALOG = console.validate_catalog(os.environ["PITLAKE_CATALOG"])
WAREHOUSE_ID = os.environ["DATABRICKS_WAREHOUSE_ID"]
INGEST_JOB_ID = int(os.environ["INGEST_JOB_ID"])

st.set_page_config(page_title=f"PITLake · {CATALOG}", layout="wide")


@st.cache_data(ttl=300, show_spinner="Querying the warehouse…")
def query(statement: str, params: dict | None = None) -> pd.DataFrame:
    cfg = Config()
    with (
        sql.connect(
            server_hostname=cfg.host,
            http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
            credentials_provider=lambda: cfg.authenticate,
        ) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(statement, params or {})
        return cursor.fetchall_arrow().to_pandas()


@st.cache_resource
def workspace() -> WorkspaceClient:
    return WorkspaceClient()


def days(key: str, default: int) -> int:
    return st.slider("Days up to the newest data", 7, 365, default, key=key)


def coverage() -> None:
    st.subheader("Freshness")
    fresh = query(console.freshness_query(CATALOG))
    if fresh.empty:
        st.info("The operations job has not checked freshness yet.")
    else:
        stale = int(fresh["stale"].sum())
        checked = fresh["checked_at"].max()
        st.caption(f"Checked {checked:%Y-%m-%d %H:%M} UTC: {stale} of {len(fresh)} tables stale.")
        st.dataframe(fresh.drop(columns="checked_at"), hide_index=True, use_container_width=True)

    st.subheader("Data present per source and period")
    summary = query(console.coverage_query(CATALOG))
    st.dataframe(summary, hide_index=True, use_container_width=True)
    if summary.empty:
        return
    chosen = st.selectbox("Rows per period", sorted(summary["silver_table"].unique()))
    daily = query(
        console.daily_rows_query(CATALOG),
        {"silver_table": chosen, "days": days("coverage_days", 90)},
    )
    if daily.empty:
        st.info("No files in that window.")
        return
    st.bar_chart(daily, x="period_start", y="rows_out", color="symbol")


def rejections() -> None:
    st.subheader("Rejection rates by rule")
    st.caption("Rows each drop rule rejected, over each table's files up to its newest one.")
    rates = query(console.rejection_query(CATALOG), {"days": days("rejection_days", 30)})
    if rates.empty:
        st.info("No files in that window.")
        return
    shown = rates.assign(rate=rates["rate"] * 100).rename(columns={"rate": "rate_%"})
    st.dataframe(
        shown,
        hide_index=True,
        use_container_width=True,
        column_config={"rate_%": st.column_config.NumberColumn(format="%.4f")},
    )
    hits = shown[shown["rejected"] > 0]
    if not hits.empty:
        st.bar_chart(
            hits.assign(rule=hits["silver_table"] + "." + hits["rule"]), x="rule", y="rate_%"
        )


def venues() -> None:
    st.subheader("Disagreements between the crypto venues")
    compared = query(console.cross_venue_query(CATALOG), {"days": days("venue_days", 60)})
    if compared.empty:
        st.info("No compared days in that window.")
    else:
        st.line_chart(compared, x="day", y=["divergent", "missing_left", "missing_right"])
        st.dataframe(compared, hide_index=True, use_container_width=True)

    st.subheader("Latest quality gate")
    gate = query(console.gate_query(CATALOG))
    if gate.empty:
        st.info("The gate has not run yet.")
        return
    first = gate.iloc[0]
    failed = gate[~gate["passed"] & ~gate["waived"]]
    st.caption(
        f"Run {first['run_id']} at {first['checked_at']:%Y-%m-%d %H:%M} UTC: {len(gate)} checks, "
        f"{int(gate['waived'].sum())} waived, {len(failed)} failed."
    )
    flagged = gate[~gate["passed"] | gate["waived"]]
    st.dataframe(flagged.drop(columns=["run_id", "checked_at"]), hide_index=True)


def backtests() -> None:
    st.subheader("Backtest runs")
    runs = query(console.backtests_query(CATALOG))
    if runs.empty:
        st.info("No backtests yet.")
        return
    metrics = pd.DataFrame([console.headline(m) for m in runs["metrics"]])
    listing = pd.concat(
        [
            runs[["backtest_id", "created_at", "strategy", "period_start", "period_end"]],
            metrics,
            runs[["replay_of", "reproduced"]],
        ],
        axis=1,
    )
    st.dataframe(listing, hide_index=True, use_container_width=True)
    chosen = st.selectbox("Details", runs["backtest_id"])
    row = runs[runs["backtest_id"] == chosen].iloc[0]
    st.json(
        {
            "params": row["params"],
            "metrics": console.as_dict(row["metrics"]),
            "table_versions": console.as_dict(row["table_versions"]),
            "silver_versions": console.as_dict(row["silver_versions"]),
            "gold_commit_run_id": row["gold_commit_run_id"],
            "code_commit": row["code_commit"],
            "package_version": row["package_version"],
            "input_hash": row["input_hash"],
            "result_hash": row["result_hash"],
            "mlflow": f"{row['mlflow_experiment']} run {row['mlflow_run_id']}",
        }
    )


def performance() -> None:
    st.subheader("Query cost per job and day")
    st.caption(
        "From the workspace's query history, recorded daily by the operations job. Files skipped "
        "are those Delta ruled out from statistics without reading them (ADR 0013)."
    )
    stats = query(console.performance_query(CATALOG), {"days": days("performance_days", 30)})
    if stats.empty:
        st.info("The operations job has not recorded query statistics yet.")
        return
    workloads = sorted(stats["workload"].unique())
    target = CATALOG.removeprefix("pitlake_")
    default = f"pitlake-transform-{target}"
    chosen = st.selectbox(
        "Job", workloads, index=workloads.index(default) if default in workloads else 0
    )
    job = stats[stats["workload"] == chosen]
    st.bar_chart(job, x="day", y=["read_files", "pruned_files"])
    st.line_chart(job, x="day", y="read_rows")
    st.dataframe(
        job.drop(columns="workload"),
        hide_index=True,
        use_container_width=True,
        column_config={"pruned_share": st.column_config.NumberColumn(format="%.3f")},
    )


def ingest() -> None:
    st.subheader("Run the ingestion job")
    registry = query(console.registry_query(CATALOG))
    registered = list(registry["dataset"])
    with st.form("ingest"):
        chosen = st.multiselect("Datasets", registered)
        ranged = st.checkbox("Only a date range")
        today = date.today()
        first = st.date_input("First day", today - timedelta(days=7))
        last = st.date_input("Last day", today - timedelta(days=1))
        reload = st.checkbox("Reload files already in Bronze for that range")
        max_files = st.number_input("Files per symbol (0 = no limit)", 0, 10_000, 0)
        submitted = st.form_submit_button("Start run")
    if submitted:
        try:
            params = console.ingest_parameters(
                chosen,
                first if ranged else None,
                last if ranged else None,
                reload,
                int(max_files),
                registered,
            )
            run = workspace().jobs.run_now(job_id=INGEST_JOB_ID, job_parameters=params)
            url = f"{workspace().config.host.rstrip('/')}/jobs/{INGEST_JOB_ID}/runs/{run.run_id}"
            st.success(f"Started run {run.run_id}: {url}")
        except ValueError as error:
            st.error(str(error))
        except Exception as error:
            st.error(f"The job did not start: {error}")

    st.subheader("Recent runs")
    rows = []
    for run in workspace().jobs.list_runs(job_id=INGEST_JOB_ID, limit=10):
        status = run.status
        rows.append(
            {
                "run_id": run.run_id,
                "started": datetime.fromtimestamp(run.start_time / 1000, UTC),
                "state": status.state.value if status and status.state else "",
                "result": status.termination_details.code.value
                if status and status.termination_details and status.termination_details.code
                else "",
                "parameters": {p.name: p.value or p.default for p in run.job_parameters or []},
            }
        )
    st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)


st.title(f"PITLake console · {CATALOG}")
tabs = st.tabs(["Coverage", "Rejections", "Crypto venues", "Backtests", "Performance", "Ingest"])
pages = (coverage, rejections, venues, backtests, performance, ingest)
for tab, page in zip(tabs, pages, strict=True):
    with tab:
        page()
