"""Streamlit multi-page: Pipeline Health & Source Coverage."""
import os
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(
    page_title="Pipeline Health — AQI Dashboard",
    page_icon="🔧",
    layout="wide",
)


@st.cache_resource
def _engine():
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    user = os.environ.get("POSTGRES_USER", "aqi_user")
    password = os.environ.get("POSTGRES_PASSWORD", "")
    db = os.environ.get("AQI_DB_NAME", "aqi_warehouse")
    return create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}",
        pool_pre_ping=True,
    )


@st.cache_data(ttl=30)
def _query(sql: str) -> pd.DataFrame:
    try:
        return pd.read_sql(text(sql), _engine())
    except Exception as exc:
        st.warning(f"DB query failed: {exc}")
        return pd.DataFrame()


st.title("🔧 Pipeline Health & Source Coverage")

# ---- KPI row ----
kpis = _query("""
    SELECT
        (SELECT COUNT(*) FROM ops.pipeline_run) AS total_runs,
        (SELECT COUNT(*) FROM ops.pipeline_run WHERE status = 'SUCCESS') AS success_runs,
        (SELECT COUNT(*) FROM ops.pipeline_run WHERE status = 'FAILED') AS failed_runs,
        (SELECT COUNT(*) FROM ops.rejected_records) AS rejected_records,
        (SELECT COUNT(*) FROM ops.ingestion_log WHERE status = 'FAILED') AS ingestion_errors,
        (SELECT COUNT(DISTINCT source_id) FROM ops.source_coverage) AS sources_covered
""")

if not kpis.empty:
    k = kpis.iloc[0]
    cols = st.columns(6)
    cols[0].metric("Total Runs", f"{int(k.total_runs or 0):,}")
    cols[1].metric("✅ Successful", f"{int(k.success_runs or 0):,}")
    cols[2].metric("❌ Failed", f"{int(k.failed_runs or 0):,}")
    cols[3].metric("Rejected Records", f"{int(k.rejected_records or 0):,}")
    cols[4].metric("Ingestion Errors", f"{int(k.ingestion_errors or 0):,}")
    cols[5].metric("Sources Covered", f"{int(k.sources_covered or 0):,}")

st.divider()

tab_runs, tab_coverage, tab_dq, tab_rejected = st.tabs([
    "🏃 Pipeline Runs", "📡 Source Coverage", "🔍 DQ Results", "🗑️ Rejected Records"
])

with tab_runs:
    st.subheader("Recent Pipeline Runs")
    runs = _query("""
        SELECT run_id::text AS run_id, started_at, finished_at, status,
               EXTRACT(EPOCH FROM (finished_at - started_at)) AS duration_secs,
               summary_json
        FROM ops.pipeline_run
        ORDER BY started_at DESC
        LIMIT 50
    """)
    if runs.empty:
        st.info("No pipeline runs yet. Trigger the `aqi_etl_pipeline` DAG in Airflow.")
    else:
        # Success rate chart
        status_counts = runs["status"].value_counts().reset_index()
        status_counts.columns = ["status", "count"]
        st.caption("Run status and durations below are limited to the 50 most recent pipeline runs.")
        col1, col2 = st.columns([1, 2])
        with col1:
            st.plotly_chart(
                px.pie(status_counts, names="status", values="count",
                       title="Run Status Distribution",
                       color="status",
                       color_discrete_map={"SUCCESS": "#2ecc71", "FAILED": "#e74c3c", "RUNNING": "#3498db"}),
                use_container_width=True,
            )
        with col2:
            st.plotly_chart(
                px.bar(runs.head(20), x="started_at", y="duration_secs",
                       color="status",
                       color_discrete_map={"SUCCESS": "#2ecc71", "FAILED": "#e74c3c"},
                       title="Run Duration (seconds)"),
                use_container_width=True,
            )
        st.dataframe(runs[["run_id", "started_at", "finished_at", "status", "duration_secs"]],
                     use_container_width=True, hide_index=True)

with tab_coverage:
    st.subheader("Source Coverage Matrix")
    coverage = _query("""
        SELECT source_id, layer, row_count, station_count,
               date_min, date_max, status, checked_at
        FROM ops.source_coverage
        WHERE run_id = (
            SELECT run_id FROM ops.pipeline_run ORDER BY started_at DESC LIMIT 1
        )
        ORDER BY checked_at DESC, source_id, layer
    """)
    if coverage.empty:
        st.info("No coverage records yet.")
    else:
        # Pivot for heatmap view
        try:
            pivot = coverage.pivot_table(
                index="source_id", columns="layer", values="row_count", aggfunc="max"
            )
            st.subheader("Row Count by Source × Layer")
            st.dataframe(pivot.style.background_gradient(cmap="Greens", axis=None),
                         use_container_width=True)
        except Exception:
            pass
        st.dataframe(coverage, use_container_width=True, hide_index=True)

    st.subheader("Recent Ingestion Log (last 100)")
    ingest_log = _query("""
        SELECT source_id, status, rows_extracted, endpoint_or_file,
               error_message, extraction_ts
        FROM ops.ingestion_log
        ORDER BY extraction_ts DESC
        LIMIT 100
    """)
    if not ingest_log.empty:
        st.dataframe(ingest_log, use_container_width=True, hide_index=True)

with tab_dq:
    st.subheader("Data Quality Check Results")
    dq = _query("""
        SELECT source_id, run_id::text AS run_id, check_name, layer, status,
               passed_count, failed_count, total_count, failure_rate, threshold, notes, checked_at
        FROM ops.dq_results
        ORDER BY checked_at DESC
        LIMIT 200
    """)
    if dq.empty:
        st.info("No DQ results yet.")
    else:
        dq_summary = dq.groupby(["source_id", "check_name", "status"]).size().reset_index(name="count")
        if not dq_summary.empty:
            st.plotly_chart(
                px.bar(dq_summary, x="source_id", y="count", color="status",
                       barmode="group",
                       color_discrete_map={"PASS": "#2ecc71", "FAIL": "#e74c3c", "WARN": "#f39c12", "SKIP": "#95a5a6"},
                       title="DQ Check Results by Source"),
                use_container_width=True,
            )
        st.dataframe(dq, use_container_width=True, hide_index=True)

with tab_rejected:
    st.subheader("Rejected Records")
    rejected = _query("""
        SELECT source_id, run_id::text AS run_id, layer, rule_violated, rejection_reason,
               pollutant_code, station_id, timestamp_utc, original_row, rejected_at
        FROM ops.rejected_records
        ORDER BY rejected_at DESC
        LIMIT 100
    """)
    if rejected.empty:
        st.success("✅ No rejected records found.")
    else:
        by_reason = rejected.groupby("rejection_reason").size().reset_index(name="row_count")
        st.plotly_chart(
            px.bar(by_reason, x="rejection_reason", y="row_count",
                   title="Rejection Reasons"),
            use_container_width=True,
        )
        st.dataframe(rejected, use_container_width=True, hide_index=True)
