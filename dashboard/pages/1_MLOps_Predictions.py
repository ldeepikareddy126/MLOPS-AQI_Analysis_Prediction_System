"""Streamlit multi-page: MLOps Predictions & Drift page."""
import os
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(
    page_title="MLOps & Predictions — AQI Dashboard",
    page_icon="🤖",
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


@st.cache_data(ttl=60)
def _query(sql: str) -> pd.DataFrame:
    try:
        return pd.read_sql(text(sql), _engine())
    except Exception as exc:
        st.warning(f"DB query failed: {exc}")
        return pd.DataFrame()


def _api_base() -> str:
    return os.environ.get("API_BASE_URL", "http://api:8000")


st.title("🤖 MLOps Lifecycle & Predictions")

# ---- Summary metrics ----
summary = _query("""
    SELECT
      (SELECT COUNT(*) FROM monitoring.prediction_log)            AS total_preds,
      (SELECT COUNT(*) FROM monitoring.alerts WHERE NOT is_resolved) AS active_alerts,
      (SELECT COUNT(*) FROM monitoring.drift_metrics
       WHERE evaluated_at = (SELECT MAX(evaluated_at) FROM monitoring.drift_metrics)
         AND drift_status = 'SIGNIFICANT') AS sig_drifts,
      (SELECT COUNT(*) FROM analytics.features_daily)             AS feature_vecs
""")

cols = st.columns(4)
if not summary.empty:
    s = summary.iloc[0]
    cols[0].metric("Successful API Predictions", f"{int(s.total_preds or 0):,}")
    cols[1].metric("🚨 Active Alerts", f"{int(s.active_alerts or 0):,}")
    cols[2].metric("📊 Significant Drifts", f"{int(s.sig_drifts or 0):,}")
    cols[3].metric("Feature Vectors Stored", f"{int(s.feature_vecs or 0):,}")

st.divider()

tab_pred, tab_drift, tab_perf, tab_alerts, tab_health, tab_api = st.tabs([
    "📈 Predictions", "🌀 Feature Drift", "📊 Model Performance", "🚨 Alerts", "⚙️ API Health", "🔗 API Test"
])

with tab_pred:
    st.subheader("Recent Predictions & Ground Truth")
    preds = _query("""
        SELECT city_id, as_of_date, prediction_date, predicted_aqi,
               actual_aqi, predicted_category, actual_category,
               model_name, model_version, prediction_type, created_at
        FROM analytics.predictions
        ORDER BY created_at DESC
        LIMIT 200
    """)
    if preds.empty:
        st.info("No predictions yet. Trigger training + batch predict via the Airflow UI.")
    else:
        st.dataframe(preds, use_container_width=True, hide_index=True)

        with_actuals = preds.dropna(subset=["predicted_aqi", "actual_aqi"])
        if not with_actuals.empty:
            fig = px.scatter(
                with_actuals, x="actual_aqi", y="predicted_aqi",
                color="city_id", hover_data=["prediction_date", "model_version"],
                title="Predicted vs Actual AQI",
            )
            min_v = min(with_actuals["actual_aqi"].min(), with_actuals["predicted_aqi"].min())
            max_v = max(with_actuals["actual_aqi"].max(), with_actuals["predicted_aqi"].max())
            fig.add_shape(type="line", line=dict(dash="dash", color="gray"),
                         x0=min_v, y0=min_v, x1=max_v, y1=max_v)
            st.plotly_chart(fig, use_container_width=True)

with tab_drift:
    st.subheader("Population Stability Index (PSI) & KS Test")
    drift = _query("""
        SELECT feature_name, evaluated_at, psi, ks_statistic, ks_pvalue,
               drift_status, baseline_start, baseline_end, current_start, current_end
        FROM monitoring.drift_metrics
        WHERE evaluated_at = (SELECT MAX(evaluated_at) FROM monitoring.drift_metrics)
        ORDER BY psi DESC
        LIMIT 50
    """)
    if drift.empty:
        st.info("No drift metrics yet. Trigger the `aqi_monitoring` DAG in Airflow.")
    else:
        fig_drift = px.bar(
            drift.head(20), x="feature_name", y="psi", color="drift_status",
            color_discrete_map={"STABLE": "#2ecc71", "MODERATE": "#f39c12", "SIGNIFICANT": "#e74c3c"},
            title="Feature PSI (Moderate ≥ 0.1, Significant ≥ 0.2)",
        )
        fig_drift.add_hline(y=0.1, line_dash="dash", line_color="orange", annotation_text="Moderate")
        fig_drift.add_hline(y=0.2, line_dash="dash", line_color="red", annotation_text="Significant")
        st.plotly_chart(fig_drift, use_container_width=True)
        st.dataframe(drift, use_container_width=True, hide_index=True)

with tab_perf:
    st.subheader("Daily Model Performance")
    perf = _query("""
        SELECT date_evaluated, model_name, model_version,
               mae, rmse, r2, f1_macro, accuracy, sample_count
        FROM monitoring.model_performance_daily
        ORDER BY date_evaluated DESC
        LIMIT 90
    """)
    if perf.empty:
        st.info("No matured forecast outcomes have been evaluated yet; performance metrics appear after predictions can be matched to actual AQI.")
    else:
        col1, col2 = st.columns(2)
        with col1:
            st.plotly_chart(
                px.line(perf, x="date_evaluated", y="mae", color="model_name",
                        markers=True, title="MAE Trend"),
                use_container_width=True,
            )
        with col2:
            st.plotly_chart(
                px.line(perf, x="date_evaluated", y="r2", color="model_name",
                        markers=True, title="R² Trend"),
                use_container_width=True,
            )
        st.dataframe(perf, use_container_width=True, hide_index=True)

with tab_alerts:
    st.subheader("Monitoring Alerts")
    alerts = _query("""
        SELECT alert_id::text AS alert_id, raised_at, alert_type, severity,
               description, metric_value, threshold_value, is_resolved, resolved_at
        FROM monitoring.alerts
        ORDER BY raised_at DESC
        LIMIT 50
    """)
    if alerts.empty:
        st.success("✅ No alerts raised.")
    else:
        # Colour by severity
        def _row_color(row):
            if row.severity == "CRITICAL":
                return ["background-color: #fce4e4"] * len(row)
            elif row.severity == "WARNING":
                return ["background-color: #fff3cd"] * len(row)
            return [""] * len(row)

        st.dataframe(
            alerts.style.apply(_row_color, axis=1),
            use_container_width=True,
            hide_index=True,
        )

with tab_health:
    st.subheader("Prediction API: last 24 hours")
    health = _query("""
        SELECT COUNT(*) AS requests,
               percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95_latency_ms,
               COUNT(*) FILTER (WHERE status_code >= 500) AS server_errors
        FROM monitoring.api_request_log
        WHERE timestamp >= NOW() - INTERVAL '24 hours'
    """)
    if health.empty:
        st.info("API request health is not available yet.")
    else:
        h = health.iloc[0]
        if h.requests < 10:
            st.caption(f"Only {int(h.requests)} requests in this window; p95/error-rate indicators are not statistically meaningful yet.")
        health_cols = st.columns(3)
        health_cols[0].metric("Requests", f"{int(h.requests or 0):,}")
        health_cols[1].metric(
            "p95 latency",
            f"{h.p95_latency_ms:.1f} ms" if pd.notna(h.p95_latency_ms) else "N/A",
        )
        health_cols[2].metric("5xx errors", f"{int(h.server_errors or 0):,}")
    requests = _query("""
        SELECT timestamp, endpoint, status_code, latency_ms, error_type
        FROM monitoring.api_request_log
        ORDER BY timestamp DESC
        LIMIT 100
    """)
    st.dataframe(requests, use_container_width=True, hide_index=True)

with tab_api:
    st.subheader("🔗 Live API Inference Test")
    st.markdown(
        f"FastAPI server: `{_api_base()}`  \n"
        "Use the form below to send a live prediction request."
    )

    cities_resp = _query("SELECT city_id, city_name FROM analytics.dim_city ORDER BY city_name")
    city_options = (
        cities_resp.set_index("city_id")["city_name"].to_dict()
        if not cities_resp.empty
        else {"delhi": "Delhi", "mumbai": "Mumbai"}
    )

    col_city, col_date = st.columns(2)
    with col_city:
        selected_city = st.selectbox(
            "City", options=list(city_options.keys()),
            format_func=lambda x: city_options.get(x, x),
        )
    with col_date:
        import datetime
        selected_date = st.date_input("As-of date", value=datetime.date.today())

    if st.button("🚀 Get Prediction", type="primary"):
        import requests
        try:
            api_url = f"{_api_base()}/predict"
            payload = {"city_id": selected_city, "as_of_date": selected_date.isoformat()}
            r = requests.post(api_url, json=payload, timeout=30)
            if r.status_code == 200:
                data = r.json()
                result_cols = st.columns(3)
                predicted_aqi = data.get("predicted_aqi")
                result_cols[0].metric("Predicted AQI", f"{predicted_aqi:.1f}" if predicted_aqi is not None else "N/A")
                result_cols[1].metric("Category", data.get("predicted_category", "N/A"))
                result_cols[2].metric("Latency", f"{data.get('latency_ms', 0):.1f} ms")
                st.json(data)
            else:
                st.error(f"API Error {r.status_code}: {r.text}")
        except requests.exceptions.ConnectionError:
            st.warning(
                "⚠️ API service not reachable. Start the full stack with "
                "`make up-all` (requires Stage B profile)."
            )
        except Exception as exc:
            st.error(f"Request failed: {exc}")
