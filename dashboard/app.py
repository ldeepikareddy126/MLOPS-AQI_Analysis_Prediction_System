"""Streamlit dashboard for Stage A AQI pipeline outputs."""

from __future__ import annotations

import os

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text


st.set_page_config(page_title="AQI Pipeline Dashboard", page_icon="AQI", layout="wide")


@st.cache_resource
def engine():
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    user = os.environ.get("POSTGRES_USER", "aqi_user")
    password = os.environ.get("POSTGRES_PASSWORD", "")
    db = os.environ.get("AQI_DB_NAME", "aqi_warehouse")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}", pool_pre_ping=True)


@st.cache_data(ttl=60)
def query(sql: str, params: dict[str, str] | None = None) -> pd.DataFrame:
    try:
        return pd.read_sql(text(sql), engine(), params=params or {})
    except Exception as exc:
        st.warning(f"Database query failed: {exc}")
        return pd.DataFrame()


st.title("AQI Analysis & Pipeline Health")
st.caption(
    "Explore observed air quality, the data pipeline that prepares it, and the "
    "MLOps tools used to make and monitor next-day predictions."
)

page = st.sidebar.radio(
    "View",
    [
        "Overview",
        "AQI Trends",
        "City Comparison",
        "Pollutants",
        "Patterns & Contributions",
        "Weather Impact",
        "Source Coverage",
        "MLOps & Predictions",
    ],
)
st.sidebar.caption(
    "Start with **Overview** for a project guide. Use **AQI Trends**, "
    "**City Comparison**, **Pollutants**, and **Patterns & Contributions** to explore observations; "
    "**Source Coverage** and **MLOps & Predictions** explain system health."
)

if page == "Overview":
    st.subheader("Welcome — what this project does")
    st.markdown(
        "This project collects public air-quality and weather observations, "
        "checks and standardizes them, stores them in PostgreSQL, and presents "
        "city/station trends. Its MLOps extension builds next-day AQI predictions "
        "and monitors model inputs and service health."
    )

    st.subheader("How data moves through the project")
    flow = st.columns(4)
    flow_steps = [
        ("Sources → ingest", "OpenAQ and Open-Meteo, with optional CPCB/Kaggle files collected by Python."),
        ("Raw → transform", "Original payloads are retained; Airflow validates, normalizes, and deduplicates observations."),
        ("Warehouse → explore", "PostgreSQL stores clean facts and analytics marts for this Streamlit dashboard."),
        ("Features → monitor", "MLflow tracks models; FastAPI serves forecasts while monitoring checks drift and service health."),
    ]
    for column, (heading, detail) in zip(flow, flow_steps):
        with column:
            st.markdown(f"**{heading}**")
            st.caption(detail)

    latest = query(
        """
        SELECT
          (SELECT COUNT(*) FROM analytics.fact_station_hourly) AS hourly_facts,
          (SELECT COUNT(*) FROM analytics.mart_station_daily_aqi) AS station_days,
          (SELECT COUNT(*) FROM ops.ingestion_log) AS ingestion_events,
          (SELECT COUNT(*) FROM ops.rejected_records) AS rejected_records
        """
    )
    cols = st.columns(4)
    if not latest.empty:
        row = latest.iloc[0]
        cols[0].metric("Hourly fact rows", f"{int(row.hourly_facts):,}", help="All hourly station records currently in the analytics fact table.")
        cols[1].metric("Station-days", f"{int(row.station_days):,}", help="Daily AQI summaries by monitoring station.")
        cols[2].metric("Ingestion attempts", f"{int(row.ingestion_events):,}", help="All recorded source extraction attempts, including retries and failures.")
        cols[3].metric("Rejected observations", f"{int(row.rejected_records):,}", help="Rows withheld from cleaned analytics by validation rules.")
    st.caption("These are current warehouse totals, not counts from only the most recent run.")

    with st.expander("How to interpret AQI and its categories", expanded=False):
        st.markdown(
            "A higher AQI indicates worse air quality. This project calculates an "
            "educational AQI from available pollutant measurements; a daily value "
            "is shown only when the required pollutant coverage is sufficient. "
            "It is not a regulator-certified reading or health advisory."
        )
        aqi_scale = pd.DataFrame(
            [
                {"Category": "Good", "Low": 0, "Width": 50, "Range": "0–50", "Color": "#00a65a"},
                {"Category": "Satisfactory", "Low": 51, "Width": 50, "Range": "51–100", "Color": "#7cb342"},
                {"Category": "Moderate", "Low": 101, "Width": 100, "Range": "101–200", "Color": "#fbc02d"},
                {"Category": "Poor", "Low": 201, "Width": 100, "Range": "201–300", "Color": "#ef6c00"},
                {"Category": "Very Poor", "Low": 301, "Width": 100, "Range": "301–400", "Color": "#d84343"},
                {"Category": "Severe", "Low": 401, "Width": 100, "Range": "401–500", "Color": "#7e0023"},
            ]
        )
        scale = go.Figure()
        for _, band in aqi_scale.iterrows():
            scale.add_trace(
                go.Bar(
                    x=[band["Width"]],
                    y=[band["Category"]],
                    base=[band["Low"]],
                    orientation="h",
                    marker_color=band["Color"],
                    text=[band["Range"]],
                    textposition="inside",
                    hovertemplate=f"{band['Category']}: AQI {band['Range']}<extra></extra>",
                    showlegend=False,
                )
            )
        scale.update_layout(
            title="AQI category scale used in this project",
            xaxis=dict(title="AQI value", range=[0, 500], dtick=50),
            yaxis=dict(title=None, categoryorder="array", categoryarray=aqi_scale["Category"].tolist()),
            barmode="overlay",
            height=300,
            margin=dict(l=10, r=10, t=55, b=35),
        )
        st.plotly_chart(scale, use_container_width=True)

    trend = query(
        """
        SELECT c.city_name, m.date_utc, m.aqi_mean, m.aqi_category
        FROM analytics.mart_city_daily_aqi m
        JOIN analytics.dim_city c ON c.city_id = m.city_id
        WHERE m.date_utc >= (
            SELECT MAX(date_utc) - INTERVAL '30 days'
            FROM analytics.mart_city_daily_aqi
        )
        ORDER BY m.date_utc
        """
    )
    categories = query(
        """
        SELECT aqi_category, COUNT(*) AS city_days
        FROM analytics.mart_city_daily_aqi
        WHERE date_utc = (SELECT MAX(date_utc) FROM analytics.mart_city_daily_aqi)
          AND aqi_category IS NOT NULL
        GROUP BY aqi_category
        ORDER BY city_days DESC
        """
    )
    st.subheader("Recent air-quality snapshot")
    if trend.empty:
        st.info("No city-level AQI data is available yet. Run the ETL DAG after data sources are configured.")
    else:
        chart_cols = st.columns([2, 1])
        with chart_cols[0]:
            st.plotly_chart(
                px.line(
                    trend,
                    x="date_utc",
                    y="aqi_mean",
                    color="city_name",
                    markers=True,
                    title="Daily mean AQI by city · latest 30 days",
                    labels={"date_utc": "Date", "aqi_mean": "Mean AQI", "city_name": "City"},
                ),
                use_container_width=True,
            )
        with chart_cols[1]:
            if categories.empty:
                st.info("No AQI categories for the latest available date.")
            else:
                st.plotly_chart(
                    px.pie(
                        categories,
                        names="aqi_category",
                        values="city_days",
                        hole=0.45,
                        title="City category mix · latest date",
                    ),
                    use_container_width=True,
                )
        st.caption(
            "The trend uses available city-day averages. The category chart counts "
            "cities with data on the latest date in the warehouse; it is not a "
            "population-weighted or station-weighted measure."
        )

    runs = query(
        """
        SELECT run_id::text AS run_id, started_at, finished_at, status, summary_json
        FROM ops.pipeline_run
        ORDER BY started_at DESC
        LIMIT 10
        """
    )
    st.subheader("Recent Pipeline Runs")
    st.caption("Each run is a separate processing attempt. Historical failures remain visible for troubleshooting and audit.")
    st.dataframe(runs, use_container_width=True, hide_index=True)

elif page == "AQI Trends":
    st.caption(
        "View city-level daily AQI or drill into station-level daily AQI. The city "
        "value summarizes available stations; gaps can reflect missing source data "
        "rather than clean air."
    )
    trend_level = st.radio("Trend level", ["City", "Station"], horizontal=True)
    if trend_level == "City":
        df = query(
            """
            SELECT c.city_name, m.date_utc, m.aqi_mean, m.aqi_max, m.aqi_category
            FROM analytics.mart_city_daily_aqi m
            JOIN analytics.dim_city c ON c.city_id = m.city_id
            WHERE m.date_utc >= (
                SELECT MAX(date_utc) - INTERVAL '30 days'
                FROM analytics.mart_city_daily_aqi
            )
            ORDER BY m.date_utc
            """
        )
        series_column = "city_name"
        measure_column = "aqi_mean"
        chart_title = "City Daily AQI Mean (latest 30 days)"
        measure_label = "Mean AQI"
    else:
        stations = query("SELECT city_id, city_name FROM analytics.dim_city ORDER BY city_name")
        if stations.empty:
            df = pd.DataFrame()
        else:
            city_options = stations.set_index("city_id")["city_name"].to_dict()
            selected_city = st.selectbox(
                "City",
                options=list(city_options),
                format_func=lambda value: city_options[value],
            )
            df = query(
                """
                SELECT s.station_name AS station_name, m.date_utc,
                       m.aqi, m.aqi_category
                FROM analytics.mart_station_daily_aqi m
                JOIN analytics.dim_station s
                  ON s.station_id_canonical = m.station_id_canonical
                WHERE m.city_id = :city_id
                  AND m.aqi IS NOT NULL
                  AND m.date_utc >= (
                      SELECT MAX(date_utc) - INTERVAL '30 days'
                      FROM analytics.mart_station_daily_aqi
                      WHERE city_id = :city_id
                  )
                ORDER BY m.date_utc, station_name
                """,
                {"city_id": selected_city},
            )
        series_column = "station_name"
        measure_column = "aqi"
        chart_title = f"Station Daily AQI (latest 30 days)"
        measure_label = "AQI"
    if df.empty:
        st.info("No AQI mart rows yet. Run the Stage A pipeline after providing pollutant data.")
    else:
        st.caption(f"Showing the latest 30 days of available data ({df.date_utc.min()} to {df.date_utc.max()}).")
        st.plotly_chart(
            px.line(
                df,
                x="date_utc",
                y=measure_column,
                color=series_column,
                markers=True,
                title=chart_title,
                labels={
                    "date_utc": "Date",
                    measure_column: measure_label,
                    series_column: "City" if trend_level == "City" else "Station",
                },
            ),
            use_container_width=True,
        )
        st.dataframe(df.tail(100), use_container_width=True, hide_index=True)

elif page == "City Comparison":
    st.caption(
        "Compare the same latest 30-day calendar window across cities. The bars "
        "show each city's average daily AQI; this is not a population-weighted comparison."
    )
    df = query(
        """
        SELECT c.city_name, AVG(m.aqi_mean) AS avg_aqi, MAX(m.aqi_max) AS worst_aqi,
               COUNT(*) AS days
        FROM analytics.mart_city_daily_aqi m
        JOIN analytics.dim_city c ON c.city_id = m.city_id
        WHERE m.date_utc >= (
            SELECT MAX(date_utc) - INTERVAL '30 days'
            FROM analytics.mart_city_daily_aqi
        )
        GROUP BY c.city_name
        ORDER BY avg_aqi DESC
        """
    )
    if df.empty:
        st.info("No city AQI data yet.")
    else:
        st.caption("Comparison uses the same latest 30-day calendar window for every city; days counts available city-day observations.")
        st.plotly_chart(px.bar(df, x="city_name", y="avg_aqi", color="worst_aqi", title="Average AQI by City (latest 30 days)"), use_container_width=True)
        st.dataframe(df, use_container_width=True, hide_index=True)

elif page == "Pollutants":
    st.caption(
        "Inspect pollutant concentrations by city and day. 24-hour pollutants use "
        "their daily mean; CO and ozone use the required rolling 8-hour maximum. "
        "Concentrations are not AQI values."
    )
    df = query(
        """
        SELECT c.city_name, p.pollutant_code, p.date_utc,
               AVG(
                   CASE
                       WHEN p.pollutant_code IN ('CO', 'O3')
                           THEN NULLIF(p.eight_hour_max::text, 'NaN')::double precision
                       ELSE NULLIF(p.daily_mean::text, 'NaN')::double precision
                   END
               ) AS daily_mean,
               MIN(p.unit) AS unit,
               AVG(NULLIF(p.sub_index::text, 'NaN')::double precision) AS sub_index_mean
        FROM clean.daily_averages p
        JOIN analytics.dim_city c ON c.city_id = p.city_id
        WHERE p.date_utc >= (
            SELECT MAX(date_utc) - INTERVAL '30 days'
            FROM clean.daily_averages
        )
        GROUP BY c.city_name, p.pollutant_code, p.date_utc
        ORDER BY p.date_utc, c.city_name
        """
    )
    if df.empty:
        st.info("No pollutant mart rows yet.")
    else:
        st.caption(f"Showing the latest 30 days of available data ({df.date_utc.min()} to {df.date_utc.max()}).")
        pollutant = st.selectbox("Pollutant", sorted(df.pollutant_code.dropna().unique()))
        view = df[(df.pollutant_code == pollutant) & df.daily_mean.notna()]
        if view.empty:
            st.info(f"No valid {pollutant} concentration values are available in this date window.")
        else:
            unit = view.unit.dropna().iloc[0] if view.unit.notna().any() else "source standard unit"
            statistic = "8-hour maximum" if pollutant in {"CO", "O3"} else "daily mean"
            st.plotly_chart(
                px.line(
                    view,
                    x="date_utc",
                    y="daily_mean",
                    color="city_name",
                    markers=True,
                    title=f"{pollutant} {statistic} by city ({unit})",
                    labels={"date_utc": "Date", "daily_mean": f"{statistic.title()} ({unit})", "city_name": "City"},
                ),
                use_container_width=True,
            )
        st.dataframe(view.tail(200), use_container_width=True, hide_index=True)

elif page == "Weather Impact":
    st.caption(
        "This view shows statistical association between weather and AQI, not "
        "proof that a weather variable caused a change in pollution."
    )
    df = query(
        """
        SELECT c.city_name, weather_variable, bin_label, aqi_mean, pearson_r, obs_count
        FROM analytics.mart_weather_impact w
        JOIN analytics.dim_city c ON c.city_id = w.city_id
        ORDER BY c.city_name
        """
    )
    if df.empty:
        st.info("No weather impact rows yet.")
    else:
        weather_variable = st.selectbox("Weather variable", sorted(df.weather_variable.dropna().unique()))
        weather_view = df[df.weather_variable == weather_variable]
        st.caption("Pearson correlation is descriptive, not causal; each city value uses its available weather/AQI observations.")
        st.plotly_chart(
            px.bar(
                weather_view,
                x="city_name",
                y="pearson_r",
                color="pearson_r",
                hover_data=["obs_count", "aqi_mean"],
                title=f"{weather_variable.title()}–AQI Correlation by City (Pearson r)",
                color_continuous_scale="RdBu",
                range_color=[-1, 1],
            ),
            use_container_width=True,
        )
        st.dataframe(weather_view, use_container_width=True, hide_index=True)

elif page == "Patterns & Contributions":
    st.caption(
        "Explore typical AQI by hour and weekday, monthly seasonality, and which "
        "pollutant most often had the highest AQI sub-index. Dominant-pollutant "
        "share counts days; it is not a source-emissions inventory."
    )
    cities = query("SELECT city_id, city_name FROM analytics.dim_city ORDER BY city_name")
    if cities.empty:
        st.info("City dimensions are not available yet.")
    else:
        city_names = cities.set_index("city_id")["city_name"].to_dict()
        city_id = st.selectbox(
            "City",
            options=list(city_names),
            format_func=lambda value: city_names[value],
        )

        hourly = query(
            """
            SELECT city_id, hour_of_day,
                   CASE day_of_week
                       WHEN 0 THEN 'Sun'
                       WHEN 1 THEN 'Mon'
                       WHEN 2 THEN 'Tue'
                       WHEN 3 THEN 'Wed'
                       WHEN 4 THEN 'Thu'
                       WHEN 5 THEN 'Fri'
                       WHEN 6 THEN 'Sat'
                   END AS day_name,
                   aqi_mean, pm25_mean, obs_count
            FROM analytics.mart_hourly_pattern
            ORDER BY day_of_week, hour_of_day
            """
        )
        monthly = query(
            """
            SELECT city_id, make_date(year::integer, month::integer, 1) AS month_date,
                   aqi_mean, aqi_p75, pm25_mean, pm10_mean, total_days
            FROM analytics.mart_monthly_pattern
            ORDER BY year, month
            """
        )
        contribution = query(
            """
            SELECT city_id, make_date(year::integer, month::integer, 1) AS month_date,
                   pollutant_code, dominant_days, total_days, contribution_pct
            FROM analytics.mart_pollutant_contribution
            ORDER BY year, month, pollutant_code
            """
        )

        city_hourly = hourly[hourly.city_id == city_id] if not hourly.empty else hourly
        city_monthly = monthly[monthly.city_id == city_id] if not monthly.empty else monthly
        city_contribution = (
            contribution[contribution.city_id == city_id]
            if not contribution.empty
            else contribution
        )

        chart_cols = st.columns(2)
        with chart_cols[0]:
            st.subheader("Hourly and weekday pattern")
            if city_hourly.empty or city_hourly.aqi_mean.notna().sum() == 0:
                st.info("No hourly AQI pattern is available for this city.")
            else:
                day_order = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
                st.plotly_chart(
                    px.density_heatmap(
                        city_hourly,
                        x="hour_of_day",
                        y="day_name",
                        z="pm25_mean",
                        histfunc="avg",
                        category_orders={"day_name": day_order},
                        color_continuous_scale="YlOrRd",
                        title="PM2.5 concentration by hour and weekday",
                        labels={
                            "hour_of_day": "Hour (UTC)",
                            "day_name": "Day of week",
                            "pm25_mean": "Mean PM2.5 (µg/m³)",
                        },
                    ),
                    use_container_width=True,
                )
                st.caption(
                    "The heatmap uses hourly PM2.5 measurements, not hourly AQI. The "
                    "warehouse bins hours in UTC; coverage varies by city and time."
                )

        with chart_cols[1]:
            st.subheader("Monthly AQI pattern")
            if city_monthly.empty or city_monthly.aqi_mean.notna().sum() == 0:
                st.info("No monthly AQI pattern is available for this city.")
            else:
                monthly_long = city_monthly.melt(
                    id_vars=["month_date"],
                    value_vars=["aqi_mean", "aqi_p75"],
                    var_name="statistic",
                    value_name="aqi",
                )
                monthly_long["statistic"] = monthly_long["statistic"].map(
                    {"aqi_mean": "Mean AQI", "aqi_p75": "75th percentile AQI"}
                )
                st.plotly_chart(
                    px.line(
                        monthly_long,
                        x="month_date",
                        y="aqi",
                        color="statistic",
                        markers=True,
                        title="Monthly mean and 75th-percentile AQI",
                        labels={"month_date": "Month", "aqi": "AQI", "statistic": "Measure"},
                    ),
                    use_container_width=True,
                )
                st.caption(
                    "The 75th percentile shows higher-AQI days, while the mean "
                    "summarizes the available daily observations."
                )

        st.subheader("Most frequent dominant pollutant by month")
        if city_contribution.empty:
            st.info("No dominant-pollutant contribution data is available for this city.")
        else:
            st.plotly_chart(
                px.area(
                    city_contribution,
                    x="month_date",
                    y="contribution_pct",
                    color="pollutant_code",
                    groupnorm="percent",
                    title="Share of valid station-days by dominant pollutant",
                    labels={
                        "month_date": "Month",
                        "contribution_pct": "Share of dominant days (%)",
                        "pollutant_code": "Dominant pollutant",
                    },
                    hover_data=["dominant_days", "total_days"],
                ),
                use_container_width=True,
            )
            st.caption(
                "A pollutant is dominant when it has the highest valid AQI sub-index "
                "for that station-day. This chart describes AQI dominance, not the "
                "percentage of pollution emitted by each source."
            )

elif page == "Source Coverage":
    st.caption(
        "Coverage follows records across raw, staging, clean, and analytics layers "
        "for the latest pipeline run. A skipped optional source is not necessarily a failure."
    )
    df = query(
        """
        SELECT source_id, layer, row_count, station_count, date_min, date_max, status, checked_at
        FROM ops.source_coverage
        WHERE run_id = (
            SELECT run_id FROM ops.pipeline_run ORDER BY started_at DESC LIMIT 1
        )
        ORDER BY checked_at DESC, source_id, layer
        """
    )
    latest_logs = query(
        """
        SELECT source_id, status, rows_extracted, endpoint_or_file, error_message, extraction_ts
        FROM ops.ingestion_log
        ORDER BY extraction_ts DESC
        LIMIT 50
        """
    )
    st.subheader("Coverage Matrix")
    st.dataframe(df, use_container_width=True, hide_index=True)
    st.subheader("Recent Ingestion Log")
    st.dataframe(latest_logs, use_container_width=True, hide_index=True)

elif page == "MLOps & Predictions":
    st.header("MLOps Lifecycle, Predictions & Drift Monitoring")
    st.caption(
        "The model predicts next-calendar-day AQI from historical features. "
        "Drift signals changes in model inputs; it does not by itself prove that "
        "prediction quality has worsened. Performance is evaluated after actual "
        "outcomes become available."
    )

    m_summary = query(
        """
        SELECT
          (SELECT COUNT(*) FROM monitoring.prediction_log) AS total_preds,
          (SELECT COUNT(*) FROM monitoring.alerts WHERE NOT is_resolved) AS active_alerts,
          (SELECT COUNT(*) FROM monitoring.drift_metrics
           WHERE evaluated_at = (SELECT MAX(evaluated_at) FROM monitoring.drift_metrics)
             AND drift_status = 'SIGNIFICANT') AS drifted_features,
          (SELECT COUNT(*) FROM analytics.features_daily) AS stored_feature_days
        """
    )
    cols = st.columns(4)
    if not m_summary.empty:
        s = m_summary.iloc[0]
        cols[0].metric("Successful API Predictions", f"{int(s.total_preds or 0):,}")
        cols[1].metric("Active Alerts", f"{int(s.active_alerts or 0):,}")
        cols[2].metric("Significant Drifts", f"{int(s.drifted_features or 0):,}")
        cols[3].metric("Feature Vectors", f"{int(s.stored_feature_days or 0):,}")

    tab_pred, tab_drift, tab_perf, tab_alerts, tab_health = st.tabs(
        ["Predictions", "Feature Drift (PSI)", "Model Performance", "Alerts", "API Health"]
    )

    with tab_pred:
        st.subheader("Recent Predictions & Ground Truth")
        preds = query(
            """
            SELECT city_id, as_of_date, prediction_date, predicted_aqi,
                   actual_aqi, predicted_category, actual_category,
                   model_name, model_version, prediction_type, created_at
            FROM analytics.predictions
            ORDER BY created_at DESC
            LIMIT 100
            """
        )
        if preds.empty:
            st.info("No predictions recorded yet. Run inference via API or Stage B pipeline.")
        else:
            st.dataframe(preds, use_container_width=True, hide_index=True)

            with_actuals = preds.dropna(subset=["predicted_aqi", "actual_aqi"])
            if not with_actuals.empty:
                st.subheader("Predicted vs Actual AQI")
                fig = px.scatter(
                    with_actuals,
                    x="actual_aqi",
                    y="predicted_aqi",
                    color="city_id",
                    hover_data=["prediction_date", "model_version"],
                    title="Predicted vs Actual AQI (Diagonal = Perfect Prediction)",
                )
                min_val = min(with_actuals["actual_aqi"].min(), with_actuals["predicted_aqi"].min())
                max_val = max(with_actuals["actual_aqi"].max(), with_actuals["predicted_aqi"].max())
                fig.add_shape(
                    type="line", line=dict(dash="dash", color="gray"),
                    x0=min_val, y0=min_val, x1=max_val, y1=max_val,
                )
                st.plotly_chart(fig, use_container_width=True)

    with tab_drift:
        st.subheader("Population Stability Index (PSI) & KS Test")
        drift = query(
            """
            SELECT feature_name, evaluated_at, psi, ks_statistic, ks_pvalue,
                   drift_status, baseline_start, baseline_end, current_start, current_end
            FROM monitoring.drift_metrics
            WHERE evaluated_at = (SELECT MAX(evaluated_at) FROM monitoring.drift_metrics)
            ORDER BY psi DESC
            LIMIT 50
            """
        )
        if drift.empty:
            st.info("No drift metrics evaluated yet. Trigger the aqi_monitoring DAG.")
        else:
            fig_drift = px.bar(
                drift.head(20),
                x="feature_name",
                y="psi",
                color="drift_status",
                color_discrete_map={"STABLE": "#2ecc71", "MODERATE": "#f39c12", "SIGNIFICANT": "#e74c3c"},
                title="Feature PSI (Thresholds: 0.1 Moderate, 0.2 Significant)",
            )
            fig_drift.add_hline(y=0.1, line_dash="dash", line_color="orange", annotation_text="Moderate (0.1)")
            fig_drift.add_hline(y=0.2, line_dash="dash", line_color="red", annotation_text="Significant (0.2)")
            st.plotly_chart(fig_drift, use_container_width=True)
            st.dataframe(drift, use_container_width=True, hide_index=True)

    with tab_perf:
        st.subheader("Daily Model Performance Tracking")
        perf = query(
            """
            SELECT date_evaluated, model_name, model_version,
                   mae, rmse, r2, f1_macro, accuracy, sample_count
            FROM monitoring.model_performance_daily
            ORDER BY date_evaluated DESC
            LIMIT 60
            """
        )
        if perf.empty:
            st.info("No matured forecast outcomes have been evaluated yet; performance metrics will appear after predictions can be matched to actual AQI.")
        else:
            fig_mae = px.line(
                perf,
                x="date_evaluated",
                y="mae",
                color="model_name",
                markers=True,
                title="Mean Absolute Error (MAE) Trend",
            )
            st.plotly_chart(fig_mae, use_container_width=True)
            st.dataframe(perf, use_container_width=True, hide_index=True)

    with tab_alerts:
        st.subheader("Monitoring Alerts")
        alerts = query(
            """
            SELECT alert_id::text AS alert_id, raised_at, alert_type, severity,
                   description, metric_value, threshold_value, is_resolved, resolved_at
            FROM monitoring.alerts
            ORDER BY raised_at DESC
            LIMIT 50
            """
        )
        if alerts.empty:
            st.success("No active or historical alerts.")
        else:
            st.dataframe(alerts, use_container_width=True, hide_index=True)

    with tab_health:
        st.subheader("Prediction API: last 24 hours")
        api_health = query(
            """
            SELECT COUNT(*) AS requests,
                   percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95_latency_ms,
                   COUNT(*) FILTER (WHERE status_code >= 500) AS server_errors
            FROM monitoring.api_request_log
            WHERE timestamp >= NOW() - INTERVAL '24 hours'
            """
        )
        if api_health.empty:
            st.info("API request health is not available yet.")
        else:
            row = api_health.iloc[0]
            if row.requests < 10:
                st.caption(f"Only {int(row.requests)} requests in this window; p95/error-rate indicators are not statistically meaningful yet.")
            health_cols = st.columns(3)
            health_cols[0].metric("Requests", f"{int(row.requests or 0):,}")
            health_cols[1].metric(
                "p95 latency",
                f"{row.p95_latency_ms:.1f} ms" if pd.notna(row.p95_latency_ms) else "N/A",
            )
            health_cols[2].metric("5xx errors", f"{int(row.server_errors or 0):,}")
        request_log = query(
            """
            SELECT timestamp, endpoint, status_code, latency_ms, error_type
            FROM monitoring.api_request_log
            ORDER BY timestamp DESC
            LIMIT 100
            """
        )
        st.dataframe(request_log, use_container_width=True, hide_index=True)
