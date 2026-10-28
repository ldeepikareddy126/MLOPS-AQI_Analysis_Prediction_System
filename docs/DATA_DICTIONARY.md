# Data dictionary and validation rules

The authoritative column definitions are in `sql/ddl/`. This guide describes the logical model and the fields most relevant to analysis; the DDL should be consulted for exact SQL types, nullability, keys, and indexes.

## Warehouse layers

| Schema/table | Grain and purpose | Important fields |
|---|---|---|
| `raw.openaq_measurements` | One API measurement record in the relational raw layer | `run_id`, location/sensor identifiers, `parameter`, raw `value`/`unit`, UTC timestamp, source metadata |
| `raw.open_meteo_weather` | One city and weather timestamp | `run_id`, city/coordinates, timestamp, temperature, humidity, precipitation, wind, pressure and related raw weather values |
| `raw.cpcb_measurements`, `raw.kaggle_measurements` | One parsed source-file observation | `run_id`, original station/city/date/parameter/value/unit fields and source file identity |
| `staging.measurements` | One run-scoped, normalized pollutant observation before cleaning | `run_id`, `source_system`, canonical station/city/pollutant, normalized value/unit, original unit, `timestamp_utc`, `timestamp_ist`, `hour_utc`, `date_utc`, source-row reference |
| `staging.weather` | One run-scoped city/hour weather row | `run_id`, city, coordinates, UTC timestamp and normalized weather variables |
| `clean.measurements_hourly` | One winning station/pollutant/hour observation after source-priority deduplication | canonical station, city, pollutant, hour, normalized value/unit, source/run lineage |
| `clean.daily_averages` | One station/pollutant/day aggregate | daily mean (24-hour pollutants), 8-hour max (8-hour pollutants), sub-index, observation count and unit |
| `clean.station_hourly_completeness` | One station/pollutant/observed period summary | expected/present hours and period start/end; absent periods with no observations are not represented as rows |
| `analytics.dim_city`, `dim_station`, `dim_date` | City, station and date lookup dimensions | stable IDs, readable names/geography, calendar attributes |
| `analytics.fact_station_hourly` | One station/hour with pollutant and weather features | pollutant columns, weather variables, `has_weather`, pollutant count, source systems |
| `analytics.mart_station_daily_aqi` | One station/day | AQI/category, dominant pollutant, pollutant concentrations, observation hours |
| `analytics.mart_city_daily_aqi` | One city/day roll-up | average/min/max AQI, category, dominant pollutant, station count |
| `analytics.mart_pollutant_daily`, `mart_pollutant_contribution` | City/pollutant/day or city/pollutant/month summaries | daily concentration/sub-index, station counts, dominant-pollutant days and contribution percent |
| `analytics.mart_hourly_pattern`, `mart_monthly_pattern`, `mart_weather_impact`, `mart_data_quality` | Gold analytical marts | hourly/weekday patterns, monthly category counts, temperature relationship, and completeness |
| `analytics.features_daily` | One city/date/feature-version vector, including inference dates without matured labels | lag/rolling features JSON, nullable next-calendar-day AQI target/category, feature version and build time |
| `analytics.predictions` | One city/as-of-date/model/type prediction | prediction date, AQI/category predictions, actuals when available, model/version and type |

## Operational and MLOps tables

- `ops.pipeline_run`: one orchestration run, dates/configuration, status, start/finish, summary.
- `ops.source_registry`: source name, URL, access/terms, expected role, and whether it is required. OpenAQ and weather are required; CPCB and Kaggle are optional.
- `ops.ingestion_log`: per extraction/file attempt, endpoint/path, status, HTTP status, rows, duration, checksum and error.
- `ops.rejected_records`: rejected observation with source/run/layer, violated rule, explanation and original JSON.
- `ops.dq_results`: per-run check counts, failure rate, threshold and status.
- `ops.source_coverage`: source by raw/staging/clean/analytics layer with rows, dates, station count and status.
- `monitoring.prediction_log`: successful model predictions, input features, latency, model/version and optional actuals.
- `monitoring.api_request_log`: every `/predict` HTTP request's status and latency, including client/server errors; request bodies are not stored.
- `monitoring.drift_metrics`, `model_performance_daily`, `alerts`: PSI/KS results, delayed-outcome metrics and operational alerts.

## Validation and calculation rules

1. **Timestamps:** parse as UTC; retain both UTC and India-local timestamps for pollutant staging. Hourly keys use the UTC hour.
2. **Pollutants and units:** map supported aliases through the pollutant configuration and convert to canonical standard units. Unknown/non-AQI OpenAQ parameters are retained in raw and excluded from AQI pollutant staging. Do not silently reinterpret unknown units.
3. **Ranges:** configured physical pollutant bounds reject invalid/negative or implausible normalized values. Rejections carry run/source/reason; DQ results count pass/fail rows and apply the configured rejection threshold.
4. **Deduplication:** canonical station + pollutant + UTC hour; configured source priority selects the winning observation. Source tiebreak uses ingestion time.
5. **Averaging:** 24-hour pollutants use the available daily observation mean. 8-hour pollutants require eight hourly observations in the rolling window; partial windows are not treated as an 8-hour mean.
6. **AQI:** use pollutant breakpoint tables, select the highest available sub-index, require at least three valid pollutant sub-indices and at least one of PM2.5 or PM10. The result is not labeled regulator-certified; input completeness still matters.
7. **Weather join:** join hourly city weather to station-hour facts by city and UTC hour. Weather does not create pollutant observations.
8. **Missing periods:** completeness compares present distinct hours with the span between first and last observed hours. A period with zero observations is not automatically created; use source coverage/date range to identify broader gaps.
9. **Forecast features:** build a complete daily calendar per city before deriving lags/rolling values. A target is the next calendar day's AQI, never merely the next available observation; unlabeled inference dates remain in the feature store with null targets. Training filters to rows with labels and sorts globally by date (city is a deterministic tie-breaker) before chronological train/test splitting. Median imputation and scaling are fit only on the training portion.
10. **Monitoring:** API p95 latency and 5xx rate are evaluated over a configurable rolling window only after a minimum traffic count; recent source freshness and DQ results are also checked. Drift/performance metrics are only meaningful when sufficient baseline/current features or matured actual outcomes exist.

## Caveats

Coverage and observed-hour completeness are not equivalent to regulatory completeness. Current sample CPCB/Kaggle files are parser fixtures only. The warehouse is intended for educational analytics, not emergency response or regulatory reporting.
