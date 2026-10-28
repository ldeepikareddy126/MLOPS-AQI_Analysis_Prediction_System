# Data sources and access instructions

Use only public data and honor the source's current terms and rate limits. Confirm the license shown for the exact data release before redistributing any downloaded file. Store credentials in `.env`; never put them in source control.

| Source | Project use | Access |
|---|---|---|
| [OpenAQ API v3](https://api.openaq.org/v3) | Primary pollutant observations and station metadata | Create an account/API key using the [OpenAQ developer documentation](https://docs.openaq.org/). Set `OPENAQ_API_KEY` in `.env`. The collector saves response pages to raw landing, logs request/status/count, retries transient failures, and resumes pagination from checkpoints. Check current API quota/terms in the official docs. |
| [Open-Meteo Archive API](https://open-meteo.com/en/docs/historical-weather-api) | Hourly weather enrichment by configured city coordinates | No API key for the free non-commercial service. Use the [terms](https://open-meteo.com/en/terms) and attribution guidance. The collector writes hourly temperature, humidity, precipitation, wind, pressure, cloud and visibility fields to raw landing. |
| [CPCB CAAQMS](https://cpcb.nic.in/CAAQMS/) | Optional official India-specific historical observations | Download from the CPCB portal using its supported process and check any portal-specific conditions. Place a CSV/XLSX file in `data/external/cpcb/`. `data/external/cpcb/delhi_ito_sample.csv` contains only three demonstration rows and is not a historical extract. |
| [Kaggle India Air Quality dataset](https://www.kaggle.com/datasets/rohanrao/air-quality-data-in-india) | Optional historical fallback / prototyping | Obtain the exact release via the dataset page/Kaggle API or an instructor-approved public download. Check the license on that exact release and record its version/date. Configure `KAGGLE_USERNAME`/`KAGGLE_KEY` only when using the API, or place authorized CSVs in `data/external/kaggle/`. The checked-in `city_day.csv` contains only three city-day rows; the wide pollutant columns expand to 21 measurement records in the loader. It is a parser fixture, not the full historical dataset. |

## Provenance and reproducibility

Each pipeline run has a UUID. The operational tables record source, run, endpoint/file, extraction timestamp, status, row count, HTTP status where applicable, duration, and error details. Raw API payloads are also written below the configured raw landing root with run/date paths and SHA-256 checksums. The relational raw tables are useful for replay, but their natural-key upserts can represent the latest extraction rather than a permanent per-run history; the immutable raw files are the retained extraction copy.

The OpenAQ raw layer includes parameters beyond the AQI transformation's canonical pollutants (for example, relative humidity, wind and temperature). These are retained in the raw source response but excluded from pollutant staging unless mapped by the configured pollutant registry. In the verified 2026-10-15 run, 3,250,417 raw measurements minus 2,005,721 staged pollutant rows equals 1,244,696 out-of-scope rows. This is expected filtering, not unexplained loss; see [execution evidence](EXECUTION_EVIDENCE.md).

## How to refresh the sample files

Replace the samples only with a legally obtained file. Keep the original source name, release/version, download date, license/terms URL, and checksum alongside the file or in the run report. Do not manually edit the warehouse's clean or analytics tables. These sample files are useful for parser smoke tests but must not be described as representative historical datasets.
