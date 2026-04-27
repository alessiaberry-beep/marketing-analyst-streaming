# Marketing Analyst Streaming Pipeline
**LMU ISBA 4715 | Alessia Berry**

A end-to-end analytics engineering pipeline that extracts synthetic paid social data modeled after the Meta Ads API, loads it to Snowflake, and transforms it with dbt into a star schema ready for dashboarding.

---

## Pipeline Architecture

```mermaid
flowchart TD
    A[🟦 Synthetic Meta Ads API\nFastAPI · localhost:8000\n5 campaigns · 15 ads · 2 platforms]
    --> B[⚙️ extract_load.py\nPython pipeline\nGitHub Actions · daily 8AM UTC]
    B --> C[❄️ Snowflake RAW\nMARKETING_ANALYTICS.RAW\nraw_meta_ads_insights\n2700 rows · 90 days]
    C --> D[🔁 dbt Staging\nstg_meta_ads_insights\nTyped · Cleaned · Renamed]
    D --> E1[📊 fct_channel_performance\n2700 rows]
    D --> E2[📋 dim_campaign\n15 rows]
    D --> E3[📅 dim_date\n90 rows]
    D --> E4[🎯 dim_ad\n30 rows]
    E1 --> F[📈 Streamlit Dashboard\nROAS · CPA · Funnel Analysis]
    E2 --> F
    E3 --> F
    E4 --> F
```

---

## Entity Relationship Diagram (ERD)

```mermaid
erDiagram
    FCT_CHANNEL_PERFORMANCE {
        varchar performance_id PK
        varchar ad_id FK
        varchar adset_id FK
        varchar campaign_id FK
        date report_date FK
        varchar publisher_platform
        varchar objective
        float spend
        integer impressions
        integer reach
        float frequency
        integer clicks
        float cpm
        float cpc
        float ctr
        integer purchases
        float purchase_value
        float roas
    }

    DIM_CAMPAIGN {
        varchar campaign_id PK
        varchar campaign_name
        varchar objective
        varchar adset_id
        varchar adset_name
        varchar optimization_goal
        varchar ad_id FK
        varchar ad_name
    }

    DIM_AD {
        varchar ad_id PK
        varchar ad_name
        varchar adset_id
        varchar adset_name
        varchar campaign_id
        varchar campaign_name
        varchar publisher_platform
        varchar objective
    }

    DIM_DATE {
        date report_date PK
        integer year
        integer month
        integer day
        varchar day_name
        varchar month_name
        integer quarter
        boolean is_weekend
        date week_start_date
        date month_start_date
    }

    STG_PRESS_RELEASES {
        varchar press_release_id PK
        varchar title
        date published_date
        varchar url
        varchar summary
        text full_text
        timestamp scraped_at
    }

    FCT_CHANNEL_PERFORMANCE }o--|| DIM_CAMPAIGN : "ad_id"
    FCT_CHANNEL_PERFORMANCE }o--|| DIM_AD : "ad_id"
    FCT_CHANNEL_PERFORMANCE }o--|| DIM_DATE : "report_date"
```

---

## Tech Stack

| LSource | Synthetic Meta Ads API (FastAPI) |
| Orchestration | GitHub Actions |
| Data Warehouse | Snowflake |
| Transformation | dbt |
| Testing | dbt tests (18 passing) |
| Dashboard | Streamlit (Milestone 02) |

---

## Project Structure

## How to Run Locally

### 1. Start the API
```bash
cd api
pip3 install -r requirements-api.txt
uvicorn main:app --reload --port 8000
```

### 2. Run the pipeline
```bash
pip3 install -r requirements-pipeline.txt
export $(cat .env | xargs)
python3 extract_load.py

# Full 90-day backfill
python3 extract_load.py backfill
```

### 3. Run dbt
```bash
dbt deps
dbt run
dbt test
```

---

## Data Source

Synthetic data modeled after the real [Meta Ads Insights API](https://developers.facebook.com/docs/marketing-api/insights/), including:
- String-typed numeric metrics (matching real API behavior)
- Nested `actions`, `action_values`, `cost_per_action_type` arrays
- Cursor-based pagination
- `publisher_platform` breakdown (facebook / instagram)
- 5 Paramount+ campaigns across 15 ads over 90 days
