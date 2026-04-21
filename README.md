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

## Tech Stack

| LSource | Synthetic Meta Ads API (FastAPI) |
| Orchestration | GitHub Actions |
| Data Warehouse | Snowflake |
| Transformation | dbt |
| Testing | dbt tests (18 passing) |
| Dashboard | Streamlit (Milestone 02) |

---

## Project Structure
