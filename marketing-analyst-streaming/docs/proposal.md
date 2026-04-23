# Portfolio Project Proposal

## Project Proposal

| | |
|---|---|
| **Name** | Alessia Berry |
| **Project Name** | Marketing Analytics Pipeline — Streaming Industry |
| **GitHub Repo** | [github.com/alessiaberry-beep/marketing-analyst-streaming](https://github.com/alessiaberry-beep/marketing-analyst-streaming) |

---

## Job Posting

| | |
|---|---|
| **Role** | Marketing Analytics Analyst |
| **Company** | Paramount+ (Paramount Streaming) |
| **Location** | West Hollywood, CA |
| **Salary** | $86,000 – $110,000 / year |
| **Link** | [indeed.com/viewjob?jk=de0d07e5d5eadda5](https://www.indeed.com/viewjob?jk=de0d07e5d5eadda5) |

---

## Reflection

This Paramount+ Marketing Analytics Analyst posting is a direct match for the skills covered in this analytics engineering course. The role explicitly requires proficient SQL for extracting, cleaning, and transforming data within databases — the exact workflow practiced with dbt staging and mart models. It also calls for building and maintaining dashboards and data pipelines, which maps directly to our Streamlit and GitHub Actions coursework. This project will simulate the kind of marketing performance analysis this role performs daily: pulling paid acquisition channel data through an API, transforming it through a dimensional star schema in Snowflake, and surfacing channel-level ROAS and conversion metrics in a deployed Streamlit dashboard. The role's emphasis on translating complex model outputs into actionable insights directly mirrors the data storytelling principles from this course. This posting is also transferable — the same project could apply to similar analyst roles at Netflix, Hulu, Disney+, or any DTC subscription business where marketing efficiency and funnel analytics are central to growth.

---

## Data Sources

### Source 1
| | |
|---|---|
| **Name** | Meta Marketing API (Facebook Ads) |
| **Type** | REST API |
| **Link** | [developers.facebook.com/docs/marketing-apis](https://developers.facebook.com/docs/marketing-apis/) |
| **Description** | Paid social campaign data including daily spend, impressions, clicks, and conversions by campaign and ad set. Extracted via Python, loaded to Snowflake raw schema, transformed through dbt staging and mart layers, and scheduled via GitHub Actions. |

### Source 2
| | |
|---|---|
| **Name** | Paramount+ Press Releases & Streaming Industry Reports |
| **Type** | Web / Document Scrape |
| **Link** | [paramount.com/press](https://www.paramount.com/press) · [variety.com/v/streaming](https://variety.com/v/streaming/) |
| **Description** | Unstructured industry content scraped from Paramount+ press releases, earnings call transcripts, and streaming trade publications (Variety, The Hollywood Reporter). Ingested into knowledge/raw/ (15+ files from 3+ sites), then synthesized into wiki pages via Claude Code and queryable during the final interview demo. |

---

## Solution Overview

This project builds a two-path analytics pipeline. The **structured data path** extracts paid marketing channel performance data from the Meta Marketing API using a Python script, loads it to Snowflake raw, and transforms it through dbt staging and mart layers into a star schema — a central fact table (`fct_channel_performance`) with supporting dimensions (`dim_channel`, `dim_date`, `dim_campaign`). A Streamlit dashboard deployed to Streamlit Community Cloud surfaces descriptive analytics (spend and conversion trends by channel) and diagnostic analytics (ROAS and CPA benchmarks to identify underperforming campaigns). Both paths run on a schedule via GitHub Actions.

The **knowledge base path** scrapes unstructured streaming industry content from Paramount+ press releases, earnings transcripts, and trade publications. Sources are stored in `knowledge/raw/`, then synthesized into wiki pages in `knowledge/wiki/` using Claude Code. The knowledge base is queryable live during the final interview via Claude Code running against the repo — no deployed chatbot required.

This project directly demonstrates the core skills from the Paramount+ posting: proficient SQL through dbt models, data pipeline development through GitHub Actions, dashboard creation through Streamlit, and the ability to translate paid channel data into actionable ROAS and acquisition insights. The design is transferable to any DTC streaming or subscription business — Netflix, Hulu, Max, or Disney+ — making it a durable portfolio asset beyond this single application.
