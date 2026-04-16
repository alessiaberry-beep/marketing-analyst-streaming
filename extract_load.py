"""
extract_load.py — Extract from local Synthetic Meta Ads API → Load to Snowflake RAW

Run locally:
    export $(cat .env | xargs)
    python extract_load.py

Required environment variables:
    API_BASE_URL        — http://localhost:8000
    SNOWFLAKE_ACCOUNT   — e.g. abc12345.us-east-1
    SNOWFLAKE_USER
    SNOWFLAKE_PASSWORD
    SNOWFLAKE_DATABASE
    SNOWFLAKE_WAREHOUSE
    SNOWFLAKE_SCHEMA    — RAW
"""

import os
import sys
import logging
from datetime import date, timedelta

import requests
import snowflake.connector

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

API_BASE_URL       = os.environ.get("API_BASE_URL", "http://localhost:8000")
ACCOUNT_ID         = "act_1234567890"
SNOWFLAKE_ACCOUNT  = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_USER     = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD = os.environ["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_DATABASE = os.environ["SNOWFLAKE_DATABASE"]
SNOWFLAKE_SCHEMA   = os.t("SNOWFLAKE_SCHEMA", "RAW")
SNOWFLAKE_WAREHOUSE= os.environ["SNOWFLAKE_WAREHOUSE"]

REPORT_DATE = date.today() - timedelta(days=1)

DDL = f"""
CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.RAW_META_ADS_INSIGHTS (
    account_id          VARCHAR(30),
    account_name        VARCHAR(200),
    campaign_id         VARCHAR(30),
    campaign_name       VARCHAR(200),
    objective           VARCHAR(50),
    adset_id            VARCHAR(30),
    adset_name          VARCHAR(200),
    optimization_goal   VARCHAR(50),
    ad_id               VARCHAR(30),
    ad_name             VARCHAR(200),
    publisher_platform  VARCHAR(20),
    date_start          DATE,
    date_stop           DATE,
    impressions         INTEGER,
    reach               INTEGER,
    frequency           FLOAT,
    spend               FLOAT,
    clicks              INTEGER,
    unique_clicks       INTEGER,
    cpm                 FLOAT,
    cpc                 FLOAT,
    cpp                 FLOAT,
    ctr                 FLOAT,
    link_clicks                 INTEGER,
    landing_page_views          INTEGER,
    initiate_checkouts          INTEGER,
    purchases                   INTEGER,
    purchase_value              FLOAT,
    cost_per_link_click         FLOAT,
    cost_per_landing_page_view  FLOAT,
    cost_per_initiate_checkout  FLOAT,
    cost_per_purchase           FLOAT,
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
"""

DELETE_SQL = f"""
DELETE FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.RAW_META_ADS_INSIGHTS
WHERE date_start = %s
"""

INSERT_SQL = f"""
INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.RAW_META_ADS_INSIGHTS (
    account_id, account_name, campaign_id, campaign_name, objective,
    adset_id, adset_name, optimization_goal, ad_id, ad_name,
    publisher_platform, date_start, date_stop,
    impressions, reach, frequency, spend, clicks, unique_clicks,
    cpm, cpc, cpp, ctr,
    link_clicks, landing_page_views, initiate_checkouts, purchases, purchase_value,
    cost_per_link_click, cost_per_landing_page_view,
    cost_per_initiate_checkout, cost_per_purchase
) VALUES (
    %(account_id)s, %(account_name)s, %(campaign_id)s, %(campaign_name)s, %(objective)s,
    %(adset_id)s, %(adset_name)s, %(optimization_goal)s, %(ad_id)s, %(ad_name)s,
    %(publisher_platform)s, %(date_start)s, %(date_stop)s,
    %(impressions)s, %(reach)s, %(frequency)s, %(spend)s, %(clicks)s, %(unique_clicks)s,
    %(cpm)s, %(cpc)s, %(cpp)s, %(ctr)s,
    %(link_clicks)s, %(landing_page_views)s, %(initiate_checkouts)s, %(purchases)s, %(purchase_value)s,
    %(cost_per_link_click)s, %(cost_per_landing_page_view)s,
    %(cost_per_initiate_checkout)s, %(cost_per_purchase)s
)
"""


def _action_val(actions: list | None, action_type: str) -> float:
    if not actions:
        return 0.0
    for a in actions:
        if a.get("action_type") == action_type:
            return float(a.get("value", 0))
    return 0.0


def _flatten(row: dict) -> dict:
    actions  = row.get("actions") or []
    act_vals = row.get("action_values") or []
    cost_per = row.get("cost_per_action_type") or []

    return {
        "account_id":         row["account_id"],
        "account_name":       row["account_name"],
        "campaign_id":        row["campaign_id"],
        "campaign_name":      row["campaign_name"],
        "objective":          row["objective"],
        "adset_id":           row["adset_id"],
        "adset_name":         row["adset_name"],
        "optimization_goal":  row["optimization_goal"],
        "ad_id":              row["ad_id"],
        "ad_name":            row["ad_name"],
        "publisher_platform": row["publisher_platform"],
        "date_start":         row["date_start"],
        "date_stop":          row["date_stop"],
        "impressions":    int(row["impressions"]),
        "reach":          int(row["reach"]),
        "frequency":      float(row["frequency"]),
        "spend":          float(row["spend"]),
        "clicks":         int(row["clicks"]),
        "unique_clicks":  int(row["unique_clicks"]),
        "cpm":            float(row["cpm"]),
        "cpc":            float(row["cpc"]),
        "cpp":            float(row["cpp"]),
        "ctr":            float(row["ctr"]),
        "link_clicks":           int(_action_val(actions, "link_click")),
        "landing_page_views":    int(_action_val(actions, "landing_page_view")),
        "initiate_checkouts":    int(_action_val(actions, "initiate_checkout")),
        "purchases":             int(_action_val(actions, "omni_purchase")),
        "purchase_value":        float(_action_val(act_vals, "omni_purchase")),
        "cost_per_link_click":        float(_action_val(cost_per, "link_click")),
        "cost_per_landing_page_view": float(_action_val(cost_per, "landing_page_view")),
        "cost_per_initiate_checkout": float(_action_val(cost_per, "initiate_checkout")),
        "cost_per_purchase":          float(_action_val(cost_per, "omni_purchase")),
    }


def fetch_insights(report_date: date) -> list[dict]:
    url    = f"{API_BASE_URL}/v21.0/{ACCOUNT_ID}/insights"
    params = {
        "time_range": f'{{"since":"{report_date.isoformat()}","until":"{report_date.isoformat()}"}}',
        "limit": 500,
    }

    all_rows: list[dict] = []
    after = None

    while True:
        if after:
            params["after"] = after

        log.info(f"Fetching page (after={after}) from {url}")
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        body = r.json()

        page = body.get("data", [])
        all_rows.extend(page)
        log.info(f"  Got {len(page)} rows (total so far: {len(all_rows)})")

        next_cur = body.get("paging", {}).get("next")
        if not next_cur or not page:
            break
        after = body["paging"]["cursors"]["after"]

    return all_rows


def load_to_snowflake(flat_rows: list[dict], report_date: date) -> None:
    log.info(f"Connecting to Snowflake: {SNOWFLAKE_ACCOUNT}")
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        warehouse=SNOWFLAKE_WAREHOUSE,
    )
    cur = conn.cursor()
    try:
        cur.execute(DDL)
        cur.execute(DELETE_SQL, (report_date.isoformat(),))
        log.info(f"Deleted existing rows for {report_date}")
        cur.executemany(INSERT_SQL, flat_rows)
        log.info(f"Inserted {len(flat_rows)} rows into RAW_META_ADS_INSIGHTS")
        conn.commit()
    finally:
        cur.close()
        conn.close()


def main():
    log.info(f"Starting extract-load for {REPORT_DATE}")
    raw_rows  = fetch_insights(REPORT_DATE)
    flat_rows = [_flatten(r) for r in raw_rows]
    if not flat_rows:
        log.warning("No rows returned. Exiting.")
        sys.exit(0)
    load_to_snowflake(flat_rows, REPORT_DATE)
    log.info("Done ✓")


if __name__ == "__main__":
    main()
