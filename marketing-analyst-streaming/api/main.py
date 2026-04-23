"""
main.py — Local Synthetic Meta Ads Insights API
Mirrors the real Graph API /insights endpoint response shape exactly.

Run locally:
    pip install fastapi uvicorn
    uvicorn main:app --reload --port 8000

Endpoints:
    GET /                                   → health / account info
    GET /v21.0/act_{id}/campaigns           → list campaigns
    GET /v21.0/act_{id}/insights            → account-level insights
    GET /v21.0/{campaign_id}/insights       → campaign-level insights
    GET /v21.0/{adset_id}/insights          → ad set-level insights
    GET /v21.0/{ad_id}/insights             → ad-level insights
"""

import json
from datetime import date, timedelta
from typing import Optional

from fastapi import FastAPI, Query, Path
from fastapi.middleware.cors import CORSMiddleware

from generate_data import generate_insights, CAMPAIGNS, ACCOUNT_ID, ACCOUNT_NAME

app = FastAPI(
    title="Synthetic Meta Ads Insights API",
    description=(
        "Mirrors Meta Marketing API /insightpe. "
        "Used for LMU ISBA 4715 analytics engineering portfolio project."
    ),
    version="21.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

_END   = date.today() - timedelta(days=1)
_START = _END - timedelta(days=89)
_ALL_ROWS: list[dict] = generate_insights(start_date=_START, end_date=_END)

_BY_CAMPAIGN: dict[str, list[dict]] = {}
_BY_ADSET:    dict[str, list[dict]] = {}
_BY_AD:       dict[str, list[dict]] = {}

for r in _ALL_ROWS:
    _BY_CAMPAIGN.setdefault(r["campaign_id"], []).append(r)
    _BY_ADSET.setdefault(r["adset_id"], []).append(r)
    _BY_AD.setdefault(r["ad_id"], []).append(r)


def _parse_dates(
    date_preset: Optional[str],
    time_range:  Optional[str],
) -> tuple[date, date]:
    today = date.today()

    if time_range:
        try:
            tr = json.loads(time_range)
            return date.fromisoformat(tr["since"]), date.fromisoformat(tr["until"])
        except Exception:
            pass

    presets = {
        "yesterday":  (today - timedelta(days=1),  today - timedelta(days=1)),
        "last_7d":    (today - timedelta(days=7),  today - timedelta(days=1)),
        "last_14d":   (today - timedelta(days=14), today - timedelta(days=1)),
        "last_28d":   (today - timedelta(days=28), today - timedelta(days=1)),
        "last_30d":   (today - timedelta(days=30), today - timedelta(days=1)),
        "last_90d":   (today - timedelta(days=90), today - timedelta(days=1)),
        "this_month": (today.replace(day=1),        today - timedelta(days=1)),
    }
    return presets.get(date_preset or "last_30d", presets["last_30d"])


def _filter_rows(rows: list[dict], start: date, end: date) -> list[dict]:
    return [
        r for r in rows
        if start.isoformat() <= r["date_start"] <= end.isoformat()
    ]


def _paginate(rows: list[dict], limit: int, after: Optional[str]) -> dict:
    start_idx = 0
    if after:
        try:
            start_idx = int(after)
        except ValueError:
            start_idx = 0

    page     = rows[start_idx: start_idx + limit]
    next_cur = str(start_idx + limit) if start_idx + limit < len(rows) else None

    paging: dict = {"cursors": {"before": str(start_idx), "after": str(start_idx + limit)}}
    if next_cur:
        paging["next"] = f"cursor:{next_cur}"

    return {"data": page, "paging": paging}


@app.get("/", tags=["Account"])
def account_info():
    return {
        "id":            ACCOUNT_ID,
        "name":          ACCOUNT_NAME,
        "account_id":    ACCOUNT_ID.replace("act_", ""),
        "currency":      "USD",
        "timezone_name": "America/Los_Angeles",
        "rows_available": len(_ALL_ROWS),
        "date_range": {"start": _START.isoformat(), "end": _END.isoformat()},
    }


@app.get("/v21.0/{account_id}/campaigns", tags=["Campaigns"])
def list_campaigns(account_id: str = Path(...)):
    return {
        "data": [
            {
                "id":        c["campaign_id"],
                "name":      c["campaign_name"],
                "objective": c["objective"],
                "status":    "ACTIVE",
            }
            for c in CAMPAIGNS
        ],
        "paging": {"cursors": {"before": "MA==", "after": "MA=="}},
    }


@app.get("/v21.0/{account_id}/insights", tags=["Insights"])
def account_insights(
    account_id:  str = Path(...),
    date_preset: Optional[str] = Query("last_30d"),
    time_range:  Optional[str] = Query(None),
    fields:      Optional[str] = Query(None),
    level:       Optional[str] = Query("ad"),
    limit:       int           = Query(25, ge=1, le=1000),
    after:       Optional[str] = Query(None),
):
    start, end = _parse_dates(date_preset, time_range)
    rows = _filter_rows(_ALL_ROWS, start, end)
    return _paginate(rows, limit, after)


@app.get("/v21.0/{object_id}/insights", tags=["Insights"])
def object_insights(
    object_id:   str = Path(...),
    date_preset: Optional[str] = Query("last_30d"),
    time_range:  Optional[str] = Query(None),
    fields:      Optional[str] = Query(None),
    limit:       int           = Query(25, ge=1, le=1000),
    after:       Optional[str] = Query(None),
):
    start, end = _parse_dates(date_preset, time_range)

    if object_id in _BY_CAMPAIGN:
        rows = _filter_rows(_BY_CAMPAIGN[object_id], start, end)
    elif object_id in _BY_ADSET:
        rows = _filter_rows(_BY_ADSET[object_id], start, end)
    elif object_id in _BY_AD:
        rows = _filter_rows(_BY_AD[object_id], start, end)
    else:
        return {"data": [], "paging": {"cursors": {"before": "MA==", "after": "MA=="}}}

    return _paginate(rows, limit, after)
