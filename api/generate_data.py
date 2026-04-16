"""
generate_data.py — Synthetic Meta Ads Insights Generator
Mirrors the real Meta Marketing API /insights response exactly:
  - Field names match the API (account_id, adset_id, date_start, date_stop, etc.)
  - Numeric metrics returned as STRINGS (as the real API does)
  - Conversions nested inside an `actions` array with action_type/value pairs
  - cost_per_action_type and action_values arrays included
  - publisher_platform breakdown (facebook / instagram)
  - Seeded for full reproducibility

Real API reference:
  https://developers.facebook.com/docs/marketing-api/insights/
"""

import random
from datetime import date, timedelta
from typing import Optional

SEED = 42

ACCOUNT_ID   = "act_1234567890"
ACCOUNT_NAME = "Paramount+ Paid Social"

CAMPAIGNS = [
    {
        "campaign_id":   "23851001001",
        "campaign_name": "Top Gun Maverick — Retargeting",
        "objective":     "OUTCOME_SALES",
        "adsets": [
            {
                "adset_id":          "23851002001",
             adset_name":        "Site Visitors 30d — 25-54",
                "optimization_goal": "OFFSITE_CONVERSIONS",
                "ads": [
                    {"ad_id": "23851003001", "ad_name": "TGM_Retarget_Video_30s_v1"},
                    {"ad_id": "23851003002", "ad_name": "TGM_Retarget_Carousel_v2"},
                ],
            },
            {
                "adset_id":          "23851002002",
                "adset_name":        "Video Viewers 14d — 18-44",
                "optimization_goal": "OFFSITE_CONVERSIONS",
                "ads": [
                    {"ad_id": "23851003003", "ad_name": "TGM_VideoView_Static_v1"},
                ],
            },
        ],
    },
    {
        "campaign_id":   "23851001002",
        "campaign_name": "NFL on CBS — Prospecting",
        "objective":     "OUTCOME_AWARENESS",
        "adsets": [
            {
                "adset_id":          "23851002003",
                "adset_name":        "NFL Fans — Men 25-54",
                "optimization_REACH",
                "ads": [
                    {"ad_id": "23851003004", "ad_name": "NFL_Prospecting_Video_15s"},
                    {"ad_id": "23851003005", "ad_name": "NFL_Prospecting_Image_v1"},
                ],
            },
            {
                "adset_id":          "23851002004",
                "adset_name":        "Sports Interest Broad — 18-49",
                "optimization_goal": "REACH",
                "ads": [
                    {"ad_id": "23851003006", "ad_name": "NFL_Broad_Image_v2"},
                ],
            },
        ],
    },
    {
        "campaign_id":   "23851001003",
        "campaign_name": "Tulsa King S2 — Lookalike",
        "objective":     "OUTCOME_SALES",
        "adsets": [
            {
                "adset_id":          "23851002005",
                "adset_name":        "Yellowstone LAL 2% — 35-65",
                "optimization_goal": "OFFSITE_CONVERSIONS",
                "ads": [
                    {"ad_id": "23851003007", "ad_name": "TK_deo_Trailer_v1"},
                ],
            },
            {
                "adset_id":          "23851002006",
                "adset_name":        "Drama Interest LAL 5% — 25-54",
                "optimization_goal": "OFFSITE_CONVERSIONS",
                "ads": [
                    {"ad_id": "23851003008", "ad_name": "TK_LAL_Static_Key_Art"},
                    {"ad_id": "23851003009", "ad_name": "TK_LAL_Carousel_Clips"},
                ],
            },
        ],
    },
    {
        "campaign_id":   "23851001004",
        "campaign_name": "Paramount+ Bundle — Win-Back",
        "objective":     "OUTCOME_SALES",
        "adsets": [
            {
                "adset_id":          "23851002007",
                "adset_name":        "Churned Subs 90d",
                "optimization_goal": "OFFSITE_CONVERSIONS",
                "ads": [
                    {"ad_id": "23851003010", "ad_name": "Bundle_WinBack_Offer_v1"},
                    {"ad_id": "23851003011", "ad_name": "Bundle_WinBack_r_v2"},
                ],
            },
            {
                "adset_id":          "23851002008",
                "adset_name":        "Trial Abandoners 30d",
                "optimization_goal": "OFFSITE_CONVERSIONS",
                "ads": [
                    {"ad_id": "23851003012", "ad_name": "Bundle_TrialAbandoner_Video"},
                ],
            },
        ],
    },
    {
        "campaign_id":   "23851001005",
        "campaign_name": "Free Trial Promo — Broad Acquisition",
        "objective":     "OUTCOME_SALES",
        "adsets": [
            {
                "adset_id":          "23851002009",
                "adset_name":        "Cord-Cutters Interest — 25-49",
                "optimization_goal": "OFFSITE_CONVERSIONS",
                "ads": [
                    {"ad_id": "23851003013", "ad_name": "FreeTrial_Broad_Video_30s"},
                    {"ad_id": "23851003014", "ad_name": "FreeTrial_Broad_Image_v1"},
                ],
            },
            {
            "adset_id":          "23851002010",
                "adset_name":        "Streaming Switchers — 18-44",
                "optimization_goal": "OFFSITE_CONVERSIONS",
                "ads": [
                    {"ad_id": "23851003015", "ad_name": "FreeTrial_Switchers_Carousel"},
                ],
            },
        ],
    },
]

PLATFORMS = ["facebook", "instagram"]

CAMPAIGN_PROFILE = {
    "23851001001": {"budget": (600,  1100), "is_conversion": True,  "retarget": True},
    "23851001002": {"budget": (1800, 3500), "is_conversion": False, "retarget": False},
    "23851001003": {"budget": (500,  900),  "is_conversion": True,  "retarget": False},
    "23851001004": {"budget": (900,  1800), "is_conversion": True,  "retarget": True},
    "23851001005": {"budget": (1200, 2500), "is_conversion": True,  "retarget": False},
}

SUBSCRIPTION_VALUE = 9.99


def _week_mult(d: date) -> float:
    return 1.12 if d.weekday() >= 5 else 1.0


def _season_mult(d: date) -> float:
    if d.month in (10, 11, 12): return 10
    if d.month in (1, 2):       return 0.82
    return 1.0


def generate_insights(
    start_date: Optional[date] = None,
    end_date:   Optional[date] = None,
    days: int = 90,
) -> list[dict]:
    random.seed(SEED)

    if end_date is None:
        end_date = date.today() - timedelta(days=1)
    if start_date is None:
        start_date = end_date - timedelta(days=days - 1)

    rows = []
    current = start_date

    while current <= end_date:
        wm = _week_mult(current)
        sm = _season_mult(current)

        for camp in CAMPAIGNS:
            cid      = camp["campaign_id"]
            profile  = CAMPAIGN_PROFILE[cid]
            lo, hi   = profile["budget"]
            is_conv  = profile["is_conversion"]
            retarget = profile["retarget"]

            total_ads = sum(len(a["ads"]) for a in camp["adsets"])
            n_splits  = total_ads * len(PLATFORMS)

            for adset in camp["adsets"]:
                for ad in adset["ads"]:
                    for platform in PLATFORMS:

                        spend = round(
                            random.uniform(lo, hi) / n_splits * wm * sm * random.uniform(0.82, 1.18),
                            2,
                        )

                        cpm_val     = random.uniform(7.0, 20.0)
                        impressions = max(100, int(spend / cpm_val * 1000))
                        frequency   = round(random.uniform(1.05, 2.80), 2)
                        reach       = max(1, int(impressions / frequency))

                        ctr_lo, ctr_hi = (0.025, 0.055) if retarget else (0.007, 0.022)
                        ctr_rate = random.uniform(ctr_lo, ctr_hi)
                        clicks   = max(1, int(impressions * ctr_rate))
                        unique_clicks = max(1, int(clicks * random.uniform(0.75, 0.92)))

                        cpc_val = round(spend / clicks, 4)             if clicks      else 0.0
                        cpm_out = round(spend / impressions * 1000, 4) if impressions else 0.0
                        cpp_val = round(spend / reach * 1000, 4)       if reach       else 0.0
                        ctr_out = round(ctr_rate * 100, 4)

                        actions: list[dict] = []
                        action_values: list[dict] = []
                        cost_per_action_type: list[dict] = []

                        actions.append({"action_type": "link_click", "value": str(clicks)})
                        cost_per_action_type.append(
                            {"action_type": "link_click", "value": str(cpc_val)}
                        )

                        lpv = max(0, int(clicks * random.uniform(0.72, 0.88)))
                        if lpv:
                            actions.append({"action_type": "landing_page_view", "value": str(lpv)})
                            cost_per_action_type.append({
                                "action_type": "landing_page_view",
                                "value": str(round(spend / lpv, 4)),
                            })

                        if is_conv:
                            cvr_lo, cvr_hi = (0.035, 0.11) if retarget else (0.012, 0.042)
                            cvr       = random.uniform(cvr_lo, cvr_hi)
                            purchases = max(0, int(clicks * cvr))
                            checkouts = max(purchases, int(clicks * cvr * random.uniform(1.8, 3.2)))

                            if checkouts:
                                actions.append({"action_type": "initiate_checkout", "value": str(checkouts)})
                                cost_per_action_type.append({
                                    "action_type": "initiate_checkout",
                                    "value": str(round(spend / checkouts, 4)),
                                })

                            if purchases:
                                rev = round(purchases * SUBSCRIPTION_VALUE, 2)
                                actions.append({"action_type": "omni_purchase", "value": str(purchases)})
                                action_values.append({"action_type": "omni_purchase", "value": str(rev)})
                                cost_per_action_type.append({
                                    "action_type": "omni_purchase",
                                    "value": str(round(spend / purchases, 2)),
                                })

                        rows.append({
                            "account_id":          ACCOUNT_ID,
                            "account_name":        ACCOUNT_NAME,
                            "campaign_id":         cid,
                            "campaign_name":       camp["campaign_name"],
                            "objective":           camp["objective"],
                            "adset_id":            adset["adset_id"],
                            "adset_name":          adset["adset_name"],
                            "optimization_goal":   adset["optimization_goal"],
                            "ad_id":               ad["ad_id"],
                            "ad_name":             ad["ad_name"],
                            "publisher_platform":  platform,
                            "date_start":          current.isoformat(),
                            "date_stop":           current.isoformat(),
                            "impressions":         str(impressions),
                            "reach":               str(reach),
                            "frequency":           str(frequency),
                            "spend":               str(spend),
                            "clicks":              str(clicks),
                            "unique_clicks":       str(unique_clicks),
                            "cpm":                 str(cpm_out),
                            "cpc":                 str(cpc_val),
                            "cpp":                 str(cpp_val),
                            "ctr":                 str(ctr_out),
                            "actions":              actions,
                            "action_values":        action_values if action_values else None,
                            "cost_per_action_type": cost_per_action_type,
                        })

        current += timedelta(days=1)

    return rows


if __name__ == "__main__":
    import json
    rows = generate_insights(days=90)
    print(f"Generated {len(rows):,} rows")
    print(json.dumps(rows[0], indent=2))
