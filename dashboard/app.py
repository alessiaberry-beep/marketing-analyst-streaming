"""
app.py — Paramount+ Paid Social Analytics Dashboard
Connects to Snowflake mart tables and visualizes campaign performance.
Run: streamlit run dashboard/app.py
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import snowflake.connector

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Paramount+ Paid Social Analytics",
    page_icon="🎬",
    layout="wide",
)

# ── Snowflake connection ──────────────────────────────────────────────────────
@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        schema="DBT_DEV_MARTS",
    )


@st.cache_data(ttl=600)
def query(_conn, sql):
    return pd.read_sql(sql, _conn)


# ── Load data ─────────────────────────────────────────────────────────────────
try:
    conn = get_connection()

    fct = query(conn, "SELECT * FROM FCT_CHANNEL_PERFORMANCE")
    dim_campaign = query(conn, "SELECT * FROM DIM_CAMPAIGN")
    dim_date = query(conn, "SELECT * FROM DIM_DATE")
    dim_ad = query(conn, "SELECT * FROM DIM_AD")

    # Join fact with dimensions
    df = fct.merge(dim_campaign[["AD_ID", "CAMPAIGN_NAME", "OBJECTIVE"]], on="AD_ID", how="left")
    df = df.merge(dim_date, on="REPORT_DATE", how="left")

    data_loaded = True
except Exception as e:
    st.error(f"Could not connect to Snowflake: {e}")
    data_loaded = False
    df = pd.DataFrame()

# ── Main app ──────────────────────────────────────────────────────────────────
if data_loaded and not df.empty:
    # ── Header ────────────────────────────────────────────────────────────────
    st.title("🎬 Paramount+ Paid Social Analytics")

    # ── Sidebar filters ───────────────────────────────────────────────────────
    st.sidebar.header("Filters")

    campaigns = ["All"] + sorted(df["CAMPAIGN_NAME"].dropna().unique().tolist())
    selected_campaign = st.sidebar.selectbox("Campaign", campaigns)

    platforms = ["All"] + sorted(df["PUBLISHER_PLATFORM"].dropna().unique().tolist())
    selected_platform = st.sidebar.selectbox("Platform", platforms)

    date_min = pd.to_datetime(df["REPORT_DATE"]).min()
    date_max = pd.to_datetime(df["REPORT_DATE"]).max()
    date_range = st.sidebar.date_input(
        "Date Range",
        value=(date_min, date_max),
        min_value=date_min,
        max_value=date_max,
    )

    # ── Apply filters ─────────────────────────────────────────────────────────
    filtered = df.copy()
    if selected_campaign != "All":
        filtered = filtered[filtered["CAMPAIGN_NAME"] == selected_campaign]
    if selected_platform != "All":
        filtered = filtered[filtered["PUBLISHER_PLATFORM"] == selected_platform]
    if len(date_range) == 2:
        filtered = filtered[
            (pd.to_datetime(filtered["REPORT_DATE"]) >= pd.to_datetime(date_range[0])) &
            (pd.to_datetime(filtered["REPORT_DATE"]) <= pd.to_datetime(date_range[1]))
        ]

    # ── KPI Cards ─────────────────────────────────────────────────────────────
    st.subheader("Key Metrics")
    k1, k2, k3, k4, k5 = st.columns(5)

    total_spend = filtered["SPEND"].sum()
    total_impressions = filtered["IMPRESSIONS"].sum()
    total_clicks = filtered["CLICKS"].sum()
    total_purchases = filtered["PURCHASES"].sum()
    total_revenue = filtered["PURCHASE_VALUE"].sum()
    overall_roas = total_revenue / total_spend if total_spend > 0 else 0
    overall_cpa = total_spend / total_purchases if total_purchases > 0 else 0

    k1.metric("Total Spend", f"${total_spend:,.0f}")
    k2.metric("Impressions", f"{total_impressions:,.0f}")
    k3.metric("Purchases", f"{total_purchases:,.0f}")
    k4.metric("ROAS", f"{overall_roas:.2f}x")
    k5.metric("CPA", f"${overall_cpa:.2f}")

    # ── Row 1: Spend over time + ROAS by campaign ─────────────────────────────
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Daily Spend by Platform")
        spend_by_day = (
            filtered.groupby(["REPORT_DATE", "PUBLISHER_PLATFORM"])["SPEND"]
            .sum().reset_index()
        )
        fig = px.area(
            spend_by_day, x="REPORT_DATE", y="SPEND",
            color="PUBLISHER_PLATFORM",
            color_discrete_map={"facebook": "#1877F2", "instagram": "#E1306C"},
            labels={"SPEND": "Spend ($)", "REPORT_DATE": "Date", "PUBLISHER_PLATFORM": "Platform"},
        )
        fig.update_layout(margin=dict(t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("ROAS by Campaign")
        roas_by_camp = (
            filtered.groupby("CAMPAIGN_NAME")
            .apply(lambda x: x["PURCHASE_VALUE"].sum() / x["SPEND"].sum() if x["SPEND"].sum() > 0 else 0)
            .reset_index(name="ROAS")
            .sort_values("ROAS", ascending=True)
        )
        fig2 = px.bar(
            roas_by_camp, x="ROAS", y="CAMPAIGN_NAME", orientation="h",
            color="ROAS", color_continuous_scale="Teal",
            labels={"ROAS": "ROAS (x)", "CAMPAIGN_NAME": "Campaign"},
        )
        fig2.update_layout(margin=dict(t=10, b=10), coloraxis_showscale=False)
        st.plotly_chart(fig2, use_container_width=True)

    # ── Row 2: Conversion funnel + CPA trend ──────────────────────────────────
    col3, col4 = st.columns(2)

    with col3:
        st.subheader("Conversion Funnel")
        funnel_data = {
            "Stage": ["Impressions", "Clicks", "Landing Page Views", "Checkouts", "Purchases"],
            "Count": [
                int(filtered["IMPRESSIONS"].sum()),
                int(filtered["CLICKS"].sum()),
                int(filtered["LANDING_PAGE_VIEWS"].sum()) if "LANDING_PAGE_VIEWS" in filtered.columns else 0,
                int(filtered["INITIATE_CHECKOUTS"].sum()) if "INITIATE_CHECKOUTS" in filtered.columns else 0,
                int(filtered["PURCHASES"].sum()),
            ]
        }
        fig3 = go.Figure(go.Funnel(
            y=funnel_data["Stage"],
            x=funnel_data["Count"],
            textinfo="value+percent initial",
            marker=dict(color=["#0068C9", "#2986CC", "#5CA8DC", "#83C3FF", "#00D4AA"]),
        ))
        fig3.update_layout(margin=dict(t=10, b=10))
        st.plotly_chart(fig3, use_container_width=True)

    with col4:
        st.subheader("Weekly CPA Trend")
        filtered_cpa = filtered.copy()
        filtered_cpa["WEEK"] = pd.to_datetime(filtered_cpa["REPORT_DATE"]).dt.to_period("W").astype(str)
        cpa_by_week = (
            filtered_cpa.groupby("WEEK")
            .apply(lambda x: x["SPEND"].sum() / x["PURCHASES"].sum() if x["PURCHASES"].sum() > 0 else None)
            .dropna()
            .reset_index(name="CPA")
        )
        fig4 = px.line(
            cpa_by_week, x="WEEK", y="CPA",
            markers=True,
            labels={"CPA": "Cost Per Acquisition ($)", "WEEK": "Week"},
            color_discrete_sequence=["#00D4AA"],
        )
        fig4.update_layout(margin=dict(t=10, b=10))
        st.plotly_chart(fig4, use_container_width=True)

    # ── Row 3: Platform comparison + top ads ──────────────────────────────────
    col5, col6 = st.columns(2)

    with col5:
        st.subheader("Platform Comparison")
        platform_summary = filtered.groupby("PUBLISHER_PLATFORM").agg(
            Spend=("SPEND", "sum"),
            Impressions=("IMPRESSIONS", "sum"),
            Clicks=("CLICKS", "sum"),
            Purchases=("PURCHASES", "sum"),
        ).reset_index()
        platform_summary["CTR"] = (platform_summary["Clicks"] / platform_summary["Impressions"] * 100).round(2)
        platform_summary["ROAS"] = (platform_summary["Purchases"] * 9.99 / platform_summary["Spend"]).round(2)
        platform_summary["CPA"] = (platform_summary["Spend"] / platform_summary["Purchases"]).round(2)
        st.dataframe(
            platform_summary[["PUBLISHER_PLATFORM", "Spend", "Impressions", "Purchases", "CTR", "ROAS", "CPA"]],
            hide_index=True, use_container_width=True,
        )

    with col6:
        st.subheader("Top 10 Ads by ROAS")
        ad_perf = filtered.groupby("AD_ID").agg(
            Spend=("SPEND", "sum"),
            Purchases=("PURCHASES", "sum"),
            Revenue=("PURCHASE_VALUE", "sum"),
        ).reset_index()
        ad_perf = ad_perf.merge(dim_ad[["AD_ID", "AD_NAME", "CAMPAIGN_NAME"]], on="AD_ID", how="left")
        ad_perf["ROAS"] = (ad_perf["Revenue"] / ad_perf["Spend"]).round(2)
        top_ads = ad_perf.sort_values("ROAS", ascending=False).head(10)
        st.dataframe(
            top_ads[["AD_NAME", "CAMPAIGN_NAME", "Spend", "Purchases", "ROAS"]],
            hide_index=True, use_container_width=True,
        )

    # ── Press releases section ────────────────────────────────────────────────
    st.divider()
    st.subheader("📰 Recent Paramount+ Press Releases")

    try:
        pr = query(conn, """
            SELECT title, published_date, url, summary
            FROM MARKETING_ANALYTICS.DBT_DEV_STAGING.STG_PRESS_RELEASES
            ORDER BY published_date DESC NULLS LAST
            LIMIT 10
        """)
        for _, row in pr.iterrows():
            with st.expander(f"📄 {row['TITLE']}"):
                st.caption(f"Published: {row['PUBLISHED_DATE']}")
                st.write(row["SUMMARY"] or "No summary available.")
                st.markdown(f"[Read full press release →]({row['URL']})")
    except Exception as e:
        st.warning(f"Could not load press releases: {e}")

    # ── Footer ────────────────────────────────────────────────────────────────
    st.divider()
    st.caption("Data sources: Meta Ads API · Paramount+ Press Express · LMU ISBA 4715")

else:
    st.warning("No data available. Please check your Snowflake connection.")
