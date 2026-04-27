"""
scrape_load.py — Scrape Paramount+ Press Releases → Load to Snowflake RAW
Scrapes https://www.paramountpressexpress.com/paramount-plus/
Extracts: title, date, url, summary, full_text
Loads to Snowflake RAW.RAW_PRESS_RELEASES
"""

import os
import sys
import logging
import time
from datetime import date, datetime
from typing import Optional

import requests
from bs4 import BeautifulSoup
import snowflake.connector

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
BASE_URL            = "https://www.paramountpressexpress.com/paramount-plus/"
SNOWFLAKE_ACCOUNT   = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_USER      = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD  = os.environ["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_DATABASE  = os.environ["SNOWFLAKE_DATABASE"]
SNOWFLAKE_WAREHOUSE = os.environ["SNOWFLAKE_WAREHOUSE"]
SNOWFLAKE_SCHEMA    = os.environ["SNOWFLAKE_SCHEMA"]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

# ── DDL ───────────────────────────────────────────────────────────────────────
DDL = f"""
CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.RAW_PRESS_RELEASES (
    press_release_id    VARCHAR(200),
    title               VARCHAR(500),
    published_date      DATE,
    url                 VARCHAR(1000),
    summary             VARCHAR(2000),
    full_text           TEXT,
    scraped_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
"""

MERGE_SQL = f"""
MERGE INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.RAW_PRESS_RELEASES AS target
USING (
    SELECT
        %s AS press_release_id,
        %s AS title,
        %s AS published_date,
        %s AS url,
        %s AS summary,
        %s AS full_text,
        CURRENT_TIMESTAMP() AS scraped_at
) AS source
ON target.press_release_id = source.press_release_id
WHEN NOT MATCHED THEN INSERT (
    press_release_id, title, published_date, url, summary, full_text, scraped_at
) VALUES (
    source.press_release_id, source.title, source.published_date,
    source.url, source.summary, source.full_text, source.scraped_at
);
"""


# ── Scraper ───────────────────────────────────────────────────────────────────
def scrape_press_releases(max_pages: int = 5) -> list[dict]:
    """Scrape press release listings from Paramount+ newsroom."""
    all_releases = []

    for page in range(1, max_pages + 1):
        url = BASE_URL if page == 1 else f"{BASE_URL}?pg={page}"
        log.info(f"Scraping page {page}: {url}")

        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            r.raise_for_status()
        except Exception as e:
            log.warning(f"Failed to fetch page {page}: {e}")
            continue

        soup = BeautifulSoup(r.text, "html.parser")

        # Try multiple selectors for press release items
        items = (
            soup.find_all("div", class_="ppe-release") or
            soup.find_all("article") or
            soup.find_all("div", class_="release-item")
        )

        if not items:
            # fallback: find all links that look like press releases
            items = soup.find_all("a", href=lambda h: h and "/releases/" in str(h))

        if not items:
            log.warning(f"No items found on page {page}, stopping.")
            break

        log.info(f"Found {len(items)} items on page {page}")

        for item in items:
            try:
                release = _parse_listing_item(item)
                if release:
                    all_releases.append(release)
            except Exception as e:
                log.warning(f"Error parsing item: {e}")
                continue

        time.sleep(1)  # be polite

    log.info(f"Total press releases found: {len(all_releases)}")
    return all_releases


def _parse_listing_item(item) -> Optional[dict]:
    """Parse a single press release listing item."""
    # Try to get the link
    link = item if item.name == "a" else item.find("a")
    if not link:
        return None

    url = link.get("href", "")
    if not url.startswith("http"):
        url = "https://www.paramountpressexpress.com" + url

    # Title
    title_el = item.find(["h1", "h2", "h3", "h4"]) or link
    title = title_el.get_text(strip=True) if title_el else url

    # Date
    date_el = item.find("time") or item.find(class_=lambda c: c and "date" in str(c).lower())
    published_date = None
    if date_el:
        date_str = date_el.get("datetime") or date_el.get_text(strip=True)
        published_date = _parse_date(date_str)

    # Summary
    summary_el = item.find("p") or item.find(class_=lambda c: c and "summary" in str(c).lower())
    summary = summary_el.get_text(strip=True)[:2000] if summary_el else ""

    # Generate ID from URL
    press_release_id = url.rstrip("/").split("/")[-1][:200]

    return {
        "press_release_id": press_release_id,
        "title":            title[:500],
        "published_date":   published_date,
        "url":              url[:1000],
        "summary":          summary,
        "full_text":        "",  # filled in by fetch_full_text
    }


def fetch_full_text(releases: list[dict], max_articles: int = 50) -> list[dict]:
    """Fetch full text for each press release."""
    enriched = []
    for i, release in enumerate(releases[:max_articles]):
        try:
            log.info(f"Fetching full text {i+1}/{min(len(releases), max_articles)}: {release['url']}")
            r = requests.get(release["url"], headers=HEADERS, timeout=15)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")

            # Try common content selectors
            content = (
                soup.find("div", class_=lambda c: c and "release-body" in str(c).lower()) or
                soup.find("div", class_=lambda c: c and "content" in str(c).lower()) or
                soup.find("main") or
                soup.find("article")
            )

            if content:
                # Remove script/style tags
                for tag in content(["script", "style", "nav", "header", "footer"]):
                    tag.decompose()
                full_text = content.get_text(separator="\n", strip=True)
            else:
                full_text = soup.get_text(separator="\n", strip=True)[:5000]

            # If no date from listing, try to find it in article
            if not release["published_date"]:
                date_el = soup.find("time") or soup.find(class_=lambda c: c and "date" in str(c).lower())
                if date_el:
                    date_str = date_el.get("datetime") or date_el.get_text(strip=True)
                    release["published_date"] = _parse_date(date_str)

            release["full_text"] = full_text[:10000]
            enriched.append(release)
            time.sleep(0.5)

        except Exception as e:
            log.warning(f"Failed to fetch full text for {release['url']}: {e}")
            release["full_text"] = ""
            enriched.append(release)

    return enriched


def _parse_date(date_str: str) -> Optional[str]:
    """Try to parse various date formats."""
    if not date_str:
        return None
    date_str = date_str.strip()
    formats = [
        "%Y-%m-%d", "%B %d, %Y", "%b %d, %Y",
        "%m/%d/%Y", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"
    ]
    for fmt in formats:
        try:
            return datetime.strptime(date_str[:len(fmt)+5], fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


# ── Snowflake load ────────────────────────────────────────────────────────────
def load_to_snowflake(releases: list[dict]) -> None:
    """Load press releases to Snowflake using MERGE to avoid duplicates."""
    if not releases:
        log.warning("No releases to load")
        return

    log.info("Connecting to Snowflake")
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        database=SNOWFLAKE_DATABASE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        schema=SNOWFLAKE_SCHEMA,
    )
    cur = conn.cursor()

    try:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_DATABASE}")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}")
        cur.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
        cur.execute(DDL)
        log.info("Table ready")

        inserted = 0
        for r in releases:
            cur.execute(MERGE_SQL, (
                r["press_release_id"],
                r["title"],
                r["published_date"],
                r["url"],
                r["summary"],
                r["full_text"],
            ))
            inserted += 1

        conn.commit()
        log.info(f"Loaded {inserted} press releases into RAW_PRESS_RELEASES")
    except Exception as e:
        log.error(f"Failed to load to Snowflake: {e}")
        raise
    finally:
        cur.close()
        conn.close()


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    log.info("Starting Paramount+ press release scraper")

    # Scrape press release listings
    releases = scrape_press_releases(max_pages=5)

    if not releases:
        log.warning("No press releases found, exiting")
        sys.exit(0)

    # Fetch full text for each release
    releases = fetch_full_text(releases, max_articles=50)

    # Load to Snowflake
    load_to_snowflake(releases)

    log.info("Scrape and load complete")


if __name__ == "__main__":
    main()
