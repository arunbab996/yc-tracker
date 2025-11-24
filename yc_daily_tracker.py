#!/usr/bin/env python3
"""
yc_daily_tracker.py

- Fetches YC batch companies from yc-oss JSON
- Scrapes YC website companies list for the same batch (merges in any site-only results)
- Detects new companies vs seen_slugs.json
- Scrapes founders (LinkedIn) from each company's YC page
- Appends new rows to a Google Sheet (service-account based)
- Writes CSV snapshot to ./results/ and updates seen_slugs.json

Environment variables required in CI:
 - GCP_SA_KEY_JSON  -> full JSON content of the service account key
 - SHEET_ID         -> Google Sheet ID
 - BATCH_SLUG       -> batch slug (eg. winter-2026)

Optional:
 - REQUEST_DELAY    -> seconds to sleep between requests (default 0.8)

Local CSV preview (useful in this environment for debugging):
 - LOCAL_CSV_PREVIEW points to the uploaded CSV in this environment:
   /mnt/data/yc_winter2025_sample.csv
"""

import os
import json
import time
import csv
import re
from pathlib import Path
from datetime import datetime, timezone
from dateutil import tz
import requests
from bs4 import BeautifulSoup

# Google libs
import gspread
from google.oauth2.service_account import Credentials

# Config
WORKDIR = Path("results")
WORKDIR.mkdir(exist_ok=True)
STATE_FILE = Path("seen_slugs.json")
API_BATCH_BASE = "https://yc-oss.github.io/api/batches/{batch}.json"
HEADERS = {"User-Agent": "YC-Daily-Tracker/1.0 (ci)"}
REQUEST_DELAY = float(os.environ.get("REQUEST_DELAY", "0.8"))

# Path to the CSV you uploaded in this environment (for local preview/testing)
LOCAL_CSV_PREVIEW = "/mnt/data/yc_winter2025_sample.csv"

# ---------------------------
# Helpers
# ---------------------------
def safe_env(name, default=None):
    v = os.environ.get(name, default)
    if v is None:
        return default
    return v.strip()

def load_seen():
    if STATE_FILE.exists():
        try:
            return set(json.loads(STATE_FILE.read_text(encoding="utf-8")))
        except Exception:
            return set()
    return set()

def save_seen(slugs):
    STATE_FILE.write_text(json.dumps(sorted(list(slugs)), indent=2), encoding="utf-8")

def fetch_json_batch(batch_slug):
    url = API_BATCH_BASE.format(batch=batch_slug)
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        return r.json() or []
    except Exception as e:
        print("Warning: failed to fetch yc-oss JSON:", e)
        return []

def site_batch_string_from_slug(batch_slug):
    """
    Convert 'winter-2026' -> 'Winter%202026' (website expects capitalized season + space)
    Works generically for patterns like 'winter-2026', 'summer-2025', etc.
    """
    if not batch_slug:
        return batch_slug
    # allow either "winter-2026" or "winter 2026" variants
    s = batch_slug.strip()
    s = s.replace("%20", " ").replace("-", " ")
    parts = s.split()
    if len(parts) >= 2:
        season = parts[0].capitalize()
        year = parts[1]
        return f"{season}%20{year}"
    return batch_slug

def scrape_yc_site(batch_slug):
    """
    Scrape the YC companies listing page for the given batch.
    Returns a dict slug -> minimal company dict.
    Uses the website's batch string mapping (e.g. 'Winter%202026').
    Note: YC may client-side render; this tries static scraping first.
    """
    site_batch = site_batch_string_from_slug(batch_slug)
    site_url = f"https://www.ycombinator.com/companies?batch={site_batch}"
    print("Scraping YC site:", site_url)
    results = {}
    try:
        r = requests.get(site_url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        # find anchors that link to /companies/<slug>
        anchors = soup.select('a[href*="/companies/"]')
        for a in anchors:
            href = a.get("href") or ""
            m = re.search(r'/companies/([^/?#]+)', href)
            if not m:
                continue
            slug = m.group(1).strip()
            if not slug:
                continue
            # company name (anchor text) and a best-effort one-liner
            name = a.get_text(" ", strip=True) or ""
            one_liner = ""
            parent = a.parent
            if parent:
                # collect small descriptive text near the anchor as heuristic
                txt = parent.get_text(" ", strip=True)
                # remove the name itself
                if name:
                    one_liner = txt.replace(name, "").strip()
                else:
                    one_liner = txt.strip()
            yc_url = href if href.startswith("http") else ("https://www.ycombinator.com" + href)
            results[slug] = {"slug": slug, "name": name, "one_liner": one_liner, "url": yc_url}
    except Exception as e:
        print("Warning: static scraping YC site failed:", e)
    return results

def fetch_combined_batch(batch_slug):
    """
    Primary: yc-oss JSON
    Secondary: scrape YC website and merge in any missing slugs (site-only)
    Returns a list of company dicts.
    """
    print("Fetching yc-oss JSON for batch:", batch_slug)
    companies = fetch_json_batch(batch_slug)
    slug_map = {c.get("slug"): c for c in companies if c.get("slug")}
    # scrape site and merge
    site_map = scrape_yc_site(batch_slug)
    added = 0
    for slug, comp in site_map.items():
        if slug not in slug_map:
            slug_map[slug] = comp
            added += 1
    if added:
        print(f"Merged {added} site-only companies into batch list")
    return list(slug_map.values())

# ---------------------------
# Founder extraction (unchanged, robust)
# ---------------------------
def parse_founders_from_yc_page(yc_company_url):
    try:
        r = requests.get(yc_company_url, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        print("Fetch failed for", yc_company_url, e)
        return []
    soup = BeautifulSoup(r.text, "html.parser")
    linkedin_anchors = soup.select('a[href*="linkedin.com"]')
    founders = []
    seen_links = set()
    for a in linkedin_anchors:
        href = a.get("href")
        if not href or href in seen_links:
            continue
        seen_links.add(href)
        name = None
        node = a
        for _ in range(4):
            node = node.parent
            if node is None:
                break
            txt = node.get_text(" ", strip=True)
            if txt and len(txt.split()) <= 6 and len(txt) < 120:
                name = txt.strip()
                break
        founders.append({"name": name or "", "linkedin": href})
    # fallback: look for heading with "Founder"
    if not founders:
        headers = soup.find_all(lambda tag: tag.name in ("h2","h3","h4") and "Founder" in tag.text)
        if headers:
            block = headers[0].find_next_sibling()
            if block:
                for a in block.find_all("a", href=True):
                    if "linkedin.com" in a["href"]:
                        founders.append({"name": a.get_text(strip=True), "linkedin": a["href"]})
    # dedupe
    unique = []
    seen_keys = set()
    for f in founders:
        key = (f.get("name","").strip(), f.get("linkedin",""))
        if key in seen_keys:
            continue
        seen_keys.add(key)
        unique.append(f)
    return unique

# ---------------------------
# Google Sheets helper
# ---------------------------
def gsheet_client_from_service_account_json(sa_json_str):
    info = json.loads(sa_json_str)
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def get_or_create_sheet(gc, sheet_id, create_if_missing=False, sheet_title=None):
    try:
        sh = gc.open_by_key(sheet_id)
    except Exception as e:
        if create_if_missing and sheet_title:
            sh = gc.create(sheet_title)
        else:
            raise
    ws = sh.sheet1
    header = ["checked_at_utc","slug","company_name","website","yc_url","company_linkedin","founders_json","one_liner"]
    try:
        existing = ws.row_values(1)
    except Exception:
        existing = []
    if existing != header:
        try:
            ws.update("A1", [header])
        except Exception:
            pass
    return sh, ws

# ---------------------------
# Main
# ---------------------------
def main():
    batch_slug = (safe_env("BATCH_SLUG") or "winter-2026").strip()
    # allow a debug override to use the local CSV for preview (not used in CI)
    use_local_preview = os.environ.get("USE_LOCAL_PREVIEW", "false").lower() in ("1","true","yes")
    sheet_id = safe_env("SHEET_ID")
    sa_json = os.environ.get("GCP_SA_KEY_JSON")  # do not strip; it's JSON
    if not use_local_preview and (not sheet_id or not sa_json):
        print("Missing SHEET_ID or GCP_SA_KEY_JSON environment variable (or set USE_LOCAL_PREVIEW=true for local CSV preview).")
        # continue in local-preview mode if requested
        if not use_local_preview:
            return

    seen = load_seen()
    print("Loaded seen slugs:", len(seen))

    if use_local_preview:
        # quick local preview mode: read the uploaded CSV (no web calls)
        csv_path = Path(LOCAL_CSV_PREVIEW)
        if not csv_path.exists():
            print("Local preview CSV not found at:", LOCAL_CSV_PREVIEW)
            return
        import csv as _csv
        rows = []
        with csv_path.open(encoding="utf-8") as f:
            reader = _csv.DictReader(f)
            for r in reader:
                rows.append(r)
        print("Loaded local CSV rows:", len(rows))
        batch = rows
    else:
        # live fetch: combine JSON + site scraping
        batch = fetch_combined_batch(batch_slug)

    print("Total companies obtained (merged):", len(batch))

    # Determine new companies by slug
    slugs_in_batch = [c.get("slug") for c in batch if c.get("slug")]
    new_companies = [c for c in batch if c.get("slug") and c.get("slug") not in seen]
    print("New companies to process:", len(new_companies))

    if not new_companies:
        print("No new companies. Exiting.")
        return

    # connect to Google Sheets
    gc = gsheet_client_from_service_account_json(sa_json)
    sh, ws = get_or_create_sheet(gc, sheet_id, create_if_missing=False)

    results = []
    for company in new_companies:
        slug = company.get("slug")
        name = company.get("name","") or company.get("company_name","")
        website = company.get("website","") or company.get("url","")
        yc_url = company.get("url") or company.get("yc_url") or f"https://www.ycombinator.com/companies/{slug}"
        one_liner = company.get("one_liner","") or company.get("one_liner","")
        print("Processing:", name or slug, slug)
        founders = parse_founders_from_yc_page(yc_url)
        record = {
            "checked_at_utc": datetime.now(timezone.utc).isoformat(),
            "slug": slug,
            "company_name": name,
            "website": website,
            "yc_url": yc_url,
            "company_linkedin": "",
            "founders_json": json.dumps(founders, ensure_ascii=False),
            "one_liner": one_liner
        }
        results.append(record)
        try:
            row = [record[k] for k in ["checked_at_utc","slug","company_name","website","yc_url","company_linkedin","founders_json","one_liner"]]
            ws.append_row(row, value_input_option='USER_ENTERED')
        except Exception as e:
            print("Failed to append row to sheet for", slug, ":", e)
        seen.add(slug)
        time.sleep(REQUEST_DELAY)

    nowtag = datetime.now(tz=tz.tzlocal()).strftime("%Y%m%d_%H%M%S")
    csv_path = WORKDIR / f"yc_new_{batch_slug}_{nowtag}.csv"
    try:
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["checked_at_utc","slug","company_name","website","yc_url","company_linkedin","founders_json","one_liner"])
            writer.writeheader()
            for r in results:
                writer.writerow(r)
        print("Wrote CSV:", csv_path)
    except Exception as e:
        print("Failed to write CSV:", e)

    save_seen(seen)
    print("Updated seen_slugs.json with", len(seen), "entries")

if __name__ == "__main__":
    main()
