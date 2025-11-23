#!/usr/bin/env python3
"""
yc_daily_tracker.py

- Uses yc-oss batch JSON to list all companies in a given batch (env BATCH_SLUG).
- Detects new companies vs seen_slugs.json.
- Scrapes each new company's YC page for founder names and LinkedIn anchors.
- Appends new rows to a Google Sheet (service-account based).
- Writes CSV snapshot to ./results/ and updates seen_slugs.json.

Required env secrets (in CI):
 - GCP_SA_KEY_JSON  -> raw JSON contents of the service account key
 - SHEET_ID         -> Google Sheet ID
 - BATCH_SLUG       -> batch slug (default: winter-2026)

Install deps:
 pip install requests beautifulsoup4 python-dateutil gspread google-auth

Usage:
 python yc_daily_tracker.py
"""

import os
import json
import time
import csv
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

def load_seen():
    if STATE_FILE.exists():
        try:
            return set(json.loads(STATE_FILE.read_text(encoding="utf-8")))
        except Exception:
            return set()
    return set()

def save_seen(slugs):
    STATE_FILE.write_text(json.dumps(sorted(list(slugs)), indent=2), encoding="utf-8")

def fetch_batch(batch_slug):
    url = API_BATCH_BASE.format(batch=batch_slug)
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    return r.json()

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
    if not founders:
        headers = soup.find_all(lambda tag: tag.name in ("h2","h3","h4") and "Founder" in tag.text)
        if headers:
            block = headers[0].find_next_sibling()
            if block:
                for a in block.find_all("a", href=True):
                    if "linkedin.com" in a["href"]:
                        founders.append({"name": a.get_text(strip=True), "linkedin": a["href"]})
    unique = []
    seen_keys = set()
    for f in founders:
        key = (f.get("name","").strip(), f.get("linkedin",""))
        if key in seen_keys:
            continue
        seen_keys.add(key)
        unique.append(f)
    return unique

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

def main():
    batch_slug = os.environ.get("BATCH_SLUG", "winter-2026")
    sheet_id = os.environ.get("SHEET_ID")
    sa_json = os.environ.get("GCP_SA_KEY_JSON")
    # Basic validation
    if not sheet_id or not sa_json:
        print("Missing SHEET_ID or GCP_SA_KEY_JSON environment variable")
        return

    seen = load_seen()
    print("Loaded seen slugs:", len(seen))

    try:
        batch = fetch_batch(batch_slug)
    except Exception as e:
        print("Failed to fetch batch JSON:", e)
        return

    print("Fetched batch:", batch_slug, "->", len(batch), "companies")
    new_companies = [c for c in batch if c.get("slug") not in seen]
    print("New companies to process:", len(new_companies))

    if not new_companies:
        print("No new companies. Exiting.")
        return

    try:
        gc = gsheet_client_from_service_account_json(sa_json)
        sh, ws = get_or_create_sheet(gc, sheet_id, create_if_missing=False)
    except Exception as e:
        print("Failed to connect to Google Sheets:", e)
        return

    results = []
    for company in new_companies:
        slug = company.get("slug")
        name = company.get("name","")
        website = company.get("website","")
        yc_url = company.get("url") or f"https://www.ycombinator.com/companies/{slug}"
        one_liner = company.get("one_liner","")
        print("Processing:", name, slug)
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
