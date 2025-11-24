#!/usr/bin/env python3
"""
yc_daily_tracker.py — improved:
- Normalizes output to a single header order that matches the Google Sheet.
- Avoids duplicates by checking the Google Sheet's existing slugs before appending.
- Uses yc-oss JSON + site scrape + Playwright fallback (if available).
- Only marks slugs as seen after successful append.
"""

import os
import json
import csv
import time
import requests
import re
from datetime import datetime, timezone
from bs4 import BeautifulSoup

# Playwright import (optional)
try:
    from playwright.sync_api import sync_playwright
    _PLAYWRIGHT_AVAILABLE = True
except Exception:
    _PLAYWRIGHT_AVAILABLE = False

# -------------------------
# Config & constants
# -------------------------
LOCAL_CSV_PREVIEW = "/mnt/data/yc_winter2025_sample.csv"  # local test CSV path
WORKDIR = "results"
os.makedirs(WORKDIR, exist_ok=True)

# Column order to match your sheet
SHEET_HEADER = [
    "checked_at_utc",
    "slug",
    "company_name",
    "website",
    "yc_url",
    "company_linked",
    "founders_json",
    "one_liner"
]

# -------------------------
# Helpers
# -------------------------
def safe_env(key, default=None):
    v = os.environ.get(key)
    if v is None:
        return default
    return v.strip()

def now_iso_utc():
    return datetime.now(timezone.utc).isoformat()

# -------------------------
# Seen state (local file)
# -------------------------
SEEN_PATH = "seen_slugs.json"
def load_seen():
    if os.path.exists(SEEN_PATH):
        try:
            with open(SEEN_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                return set(data if isinstance(data, list) else [])
        except Exception:
            return set()
    return set()

def save_seen(seen_set):
    try:
        with open(SEEN_PATH, "w", encoding="utf-8") as f:
            json.dump(sorted(list(seen_set)), f, indent=2)
    except Exception as e:
        print("Warning: failed to write seen_slugs.json:", e)

# -------------------------
# YC sources (JSON + site)
# -------------------------
def fetch_yc_oss_json(batch_slug):
    url = f"https://yc-oss.github.io/api/batches/{batch_slug}.json"
    print("Fetching yc-oss JSON for:", url)
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        payload = r.json()
        # payload might be a list or dict; handle both.
        companies = []
        if isinstance(payload, dict):
            # older structure: maybe 'companies' field
            if "companies" in payload and isinstance(payload["companies"], list):
                companies = payload["companies"]
            else:
                # maybe payload is already list-like under some key — be conservative
                # try to detect an inner list
                for v in payload.values():
                    if isinstance(v, list):
                        companies = v
                        break
        elif isinstance(payload, list):
            companies = payload
        # map by slug
        result = {}
        for c in companies:
            slug = c.get("slug") or c.get("name", "").lower().replace(" ", "-")
            if not slug:
                continue
            result[slug] = {
                "slug": slug,
                "name": c.get("name") or c.get("company_name") or "",
                "one_liner": c.get("one_liner") or c.get("tagline") or "",
                "url": c.get("url") or f"https://www.ycombinator.com/companies/{slug}",
                "website": c.get("website") or c.get("homepage") or ""
            }
        return result
    except Exception as e:
        print("Warning: failed to fetch yc-oss JSON:", e)
        return {}

def site_batch_string_from_slug(batch_slug):
    # Convert 'winter-2026' -> 'Winter%202026' (YC site expects capitalized season + space)
    s = (batch_slug or "").strip()
    s = s.replace("%20", " ").replace("-", " ")
    parts = s.split()
    if len(parts) >= 2:
        season = parts[0].capitalize()
        year = parts[1]
        return f"{season}%20{year}"
    return batch_slug

def scrape_yc_site(batch_slug):
    site_batch = site_batch_string_from_slug(batch_slug)
    site_url = f"https://www.ycombinator.com/companies?batch={site_batch}"
    print("Scraping YC site (static):", site_url)
    results = {}
    try:
        r = requests.get(site_url, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        anchors = soup.select('a[href*="/companies/"]')
        for a in anchors:
            href = a.get("href") or ""
            m = re.search(r'/companies/([^/?#]+)', href)
            if not m:
                continue
            slug = m.group(1).strip()
            if not slug or slug in results:
                continue
            name = a.get_text(" ", strip=True) or ""
            yc_url = href if href.startswith("http") else ("https://www.ycombinator.com" + href)
            results[slug] = {
                "slug": slug,
                "name": name,
                "one_liner": "",
                "url": yc_url,
                "website": ""
            }
        return results
    except Exception as e:
        print("Warning: static scraping failed:", e)
        return {}

def scrape_with_playwright(batch_slug, timeout=20000):
    if not _PLAYWRIGHT_AVAILABLE:
        print("Playwright not installed — skipping JS-rendered scrape.")
        return {}
    site_batch = site_batch_string_from_slug(batch_slug)
    site_url = f"https://www.ycombinator.com/companies?batch={site_batch}"
    print("Playwright rendering YC site:", site_url)
    results = {}
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(site_url, timeout=timeout)
            page.wait_for_timeout(2000)
            html = page.content()
            browser.close()
        soup = BeautifulSoup(html, "html.parser")
        anchors = soup.select('a[href*="/companies/"]')
        for a in anchors:
            href = a.get("href") or ""
            m = re.search(r'/companies/([^/?#]+)', href)
            if not m: continue
            slug = m.group(1).strip()
            if not slug or slug in results: continue
            name = a.get_text(" ", strip=True) or ""
            parent = a.parent
            one_liner = ""
            if parent:
                txt = parent.get_text(" ", strip=True)
                one_liner = txt.replace(name, "").strip()
            yc_url = href if href.startswith("http") else ("https://www.ycombinator.com" + href)
            results[slug] = {"slug": slug, "name": name, "one_liner": one_liner, "url": yc_url, "website": ""}
        return results
    except Exception as e:
        print("Playwright scraping failed:", e)
        return {}

def fetch_merged_batch(batch_slug):
    # primary: yc-oss JSON
    json_map = fetch_yc_oss_json(batch_slug)
    slug_map = dict(json_map)  # copy

    # static scrape
    site_map = scrape_yc_site(batch_slug)
    for s, comp in site_map.items():
        if s not in slug_map:
            slug_map[s] = comp

    # optionally use Playwright if static scrape failed or forced
    force = os.environ.get("FORCE_PROCESS","false").lower() == "true"
    use_pw = os.environ.get("USE_PLAYWRIGHT","false").lower() == "true"
    if (not site_map) or force or use_pw:
        pw_map = scrape_with_playwright(batch_slug)
        added = 0
        for s, comp in pw_map.items():
            if s not in slug_map:
                slug_map[s] = comp
                added += 1
        if added:
            print(f"Merged {added} entries from Playwright-rendered page")
    return slug_map

# -------------------------
# Google Sheets helpers
# -------------------------
def get_gsheet_client_from_sa_json(sa_json_str):
    # Accept either JSON content string or a path to sa.json in the runner (we write to sa.json earlier)
    try:
        from google.oauth2.service_account import Credentials
        import gspread
    except Exception as e:
        raise RuntimeError("gspread/google-auth not installed in environment: " + str(e))

    try:
        info = json.loads(sa_json_str)
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(info, scopes=scopes)
        return gspread.authorize(creds)
    except Exception as e:
        # fallback: try to read sa.json file in cwd (runner writes it)
        try:
            creds = Credentials.from_service_account_file("sa.json", scopes=scopes)
            return gspread.authorize(creds)
        except Exception as e2:
            raise RuntimeError("Failed to create gspread client: " + str(e) + " / " + str(e2))

def read_existing_sheet_slugs(gc, sheet_id):
    # returns set of slugs already in the sheet (column "slug")
    try:
        sh = gc.open_by_key(sheet_id)
        ws = sh.sheet1
        # read column index of 'slug' by header row
        header = ws.row_values(1)
        if not header:
            return set()
        # normalize header names to lower
        lower = [h.strip().lower() for h in header]
        if "slug" in lower:
            idx = lower.index("slug") + 1  # 1-based
        else:
            # default to column B if structure matches earlier expectations
            idx = 2
        vals = ws.col_values(idx)
        # exclude header
        slugs = set(v.strip() for v in vals[1:] if v.strip())
        return slugs
    except Exception as e:
        print("Warning: couldn't read sheet slugs:", e)
        return set()

def append_row_to_sheet(gc, sheet_id, row_values):
    try:
        sh = gc.open_by_key(sheet_id)
        ws = sh.sheet1
        ws.append_row(row_values, value_input_option="USER_ENTERED")
        return True
    except Exception as e:
        print("Failed to append row to sheet:", e)
        return False

# -------------------------
# Main
# -------------------------
def main():
    BATCH_SLUG = safe_env("BATCH_SLUG", "winter-2026")
    SHEET_ID = safe_env("SHEET_ID")
    GCP_SA_KEY_JSON = os.environ.get("GCP_SA_KEY_JSON")  # full JSON content
    REQUEST_DELAY = float(safe_env("REQUEST_DELAY", "0.8"))
    USE_LOCAL_PREVIEW = safe_env("USE_LOCAL_PREVIEW", "false").lower() in ("1","true","yes")

    if USE_LOCAL_PREVIEW:
        print("Using local CSV preview at", LOCAL_CSV_PREVIEW)

    seen = load_seen()
    print("Loaded seen slugs:", len(seen))

    if USE_LOCAL_PREVIEW:
        # read local CSV (for debugging only)
        rows = []
        if os.path.exists(LOCAL_CSV_PREVIEW):
            with open(LOCAL_CSV_PREVIEW, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    # ensure keys exist
                    slug = (r.get("slug") or r.get("Slug") or "").strip()
                    rows.append({
                        "slug": slug,
                        "name": r.get("company_name") or r.get("company") or r.get("name") or "",
                        "one_liner": r.get("one_liner") or r.get("tagline") or "",
                        "url": r.get("yc_url") or r.get("url") or ""
                    })
            merged_map = {r["slug"]: r for r in rows if r.get("slug")}
        else:
            print("Local CSV not found:", LOCAL_CSV_PREVIEW)
            merged_map = {}
    else:
        merged_map = fetch_merged_batch(BATCH_SLUG)

    print("Total companies obtained (merged):", len(merged_map))

    # Build set of candidates then check the sheet to avoid duplicates
    candidates = [comp for slug, comp in merged_map.items() if slug]

    # Prepare Google Sheets client (if provided) to read existing slugs
    existing_sheet_slugs = set()
    gc = None
    if SHEET_ID and GCP_SA_KEY_JSON:
        try:
            gc = get_gsheet_client_from_sa_json(GCP_SA_KEY_JSON)
            existing_sheet_slugs = read_existing_sheet_slugs(gc, SHEET_ID)
            print("Existing slugs in Google Sheet:", len(existing_sheet_slugs))
        except Exception as e:
            print("Warning: could not connect to Google Sheets:", e)
            gc = None
    else:
        print("SHEET_ID or GCP_SA_KEY_JSON missing; skipping sheet dedupe check (will rely on seen_slugs.json).")

    # Determine final-new (skip already seen AND skip already on sheet)
    final_new = []
    for comp in candidates:
        slug = comp.get("slug")
        if not slug:
            continue
        if slug in seen:
            continue
        if slug in existing_sheet_slugs:
            # already in sheet — ensure it's in seen as well to avoid reprocessing later
            seen.add(slug)
            continue
        final_new.append(comp)

    print("New companies to process (after sheet & seen dedupe):", len(final_new))
    if not final_new:
        print("No new companies. Exiting.")
        save_seen(seen)
        return

    # For each new company: scrape founders and append to sheet & collect CSV rows
    output_rows = []
    for comp in final_new:
        slug = comp.get("slug")
        name = comp.get("name") or comp.get("company_name") or ""
        website = comp.get("website") or ""
        yc_url = comp.get("url") or f"https://www.ycombinator.com/companies/{slug}"
        one_liner = comp.get("one_liner") or ""
        print("Processing:", name or slug)

        # Fetch founders JSON from the YC company page (best-effort)
        founders = []
        try:
            r = requests.get(yc_url, timeout=12)
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, "html.parser")
                linkedin_links = soup.select('a[href*="linkedin.com"]')
                seen_links = set()
                for a in linkedin_links:
                    href = a.get("href")
                    if not href or href in seen_links:
                        continue
                    seen_links.add(href)
                    # try to get nearby name text
                    name_txt = a.get_text(" ", strip=True)
                    founders.append({"name": name_txt or "", "linkedin": href})
        except Exception as e:
            print("Warning: founder fetch failed for", slug, e)

        founders_json = json.dumps(founders, ensure_ascii=False)

        row = {
            "checked_at_utc": now_iso_utc(),
            "slug": slug,
            "company_name": name,
            "website": website,
            "yc_url": yc_url,
            "company_linked": "",
            "founders_json": founders_json,
            "one_liner": one_liner
        }
        output_rows.append(row)

        # append to sheet (if available)
        appended = False
        if gc and SHEET_ID:
            try:
                values = [row[h] for h in SHEET_HEADER]
                success = append_row_to_sheet(gc, SHEET_ID, values)
                appended = bool(success)
            except Exception as e:
                print("Append exception for", slug, e)
                appended = False
        else:
            # no sheet client — skip append, but still write CSV and mark seen locally
            appended = True

        if appended:
            print("Appended:", slug)
            seen.add(slug)
            # also update existing_sheet_slugs to prevent duplicates within same run
            existing_sheet_slugs.add(slug)
        else:
            print("Failed to append for", slug, " — will not mark as seen")

        time.sleep(REQUEST_DELAY)

    # write CSV with canonical header order
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join(WORKDIR, f"yc_new_{BATCH_SLUG}_{ts}.csv")
    try:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=SHEET_HEADER)
            writer.writeheader()
            for r in output_rows:
                writer.writerow(r)
        print("Wrote CSV:", csv_path)
    except Exception as e:
        print("Failed to write CSV:", e)

    # persist seen
    save_seen(seen)
    print("Updated seen_slugs.json with", len(seen), "entries")

if __name__ == "__main__":
    main()
