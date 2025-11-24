#!/usr/bin/env python3
"""
YC Daily Tracker – full version with:
- yc-oss JSON source
- website scraper
- Playwright JS-render fallback
- Google Sheet append
- seen_slugs.json state tracking
"""

import os
import json
import csv
import time
import requests
import re
from datetime import datetime
from bs4 import BeautifulSoup

# Try to import Playwright
try:
    from playwright.sync_api import sync_playwright
    _PLAYWRIGHT_AVAILABLE = True
except Exception:
    _PLAYWRIGHT_AVAILABLE = False


# -------------------------------
# Helpers
# -------------------------------

def site_batch_string_from_slug(batch_slug):
    """
    Convert 'winter-2026' → 'Winter%202026'
    """
    season, year = batch_slug.split("-")
    return season.capitalize() + "%20" + year


def load_seen_slugs(path="seen_slugs.json"):
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def save_seen_slugs(data, path="seen_slugs.json"):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


# -------------------------------
# Fetch from yc-oss JSON
# -------------------------------

def fetch_yc_oss_json(batch_slug):
    base_url = f"https://yc-oss.github.io/api/batches/{batch_slug}.json"
    print(f"Fetching yc-oss JSON from: {base_url}")
    try:
        r = requests.get(base_url, timeout=15)
        if r.status_code != 200:
            print("YC-OSS JSON returned status:", r.status_code)
            return {}
        data = r.json()
        result = {}
        for c in data.get("companies", []):
            slug = c.get("slug")
            if slug:
                result[slug] = {
                    "slug": slug,
                    "name": c.get("name", ""),
                    "one_liner": c.get("one_liner", ""),
                    "url": f"https://www.ycombinator.com/companies/{slug}"
                }
        return result
    except Exception as e:
        print("Error fetching yc-oss JSON:", e)
        return {}


# -------------------------------
# Static scraper (non-JS)
# -------------------------------

def scrape_yc_site(batch_slug):
    site_batch = site_batch_string_from_slug(batch_slug)
    url = f"https://www.ycombinator.com/companies?batch={site_batch}"
    print("Scraping YC site:", url)
    results = {}
    try:
        resp = requests.get(url, timeout=15)
        soup = BeautifulSoup(resp.text, "html.parser")
        anchors = soup.select('a[href*="/companies/"]')
        for a in anchors:
            href = a.get('href') or ""
            m = re.search(r'/companies/([^/?#]+)', href)
            if not m:
                continue
            slug = m.group(1).strip()
            if slug not in results:
                name = a.get_text(" ", strip=True) or ""
                results[slug] = {"slug": slug, "name": name, "url": href}
        return results
    except Exception as e:
        print("Error scraping site:", e)
        return {}


# -------------------------------
# Playwright fallback
# -------------------------------

def scrape_site_with_playwright(batch_slug, timeout=20000):
    if not _PLAYWRIGHT_AVAILABLE:
        print("Playwright not available – skipping fallback.")
        return {}

    site_batch = site_batch_string_from_slug(batch_slug)
    url = f"https://www.ycombinator.com/companies?batch={site_batch}"
    print("Playwright rendering:", url)
    results = {}

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url, timeout=timeout)
            page.wait_for_timeout(2000)
            html = page.content()
            browser.close()

        soup = BeautifulSoup(html, "html.parser")
        anchors = soup.select('a[href*="/companies/"]')

        for a in anchors:
            href = a.get("href") or ""
            m = re.search(r'/companies/([^/?#]+)', href)
            if not m:
                continue
            slug = m.group(1).strip()
            if slug in results:
                continue

            name = a.get_text(" ", strip=True) or ""
            parent = a.parent
            one_liner = ""
            if parent:
                txt = parent.get_text(" ", strip=True)
                one_liner = txt.replace(name, "").strip()

            full_url = href if href.startswith("http") else "https://www.ycombinator.com" + href

            results[slug] = {
                "slug": slug,
                "name": name,
                "one_liner": one_liner,
                "url": full_url
            }

        return results

    except Exception as e:
        print("Playwright fallback failed:", e)
        return {}


# -------------------------------
# Combine sources
# -------------------------------

def fetch_combined_batch(batch_slug):
    slug_map = fetch_yc_oss_json(batch_slug)

    # static site scrape
    site_map = scrape_yc_site(batch_slug)
    for slug, comp in site_map.items():
        if slug not in slug_map:
            slug_map[slug] = comp

    # Playwright fallback logic
    force = os.environ.get("FORCE_PROCESS", "false").lower() == "true"
    use_pw = os.environ.get("USE_PLAYWRIGHT", "false").lower() == "true"

    if (not site_map) or force or use_pw:
        if _PLAYWRIGHT_AVAILABLE:
            pw_map = scrape_site_with_playwright(batch_slug)
            added = 0
            for slug, comp in pw_map.items():
                if slug not in slug_map:
                    slug_map[slug] = comp
                    added += 1
            if added:
                print(f"Merged {added} companies from Playwright render")
        else:
            print("Playwright not available for fallback.")

    return slug_map


# -------------------------------
# Google Sheets append
# -------------------------------

def append_to_sheet(row):
    try:
        import gspread
        from google.oauth2.service_account import Credentials

        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_file("sa.json", scopes=scopes)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(os.environ["SHEET_ID"]).sheet1

        sheet.append_row(row, value_input_option="RAW")
        return True
    except Exception as e:
        print("Failed to append to sheet:", e)
        return False


# -------------------------------
# Generate row for Google Sheet
# -------------------------------

def build_row(c):
    return [
        datetime.utcnow().isoformat(),
        c.get("name", ""),
        c.get("slug", ""),
        c.get("one_liner", ""),
        c.get("url", "")
    ]


# -------------------------------
# Main
# -------------------------------

def main():
    batch_slug = os.environ.get("BATCH_SLUG")
    delay = float(os.environ.get("REQUEST_DELAY", "0.5"))
    seen = load_seen_slugs()

    print("Loaded seen slugs:", len(seen))

    merged = fetch_combined_batch(batch_slug)
    print("Total companies after merge:", len(merged))

    new = [c for s, c in merged.items() if s not in seen]
    print("New companies to process:", len(new))

    # Write CSV output
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    os.makedirs("results", exist_ok=True)
    csv_path = f"results/yc_new_{batch_slug}_{ts}.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["timestamp", "name", "slug", "one_liner", "url"])
        for c in new:
            w.writerow(build_row(c))
    print("Wrote CSV:", csv_path)

    # Append to sheet
    for c in new:
        row = build_row(c)
        time.sleep(delay)
        append_to_sheet(row)
        seen[c["slug"]] = True

    save_seen_slugs(seen)
    print(f"Updated seen_slugs.json with {len(seen)} entries")


if __name__ == "__main__":
    main()
