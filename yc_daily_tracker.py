#!/usr/bin/env python3
"""
yc_daily_tracker.py — with UPSERT behavior:
- fetch yc-oss JSON + site scrape + Playwright fallback
- enrich company pages (name, website, one_liner, founders)
- if slug exists in Google Sheet -> UPDATE that row with enriched data (upsert)
- else -> APPEND
- only mark slug as seen after successful upsert/append
"""

import os
import json
import csv
import time
import requests
import re
from datetime import datetime, timezone
from bs4 import BeautifulSoup

# Try Playwright
try:
    from playwright.sync_api import sync_playwright
    _PLAYWRIGHT_AVAILABLE = True
except Exception:
    _PLAYWRIGHT_AVAILABLE = False

# Local preview CSV
LOCAL_CSV_PREVIEW = "/mnt/data/yc_winter2025_sample.csv"
WORKDIR = "results"
os.makedirs(WORKDIR, exist_ok=True)

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

SEEN_PATH = "seen_slugs.json"

# ---------- helpers ----------
def now_iso_utc():
    return datetime.now(timezone.utc).isoformat()

def safe_env(k, default=None):
    v = os.environ.get(k)
    return v.strip() if isinstance(v, str) else default

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

# ---------- YC sources ----------
def site_batch_string_from_slug(batch_slug):
    s = (batch_slug or "").strip()
    s = s.replace("%20", " ").replace("-", " ")
    parts = s.split()
    if len(parts) >= 2:
        season = parts[0].capitalize()
        year = parts[1]
        return f"{season}%20{year}"
    return batch_slug

def fetch_yc_oss_json(batch_slug):
    url = f"https://yc-oss.github.io/api/batches/{batch_slug}.json"
    print("Fetching yc-oss JSON from:", url)
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        payload = r.json()
        companies = []
        if isinstance(payload, dict):
            if "companies" in payload and isinstance(payload["companies"], list):
                companies = payload["companies"]
            else:
                for v in payload.values():
                    if isinstance(v, list):
                        companies = v
                        break
        elif isinstance(payload, list):
            companies = payload
        out = {}
        for c in companies:
            slug = c.get("slug") or (c.get("name","").lower().replace(" ", "-"))
            if not slug: continue
            out[slug] = {
                "slug": slug,
                "name": c.get("name") or "",
                "one_liner": c.get("one_liner") or c.get("tagline") or "",
                "url": c.get("url") or f"https://www.ycombinator.com/companies/{slug}",
                "website": c.get("website") or c.get("homepage") or ""
            }
        return out
    except Exception as e:
        print("Warning: failed to fetch yc-oss JSON:", e)
        return {}

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
            full = href if href.startswith("http") else ("https://www.ycombinator.com" + href)
            results[slug] = {"slug": slug, "name": name, "one_liner": "", "url": full, "website": ""}
        return results
    except Exception as e:
        print("Warning: static scraping failed:", e)
        return {}

def scrape_with_playwright(batch_slug, timeout=20000):
    if not _PLAYWRIGHT_AVAILABLE:
        print("Playwright not available — skipping.")
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
            if not m:
                continue
            slug = m.group(1).strip()
            if not slug or slug in results:
                continue
            name = a.get_text(" ", strip=True) or ""
            parent = a.parent
            one_liner = ""
            if parent:
                txt = parent.get_text(" ", strip=True)
                one_liner = txt.replace(name, "").strip()
            full = href if href.startswith("http") else ("https://www.ycombinator.com" + href)
            results[slug] = {"slug": slug, "name": name, "one_liner": one_liner, "url": full, "website": ""}
        return results
    except Exception as e:
        print("Playwright fallback failed:", e)
        return {}

def fetch_merged_batch(batch_slug):
    json_map = fetch_yc_oss_json(batch_slug)
    slug_map = dict(json_map)
    site_map = scrape_yc_site(batch_slug)
    for s, comp in site_map.items():
        if s not in slug_map:
            slug_map[s] = comp
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

# ---------- enrichment ----------
def enrich_from_yc_company_page(comp):
    slug = comp.get("slug")
    yc_url = comp.get("url") or f"https://www.ycombinator.com/companies/{slug}"
    headers = {"User-Agent": "YC-Daily-Tracker/1.0"}
    try:
        r = requests.get(yc_url, headers=headers, timeout=12)
        if r.status_code != 200:
            return comp
        soup = BeautifulSoup(r.text, "html.parser")
        og_title = soup.find("meta", property="og:title")
        if og_title and og_title.get("content"):
            comp["name"] = og_title.get("content").strip()
        else:
            h1 = soup.find(["h1","h2"])
            if h1 and h1.get_text(strip=True):
                comp["name"] = h1.get_text(" ", strip=True)
        meta_desc = soup.find("meta", attrs={"name":"description"}) or soup.find("meta", property="og:description")
        if meta_desc and meta_desc.get("content"):
            comp["one_liner"] = meta_desc.get("content").strip()
        else:
            p = soup.find("p")
            if p and p.get_text(strip=True): comp["one_liner"] = p.get_text(" ", strip=True)[:300]
        found_website = comp.get("website") or ""
        if not found_website:
            anchors = soup.find_all("a", href=True)
            for a in anchors:
                href = a["href"]
                if "ycombinator.com" in href or "linkedin.com" in href: continue
                if href.startswith("mailto:") or href.startswith("#"): continue
                if href.startswith("http"):
                    found_website = href
                    break
            if not found_website:
                can = soup.find("link", rel="canonical")
                if can and can.get("href") and "ycombinator.com" not in can.get("href"):
                    found_website = can.get("href")
        comp["website"] = found_website or comp.get("website","")
        founders = []
        linkedin_anchors = soup.select('a[href*="linkedin.com"]')
        seen_links = set()
        for a in linkedin_anchors:
            href = a.get("href")
            if not href or href in seen_links: continue
            seen_links.add(href)
            name_guess = a.get_text(" ", strip=True)
            node = a
            for _ in range(3):
                node = node.parent
                if node is None: break
                txt = node.get_text(" ", strip=True)
                if txt and len(txt.split()) < 8 and len(txt) < 120:
                    name_guess = txt.strip()
                    break
            founders.append({"name": name_guess or "", "linkedin": href})
        unique = []
        seenf = set()
        for f in founders:
            key = f.get("linkedin")
            if key in seenf: continue
            seenf.add(key)
            unique.append(f)
        comp["founders_json"] = json.dumps(unique, ensure_ascii=False)
        return comp
    except Exception as e:
        print("Warning: enrichment failed for", slug, e)
        return comp

# ---------- Google Sheets (upsert) ----------
def get_gsheet_client_from_sa_json(sa_json_str):
    try:
        from google.oauth2.service_account import Credentials
        import gspread
    except Exception as e:
        raise RuntimeError("gspread/google-auth not installed: " + str(e))
    try:
        info = json.loads(sa_json_str)
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(info, scopes=scopes)
        return __import__("gspread").authorize(creds)
    except Exception as e:
        try:
            from google.oauth2.service_account import Credentials as C2
            import gspread as g2
            creds = C2.from_service_account_file("sa.json", scopes=scopes)
            return g2.authorize(creds)
        except Exception as e2:
            raise RuntimeError("Failed building gspread client: " + str(e) + " / " + str(e2))

def read_existing_sheet_slugs_and_row(gc, sheet_id):
    """
    Returns:
      - slugs_set: set of slugs
      - slug_to_row: dict slug -> row_index (1-based)
    """
    try:
        sh = gc.open_by_key(sheet_id)
        ws = sh.sheet1
        header = ws.row_values(1)
        if not header:
            return set(), {}
        lower = [h.strip().lower() for h in header]
        if "slug" in lower:
            idx = lower.index("slug") + 1
        else:
            idx = 2
        vals = ws.col_values(idx)
        # vals includes header at index 0
        slugs = {}
        for i, v in enumerate(vals[1:], start=2):
            s = v.strip()
            if s:
                slugs[s] = i
        return set(slugs.keys()), slugs
    except Exception as e:
        print("Warning: couldn't read sheet slugs:", e)
        return set(), {}

def update_sheet_row_by_index(gc, sheet_id, row_index, values):
    """
    Overwrite the row at row_index with provided values (list) matching SHEET_HEADER.
    """
    try:
        sh = gc.open_by_key(sheet_id)
        ws = sh.sheet1
        # build A1 range for the row, starting at column A
        start_col = "A"
        end_col = chr(ord("A") + len(values) - 1)
        rng = f"{start_col}{row_index}:{end_col}{row_index}"
        ws.update(rng, [values])
        return True
    except Exception as e:
        print("Failed to update row", row_index, ":", e)
        return False

def append_row_to_sheet(gc, sheet_id, values):
    try:
        sh = gc.open_by_key(sheet_id)
        ws = sh.sheet1
        ws.append_row(values, value_input_option="USER_ENTERED")
        return True
    except Exception as e:
        print("Failed to append row to sheet:", e)
        return False

# ---------- main ----------
def main():
    BATCH_SLUG = safe_env("BATCH_SLUG", "winter-2026")
    SHEET_ID = safe_env("SHEET_ID")
    GCP_SA_KEY_JSON = os.environ.get("GCP_SA_KEY_JSON")
    REQUEST_DELAY = float(safe_env("REQUEST_DELAY", "0.8"))
    USE_LOCAL_PREVIEW = safe_env("USE_LOCAL_PREVIEW", "false").lower() in ("1","true","yes")

    seen = load_seen()
    print("Loaded seen slugs:", len(seen))

    if USE_LOCAL_PREVIEW:
        merged_map = {}
        if os.path.exists(LOCAL_CSV_PREVIEW):
            with open(LOCAL_CSV_PREVIEW, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    slug = (r.get("slug") or "").strip()
                    if not slug: continue
                    merged_map[slug] = {
                        "slug": slug,
                        "name": r.get("company_name") or r.get("company") or "",
                        "one_liner": r.get("one_liner") or "",
                        "url": r.get("yc_url") or f"https://www.ycombinator.com/companies/{slug}",
                        "website": r.get("website") or ""
                    }
        else:
            print("Local CSV not found:", LOCAL_CSV_PREVIEW)
            merged_map = {}
    else:
        merged_map = fetch_merged_batch(BATCH_SLUG)

    print("Total companies obtained (merged):", len(merged_map))

    candidates = [comp for slug, comp in merged_map.items() if slug]

    existing_sheet_slugs = set()
    slug_to_row = {}
    gc = None
    if SHEET_ID and GCP_SA_KEY_JSON:
        try:
            gc = get_gsheet_client_from_sa_json(GCP_SA_KEY_JSON)
            existing_sheet_slugs, slug_to_row = read_existing_sheet_slugs_and_row(gc, SHEET_ID)
            print("Existing slugs in sheet:", len(existing_sheet_slugs))
        except Exception as e:
            print("Warning: could not connect to Google Sheets:", e)
            gc = None
    else:
        print("SHEET_ID or GCP_SA_KEY_JSON missing; sheet dedupe skipped.")

    # Build final list to process: those not in seen OR we want to force update for some reason
    # Note: we will upsert if slug exists in sheet (update row), otherwise append.
    final_candidates = []
    for comp in candidates:
        slug = comp.get("slug")
        if not slug: continue
        if slug in seen:
            # Skip, but if sheet has an entry that looks malformed you could still force update via FORCE_PROCESS
            continue
        final_candidates.append(comp)

    print("Candidates after seen dedupe:", len(final_candidates))

    if not final_candidates:
        print("No new companies to process (after seen dedupe). Exiting.")
        save_seen(seen)
        return

    output_rows = []
    for comp in final_candidates:
        slug = comp.get("slug")
        print("Processing:", slug)
        comp = enrich_from_yc_company_page(comp)
        name = comp.get("name") or ""
        website = comp.get("website") or ""
        yc_url = comp.get("url") or f"https://www.ycombinator.com/companies/{slug}"
        one_liner = comp.get("one_liner") or ""
        founders_json = comp.get("founders_json") or "[]"

        row_obj = {
            "checked_at_utc": now_iso_utc(),
            "slug": slug,
            "company_name": name,
            "website": website,
            "yc_url": yc_url,
            "company_linked": "",
            "founders_json": founders_json,
            "one_liner": one_liner
        }

        # prepare values in header order
        values = [row_obj[h] for h in SHEET_HEADER]

        upserted = False
        if gc and SHEET_ID:
            try:
                if slug in existing_sheet_slugs and slug in slug_to_row:
                    row_index = slug_to_row[slug]
                    ok = update_sheet_row_by_index(gc, SHEET_ID, row_index, values)
                    if ok:
                        upserted = True
                        print("Updated sheet row", row_index, "for", slug)
                else:
                    ok = append_row_to_sheet(gc, SHEET_ID, values)
                    if ok:
                        upserted = True
                        print("Appended new row for", slug)
            except Exception as e:
                print("Sheet upsert error for", slug, e)
                upserted = False
        else:
            # If no sheet available, treat as appended (local debug)
            upserted = True

        if upserted:
            seen.add(slug)
            existing_sheet_slugs.add(slug)
            # if appended, we don't know the row index; it's fine
        else:
            print("Failed to upsert for", slug, "— will not mark as seen")

        output_rows.append(row_obj)
        time.sleep(REQUEST_DELAY)

    # write canonical CSV
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

    save_seen(seen)
    print("Updated seen_slugs.json with", len(seen), "entries")

if __name__ == "__main__":
    main()
