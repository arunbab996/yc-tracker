#!/usr/bin/env python3
"""
yc_daily_tracker.py — UPSERT + improved website heuristics

Features:
- Fetch yc-oss JSON, static scrape of YC companies page, Playwright JS-render fallback
- Enrich company pages to extract: company_name, website (better heuristics), one_liner, founders_json
- UPSERT behavior: update existing sheet row if slug exists, otherwise append
- Mark slugs as seen only after successful upsert/append
- Writes canonical CSV snapshot to results/
"""

import os
import json
import csv
import time
import requests
import re
from datetime import datetime, timezone
from bs4 import BeautifulSoup

# Playwright optional import
try:
    from playwright.sync_api import sync_playwright
    _PLAYWRIGHT_AVAILABLE = True
except Exception:
    _PLAYWRIGHT_AVAILABLE = False

# Local preview CSV path (for offline testing, optional)
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
    "one_liner",
]

SEEN_PATH = "seen_slugs.json"


# ---------- utilities ----------

def now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_env(k: str, default=None):
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


# ---------- date column config (for preserving first-seen date) ----------

DATE_COL_NAME = "checked_at_utc"

try:
    DATE_COL_INDEX = SHEET_HEADER.index(DATE_COL_NAME)
except ValueError:
    # If header changes and we lose this column, fall back to last column
    DATE_COL_INDEX = len(SHEET_HEADER) - 1


# ---------- YC sources ----------

def site_batch_string_from_slug(batch_slug: str) -> str:
    s = (batch_slug or "").strip()
    s = s.replace("%20", " ").replace("-", " ")
    parts = s.split()
    if len(parts) >= 2:
        season = parts[0].capitalize()
        year = parts[1]
        return f"{season}%20{year}"
    return batch_slug


def fetch_yc_oss_json(batch_slug: str):
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
            slug = c.get("slug") or (c.get("name", "").lower().replace(" ", "-"))
            if not slug:
                continue
            out[slug] = {
                "slug": slug,
                "name": c.get("name") or "",
                "one_liner": c.get("one_liner") or c.get("tagline") or "",
                "url": c.get("url") or f"https://www.ycombinator.com/companies/{slug}",
                "website": c.get("website") or c.get("homepage") or "",
            }
        return out
    except Exception as e:
        print("Warning: failed to fetch yc-oss JSON:", e)
        return {}


def scrape_yc_site(batch_slug: str):
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
            m = re.search(r"/companies/([^/?#]+)", href)
            if not m:
                continue
            slug = m.group(1).strip()
            if not slug or slug in results:
                continue
            name = a.get_text(" ", strip=True) or ""
            full = href if href.startswith("http") else ("https://www.ycombinator.com" + href)
            results[slug] = {
                "slug": slug,
                "name": name,
                "one_liner": "",
                "url": full,
                "website": "",
            }
        return results
    except Exception as e:
        print("Warning: static scraping failed:", e)
        return {}


def scrape_with_playwright(batch_slug: str, timeout: int = 20000):
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
            m = re.search(r"/companies/([^/?#]+)", href)
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
            results[slug] = {
                "slug": slug,
                "name": name,
                "one_liner": one_liner,
                "url": full,
                "website": "",
            }
        return results
    except Exception as e:
        print("Playwright fallback failed:", e)
        return {}


def fetch_merged_batch(batch_slug: str):
    json_map = fetch_yc_oss_json(batch_slug)
    slug_map = dict(json_map)

    site_map = scrape_yc_site(batch_slug)
    for s, comp in site_map.items():
        if s not in slug_map:
            slug_map[s] = comp

    force = os.environ.get("FORCE_PROCESS", "false").lower() == "true"
    use_pw = os.environ.get("USE_PLAYWRIGHT", "false").lower() == "true"

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


# ---------- enrichment with improved website heuristic ----------

def enrich_from_yc_company_page(comp: dict) -> dict:
    slug = comp.get("slug")
    yc_url = comp.get("url") or f"https://www.ycombinator.com/companies/{slug}"
    headers = {"User-Agent": "YC-Daily-Tracker/1.0"}
    try:
        r = requests.get(yc_url, headers=headers, timeout=12)
        if r.status_code != 200:
            return comp

        html = r.text
        soup = BeautifulSoup(html, "html.parser")

        # 1) company name: og:title -> h1
        og_title = soup.find("meta", property="og:title")
        if og_title and og_title.get("content"):
            comp["name"] = og_title.get("content").strip()
        else:
            h1 = soup.find(["h1", "h2"])
            if h1 and h1.get_text(strip=True):
                comp["name"] = h1.get_text(" ", strip=True)

        # 2) one_liner: meta description or short p
        meta_desc = soup.find("meta", attrs={"name": "description"}) or soup.find(
            "meta", property="og:description"
        )
        if meta_desc and meta_desc.get("content"):
            comp["one_liner"] = meta_desc.get("content").strip()
        else:
            p = soup.find("p")
            if p and p.get_text(strip=True):
                comp["one_liner"] = p.get_text(" ", strip=True)[:300]

        # 3) website: improved heuristic to avoid startupschool / tracking links
        found_website = comp.get("website") or ""
        if not found_website:
            anchors = soup.find_all("a", href=True)
            candidates = []
            blacklist_domains = (
                "ycombinator.com",
                "linkedin.com",
                "twitter.com",
                "facebook.com",
                "instagram.com",
                "startupschool.org",
            )
            for a in anchors:
                href = a["href"].strip()
                if not href:
                    continue
                if href.startswith("mailto:") or href.startswith("#"):
                    continue
                if href.startswith("//"):
                    href = "https:" + href
                if not href.startswith("http"):
                    continue

                is_low = False
                for d in blacklist_domains:
                    if d in href:
                        is_low = True
                        break
                if is_low:
                    candidates.append(("low", href))
                else:
                    candidates.append(("high", href))

            chosen = None
            slug_token = (slug or "").lower()
            name = (comp.get("name") or "").lower()
            name_tokens = [t for t in re.split(r"\W+", name) if len(t) > 2]

            # prefer high links that contain slug or name tokens
            for typ, href in candidates:
                if typ != "high":
                    continue
                if slug_token and slug_token in href:
                    chosen = href
                    break
                for t in name_tokens:
                    if t and (t in href):
                        chosen = href
                        break
                if chosen:
                    break

            if not chosen:
                # pick first high if available
                for typ, href in candidates:
                    if typ == "high":
                        chosen = href
                        break

            if not chosen:
                # fallback pick a low candidate which is not startupschool tracking or utm-heavy link
                for typ, href in candidates:
                    if typ == "low" and "startupschool.org" not in href and "utm_" not in href:
                        chosen = href
                        break

            if not chosen:
                # last resort: canonical link
                can = soup.find("link", rel="canonical")
                if can and can.get("href") and "ycombinator.com" not in can.get("href"):
                    chosen = can.get("href")
            found_website = chosen or ""

        comp["website"] = found_website or comp.get("website", "")

        # 4) founders_json: find linkedin anchors and nearby text
        founders = []
        linkedin_anchors = soup.select('a[href*="linkedin.com"]')
        seen_links = set()
        for a in linkedin_anchors:
            href = a.get("href")
            if not href or href in seen_links:
                continue
            seen_links.add(href)
            name_guess = a.get_text(" ", strip=True)
            node = a
            for _ in range(3):
                node = node.parent
                if node is None:
                    break
                txt = node.get_text(" ", strip=True)
                if txt and len(txt.split()) < 8 and len(txt) < 120:
                    name_guess = txt.strip()
                    break
            founders.append({"name": name_guess or "", "linkedin": href})

        # dedupe
        unique = []
        seenf = set()
        for f in founders:
            key = f.get("linkedin")
            if key in seenf:
                continue
            seenf.add(key)
            unique.append(f)

        comp["founders_json"] = json.dumps(unique, ensure_ascii=False)
        return comp
    except Exception as e:
        print("Warning: enrichment failed for", slug, e)
        return comp


# ---------- Google Sheets upsert helpers ----------

def get_gsheet_client_from_sa_json(sa_json_str: str):
    try:
        from google.oauth2.service_account import Credentials
        import gspread
    except Exception as e:
        raise RuntimeError("gspread/google-auth not installed: " + str(e))

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    try:
        info = json.loads(sa_json_str)
        creds = Credentials.from_service_account_info(info, scopes=scopes)
        return __import__("gspread").authorize(creds)
    except Exception as e:
        # fallback: try sa.json file
        try:
            from google.oauth2.service_account import Credentials as C2
            import gspread as g2

            creds = C2.from_service_account_file("sa.json", scopes=scopes)
            return g2.authorize(creds)
        except Exception as e2:
            raise RuntimeError("Failed building gspread client: " + str(e) + " / " + str(e2))


def read_existing_sheet_slugs_and_row(gc, sheet_id):
    try:
        sh = gc.open_by_key(sheet_id)
        ws = sh.sheet1
        header = ws.row_values(1)
        if not header:
            return set(), {}
        lower = [h.strip().lower() for h in header]
        if "slug" in lower:
            idx = lower.index("slug") + 1  # 1-based col index
        else:
            idx = 2
        vals = ws.col_values(idx)
        slugs = {}
        for i, v in enumerate(vals[1:], start=2):  # skip header row
            s = v.strip()
            if s:
                slugs[s] = i
        return set(slugs.keys()), slugs
    except Exception as e:
        print("Warning: couldn't read sheet slugs:", e)
        return set(), {}


def get_worksheet(gc, sheet_id):
    sh = gc.open_by_key(sheet_id)
    ws = sh.sheet1
    return ws


def update_sheet_row_by_index(gc, sheet_id, row_index, values):
    """
    Update an existing row in the sheet.

    IMPORTANT: we preserve the existing date in DATE_COL_INDEX,
    so we don't overwrite the original 'first seen' timestamp.
    """
    try:
        ws = get_worksheet(gc, sheet_id)

        if len(values) < len(SHEET_HEADER):
            values = values + [""] * (len(SHEET_HEADER) - len(values))

        # read existing row to keep original date
        try:
            existing_row = ws.row_values(row_index)
        except Exception as e:
            print(f"Failed to read existing row {row_index} for date preservation:", e)
            existing_row = []

        if (
            existing_row
            and len(existing_row) > DATE_COL_INDEX
            and existing_row[DATE_COL_INDEX].strip()
        ):
            # keep original date
            values[DATE_COL_INDEX] = existing_row[DATE_COL_INDEX]
        else:
            # if no existing date, ensure it's set
            if not values[DATE_COL_INDEX]:
                values[DATE_COL_INDEX] = now_iso_utc()

        start_col = "A"
        end_col = chr(ord("A") + len(values) - 1)
        rng = f"{start_col}{row_index}:{end_col}{row_index}"
        ws.update(rng, [values])
        return True
    except Exception as e:
        print("Failed to update row", row_index, ":", e)
        return False


def append_row_to_sheet(gc, sheet_id, values):
    """
    Append a new row to the sheet.

    For NEW companies, we set the date column to now if not already set.
    """
    try:
        ws = get_worksheet(gc, sheet_id)

        if len(values) < len(SHEET_HEADER):
            values = values + [""] * (len(SHEET_HEADER) - len(values))

        if not values[DATE_COL_INDEX]:
            values[DATE_COL_INDEX] = now_iso_utc()

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
    USE_LOCAL_PREVIEW = safe_env("USE_LOCAL_PREVIEW", "false").lower() in ("1", "true", "yes")

    seen = load_seen()
    print("Loaded seen slugs:", len(seen))

    # source: either local CSV preview or live YC sources
    if USE_LOCAL_PREVIEW and os.path.exists(LOCAL_CSV_PREVIEW):
        merged_map = {}
        with open(LOCAL_CSV_PREVIEW, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                slug = (r.get("slug") or "").strip()
                if not slug:
                    continue
                merged_map[slug] = {
                    "slug": slug,
                    "name": r.get("company_name") or r.get("company") or "",
                    "one_liner": r.get("one_liner") or "",
                    "url": r.get("yc_url") or f"https://www.ycombinator.com/companies/{slug}",
                    "website": r.get("website") or "",
                }
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

    # Decide which to process: skip seen; we upsert only for new (not seen)
    final_candidates = []
    for comp in candidates:
        slug = comp.get("slug")
        if not slug:
            continue
        if slug in seen:
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
            "one_liner": one_liner,
        }

        values = [row_obj[h] for h in SHEET_HEADER]

        upserted = False
        if gc and SHEET_ID:
            try:
                if slug in existing_sheet_slugs and slug in slug_to_row:
                    # Existing company: update row but KEEP original date
                    row_index = slug_to_row[slug]
                    ok = update_sheet_row_by_index(gc, SHEET_ID, row_index, values)
                    if ok:
                        upserted = True
                        print("Updated sheet row", row_index, "for", slug)
                else:
                    # New company: append row, date set to now
                    ok = append_row_to_sheet(gc, SHEET_ID, values)
                    if ok:
                        upserted = True
                        print("Appended new row for", slug)
            except Exception as e:
                print("Sheet upsert error for", slug, e)
                upserted = False
        else:
            # No Google Sheets configured -> do NOT mark as upserted.
            # This prevents us from adding slugs to seen_slugs.json without
            # actually writing them to the sheet.
            print(
                "Google Sheets not configured (gc or SHEET_ID missing) – "
                f"skipping sheet write for {slug}"
            )
            upserted = False

        if upserted:
            seen.add(slug)
            existing_sheet_slugs.add(slug)
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
