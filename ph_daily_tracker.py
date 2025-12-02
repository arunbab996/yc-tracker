#!/usr/bin/env python3
"""
ph_daily_tracker.py — Fetch top Product Hunt posts daily and upsert into a
dedicated Google Sheet tab ("Product Hunt Data") without touching YC data.

Behavior:
- Uses Product Hunt API (v2 GraphQL) to fetch top posts.
- Upserts into a Google Sheet tab:
    - New posts -> appended with first_seen_at = now (UTC).
    - Existing posts -> fields updated, but first_seen_at is preserved.
- Uses ph_seen_ids.json to avoid reprocessing posts in normal daily runs.
- Writes a canonical CSV snapshot into results/.
"""

import os
import json
import csv
import time
from datetime import datetime, timezone

import requests


# ---------- CONFIG ----------

WORKDIR = "results"
os.makedirs(WORKDIR, exist_ok=True)

# Sheet structure for the "Product Hunt Data" tab
SHEET_HEADER = [
    "first_seen_at",
    "ph_id",
    "name",
    "tagline",
    "website_url",
    "ph_url",
    "logo_url",
]

SEEN_PATH = "ph_seen_ids.json"

DATE_COL_NAME = "first_seen_at"
try:
    DATE_COL_INDEX = SHEET_HEADER.index(DATE_COL_NAME)
except ValueError:
    DATE_COL_INDEX = 0  # safest fallback: first column

# Worksheet (tab) name for Product Hunt data.
# This keeps PH data isolated from YC tab.
DEFAULT_PH_WORKSHEET_NAME = "Product Hunt Data"


# ---------- UTILS ----------

def now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_env(key: str, default=None):
    val = os.environ.get(key)
    return val.strip() if isinstance(val, str) else default


def load_seen():
    if os.path.exists(SEEN_PATH):
        try:
            with open(SEEN_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                return set(str(x) for x in data)
        except Exception as e:
            print("Warning: failed to load ph_seen_ids.json:", e)
    return set()


def save_seen(seen_set):
    try:
        with open(SEEN_PATH, "w", encoding="utf-8") as f:
            json.dump(sorted(list(seen_set)), f, indent=2)
    except Exception as e:
        print("Warning: failed to save ph_seen_ids.json:", e)


# ---------- PRODUCT HUNT API ----------

PH_API_URL = "https://api.producthunt.com/v2/api/graphql"

PH_TOP_POSTS_QUERY = """
query TopPosts($first: Int!) {
  posts(order: RANKING, first: $first) {
    edges {
      node {
        id
        name
        slug
        tagline
        website
        url
        featuredAt
        thumbnail {
          url
        }
      }
    }
  }
}
"""


def fetch_top_posts(limit: int, token: str):
    """
    Fetch top Product Hunt posts via v2 GraphQL API.
    Requires PRODUCT_HUNT_TOKEN (Bearer token).
    """
    if not token:
        raise RuntimeError("PRODUCT_HUNT_TOKEN not provided.")

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }

    payload = {
        "query": PH_TOP_POSTS_QUERY,
        "variables": {"first": limit},
    }

    print(f"Fetching top {limit} posts from Product Hunt...")
    resp = requests.post(PH_API_URL, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    posts = []
    edges = (
        data.get("data", {})
        .get("posts", {})
        .get("edges", [])
    )

    for edge in edges:
        node = edge.get("node") or {}
        pid = str(node.get("id", "")).strip()
        if not pid:
            continue

        name = node.get("name") or ""
        slug = node.get("slug") or ""
        tagline = node.get("tagline") or ""
        website = node.get("website") or ""
        url = node.get("url") or ""  # often the PH page
        featured_at = node.get("featuredAt") or ""

        # logo/thumbnail
        thumb = node.get("thumbnail") or {}
        logo_url = thumb.get("url") or ""

        # Decide website_url vs ph_url:
        # - website_url: primary product website
        # - ph_url: Product Hunt listing
        website_url = website or ""
        if not website_url and url:
            # If we only have PH URL, we'll still store it as website_url
            website_url = url

        if slug:
            ph_url = f"https://www.producthunt.com/posts/{slug}"
        else:
            ph_url = url or ""

        posts.append(
            {
                "ph_id": pid,
                "name": name,
                "tagline": tagline,
                "slug": slug,
                "website_url": website_url,
                "ph_url": ph_url,
                "logo_url": logo_url,
                "featured_at": featured_at,
            }
        )

    print("Fetched posts from Product Hunt:", len(posts))
    return posts


# ---------- GOOGLE SHEETS HELPERS (PH TAB ONLY) ----------

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

    # First try using the JSON string as service account info
    try:
        info = json.loads(sa_json_str)
        creds = Credentials.from_service_account_info(info, scopes=scopes)
        return __import__("gspread").authorize(creds)
    except Exception as e:
        # Fallback: try sa.json on disk (same pattern as YC script)
        try:
            from google.oauth2.service_account import Credentials as C2
            import gspread as g2

            creds = C2.from_service_account_file("sa.json", scopes=scopes)
            return g2.authorize(creds)
        except Exception as e2:
            raise RuntimeError(
                "Failed building gspread client: " + str(e) + " / " + str(e2)
            )


def ensure_ph_worksheet(gc, sheet_id: str, worksheet_name: str):
    """
    Open the PH worksheet (tab) if it exists, otherwise create it with header row.
    This function NEVER touches the YC tab.
    """
    import gspread  # for types/exceptions

    sh = gc.open_by_key(sheet_id)
    try:
        ws = sh.worksheet(worksheet_name)
        # If header row is empty, write it once
        header = ws.row_values(1)
        if not header:
            ws.update("A1", [SHEET_HEADER])
        return ws
    except gspread.exceptions.WorksheetNotFound:
        # Create new worksheet dedicated to Product Hunt data
        cols = len(SHEET_HEADER)
        ws = sh.add_worksheet(title=worksheet_name, rows="1000", cols=str(cols))
        ws.update("A1", [SHEET_HEADER])
        return ws


def read_existing_ids_and_rows(gc, sheet_id: str, worksheet_name: str, id_col_header="ph_id"):
    """
    Returns (existing_ids_set, id_to_row_index_dict) for Product Hunt tab only.
    """
    try:
        ws = ensure_ph_worksheet(gc, sheet_id, worksheet_name)
        header = ws.row_values(1)
        if not header:
            # header just created by ensure_ph_worksheet
            return set(), {}

        lower = [h.strip().lower() for h in header]
        if id_col_header.lower() in lower:
            idx = lower.index(id_col_header.lower()) + 1  # 1-based
        else:
            # default to second column if unknown
            idx = 2

        vals = ws.col_values(idx)
        id_to_row = {}
        for i, v in enumerate(vals[1:], start=2):
            s = str(v).strip()
            if s:
                id_to_row[s] = i

        return set(id_to_row.keys()), id_to_row
    except Exception as e:
        print("Warning: couldn't read PH sheet ids:", e)
        return set(), {}


def update_row_preserve_date(gc, sheet_id: str, worksheet_name: str, row_index: int, values):
    """
    Update an existing row in the PH worksheet, preserving first_seen_at.
    """
    try:
        ws = ensure_ph_worksheet(gc, sheet_id, worksheet_name)

        if len(values) < len(SHEET_HEADER):
            values = values + [""] * (len(SHEET_HEADER) - len(values))

        try:
            existing_row = ws.row_values(row_index)
        except Exception as e:
            print(f"Failed to read existing PH row {row_index}:", e)
            existing_row = []

        # Preserve original date if present
        if (
            existing_row
            and len(existing_row) > DATE_COL_INDEX
            and existing_row[DATE_COL_INDEX].strip()
        ):
            values[DATE_COL_INDEX] = existing_row[DATE_COL_INDEX]
        else:
            if not values[DATE_COL_INDEX]:
                values[DATE_COL_INDEX] = now_iso_utc()

        start_col = "A"
        end_col = chr(ord("A") + len(values) - 1)
        rng = f"{start_col}{row_index}:{end_col}{row_index}"
        ws.update(rng, [values])
        return True
    except Exception as e:
        print("Failed to update PH row", row_index, ":", e)
        return False


def append_row_with_date(gc, sheet_id: str, worksheet_name: str, values):
    """
    Append a new row in the PH worksheet, setting first_seen_at if empty.
    """
    try:
        ws = ensure_ph_worksheet(gc, sheet_id, worksheet_name)

        if len(values) < len(SHEET_HEADER):
            values = values + [""] * (len(SHEET_HEADER) - len(values))

        if not values[DATE_COL_INDEX]:
            values[DATE_COL_INDEX] = now_iso_utc()

        ws.append_row(values, value_input_option="USER_ENTERED")
        return True
    except Exception as e:
        print("Failed to append PH row:", e)
        return False


# ---------- MAIN ----------

def main():
    PRODUCT_HUNT_TOKEN = safe_env("PRODUCT_HUNT_TOKEN")
    SHEET_ID = safe_env("SHEET_ID")  # same as YC sheet
    GCP_SA_KEY_JSON = os.environ.get("GCP_SA_KEY_JSON")
    PH_WORKSHEET_NAME = safe_env("PH_WORKSHEET_NAME", DEFAULT_PH_WORKSHEET_NAME)

    PH_LIMIT = int(safe_env("PH_LIMIT", "30"))
    REQUEST_DELAY = float(safe_env("PH_REQUEST_DELAY", "0.25"))

    if not PRODUCT_HUNT_TOKEN:
        print("ERROR: PRODUCT_HUNT_TOKEN env var is required.")
        return

    seen = load_seen()
    print("Loaded seen PH ids:", len(seen))

    try:
        posts = fetch_top_posts(limit=PH_LIMIT, token=PRODUCT_HUNT_TOKEN)
    except Exception as e:
        print("Failed to fetch from Product Hunt:", e)
        return

    gc = None
    existing_ids = set()
    id_to_row = {}

    if SHEET_ID and GCP_SA_KEY_JSON:
        try:
            gc = get_gsheet_client_from_sa_json(GCP_SA_KEY_JSON)
            existing_ids, id_to_row = read_existing_ids_and_rows(
                gc,
                SHEET_ID,
                worksheet_name=PH_WORKSHEET_NAME,
                id_col_header="ph_id",
            )
            print("Existing PH ids in sheet:", len(existing_ids))
        except Exception as e:
            print("Warning: could not connect to Google Sheets for PH:", e)
            gc = None
    else:
        print("SHEET_ID or GCP_SA_KEY_JSON missing; PH sheet upsert skipped.")

    # Filter out posts we've already seen in normal mode
    final_posts = []
    for p in posts:
        pid = p.get("ph_id")
        if not pid:
            continue
        if pid in seen:
            continue
        final_posts.append(p)

    print("PH posts after seen dedupe:", len(final_posts))
    if not final_posts:
        print("No new Product Hunt posts to process.")
        return

    output_rows = []

    for p in final_posts:
        pid = p["ph_id"]
        print("Processing PH post:", pid, "-", p.get("name", ""))

        row_obj = {
            "first_seen_at": now_iso_utc(),
            "ph_id": pid,
            "name": p.get("name") or "",
            "tagline": p.get("tagline") or "",
            "website_url": p.get("website_url") or "",
            "ph_url": p.get("ph_url") or "",
            "logo_url": p.get("logo_url") or "",
        }

        values = [row_obj[h] for h in SHEET_HEADER]

        upserted = False
        if gc and SHEET_ID:
            try:
                if pid in existing_ids and pid in id_to_row:
                    row_index = id_to_row[pid]
                    ok = update_row_preserve_date(
                        gc, SHEET_ID, PH_WORKSHEET_NAME, row_index, values
                    )
                    if ok:
                        upserted = True
                        print("Updated PH row", row_index, "for PH id", pid)
                else:
                    ok = append_row_with_date(
                        gc, SHEET_ID, PH_WORKSHEET_NAME, values
                    )
                    if ok:
                        upserted = True
                        print("Appended PH row for PH id", pid)
            except Exception as e:
                print("PH sheet upsert error for id", pid, ":", e)
                upserted = False
        else:
            print(
                "Google Sheets not configured for PH (gc or SHEET_ID missing) – "
                f"skipping sheet write for PH id {pid}"
            )
            upserted = False

        if upserted:
            seen.add(pid)
            existing_ids.add(pid)
        else:
            print("Failed to upsert PH id", pid, "— not marking as seen.")

        output_rows.append(row_obj)
        time.sleep(REQUEST_DELAY)

    # Write CSV snapshot
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join(WORKDIR, f"ph_top_{ts}.csv")
    try:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=SHEET_HEADER)
            writer.writeheader()
            for r in output_rows:
                writer.writerow(r)
        print("Wrote PH CSV:", csv_path)
    except Exception as e:
        print("Failed to write PH CSV:", e)

    save_seen(seen)
    print("Updated ph_seen_ids.json with", len(seen), "entries")


if __name__ == "__main__":
    main()
