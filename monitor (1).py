"""
Buy Box Monitor
---------------
Reads brand/ASIN/marketplace config from Google Sheets,
checks Buy Box status via Keepa, and sends Slack alerts
when issues are detected. Runs every 2 hours automatically.

Requirements:
    pip install keepa requests gspread
"""

import keepa
import requests
import json
import time
import os
import csv
import io
from datetime import datetime

# ── Configuration ──────────────────────────────────────────────────────────────

KEEPA_API_KEY = "fplfd4nmce9ijnip516a9lckgfqal2rct3b0on8tljkoksmbf67ss1lcproo89i6"
DEFAULT_SLACK_WEBHOOK = "https://hooks.slack.com/services/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"
SHEET_ID = "1LiLM1ogDlowWOHhX5ZYMvSgu3ZHp5kBhef8fsaeXKnc"
CHECK_INTERVAL = 86400  # 24 hours in seconds
STATE_FILE = "buybox_state.json"

DOMAIN_MAP = {
    "US": "US", "UK": "GB", "DE": "DE", "FR": "FR",
    "IT": "IT", "ES": "ES", "CA": "CA", "AU": "AU", "JP": "JP", "IN": "IN",
}

# ── Helpers ─────────────────────────────────────────────────────────────────────

def normalize(h):
    return h.strip().lower().replace(" ", "").replace("_", "")

def get_col(row, *candidates):
    for key in row:
        if key is None:
            continue
        for c in candidates:
            if normalize(str(key)) == normalize(c):
                val = row[key]
                return str(val).strip() if val is not None else ""
    return ""

# ── Load Google Sheet ───────────────────────────────────────────────────────────

def load_sheet():
    """Load brand config directly from Google Sheet using gviz CSV endpoint."""
    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv"
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        text = r.text
    except Exception as e:
        raise Exception(f"Could not fetch Google Sheet: {e}")

    reader = csv.DictReader(io.StringIO(text))
    print(f"  Columns detected: {list(reader.fieldnames)}")

    brands = []
    for row in reader:
        brand_name  = get_col(row, "Brand Name", "BrandName")
        seller_name = get_col(row, "Seller Name", "SellerName")
        asins_raw   = get_col(row, "ASINs", "ASIN")
        marketplace = get_col(row, "Marketplace") or "US"
        keepa_key   = get_col(row, "Keepa API Key", "KeepaAPIKey") or KEEPA_API_KEY
        slack_hook  = get_col(row, "Slack Webhook", "SlackWebhook") or DEFAULT_SLACK_WEBHOOK

        if not brand_name or not asins_raw:
            continue

        asins = [a.strip() for a in asins_raw.split(",") if a.strip()]
        brands.append({
            "brand_name":    brand_name,
            "seller_name":   seller_name,
            "asins":         asins,
            "marketplace":   marketplace.strip().upper(),
            "keepa_key":     keepa_key,
            "slack_webhook": slack_hook,
        })
    return brands

# ── State Management ────────────────────────────────────────────────────────────

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {}

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

# ── Slack Alerts ────────────────────────────────────────────────────────────────

def send_slack_alert(webhook_url, message):
    if not webhook_url:
        print(f"  [WARNING] No Slack webhook configured.")
        return
    try:
        r = requests.post(webhook_url, json={"text": message}, timeout=10)
        if r.status_code == 200:
            print(f"  [SLACK] Alert sent successfully.")
        else:
            print(f"  [SLACK] Failed to send alert. Status: {r.status_code}")
    except Exception as e:
        print(f"  [ERROR] Slack alert failed: {e}")

# ── Buy Box Check ───────────────────────────────────────────────────────────────

def check_buybox(brand, state):
    brand_name    = brand["brand_name"]
    seller_name   = brand["seller_name"].lower()
    asins         = brand["asins"]
    marketplace   = brand["marketplace"]
    keepa_key     = brand["keepa_key"]
    slack_webhook = brand["slack_webhook"]
    domain_id     = DOMAIN_MAP.get(marketplace, "US")

    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Checking {brand_name} ({marketplace}) — {len(asins)} ASINs")

    try:
        api = keepa.Keepa(keepa_key)
        print(f"  Keepa tokens available: {api.tokens_left}")
    except Exception as e:
        print(f"  [ERROR] Could not connect to Keepa: {e}")
        return state

    for i in range(0, len(asins), 10):
        batch = asins[i:i+10]
        print(f"  Checking batch {i//10 + 1}/{(len(asins)-1)//10 + 1}: {len(batch)} ASINs")
        try:
            products = api.query(
                batch,
                domain=domain_id,
                buybox=True,
                progress_bar=False
            )
        except Exception as e:
            print(f"  [ERROR] Keepa query failed: {e}")
            continue

        for product in products:
            asin      = product.get("asin", "Unknown")
            title     = product.get("title", "Unknown Product")[:60]
            state_key = f"{brand_name}_{marketplace}_{asin}"

            # Get current Buy Box seller
            bb_history = product.get("buyBoxSellerIdHistory", [])
            current_bb_seller = bb_history[-1] if bb_history else None

            # Detect suppression
            suppressed = False
            try:
                csv_data = product.get("csv", [])
                if len(csv_data) > 18 and csv_data[18]:
                    suppressed = (csv_data[18][-1] == -1)
            except Exception:
                pass

            # Determine issue
            issue        = None
            issue_detail = ""

            if suppressed or current_bb_seller is None:
                issue        = "🔴 Buy Box Suppressed"
                issue_detail = "No seller currently holds the Buy Box. Possible pricing or eligibility issue."
            elif seller_name and seller_name not in str(current_bb_seller).lower():
                prev_seller = state.get(state_key, {}).get("bb_seller")
                if prev_seller != current_bb_seller:
                    if prev_seller and seller_name in str(prev_seller).lower():
                        issue        = "🚨 Buy Box Lost"
                        issue_detail = f"New Buy Box owner: *{current_bb_seller}*"
                    else:
                        issue        = "⚠️ Buy Box Not Yours"
                        issue_detail = f"Current Buy Box owner: *{current_bb_seller}*"

            if issue:
                message = (
                    f"{issue} — `{asin}`\n"
                    f"*Brand:* {brand_name} ({marketplace})\n"
                    f"*Product:* {title}\n"
                    f"*Issue:* {issue_detail}\n"
                    f"*Time:* {datetime.now().strftime('%d %b %Y, %H:%M')}"
                )
                print(f"  [ALERT] {issue} — {asin}")
                send_slack_alert(slack_webhook, message)
            else:
                print(f"  [OK] {asin} — Buy Box healthy")

            state[state_key] = {
                "bb_seller":  current_bb_seller,
                "suppressed": suppressed,
                "last_check": datetime.now().isoformat(),
            }

        time.sleep(2)

    return state

# ── Main Loop ───────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  Buy Box Monitor — Starting up")
    print(f"  Check interval: every {CHECK_INTERVAL // 3600} hours")
    print("=" * 60)

    while True:
        state = load_state()

        try:
            brands = load_sheet()
            print(f"\nLoaded {len(brands)} brand/marketplace rows from Google Sheet")
            for b in brands:
                print(f"  → {b['brand_name']} ({b['marketplace']}) — {len(b['asins'])} ASINs")
        except Exception as e:
            print(f"[ERROR] Could not load Google Sheet: {e}")
            brands = []

        for brand in brands:
            state = check_buybox(brand, state)

        save_state(state)

        next_check = datetime.fromtimestamp(time.time() + CHECK_INTERVAL)
        print(f"\nNext check at: {next_check.strftime('%Y-%m-%d %H:%M:%S')}")
        print("-" * 60)

        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()
