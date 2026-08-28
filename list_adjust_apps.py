"""
Adjust — SAARE app tokens discover karo                             v1.0
========================================================================
200 apps ke tokens dashboard se ek-ek karke nikalna 200 clicks hai.
Ye script wahi Report Service API use karti hai jo main loader use karta
hai — aur ek hi call mein har us app ka token de deti hai jis tak tumhare
API token ki rasai hai.

CHAL KAISE RAHI HAI
───────────────────
Report Service ko `dimensions=app,app_token` ke saath call karo LEKIN
`app_token__in` filter ke BAGAIR. Tab Adjust har wo app lauta deta hai
jis tak tumhari rasai hai — naam aur token dono ke saath.

Yani wahi endpoint jo data laata hai, wahi tokens bhi de deta hai.
Alag credentials, alag permission, kuch nahi chahiye.

⚠️ EK IMAANDAR CAVEAT
─────────────────────
Report Service sirf wo apps lauta hai jinki us window mein KOI activity
thi (installs/clicks/sessions/revenue). Ekdum naya app ya bilkul mara hua
app list mein nahi aayega.

Isi liye default window 180 din hai — taake mausami/kam-chalne wale apps
bhi pakde jayen. Phir bhi list ko dashboard ke app count se milaa lena.

CHALANE KA TAREEQA
──────────────────
    export ADJUST_API_TOKEN='...'
    python list_adjust_apps.py

    # ya lamba window
    LOOKBACK_DAYS=365 python list_adjust_apps.py

Ye do file likhti hai:
    app_tokens.txt        ← main loader isay padhta hai (ek token per line,
                             saath mein app ka naam comment ki soorat mein)
    adjust_apps.csv       ← insaanon ke padhne ke liye (naam, token, metrics)

Purani `app_tokens.txt` ko OVERWRITE karne se pehle backup bana leti hai.
========================================================================
"""

import os
import sys
import csv
import json
import time
import shutil
import logging
from datetime import date, timedelta

import requests

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("adjust_apps")

ADJUST_ENDPOINT = "https://automate.adjust.com/reports-service/report"

ADJUST_API_TOKEN = os.environ.get("ADJUST_API_TOKEN", "")
LOOKBACK_DAYS    = int(os.environ.get("LOOKBACK_DAYS", "180"))
REQUEST_TIMEOUT  = int(os.environ.get("REQUEST_TIMEOUT", "300"))
MAX_RETRIES      = int(os.environ.get("MAX_RETRIES", "5"))
OUT_TOKENS       = os.environ.get("OUT_TOKENS", "app_tokens.txt")
OUT_CSV          = os.environ.get("OUT_CSV", "adjust_apps.csv")
UTC_OFFSET       = os.environ.get("UTC_OFFSET", "+00:00")

RETRYABLE = {408, 429, 500, 502, 503, 504}


def fetch_all_apps(start: date, end: date):
    """
    Har app + uska token. Fail pe None (khali list NAHI) — taake caller
    galti se app_tokens.txt khali na likh de.
    """
    headers = {"Authorization": f"Bearer {ADJUST_API_TOKEN}",
               "Accept": "application/json"}
    params = {
        # 👇 app_token__in JAAN-BOOJH KE nahi diya — tabhi SAB apps aate hain
        "date_period": f"{start.isoformat()}:{end.isoformat()}",
        # store_id/store_type/os_name bhi — yehi app_master_v2 ka join key hai
        "dimensions":  "app,app_token,store_id,store_type,os_name",
        "metrics":     "installs,clicks,sessions,revenue",
        "utc_offset":  UTC_OFFSET,
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(ADJUST_ENDPOINT, headers=headers,
                             params=params, timeout=REQUEST_TIMEOUT)
            # body pehle parse karo — status chahe kuch bhi ho
            try:
                body = r.json()
            except ValueError:
                body = None

            if r.status_code == 200 and isinstance(body, dict):
                return body.get("rows")

            detail = ""
            if isinstance(body, dict):
                detail = (body.get("error") or body.get("message")
                          or json.dumps(body)[:300])
            else:
                detail = (r.text or "")[:300]

            if r.status_code in (401, 403):
                log.error("HTTP %s — API token galat hai ya permission nahi: %s",
                          r.status_code, detail)
                return None

            if r.status_code in RETRYABLE and attempt < MAX_RETRIES:
                wait = 30 * attempt
                log.warning("HTTP %s — %ss wait, retry %d/%d | %s",
                            r.status_code, wait, attempt, MAX_RETRIES, detail)
                time.sleep(wait)
                continue

            log.error("HTTP %s — %s", r.status_code, detail)
            return None

        except requests.exceptions.RequestException as exc:
            if attempt < MAX_RETRIES:
                wait = 30 * attempt
                log.warning("Network error — %ss wait, retry %d/%d: %s",
                            wait, attempt, MAX_RETRIES, exc)
                time.sleep(wait)
                continue
            log.error("%d retries ke baad haar gaye: %s", MAX_RETRIES, exc)
            return None
    return None


def main():
    if not ADJUST_API_TOKEN.strip():
        log.error("ADJUST_API_TOKEN env set nahi hai.")
        sys.exit(1)

    end   = date.today() - timedelta(days=1)
    start = end - timedelta(days=LOOKBACK_DAYS - 1)
    log.info("Apps dhoond rahe hain: %s → %s (%d din)", start, end, LOOKBACK_DAYS)

    rows = fetch_all_apps(start, end)
    if rows is None:
        log.error("Fetch fail — koi file NAHI likhi gayi (purani mehfooz hai).")
        sys.exit(1)

    def num(v):
        try:
            return float(v) if v not in (None, "") else 0.0
        except (TypeError, ValueError):
            return 0.0

    apps = {}
    no_token = 0
    for r in rows:
        tok = (r.get("app_token") or "").strip()
        if not tok:
            no_token += 1
            continue
        a = apps.setdefault(tok, {"name": r.get("app") or "",
                                  "store_id": "", "store_type": "", "os_name": "",
                                  "installs": 0.0, "clicks": 0.0,
                                  "sessions": 0.0, "revenue": 0.0})
        if not a["name"] and r.get("app"):
            a["name"] = r["app"]
        for f in ("store_id", "store_type", "os_name"):
            if not a[f] and r.get(f):
                a[f] = str(r[f]).strip()
        for m in ("installs", "clicks", "sessions", "revenue"):
            a[m] += num(r.get(m))

    if not apps:
        log.error("Ek bhi app nahi mila — koi file NAHI likhi gayi.")
        log.error("Window barha ke dekho: LOOKBACK_DAYS=365")
        sys.exit(1)

    if no_token:
        log.warning("%d rows mein app_token khali tha (chhodi gayin)", no_token)

    log.info("✅ %d apps mile", len(apps))

    # ── purani file ka backup ──
    if os.path.exists(OUT_TOKENS):
        bak = f"{OUT_TOKENS}.bak"
        shutil.copy2(OUT_TOKENS, bak)
        with open(OUT_TOKENS, "r", encoding="utf-8") as fh:
            old = {ln.split("#", 1)[0].strip() for ln in fh}
        old.discard("")
        new_tok = set(apps)
        added   = new_tok - old
        removed = old - new_tok
        log.info("Purani file: %d tokens (backup → %s)", len(old), bak)
        if added:
            log.info("  ➕ naye: %d", len(added))
        if removed:
            # Ye AHEM hai: agar koi token gir raha hai to wo app is window
            # mein khamosh tha — uska matlab band hona zaroori NAHI.
            log.warning("  ⚠️  purani file mein the, ab nahi aaye: %d", len(removed))
            for t in sorted(removed)[:10]:
                log.warning("       %s", t)
            log.warning("     (ho sakta hai in ki is window mein activity na ho —")
            log.warning("      hatane se pehle dashboard pe tasdeeq karo)")

    # ── app_tokens.txt ──
    with open(OUT_TOKENS, "w", encoding="utf-8") as fh:
        fh.write(f"# Adjust app tokens — {date.today().isoformat()} ko banayi\n")
        fh.write(f"# Source: Report Service, window {start} → {end}\n")
        fh.write(f"# Kul {len(apps)} apps\n\n")
        for tok, a in sorted(apps.items(), key=lambda x: -x[1]["installs"]):
            key = a["store_id"] or "⚠️ STORE_ID NAHI"
            fh.write(f"{tok}    # {a['name']}  |  {key}\n")
    log.info("📄 %s likh di (%d tokens)", OUT_TOKENS, len(apps))

    # ── adjust_apps.csv ──
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["app_token", "app_name", "store_id", "store_type",
                    "os_name", "android_package", "apple_id",
                    "installs", "clicks", "sessions", "revenue"])
        for tok, a in sorted(apps.items(), key=lambda x: -x[1]["installs"]):
            sid, os_l = a["store_id"], a["os_name"].lower()
            and_pkg = sid.lower() if ("." in sid and "ios" not in os_l) else ""
            app_id  = sid if sid.isdigit() else ""
            w.writerow([tok, a["name"], sid, a["store_type"], a["os_name"],
                        and_pkg, app_id,
                        int(a["installs"]), int(a["clicks"]),
                        int(a["sessions"]), round(a["revenue"], 4)])
    log.info("📄 %s likh di", OUT_CSV)

    # ── khulasa ──
    tot_i = sum(a["installs"] for a in apps.values())
    tot_r = sum(a["revenue"] for a in apps.values())
    zero  = sum(1 for a in apps.values() if a["installs"] == 0)
    log.info("─" * 60)
    log.info("Kul apps    : %d", len(apps))
    log.info("Kul installs: %s", f"{int(tot_i):,}")
    log.info("Kul revenue : %s", f"{tot_r:,.2f}")
    if zero:
        log.info("0 installs wale: %d (phir bhi shaamil hain)", zero)

    # 🔑 Store key coverage — mapping ke liye sab se ahem ginti
    no_sid = [t for t, a in apps.items() if not a["store_id"]]
    log.info("store_id maujood : %d/%d apps", len(apps) - len(no_sid), len(apps))
    if no_sid:
        log.warning("⚠️  %d apps ka store_id NAHI mila — inka data BigQuery mein",
                    len(no_sid))
        log.warning("    aayega lekin app_master_v2 se JUD NAHI payega:")
        for t in no_sid[:10]:
            log.warning("       %s  %s", t, apps[t]["name"][:45])
    log.info("─" * 60)
    log.info("Top 10:")
    for tok, a in sorted(apps.items(), key=lambda x: -x[1]["installs"])[:10]:
        log.info("  %-14s %-32s %-34s %9s inst",
                 tok, a["name"][:32], (a["store_id"] or "—")[:34],
                 f"{int(a['installs']):,}")
    log.info("─" * 60)
    log.info("⚠️  Ye ginti Adjust dashboard ke app count se MILAA lena.")
    log.info("    Farq ho to wo apps hain jinki is window mein koi activity")
    log.info("    nahi thi — LOOKBACK_DAYS=365 se dobara chala ke dekho.")


if __name__ == "__main__":
    main()
