"""
Adjust Report Service  →  BigQuery   ·   "SAB KUCH" EDITION          v2.0
==========================================================================
v1 sirf 4 metrics aur app-level grain laata tha. v2 SAB kuch laata hai:
har metric, har dimension, teen alag grain pe.

────────────────────────────────────────────────────────────────────────
TEEN TABLE — teen alag grain
────────────────────────────────────────────────────────────────────────
  1. app_daily_performance
     day × app × store × os
     → Looker/dashboards ke liye halki table, app-level totals

  2. campaign_daily_performance          🔑 ASLI MMP KA MAQSAD
     day × app × os × partner × campaign × adgroup × creative
     → kaunsa network/campaign kaunse install laaya, CPI kya raha

  3. country_daily_performance
     day × app × os × country × device
     → geo/device optimization

  Alag tables isliye ke attribution dimensions ke saath rows lakhon mein
  chale jate hain. Sab ek table mein daalo to:
     · Looker slow ho jata hai
     · app-level totals galat aane lagte hain (SUM double ho jata hai)
     · partition/cluster ka faida khatam

────────────────────────────────────────────────────────────────────────
🤝 AUTO-NEGOTIATION — "sab maango, jo mile wo lo"
────────────────────────────────────────────────────────────────────────
Adjust ke exact metric/dimension naam har account pe ek jaise nahi hote
(kuch features plan ke saath aate hain). Naam yaad se likhne ka anjaam:

    · galat naam  → Adjust warning deta hai, column KHALI aata hai
    · aadha sahi  → koi error nahi, bas data adhoora
    · mahino kisi ko pata nahi chalta

Isliye v2 pehli baar POORI list maangta hai. Agar Adjust radd kare to
har metric/dimension ALAG ALAG test karta hai, jo chale wahi rakhta hai,
aur baqi ko saaf saaf log karta hai. Nateeja memory mein cache hota hai —
baaki chunks pe dobara test nahi hota.

    ✅ Jo mila wo poora aata hai
    ❌ Jo nahi mila wo LOG hota hai (chup-chaap gayab nahi hota)

Schema bhi ISI ke mutabiq banti hai — khali columns nahi bante.

────────────────────────────────────────────────────────────────────────
🛡️ v1 ke SAARE 10 GUARDS bar-qarar hain
────────────────────────────────────────────────────────────────────────
  #1  DELETE account-scoped   (`app_token IN UNNEST(@ok)`)
  #2  fail = None, khali list NAHI
  #3  har call pe timeout
  #4  error body parse + raise
  #5  koi app khamoshi se nahi girta
  #6  FAILURES → sys.exit(1)
  #7  load job (streaming NAHI) + row-count verify
      → TRUNCATE+streaming ne Facebook mein 20% rows khamoshi se khaye the
  #8  har HTTP call pe retry + backoff
  #9  adhoori discovery pe BigQuery ko haath hi na lagao
  #10 asli error message log (raise_for_status se pehle body)

────────────────────────────────────────────────────────────────────────
ENV VARS
────────────────────────────────────────────────────────────────────────
LAZMI:  ADJUST_API_TOKEN · GCP_PROJECT · GCP_CREDENTIALS_JSON
APPS :  ADJUST_APP_TOKENS (comma) ya APP_TOKENS_FILE (default app_tokens.txt)

OPTIONAL:
  BQ_DATASET        adjust_data
  BQ_LOCATION       US
  LOOKBACK_DAYS     30
  CHUNK_SIZE        50
  REPORTS           app,campaign,country   (kaunsi tables banani hain)
  MAX_RETRIES       5
  REQUEST_TIMEOUT   300
  UTC_OFFSET        +00:00
  ATTRIBUTION_TYPE  all
  DRY_RUN           0
==========================================================================
"""

import os
import sys
import json
import time
import logging
from datetime import date, timedelta

import requests
from google.cloud import bigquery
from google.api_core import exceptions as gexc
from google.oauth2 import service_account

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("adjust_v2")

ADJUST_ENDPOINT = "https://automate.adjust.com/reports-service/report"

ADJUST_API_TOKEN     = os.environ.get("ADJUST_API_TOKEN", "")
GCP_PROJECT          = os.environ.get("GCP_PROJECT", "")
GCP_CREDENTIALS_JSON = os.environ.get("GCP_CREDENTIALS_JSON", "")

BQ_DATASET  = os.environ.get("BQ_DATASET", "adjust_data")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")

LOOKBACK_DAYS    = int(os.environ.get("LOOKBACK_DAYS", "30"))
CHUNK_SIZE       = int(os.environ.get("CHUNK_SIZE", "50"))
MAX_RETRIES      = int(os.environ.get("MAX_RETRIES", "5"))
REQUEST_TIMEOUT  = int(os.environ.get("REQUEST_TIMEOUT", "300"))
UTC_OFFSET       = os.environ.get("UTC_OFFSET", "+00:00")
ATTRIBUTION_TYPE = os.environ.get("ATTRIBUTION_TYPE", "all")
DRY_RUN          = os.environ.get("DRY_RUN", "0") == "1"
ENABLED_REPORTS  = [r.strip().lower() for r in
                    os.environ.get("REPORTS", "app,campaign,country").split(",")
                    if r.strip()]

APP_TOKENS_ENV  = os.environ.get("ADJUST_APP_TOKENS", "")
APP_TOKENS_FILE = os.environ.get("APP_TOKENS_FILE", "app_tokens.txt")

RUN_ID   = f"{date.today().isoformat()}-{int(time.time())}"
FAILURES = []


def record_failure(where, detail):
    msg = f"{where}: {detail}"
    FAILURES.append(msg)
    log.error("  ❌ %s", msg)


def die(msg):
    log.error("=" * 74)
    log.error("🔴 %s", msg)
    log.error("   BigQuery ko HAATH NAHI LAGAYA — purana data mehfooz hai.")
    log.error("=" * 74)
    sys.exit(1)


# ══════════════════════════════════════════════════════════════════════════
#  SAB KUCH — poori list. Jo Adjust na de, wo khud gir jayega (log ke saath).
# ══════════════════════════════════════════════════════════════════════════
ALL_METRICS = [
    # core
    "installs", "clicks", "impressions", "sessions",
    "limit_ad_tracking_installs", "limit_ad_tracking_install_rate",
    "click_conversion_rate", "impression_conversion_rate",
    # lifecycle
    "reattributions", "reattribution_reinstalls", "reinstalls",
    "uninstalls", "uninstall_cohort", "uninstall_rate",
    "first_reinstalls", "first_reattributions", "deattributions",
    # users
    "daus", "waus", "maus",
    # revenue
    "revenue", "revenue_events", "revenue_per_install",
    "ad_revenue", "all_revenue", "arpdau", "arpu",
    # cost — UA spend Adjust ke andar se
    "cost", "network_cost", "network_cost_diff", "ad_spend",
    "ecpi", "ecpi_all", "cost_per_install",
    "click_cost", "impression_cost", "install_cost",
    # profitability
    "gross_profit", "roas", "return_on_investment",
    # events
    "events", "first_events", "all_events",
    # cohort (shayad alag endpoint maange — negotiation bata degi)
    "retained_users_d1", "retained_users_d7", "retained_users_d30",
    "retention_rate_d1", "retention_rate_d7", "retention_rate_d30",
    "roas_d0", "roas_d1", "roas_d7", "roas_d30",
    "revenue_total_d0", "revenue_total_d7", "revenue_total_d30",
    "lifetime_value",
]

# Ye INTEGER hain; baqi sab FLOAT (rates/revenue/cost)
INT_METRICS = {
    "installs", "clicks", "impressions", "sessions",
    "limit_ad_tracking_installs", "reattributions", "reattribution_reinstalls",
    "reinstalls", "uninstalls", "uninstall_cohort",
    "first_reinstalls", "first_reattributions", "deattributions",
    "daus", "waus", "maus", "revenue_events",
    "events", "first_events", "all_events",
    "retained_users_d1", "retained_users_d7", "retained_users_d30",
}

# ── TEEN REPORT ────────────────────────────────────────────────────────────
REPORTS = [
    {
        "key":        "app",
        "table":      "app_daily_performance",
        "dimensions": ["day", "app", "app_token", "store_id", "store_type", "os_name"],
        "cluster":    ["app_token", "os_name"],
        "desc":       "app-level — Looker/dashboards",
    },
    {
        "key":        "campaign",
        "table":      "campaign_daily_performance",
        "dimensions": ["day", "app", "app_token", "os_name",
                       "partner_name", "network", "campaign",
                       "adgroup", "creative"],
        "cluster":    ["app_token", "partner_name", "campaign"],
        "desc":       "🔑 attribution — kaunsa network/campaign kya laaya",
    },
    {
        "key":        "country",
        "table":      "country_daily_performance",
        "dimensions": ["day", "app", "app_token", "os_name",
                       "country", "country_code", "device_type"],
        "cluster":    ["app_token", "country_code"],
        "desc":       "geo/device breakdown",
    },
]


# ─── HELPERS ───────────────────────────────────────────────────────────────
def _to_int(v):
    try:
        return int(float(v)) if v not in (None, "") else 0
    except (TypeError, ValueError):
        return 0


def _to_float(v):
    try:
        return float(v) if v not in (None, "") else 0.0
    except (TypeError, ValueError):
        return 0.0


SENTINELS = {"unknown", "missing", "n/a", "na", "-", "none", "null", ""}


def _clean(v):
    """Adjust ke sentinel ('unknown'/'missing') ko NULL bana do."""
    if v is None:
        return None
    s = str(v).strip()
    return None if s.lower() in SENTINELS else s


def derive_store_keys(store_id, store_type, os_name):
    """
    Adjust ka store_id → app_master_v2 ke join keys.

        Android google_play : "com.example.app"  → android_package
        iOS app_store       : "1234567890"       → apple_id (INT64)
        Amazon appstore     : "B0CX5N7R6L"       → amazon_asin

    Asli data se seekha (2026-08-28):
      · store_type 'app_store' aata hai (na ke 'itunes')
      · sentinel DO hain: 'unknown' AUR 'missing'
      · amazon rows maujood hain — ASIN, 11,450 installs
      · iOS ka bundle-id kabhi android_package NAHI banana (master alag
        column `ios_bundle_id` rakhta hai) — warna jhoota mapping ban jata

    Samajh na aaye to sab None — jhoota mapping se behtar hai ke row
    unmapped rahe aur monitor use pakde.
    """
    sid = _clean(store_id)
    if not sid:
        return None, None, None

    os_l = (os_name or "").strip().lower()
    st_l = (store_type or "").strip().lower()

    if "amazon" in st_l:
        return None, None, sid.upper()

    is_ios = ("ios" in os_l) or ("app_store" in st_l) or ("itunes" in st_l)
    is_and = ("android" in os_l) or ("google" in st_l) or ("play" in st_l)

    if sid.isdigit():
        if is_and:
            return None, None, None       # numeric + android = shak
        try:
            return None, int(sid), None
        except ValueError:
            return None, None, None

    if "." in sid:
        if is_ios:
            return None, None, None       # iOS bundle — apple_id nahi
        return sid.lower(), None, None

    return None, None, None


def load_app_tokens():
    if APP_TOKENS_ENV.strip():
        tokens = [t.strip() for t in APP_TOKENS_ENV.split(",")]
        src = "ADJUST_APP_TOKENS env"
    elif os.path.exists(APP_TOKENS_FILE):
        with open(APP_TOKENS_FILE, "r", encoding="utf-8") as fh:
            tokens = [ln.split("#", 1)[0].strip() for ln in fh]
        src = f"file {APP_TOKENS_FILE}"
    else:
        cwd = os.getcwd()
        log.error("Working directory: %s", cwd)
        try:
            for f in sorted(os.listdir(cwd))[:40]:
                log.error("   • %s%s", f,
                          "  👈 ye chahiye tha" if f == APP_TOKENS_FILE else "")
        except OSError:
            pass
        die(f"App tokens nahi mile — na env mein, na '{APP_TOKENS_FILE}' mein.")

    seen, clean = set(), []
    for t in tokens:
        if t and t not in seen:
            seen.add(t)
            clean.append(t)
    if not clean:
        die(f"{src} mein ek bhi valid app token nahi mila.")
    log.info("App tokens: %d unique (%s)", len(clean), src)
    return clean


def chunked(seq, size):
    if size <= 0:
        yield seq
        return
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


# ─── ADJUST API ────────────────────────────────────────────────────────────
RETRYABLE_STATUS = {408, 429, 500, 502, 503, 504}


def call_adjust(params, label, quiet=False):
    """
    Returns (body, error_text).
      body=dict → kaamyab
      body=None → nakaam (error_text mein wajah)

    🛡️ #10 — body HAMESHA pehle parse hoti hai, status dekhne se pehle.
    🛡️ #2  — nakami pe None (khali list NAHI).
    🛡️ #3  — har call pe REQUEST_TIMEOUT.
    🛡️ #8  — aarzi errors pe retry + backoff.
    """
    headers = {"Authorization": f"Bearer {ADJUST_API_TOKEN}",
               "Accept": "application/json"}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(ADJUST_ENDPOINT, headers=headers,
                             params=params, timeout=REQUEST_TIMEOUT)
            try:
                body = r.json()
            except ValueError:
                body = None

            if r.status_code == 200 and isinstance(body, dict):
                return body, ""

            detail = ""
            if isinstance(body, dict):
                detail = (body.get("error") or body.get("message")
                          or body.get("detail") or json.dumps(body))[:300]
            else:
                detail = (r.text or "")[:300]

            if r.status_code in (401, 403):
                die(f"HTTP {r.status_code} — ADJUST_API_TOKEN galat hai ya "
                    f"permission nahi: {detail}")

            if r.status_code in RETRYABLE_STATUS and attempt < MAX_RETRIES:
                wait = min(30 * attempt, 300)
                if not quiet:
                    log.warning("  %s: HTTP %s — %ss wait, retry %d/%d",
                                label, r.status_code, wait, attempt, MAX_RETRIES)
                    log.warning("    Adjust: %s", detail[:200])
                time.sleep(wait)
                continue

            return None, f"HTTP {r.status_code}: {detail}"

        except requests.exceptions.RequestException as exc:
            if attempt < MAX_RETRIES:
                wait = min(30 * attempt, 300)
                if not quiet:
                    log.warning("  %s: network — %ss wait, retry %d/%d: %s",
                                label, wait, attempt, MAX_RETRIES, exc)
                time.sleep(wait)
                continue
            return None, f"network: {exc}"

    return None, "retries khatam"


# ══════════════════════════════════════════════════════════════════════════
#  🤝 NEGOTIATION — "sab maango, jo mile wo lo, baqi LOG karo"
# ══════════════════════════════════════════════════════════════════════════
_NEG_CACHE = {}     # report_key → (dimensions, metrics)


def _probe_one(dims, mets, token, period, label):
    """Ek combo test karo. (ok, warnings) lauta do."""
    body, err = call_adjust({
        "app_token__in":    token,
        "date_period":      period,
        "dimensions":       ",".join(dims),
        "metrics":          ",".join(mets),
        "utc_offset":       UTC_OFFSET,
        "attribution_type": ATTRIBUTION_TYPE,
    }, label, quiet=True)
    if body is None:
        return False, [err]
    warns = [str(w) for w in (body.get("warnings") or [])]
    if body.get("rows") is None:
        return False, warns or ["'rows' key hi nahi aayi"]
    return (not warns), warns


def negotiate(report, token, period):
    """
    Pehle POORI list maango. Kaamyab → wahi use karo.
    Nakaam → har dimension/metric ALAG test karke jo chale wahi rakho.

    Nateeja cache hota hai: 141 apps ke liye dobara test nahi hota.
    """
    key = report["key"]
    if key in _NEG_CACHE:
        return _NEG_CACHE[key]

    want_d = list(report["dimensions"])
    want_m = list(ALL_METRICS)

    log.info("🤝 [%s] negotiation — %d dimensions + %d metrics maang rahe hain",
             key, len(want_d), len(want_m))

    ok, warns = _probe_one(want_d, want_m, token, period, f"neg[{key}]")
    if ok:
        log.info("   ✅ Adjust ne POORI list qubool kar li")
        _NEG_CACHE[key] = (want_d, want_m)
        return _NEG_CACHE[key]

    log.warning("   ⚠️  poori list qubool nahi hui — ek ek karke test kar rahe hain")
    for w in warns[:3]:
        log.warning("      %s", w[:200])

    # ── dimensions: 'day' base, baqi ek ek ──
    good_d, bad_d = ["day"], []
    for d in want_d:
        if d == "day":
            continue
        ok, _ = _probe_one(["day", d], ["installs"], token, period, d)
        (good_d if ok else bad_d).append(d)
        time.sleep(0.4)

    # ── metrics: good dimensions ke saath ek ek ──
    good_m, bad_m = [], []
    for m in want_m:
        ok, _ = _probe_one(["day"], [m], token, period, m)
        (good_m if ok else bad_m).append(m)
        time.sleep(0.4)

    if not good_m:
        die(f"[{key}] ek bhi metric qubool nahi hua — aage badhna bekaar hai.")

    log.info("   ✅ [%s] chale — dimensions %d/%d, metrics %d/%d",
             key, len(good_d), len(want_d), len(good_m), len(want_m))
    if bad_d:
        log.warning("   ❌ [%s] dimensions jo NAHI chale (%d): %s",
                    key, len(bad_d), ", ".join(bad_d))
    if bad_m:
        log.warning("   ❌ [%s] metrics jo NAHI chale (%d): %s",
                    key, len(bad_m), ", ".join(bad_m))

    _NEG_CACHE[key] = (good_d, good_m)
    return _NEG_CACHE[key]


# ─── SCHEMA (negotiation ke mutabiq banti hai) ─────────────────────────────
DERIVED_FIELDS = [
    bigquery.SchemaField("android_package", "STRING",
                         description="DERIVED store_id se — app_master_v2.android_package"),
    bigquery.SchemaField("apple_id", "INTEGER",
                         description="DERIVED store_id se — app_master_v2.apple_id"),
    bigquery.SchemaField("amazon_asin", "STRING",
                         description="DERIVED store_id se — app_master_v2.amazon_asin"),
]
AUDIT_FIELDS = [
    bigquery.SchemaField("_ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("_run_id", "STRING"),
]


def build_schema(dimensions, metrics):
    """Sirf un columns ki schema jo Adjust ne WAQAI di — khali columns nahi."""
    fields = []
    for d in dimensions:
        if d == "day":
            fields.append(bigquery.SchemaField("date", "DATE", mode="REQUIRED"))
        else:
            fields.append(bigquery.SchemaField(d, "STRING"))

    # app_token har report mein lazmi hai — DELETE guard isi par chalta hai
    if "app_token" not in dimensions:
        fields.append(bigquery.SchemaField("app_token", "STRING", mode="REQUIRED"))

    if "store_id" in dimensions:
        fields.extend(DERIVED_FIELDS)

    for m in metrics:
        fields.append(bigquery.SchemaField(
            m, "INTEGER" if m in INT_METRICS else "FLOAT"))

    fields.extend(AUDIT_FIELDS)
    return fields


def parse_rows(raw_rows, dimensions, metrics, tokens, label):
    """Adjust ke rows → BigQuery dicts."""
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
    out, skipped, no_key = [], 0, 0
    has_store = "store_id" in dimensions

    for r in raw_rows:
        day = (r.get("day") or r.get("date") or "").strip()[:10]
        tok = (r.get("app_token") or "").strip()
        # 🛡️ Key ke bagair row bemaani hai — DELETE guard bhi us par nahi chalega
        if not day or not tok:
            skipped += 1
            continue

        row = {"date": day, "app_token": tok}
        for d in dimensions:
            if d in ("day", "app_token"):
                continue
            row[d] = _clean(r.get(d))

        if has_store:
            ap, ai, az = derive_store_keys(
                r.get("store_id"), r.get("store_type"), r.get("os_name"))
            row["android_package"] = ap
            row["apple_id"]        = ai
            row["amazon_asin"]     = az
            if not (ap or ai or az):
                no_key += 1

        for m in metrics:
            row[m] = _to_int(r.get(m)) if m in INT_METRICS else _to_float(r.get(m))

        row["_ingested_at"] = now_iso
        row["_run_id"]      = RUN_ID
        out.append(row)

    if skipped:
        log.warning("  %s: %d rows chhodi (day/app_token khali)", label, skipped)
    if no_key:
        log.warning("  %s: %d rows mein koi store key nahi (unmapped rahenge)",
                    label, no_key)
    return out


def fetch_chunk(report, tokens, start, end, dimensions, metrics, label):
    """
    Ek chunk ka data. Returns (rows, ok_tokens).
    🛡️ #2 — nakami pe (None, set()) — in apps ka purana data CHHUA NAHI jayega.
    """
    body, err = call_adjust({
        "app_token__in":    ",".join(tokens),
        "date_period":      f"{start.isoformat()}:{end.isoformat()}",
        "dimensions":       ",".join(dimensions),
        "metrics":          ",".join(metrics),
        "utc_offset":       UTC_OFFSET,
        "attribution_type": ATTRIBUTION_TYPE,
    }, label)

    if body is None:
        log.error("  %s: %s", label, err)
        return None, set()

    for w in (body.get("warnings") or []):
        log.warning("  %s: Adjust warning — %s", label, str(w)[:200])

    raw = body.get("rows")
    if raw is None:
        log.error("  %s: response mein 'rows' key nahi", label)
        return None, set()

    rows = parse_rows(raw, dimensions, metrics, tokens, label)
    log.info("  %s: %s rows, %d apps", label, f"{len(rows):,}",
             len({r['app_token'] for r in rows}))
    return rows, set(tokens)


# ─── BIGQUERY ──────────────────────────────────────────────────────────────
def get_bq_client():
    if not GCP_CREDENTIALS_JSON.strip():
        die("GCP_CREDENTIALS_JSON env khali hai.")
    try:
        info = json.loads(GCP_CREDENTIALS_JSON)
    except json.JSONDecodeError as exc:
        die(f"GCP_CREDENTIALS_JSON valid JSON nahi: {exc}")
    if not isinstance(info, dict):
        die("GCP_CREDENTIALS_JSON JSON object hona chahiye.")

    REQUIRED = ["type", "project_id", "private_key_id", "private_key",
                "client_email", "token_uri"]
    missing = [k for k in REQUIRED if not str(info.get(k, "")).strip()]
    if missing:
        log.error("GCP_CREDENTIALS_JSON mein ye fields nahi mile: %s", missing)
        log.error("Jo fields mile: %s", sorted(info.keys()))
        log.error("Sahi cheez: GCP se DOWNLOAD ki hui poori .json key file")
        log.error("  IAM & Admin → Service Accounts → Keys → Add key → JSON")
        die("Service-account JSON adhoori hai.")
    if "BEGIN PRIVATE KEY" not in info["private_key"]:
        die("private_key adhoora lagta hai (BEGIN PRIVATE KEY nahi mila).")

    log.info("Service account: %s (project %s)",
             info.get("client_email"), info.get("project_id"))
    creds = service_account.Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/cloud-platform"])
    return bigquery.Client(project=GCP_PROJECT, credentials=creds,
                           location=BQ_LOCATION)


def ensure_table(client, table_name, schema, cluster):
    ds_ref = f"{GCP_PROJECT}.{BQ_DATASET}"
    try:
        client.get_dataset(ds_ref)
    except gexc.NotFound:
        log.info("Dataset %s bana rahe hain (%s)", ds_ref, BQ_LOCATION)
        ds = bigquery.Dataset(ds_ref)
        ds.location = BQ_LOCATION
        client.create_dataset(ds)

    tbl_ref = f"{ds_ref}.{table_name}"
    names = {f.name for f in schema}
    cl = [c if c != "day" else "date" for c in cluster if c in names][:4]

    try:
        table = client.get_table(tbl_ref)
    except gexc.NotFound:
        log.info("Table %s bana rahe hain (partition: date, cluster: %s)",
                 tbl_ref, cl)
        t = bigquery.Table(tbl_ref, schema=schema)
        t.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="date")
        if cl:
            t.clustering_fields = cl
        client.create_table(t)
        return tbl_ref

    # schema align — sirf ADD, kabhi drop/badal nahi
    have = {f.name for f in table.schema}
    add = [bigquery.SchemaField(f.name, f.field_type, mode="NULLABLE",
                                description=f.description)
           for f in schema if f.name not in have]
    if add:
        log.info("Schema align [%s]: %d naye column — %s",
                 table_name, len(add), [f.name for f in add])
        table.schema = list(table.schema) + add
        client.update_table(table, ["schema"])

    extra = have - names
    if extra:
        log.warning("[%s] table mein extra columns (chhode ja rahe hain): %s",
                    table_name, sorted(extra))
    return tbl_ref


def replace_window(client, tbl_ref, table_name, rows, schema,
                   ok_tokens, start, end, all_ok):
    """
    🛡️ #1 — DELETE sirf un apps ka jo IS RUN mein aaye.
    🛡️ #7 — load job (streaming NAHI) + row-count verify.
    """
    if not rows:
        log.warning("  [%s] 0 rows — DELETE/LOAD kuch nahi (purana mehfooz)",
                    table_name)
        return
    if not ok_tokens:
        record_failure(table_name, "ok_tokens khali — kuch nahi kiya")
        return

    ok_list = sorted(ok_tokens)
    try:
        client.query(
            f"""DELETE FROM `{tbl_ref}`
                WHERE date BETWEEN @start AND @end
                  AND app_token IN UNNEST(@ok)""",
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("start", "DATE", start.isoformat()),
                bigquery.ScalarQueryParameter("end", "DATE", end.isoformat()),
                bigquery.ArrayQueryParameter("ok", "STRING", ok_list),
            ])).result()
        log.info("  [%s] cleared %s → %s for %d apps%s",
                 table_name, start, end, len(ok_list),
                 "" if all_ok else "  ⚠️ (kuch chunk fail — baqi apps CHHUE NAHI)")
    except Exception as exc:
        record_failure(f"delete[{table_name}]", exc)
        return

    try:
        job = client.load_table_from_json(
            rows, tbl_ref,
            job_config=bigquery.LoadJobConfig(
                schema=schema,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON))
        job.result()
        loaded = job.output_rows
    except Exception as exc:
        record_failure(f"load[{table_name}]", exc)
        return

    if loaded != len(rows):
        record_failure(table_name,
                       f"row count mismatch — bheje {len(rows):,}, gaye {loaded:,}")
    else:
        log.info("  ✅ [%s] %s rows (verified)", table_name, f"{loaded:,}")


# ─── MAIN ──────────────────────────────────────────────────────────────────
def main():
    log.info("🚀 Adjust → BigQuery  v2.0  ·  SAB KUCH EDITION")

    for name, val in (("ADJUST_API_TOKEN", ADJUST_API_TOKEN),
                      ("GCP_PROJECT", GCP_PROJECT),
                      ("GCP_CREDENTIALS_JSON", GCP_CREDENTIALS_JSON)):
        if not val.strip():
            die(f"Env var {name} set nahi hai.")

    tokens = load_app_tokens()
    end    = date.today() - timedelta(days=1)
    start  = end - timedelta(days=LOOKBACK_DAYS - 1)
    period = f"{start.isoformat()}:{end.isoformat()}"

    active = [r for r in REPORTS if r["key"] in ENABLED_REPORTS]
    if not active:
        die(f"REPORTS='{','.join(ENABLED_REPORTS)}' se koi report nahi mili. "
            f"Mumkin: {', '.join(r['key'] for r in REPORTS)}")

    log.info("   Window : %s → %s  (%d din)", start, end, LOOKBACK_DAYS)
    log.info("   Apps   : %d  |  chunk: %s", len(tokens),
             CHUNK_SIZE if CHUNK_SIZE > 0 else "sab ek saath")
    log.info("   Dataset: %s.%s (%s)", GCP_PROJECT, BQ_DATASET, BQ_LOCATION)
    log.info("   DRY_RUN: %s  |  run_id: %s", DRY_RUN, RUN_ID)
    log.info("   Reports:")
    for r in active:
        log.info("      • %-28s %s", r["table"], r["desc"])

    client = None if DRY_RUN else get_bq_client()

    for report in active:
        key, table = report["key"], report["table"]
        log.info("")
        log.info("═" * 74)
        log.info("📊 %s  —  %s", table, report["desc"])
        log.info("═" * 74)

        # 🤝 pehle tay karo ke Adjust is grain pe kya deta hai
        dims, mets = negotiate(report, tokens[0], period)
        schema = build_schema(dims, mets)
        log.info("   Schema: %d columns (%d dimension + %d metric + derived/audit)",
                 len(schema), len(dims), len(mets))

        all_rows, ok_tokens = [], set()
        chunks = list(chunked(tokens, CHUNK_SIZE))
        for i, chunk in enumerate(chunks, 1):
            label = f"[{key}] chunk {i}/{len(chunks)} ({len(chunk)} apps)"
            rows, ok = fetch_chunk(report, chunk, start, end, dims, mets, label)
            if rows is None:
                # 🛡️ #2 + #5 — in apps ka data BigQuery mein CHHUA NAHI jayega
                record_failure(label, "fetch fail — in apps ka data CHHUA NAHI jayega")
                continue
            all_rows.extend(rows)
            ok_tokens |= ok

        all_ok = (len(ok_tokens) == len(tokens))
        log.info("   Kul: %s rows, %d/%d apps kaamyab",
                 f"{len(all_rows):,}", len(ok_tokens), len(tokens))

        if DRY_RUN:
            log.info("   [DRY_RUN] BigQuery chhua nahi")
            if all_rows:
                log.info("   [DRY_RUN] sample: %s",
                         json.dumps(all_rows[0], indent=2)[:1200])
            continue

        if not ok_tokens:
            record_failure(table, "koi chunk kaamyab nahi — kuch nahi kiya")
            continue

        tbl_ref = ensure_table(client, table, schema, report["cluster"])
        replace_window(client, tbl_ref, table, all_rows, schema,
                       ok_tokens, start, end, all_ok)

    # 🛡️ #6 — exit code sach bolta hai
    log.info("")
    if FAILURES:
        log.error("=" * 74)
        log.error("🔴 %d MASLE — run FAIL samjha jayega:", len(FAILURES))
        for f in FAILURES:
            log.error("   • %s", f)
        log.error("=" * 74)
        log.error("Jin apps ka fetch fail hua, unka purana data CHHUA NAHI gaya.")
        sys.exit(1)

    log.info("✅ Mukammal — %d report, koi masla nahi.", len(active))


if __name__ == "__main__":
    main()
