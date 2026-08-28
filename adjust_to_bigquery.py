"""
Adjust Report Service  →  BigQuery                                      v1.0
============================================================================
200 apps ke liye rozana aggregated performance data, GitHub Actions se.

Endpoint : https://automate.adjust.com/reports-service/report   (Report Service)
           ⚠️ Legacy KPI Service (v1) DEPRECATED hai — wo use NAHI ki gayi.
Auth     : Authorization: Bearer <ADJUST_API_TOKEN>

────────────────────────────────────────────────────────────────────────────
YE SCRIPT KYUN AISI LIKHI GAYI — 10 ASLI BUGS SE SEEKHA
────────────────────────────────────────────────────────────────────────────
Terafort ke 12 loaders ka audit karte hue ye 10 bugs mile (har ek se asli
data ka nuqsan hua). Naye loader mein wahi ghaltiyan dobara na hon, isliye
har ek ka guard pehle din se lagaya gaya hai:

 #1  GLOBAL DELETE
     Purane loader `DELETE WHERE date BETWEEN start AND end` karte the,
     phir sirf maujooda entities load karte the. Jo entity list se girti
     (disabled hui, fetch fail hua) uska data UR JATA tha.
     → Yahan: DELETE mein `AND app_token IN UNNEST(@ok_tokens)`.
       Sirf un apps ka data hatata hai jo IS RUN mein KAAMYABI se aaye.

 #2  FAIL == KHALI
     Fetch fail hone pe `return []` hota tha — "koi data nahi" se farq hi
     nahi tha. Baqi apps ke rows DELETE trigger kar dete the.
     → Yahan: fail pe `None`. None wala chunk ok_tokens mein nahi aata.

 #3  BE-HAD POLLING / TIMEOUT
     `while status != done` bina limit ke — ek run 80% pe atka raha aur
     1 ghante baad GitHub ne maar diya.
     → Yahan: har request pe REQUEST_TIMEOUT, aur bounded retries.

 #4  ERROR CHUP-CHAAP NIGAL JANA
     `.json().get("data", [])` — API error pe khali list milti thi.
     → Yahan: har response ka body parse hota hai, error uthaya jata hai.

 #5  ENTITY KHAMOSHI SE GIRNA
     `if status == 1` filter ne disabled account list se nikala, phir #1 ne
     uska data uda diya ($1,411 + 221 ads).
     → Yahan: app token list se koi token khamoshi se nahi girta —
       jo fail ho wo FAILURES mein darj hota hai aur exit(1) karta hai.

 #6  EXIT CODE HAMESHA 0
     Sab kuch `log.warning` tha — workflow green dikhta tha jabke data ur
     chuka hota tha.
     → Yahan: FAILURES list → aakhir mein sys.exit(1).

 #7  TRUNCATE + STREAMING INSERT = KHAMOSH ROW LOSS  ⚠️ sab se chhupa hua
     `TRUNCATE` ke 1 second baad `insert_rows_json` — BigQuery ka streaming
     buffer reset ho jata hai aur rows BINA ERROR ke gir jati hain.
     Saabit: adsets 712 load → 567 mile (−20%), ads 2,417 → 2,010 (−17%).
     DELETE wale tables (streaming nahi) 465 → 465 poore the.
     → Yahan: streaming bilkul nahi. Sirf `load_table_from_json` (load job):
       atomic, buffer-free, MUFT, aur `job.output_rows` se tasdeeq hoti hai.

 #8  KISI EK CALL PE RETRY NA HONA
     Ek function mein retry nahi tha — teen run mein teen baar toota.
     → Yahan: har HTTP call `_request_with_retry()` se guzarta hai.

 #9  ADHOORI DISCOVERY PE AAGE BARHNA
     Account list adhoori aayi (15 ki jagah 10), script chal padi, aur
     dimension tables ka atomic replace 5 accounts ka data uda deta.
     → Yahan: agar KOI BHI chunk fail ho to atomic replace nahi hota —
       sirf kaamyab apps ka scoped replace hota hai.

 #10 ASLI ERROR KA NA DIKHNA
     `raise_for_status()` body padhne se PEHLE raise karta tha, isliye
     "code=80004 rate limit" jaisa asli message dikhta hi nahi tha.
     → Yahan: body pehle parse hoti hai, phir status dekha jata hai.

 + keepalive: GitHub 60 din tak commit na hone pe scheduled workflows KHUD
   band kar deta hai (isi ne pehle ~$27K AdMob aur 30 din FX khaye).
   YAML mein keepalive job shaamil hai.

────────────────────────────────────────────────────────────────────────────
ENV VARS
────────────────────────────────────────────────────────────────────────────
LAZMI:
  ADJUST_API_TOKEN        GitHub Secret
  GCP_PROJECT             GCP project id
  GCP_CREDENTIALS_JSON    service-account JSON (poora, GitHub Secret)

APP TOKENS (do mein se ek):
  ADJUST_APP_TOKENS       comma-separated, ya
  APP_TOKENS_FILE         repo mein file ka path (default: app_tokens.txt)
                          — ek token per line, `#` se comment

OPTIONAL:
  BQ_DATASET              default adjust_data
  BQ_TABLE                default app_daily_performance
  BQ_LOCATION             default US
  LOOKBACK_DAYS           default 30
  CHUNK_SIZE              default 50   (0 = sab ek request mein)
  MAX_RETRIES             default 5
  REQUEST_TIMEOUT         default 180  seconds
  UTC_OFFSET              default +00:00
  ATTRIBUTION_TYPE        default all      (all|click|impression)
  DRY_RUN                 default 0        (1 = BigQuery ko haath na lagao)
============================================================================
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("adjust_to_bq")

# ─── CONFIG ────────────────────────────────────────────────────────────────
ADJUST_ENDPOINT = "https://automate.adjust.com/reports-service/report"

ADJUST_API_TOKEN     = os.environ.get("ADJUST_API_TOKEN", "")
GCP_PROJECT          = os.environ.get("GCP_PROJECT", "")
GCP_CREDENTIALS_JSON = os.environ.get("GCP_CREDENTIALS_JSON", "")

BQ_DATASET  = os.environ.get("BQ_DATASET",  "adjust_data")
BQ_TABLE    = os.environ.get("BQ_TABLE",    "app_daily_performance")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")

LOOKBACK_DAYS    = int(os.environ.get("LOOKBACK_DAYS", "30"))
CHUNK_SIZE       = int(os.environ.get("CHUNK_SIZE", "50"))
MAX_RETRIES      = int(os.environ.get("MAX_RETRIES", "5"))
REQUEST_TIMEOUT  = int(os.environ.get("REQUEST_TIMEOUT", "180"))
UTC_OFFSET       = os.environ.get("UTC_OFFSET", "+00:00")
ATTRIBUTION_TYPE = os.environ.get("ATTRIBUTION_TYPE", "all")
DRY_RUN          = os.environ.get("DRY_RUN", "0") == "1"

APP_TOKENS_ENV  = os.environ.get("ADJUST_APP_TOKENS", "")
APP_TOKENS_FILE = os.environ.get("APP_TOKENS_FILE", "app_tokens.txt")

# Adjust dimensions/metrics.
#   `app_token` bhi maangte hain (sirf `app` naam nahi) — kyunki naam badal
#   sakta hai, token nahi. DELETE guard aur joins isi par chalte hain.
DIMENSIONS = ["day", "app", "app_token"]
METRICS    = ["installs", "clicks", "sessions", "revenue"]

RUN_ID = f"{date.today().isoformat()}-{int(time.time())}"

# 🛡️ #6 — har masla yahan darj hota hai; aakhir mein exit code tay karta hai.
FAILURES = []


def record_failure(where: str, detail) -> None:
    msg = f"{where}: {detail}"
    FAILURES.append(msg)
    log.error("  ❌ %s", msg)


def die(msg: str) -> None:
    """Aisi ghalti jispe BigQuery ko haath lagana hi khatarnak hai."""
    log.error("=" * 70)
    log.error("🔴 %s", msg)
    log.error("   BigQuery ko HAATH NAHI LAGAYA — purana data mehfooz hai.")
    log.error("=" * 70)
    sys.exit(1)


# ─── SCHEMA ────────────────────────────────────────────────────────────────
# Partitioned by `date`, clustered by `app_token`.
#   Kyun: purane loaders (Mintegral advertiser) har run mein ~250 din full
#   refresh karte the — na partition, na cluster. Mehnga aur risky.
#   Partition + cluster se DELETE sirf mutalliqa partitions ko chhuta hai.
SCHEMA = [
    bigquery.SchemaField("date",         "DATE",      mode="REQUIRED",
                         description="Adjust 'day' dimension (UTC_OFFSET ke hisaab se)"),
    bigquery.SchemaField("app_token",    "STRING",    mode="REQUIRED",
                         description="Adjust app token — stable join key"),
    bigquery.SchemaField("app_name",     "STRING",
                         description="Adjust 'app' dimension — badal sakta hai"),
    bigquery.SchemaField("installs",     "INTEGER"),
    bigquery.SchemaField("clicks",       "INTEGER"),
    bigquery.SchemaField("sessions",     "INTEGER"),
    bigquery.SchemaField("revenue",      "FLOAT"),
    bigquery.SchemaField("_ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("_run_id",      "STRING",
                         description="ek run ki saari rows ka sanjha id"),
]


# ─── APP TOKENS ────────────────────────────────────────────────────────────
def load_app_tokens() -> list:
    """
    Env se, warna repo file se. 200 tokens ek secret mein rakhna bhaddha hai,
    isliye file wala rasta bhi maujood hai.

    🛡️ #5 + #9 — khali ya adhoori list pe FORAN ruk jate hain. Purane loaders
       khali list pe bhi aage barh jate the aur global DELETE sab uda deta tha.
    """
    tokens = []

    if APP_TOKENS_ENV.strip():
        tokens = [t.strip() for t in APP_TOKENS_ENV.split(",")]
        src = "ADJUST_APP_TOKENS env"
    elif os.path.exists(APP_TOKENS_FILE):
        with open(APP_TOKENS_FILE, "r", encoding="utf-8") as fh:
            tokens = [ln.split("#", 1)[0].strip() for ln in fh]
        src = f"file {APP_TOKENS_FILE}"
    else:
        die(f"App tokens nahi mile — na ADJUST_APP_TOKENS env mein, "
            f"na {APP_TOKENS_FILE} file mein.")

    # saaf karo: khali, duplicate — lekin tarteeb barqarar rakho
    seen, clean = set(), []
    for t in tokens:
        if t and t not in seen:
            seen.add(t)
            clean.append(t)

    if not clean:
        die(f"{src} mein ek bhi valid app token nahi mila.")

    dupes = len(tokens) - len([t for t in tokens if t]) * 0 - len(clean)
    log.info("App tokens: %d unique (%s)%s",
             len(clean), src, f" — {dupes} duplicate/khali hataye" if dupes else "")
    return clean


def chunked(seq: list, size: int):
    """size=0 → sab ek hi chunk mein (user ne 'sab ek saath' maanga tha)."""
    if size <= 0:
        yield seq
        return
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


# ─── ADJUST API ────────────────────────────────────────────────────────────
# Adjust ke aarzi (transient) haalat — inpe retry karna chahiye.
RETRYABLE_STATUS = {408, 429, 500, 502, 503, 504}


def _request_with_retry(params: dict, label: str):
    """
    Ek Adjust request, retry + backoff ke saath.

    🛡️ #10 — body HAMESHA pehle parse hoti hai, `raise_for_status()` se pehle.
       Purane code mein raise pehle hota tha, isliye API ka asli message
       ("rate limit", "invalid token") kabhi log mein nahi aata tha aur
       retry ka faisla bhi nahi ho pata tha.

    🛡️ #2 — FAIL PE None (khali list NAHI). Caller isay "is chunk ka data
       mat chhedo" samajhta hai.

    🛡️ #3 — har call pe REQUEST_TIMEOUT; koi be-had intezaar nahi.
    """
    headers = {
        "Authorization": f"Bearer {ADJUST_API_TOKEN}",
        "Accept": "application/json",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                ADJUST_ENDPOINT,
                headers=headers,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            # 🛡️ #10: body pehle — status chahe 200 ho ya 500
            try:
                body = resp.json()
            except ValueError:
                body = None

            if resp.status_code == 200 and isinstance(body, dict):
                return body

            # Adjust error ka message nikalo (jitna mile)
            detail = ""
            if isinstance(body, dict):
                detail = (body.get("error")
                          or body.get("message")
                          or body.get("detail")
                          or json.dumps(body)[:300])
            else:
                detail = (resp.text or "")[:300]

            if resp.status_code in (401, 403):
                # permanent — retry bekaar hai
                log.error("  %s: HTTP %s — token/permission ka masla: %s",
                          label, resp.status_code, detail)
                return None

            if resp.status_code in RETRYABLE_STATUS and attempt < MAX_RETRIES:
                wait = min(30 * attempt, 300)
                log.warning("  %s: HTTP %s — %ss wait, retry %d/%d",
                            label, resp.status_code, wait, attempt, MAX_RETRIES)
                log.warning("    Adjust: %s", detail)
                time.sleep(wait)
                continue

            log.error("  %s: HTTP %s — %s", label, resp.status_code, detail)
            return None

        except requests.exceptions.RequestException as exc:
            # network / timeout — ye bhi aarzi hai
            if attempt < MAX_RETRIES:
                wait = min(30 * attempt, 300)
                log.warning("  %s: network error — %ss wait, retry %d/%d: %s",
                            label, wait, attempt, MAX_RETRIES, exc)
                time.sleep(wait)
                continue
            log.error("  %s: %d retries ke baad haar gaye: %s",
                      label, MAX_RETRIES, exc)
            return None

    return None


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


def fetch_chunk(tokens: list, start: date, end: date, label: str):
    """
    Ek chunk (app tokens ka guchha) ka data.

    Returns (rows, ok_tokens):
        rows      — BigQuery ke liye taiyar dicts
        ok_tokens — is chunk ke tokens (sirf tab jab fetch KAAMYAB ho)
    Fail pe (None, set()) — caller in tokens ka purana data BILKUL nahi chhuta.
    """
    params = {
        "app_token__in":    ",".join(tokens),      # ← user ki requirement
        "date_period":      f"{start.isoformat()}:{end.isoformat()}",
        "dimensions":       ",".join(DIMENSIONS),
        "metrics":          ",".join(METRICS),
        "utc_offset":       UTC_OFFSET,
        "attribution_type": ATTRIBUTION_TYPE,
    }

    body = _request_with_retry(params, label)
    if body is None:
        return None, set()

    # Adjust warnings chup-chaap nigal jana khatarnak hai — log karo.
    for w in (body.get("warnings") or []):
        log.warning("  %s: Adjust warning — %s", label, str(w)[:200])

    raw_rows = body.get("rows")
    if raw_rows is None:
        log.error("  %s: response mein 'rows' key hi nahi — %s",
                  label, json.dumps(body)[:300])
        return None, set()

    now_iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
    rows, skipped = [], 0

    for r in raw_rows:
        tok = (r.get("app_token") or "").strip()
        day = (r.get("day") or r.get("date") or "").strip()[:10]

        # 🛡️ Key ke bagair row BigQuery mein daalna bemaani hai — aur
        #    DELETE guard bhi us par nahi chal sakta.
        if not tok or not day:
            skipped += 1
            continue

        rows.append({
            "date":         day,
            "app_token":    tok,
            "app_name":     r.get("app"),
            "installs":     _to_int(r.get("installs")),
            "clicks":       _to_int(r.get("clicks")),
            "sessions":     _to_int(r.get("sessions")),
            "revenue":      _to_float(r.get("revenue")),
            "_ingested_at": now_iso,
            "_run_id":      RUN_ID,
        })

    if skipped:
        log.warning("  %s: %d rows chhodi gayin (app_token/day khali)",
                    label, skipped)

    seen_tokens = {r["app_token"] for r in rows}
    silent = set(tokens) - seen_tokens
    if silent:
        # Ye normal ho sakta hai (us app ka window mein koi activity nahi),
        # lekin log zaroor hona chahiye — #5 wala sabaq.
        log.info("  %s: %d/%d tokens ka koi data nahi aaya (activity nahi?)",
                 label, len(silent), len(tokens))

    log.info("  %s: %d rows, %d apps", label, len(rows), len(seen_tokens))

    # Fetch kaamyab hua — is chunk ke SAARE tokens 'ok' hain, chahe kisi ka
    # data khali ho. Isi se DELETE window sahi banti hai (#1).
    return rows, set(tokens)


# ─── BIGQUERY ──────────────────────────────────────────────────────────────
def get_bq_client() -> bigquery.Client:
    """
    Service-account JSON GitHub Secret se — disk pe kabhi nahi likhi jati.
    """
    if not GCP_CREDENTIALS_JSON.strip():
        die("GCP_CREDENTIALS_JSON env khali hai.")
    try:
        info = json.loads(GCP_CREDENTIALS_JSON)
    except json.JSONDecodeError as exc:
        die(f"GCP_CREDENTIALS_JSON valid JSON nahi hai: {exc}")

    creds = service_account.Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(project=GCP_PROJECT, credentials=creds,
                           location=BQ_LOCATION)


def ensure_dataset_and_table(client: bigquery.Client) -> str:
    """
    Dataset/table na hon to bana do. Maujood table ka schema ALIGN karo —
    sirf naye columns ADD hote hain, koi column kabhi hataya/badla nahi jata.
    """
    ds_ref = f"{GCP_PROJECT}.{BQ_DATASET}"
    try:
        client.get_dataset(ds_ref)
    except gexc.NotFound:
        log.info("Dataset %s bana rahe hain (%s)", ds_ref, BQ_LOCATION)
        ds = bigquery.Dataset(ds_ref)
        ds.location = BQ_LOCATION
        client.create_dataset(ds)

    tbl_ref = f"{ds_ref}.{BQ_TABLE}"
    try:
        table = client.get_table(tbl_ref)
    except gexc.NotFound:
        log.info("Table %s bana rahe hain (partition: date, cluster: app_token)",
                 tbl_ref)
        table = bigquery.Table(tbl_ref, schema=SCHEMA)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="date"
        )
        table.clustering_fields = ["app_token"]
        client.create_table(table)
        return tbl_ref

    # ── schema alignment (sirf ADD) ──
    have = {f.name for f in table.schema}
    missing = [f for f in SCHEMA if f.name not in have]
    if missing:
        log.info("Schema align: %d naye column add kar rahe hain — %s",
                 len(missing), [f.name for f in missing])
        # naye columns hamesha NULLABLE (REQUIRED add nahi ho sakta)
        added = [bigquery.SchemaField(f.name, f.field_type, mode="NULLABLE",
                                      description=f.description)
                 for f in missing]
        table.schema = list(table.schema) + added
        client.update_table(table, ["schema"])

    extra = have - {f.name for f in SCHEMA}
    if extra:
        # Kabhi drop nahi karte — downstream (staging/Looker) toot sakta hai.
        log.warning("Table mein extra columns hain (chhode ja rahe hain): %s",
                    sorted(extra))
    return tbl_ref


def replace_window(client: bigquery.Client, tbl_ref: str, rows: list,
                   ok_tokens: set, start: date, end: date,
                   all_tokens_ok: bool) -> None:
    """
    Window ka data badalna — magar SIRF un apps ka jo is run mein aaye.

    🛡️ #1 — DELETE mein `app_token IN UNNEST(@ok)`.
       Purane loaders poore date-range ka DELETE karte the aur phir sirf
       maujooda entities load karte the. Jo entity gir jati, uska data ur
       jata tha (saabit: ek account ka 30-din window rows=0 ho gaya jabke
       window se bahar ka 1,280 rows bacha raha).

    🛡️ #7 — streaming insert BILKUL NAHI. Sirf load job.
       TRUNCATE/DML ke foran baad `insert_rows_json` rows KHAMOSHI se gira
       deta hai (712 → 567 = −20%). Load job atomic hai, buffer use nahi
       karta, MUFT hai, aur `job.output_rows` se ginti verify hoti hai.

    🛡️ #9 — agar koi chunk fail hua to `all_tokens_ok=False`; tab bhi hum
       sirf kaamyab apps ka data replace karte hain — baqi bilkul nahi chhute.
    """
    if not rows:
        # 🛡️ Khali nateeje pe kabhi DELETE nahi — warna poora window ur jata.
        log.warning("  0 rows — DELETE/LOAD kuch nahi kiya (purana data mehfooz)")
        return

    if not ok_tokens:
        record_failure("replace_window", "ok_tokens khali — kuch nahi kiya")
        return

    ok_list = sorted(ok_tokens)

    # ── 1. Scoped DELETE ──
    delete_sql = f"""
        DELETE FROM `{tbl_ref}`
        WHERE date BETWEEN @start AND @end
          AND app_token IN UNNEST(@ok)
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("start", "DATE", start.isoformat()),
        bigquery.ScalarQueryParameter("end",   "DATE", end.isoformat()),
        bigquery.ArrayQueryParameter("ok", "STRING", ok_list),
    ])
    try:
        del_job = client.query(delete_sql, job_config=job_config)
        del_job.result()
        log.info("  Cleared %s → %s for %d/%s apps%s",
                 start, end, len(ok_list), len(ok_list),
                 "" if all_tokens_ok else "  ⚠️ (kuch chunk fail — baqi apps CHHUE NAHI)")
    except Exception as exc:
        record_failure("delete", exc)
        return

    # ── 2. Load job (streaming NAHI) ──
    load_cfg = bigquery.LoadJobConfig(
        schema=SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    try:
        load_job = client.load_table_from_json(rows, tbl_ref, job_config=load_cfg)
        load_job.result()          # error pe raise karta hai
        loaded = load_job.output_rows
    except Exception as exc:
        record_failure("load", exc)
        return

    # 🛡️ #7 — jitne bheje utne hi gaye?
    if loaded != len(rows):
        record_failure("load",
                       f"row count mismatch — bheje {len(rows):,}, gaye {loaded:,}")
    else:
        log.info("  ✅ %s rows → %s (verified)", f"{loaded:,}", tbl_ref)


# ─── MAIN ──────────────────────────────────────────────────────────────────
def main() -> None:
    log.info("🚀 Adjust Report Service → BigQuery  v1.0")

    # ── config validation: BigQuery chhune se PEHLE ──
    for name, val in (("ADJUST_API_TOKEN", ADJUST_API_TOKEN),
                      ("GCP_PROJECT", GCP_PROJECT),
                      ("GCP_CREDENTIALS_JSON", GCP_CREDENTIALS_JSON)):
        if not val.strip():
            die(f"Env var {name} set nahi hai.")

    tokens = load_app_tokens()

    end   = date.today() - timedelta(days=1)          # kal tak (aaj adhoora hai)
    start = end - timedelta(days=LOOKBACK_DAYS - 1)

    log.info("   Window   : %s → %s  (%d din)", start, end, LOOKBACK_DAYS)
    log.info("   Apps     : %d  |  chunk size: %s",
             len(tokens), CHUNK_SIZE if CHUNK_SIZE > 0 else "sab ek saath")
    log.info("   Dest     : %s.%s.%s (%s)",
             GCP_PROJECT, BQ_DATASET, BQ_TABLE, BQ_LOCATION)
    log.info("   DRY_RUN  : %s  |  run_id: %s", DRY_RUN, RUN_ID)

    if CHUNK_SIZE <= 0 and len(tokens) > 100:
        # Imaandar warning: 200 tokens ka query-string ~3 KB+ ho jata hai.
        # Kaam kar sakta hai, lekin ek fail = poora din khali. Chunking se
        # nuqsan sirf us chunk tak mehdood rehta hai (#2 ka amali faida).
        log.warning("   ⚠️  CHUNK_SIZE=0 aur %d tokens — ek hi bara request. "
                    "Fail hua to poora din khali. CHUNK_SIZE=50 mehfooz hai.",
                    len(tokens))

    # ── FETCH ──
    all_rows, ok_tokens = [], set()
    chunks = list(chunked(tokens, CHUNK_SIZE))
    for i, chunk in enumerate(chunks, 1):
        label = f"chunk {i}/{len(chunks)} ({len(chunk)} apps)"
        log.info("Fetching %s ...", label)
        rows, ok = fetch_chunk(chunk, start, end, label)

        if rows is None:
            # 🛡️ #2 + #5 — fail chup-chaap nahi jata; in apps ka data
            #    BigQuery mein bilkul nahi chhua jayega.
            record_failure(label, "fetch fail — in apps ka data CHHUA NAHI jayega")
            continue

        all_rows.extend(rows)
        ok_tokens |= ok

    all_tokens_ok = (len(ok_tokens) == len(tokens))
    log.info("Fetch mukammal: %s rows, %d/%d apps kaamyab",
             f"{len(all_rows):,}", len(ok_tokens), len(tokens))

    if not ok_tokens:
        die("Ek bhi chunk kaamyab nahi hua.")

    # ── LOAD ──
    if DRY_RUN:
        log.info("[DRY_RUN] %s rows taiyar the, %d apps — BigQuery chhua nahi",
                 f"{len(all_rows):,}", len(ok_tokens))
        sample = all_rows[:2]
        log.info("[DRY_RUN] sample: %s", json.dumps(sample, indent=2)[:800])
    else:
        client  = get_bq_client()
        tbl_ref = ensure_dataset_and_table(client)
        replace_window(client, tbl_ref, all_rows, ok_tokens,
                       start, end, all_tokens_ok)

    # ── EXIT CODE (#6) ──
    if FAILURES:
        log.error("=" * 70)
        log.error("🔴 %d MASLE — run FAIL samjha jayega:", len(FAILURES))
        for f in FAILURES:
            log.error("   • %s", f)
        log.error("=" * 70)
        log.error("Jin apps ka fetch fail hua, unka purana data CHHUA NAHI gaya.")
        sys.exit(1)

    log.info("✅ Mukammal — %s rows, %d apps, koi masla nahi.",
             f"{len(all_rows):,}", len(ok_tokens))


if __name__ == "__main__":
    main()
