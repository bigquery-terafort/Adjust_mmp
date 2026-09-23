"""
Adjust Report Service -> BigQuery · COMPLETE REPORTING WAREHOUSE v3.0
=====================================================================
Purpose
-------
Pull the maximum practical/defensible coverage available from Adjust's
AGGREGATED Report Service API into BigQuery for product, monetization and UA
analysis.

Important boundary
------------------
Report Service is aggregated data. It is NOT raw user/device-level data.
For user-level installs/sessions/events/reattributions/ad revenue/uninstalls,
also enable Adjust Raw Data Export -> Cloud Storage and load that separately.

Major v3 fixes vs v2.x
----------------------
1. Uses documented `attribution_types` (plural), not `attribution_type`.
2. Uses Adjust Events endpoint to discover real event slugs per app.
3. Uses Filters Data endpoint to discover dimensions/cohort periods/options.
4. Supports D0-D120 cohort periods (plus optional W/M periods).
5. Cohorts are stored LONG: one row per cohort_period, avoiding thousands of
   metric columns and BigQuery column explosion.
6. Event cohorts are stored LONG by event_slug + cohort_period.
7. Stable network IDs (campaign/adgroup/creative IDs) are requested.
8. Metric requests are chunked and merged by dimensions.
9. Supported metrics/dimensions are negotiated with divide-and-conquer probes.
10. Successful zero-row responses correctly CLEAR stale data for the window.
11. Writes use staging + a BigQuery transaction, so failed loads never leave a
    production table half-deleted.
12. Separate spend reconciliation table can store adjust/network/mixed cost.
13. Separate mature + immature cohort snapshots are supported.
14. Rolling cohort window defaults to 150 days so D120 can mature.
15. Catalog tables store what Adjust says is available (dimensions, periods,
    events, filter options) for auditability and future schema changes.

Recommended scheduled defaults
------------------------------
REPORTS=app,campaign,country,cohort,event,event_cohort,spend
LOOKBACK_DAYS=14
COHORT_LOOKBACK_DAYS=150
COHORT_MATURITIES=immature,mature
AD_SPEND_MODE=network
SPEND_RECON_MODES=adjust,network,mixed
ATTRIBUTION_SOURCE=dynamic

For all D0-D120 event cohort periods, set:
EVENT_COHORT_PERIODS=all_days
This can be API-expensive if you have many custom events. The default uses the
most decision-useful periods while the general cohort table still stores D0-D120.

Required env
------------
ADJUST_API_TOKEN
GCP_PROJECT
GCP_CREDENTIALS_JSON
ADJUST_APP_TOKENS   comma separated, OR APP_TOKENS_FILE

Optional env
------------
BQ_DATASET=adjust_data
BQ_LOCATION=US
LOOKBACK_DAYS=14
COHORT_LOOKBACK_DAYS=150
CHUNK_SIZE=25
METRIC_BATCH_SIZE=35
MAX_RETRIES=6
REQUEST_TIMEOUT=300
UTC_OFFSET=+00:00
REPORTING_CURRENCY=USD
AD_SPEND_MODE=network
SPEND_RECON_MODES=adjust,network,mixed
ATTRIBUTION_SOURCE=dynamic
ATTRIBUTION_TYPES=                 # empty = Adjust default/all available
COHORT_MATURITIES=immature,mature
COHORT_PERIODS=all_days            # all_days | key | comma slugs e.g. d0,d1,d7
EVENT_COHORT_PERIODS=key           # all_days | key | comma slugs
INCLUDE_WEEKLY_MONTHLY_COHORTS=0
REPORTS=app,campaign,country,cohort,event,event_cohort,spend
START_DATE=                         # YYYY-MM-DD optional backfill override
END_DATE=                           # YYYY-MM-DD optional backfill override
DRY_RUN=0

Docs checked: 2026-09-23
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
import uuid
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable

import requests
from google.api_core import exceptions as gexc
from google.cloud import bigquery
from google.oauth2 import service_account

# -----------------------------------------------------------------------------
# Logging / constants
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("adjust_v3")

REPORT_ENDPOINT = "https://automate.adjust.com/reports-service/report"
EVENTS_ENDPOINT = "https://automate.adjust.com/reports-service/events"
FILTERS_ENDPOINT = "https://automate.adjust.com/reports-service/filters_data"

ADJUST_API_TOKEN = os.environ.get("ADJUST_API_TOKEN", "").strip()
GCP_PROJECT = os.environ.get("GCP_PROJECT", "").strip()
GCP_CREDENTIALS_JSON = os.environ.get("GCP_CREDENTIALS_JSON", "").strip()

BQ_DATASET = os.environ.get("BQ_DATASET", "adjust_data").strip()
BQ_LOCATION = os.environ.get("BQ_LOCATION", "US").strip()

LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "14"))
COHORT_LOOKBACK_DAYS = int(os.environ.get("COHORT_LOOKBACK_DAYS", "150"))
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", "25"))
METRIC_BATCH_SIZE = int(os.environ.get("METRIC_BATCH_SIZE", "35"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "6"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "300"))
UTC_OFFSET = os.environ.get("UTC_OFFSET", "+00:00").strip()
REPORTING_CURRENCY = os.environ.get("REPORTING_CURRENCY", "USD").strip().upper()
AD_SPEND_MODE = os.environ.get("AD_SPEND_MODE", "network").strip().lower()
SPEND_RECON_MODES = [x.strip().lower() for x in os.environ.get(
    "SPEND_RECON_MODES", "adjust,network,mixed").split(",") if x.strip()]
ATTRIBUTION_SOURCE = os.environ.get("ATTRIBUTION_SOURCE", "dynamic").strip().lower()
ATTRIBUTION_TYPES = [x.strip() for x in os.environ.get("ATTRIBUTION_TYPES", "").split(",") if x.strip()]
COHORT_MATURITIES = [x.strip().lower() for x in os.environ.get(
    "COHORT_MATURITIES", "immature,mature").split(",") if x.strip()]
COHORT_PERIODS_SETTING = os.environ.get("COHORT_PERIODS", "all_days").strip().lower()
EVENT_COHORT_PERIODS_SETTING = os.environ.get("EVENT_COHORT_PERIODS", "key").strip().lower()
INCLUDE_WEEKLY_MONTHLY_COHORTS = os.environ.get("INCLUDE_WEEKLY_MONTHLY_COHORTS", "0") == "1"
DRY_RUN = os.environ.get("DRY_RUN", "0") == "1"

ENABLED_REPORTS = [x.strip().lower() for x in os.environ.get(
    "REPORTS",
    "app,campaign,country,cohort,event,event_cohort,spend",
).split(",") if x.strip()]

APP_TOKENS_ENV = os.environ.get("ADJUST_APP_TOKENS", "")
APP_TOKENS_FILE = os.environ.get("APP_TOKENS_FILE", "app_tokens.txt")
START_DATE_ENV = os.environ.get("START_DATE", "").strip()
END_DATE_ENV = os.environ.get("END_DATE", "").strip()

RUN_ID = f"{date.today().isoformat()}-{int(time.time())}-{uuid.uuid4().hex[:8]}"
FAILURES: list[str] = []

SENTINELS = {"unknown", "missing", "n/a", "na", "-", "none", "null", ""}
RETRYABLE_STATUS = {408, 429, 500, 502, 503, 504}

# -----------------------------------------------------------------------------
# Report definitions
# -----------------------------------------------------------------------------
# Stable IDs are deliberately included. Names can change; IDs are join keys.
REPORTS: dict[str, dict[str, Any]] = {
    "app": {
        "table": "app_daily_performance",
        "dimensions": [
            "day", "app", "app_token", "store_id", "store_type",
            "os_name", "platform", "currency_code",
        ],
        "cluster": ["app_token", "os_name", "store_type"],
        "desc": "app/store/os daily KPIs",
    },
    "campaign": {
        "table": "campaign_daily_performance",
        "dimensions": [
            "day", "app", "app_token", "os_name", "platform",
            "partner_name", "partner", "partner_id", "channel", "network",
            "ad_account_id",
            "campaign", "campaign_network", "campaign_id_network",
            "adgroup", "adgroup_network", "adgroup_id_network",
            "creative", "creative_network", "creative_id_network",
            "source_network", "source_id_network",
            "country_code",
        ],
        "cluster": ["app_token", "partner", "campaign_id_network", "country_code"],
        "desc": "MMP attribution / campaign / adgroup / creative",
    },
    "country": {
        "table": "country_daily_performance",
        "dimensions": [
            "day", "app", "app_token", "os_name", "platform",
            "country", "country_code", "region", "device_type",
            "partner_name", "network",
        ],
        "cluster": ["app_token", "country_code", "os_name"],
        "desc": "country/business-region/device KPIs",
    },
    "spend": {
        "table": "spend_reconciliation_daily",
        "dimensions": [
            "day", "app", "app_token", "partner_name", "partner", "network",
            "ad_account_id", "campaign_network", "campaign_id_network",
            "adgroup_network", "adgroup_id_network",
            "creative_network", "creative_id_network", "country_code",
        ],
        "cluster": ["app_token", "partner", "campaign_id_network"],
        "desc": "adjust/network/mixed spend reconciliation",
    },
}

COHORT_DIMENSIONS = [
    "day", "app", "app_token", "os_name", "platform",
    "partner_name", "partner", "partner_id", "channel", "network",
    "campaign_network", "campaign_id_network",
    "adgroup_network", "adgroup_id_network",
    "creative_network", "creative_id_network",
    "country_code",
]

EVENT_DIMENSIONS = [
    "day", "app", "app_token", "os_name", "platform",
    "partner_name", "partner", "network",
    "campaign_network", "campaign_id_network",
    "country_code",
]

# -----------------------------------------------------------------------------
# Metric candidates
# -----------------------------------------------------------------------------
# These cover core conversion, lifecycle, monetization, cost, fraud, assist,
# subscription and SKAN families. Unsupported/plan-gated metrics are filtered by
# runtime negotiation instead of silently creating empty columns.
BASE_METRICS = [
    # Acquisition / conversion
    "installs", "clicks", "impressions", "sessions", "events",
    "click_conversion_rate", "impression_conversion_rate", "ctr",
    "limit_ad_tracking_installs", "limit_ad_tracking_install_rate",
    "reattributions", "reattribution_reinstalls", "reinstalls",
    "first_reinstalls", "first_uninstalls", "first_reattributions",
    "uninstalls", "uninstall_cohort", "uninstall_rate", "deattributions",
    "gdpr_forgets", "daus", "waus", "maus",
    # ATT
    "att_status_authorized", "att_status_non_determined", "att_status_denied",
    "att_status_restricted", "att_consent_rate",
    # Revenue / ads / payer
    "revenue", "cohort_revenue", "revenue_events", "revenue_per_install",
    "ad_impressions", "ad_revenue", "cohort_ad_revenue", "ad_rpm",
    "all_revenue", "cohort_all_revenue", "arpdau", "arpu",
    # Spend
    "cost", "adjust_cost", "network_cost", "network_cost_diff",
    "click_cost", "impression_cost", "install_cost", "event_cost",
    "paid_clicks", "paid_impressions", "paid_installs",
    "ecpi", "ecpi_all", "network_ecpi", "ecpc", "ecpm",
    "cost_per_install", "gross_profit", "roas", "return_on_investment",
    # Assist / qualifiers
    "assisted_installs", "qualifiers", "impression_based_qualifiers",
    "click_based_qualifiers", "assisted_reattributions", "non_assisted_installs",
    # Fraud basics
    "rejected_installs", "rejected_install_rate", "rejected_installs_anon_ip",
    "rejected_installs_invalid_signature", "rejected_install_invalid_signature_rate",
    # Subscription events / revenue
    "subscrevnt_activation_events", "subscrevnt_billing_retry_events",
    "subscrevnt_cancellation_events", "subscrevnt_discounted_offer_events",
    "subscrevnt_expiration_events", "subscrevnt_first_conversion_events",
    "subscrevnt_grace_period_events", "subscrevnt_on_hold_events",
    "subscrevnt_paused_events", "subscrevnt_price_accepted_events",
    "subscrevnt_reactivation_events", "subscrevnt_refund_events",
    "subscrevnt_renewal_events", "subscrevnt_trial_started_events",
    "subscrevnt_revenue", "subscrevnt_unknown_revenue",
    # InSight / incremental if entitled
    "average_revenue_per_event", "incremental_revenue", "incremental_roas",
    # SKAN common
    "skad_installs", "skad_reinstalls", "skad_total_installs", "skad_qualifiers",
    "invalid_payloads", "valid_conversions", "network_ad_spend_skan",
    "skad_revenue_min_roas", "skad_revenue_est_roas", "skad_revenue_max_roas",
    "skan_total_revenue_min", "skan_total_revenue_est", "skan_total_revenue_max",
    "skan_ad_rpu_min", "skan_ad_rpu_est", "skan_ad_rpu_max",
    "skan_iap_rpu_min", "skan_iap_rpu_est", "skan_iap_rpu_max",
    "general_revenue_events_min", "general_revenue_events_est", "general_revenue_events_max",
]
BASE_METRICS += [f"conversion_{i}" for i in range(1, 7)]
BASE_METRICS += [f"conversion_value_{i}" for i in range(0, 64)]
BASE_METRICS = list(dict.fromkeys(BASE_METRICS))

SPEND_METRICS = [
    "cost", "adjust_cost", "network_cost", "network_cost_diff",
    "click_cost", "impression_cost", "install_cost", "event_cost",
    "paid_clicks", "paid_impressions", "paid_installs",
    "clicks", "impressions", "installs", "network_clicks", "network_impressions",
    "network_installs", "ecpc", "ecpm", "ecpi", "network_ecpi",
]

# Standardized field -> Adjust API metric template.
# Period is lower-case: d0..d120, w0..w52, m0..m36.
COHORT_FAMILIES: dict[str, str] = {
    # Retention / sessions / engagement
    "cohort_size": "cohort_size_{p}",
    "retained_users": "retained_users_{p}",
    "retention_rate": "retention_rate_{p}",
    "sessions": "sessions_{p}",
    "non_install_sessions": "non_install_sessions_{p}",
    "sessions_per_user": "sessions_per_user_{p}",
    "time_spent": "time_spent_{p}",
    "time_spent_rate": "time_spent_rate_{p}",
    "time_spent_per_user": "time_spent_per_user_{p}",
    "time_spent_per_active_user": "time_spent_per_active_user_{p}",
    "time_spent_per_session": "time_spent_per_session_{p}",
    # Ad monetization
    "ad_impressions": "ad_impressions_{p}",
    "ad_impressions_total": "ad_impressions_total_{p}",
    "ad_impressions_total_in_cohort": "ad_impressions_total_in_cohort_{p}",
    "ad_revenue": "ad_revenue_{p}",
    "ad_revenue_total": "ad_revenue_total_{p}",
    "ad_revenue_total_per_user": "ad_revenue_total_per_user_{p}",
    "ad_revenue_total_per_paying_user": "ad_revenue_total_per_paying_user_{p}",
    "ad_revenue_total_in_cohort": "ad_revenue_total_in_cohort_{p}",
    "ad_rpm": "ad_rpm_{p}",
    # IAP revenue
    "revenue": "revenue_{p}",
    "revenue_per_user": "revenue_per_user_{p}",
    "revenue_per_paying_user": "revenue_per_paying_user_{p}",
    "revenue_total": "revenue_total_{p}",
    "revenue_total_per_user": "revenue_total_per_user_{p}",
    "revenue_total_per_paying_user": "revenue_total_per_paying_user_{p}",
    "revenue_total_in_cohort": "revenue_total_in_cohort_{p}",
    "revenue_events": "revenue_events_{p}",
    "revenue_events_total": "revenue_events_total_{p}",
    "revenue_events_per_user": "revenue_events_per_user_{p}",
    "revenue_events_per_paying_user": "revenue_events_per_paying_user_{p}",
    # All revenue / LTV / ROAS
    "all_revenue": "all_revenue_{p}",
    "all_revenue_per_user": "all_revenue_per_user_{p}",
    "all_revenue_total": "all_revenue_total_{p}",
    "all_revenue_total_per_user": "all_revenue_total_per_user_{p}",
    "all_revenue_total_in_cohort": "all_revenue_total_in_cohort_{p}",
    "lifetime_value": "lifetime_value_{p}",
    "lifetime_value_ad": "lifetime_value_ad_{p}",
    "lifetime_value_iap": "lifetime_value_iap_{p}",
    "paying_user_lifetime_value": "paying_user_lifetime_value_{p}",
    "paying_user_lifetime_value_ad": "paying_user_lifetime_value_ad_{p}",
    "paying_user_lifetime_value_iap": "paying_user_lifetime_value_iap_{p}",
    "roas": "roas_{p}",
    "roas_ad": "roas_ad_{p}",
    "roas_iap": "roas_iap_{p}",
    # Payer metrics
    "first_paying_users": "first_paying_users_{p}",
    "first_paying_users_total": "first_paying_users_total_{p}",
    "paying_users": "paying_users_{p}",
    "paying_user_size": "paying_user_size_{p}",
    "paying_users_rate": "paying_users_rate_{p}",
    "paying_user_rate": "paying_user_rate_{p}",
    "paying_users_retention_rate": "paying_users_retention_rate_{p}",
    "retention_rate_paying_users": "retention_rate_paying_users_{p}",
    "first_time_paying_user_conversion_rate": "first_time_paying_user_conversion_rate_{p}",
    "first_time_paying_user_conversion_rate_total": "first_time_paying_user_conversion_rate_total_{p}",
    "paying_user_conversion_rate": "paying_user_conversion_rate_{p}",
    "cost_per_first_time_paying_user_total": "cost_per_paying_user_{p}",
    # Lifecycle
    "deattributions": "deattributions_{p}",
    "deattributions_per_user": "deattributions_per_user_{p}",
    "reattributions": "reattributions_{p}",
    "reattributions_per_user": "reattributions_per_user_{p}",
    "reinstalls": "reinstalls_{p}",
    "first_reinstalls": "first_reinstalls_{p}",
    "uninstalls": "uninstalls_{p}",
    "first_uninstalls": "first_uninstalls_{p}",
    "gdpr_forgets": "gdpr_forgets_{p}",
}

EVENT_COHORT_FAMILIES: dict[str, str] = {
    "events": "{e}_{p}_events_cohort",
    "conversions": "{e}_{p}_conversions_cohort",
    "revenue": "{e}_{p}_revenue_cohort",
    "converted_user_size": "{e}_{p}_converted_user_size_cohort",
    "events_per_conversion": "{e}_{p}_events_per_conversion_cohort",
    "revenue_per_conversion": "{e}_{p}_revenue_per_conversion_cohort",
    "events_rate": "{e}_{p}_events_rate_cohort",
    "conversions_rate": "{e}_{p}_conversions_rate_cohort",
    "events_cost": "{e}_{p}_events_cost_cohort",
    "conversions_cost": "{e}_{p}_conversions_cost_cohort",
    "events_per_period": "{e}_{p}_events_per_period",
    "revenue_per_period": "{e}_{p}_revenue_per_period",
}

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def record_failure(where: str, detail: Any) -> None:
    msg = f"{where}: {detail}"
    FAILURES.append(msg)
    log.error("❌ %s", msg)


def die(msg: str) -> None:
    log.error("=" * 88)
    log.error("🔴 %s", msg)
    log.error("=" * 88)
    raise SystemExit(1)


def _clean(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return None if s.lower() in SENTINELS else s


def _num(v: Any) -> float | None:
    if v in (None, ""):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def chunked(seq: list[Any], size: int) -> Iterable[list[Any]]:
    if size <= 0:
        yield seq
        return
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def parse_date_env(value: str, name: str) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        die(f"{name} must be YYYY-MM-DD, got {value!r}")


def derive_store_keys(store_id: Any, store_type: Any, os_name: Any):
    sid = _clean(store_id)
    if not sid:
        return None, None, None
    os_l = (_clean(os_name) or "").lower()
    st_l = (_clean(store_type) or "").lower()
    if "amazon" in st_l:
        return None, None, sid.upper()
    is_ios = "ios" in os_l or "app_store" in st_l or "itunes" in st_l
    is_android = "android" in os_l or "google" in st_l or "play" in st_l
    if sid.isdigit():
        if is_android:
            return None, None, None
        try:
            return None, int(sid), None
        except ValueError:
            return None, None, None
    if "." in sid:
        if is_ios:
            return None, None, None
        return sid.lower(), None, None
    return None, None, None


def load_app_tokens() -> list[str]:
    if APP_TOKENS_ENV.strip():
        raw = [x.strip() for x in APP_TOKENS_ENV.split(",")]
        source = "ADJUST_APP_TOKENS"
    elif os.path.exists(APP_TOKENS_FILE):
        with open(APP_TOKENS_FILE, "r", encoding="utf-8") as fh:
            raw = [ln.split("#", 1)[0].strip() for ln in fh]
        source = APP_TOKENS_FILE
    else:
        die(f"No app tokens: set ADJUST_APP_TOKENS or provide {APP_TOKENS_FILE}")
    out: list[str] = []
    seen = set()
    for t in raw:
        if t and t not in seen:
            seen.add(t)
            out.append(t)
    if not out:
        die(f"No valid app tokens in {source}")
    log.info("Apps: %d unique tokens (%s)", len(out), source)
    return out


def request_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {ADJUST_API_TOKEN}", "Accept": "application/json"}


def http_get_json(url: str, params: dict[str, Any], label: str, quiet: bool = False):
    """Return (json_body, error). 204 is a successful empty response."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=request_headers(), params=params, timeout=REQUEST_TIMEOUT)
            if r.status_code == 204:
                return {}, ""
            body: Any = None
            try:
                body = r.json()
            except ValueError:
                pass

            if r.status_code == 200:
                return body, ""

            if isinstance(body, dict):
                detail = body.get("error") or body.get("message") or body.get("detail") or json.dumps(body)
            else:
                detail = r.text or ""
            detail = str(detail)[:700]

            if r.status_code in (401, 403):
                die(f"Adjust HTTP {r.status_code}: token/permission problem: {detail}")

            if r.status_code in RETRYABLE_STATUS and attempt < MAX_RETRIES:
                retry_after = r.headers.get("Retry-After")
                wait = int(retry_after) if retry_after and retry_after.isdigit() else min(5 * (2 ** (attempt - 1)), 120)
                if not quiet:
                    log.warning("%s HTTP %s; retry %d/%d in %ss", label, r.status_code, attempt, MAX_RETRIES, wait)
                time.sleep(wait)
                continue
            return None, f"HTTP {r.status_code}: {detail}"
        except requests.exceptions.RequestException as exc:
            if attempt < MAX_RETRIES:
                wait = min(5 * (2 ** (attempt - 1)), 120)
                if not quiet:
                    log.warning("%s network error; retry %d/%d in %ss: %s", label, attempt, MAX_RETRIES, wait, exc)
                time.sleep(wait)
                continue
            return None, f"network: {exc}"
    return None, "retries exhausted"


def base_report_params(tokens: list[str], start: date, end: date, dimensions: list[str], metrics: list[str],
                       *, ad_spend_mode: str | None = None, cohort_maturity: str | None = None) -> dict[str, Any]:
    p: dict[str, Any] = {
        "app_token__in": ",".join(tokens),
        "date_period": f"{start.isoformat()}:{end.isoformat()}",
        "dimensions": ",".join(dimensions),
        "metrics": ",".join(metrics),
        "utc_offset": UTC_OFFSET,
        "format_dates": "false",
        "currency": REPORTING_CURRENCY,
        "attribution_source": ATTRIBUTION_SOURCE,
    }
    if ATTRIBUTION_TYPES:
        p["attribution_types"] = ",".join(ATTRIBUTION_TYPES)
    if ad_spend_mode:
        p["ad_spend_mode"] = ad_spend_mode
    if cohort_maturity:
        p["cohort_maturity"] = cohort_maturity
    return p


def report_call(tokens: list[str], start: date, end: date, dimensions: list[str], metrics: list[str], label: str,
                *, ad_spend_mode: str | None = None, cohort_maturity: str | None = None, quiet: bool = False):
    params = base_report_params(tokens, start, end, dimensions, metrics,
                                ad_spend_mode=ad_spend_mode, cohort_maturity=cohort_maturity)
    body, err = http_get_json(REPORT_ENDPOINT, params, label, quiet=quiet)
    if body is None:
        return None, err, []
    if body == {}:
        return [], "", []
    if not isinstance(body, dict):
        return None, "unexpected non-object report response", []
    warnings = [str(w) for w in (body.get("warnings") or [])]
    rows = body.get("rows")
    if rows is None:
        return None, "response missing rows", warnings
    if not isinstance(rows, list):
        return None, "response rows is not a list", warnings
    return rows, "", warnings

# -----------------------------------------------------------------------------
# Adjust catalog discovery
# -----------------------------------------------------------------------------
def discover_filters() -> dict[str, list[dict[str, Any]]]:
    wanted = [
        "dimensions", "full_cohort_periods", "attribution_types", "ad_spend_mode",
        "cohort_maturity", "currencies", "apps", "apps_network", "attributes",
        "store_type", "os_names", "platform", "partners", "networks",
        "ad_revenue_sources", "iap_revenue_mode", "subscription_revenue_mode",
    ]
    body, err = http_get_json(FILTERS_ENDPOINT, {"required_filters": ",".join(wanted)}, "filters_data")
    if body is None:
        log.warning("Filters Data discovery failed: %s", err)
        return {}
    if not isinstance(body, dict):
        log.warning("Filters Data response not an object")
        return {}
    out: dict[str, list[dict[str, Any]]] = {}
    for k, v in body.items():
        if isinstance(v, list):
            out[k] = [x for x in v if isinstance(x, dict)]
    return out


def discover_events(tokens: list[str]) -> list[dict[str, Any]]:
    body, err = http_get_json(EVENTS_ENDPOINT, {
        "app_token__in": ",".join(tokens),
        "tokens_mapping": "true",
    }, "events")
    if body is None:
        log.warning("Events discovery failed: %s", err)
        return []
    if not isinstance(body, list):
        log.warning("Events endpoint returned unexpected shape: %s", type(body).__name__)
        return []
    return [x for x in body if isinstance(x, dict) and _clean(x.get("id"))]


def catalog_ids(filters: dict[str, list[dict[str, Any]]], key: str) -> set[str]:
    return {str(x.get("id")) for x in filters.get(key, []) if x.get("id") is not None}


def choose_dimensions(desired: list[str], filters: dict[str, list[dict[str, Any]]]) -> list[str]:
    available = catalog_ids(filters, "dimensions")
    # If discovery failed, use documented desired list and let report negotiation catch issues.
    if not available:
        return desired[:]
    return [d for d in desired if d in available]


def normalize_period_id(x: str) -> str | None:
    s = str(x).strip().lower().replace(" ", "")
    # Accept d7, 7d, w3, 3w, m2, 2m.
    m = re.fullmatch(r"([dwm])(\d+)", s)
    if m:
        return f"{m.group(1)}{int(m.group(2))}"
    m = re.fullmatch(r"(\d+)([dwm])", s)
    if m:
        return f"{m.group(2)}{int(m.group(1))}"
    return None


def discovered_periods(filters: dict[str, list[dict[str, Any]]]) -> list[str]:
    out: set[str] = set()
    for item in filters.get("full_cohort_periods", []):
        for value in (item.get("id"), item.get("name"), item.get("short_name")):
            p = normalize_period_id(str(value or ""))
            if p:
                out.add(p)
    # Guaranteed documented fallback.
    if not out:
        out.update(f"d{i}" for i in range(121))
        if INCLUDE_WEEKLY_MONTHLY_COHORTS:
            out.update(f"w{i}" for i in range(53))
            out.update(f"m{i}" for i in range(37))
    return sorted(out, key=period_sort_key)


def period_sort_key(p: str):
    unit = p[0]
    n = int(p[1:])
    return ({"d": 0, "w": 1, "m": 2}.get(unit, 9), n)


def select_periods(setting: str, available: list[str]) -> list[str]:
    key = ["d0", "d1", "d3", "d7", "d14", "d30", "d60", "d90", "d120"]
    if setting in ("all", "all_days"):
        chosen = [p for p in available if p.startswith("d")]
    elif setting == "all_periods":
        chosen = available[:]
    elif setting == "key":
        chosen = [p for p in key if p in set(available)]
    else:
        requested = [normalize_period_id(x) for x in setting.split(",")]
        chosen = [p for p in requested if p and p in set(available)]
    return chosen or [p for p in key if p in set(available)]

# -----------------------------------------------------------------------------
# Negotiation
# -----------------------------------------------------------------------------
_NEG_DIM_CACHE: dict[tuple[str, ...], list[str]] = {}
_NEG_METRIC_CACHE: dict[tuple[tuple[str, ...], tuple[str, ...], str, str], list[str]] = {}


def warnings_are_problem(warnings: list[str]) -> bool:
    if not warnings:
        return False
    text = " ".join(warnings).lower()
    suspicious = ["invalid", "unsupported", "unknown", "not available", "not supported", "metric", "dimension"]
    return any(x in text for x in suspicious)


def negotiate_dimensions(dimensions: list[str], token: str, sample_start: date, sample_end: date) -> list[str]:
    key = tuple(dimensions)
    if key in _NEG_DIM_CACHE:
        return _NEG_DIM_CACHE[key]
    if "day" not in dimensions:
        dimensions = ["day"] + dimensions
    good = ["day"]
    for d in dimensions:
        if d == "day":
            continue
        rows, err, warns = report_call([token], sample_start, sample_end, ["day", d], ["installs"], f"probe_dim:{d}", quiet=True)
        if rows is not None and not warnings_are_problem(warns):
            good.append(d)
        else:
            log.warning("Dimension not usable: %s (%s %s)", d, err, warns[:1])
        time.sleep(0.08)
    # app_token is required for safe warehouse replacement. If unavailable, abort.
    if "app_token" in dimensions and "app_token" not in good:
        die("Adjust did not accept app_token dimension; cannot safely scope BigQuery replacement")
    _NEG_DIM_CACHE[key] = good
    return good


def metric_batch_supported(dimensions: list[str], metrics: list[str], token: str,
                           sample_start: date, sample_end: date, *,
                           ad_spend_mode: str, cohort_maturity: str | None) -> bool:
    rows, err, warns = report_call(
        [token], sample_start, sample_end, dimensions, metrics,
        f"probe_metrics[{len(metrics)}]", ad_spend_mode=ad_spend_mode,
        cohort_maturity=cohort_maturity, quiet=True,
    )
    return rows is not None and not warnings_are_problem(warns)


def negotiate_metrics(dimensions: list[str], candidates: list[str], token: str,
                      sample_start: date, sample_end: date, *,
                      ad_spend_mode: str, cohort_maturity: str | None) -> list[str]:
    # Cache only on the exact candidate universe/config.
    ck = (tuple(dimensions), tuple(candidates), ad_spend_mode or "", cohort_maturity or "")
    if ck in _NEG_METRIC_CACHE:
        return _NEG_METRIC_CACHE[ck]

    def split_probe(items: list[str]) -> list[str]:
        if not items:
            return []
        if metric_batch_supported(dimensions, items, token, sample_start, sample_end,
                                  ad_spend_mode=ad_spend_mode, cohort_maturity=cohort_maturity):
            return items
        if len(items) == 1:
            return []
        mid = len(items) // 2
        return split_probe(items[:mid]) + split_probe(items[mid:])

    supported: list[str] = []
    for batch in chunked(list(dict.fromkeys(candidates)), METRIC_BATCH_SIZE):
        supported.extend(split_probe(batch))
        time.sleep(0.1)
    supported = list(dict.fromkeys(supported))
    dropped = [m for m in candidates if m not in set(supported)]
    log.info("Metrics supported %d/%d", len(supported), len(candidates))
    if dropped:
        log.info("Unsupported/plan-gated metric candidates (%d): %s%s",
                 len(dropped), ", ".join(dropped[:30]), " ..." if len(dropped) > 30 else "")
    _NEG_METRIC_CACHE[ck] = supported
    return supported

# -----------------------------------------------------------------------------
# Fetch + merge metric chunks
# -----------------------------------------------------------------------------
def dim_key(row: dict[str, Any], dimensions: list[str]) -> tuple[Any, ...]:
    return tuple(_clean(row.get(d)) for d in dimensions)


def fetch_merged(tokens: list[str], start: date, end: date, dimensions: list[str], metrics: list[str], label: str,
                 *, ad_spend_mode: str, cohort_maturity: str | None = None):
    """Fetch metric batches and merge them by requested dimensions.

    Returns None on any batch failure. Returns [] on a successful zero-row result.
    """
    merged: dict[tuple[Any, ...], dict[str, Any]] = {}
    any_success = False
    for i, batch in enumerate(chunked(metrics, METRIC_BATCH_SIZE), 1):
        rows, err, warns = report_call(
            tokens, start, end, dimensions, batch,
            f"{label}:metric_batch {i}", ad_spend_mode=ad_spend_mode,
            cohort_maturity=cohort_maturity,
        )
        if rows is None:
            log.error("%s failed: %s", label, err)
            return None
        any_success = True
        for w in warns:
            log.warning("%s Adjust warning: %s", label, w[:500])
        for raw in rows:
            if not isinstance(raw, dict):
                continue
            k = dim_key(raw, dimensions)
            rec = merged.setdefault(k, {d: raw.get(d) for d in dimensions})
            # attr_dependency can contain IDs even when a top-level field is absent.
            dep = raw.get("attr_dependency")
            if isinstance(dep, dict):
                for d in dimensions:
                    if rec.get(d) in (None, "", "unknown", "missing") and d in dep:
                        rec[d] = dep.get(d)
            for m in batch:
                if m in raw:
                    rec[m] = raw.get(m)
        time.sleep(0.05)
    if not any_success:
        return None
    return list(merged.values())

# -----------------------------------------------------------------------------
# BigQuery schema / loading
# -----------------------------------------------------------------------------
def get_bq_client() -> bigquery.Client:
    if not GCP_CREDENTIALS_JSON:
        die("GCP_CREDENTIALS_JSON is empty")
    try:
        info = json.loads(GCP_CREDENTIALS_JSON)
    except json.JSONDecodeError as exc:
        die(f"GCP_CREDENTIALS_JSON invalid: {exc}")
    creds = service_account.Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/cloud-platform"])
    return bigquery.Client(project=GCP_PROJECT, credentials=creds, location=BQ_LOCATION)


def ensure_dataset(client: bigquery.Client) -> None:
    ds_ref = f"{GCP_PROJECT}.{BQ_DATASET}"
    try:
        client.get_dataset(ds_ref)
    except gexc.NotFound:
        ds = bigquery.Dataset(ds_ref)
        ds.location = BQ_LOCATION
        client.create_dataset(ds)
        log.info("Created dataset %s", ds_ref)


def schema_for_flat(dimensions: list[str], metrics: list[str], *, extra_fields: list[bigquery.SchemaField] | None = None):
    fields: list[bigquery.SchemaField] = []
    for d in dimensions:
        if d == "day":
            fields.append(bigquery.SchemaField("date", "DATE", mode="REQUIRED"))
        elif d == "app_token":
            fields.append(bigquery.SchemaField("app_token", "STRING", mode="REQUIRED"))
        else:
            fields.append(bigquery.SchemaField(d, "STRING"))
    if "store_id" in dimensions:
        fields += [
            bigquery.SchemaField("android_package", "STRING"),
            bigquery.SchemaField("apple_id", "INTEGER"),
            bigquery.SchemaField("amazon_asin", "STRING"),
        ]
    for m in metrics:
        fields.append(bigquery.SchemaField(m, "FLOAT"))
    if extra_fields:
        fields.extend(extra_fields)
    fields += [
        bigquery.SchemaField("_ingested_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("_run_id", "STRING", mode="REQUIRED"),
    ]
    return dedupe_schema(fields)


def dedupe_schema(fields: list[bigquery.SchemaField]) -> list[bigquery.SchemaField]:
    out = []
    seen = set()
    for f in fields:
        if f.name not in seen:
            seen.add(f.name)
            out.append(f)
    return out


def cohort_schema(dimensions: list[str], fields: list[str], *, event: bool = False):
    schema = schema_for_flat(dimensions, [], extra_fields=[
        bigquery.SchemaField("cohort_period", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("cohort_maturity", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("ad_spend_mode", "STRING", mode="REQUIRED"),
    ])
    # Insert event metadata before metrics if event cohort table.
    if event:
        # rebuild before audit columns for readability, but BigQuery doesn't care.
        schema = [f for f in schema if f.name not in ("_ingested_at", "_run_id")]
        schema += [
            bigquery.SchemaField("event_slug", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("event_name", "STRING"),
            bigquery.SchemaField("event_short_name", "STRING"),
        ]
    for x in fields:
        schema.append(bigquery.SchemaField(x, "FLOAT"))
    if event:
        schema += [
            bigquery.SchemaField("_ingested_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("_run_id", "STRING", mode="REQUIRED"),
        ]
    return dedupe_schema(schema)


def ensure_table(client: bigquery.Client, table_name: str, schema: list[bigquery.SchemaField], cluster: list[str]) -> str:
    ensure_dataset(client)
    ref = f"{GCP_PROJECT}.{BQ_DATASET}.{table_name}"
    names = {f.name for f in schema}
    cluster_fields = [x for x in cluster if x in names][:4]
    try:
        table = client.get_table(ref)
    except gexc.NotFound:
        table = bigquery.Table(ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field="date")
        if cluster_fields:
            table.clustering_fields = cluster_fields
        client.create_table(table)
        log.info("Created %s", ref)
        return ref

    have = {f.name: f for f in table.schema}
    new_fields = []
    for f in schema:
        if f.name not in have:
            new_fields.append(bigquery.SchemaField(f.name, f.field_type, mode="NULLABLE", description=f.description))
    if new_fields:
        table.schema = list(table.schema) + new_fields
        client.update_table(table, ["schema"])
        log.info("%s schema +%d fields", table_name, len(new_fields))
    return ref


def parse_flat_rows(raw_rows: list[dict[str, Any]], dimensions: list[str], metrics: list[str], *, extras: dict[str, Any] | None = None):
    now = datetime.now(timezone.utc).isoformat()
    out: list[dict[str, Any]] = []
    for raw in raw_rows:
        day = str(raw.get("day") or raw.get("date") or "")[:10]
        tok = str(raw.get("app_token") or "").strip()
        if not day or not tok:
            continue
        row: dict[str, Any] = {"date": day, "app_token": tok}
        for d in dimensions:
            if d in ("day", "app_token"):
                continue
            row[d] = _clean(raw.get(d))
        if "store_id" in dimensions:
            ap, ai, az = derive_store_keys(raw.get("store_id"), raw.get("store_type"), raw.get("os_name"))
            row["android_package"], row["apple_id"], row["amazon_asin"] = ap, ai, az
        for m in metrics:
            if m in raw:
                row[m] = _num(raw.get(m))
        if extras:
            row.update(extras)
        row["_ingested_at"] = now
        row["_run_id"] = RUN_ID
        out.append(row)
    return out


def safe_ident(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Unsafe BigQuery identifier: {name}")
    return name


def atomic_replace_window(client: bigquery.Client, table_name: str, rows: list[dict[str, Any]],
                          schema: list[bigquery.SchemaField], ok_tokens: set[str], start: date, end: date,
                          *, cluster: list[str], extra_delete_sql: str = "", extra_query_params: list[Any] | None = None):
    """Load staging first, then DELETE+INSERT inside one BigQuery transaction.

    Empty successful result intentionally deletes stale data for the successful apps/window.
    """
    if not ok_tokens:
        record_failure(table_name, "no successful app tokens")
        return
    target = ensure_table(client, table_name, schema, cluster)
    cols = [safe_ident(f.name) for f in schema]
    run_suffix = re.sub(r"[^A-Za-z0-9_]", "_", RUN_ID)[-80:]
    staging_name = f"{table_name}__stg_{run_suffix}"
    staging = f"{GCP_PROJECT}.{BQ_DATASET}.{staging_name}"

    try:
        st = bigquery.Table(staging, schema=schema)
        client.create_table(st, exists_ok=False)
        if rows:
            job = client.load_table_from_json(
                rows, staging,
                job_config=bigquery.LoadJobConfig(
                    schema=schema,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                ),
            )
            job.result()
            if job.output_rows != len(rows):
                raise RuntimeError(f"staging row mismatch expected={len(rows)} loaded={job.output_rows}")

        params: list[Any] = [
            bigquery.ScalarQueryParameter("start", "DATE", start.isoformat()),
            bigquery.ScalarQueryParameter("end", "DATE", end.isoformat()),
            bigquery.ArrayQueryParameter("ok", "STRING", sorted(ok_tokens)),
        ]
        if extra_query_params:
            params.extend(extra_query_params)
        where_extra = f"\n AND {extra_delete_sql}" if extra_delete_sql else ""
        col_sql = ", ".join(f"`{c}`" for c in cols)

        # Backward-compatible insert: existing v1/v2 tables may have count metrics
        # (for example installs/clicks/sessions) as INT64 while v3 staging keeps
        # report metrics numeric/FLOAT64 for maximum Adjust API compatibility.
        # BigQuery will not implicitly insert FLOAT64 into INT64.  Build the
        # SELECT list from the *actual target schema* and explicitly cast only
        # when the staging and target types differ.  This preserves historical
        # tables without DROP/RECREATE or destructive schema migrations.
        target_table = client.get_table(target)
        target_types = {f.name: f.field_type.upper() for f in target_table.schema}
        staging_types = {f.name: f.field_type.upper() for f in schema}

        bq_cast_type = {
            "INTEGER": "INT64", "INT64": "INT64",
            "FLOAT": "FLOAT64", "FLOAT64": "FLOAT64",
            "BOOLEAN": "BOOL", "BOOL": "BOOL",
            "STRING": "STRING",
            "DATE": "DATE", "DATETIME": "DATETIME",
            "TIMESTAMP": "TIMESTAMP", "TIME": "TIME",
            "NUMERIC": "NUMERIC", "BIGNUMERIC": "BIGNUMERIC",
            "BYTES": "BYTES",
        }

        select_exprs = []
        casted = []
        for c in cols:
            src_t = staging_types.get(c, "")
            dst_t = target_types.get(c, src_t)
            src_norm = bq_cast_type.get(src_t, src_t)
            dst_norm = bq_cast_type.get(dst_t, dst_t)
            if src_norm and dst_norm and src_norm != dst_norm:
                select_exprs.append(f"CAST(`{c}` AS {dst_norm}) AS `{c}`")
                casted.append(f"{c}:{src_norm}->{dst_norm}")
            else:
                select_exprs.append(f"`{c}`")

        if casted:
            log.info("  [%s] schema compatibility casts: %s", table_name, ", ".join(casted[:25]))
            if len(casted) > 25:
                log.info("  [%s] ... plus %d more casts", table_name, len(casted) - 25)

        select_sql = ", ".join(select_exprs)
        sql = f"""
        BEGIN TRANSACTION;
        DELETE FROM `{target}`
         WHERE date BETWEEN @start AND @end
           AND app_token IN UNNEST(@ok){where_extra};
        INSERT INTO `{target}` ({col_sql})
        SELECT {select_sql} FROM `{staging}`;
        COMMIT TRANSACTION;
        """
        client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
        log.info("✅ %s: replaced %s..%s for %d apps with %d rows", table_name, start, end, len(ok_tokens), len(rows))
    except Exception as exc:
        record_failure(f"atomic_replace[{table_name}]", exc)
    finally:
        try:
            client.delete_table(staging, not_found_ok=True)
        except Exception as exc:
            log.warning("Could not delete staging %s: %s", staging, exc)

# -----------------------------------------------------------------------------
# Catalog BigQuery tables
# -----------------------------------------------------------------------------
def load_simple_snapshot(client: bigquery.Client, table_name: str, rows: list[dict[str, Any]], schema: list[bigquery.SchemaField]):
    ensure_dataset(client)
    ref = f"{GCP_PROJECT}.{BQ_DATASET}.{table_name}"
    try:
        client.get_table(ref)
    except gexc.NotFound:
        client.create_table(bigquery.Table(ref, schema=schema))
    if rows:
        client.load_table_from_json(rows, ref, job_config=bigquery.LoadJobConfig(
            schema=schema, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)).result()
    else:
        client.query(f"TRUNCATE TABLE `{ref}`").result()


def persist_catalogs(client: bigquery.Client, filters: dict[str, list[dict[str, Any]]], events: list[dict[str, Any]], periods: list[str]):
    if DRY_RUN:
        return
    now = datetime.now(timezone.utc).isoformat()
    filter_rows = []
    for kind, items in filters.items():
        for item in items:
            filter_rows.append({
                "filter_type": kind,
                "id": _clean(item.get("id")),
                "name": _clean(item.get("name")),
                "short_name": _clean(item.get("short_name")),
                "section": _clean(item.get("section")),
                "formatting": _clean(item.get("formatting")),
                "description": _clean(item.get("description")),
                "_ingested_at": now,
            })
    load_simple_snapshot(client, "adjust_filter_catalog", filter_rows, [
        bigquery.SchemaField("filter_type", "STRING"), bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("name", "STRING"), bigquery.SchemaField("short_name", "STRING"),
        bigquery.SchemaField("section", "STRING"), bigquery.SchemaField("formatting", "STRING"),
        bigquery.SchemaField("description", "STRING"), bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
    ])

    event_rows = []
    for e in events:
        event_rows.append({
            "event_slug": _clean(e.get("id")), "event_name": _clean(e.get("name")),
            "event_short_name": _clean(e.get("short_name")), "section": _clean(e.get("section")),
            "formatting": _clean(e.get("formatting")), "is_skad_event": bool(e.get("is_skad_event")),
            "app_tokens_json": json.dumps(e.get("app_token") or []),
            "event_tokens_json": json.dumps(e.get("tokens") or []),
            "mapping_json": json.dumps(e.get("app_token_x_event_tokens_mapping") or {}),
            "_ingested_at": now,
        })
    load_simple_snapshot(client, "adjust_event_catalog", event_rows, [
        bigquery.SchemaField("event_slug", "STRING"), bigquery.SchemaField("event_name", "STRING"),
        bigquery.SchemaField("event_short_name", "STRING"), bigquery.SchemaField("section", "STRING"),
        bigquery.SchemaField("formatting", "STRING"), bigquery.SchemaField("is_skad_event", "BOOLEAN"),
        bigquery.SchemaField("app_tokens_json", "STRING"), bigquery.SchemaField("event_tokens_json", "STRING"),
        bigquery.SchemaField("mapping_json", "STRING"), bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
    ])

    period_rows = [{"cohort_period": p, "unit": p[0], "number": int(p[1:]), "_ingested_at": now} for p in periods]
    load_simple_snapshot(client, "adjust_cohort_period_catalog", period_rows, [
        bigquery.SchemaField("cohort_period", "STRING"), bigquery.SchemaField("unit", "STRING"),
        bigquery.SchemaField("number", "INTEGER"), bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
    ])

# -----------------------------------------------------------------------------
# Base reports
# -----------------------------------------------------------------------------
def run_flat_report(client: bigquery.Client | None, report_key: str, tokens: list[str], start: date, end: date,
                    filters: dict[str, list[dict[str, Any]]], sample_start: date, sample_end: date):
    cfg = REPORTS[report_key]
    dims = choose_dimensions(cfg["dimensions"], filters)
    dims = negotiate_dimensions(dims, tokens[0], sample_start, sample_end)
    candidates = SPEND_METRICS if report_key == "spend" else BASE_METRICS
    modes = SPEND_RECON_MODES if report_key == "spend" else [AD_SPEND_MODE]

    all_rows: list[dict[str, Any]] = []
    ok_tokens: set[str] = set()
    union_metrics: list[str] = []

    for mode in modes:
        supported = negotiate_metrics(dims, candidates, tokens[0], sample_start, sample_end,
                                      ad_spend_mode=mode, cohort_maturity=None)
        union_metrics.extend(supported)
        for ci, app_chunk in enumerate(chunked(tokens, CHUNK_SIZE), 1):
            raw = fetch_merged(app_chunk, start, end, dims, supported,
                               f"{report_key}:{mode}:chunk{ci}", ad_spend_mode=mode)
            if raw is None:
                record_failure(f"{report_key}:{mode}:chunk{ci}", "fetch failed")
                continue
            parsed = parse_flat_rows(raw, dims, supported, extras={"ad_spend_mode": mode})
            all_rows.extend(parsed)
            ok_tokens.update(app_chunk)

    union_metrics = list(dict.fromkeys(union_metrics))
    schema = schema_for_flat(dims, union_metrics, extra_fields=[
        bigquery.SchemaField("ad_spend_mode", "STRING", mode="REQUIRED")
    ])
    # Rows fetched under modes with fewer metrics simply leave missing nullable fields.
    if DRY_RUN:
        log.info("[DRY] %s rows=%d metrics=%d sample=%s", cfg["table"], len(all_rows), len(union_metrics), json.dumps(all_rows[:1], default=str)[:800])
        return
    assert client is not None
    if report_key == "spend":
        # Replace all configured modes in one transaction, so no extra filter needed.
        atomic_replace_window(client, cfg["table"], all_rows, schema, ok_tokens, start, end,
                              cluster=cfg["cluster"] + ["ad_spend_mode"])
    else:
        atomic_replace_window(client, cfg["table"], all_rows, schema, ok_tokens, start, end,
                              cluster=cfg["cluster"] + ["ad_spend_mode"],
                              extra_delete_sql="ad_spend_mode = @mode",
                              extra_query_params=[bigquery.ScalarQueryParameter("mode", "STRING", AD_SPEND_MODE)])

# -----------------------------------------------------------------------------
# Cohort report (normalized LONG)
# -----------------------------------------------------------------------------
def build_cohort_metric_map(periods: list[str]):
    slug_to_info: dict[str, tuple[str, str]] = {}
    for p in periods:
        for field, tmpl in COHORT_FAMILIES.items():
            slug = tmpl.format(p=p)
            slug_to_info[slug] = (p, field)
    return slug_to_info


def normalize_cohort_rows(raw_rows: list[dict[str, Any]], dimensions: list[str], supported_metrics: list[str],
                          metric_map: dict[str, tuple[str, str]], maturity: str):
    now = datetime.now(timezone.utc).isoformat()
    out: list[dict[str, Any]] = []
    supported_set = set(supported_metrics)
    by_period: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for slug, (p, field) in metric_map.items():
        if slug in supported_set:
            by_period[p].append((slug, field))

    for raw in raw_rows:
        day = str(raw.get("day") or raw.get("date") or "")[:10]
        tok = str(raw.get("app_token") or "").strip()
        if not day or not tok:
            continue
        base = {"date": day, "app_token": tok}
        for d in dimensions:
            if d not in ("day", "app_token"):
                base[d] = _clean(raw.get(d))
        for p, mappings in by_period.items():
            # Only create the period row if the API actually returned at least one metric key.
            if not any(slug in raw for slug, _ in mappings):
                continue
            rec = dict(base)
            rec["cohort_period"] = p
            rec["cohort_maturity"] = maturity
            rec["ad_spend_mode"] = AD_SPEND_MODE
            for slug, field in mappings:
                if slug in raw:
                    rec[field] = _num(raw.get(slug))
            rec["_ingested_at"] = now
            rec["_run_id"] = RUN_ID
            out.append(rec)
    return out


def run_cohort_report(client: bigquery.Client | None, tokens: list[str], start: date, end: date,
                      filters: dict[str, list[dict[str, Any]]], periods: list[str], sample_start: date, sample_end: date):
    dims = choose_dimensions(COHORT_DIMENSIONS, filters)
    dims = negotiate_dimensions(dims, tokens[0], sample_start, sample_end)
    metric_map = build_cohort_metric_map(periods)
    candidates = list(metric_map.keys())
    fields = list(COHORT_FAMILIES.keys())
    schema = cohort_schema(dims, fields)
    all_rows: list[dict[str, Any]] = []
    ok_tokens: set[str] = set()

    for maturity in COHORT_MATURITIES:
        supported = negotiate_metrics(dims, candidates, tokens[0], sample_start, sample_end,
                                      ad_spend_mode=AD_SPEND_MODE, cohort_maturity=maturity)
        if not supported:
            log.warning("No cohort metrics supported for maturity=%s", maturity)
            continue
        for ci, app_chunk in enumerate(chunked(tokens, CHUNK_SIZE), 1):
            raw = fetch_merged(app_chunk, start, end, dims, supported,
                               f"cohort:{maturity}:chunk{ci}", ad_spend_mode=AD_SPEND_MODE,
                               cohort_maturity=maturity)
            if raw is None:
                record_failure(f"cohort:{maturity}:chunk{ci}", "fetch failed")
                continue
            all_rows.extend(normalize_cohort_rows(raw, dims, supported, metric_map, maturity))
            ok_tokens.update(app_chunk)

    if DRY_RUN:
        log.info("[DRY] cohort rows=%d periods=%d sample=%s", len(all_rows), len(periods), json.dumps(all_rows[:1], default=str)[:900])
        return
    assert client is not None
    atomic_replace_window(client, "cohort_daily_performance", all_rows, schema, ok_tokens, start, end,
                          cluster=["app_token", "partner", "campaign_id_network", "cohort_period"])

# -----------------------------------------------------------------------------
# Event reports
# -----------------------------------------------------------------------------
def event_applies_to_token(event: dict[str, Any], token: str) -> bool:
    app_tokens = event.get("app_token") or []
    if isinstance(app_tokens, list) and app_tokens:
        return token in app_tokens
    mapping = event.get("app_token_x_event_tokens_mapping") or {}
    if isinstance(mapping, dict) and mapping:
        return token in mapping
    return True


def run_event_daily(client: bigquery.Client | None, tokens: list[str], start: date, end: date,
                    filters: dict[str, list[dict[str, Any]]], events: list[dict[str, Any]], sample_start: date, sample_end: date):
    dims = choose_dimensions(EVENT_DIMENSIONS, filters)
    dims = negotiate_dimensions(dims, tokens[0], sample_start, sample_end)
    schema = schema_for_flat(dims, ["event_count"], extra_fields=[
        bigquery.SchemaField("event_slug", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("event_name", "STRING"),
        bigquery.SchemaField("event_short_name", "STRING"),
        bigquery.SchemaField("ad_spend_mode", "STRING", mode="REQUIRED"),
    ])
    all_rows: list[dict[str, Any]] = []
    ok_tokens: set[str] = set()
    now = datetime.now(timezone.utc).isoformat()

    for ev_i, ev in enumerate(events, 1):
        slug = _clean(ev.get("id"))
        if not slug:
            continue
        metric = f"{slug}_events"
        eligible_tokens = [t for t in tokens if event_applies_to_token(ev, t)]
        if not eligible_tokens:
            continue
        supported = negotiate_metrics(dims, [metric], eligible_tokens[0], sample_start, sample_end,
                                      ad_spend_mode=AD_SPEND_MODE, cohort_maturity=None)
        if not supported:
            continue
        for ci, app_chunk in enumerate(chunked(eligible_tokens, CHUNK_SIZE), 1):
            raw = fetch_merged(app_chunk, start, end, dims, supported,
                               f"event:{slug}:chunk{ci}", ad_spend_mode=AD_SPEND_MODE)
            if raw is None:
                record_failure(f"event:{slug}:chunk{ci}", "fetch failed")
                continue
            for rr in raw:
                day = str(rr.get("day") or rr.get("date") or "")[:10]
                tok = str(rr.get("app_token") or "").strip()
                if not day or not tok:
                    continue
                rec = {"date": day, "app_token": tok}
                for d in dims:
                    if d not in ("day", "app_token"):
                        rec[d] = _clean(rr.get(d))
                rec.update({
                    "event_slug": slug,
                    "event_name": _clean(ev.get("name")),
                    "event_short_name": _clean(ev.get("short_name")),
                    "event_count": _num(rr.get(metric)),
                    "ad_spend_mode": AD_SPEND_MODE,
                    "_ingested_at": now,
                    "_run_id": RUN_ID,
                })
                all_rows.append(rec)
            ok_tokens.update(app_chunk)
        if ev_i % 25 == 0:
            log.info("Event daily progress %d/%d", ev_i, len(events))

    if DRY_RUN:
        log.info("[DRY] event_daily rows=%d events=%d", len(all_rows), len(events))
        return
    assert client is not None
    atomic_replace_window(client, "event_daily_performance", all_rows, schema, ok_tokens, start, end,
                          cluster=["app_token", "event_slug", "partner", "campaign_id_network"])


def build_event_cohort_metric_map(event_slug: str, periods: list[str]):
    m: dict[str, tuple[str, str]] = {}
    for p in periods:
        for field, tmpl in EVENT_COHORT_FAMILIES.items():
            slug = tmpl.format(e=event_slug, p=p)
            m[slug] = (p, field)
    return m


def run_event_cohort(client: bigquery.Client | None, tokens: list[str], start: date, end: date,
                     filters: dict[str, list[dict[str, Any]]], events: list[dict[str, Any]], periods: list[str],
                     sample_start: date, sample_end: date):
    dims = choose_dimensions(EVENT_DIMENSIONS, filters)
    dims = negotiate_dimensions(dims, tokens[0], sample_start, sample_end)
    schema = cohort_schema(dims, list(EVENT_COHORT_FAMILIES.keys()), event=True)
    all_rows: list[dict[str, Any]] = []
    ok_tokens: set[str] = set()
    now = datetime.now(timezone.utc).isoformat()

    for maturity in COHORT_MATURITIES:
        for ev_i, ev in enumerate(events, 1):
            slug = _clean(ev.get("id"))
            if not slug:
                continue
            eligible_tokens = [t for t in tokens if event_applies_to_token(ev, t)]
            if not eligible_tokens:
                continue
            metric_map = build_event_cohort_metric_map(slug, periods)
            candidates = list(metric_map.keys())
            supported = negotiate_metrics(dims, candidates, eligible_tokens[0], sample_start, sample_end,
                                          ad_spend_mode=AD_SPEND_MODE, cohort_maturity=maturity)
            if not supported:
                continue
            supported_set = set(supported)
            by_period: dict[str, list[tuple[str, str]]] = defaultdict(list)
            for metric_slug, (p, field) in metric_map.items():
                if metric_slug in supported_set:
                    by_period[p].append((metric_slug, field))

            for ci, app_chunk in enumerate(chunked(eligible_tokens, CHUNK_SIZE), 1):
                raw = fetch_merged(app_chunk, start, end, dims, supported,
                                   f"event_cohort:{maturity}:{slug}:chunk{ci}",
                                   ad_spend_mode=AD_SPEND_MODE, cohort_maturity=maturity)
                if raw is None:
                    record_failure(f"event_cohort:{maturity}:{slug}:chunk{ci}", "fetch failed")
                    continue
                for rr in raw:
                    day = str(rr.get("day") or rr.get("date") or "")[:10]
                    tok = str(rr.get("app_token") or "").strip()
                    if not day or not tok:
                        continue
                    base = {"date": day, "app_token": tok}
                    for d in dims:
                        if d not in ("day", "app_token"):
                            base[d] = _clean(rr.get(d))
                    for p, mappings in by_period.items():
                        if not any(ms in rr for ms, _ in mappings):
                            continue
                        rec = dict(base)
                        rec.update({
                            "cohort_period": p,
                            "cohort_maturity": maturity,
                            "ad_spend_mode": AD_SPEND_MODE,
                            "event_slug": slug,
                            "event_name": _clean(ev.get("name")),
                            "event_short_name": _clean(ev.get("short_name")),
                        })
                        for ms, field in mappings:
                            if ms in rr:
                                rec[field] = _num(rr.get(ms))
                        rec["_ingested_at"] = now
                        rec["_run_id"] = RUN_ID
                        all_rows.append(rec)
                ok_tokens.update(app_chunk)
            if ev_i % 10 == 0:
                log.info("Event cohort [%s] progress %d/%d", maturity, ev_i, len(events))

    if DRY_RUN:
        log.info("[DRY] event_cohort rows=%d events=%d periods=%d", len(all_rows), len(events), len(periods))
        return
    assert client is not None
    atomic_replace_window(client, "event_cohort_daily_performance", all_rows, schema, ok_tokens, start, end,
                          cluster=["app_token", "event_slug", "cohort_period", "campaign_id_network"])

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main() -> None:
    log.info("🚀 Adjust -> BigQuery v3.0 COMPLETE REPORTING WAREHOUSE")
    for name, value in (
        ("ADJUST_API_TOKEN", ADJUST_API_TOKEN),
        ("GCP_PROJECT", GCP_PROJECT),
        ("GCP_CREDENTIALS_JSON", GCP_CREDENTIALS_JSON),
    ):
        if not value:
            die(f"Required env {name} is empty")

    if AD_SPEND_MODE not in {"adjust", "network", "mixed"}:
        die("AD_SPEND_MODE must be adjust, network or mixed")
    bad_maturity = [x for x in COHORT_MATURITIES if x not in {"mature", "immature"}]
    if bad_maturity:
        die(f"Invalid COHORT_MATURITIES: {bad_maturity}")

    tokens = load_app_tokens()
    filters = discover_filters()
    events = discover_events(tokens)
    all_periods = discovered_periods(filters)
    cohort_periods = select_periods(COHORT_PERIODS_SETTING, all_periods)
    event_periods = select_periods(EVENT_COHORT_PERIODS_SETTING, all_periods)

    # Explicit backfill override, otherwise rolling windows.
    end_override = parse_date_env(END_DATE_ENV, "END_DATE")
    start_override = parse_date_env(START_DATE_ENV, "START_DATE")
    end = end_override or (date.today() - timedelta(days=1))
    flat_start = start_override or (end - timedelta(days=max(LOOKBACK_DAYS, 1) - 1))
    cohort_start = start_override or (end - timedelta(days=max(COHORT_LOOKBACK_DAYS, 1) - 1))
    if flat_start > end or cohort_start > end:
        die("Start date is after end date")

    # Probe the last few days. Recent data is more likely to exist.
    sample_end = end
    sample_start = max(date(2020, 1, 1), end - timedelta(days=6))

    log.info("Flat window   : %s -> %s", flat_start, end)
    log.info("Cohort window : %s -> %s", cohort_start, end)
    log.info("Cohort periods: %d (%s..%s)", len(cohort_periods), cohort_periods[0], cohort_periods[-1])
    log.info("Event periods : %d", len(event_periods))
    log.info("Discovered events: %d", len(events))
    log.info("Spend mode=%s | spend recon=%s | attribution_source=%s | currency=%s",
             AD_SPEND_MODE, SPEND_RECON_MODES, ATTRIBUTION_SOURCE, REPORTING_CURRENCY)
    log.info("Reports: %s", ",".join(ENABLED_REPORTS))

    client = None if DRY_RUN else get_bq_client()
    if client:
        persist_catalogs(client, filters, events, all_periods)

    # Base reports
    for key in ("app", "campaign", "country", "spend"):
        if key in ENABLED_REPORTS:
            log.info("=" * 88)
            log.info("📊 %s", key)
            run_flat_report(client, key, tokens, flat_start, end, filters, sample_start, sample_end)

    if "cohort" in ENABLED_REPORTS:
        log.info("=" * 88)
        log.info("📊 cohort D0-D120 normalized")
        run_cohort_report(client, tokens, cohort_start, end, filters, cohort_periods, sample_start, sample_end)

    if "event" in ENABLED_REPORTS:
        log.info("=" * 88)
        log.info("📊 discovered custom events")
        run_event_daily(client, tokens, flat_start, end, filters, events, sample_start, sample_end)

    if "event_cohort" in ENABLED_REPORTS:
        log.info("=" * 88)
        log.info("📊 custom event cohort economics")
        run_event_cohort(client, tokens, cohort_start, end, filters, events, event_periods, sample_start, sample_end)

    if FAILURES:
        log.error("=" * 88)
        log.error("🔴 Completed with %d failures", len(FAILURES))
        for f in FAILURES:
            log.error("  • %s", f)
        raise SystemExit(1)

    log.info("✅ Complete. Run ID: %s", RUN_ID)


if __name__ == "__main__":
    main()
