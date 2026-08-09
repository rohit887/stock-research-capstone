# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Price ingestion (Massive daily bars)
# MAGIC
# MAGIC Loads ~13 months of daily OHLCV for the 36 scope tickers into `companies`
# MAGIC and `price_history`.
# MAGIC
# MAGIC **Plain Python on purpose — not Spark.** At ~3 requests/min the job is
# MAGIC rate-limited, not compute-limited, so Spark parallelism buys nothing (and would
# MAGIC just trip the 429 limit faster). This is a deliberate design choice; see README.
# MAGIC
# MAGIC **Resilience:** per-ticker checkpoint (skips tickers already loaded),
# MAGIC exponential backoff on 429, upsert so re-runs are idempotent.
# MAGIC
# MAGIC **Auth:** native `student` password (session affinity → reliable multi-row
# MAGIC writes), same as `00_setup.py`. Massive key from secret `capstone/massive_api_key`.
# MAGIC
# MAGIC **Checkpoint:** `companies` = 36 and `price_history` ≈ 276 trading days × 36.

# COMMAND ----------

# MAGIC %pip install "databricks-sdk>=0.125.0"

# COMMAND ----------

# Config widgets. lakebase_instance is the only required value; host auto-derives.
dbutils.widgets.text("lakebase_instance", "projects/stock-research-capstone/branches/production", "Lakebase instance / resource name  *REQUIRED*")
dbutils.widgets.text("pg_database", "databricks_postgres", "Database name")
dbutils.widgets.text("pg_host", "", "Host override (blank = auto from endpoint)")
dbutils.widgets.text("pg_user", "student", "Postgres role (native password auth)")
dbutils.widgets.text("pg_port", "5432", "Port")
dbutils.widgets.text("from_date", "", "History start YYYY-MM-DD (blank = ~13 months back)")
dbutils.widgets.text("to_date", "", "History end YYYY-MM-DD (blank = today)")
dbutils.widgets.text("req_interval_s", "21", "Seconds between API calls (~3/min)")
dbutils.widgets.dropdown("force_reload", "false", ["false", "true"], "Re-fetch tickers already loaded")

# COMMAND ----------

# DBTITLE 1,Connection (native password, same pattern as 00_setup)
import os
import time
import datetime
import psycopg2
from psycopg2.extras import execute_values
import requests
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

LAKEBASE_INSTANCE = dbutils.widgets.get("lakebase_instance").strip()
PG_DATABASE = dbutils.widgets.get("pg_database").strip() or "databricks_postgres"
PG_USER = dbutils.widgets.get("pg_user").strip() or "student"
PG_PORT = int(dbutils.widgets.get("pg_port").strip() or "5432")
assert LAKEBASE_INSTANCE, "Set the 'lakebase_instance' widget."

# Derive host from the Lakebase endpoint via the postgres SDK surface.
_endpoints = list(w.postgres.list_endpoints(parent=LAKEBASE_INSTANCE))
assert _endpoints, f"No endpoints for {LAKEBASE_INSTANCE}. Is the compute active?"
_primary = w.postgres.get_endpoint(name=_endpoints[0].name)
_auto_host = _primary.status.hosts.host if _primary.status and _primary.status.hosts else None
PG_HOST = dbutils.widgets.get("pg_host").strip() or _auto_host
assert PG_HOST, "Could not derive host; set the pg_host widget."


def _password() -> str:
    # Native password (session affinity) for reliable writes. Never hardcoded.
    return dbutils.secrets.get("lakebase", "password")


def get_connection():
    return psycopg2.connect(
        host=PG_HOST, dbname=PG_DATABASE, user=PG_USER,
        password=_password(), port=PG_PORT, sslmode="require",
    )


with get_connection() as _c, _c.cursor() as _cur:
    _cur.execute("SELECT current_user, current_database();")
    print("Connected:", _cur.fetchone())

# COMMAND ----------

# DBTITLE 1,Tickers, sectors, and the Massive fetch (with backoff)
# 36 S&P 500 large caps across 6 sectors (scope §4). Sector is used to populate
# companies.sector and to enable cross-sector comparative retrieval later.
SECTORS = {
    "Mega-cap tech":            ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "AVGO", "ORCL", "CRM", "ADBE"],
    "Semiconductors & hardware":["AMD", "INTC", "QCOM", "TXN", "MU"],
    "Financials":               ["JPM", "BAC", "GS", "MS", "V", "MA"],
    "Healthcare":               ["UNH", "JNJ", "LLY", "PFE", "ABBV"],
    "Consumer":                 ["WMT", "COST", "HD", "MCD", "NKE"],
    "Energy & industrial":      ["XOM", "CVX", "CAT", "BA", "GE"],
}
SECTOR = {t: s for s, ts in SECTORS.items() for t in ts}
TICKERS = list(SECTOR.keys())
assert len(TICKERS) == 36, f"expected 36 tickers, got {len(TICKERS)}"

MASSIVE_BASE = "https://api.massive.com"
MASSIVE_KEY = dbutils.secrets.get("capstone", "massive_api_key")

# Date range: default ~13 months back (Massive free-tier ceiling ~276 trading days).
_today = datetime.date.today()
FROM_DATE = dbutils.widgets.get("from_date").strip() or (_today - datetime.timedelta(days=400)).isoformat()
TO_DATE = dbutils.widgets.get("to_date").strip() or _today.isoformat()
REQ_INTERVAL = float(dbutils.widgets.get("req_interval_s").strip() or "21")
FORCE = dbutils.widgets.get("force_reload") == "true"
print(f"range {FROM_DATE} -> {TO_DATE}   interval={REQ_INTERVAL}s   force={FORCE}")


def fetch_daily_bars(ticker: str, max_retries: int = 6):
    """Daily OHLCV from Massive. Exponential backoff on 429 starting ~25s.

    Returns the list of bar dicts (keys t,o,h,l,c,v). Raises on auth errors.
    """
    url = f"{MASSIVE_BASE}/v2/aggs/ticker/{ticker}/range/1/day/{FROM_DATE}/{TO_DATE}"
    params = {"apiKey": MASSIVE_KEY, "adjusted": "true", "sort": "asc", "limit": 50000}
    backoff = 25
    for attempt in range(1, max_retries + 1):
        r = requests.get(url, params=params, timeout=60)
        if r.status_code == 200:
            return r.json().get("results") or []
        if r.status_code == 429:
            print(f"    429 rate-limited; sleeping {backoff}s (attempt {attempt}/{max_retries})")
            time.sleep(backoff)
            backoff *= 2
            continue
        if r.status_code in (401, 403):
            raise RuntimeError(f"{ticker}: auth/entitlement error {r.status_code}: {r.text[:200]}")
        # transient (5xx / network) — brief pause and retry
        print(f"    {r.status_code}; retry in 5s")
        time.sleep(5)
    raise RuntimeError(f"{ticker}: exhausted {max_retries} retries")

# COMMAND ----------

# DBTITLE 1,Upsert helpers
def upsert_company(conn, ticker: str, sector: str, name: str | None = None):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO companies (ticker, company_name, sector)
            VALUES (%s, %s, %s)
            ON CONFLICT (ticker) DO UPDATE SET
                company_name = COALESCE(EXCLUDED.company_name, companies.company_name),
                sector       = EXCLUDED.sector,
                updated_at   = now();
            """,
            (ticker, name, sector),
        )
    conn.commit()


def upsert_prices(conn, ticker: str, bars: list) -> int:
    rows = []
    for b in bars:
        # Massive/Polygon daily timestamp `t` is epoch milliseconds.
        d = datetime.datetime.utcfromtimestamp(b["t"] / 1000).date()
        rows.append((ticker, d, b.get("o"), b.get("h"), b.get("l"), b.get("c"), b.get("v")))
    if not rows:
        return 0
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO price_history (ticker, trade_date, open, high, low, close, volume)
            VALUES %s
            ON CONFLICT (ticker, trade_date) DO UPDATE SET
                open   = EXCLUDED.open,  high = EXCLUDED.high,
                low    = EXCLUDED.low,   close = EXCLUDED.close,
                volume = EXCLUDED.volume, updated_at = now();
            """,
            rows,
        )
    conn.commit()
    return len(rows)


def already_loaded(conn, ticker: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM price_history WHERE ticker = %s;", (ticker,))
        return cur.fetchone()[0] > 0

# COMMAND ----------

# DBTITLE 1,Ingest — checkpointed, rate-limited
conn = get_connection()
loaded, skipped, failed = [], [], []

for i, ticker in enumerate(TICKERS, 1):
    upsert_company(conn, ticker, SECTOR[ticker])  # always keep companies current

    if not FORCE and already_loaded(conn, ticker):
        print(f"[{i:02d}/36] {ticker:5s} already loaded — skip")
        skipped.append(ticker)
        continue

    try:
        bars = fetch_daily_bars(ticker)
        n = upsert_prices(conn, ticker, bars)
        print(f"[{i:02d}/36] {ticker:5s} {n} bars")
        loaded.append(ticker)
    except Exception as e:  # noqa: BLE001
        print(f"[{i:02d}/36] {ticker:5s} FAILED: {e}")
        failed.append(ticker)

    time.sleep(REQ_INTERVAL)  # ~3 req/min ceiling

print(f"\nloaded={len(loaded)}  skipped={len(skipped)}  failed={len(failed)}")
if failed:
    print(f"failed tickers (re-run to retry, checkpoint skips the rest): {failed}")

# COMMAND ----------

# DBTITLE 1,Verify — checkpoint counts
with get_connection() as conn, conn.cursor() as cur:
    cur.execute("SELECT count(*) FROM companies;")
    print("companies:", cur.fetchone()[0], "(expect 36)")

    cur.execute("SELECT count(*), count(DISTINCT ticker) FROM price_history;")
    total, tick = cur.fetchone()
    print(f"price_history: {total} rows across {tick} tickers (expect ~276 x 36)")

    cur.execute("""
        SELECT ticker, count(*) AS bars, min(trade_date) AS first, max(trade_date) AS last
        FROM price_history GROUP BY ticker ORDER BY bars ASC LIMIT 5;
    """)
    print("\nfewest-bar tickers (watch for short/failed loads):")
    for row in cur.fetchall():
        print("  ", row)

    cur.execute("SELECT ticker FROM companies c WHERE NOT EXISTS "
                "(SELECT 1 FROM price_history p WHERE p.ticker = c.ticker);")
    missing = [r[0] for r in cur.fetchall()]
    print("\ntickers with NO price rows:", missing if missing else "none")

assert not missing, f"tickers missing price data: {missing}"
print("\nPhase 1 checkpoint PASSED — prices loaded for all 36 tickers.")
