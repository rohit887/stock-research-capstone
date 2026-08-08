# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup & Schema
# MAGIC
# MAGIC Automates the automatable parts of the scope doc's new-account checklist (§9):
# MAGIC connect to Lakebase, install/verify the three extensions, run `sql/schema.sql`
# MAGIC statement-by-statement, confirm both serving models, and print table row counts.
# MAGIC
# MAGIC **Manual prec: do these first (this notebook cannot):**
# MAGIC 1. LinkedIn identity verification — unlocks outbound internet. Nothing works without it.
# MAGIC 2. Secret scope `capstone` created and Massive key stored:
# MAGIC    `databricks secrets create-scope capstone` / `... put-secret capstone massive_api_key`
# MAGIC 3. Lakebase project created, compute **Active**.
# MAGIC
# MAGIC **Checkpoint for Phase 0:** 8 tables exist, 3 extensions report versions, both models present.

# COMMAND ----------

# MAGIC %pip install "databricks-sdk>=0.125.0" psycopg2-binary
# MAGIC %restart_python

# COMMAND ----------

# Config — the ONLY value you MUST supply is the Lakebase instance name
# (the "resource name" shown on the Lakebase instance page). Host and user are
# auto-derived from it via the SDK; the override widgets stay blank unless the
# auto-derivation fails and you need to paste values manually.
dbutils.widgets.text("lakebase_instance", "", "Lakebase instance / resource name  *REQUIRED*")
dbutils.widgets.text("pg_database", "databricks_postgres", "Database name")
dbutils.widgets.text("pg_host", "", "Host override (blank = auto from instance)")
dbutils.widgets.text("pg_user", "", "User override (blank = current identity)")
dbutils.widgets.text("pg_port", "5432", "Port")
dbutils.widgets.text("repo_root", "", "Repo root path (blank = auto-detect)")
dbutils.widgets.text("contact_email", "rohit885@gmail.com", "Contact email for SEC EDGAR User-Agent")

# COMMAND ----------

import os
import uuid
import psycopg2
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

LAKEBASE_INSTANCE = dbutils.widgets.get("lakebase_instance").strip()
PG_DATABASE = dbutils.widgets.get("pg_database").strip() or "databricks_postgres"
PG_PORT = int(dbutils.widgets.get("pg_port").strip() or "5432")
assert LAKEBASE_INSTANCE, "Set the 'lakebase_instance' widget to your Lakebase resource name."

# Auto-derive host + user from the instance (override widgets win if set).
_instance = w.database.get_database_instance(name=LAKEBASE_INSTANCE)
_auto_host = (getattr(_instance, "read_write_dns", None)
              or getattr(_instance, "dns", None)
              or getattr(_instance, "host", None))
PG_HOST = dbutils.widgets.get("pg_host").strip() or _auto_host
PG_USER = dbutils.widgets.get("pg_user").strip() or w.current_user.me().user_name

print(f"instance = {LAKEBASE_INSTANCE}  (state={getattr(_instance, 'state', '?')})")
print(f"host     = {PG_HOST}")
print(f"user     = {PG_USER}")
print(f"database = {PG_DATABASE}   port = {PG_PORT}")
assert PG_HOST, ("Could not auto-derive the host from the instance. Open the Lakebase "
                 "instance page, copy its host/DNS, and paste it into the pg_host widget.")


def _lakebase_password() -> str:
    """Short-lived credential used as the Postgres password.

    Preferred: Lakebase-native database credential (an OAuth token scoped to the
    instance). Fallback: the workspace auth token from the SDK config.
    """
    try:
        cred = w.database.generate_database_credential(
            request_id=str(uuid.uuid4()),
            instance_names=[LAKEBASE_INSTANCE],
        )
        if getattr(cred, "token", None):
            return cred.token
    except Exception as e:  # noqa: BLE001
        print(f"[creds] generate_database_credential unavailable, falling back: {e}")
    headers = w.config.authenticate()  # {'Authorization': 'Bearer ...'}
    return headers["Authorization"].split(" ", 1)[1]


def get_connection():
    return psycopg2.connect(
        host=PG_HOST,
        dbname=PG_DATABASE,
        user=PG_USER,
        password=_lakebase_password(),
        port=PG_PORT,
        sslmode="require",
    )


# Smoke-test the connection.
with get_connection() as conn, conn.cursor() as cur:
    cur.execute("SELECT version();")
    print("Connected:", cur.fetchone()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Outbound internet / SEC EDGAR reachability
# MAGIC EDGAR needs no key — only a `User-Agent` with a contact email. A 200 here
# MAGIC confirms the per-account outbound allowlist is open (LinkedIn verify done).

# COMMAND ----------

import requests

CONTACT_EMAIL = dbutils.widgets.get("contact_email").strip()
HEADERS = {"User-Agent": f"stock-research-capstone {CONTACT_EMAIL}"}

r = requests.get("https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0000320193&type=10-K&count=1",
                 headers=HEADERS, timeout=30)
print("EDGAR status:", r.status_code, "(expect 200)")
assert r.status_code == 200, "EDGAR unreachable — check LinkedIn identity verification / outbound allowlist."

# COMMAND ----------

# MAGIC %md
# MAGIC ## Serving endpoints — confirm both models present

# COMMAND ----------

wanted = {"databricks-llama-4-maverick", "databricks-gte-large-en"}
present = {e.name for e in w.serving_endpoints.list()}
for m in sorted(wanted):
    print(("  OK  " if m in present else " MISSING ") + m)
missing = wanted - present
assert not missing, f"Missing serving endpoints: {missing}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run sql/schema.sql
# MAGIC Split on top-level semicolons while respecting `$$`-dollar-quoted bodies
# MAGIC (so the `set_updated_at()` trigger function survives), then execute each
# MAGIC statement and report pass/fail. Idempotent — safe to re-run.

# COMMAND ----------

def _resolve_repo_root() -> str:
    override = dbutils.widgets.get("repo_root").strip()
    if override:
        return override
    # In a Databricks Git folder the repo is on the local FS; try common spots.
    for cand in (os.getcwd(), os.path.dirname(os.getcwd())):
        if os.path.exists(os.path.join(cand, "sql", "schema.sql")):
            return cand
    raise FileNotFoundError("Could not locate sql/schema.sql — set the repo_root widget.")


def split_sql_statements(sql: str):
    """Split into statements on ';' outside dollar-quoted blocks and line comments."""
    statements, buf = [], []
    i, n = 0, len(sql)
    dollar_tag = None  # e.g. '$$' or '$body$' when inside a dollar-quoted block
    while i < n:
        ch = sql[i]
        # line comment -> skip to newline (only when not inside a dollar block)
        if dollar_tag is None and sql.startswith("--", i):
            j = sql.find("\n", i)
            j = n if j == -1 else j
            buf.append(sql[i:j])
            i = j
            continue
        if ch == "$":
            end = sql.find("$", i + 1)
            tag = sql[i:end + 1] if end != -1 else None
            if tag and all(c.isalnum() or c == "_" or c == "$" for c in tag):
                if dollar_tag is None:
                    dollar_tag = tag
                    buf.append(tag); i = end + 1; continue
                elif tag == dollar_tag:
                    dollar_tag = None
                    buf.append(tag); i = end + 1; continue
        if ch == ";" and dollar_tag is None:
            stmt = "".join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements


schema_path = os.path.join(_resolve_repo_root(), "sql", "schema.sql")
with open(schema_path) as f:
    schema_sql = f.read()

stmts = split_sql_statements(schema_sql)
print(f"Executing {len(stmts)} statements from {schema_path}\n")

failures = []
with get_connection() as conn:
    conn.autocommit = True
    with conn.cursor() as cur:
        for idx, stmt in enumerate(stmts, 1):
            label = " ".join(stmt.split())[:70]
            try:
                cur.execute(stmt)
                print(f"[{idx:02d}] OK    {label}")
            except Exception as e:  # noqa: BLE001
                failures.append((idx, label, str(e).splitlines()[0]))
                print(f"[{idx:02d}] FAIL  {label}\n         -> {str(e).splitlines()[0]}")

print("\nDone." if not failures else f"\n{len(failures)} statement(s) failed (see above).")
print("Index statements using lakebase_ann / lakebase_bm25 may need the fallback "
      "(see sql/schema.sql Section 5) if the -dev extensions differ.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify — extensions, tables, row counts

# COMMAND ----------

EXPECTED_TABLES = [
    "companies", "price_history", "filings", "filing_chunks",
    "users", "watchlist_tickers", "research_notes", "agent_events",
]

with get_connection() as conn, conn.cursor() as cur:
    cur.execute("""
        SELECT extname, extversion FROM pg_extension
        WHERE extname IN ('vector','lakebase_vector','lakebase_text')
        ORDER BY extname;
    """)
    print("Extensions:")
    for name, ver in cur.fetchall():
        print(f"  {name:16s} {ver}")

    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' ORDER BY table_name;
    """)
    found = {r[0] for r in cur.fetchall()}
    print("\nTables:")
    for t in EXPECTED_TABLES:
        marker = "OK " if t in found else "MISSING"
        n = "-"
        if t in found:
            cur.execute(f"SELECT count(*) FROM {t};")
            n = cur.fetchone()[0]
        print(f"  [{marker}] {t:20s} rows={n}")

missing_tables = set(EXPECTED_TABLES) - found
assert not missing_tables, f"Missing tables: {missing_tables}"
print("\nPhase 0 checkpoint PASSED — schema is provisioned.")
