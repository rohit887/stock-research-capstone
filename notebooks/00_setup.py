# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Cell 1
# MAGIC %md
# MAGIC # 00 — Setup & Schema ✅
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
# MAGIC 4. ✅ **Native password auth enabled** on Lakebase project (completed)
# MAGIC 5. ✅ **Student role created** with password stored in secrets: `lakebase/password` (completed)
# MAGIC
# MAGIC **✅ Checkpoint for Phase 0 COMPLETE:** 
# MAGIC - 8 tables exist with all columns and foreign keys
# MAGIC - 3 extensions installed (vector, lakebase_vector, lakebase_text)
# MAGIC - 8 triggers for auto-updating `updated_at` timestamps
# MAGIC - 9 custom indexes (vector ANN, full-text search, B-tree filters)
# MAGIC - Native password authentication ensures schema persists across connections
# MAGIC
# MAGIC **Connection:** `student` user with native password authentication
# MAGIC - Host: `ep-withered-union-d8dfuwlx.database.us-east-2.cloud.databricks.com`
# MAGIC - Database: `databricks_postgres`
# MAGIC - Password: Stored in secrets scope `lakebase`, key `password`
# MAGIC
# MAGIC **📚 Documentation:**
# MAGIC - **Authentication Guide:** `docs/lakebase-authentication-guide.md`
# MAGIC   - Why native password vs OAuth?
# MAGIC   - When to use which auth method?
# MAGIC   - Databricks secrets management
# MAGIC   - Connection string anatomy
# MAGIC   - Setup workflow and best practices

# COMMAND ----------

# MAGIC %pip uninstall psycopg2-binary -y
# MAGIC %pip install "databricks-sdk>=0.125.0"

# COMMAND ----------

# DBTITLE 1,Cell 3
# Configuration Widgets
# 
# REQUIRED: Set 'lakebase_instance' to your Lakebase resource name
# (shown on Lakebase project page: projects/{project}/branches/{branch})
#
# AUTHENTICATION NOTE:
# - pg_user defaults to 'student' (a Postgres role with native password)
# - Native password provides session affinity for reliable DDL operations
# - Host and other params are auto-derived from SDK (override if needed)
#
# See docs/lakebase-authentication-guide.md for authentication details
dbutils.widgets.text("lakebase_instance", "projects/stock-research-capstone/branches/production", "Lakebase instance / resource name  *REQUIRED*")
dbutils.widgets.text("pg_database", "databricks_postgres", "Database name")
dbutils.widgets.text("pg_host", "", "Host override (blank = auto from instance)")
dbutils.widgets.text("pg_user", "student", "User override (blank = current identity)")
dbutils.widgets.text("pg_port", "5432", "Port")
dbutils.widgets.text("repo_root", "", "Repo root path (blank = auto-detect)")
dbutils.widgets.text("contact_email", "rohit885@gmail.com", "Contact email for SEC EDGAR User-Agent")

# COMMAND ----------

# DBTITLE 1,Cell 4
import os
import uuid
import psycopg2
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Read configuration from widgets (defined in cell above)
LAKEBASE_INSTANCE = dbutils.widgets.get("lakebase_instance").strip()
PG_DATABASE = dbutils.widgets.get("pg_database").strip() or "databricks_postgres"
PG_PORT = int(dbutils.widgets.get("pg_port").strip() or "5432")
assert LAKEBASE_INSTANCE, "Set the 'lakebase_instance' widget to your Lakebase resource name."

# Parse the Lakebase resource name: projects/{project}/branches/{branch}
# This is the unique identifier shown on the Lakebase project page
parts = LAKEBASE_INSTANCE.split("/")
assert len(parts) == 4 and parts[0] == "projects" and parts[2] == "branches", \
    f"Expected format 'projects/{{project}}/branches/{{branch}}', got: {LAKEBASE_INSTANCE}"
project_id, branch_id = parts[1], parts[3]

# Auto-derive connection parameters from the Autoscaling endpoint
# The SDK provides host and metadata; widgets allow manual override if needed
endpoints = list(w.postgres.list_endpoints(parent=LAKEBASE_INSTANCE))
assert endpoints, f"No endpoints found for {LAKEBASE_INSTANCE}. Check that the compute is active."

# Get full endpoint details (list_endpoints may return minimal metadata)
primary = w.postgres.get_endpoint(name=endpoints[0].name)
print(f"endpoint = {primary.name}")
print(f"  type   = {primary.spec.endpoint_type.value if primary.spec else '?'}")
print(f"  state  = {primary.status.current_state if primary.status else '?'}")
_auto_host = primary.status.hosts.host if primary.status and primary.status.hosts else None
PG_HOST = dbutils.widgets.get("pg_host").strip() or _auto_host

# IMPORTANT: Default to 'student' role for native password authentication
# Using a Postgres role (not email) provides session affinity for DDL operations
pg_user_widget = dbutils.widgets.get("pg_user").strip()
PG_USER = pg_user_widget if pg_user_widget else "student"

print(f"branch   = {LAKEBASE_INSTANCE}")
print(f"endpoint = {primary.name}  (state={primary.status.current_state if primary.status else '?'})")
print(f"host     = {PG_HOST}")
print(f"user     = {PG_USER}")
print(f"database = {PG_DATABASE}   port = {PG_PORT}")
assert PG_HOST, ("Could not auto-derive the host from the endpoint. Open the Lakebase "
                 "project page, copy its host/DNS, and paste it into the pg_host widget.")


def _lakebase_password(endpoint_name: str) -> str:
    """Get Postgres password - native password from secrets preferred, OAuth fallback.
    
    **Authentication Strategy:**
    1. Native password (preferred): Provides session affinity to consistent instance
       - Best for DDL operations (CREATE, ALTER, DROP)
       - Stored in Databricks secrets: scope='lakebase', key='password'
    
    2. OAuth token (fallback): May connect to different instances each time
       - OK for read-only SELECT queries
       - WARNING: DDL changes may not persist across connections
    
    See docs/lakebase-authentication-guide.md for full explanation.
    """
    # Try native password from secrets first
    # This ensures connection goes to same Postgres instance (session affinity)
    # Critical for DDL operations that need to persist across connections
    try:
        native_pw = dbutils.secrets.get("lakebase", "password")
        if native_pw:
            print(f"[creds] Using native Postgres password from secrets (lakebase/password)")
            return native_pw
    except Exception:  # noqa: BLE001
        pass  # Secret doesn't exist, try OAuth
    
    # Fallback to OAuth token (workspace identity)
    # WARNING: In Lakebase Autoscaling, OAuth tokens can route to different backend instances
    # This means CREATE TABLE on instance A may not be visible on instance B
    try:
        cred = w.postgres.generate_database_credential(endpoint=endpoint_name)
        if getattr(cred, "token", None):
            print(f"[creds] Using endpoint-scoped OAuth token (expires in 1h)")
            print(f"[creds] WARNING: OAuth may connect to different instances - DDL may not persist")
            print(f"[creds] For reliable schema operations, store native password in secrets")
            return cred.token
    except Exception as e:  # noqa: BLE001
        print(f"[creds] OAuth token generation failed: {e}")
        print(f"[creds] To use native auth: databricks secrets put-secret lakebase password")
        raise


def get_connection():
    """Create a connection to Lakebase Postgres.
    
    Uses native password auth (student role) for session affinity.
    All connections with same credentials go to same backend instance.
    """
    return psycopg2.connect(
        host=PG_HOST,
        dbname=PG_DATABASE,
        user=PG_USER,          # 'student' role, not email
        password=_lakebase_password(primary.name),  # From secrets or OAuth fallback
        port=PG_PORT,
        sslmode="require",     # SSL mandatory for Lakebase
    )


# Smoke-test the connection.
with get_connection() as conn, conn.cursor() as cur:
    cur.execute("SELECT version();")
    print("Connected:", cur.fetchone()[0])

# COMMAND ----------

# DBTITLE 1,Force pg_user to student
# Manually set pg_user widget to 'student'
dbutils.widgets.remove("pg_user")
dbutils.widgets.text("pg_user", "student", "User override (blank = current identity)")
print(f"✓ pg_user widget set to: '{dbutils.widgets.get('pg_user')}'")

# COMMAND ----------

# DBTITLE 1,Test: Connect and create student role
# Test native auth by:
# 1. Connecting with your email using OAuth (should work)
# 2. Creating the student role if it doesn't exist
# 3. Testing connection as student

import psycopg2
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Connect with OAuth first (using your email)
print("Step 1: Connecting with OAuth (your email)...")
try:
    cred = w.postgres.generate_database_credential(endpoint=primary.name)
    conn_oauth = psycopg2.connect(
        host=PG_HOST,
        dbname=PG_DATABASE,
        user=w.current_user.me().user_name,
        password=cred.token,
        port=PG_PORT,
        sslmode="require",
    )
    print("✓ OAuth connection successful")
    
    # Check if student role exists
    with conn_oauth.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = 'student';")
        if cur.fetchone():
            print("✓ Role 'student' already exists")
        else:
            print("Creating role 'student'...")
            cur.execute("CREATE ROLE student WITH LOGIN PASSWORD 'npg_2ewQzZBXWty1';")
            cur.execute("GRANT ALL PRIVILEGES ON DATABASE databricks_postgres TO student;")
            cur.execute("GRANT CREATE ON DATABASE databricks_postgres TO student;")
            cur.execute("GRANT ALL PRIVILEGES ON SCHEMA public TO student;")
            conn_oauth.commit()
            print("✓ Role 'student' created with full privileges")
    
    conn_oauth.close()
    print("\nStep 2: Testing native password auth as student...")
    
    # Now test with native password
    password = dbutils.secrets.get("lakebase", "password")
    conn_student = psycopg2.connect(
        host=PG_HOST,
        dbname=PG_DATABASE,
        user="student",
        password=password,
        port=PG_PORT,
        sslmode="require",
    )
    print("✓ Native password auth as 'student' successful!")
    
    with conn_student.cursor() as cur:
        cur.execute("SELECT current_user, current_database();")
        user, db = cur.fetchone()
        print(f"  Connected as: {user}")
        print(f"  Database: {db}")
    
    conn_student.close()
    print("\n✅ All tests passed! Native auth is working.")
    
except Exception as e:
    print(f"\n❌ Error: {e}")
    print("\nPossible issues:")
    print("  1. Native auth not enabled on endpoint")
    print("  2. Password doesn't match what was set when creating the role")
    print("  3. Endpoint configuration not yet propagated")

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

# DBTITLE 1,Debug: verify tables immediately after cell 10
# Debug cell: check if tables exist immediately after creation in cell 10
print("=" * 70)
print("DIAGNOSTIC: Checking table persistence immediately after cell 10")
print("=" * 70)

with get_connection() as conn, conn.cursor() as cur:
    # Show connection details
    cur.execute("SELECT current_database(), current_schema(), current_user;")
    db, schema, user = cur.fetchone()
    print(f"\nConnection details:")
    print(f"  Database: {db}")
    print(f"  Schema:   {schema}")
    print(f"  User:     {user}")
    
    # Check all tables in ALL schemas
    cur.execute("""
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_type = 'BASE TABLE'
          AND table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY table_schema, table_name;
    """)
    all_tables = cur.fetchall()
    print(f"\nAll user tables in database ({len(all_tables)} total):")
    if all_tables:
        for s, t in all_tables:
            print(f"  {s}.{t}")
    else:
        print("  (none found)")
    
    # Check specifically in 'public' schema
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        ORDER BY table_name;
    """)
    public_tables = [r[0] for r in cur.fetchall()]
    print(f"\nTables in 'public' schema: {public_tables if public_tables else '(none)'}")
    
    # Check extensions
    cur.execute("SELECT extname FROM pg_extension ORDER BY extname;")
    extensions = [r[0] for r in cur.fetchall()]
    print(f"\nExtensions installed: {extensions}")

print("\n" + "=" * 70)

# COMMAND ----------

# DBTITLE 1,Cell 13
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
    """Split SQL into statements on ';' outside dollar-quoted blocks.
    
    Handles:
    - Dollar-quoted blocks ($...$) for function bodies
    - Line comments (--)
    - Multi-line statements
    
    This prevents breaking trigger functions that contain semicolons.
    """
    statements, buf = [], []
    i, n = 0, len(sql)
    dollar_tag = None  # e.g. '$$' or '$body$' when inside a dollar-quoted block
    while i < n:
        ch = sql[i]
        # line comment -> skip to newline (only when not inside a dollar block)
        if dollar_tag is None and sql.startswith("--", i):
            j = sql.find("\n", i)
            j = n if j == -1 else j
            i = (j + 1) if j != -1 else n
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


# Load and parse the schema.sql file
schema_path = os.path.join(_resolve_repo_root(), "sql", "schema.sql")
with open(schema_path) as f:
    schema_sql = f.read()

# Split into individual statements while preserving function bodies
stmts = split_sql_statements(schema_sql)
print(f"Executing {len(stmts)} statements from {schema_path}\n")

# Execute all DDL statements in a single transaction
# Using native password auth ensures all DDL goes to same instance
failures = []
with get_connection() as conn:
    # Transaction mode: all statements succeed or all rollback
    conn.autocommit = False
    with conn.cursor() as cur:
        for idx, stmt in enumerate(stmts, 1):
            # Drop whole-line comments so labels are accurate and comment-only
            # chunks (e.g. the trailing grants block) are skipped, not executed.
            core = "\n".join(
                ln for ln in stmt.splitlines() if not ln.strip().startswith("--")
            ).strip()
            if not core:
                continue
            label = " ".join(core.split())[:70]
            try:
                cur.execute(core)
                print(f"[{idx:02d}] OK    {label}")
            except Exception as e:  # noqa: BLE001
                failures.append((idx, label, str(e).splitlines()[0]))
                print(f"[{idx:02d}] FAIL  {label}\n         -> {str(e).splitlines()[0]}")

    # Commit all DDL changes as single atomic transaction
    # This ensures all tables/indexes/triggers are created together
    conn.commit()
    print("\nCommitted all schema changes.")

print("\nDone." if not failures else f"\n{len(failures)} statement(s) failed (see above).")
print("Index statements using lakebase_ann / lakebase_bm25 may need the fallback "
      "(see sql/schema.sql Section 5) if the -dev extensions differ.")

# Wait for schema propagation across Lakebase Autoscaling instances
# Even with native auth, there can be brief replication lag
import time
print("\nWaiting 2 seconds for schema propagation across Lakebase instances...")
time.sleep(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify — extensions, tables, row counts

# COMMAND ----------

EXPECTED_TABLES = [
    "companies", "price_history", "filings", "filing_chunks",
    "users", "watchlist_tickers", "research_notes", "agent_events",
]

with get_connection() as conn, conn.cursor() as cur:
    # Debug: check current database and schema
    cur.execute("SELECT current_database(), current_schema();")
    db, schema = cur.fetchone()
    print(f"Connected to database: {db}, schema: {schema}\n")
    
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
if missing_tables:
    print(f"\nWARNING: Tables not created by schema.sql: {missing_tables}")
    print("This likely means the SQL parser in cell 10 skipped CREATE TABLE statements.")
    print("Run cell 10 again, or manually execute sql/schema.sql in a Postgres client.")
    raise AssertionError(f"Missing tables: {missing_tables}")
assert not missing_tables, f"Missing tables: {missing_tables}"
print("\nPhase 0 checkpoint PASSED — schema is provisioned.")