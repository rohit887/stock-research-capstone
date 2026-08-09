# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "\"databricks-sdk>=0.125.0\"",
# ]
# ///
# MAGIC %md
# MAGIC # 04 — Test adapter modules (before the MCP server)
# MAGIC
# MAGIC Imports `mcp-server/research_store.py` and `mcp-server/market_data.py` and calls
# MAGIC every function directly against Lakebase — the whole point of the adapter split
# MAGIC is that retrieval/price/write logic is debuggable as plain Python **before** the
# MAGIC MCP transport exists (build-order step 5 before step 6).
# MAGIC
# MAGIC Sets the `PG*` / API env vars the modules expect from your secrets, then runs
# MAGIC each of the six tool-backing functions plus the event log.

# COMMAND ----------

# MAGIC %pip install "databricks-sdk>=0.125.0"

# COMMAND ----------

# DBTITLE 1,Install lxml for BeautifulSoup
# MAGIC %pip install lxml

# COMMAND ----------

# DBTITLE 1,Restart Python to load lxml
dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("lakebase_instance", "projects/stock-research-capstone/branches/production", "Lakebase instance / resource name")
dbutils.widgets.text("pg_database", "databricks_postgres", "Database name")
dbutils.widgets.text("pg_host", "", "Host override (blank = auto)")
dbutils.widgets.text("repo_root", "", "Repo root (blank = auto-detect)")
dbutils.widgets.text("test_user", "demo-user", "Test user_id")

# COMMAND ----------

# DBTITLE 1,Wire env vars from secrets, put mcp-server on the path
import os
import sys
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
inst = dbutils.widgets.get("lakebase_instance").strip()
eps = list(w.postgres.list_endpoints(parent=inst))
primary = w.postgres.get_endpoint(name=eps[0].name)
host = dbutils.widgets.get("pg_host").strip() or (primary.status.hosts.host if primary.status and primary.status.hosts else None)
assert host, "Could not derive host; set pg_host."

os.environ["PGHOST"] = host
os.environ["PGDATABASE"] = dbutils.widgets.get("pg_database").strip() or "databricks_postgres"
os.environ["PGUSER"] = "student"
os.environ["PGPASSWORD"] = dbutils.secrets.get("lakebase", "password")
os.environ["PGPORT"] = "5432"
os.environ["MASSIVE_API_KEY"] = dbutils.secrets.get("capstone", "massive_api_key")
os.environ["EDGAR_CONTACT_EMAIL"] = "rohit885@gmail.com"


def _repo_root():
    override = dbutils.widgets.get("repo_root").strip()
    if override:
        return override
    for cand in (os.getcwd(), os.path.dirname(os.getcwd())):
        if os.path.exists(os.path.join(cand, "mcp-server", "research_store.py")):
            return cand
    raise FileNotFoundError("Set repo_root widget to the repo path.")


sys.path.insert(0, os.path.join(_repo_root(), "mcp-server"))
import research_store as rs
import market_data as md
print("imported adapters from", os.path.join(_repo_root(), "mcp-server"))

# COMMAND ----------

# DBTITLE 1,search_filings — hybrid retrieval
for r in rs.search_filings("cyclicality of DRAM and NAND pricing", ticker="MU", top_k=3):
    print(f"[{r['ticker']}] {r['score']:.4f}  {r['chunk_text'][:120].strip()}")
print("---")
for r in rs.search_filings("which chipmakers worry about customer concentration", top_k=3):
    print(f"[{r['ticker']}] {r['score']:.4f}  {r['chunk_text'][:120].strip()}")

# COMMAND ----------

# DBTITLE 1,get_price_summary + compare_tickers
import json
print(json.dumps(rs.get_price_summary("NVDA", "1y"), indent=2))
print(json.dumps(rs.compare_tickers(["NVDA", "AMD", "INTC"], metric="return", period="1y"), indent=2))
print(json.dumps(rs.compare_tickers(["JPM", "BAC", "GS"], metric="volatility", period="6m"), indent=2))

# COMMAND ----------

# DBTITLE 1,assess_risk_signal — the tool that reasons (price + filings)
print(json.dumps(rs.assess_risk_signal("MU"), indent=2))
print(json.dumps(rs.assess_risk_signal("AAPL"), indent=2))

# COMMAND ----------

# DBTITLE 1,Write tools — add_to_watchlist + save_research_note
u = dbutils.widgets.get("test_user").strip()
print(rs.add_to_watchlist(u, "NVDA"))
print(rs.add_to_watchlist(u, "MU"))
print(rs.add_to_watchlist(u, "ZZZZ"))  # unknown ticker -> ok:False
print(rs.save_research_note(u, "NVDA", "Watching foundry-concentration risk into next earnings."))
print("watchlist:", rs.get_watchlist(u))
print("notes:", rs.list_notes(u))

# COMMAND ----------

# DBTITLE 1,log_event — confirm agent_events is capturing
rs.log_event(u, "test message", "search_filings", {"query": "demo"}, 123, 3)
with rs.get_connection() as c, c.cursor() as cur:
    cur.execute("SELECT count(*) FROM agent_events;")
    print("agent_events rows:", cur.fetchone()[0])

# COMMAND ----------

# DBTITLE 1,market_data — third-party HTTP adapter (spot check)
cik = md.resolve_cik("AAPL")
print("AAPL CIK:", cik)
meta = md.latest_10k(cik[0])
print("latest 10-K:", meta["accession"], meta["filing_date"])
txt = md.html_to_text(md.fetch_filing_html(meta["doc_url"]))
sec = md.extract_item_1a(txt)
print("Item 1A chars:", len(sec), "| chunks:", len(md.chunk_text(sec)))

# COMMAND ----------

print("Phase 4 checkpoint: all adapter functions callable directly against Lakebase.")