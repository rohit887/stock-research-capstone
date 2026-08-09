# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — EDGAR filing ingestion (Spark)
# MAGIC
# MAGIC For each of the 36 tickers: resolve its SEC CIK, find the most recent **10-K**,
# MAGIC extract **Item 1A (Risk Factors)**, chunk it, embed each chunk to 1024-dim with
# MAGIC `databricks-gte-large-en`, and load `filings` + `filing_chunks`.
# MAGIC
# MAGIC **Why Spark here (and not for prices):** filing text is unstructured and the
# MAGIC fetch+strip+extract+chunk work is CPU/IO across 36 documents — genuinely
# MAGIC parallelizable. We distribute that stage with an RDD `flatMap`. (Prices were
# MAGIC rate-limited, so Spark bought nothing there — see `01_ingest_prices.py`.)
# MAGIC
# MAGIC **Extraction robustness:** match the **last** occurrence of the Item 1A start
# MAGIC marker (skips the table of contents), end at Item 1B / Item 2. Per-filing
# MAGIC extracted length is logged so parse failures surface immediately.
# MAGIC
# MAGIC **`section` is a column** → adding Item 7 later is just another `section` value.
# MAGIC
# MAGIC **Checkpoint:** `filings` = 36; `filing_chunks` populated with 1024-dim vectors;
# MAGIC no zero-length Item 1A extractions.

# COMMAND ----------

# MAGIC %pip install "databricks-sdk>=0.125.0" beautifulsoup4 lxml

# COMMAND ----------

dbutils.widgets.text("lakebase_instance", "projects/stock-research-capstone/branches/production", "Lakebase instance / resource name  *REQUIRED*")
dbutils.widgets.text("pg_database", "databricks_postgres", "Database name")
dbutils.widgets.text("pg_host", "", "Host override (blank = auto from endpoint)")
dbutils.widgets.text("pg_user", "student", "Postgres role (native password auth)")
dbutils.widgets.text("pg_port", "5432", "Port")
dbutils.widgets.text("contact_email", "rohit885@gmail.com", "Contact email for SEC EDGAR User-Agent (required by SEC)")
dbutils.widgets.text("section", "Item 1A", "Section label stored on chunks")
dbutils.widgets.text("chunk_tokens", "500", "Target tokens per chunk")
dbutils.widgets.text("overlap_tokens", "75", "Overlap tokens between chunks")
dbutils.widgets.text("embed_batch", "32", "Texts per embedding call")
dbutils.widgets.dropdown("force_reload", "false", ["false", "true"], "Re-ingest tickers already having chunks")

# COMMAND ----------

# DBTITLE 1,Connection + SDK (native password, same pattern as 00/01)
import os
import re
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

_endpoints = list(w.postgres.list_endpoints(parent=LAKEBASE_INSTANCE))
assert _endpoints, f"No endpoints for {LAKEBASE_INSTANCE}. Is the compute active?"
_primary = w.postgres.get_endpoint(name=_endpoints[0].name)
_auto_host = _primary.status.hosts.host if _primary.status and _primary.status.hosts else None
PG_HOST = dbutils.widgets.get("pg_host").strip() or _auto_host
assert PG_HOST, "Could not derive host; set the pg_host widget."


def get_connection():
    return psycopg2.connect(
        host=PG_HOST, dbname=PG_DATABASE, user=PG_USER,
        password=dbutils.secrets.get("lakebase", "password"),
        port=PG_PORT, sslmode="require",
    )


CONTACT_EMAIL = dbutils.widgets.get("contact_email").strip()
UA = {"User-Agent": f"stock-research-capstone {CONTACT_EMAIL}"}
SECTION = dbutils.widgets.get("section").strip() or "Item 1A"
CHUNK_TOKENS = int(dbutils.widgets.get("chunk_tokens").strip() or "500")
OVERLAP_TOKENS = int(dbutils.widgets.get("overlap_tokens").strip() or "75")
EMBED_BATCH = int(dbutils.widgets.get("embed_batch").strip() or "32")
FORCE = dbutils.widgets.get("force_reload") == "true"

with get_connection() as _c, _c.cursor() as _cur:
    _cur.execute("SELECT count(*) FROM companies;")
    print("companies in DB:", _cur.fetchone()[0], "(run 01_ingest_prices first)")

# COMMAND ----------

# DBTITLE 1,Resolve CIKs and the latest 10-K per ticker (driver, rate-limited)
with get_connection() as conn, conn.cursor() as cur:
    cur.execute("SELECT ticker FROM companies ORDER BY ticker;")
    TICKERS = [r[0] for r in cur.fetchall()]
assert TICKERS, "No companies found — run 01_ingest_prices.py first."
print(f"{len(TICKERS)} tickers")

# SEC ticker -> CIK map (single request, zero-padded 10-digit CIK).
_cm = requests.get("https://www.sec.gov/files/company_tickers.json", headers=UA, timeout=60).json()
TICKER2CIK = {v["ticker"].upper(): (str(v["cik_str"]).zfill(10), v["title"]) for v in _cm.values()}


def latest_10k(cik: str):
    """Return metadata for the most recent 10-K, or None."""
    r = requests.get(f"https://data.sec.gov/submissions/CIK{cik}.json", headers=UA, timeout=60)
    r.raise_for_status()
    recent = r.json()["filings"]["recent"]
    for form, acc, doc, fdate, rdate in zip(
        recent["form"], recent["accessionNumber"], recent["primaryDocument"],
        recent["filingDate"], recent["reportDate"],
    ):
        if form == "10-K":
            acc_nodash = acc.replace("-", "")
            url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_nodash}/{doc}"
            return {"accession": acc, "doc_url": url, "filing_date": fdate or None,
                    "period_of_report": rdate or None}
    return None


filing_meta = []
missing = []
for t in TICKERS:
    info = TICKER2CIK.get(t.upper())
    if not info:
        missing.append(t)
        continue
    cik, title = info
    meta = latest_10k(cik)
    time.sleep(0.2)  # be polite to SEC (well under 10 req/s)
    if not meta:
        missing.append(t)
        continue
    meta.update({"ticker": t, "cik": cik, "company_name": title})
    filing_meta.append(meta)
    print(f"  {t:5s} CIK {cik}  {meta['accession']}  filed {meta['filing_date']}")

print(f"\nresolved {len(filing_meta)} filings; missing/none: {missing if missing else 'none'}")

# COMMAND ----------

# DBTITLE 1,Upsert companies.cik + filings; build accession -> filing_id map
with get_connection() as conn, conn.cursor() as cur:
    for m in filing_meta:
        cur.execute(
            "UPDATE companies SET cik = %s, "
            "company_name = COALESCE(company_name, %s), updated_at = now() WHERE ticker = %s;",
            (m["cik"], m["company_name"], m["ticker"]),
        )
        cur.execute(
            """
            INSERT INTO filings (ticker, form_type, accession_no, filing_date, period_of_report, source_url)
            VALUES (%s, '10-K', %s, %s, %s, %s)
            ON CONFLICT (accession_no) DO UPDATE SET
                filing_date = EXCLUDED.filing_date,
                period_of_report = EXCLUDED.period_of_report,
                source_url = EXCLUDED.source_url,
                updated_at = now();
            """,
            (m["ticker"], m["accession"], m["filing_date"], m["period_of_report"], m["doc_url"]),
        )
    conn.commit()

    cur.execute("SELECT filing_id, ticker, accession_no FROM filings;")
    ACC2FILING = {acc: (fid, tk) for fid, tk, acc in cur.fetchall()}
print(f"filings rows: {len(ACC2FILING)}")

# COMMAND ----------

# DBTITLE 1,Distributed fetch + extract Item 1A + chunk  (Spark RDD flatMap)
# These run on executors — pure functions over (requests, bs4, re); no DB/SDK auth needed.
_UA = UA
_SECTION = SECTION
_CHUNK_TOKENS = CHUNK_TOKENS
_OVERLAP_TOKENS = OVERLAP_TOKENS

START_PATS = [
    r"item\s*1a[\.\)\s:\-—]*risk\s+factors",  # preferred: heading with title
    r"item\s*1a\b",                            # fallback: bare marker
]
END_PATS = [r"item\s*1b\b", r"item\s*2\b"]


def _html_to_text(html: str) -> str:
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style"]):
        tag.decompose()
    text = soup.get_text(" ")
    return re.sub(r"\s+", " ", text).strip()


def _extract_section(text: str) -> str:
    start = None
    for pat in START_PATS:
        hits = list(re.finditer(pat, text, re.IGNORECASE))
        if hits:
            start = hits[-1].start()  # LAST occurrence skips the table of contents
            break
    if start is None:
        return ""
    end = len(text)
    for ep in END_PATS:
        m = re.search(ep, text[start + 50:], re.IGNORECASE)
        if m:
            end = min(end, start + 50 + m.start())
    return text[start:end].strip()


def _chunk(text: str):
    words = text.split()
    cw = max(1, int(_CHUNK_TOKENS * 0.75))      # ~0.75 words per token
    ow = max(0, int(_OVERLAP_TOKENS * 0.75))
    step = max(1, cw - ow)
    out = []
    for i in range(0, len(words), step):
        piece = words[i:i + cw]
        if not piece:
            break
        out.append(" ".join(piece))
        if i + cw >= len(words):
            break
    return out


def process_filing(m: dict):
    """Fetch the 10-K, extract Item 1A, return a list of chunk dicts."""
    try:
        html = requests.get(m["doc_url"], headers=_UA, timeout=90).text
        time.sleep(0.2)
        section_text = _extract_section(_html_to_text(html))
        rows = []
        for idx, ch in enumerate(_chunk(section_text)):
            rows.append({
                "ticker": m["ticker"], "accession": m["accession"], "section": _SECTION,
                "chunk_index": idx, "chunk_text": ch,
                "token_count": int(len(ch.split()) / 0.75), "section_len": len(section_text),
            })
        if not rows:  # emit a marker row so zero-length extractions are visible
            rows.append({"ticker": m["ticker"], "accession": m["accession"], "section": _SECTION,
                         "chunk_index": -1, "chunk_text": "", "token_count": 0,
                         "section_len": len(section_text)})
        return rows
    except Exception as e:  # noqa: BLE001
        return [{"ticker": m["ticker"], "accession": m["accession"], "section": _SECTION,
                 "chunk_index": -2, "chunk_text": f"ERROR: {e}", "token_count": 0, "section_len": -1}]


targets = filing_meta if FORCE else filing_meta  # (FORCE handled below when writing)
rdd = spark.sparkContext.parallelize(targets, numSlices=min(8, max(1, len(targets))))
all_rows = rdd.flatMap(process_filing).collect()

# Per-filing extraction log (catch zero-length / errors early).
by_ticker = {}
for r in all_rows:
    by_ticker.setdefault(r["ticker"], {"chunks": 0, "section_len": r["section_len"]})
    if r["chunk_index"] >= 0:
        by_ticker[r["ticker"]]["chunks"] += 1
    by_ticker[r["ticker"]]["section_len"] = r["section_len"]

print("per-filing extraction (section_len chars / chunks):")
zero = []
for t in sorted(by_ticker):
    info = by_ticker[t]
    print(f"  {t:5s} len={info['section_len']:>8}  chunks={info['chunks']}")
    if info["chunks"] == 0:
        zero.append(t)
print(f"\nzero-chunk tickers (need attention): {zero if zero else 'none'}")

chunk_rows = [r for r in all_rows if r["chunk_index"] >= 0 and r["chunk_text"]]
print(f"total chunks to embed: {len(chunk_rows)}")

# COMMAND ----------

# DBTITLE 1,Embed chunks (databricks-gte-large-en, batched on driver)
def embed_texts(texts):
    resp = w.serving_endpoints.query(name="databricks-gte-large-en", input=texts)
    return [d.embedding for d in resp.data]


embeddings = []
for i in range(0, len(chunk_rows), EMBED_BATCH):
    batch = [r["chunk_text"] for r in chunk_rows[i:i + EMBED_BATCH]]
    vecs = embed_texts(batch)
    embeddings.extend(vecs)
    print(f"  embedded {min(i + EMBED_BATCH, len(chunk_rows))}/{len(chunk_rows)}")

assert len(embeddings) == len(chunk_rows), "embedding count mismatch"
if embeddings:
    print("embedding dim:", len(embeddings[0]), "(expect 1024)")

# COMMAND ----------

# DBTITLE 1,Write filing_chunks (vector cast), then verify
def vec_literal(emb):
    return "[" + ",".join(f"{x:.6f}" for x in emb) + "]"

rows = []
for r, emb in zip(chunk_rows, embeddings):
    fid_tk = ACC2FILING.get(r["accession"])
    if not fid_tk:
        continue
    filing_id, _ = fid_tk
    rows.append((filing_id, r["ticker"], r["section"], r["chunk_index"],
                 r["chunk_text"], r["token_count"], vec_literal(emb)))

with get_connection() as conn, conn.cursor() as cur:
    if FORCE:
        cur.execute("DELETE FROM filing_chunks WHERE section = %s;", (SECTION,))
    execute_values(
        cur,
        """
        INSERT INTO filing_chunks
            (filing_id, ticker, section, chunk_index, chunk_text, token_count, embedding)
        VALUES %s
        ON CONFLICT (filing_id, section, chunk_index) DO UPDATE SET
            chunk_text = EXCLUDED.chunk_text,
            token_count = EXCLUDED.token_count,
            embedding = EXCLUDED.embedding,
            updated_at = now();
        """,
        rows,
        template="(%s,%s,%s,%s,%s,%s,%s::vector)",
    )
    conn.commit()
print(f"wrote {len(rows)} chunk rows")

# COMMAND ----------

# DBTITLE 1,Verify — checkpoint
with get_connection() as conn, conn.cursor() as cur:
    cur.execute("SELECT count(*) FROM filings;")
    print("filings:", cur.fetchone()[0], "(expect ~36)")

    cur.execute("SELECT count(*), count(DISTINCT filing_id) FROM filing_chunks WHERE section = %s;", (SECTION,))
    total, nfil = cur.fetchone()
    print(f"filing_chunks[{SECTION}]: {total} chunks across {nfil} filings")

    cur.execute("SELECT vector_dims(embedding) FROM filing_chunks WHERE embedding IS NOT NULL LIMIT 1;")
    dim = cur.fetchone()
    print("embedding dim:", dim[0] if dim else "N/A", "(expect 1024)")

    cur.execute("""
        SELECT c.ticker, count(*) AS chunks
        FROM filing_chunks fc JOIN companies c ON c.ticker = fc.ticker
        WHERE fc.section = %s GROUP BY c.ticker ORDER BY chunks ASC LIMIT 5;
    """, (SECTION,))
    print("\nfewest-chunk filings:")
    for row in cur.fetchall():
        print("  ", row)

    cur.execute("""
        SELECT f.ticker FROM filings f
        WHERE NOT EXISTS (SELECT 1 FROM filing_chunks fc
                          WHERE fc.filing_id = f.filing_id AND fc.section = %s);
    """, (SECTION,))
    no_chunks = [r[0] for r in cur.fetchall()]
    print("\nfilings with NO chunks:", no_chunks if no_chunks else "none")

print("\nPhase 2 checkpoint: verify filings≈36, chunks>0 per filing, dim=1024.")
