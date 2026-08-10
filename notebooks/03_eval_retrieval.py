# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "\"databricks-sdk>=0.125.0\"",
#   "openai",
# ]
# ///
# MAGIC %md
# MAGIC # 03 — Retrieval test (hybrid vs vector vs BM25)
# MAGIC
# MAGIC Validates search quality **directly against Lakebase, before any agent exists**
# MAGIC (build-order step 4). Implements the three retrieval configurations and eyeballs
# MAGIC them on exact-token, paraphrase, and cross-sector queries:
# MAGIC
# MAGIC - **vector-only** — ANN over the 1024-dim embeddings (`embedding <=> qvec`)
# MAGIC - **bm25-only** — native Postgres FTS (`ts_rank_cd` over `to_tsvector`)
# MAGIC - **hybrid** — Reciprocal Rank Fusion of the two (the pattern the MCP
# MAGIC   `search_filings` tool will reuse; see `capstone-schema.md` §5)
# MAGIC
# MAGIC This is the v1 smoke test. Phase 8 extends this file into the graded harness
# MAGIC (20 labelled questions, recall@5 + MRR across all three configs).
# MAGIC
# MAGIC **Checkpoint:** exact-token queries surface the right ticker's passages, and
# MAGIC hybrid looks at least as good as either single method.

# COMMAND ----------

# MAGIC %pip install "databricks-sdk>=0.125.0" openai

# COMMAND ----------

dbutils.widgets.text("lakebase_instance", "projects/stock-research-capstone/branches/production", "Lakebase instance / resource name  *REQUIRED*")
dbutils.widgets.text("pg_database", "databricks_postgres", "Database name")
dbutils.widgets.text("pg_host", "", "Host override (blank = auto from endpoint)")
dbutils.widgets.text("pg_user", "student", "Postgres role (native password auth)")
dbutils.widgets.text("pg_port", "5432", "Port")
dbutils.widgets.text("top_k", "5", "Results per query")

# COMMAND ----------

# DBTITLE 1,Connection + query embedding
import psycopg2
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

LAKEBASE_INSTANCE = dbutils.widgets.get("lakebase_instance").strip()
PG_DATABASE = dbutils.widgets.get("pg_database").strip() or "databricks_postgres"
PG_USER = dbutils.widgets.get("pg_user").strip() or "student"
PG_PORT = int(dbutils.widgets.get("pg_port").strip() or "5432")
TOP_K = int(dbutils.widgets.get("top_k").strip() or "5")

_endpoints = list(w.postgres.list_endpoints(parent=LAKEBASE_INSTANCE))
_primary = w.postgres.get_endpoint(name=_endpoints[0].name)
PG_HOST = dbutils.widgets.get("pg_host").strip() or (_primary.status.hosts.host if _primary.status and _primary.status.hosts else None)
assert PG_HOST, "Could not derive host; set pg_host."


def get_connection():
    return psycopg2.connect(
        host=PG_HOST, dbname=PG_DATABASE, user=PG_USER,
        password=dbutils.secrets.get("lakebase", "password"),
        port=PG_PORT, sslmode="require",
    )


def embed_query(text: str):
    resp = w.serving_endpoints.query(name="databricks-gte-large-en", input=[text])
    return resp.data[0].embedding


def _vec_literal(emb):
    return "[" + ",".join(f"{x:.6f}" for x in emb) + "]"


import re

# Question/function words to drop so BM25 ORs only the content terms. plainto_tsquery
# ANDs every term (too strict for NL queries → no matches); we OR content lexemes.
_STOP = {"the", "a", "an", "of", "to", "and", "or", "in", "on", "for", "do", "does",
         "did", "what", "how", "is", "are", "was", "were", "be", "been", "about",
         "from", "with", "by", "as", "at", "that", "this", "these", "those", "our",
         "we", "us", "you", "your", "it", "its", "their", "they", "which", "who",
         "when", "where", "why", "can", "could", "should", "would", "may", "might",
         "will", "than", "then", "say", "says", "describe", "worry", "worries",
         # structural tokens present in every Item 1A chunk -> pure BM25 noise
         "item", "1a", "1b", "1c", "part"}


def _or_tsquery(query: str) -> str:
    """Turn an NL query into an OR-ed tsquery string (content words only)."""
    words = re.findall(r"[a-z0-9]+", query.lower())
    seen, uniq = set(), []
    for w in words:
        if len(w) > 1 and w not in _STOP and w not in seen:
            seen.add(w)
            uniq.append(w)
    return " | ".join(uniq)


with get_connection() as _c, _c.cursor() as _cur:
    _cur.execute("SELECT count(*) FROM filing_chunks WHERE embedding IS NOT NULL;")
    print("embedded chunks:", _cur.fetchone()[0])

# COMMAND ----------

# DBTITLE 1,Three retrieval configurations
def vector_search(query, ticker=None, section=None, top_k=TOP_K):
    qv = _vec_literal(embed_query(query))
    sql = """
        SELECT chunk_id, ticker, section, left(chunk_text, 180) AS snip,
               (embedding <=> %(qv)s::vector) AS dist
        FROM filing_chunks
        WHERE embedding IS NOT NULL
          AND (%(tk)s IS NULL OR ticker = %(tk)s)
          AND (%(sec)s IS NULL OR section = %(sec)s)
        ORDER BY embedding <=> %(qv)s::vector
        LIMIT %(k)s;
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, {"qv": qv, "tk": ticker, "sec": section, "k": top_k})
        return cur.fetchall()


def bm25_search(query, ticker=None, section=None, top_k=TOP_K):
    tsq = _or_tsquery(query)
    if not tsq:
        return []
    sql = """
        SELECT chunk_id, ticker, section, left(chunk_text, 180) AS snip,
               ts_rank_cd(to_tsvector('english', chunk_text),
                          to_tsquery('english', %(q)s)) AS rank
        FROM filing_chunks
        WHERE to_tsvector('english', chunk_text) @@ to_tsquery('english', %(q)s)
          AND (%(tk)s IS NULL OR ticker = %(tk)s)
          AND (%(sec)s IS NULL OR section = %(sec)s)
        ORDER BY rank DESC
        LIMIT %(k)s;
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, {"q": tsq, "tk": ticker, "sec": section, "k": top_k})
        return cur.fetchall()


def hybrid_search(query, ticker=None, section=None, top_k=TOP_K, k_rrf=60, pool=50):
    """Reciprocal Rank Fusion of vector + BM25 (capstone-schema.md §5)."""
    qv = _vec_literal(embed_query(query))
    tsq = _or_tsquery(query)
    sql = """
        WITH filtered AS (
            SELECT chunk_id, ticker, section, chunk_text, embedding
            FROM filing_chunks
            WHERE embedding IS NOT NULL
              AND (%(tk)s IS NULL OR ticker = %(tk)s)
              AND (%(sec)s IS NULL OR section = %(sec)s)
        ),
        vec AS (
            SELECT chunk_id, ROW_NUMBER() OVER (ORDER BY embedding <=> %(qv)s::vector) AS rank
            FROM filtered ORDER BY embedding <=> %(qv)s::vector LIMIT %(pool)s
        ),
        bm25 AS (
            SELECT chunk_id, ROW_NUMBER() OVER (
                       ORDER BY ts_rank_cd(to_tsvector('english', chunk_text),
                                           to_tsquery('english', %(q)s)) DESC) AS rank
            FROM filtered
            WHERE %(q)s <> '' AND to_tsvector('english', chunk_text) @@ to_tsquery('english', %(q)s)
            LIMIT %(pool)s
        ),
        fused AS (
            SELECT chunk_id, SUM(1.0 / (%(k)s + rank)) AS rrf
            FROM (SELECT chunk_id, rank FROM vec
                  UNION ALL
                  SELECT chunk_id, rank FROM bm25) u
            GROUP BY chunk_id
        )
        SELECT f.chunk_id, c.ticker, c.section, left(c.chunk_text, 180) AS snip, f.rrf
        FROM fused f JOIN filtered c USING (chunk_id)
        ORDER BY f.rrf DESC
        LIMIT %(topk)s;
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, {"qv": qv, "q": tsq, "tk": ticker, "sec": section,
                          "k": k_rrf, "pool": pool, "topk": top_k})
        return cur.fetchall()

# COMMAND ----------

# DBTITLE 1,Compare the three configs on sample queries
def _show(title, rows):
    print(f"  {title}")
    if not rows:
        print("    (no results)")
    for cid, tk, sec, snip, score in rows:
        print(f"    [{tk:5s}] {float(score):.4f}  {snip.strip()[:120]}")


# (query, optional ticker filter) — mix of exact-token, paraphrase, cross-sector.
SAMPLES = [
    ("What does MU say about memory market cyclicality?", "MU"),          # exact-token, single ticker
    ("which chipmakers worry about customer concentration", None),        # paraphrase (vector strength)
    ("Item 1A cyclicality of DRAM and NAND pricing", None),               # exact-token (BM25 strength)
    ("how do banks describe interest rate and credit risk", None),        # concept
    ("supply chain dependence on a single overseas foundry", None),       # paraphrase, cross-ticker
    ("risks from artificial intelligence competition", None),            # concept, cross-sector
]

for query, tk in SAMPLES:
    print("=" * 100)
    print(f"QUERY: {query}" + (f"   [ticker={tk}]" if tk else ""))
    print("-" * 100)
    _show("vector-only", vector_search(query, ticker=tk))
    _show("bm25-only  ", bm25_search(query, ticker=tk))
    _show("hybrid RRF ", hybrid_search(query, ticker=tk))
    print()

print("Eyeball check: exact-token queries should favor bm25; paraphrase should favor")
print("vector; hybrid should be at least as relevant as the better of the two.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Graded eval — A. Retrieval quality (recall@5 + MRR)
# MAGIC
# MAGIC 20 questions across four types. "Gold" chunks are derived from the corpus:
# MAGIC a chunk is relevant if it is from the expected ticker **and** contains one of
# MAGIC the concept keywords — materialized to chunk_ids from the DB (so this is
# MAGIC chunk-id-based, just auto-labelled rather than hand-hunted). Adversarial
# MAGIC questions have no in-corpus answer and are reported separately.
# MAGIC
# MAGIC We run **all three configurations** and report recall@5 + MRR for each — the
# MAGIC comparison is the finding, and it either validates hybrid or honestly does not.

# COMMAND ----------

# id, type, tickers (expected), keywords (any-match, ILIKE), adversarial
QUESTIONS = [
    # --- exact-token (BM25 strength) ---
    ("E1", "exact", "Micron DRAM and NAND average selling price swings", ["MU"], ["NAND", "DRAM"], False),
    ("E2", "exact", "NVIDIA reliance on third-party foundries for wafer fabrication", ["NVDA"], ["foundr"], False),
    ("E3", "exact", "Qualcomm revenue concentration among a few customers", ["QCOM"], ["concentration"], False),
    ("E4", "exact", "Bank of America interest rate risk on net interest income", ["BAC"], ["interest rate"], False),
    ("E5", "exact", "Intel manufacturing yields and process technology", ["INTC"], ["yield"], False),
    ("E6", "exact", "Exxon crude oil and natural gas commodity price volatility", ["XOM"], ["crude oil", "natural gas"], False),
    # --- paraphrase (vector strength; filing uses different words) ---
    ("P1", "paraphrase", "chipmakers that could be hurt if a key overseas chip factory is disrupted", ["NVDA", "QCOM", "AMD", "AVGO"], ["foundr", "third party", "third-party"], False),
    ("P2", "paraphrase", "companies dependent on a small group of large buyers", ["QCOM", "MU", "AVGO"], ["customers", "customer"], False),
    ("P3", "paraphrase", "banks exposed to borrowers failing to repay loans", ["JPM", "BAC", "GS", "MS"], ["credit"], False),
    ("P4", "paraphrase", "firms worried about attracting and keeping skilled engineers", ["NVDA", "AMD", "INTC", "ADBE", "CRM"], ["personnel", "talent", "retain", "employees"], False),
    ("P5", "paraphrase", "exposure to hackers breaching systems and stealing data", ["MSFT", "AAPL", "V", "MA"], ["cybersecurity", "breach", "cyber"], False),
    ("P6", "paraphrase", "damage to brand reputation hurting sales", ["NKE", "MCD", "COST", "WMT"], ["reputation", "brand"], False),
    # --- cross-sector comparative (retrieval breadth) ---
    ("C1", "cross", "how semiconductor firms and banks each describe regulatory risk", ["MU", "NVDA", "JPM", "BAC"], ["regulat"], False),
    ("C2", "cross", "supply-chain risk across chipmakers and consumer companies", ["NVDA", "AVGO", "NKE", "COST"], ["supply"], False),
    ("C3", "cross", "foreign-exchange and international operations risk", ["AAPL", "MCD", "PFE", "CAT"], ["foreign", "currency"], False),
    ("C4", "cross", "competition risk in tech versus healthcare", ["NVDA", "MSFT", "LLY", "JNJ"], ["competition", "competitors"], False),
    # --- adversarial (answer NOT in corpus) ---
    ("A1", "adversarial", "what does Tesla say about battery supply risks", ["TSLA"], ["battery"], True),
    ("A2", "adversarial", "Bitcoin holdings and crypto custody risk", ["MSTR"], ["bitcoin", "crypto"], True),
    ("A3", "adversarial", "airline passenger revenue seasonality risk", ["DAL"], ["passenger"], True),
    ("A4", "adversarial", "risks from streaming subscriber churn", ["NFLX"], ["subscriber"], True),
]


def _gold_ids(tickers, keywords):
    conds = " OR ".join([f"chunk_text ILIKE %(kw{i})s" for i in range(len(keywords))])
    params = {"tks": tickers, **{f"kw{i}": f"%{k}%" for i, k in enumerate(keywords)}}
    sql = (f"SELECT chunk_id FROM filing_chunks WHERE ticker = ANY(%(tks)s) "
           f"AND section = 'Item 1A' AND ({conds});")
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return {r[0] for r in cur.fetchall()}


# Materialize gold sets and warn on any that are empty (keyword needs tuning).
GOLD = {}
print("Building gold sets from the corpus:")
for qid, qtype, q, tks, kws, adv in QUESTIONS:
    g = set() if adv else _gold_ids(tks, kws)
    GOLD[qid] = g
    flag = "" if (adv or g) else "  <-- GOLD EMPTY (tune keywords)"
    print(f"  {qid} [{qtype:11s}] gold={len(g):3d}{flag}")

# COMMAND ----------

# DBTITLE 1,Score recall@5 + MRR for each configuration
CONFIGS = {"vector": vector_search, "bm25": bm25_search, "hybrid": hybrid_search}


def _first_rank(rows, gold):
    for i, r in enumerate(rows, 1):
        if r[0] in gold:      # r[0] = chunk_id
            return i
    return None


def evaluate(search_fn):
    answerable = [x for x in QUESTIONS if not x[5]]
    hit, rr = 0, 0.0
    per = []
    for qid, qtype, q, tks, kws, adv in answerable:
        rows = search_fn(q, top_k=5)          # global retrieval (no ticker filter)
        rank = _first_rank(rows, GOLD[qid])
        hit += 1 if rank else 0
        rr += (1.0 / rank) if rank else 0.0
        per.append((qid, qtype, rank))
    n = len(answerable)
    return {"recall@5": hit / n, "MRR": rr / n, "per": per, "n": n}


results = {name: evaluate(fn) for name, fn in CONFIGS.items()}

print(f"{'config':8s}  recall@5   MRR")
for name, r in results.items():
    print(f"{name:8s}   {r['recall@5']:.3f}   {r['MRR']:.3f}")

print("\nper-question first-relevant rank (lower is better; '-' = miss):")
print(f"{'q':4s}{'type':12s}  vector  bm25  hybrid")
for i, (qid, qtype, _) in enumerate(results["vector"]["per"]):
    ranks = [results[c]["per"][i][2] for c in CONFIGS]
    print(f"{qid:4s}{qtype:12s}  " + "  ".join(f"{(r if r else '-'):>5}" for r in ranks))

# COMMAND ----------

# DBTITLE 1,Adversarial — should NOT surface a confident on-topic hit
print("Adversarial (no in-corpus answer). Top-1 hybrid score + whether any keyword "
      "appears in top-5 (ideally none):")
for qid, qtype, q, tks, kws, adv in [x for x in QUESTIONS if x[5]]:
    rows = hybrid_search(q, top_k=5)
    top_score = rows[0][4] if rows else None
    kw_hit = any(any(k.lower() in (r[3] or "").lower() for k in kws) for r in rows)
    print(f"  {qid}: top1_score={top_score}  keyword_in_top5={kw_hit}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Graded eval — B. Tool selection (correct-tool rate)
# MAGIC
# MAGIC 15 natural-language prompts with the tool a correct agent should pick. We ask
# MAGIC the model once per prompt and read its FIRST tool choice (structured or the
# MAGIC Llama text format), then compare to the expected tool. This is where
# MAGIC under-specified docstrings would show up.

# COMMAND ----------

import os as _os, sys as _sys


def _find_repo(anchor="frontend/agent.py"):
    here = _os.getcwd()
    for cand in (here, _os.path.dirname(here), _os.path.dirname(_os.path.dirname(here))):
        if _os.path.exists(_os.path.join(cand, anchor)):
            return cand
    raise FileNotFoundError(f"could not locate {anchor}; set repo path manually")


_sys.path.insert(0, _os.path.join(_find_repo(), "frontend"))
import agent  # noqa: E402  (frontend/agent.py: MODEL, TOOLS, SYSTEM_PROMPT, _parse_text_tool_calls)

TOOL_PROMPTS = [
    ("How has Apple's stock performed this year?", "get_price_summary"),
    ("What does Micron say about memory pricing risk?", "search_filings"),
    ("Compare NVDA, AMD and INTC by return.", "compare_tickers"),
    ("Is there anything concerning about Boeing's recent price action?", "assess_risk_signal"),
    ("Add Nvidia to my watchlist.", "add_to_watchlist"),
    ("Save a note that I like Costco's membership model.", "save_research_note"),
    ("Which banks discuss credit risk in their filings?", "search_filings"),
    ("What's the annualized volatility of JPMorgan?", "get_price_summary"),
    ("Show me my watchlist.", "get_watchlist"),
    ("What notes have I saved so far?", "list_notes"),
    ("Rank the mega-cap tech names by maximum drawdown.", "compare_tickers"),
    ("Does NVIDIA warn about competition in AI?", "search_filings"),
    ("Track Exxon for me.", "add_to_watchlist"),
    ("How risky does Micron look right now?", "assess_risk_signal"),
    ("Remember that I want to revisit Adobe after earnings.", "save_research_note"),
]


def predict_tool(prompt: str):
    client = agent._openai_client()
    resp = client.chat.completions.create(
        model=agent.MODEL,
        messages=[{"role": "system", "content": agent.SYSTEM_PROMPT},
                  {"role": "user", "content": prompt}],
        tools=agent.TOOLS, tool_choice="auto", temperature=0)
    msg = resp.choices[0].message
    if msg.tool_calls:
        return msg.tool_calls[0].function.name
    calls = agent._parse_text_tool_calls(msg.content)
    return calls[0]["name"] if calls else None


correct = 0
print(f"{'expected':20s}{'predicted':20s}  ok  prompt")
for prompt, expected in TOOL_PROMPTS:
    pred = predict_tool(prompt)
    ok = pred == expected
    correct += ok
    print(f"{expected:20s}{str(pred):20s}  {'Y' if ok else 'n'}   {prompt[:44]}")

print(f"\ncorrect-tool rate: {correct}/{len(TOOL_PROMPTS)} = {correct/len(TOOL_PROMPTS):.0%}")