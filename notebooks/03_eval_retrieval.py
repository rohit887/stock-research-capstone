# Databricks notebook source
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

# MAGIC %pip install "databricks-sdk>=0.125.0"

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
    sql = """
        SELECT chunk_id, ticker, section, left(chunk_text, 180) AS snip,
               ts_rank_cd(to_tsvector('english', chunk_text),
                          plainto_tsquery('english', %(q)s)) AS rank
        FROM filing_chunks
        WHERE to_tsvector('english', chunk_text) @@ plainto_tsquery('english', %(q)s)
          AND (%(tk)s IS NULL OR ticker = %(tk)s)
          AND (%(sec)s IS NULL OR section = %(sec)s)
        ORDER BY rank DESC
        LIMIT %(k)s;
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, {"q": query, "tk": ticker, "sec": section, "k": top_k})
        return cur.fetchall()


def hybrid_search(query, ticker=None, section=None, top_k=TOP_K, k_rrf=60, pool=50):
    """Reciprocal Rank Fusion of vector + BM25 (capstone-schema.md §5)."""
    qv = _vec_literal(embed_query(query))
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
                                           plainto_tsquery('english', %(q)s)) DESC) AS rank
            FROM filtered
            WHERE to_tsvector('english', chunk_text) @@ plainto_tsquery('english', %(q)s)
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
        cur.execute(sql, {"qv": qv, "q": query, "tk": ticker, "sec": section,
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
