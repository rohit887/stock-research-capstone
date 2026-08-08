# Lakebase Schema — Companion Reference

Companion to `capstone-scope-stock-research-assistant.md` (§3, §4). The runnable
DDL lives in [`sql/schema.sql`](sql/schema.sql); this document explains the *why*,
the index choices, and the **hybrid-search (RRF) query pattern** the retrieval
layer implements.

Everything is one Lakebase (Postgres 17) database. The scope doc's §3 "Why a
single store" is the governing decision: the agent's most valuable queries join
unstructured retrieval with user state (e.g. *"filings relevant to my watchlist
that I haven't written a note on yet"*), which is one SQL statement here and two
round trips + an app-side join if split across Delta and Lakebase.

---

## 1. Extensions

Installed **per-database** (they do not carry across databases or accounts).
Re-verify after any new-account setup.

```sql
CREATE EXTENSION IF NOT EXISTS vector;           -- pgvector: vector(1024) type
CREATE EXTENSION IF NOT EXISTS lakebase_vector;  -- lakebase_ann index AM
CREATE EXTENSION IF NOT EXISTS lakebase_text;    -- lakebase_bm25 index AM
```

Confirm versions (expected from the scope's verified-deps table):

```sql
SELECT extname, extversion FROM pg_extension
WHERE extname IN ('vector','lakebase_vector','lakebase_text');
-- vector 0.8.0 | lakebase_vector 1.0.0-dev | lakebase_text 0.1.0-dev
```

> The `-dev` suffixes are genuine early builds. Verify index access-method names
> and opclasses against current Lakebase docs before assuming the syntax in
> `sql/schema.sql`. Fallbacks are provided (see §4).

---

## 2. Tables

Eight tables, matching scope §4.

| Table | Grain | Written by | Notes |
|---|---|---|---|
| `companies` | one row per ticker | ingestion | `cik` = zero-padded SEC Central Index Key |
| `price_history` | ticker × trading day | ingestion | PK `(ticker, trade_date)`; daily OHLCV |
| `filings` | one filing per ticker | ingestion | `accession_no` natural key; `form_type` defaults `10-K` |
| `filing_chunks` | chunk × section × filing | ingestion (Spark) | `embedding vector(1024)`; `section` is a **column** |
| `users` | one row per user | app | single-user Free Edition, but modelled for many |
| `watchlist_tickers` | user × ticker | app / agent | write target of `add_to_watchlist` |
| `research_notes` | one note | app / agent | write target of `save_research_note` |
| `agent_events` | one tool invocation | agent | **eval dataset + Phase-2 seed; cannot be backfilled** |

### Design decisions baked into the DDL

- **`section` is a column, not a table.** Adding Item 7 (MD&A) later inserts rows
  with `section = 'Item 7'` — no migration. The extractor is parameterized by
  start/end regex markers keyed on section name.
- **`created_at` / `updated_at` on every table**, with a `set_updated_at()`
  trigger. Costs nothing now and enables Phase-2 incremental (CDC-style)
  extraction of the append-only `agent_events` stream.
- **`ticker` denormalized onto `filing_chunks`.** Hybrid search filters by ticker
  (and often section) *before* ranking; keeping ticker on the chunk avoids a join
  on the hot path and lets BM25 match ticker as an exact token.
- **`agent_events` created in Phase 1**, not later. It is the tool-selection eval
  dataset (correct-tool rate) and the Phase-2 OLAP source. Historical events can't
  be reconstructed after the fact, so the table must exist before the agent runs.

---

## 3. Indexes

| Index | Table | Purpose |
|---|---|---|
| `idx_filing_chunks_ann` | `filing_chunks (embedding)` | ANN vector search (`lakebase_ann`) |
| `idx_filing_chunks_bm25` | `filing_chunks (chunk_text)` | BM25 lexical search (`lakebase_bm25`) |
| `idx_filing_chunks_ticker_section` | `filing_chunks (ticker, section)` | filter before rank |
| `idx_price_history_date` | `price_history (trade_date)` | range scans for price summaries |
| `idx_research_notes_user_tk` | `research_notes (user_id, ticker)` | watchlist × notes joins |
| `idx_agent_events_ts`, `_tool` | `agent_events` | eval + Phase-2 aggregates |

---

## 4. Fallbacks (if the `-dev` index AMs are unavailable)

Lakebase Search is Beta and pgvector-compatible, so degradation is a one-line
index swap — the retrieval SQL is unchanged because it uses the standard `<=>`
cosine operator and `to_tsquery`/RRF logic on top.

**Vector ANN fallback (pgvector HNSW):**
```sql
CREATE INDEX idx_filing_chunks_hnsw
    ON filing_chunks USING hnsw (embedding vector_cosine_ops);
```

**BM25 fallback (native Postgres FTS):**
```sql
CREATE INDEX idx_filing_chunks_fts
    ON filing_chunks USING gin (to_tsvector('english', chunk_text));
```

Both fallbacks are present, commented, in `sql/schema.sql`.

---

## 5. Hybrid-search query pattern (Reciprocal Rank Fusion)

Pure vector search misses rare exact tokens (ticker symbols, "Item 1A", specific
accounting line items); BM25 misses paraphrase and concept. RRF fuses the two
rank lists: each document scores `sum(1 / (k + rank_i))` across the lists it
appears in, with `k = 60` conventional. This is the single query behind
`search_filings` (implemented in `research_store.py`, Phase 4).

```sql
-- :query_text  -> user query string (for BM25)
-- :query_vec   -> 1024-dim embedding of the query (for ANN)
-- :ticker      -> optional ticker filter (NULL = all)
-- :section     -> optional section filter (NULL = all)
-- :top_k       -> rows to return
-- k = 60 (RRF constant)

WITH filtered AS (
    SELECT chunk_id, ticker, section, chunk_text, embedding
    FROM filing_chunks
    WHERE (:ticker  IS NULL OR ticker  = :ticker)
      AND (:section IS NULL OR section = :section)
),
vec AS (   -- semantic / paraphrase
    SELECT chunk_id,
           ROW_NUMBER() OVER (ORDER BY embedding <=> :query_vec) AS rank
    FROM filtered
    ORDER BY embedding <=> :query_vec
    LIMIT 50
),
bm25 AS (  -- exact-token / lexical
    SELECT chunk_id,
           ROW_NUMBER() OVER (
               ORDER BY ts_rank_cd(to_tsvector('english', chunk_text),
                                   plainto_tsquery('english', :query_text)) DESC
           ) AS rank
    FROM filtered
    WHERE to_tsvector('english', chunk_text) @@ plainto_tsquery('english', :query_text)
    LIMIT 50
),
fused AS (
    SELECT chunk_id, SUM(1.0 / (60 + rank)) AS rrf_score
    FROM (
        SELECT chunk_id, rank FROM vec
        UNION ALL
        SELECT chunk_id, rank FROM bm25
    ) r
    GROUP BY chunk_id
)
SELECT c.chunk_id, c.ticker, c.section, c.chunk_text, f.rrf_score
FROM fused f
JOIN filtered c USING (chunk_id)
ORDER BY f.rrf_score DESC
LIMIT :top_k;
```

Notes:
- The `bm25` CTE above uses native Postgres FTS so the pattern is runnable on the
  fallback path. When `lakebase_bm25` is confirmed, its ranking function replaces
  the `ts_rank_cd` expression; the RRF fusion around it is identical.
- The eval harness (Phase 8) runs this with **three configurations** — vec-only
  (drop the `bm25` CTE), bm25-only (drop `vec`), and hybrid (both) — and reports
  recall@5 + MRR for each. The comparison is the finding.

---

## 6. Representative cross-cutting query

The query that justifies the single store — *watchlist filings with no note yet*:

```sql
SELECT DISTINCT fc.ticker, f.filing_date
FROM watchlist_tickers w
JOIN filing_chunks fc ON fc.ticker = w.ticker
JOIN filings f        ON f.filing_id = fc.filing_id
LEFT JOIN research_notes n
       ON n.user_id = w.user_id AND n.ticker = fc.ticker
WHERE w.user_id = :user_id
  AND n.note_id IS NULL;
```

One statement, one round trip — the operational payoff of not splitting stores.
