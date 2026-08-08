-- =====================================================================
-- AI Stock Market Research Assistant — Lakebase (Postgres 17) schema
-- =====================================================================
-- Single-store design: user state, prices, filings, chunks, and embeddings
-- all live here. See capstone-schema.md for rationale, the hybrid-search
-- (RRF) query pattern, and index-syntax notes.
--
-- Run order matters. Sections are separated so that the standard table DDL
-- (Section 3) succeeds even if the early `-dev` index access methods in
-- Section 5 need a syntax tweak. notebooks/00_setup.py runs this file
-- statement-by-statement and reports which statements failed.
--
-- Idempotent: safe to re-run. Uses IF NOT EXISTS throughout.
-- =====================================================================


-- ---------------------------------------------------------------------
-- Section 1 — Extensions
-- ---------------------------------------------------------------------
-- vector           (pgvector)      -> vector(1024) type + hnsw/ivfflat fallback
-- lakebase_vector  (lakebase_ann)  -> ANN index access method
-- lakebase_text    (lakebase_bm25) -> BM25 full-text index access method
-- These are per-database. Confirm versions after install (see 00_setup.py).
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS lakebase_vector;
CREATE EXTENSION IF NOT EXISTS lakebase_text;


-- ---------------------------------------------------------------------
-- Section 2 — updated_at trigger helper
-- ---------------------------------------------------------------------
-- Every mutable table carries created_at / updated_at so Phase-2 can do
-- incremental (CDC-style) extraction. This trigger keeps updated_at honest.
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- ---------------------------------------------------------------------
-- Section 3 — Tables (standard Postgres; the reliable part)
-- ---------------------------------------------------------------------

-- companies: one row per ticker  (written by ingestion)
CREATE TABLE IF NOT EXISTS companies (
    ticker        TEXT PRIMARY KEY,
    company_name  TEXT,
    sector        TEXT,
    cik           TEXT,                       -- SEC Central Index Key, zero-padded
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- price_history: one row per ticker per trading day  (written by ingestion)
CREATE TABLE IF NOT EXISTS price_history (
    ticker        TEXT NOT NULL REFERENCES companies(ticker) ON DELETE CASCADE,
    trade_date    DATE NOT NULL,
    open          NUMERIC(18,4),
    high          NUMERIC(18,4),
    low           NUMERIC(18,4),
    close         NUMERIC(18,4),
    volume        BIGINT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ticker, trade_date)
);

-- filings: one row per filing per ticker  (written by ingestion)
CREATE TABLE IF NOT EXISTS filings (
    filing_id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ticker            TEXT NOT NULL REFERENCES companies(ticker) ON DELETE CASCADE,
    form_type         TEXT NOT NULL DEFAULT '10-K',
    accession_no      TEXT UNIQUE,             -- SEC accession number, natural key
    filing_date       DATE,
    period_of_report  DATE,
    source_url        TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- filing_chunks: one row per chunk per section per filing  (written by Spark)
-- `section` is a COLUMN, not a table -> adding Item 7 (MD&A) later inserts
-- rows, no migration. `embedding` holds the 1024-dim gte-large-en vector.
CREATE TABLE IF NOT EXISTS filing_chunks (
    chunk_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    filing_id     BIGINT NOT NULL REFERENCES filings(filing_id) ON DELETE CASCADE,
    ticker        TEXT NOT NULL,               -- denormalized for cheap filtering / exact-token BM25
    section       TEXT NOT NULL,               -- e.g. 'Item 1A'
    chunk_index   INT  NOT NULL,               -- order within (filing, section)
    chunk_text    TEXT NOT NULL,
    token_count   INT,
    embedding     vector(1024),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (filing_id, section, chunk_index)
);

-- users: one row per user  (written by app)
CREATE TABLE IF NOT EXISTS users (
    user_id       TEXT PRIMARY KEY,
    display_name  TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- watchlist_tickers: one row per user per ticker  (written by app / agent)
CREATE TABLE IF NOT EXISTS watchlist_tickers (
    user_id       TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    ticker        TEXT NOT NULL REFERENCES companies(ticker) ON DELETE CASCADE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, ticker)
);

-- research_notes: one row per note  (written by app / agent)
CREATE TABLE IF NOT EXISTS research_notes (
    note_id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id       TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    ticker        TEXT,
    note          TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- agent_events: one row per tool invocation  (written by agent) — CREATE IN PHASE 1.
-- This is the eval dataset and the Phase-2 OLAP seed; it cannot be backfilled.
-- `ts` is the canonical event time; created_at/updated_at kept for table uniformity.
CREATE TABLE IF NOT EXISTS agent_events (
    event_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id       TEXT,
    ts            TIMESTAMPTZ NOT NULL DEFAULT now(),
    user_message  TEXT,
    tool_called   TEXT,
    tool_args     JSONB,
    latency_ms    INT,
    result_count  INT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);


-- ---------------------------------------------------------------------
-- Section 4 — updated_at triggers
-- ---------------------------------------------------------------------
DROP TRIGGER IF EXISTS trg_companies_updated        ON companies;
DROP TRIGGER IF EXISTS trg_price_history_updated     ON price_history;
DROP TRIGGER IF EXISTS trg_filings_updated           ON filings;
DROP TRIGGER IF EXISTS trg_filing_chunks_updated     ON filing_chunks;
DROP TRIGGER IF EXISTS trg_users_updated             ON users;
DROP TRIGGER IF EXISTS trg_watchlist_tickers_updated ON watchlist_tickers;
DROP TRIGGER IF EXISTS trg_research_notes_updated    ON research_notes;
DROP TRIGGER IF EXISTS trg_agent_events_updated      ON agent_events;

CREATE TRIGGER trg_companies_updated        BEFORE UPDATE ON companies        FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_price_history_updated     BEFORE UPDATE ON price_history     FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_filings_updated           BEFORE UPDATE ON filings           FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_filing_chunks_updated     BEFORE UPDATE ON filing_chunks     FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_users_updated             BEFORE UPDATE ON users             FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_watchlist_tickers_updated BEFORE UPDATE ON watchlist_tickers FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_research_notes_updated    BEFORE UPDATE ON research_notes    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_agent_events_updated      BEFORE UPDATE ON agent_events      FOR EACH ROW EXECUTE FUNCTION set_updated_at();


-- ---------------------------------------------------------------------
-- Section 5 — Indexes
-- ---------------------------------------------------------------------
-- Plain btree filters used by every hybrid query (filter then rank).
CREATE INDEX IF NOT EXISTS idx_filing_chunks_ticker   ON filing_chunks (ticker);
CREATE INDEX IF NOT EXISTS idx_filing_chunks_section  ON filing_chunks (section);
CREATE INDEX IF NOT EXISTS idx_filing_chunks_ticker_section ON filing_chunks (ticker, section);
CREATE INDEX IF NOT EXISTS idx_price_history_date     ON price_history (trade_date);
CREATE INDEX IF NOT EXISTS idx_research_notes_user_tk ON research_notes (user_id, ticker);
CREATE INDEX IF NOT EXISTS idx_agent_events_ts        ON agent_events (ts);
CREATE INDEX IF NOT EXISTS idx_agent_events_tool      ON agent_events (tool_called);

-- --- Vector ANN index (lakebase_vector extension) --------------------
-- NOTE: lakebase_vector is an early -dev build. Verify the access-method
-- name and opclass against current docs. If lakebase_ann is unavailable,
-- the pgvector HNSW fallback (below) is index-compatible — the retrieval
-- SQL uses the `<=>` cosine-distance operator either way.
CREATE INDEX IF NOT EXISTS idx_filing_chunks_ann
    ON filing_chunks USING lakebase_ann (embedding vector_cosine_ops);

-- Fallback (pgvector HNSW) — uncomment if lakebase_ann is not available:
-- CREATE INDEX IF NOT EXISTS idx_filing_chunks_hnsw
--     ON filing_chunks USING hnsw (embedding vector_cosine_ops);

-- --- BM25 full-text index (lakebase_text extension) ------------------
-- NOTE: lakebase_text is an early -dev build. Verify access-method name.
-- Fallback is a standard Postgres GIN index on to_tsvector('english', chunk_text).
CREATE INDEX IF NOT EXISTS idx_filing_chunks_bm25
    ON filing_chunks USING lakebase_bm25 (chunk_text);

-- Fallback (native Postgres FTS) — uncomment if lakebase_bm25 unavailable:
-- CREATE INDEX IF NOT EXISTS idx_filing_chunks_fts
--     ON filing_chunks USING gin (to_tsvector('english', chunk_text));


-- ---------------------------------------------------------------------
-- Section 6 — Grants (OPTIONAL / parameterized)
-- ---------------------------------------------------------------------
-- The MCP server and Streamlit app connect as a separate identity from the
-- schema creator. Grant that role read/write. Replace :app_role with the
-- actual app service-principal role, then uncomment. Left commented so this
-- file runs clean when the role does not yet exist.
--
-- GRANT USAGE ON SCHEMA public TO :app_role;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO :app_role;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO :app_role;
-- ALTER DEFAULT PRIVILEGES IN SCHEMA public
--     GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO :app_role;
