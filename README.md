# AI Stock Market Research Assistant

A research assistant that lets a user keep a watchlist, ask natural-language
questions about what companies have **said** in their SEC filings and how their
prices have **moved**, and record findings back into persistent state.

> **Thesis:** price data tells you *what* happened; filings tell you *why
> management thinks* it happened. Combining both in one retrieval layer is the
> point — neither alone is interesting.

> ⚠️ **Disclaimer:** This is a research and educational tool. It is **not** a
> trading system, a backtester, or a source of investment advice. Nothing it
> produces constitutes a recommendation to buy or sell any security.

Databricks "Rise of the AI Data Engineer" capstone · Databricks Free Edition.

📸 **Proof it works:** see [`EVIDENCE.md`](EVIDENCE.md) — both apps deployed, live
tool-call screenshots in [`snapshots/`](snapshots/), and the measured eval results.

---

## Architecture

- **Lakebase (Postgres 17)** — single store: user state, prices, filing chunks, embeddings.
- **Lakebase Search** — `lakebase_ann` (vector) + native Postgres FTS for hybrid retrieval fused by RRF.
- **Model serving** — `databricks-llama-4-maverick` (chat), `databricks-gte-large-en` (embeddings, 1024-dim).
- **MCP server** — custom `fastmcp` server exposing the tools over SSE, deployed as the Databricks App `stock-research-mcp-server` (source: `mcp-server-app/`).
- **Frontend** — the Streamlit Databricks App `stock-research-assistant` (source: `frontend/`).
- **Connection** — deployed apps read the Postgres URL from a Databricks secret (`lakebase/connection-url`, base64-decoded at runtime); notebooks connect via `dbutils`/`PG*` env. No credentials in source.
- **Deliberately excluded** — Genie, Delta tables, Databricks Vector Search, Agent Bricks.

```
stock-research-capstone/
├── mcp-server/           # canonical adapters (testable, imported by notebooks)
│   ├── research_store.py     # ALL Lakebase queries + query embedding
│   ├── market_data.py        # Massive + EDGAR HTTP/parsing
│   └── server.py             # (reference) thin @mcp.tool wrappers
├── mcp-server-app/       # deployed MCP app (self-contained copy of adapters)
│   ├── app.py, app.yaml, requirements.txt
│   ├── research_store.py, market_data.py
├── frontend/             # deployed Streamlit app (self-contained)
│   ├── app.py                # UI: watchlist, chat, visible tool calls, notes
│   ├── agent.py              # llama-4-maverick tool-calling loop
│   ├── research_store.py     # copy of the adapter
│   ├── app.yaml, requirements.txt
├── notebooks/
│   ├── 00_setup.py           # extensions + schema + checks
│   ├── 01_ingest_prices.py   # plain Python, rate-limited
│   ├── 02_ingest_filings.py  # Spark
│   ├── 03_eval_retrieval.py  # retrieval smoke test + graded eval harness
│   └── 04_test_adapters.py   # adapter unit checks against Lakebase
├── sql/schema.sql            # Lakebase DDL
├── capstone-schema.md        # schema rationale + hybrid-search (RRF) query pattern
└── docs/lakebase-authentication-guide.md
```

> **Why the adapters are duplicated across `mcp-server/`, `mcp-server-app/`, and
> `frontend/`:** Databricks Apps bundle only the app's own source folder (sibling
> imports fail at deploy), so each deployable carries its own copy of
> `research_store.py`. `mcp-server/` is the canonical, notebook-tested copy.

---

## Build status — complete

| Phase | Deliverable | Status |
|---|---|---|
| 0 | Repo bootstrap + schema | ✅ |
| 1 | Price ingestion (plain Python) | ✅ |
| 2 | EDGAR filing ingestion (Spark) | ✅ |
| 3 | Retrieval test (3 configs) | ✅ |
| 4 | Adapter modules | ✅ |
| 5 | MCP server (`stock-research-mcp-server`) | ✅ deployed |
| 6 | Agent (llama-4-maverick tool-calling) | ✅ |
| 7 | Streamlit frontend | ✅ deployed |
| 8 | Eval harness | ✅ |
| 9 | Submission polish | ✅ |

**Data loaded:** 36 tickers · ~9,900 daily price rows · 36 10-Ks · 3,377 Item 1A chunks (1024-dim).

---

## Eval results (measured)

### A. Retrieval quality — recall@5 + MRR across three configurations
16 answerable questions (6 exact-token / 6 paraphrase / 4 cross-sector); gold
chunks auto-derived from the corpus (expected ticker + concept keyword).

| config | recall@5 | MRR |
|---|---|---|
| **vector-only** | **0.938** | **0.688** |
| bm25-only | 0.312 | 0.200 |
| hybrid (RRF) | 0.812 | 0.591 |

**Finding (honest, and the point of running all three):** on this corpus **vector
retrieval beat hybrid.** BM25 only helped where the query carried rare, distinctive
tokens present in the target chunks (DRAM/NAND, "interest rate"); for questions
whose relevance depends on *meaning* or *company identity*, BM25 returned lexical
noise from the wrong tickers, and equal-weight RRF fused that noise in — dragging
hybrid *below* pure vector. This **contradicts the initial hybrid hypothesis for
this query mix**, which is a stronger, measured result than an unmeasured claim
that hybrid "must" be better. Hybrid remains in the app (recall 0.812 is still
good and it demonstrates the technique); a principled next step is a
vector-weighted RRF that keeps BM25's narrow exact-token wins without its noise.

**Adversarial (4 out-of-corpus questions):** 0/4 surfaced a keyword-matching
passage (top RRF scores ~0.02–0.03) — the system does not manufacture relevance
for topics it doesn't hold.

### B. Tool selection — correct-tool rate
15 natural-language prompts with the expected tool. **14/15 (93%)** on the first
run; the single miss ("*Remember* that I want to revisit Adobe…" → `get_watchlist`)
was an under-specified `save_research_note` docstring, since strengthened — exactly
what this eval is for. `agent_events` captures every real tool call automatically.

---

## Design decisions

**Why a single store.** The agent's most valuable queries join unstructured
retrieval with user state (e.g. *"watchlist filings I haven't noted yet"*) — one
SQL statement in Lakebase, versus two round trips and an app-side join if split
across Delta and Lakebase. See `capstone-schema.md` §6.

**Why hybrid search (and what we found).** BM25 catches rare exact tokens
(tickers, "Item 1A", accounting line items) that pure vector search misses; vector
catches paraphrase and concept. We fused both with reciprocal rank fusion and
**measured all three configurations** — and honestly, vector won on this corpus
(see Eval results). The measurement *is* the deliverable.

**Why native FTS instead of `lakebase_bm25`.** The `-dev` `lakebase_bm25` access
method has no default operator class for `text` and needs a build-specific opclass
we couldn't verify; the RRF query ranks lexical matches with native Postgres FTS
(`to_tsvector` + `ts_rank_cd`, GIN-indexed) anyway. We didn't take a dependency on
an unstable operator class. `lakebase_ann` (vector) *is* used and works.

**Why native-password auth for Lakebase writes.** On Lakebase Autoscaling, DDL/
transactional work over the OAuth database token could route to different backend
instances and not persist; a native password role (`student`) has session affinity.
Documented in `docs/lakebase-authentication-guide.md`.

**Why Spark for filings but not prices.** Filing ingestion (fetch → strip HTML →
extract Item 1A → chunk across 36 documents) is genuine distributable
unstructured-data work — done with a Spark RDD `flatMap`. Prices are **rate-limited
at ~3 req/min**, so the job is IO-bound, not compute-bound; Spark buys nothing and
would only trip the 429 limit faster. Prices are plain, checkpointed Python.
Knowing when *not* to reach for Spark is the point.

**Why not Agent Bricks.** Free Edition doesn't support Knowledge Assistant, and
other Agent Bricks components are uncertain there. The agent is built directly
against `databricks-llama-4-maverick`, which also gives full control of the system
prompt and tool-selection logic.

**Tool docstrings are load-bearing.** `search_filings` and `get_price_summary` both
"sound like getting info about a company"; their docstrings draw the line sharply —
one covers *what management wrote about risk/strategy*, the other *numeric price
movement*. The tool-selection eval measures whether that line holds (93% → fixed).

---

## Worked examples (real tool calls)

1. **Combined (the thesis).** *"How has NVDA performed this year, and what does it
   say about supply-chain risk?"* → `get_price_summary(NVDA, ytd)` (18.59% return,
   38.6% ann. vol, −19.4% max drawdown) **and** `search_filings(supply-chain risk,
   NVDA)` citing NVDA's third-party-foundry / geopolitical supply passage.
2. **Filing retrieval.** *"What does MU say about memory pricing cyclicality?"* →
   `search_filings(..., MU)` returning Micron's real disclosure that DRAM ASPs have
   ranged +40%/−40% and NAND +30%/−50% over five years.
3. **Derived judgment.** *"Is anything concerning about MU's recent price action?"*
   → `assess_risk_signal(MU)` → **elevated**, from 90-day drawdown + 30-day vol vs
   MU's own history, with the seeded Item 1A passages behind it.
4. **Write + persistence.** *"Add AAPL to my watchlist and save a note that I'm
   tracking its services growth."* → `add_to_watchlist` + `save_research_note`;
   both survive a page refresh (state in Lakebase).
5. **Honesty.** *"What does Tesla say about battery risks?"* (TSLA not covered) →
   the agent declines rather than inventing a quote.

---

## Data scope

36 S&P 500 large caps across 6 sectors · daily OHLCV ~13 months (Massive free-tier
ceiling) · most recent 10-K per ticker, **Item 1A only** · ~500-token chunks,
75-token overlap, 1024-dim `gte-large-en` embeddings.

> Chunk size is **500 tokens, not the scoped ~800** — `gte-large-en` truncates at
> 512 tokens, so 800-token chunks would silently lose ~40% of their content.

---

## Limitations & next steps

**Limitations**
- **Item 1A extraction** is robust for ~34/36 filings; a couple (MS, INTC) use bare
  "Risk Factors" headings with no clean end boundary and **over-extract** (extra
  neighbouring text). Over-extraction was chosen over missing the section; the
  per-filing length log flags outliers.
- **Hybrid < vector** on this corpus (see Eval). BM25's value here is narrow.
- **Prices** are end-of-day only, ~13 months (free-tier limits). Single-user state.
- **Adapter duplication** across the two apps + canonical module (Databricks Apps
  packaging constraint) — must be kept in sync.

**Next steps**
- Vector-weighted RRF to make hybrid competitive while retaining exact-token wins.
- Item 7 (MD&A) — a new `section` value, no schema migration.
- **Phase 2 OLAP:** `agent_events` is append-only and unbounded (clickstream-shaped)
  — the natural candidate to flow to Delta (bronze→silver→gold) for tool-choice,
  retrieval-quality-over-time, and latency dashboards. Prices/chunks are too small
  to deserve a lakehouse. Every table already carries `created_at`/`updated_at` for
  incremental extraction.

---

## Run / deploy

Setup order (see `notebooks/00_setup.py` header): LinkedIn identity verification →
secret scopes (`capstone/massive_api_key`, `lakebase/password`) → Lakebase active →
run `00` (schema) → `01` (prices) → `02` (filings) → `03`/`04` (validate). Deploy
both apps **from workspace**; grant each app's service principal the Lakebase
resource and CAN QUERY on the serving endpoints it uses.

Secrets are stored in **Databricks secret scopes** and referenced by name — never
hardcoded in application code.
