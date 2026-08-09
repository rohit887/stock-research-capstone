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

---

## Architecture

- **Lakebase (Postgres 17)** — single store: user state, prices, filing chunks, embeddings.
- **Lakebase Search** — `lakebase_vector` + `lakebase_text` for hybrid (vector + BM25) retrieval fused by RRF.
- **Model serving** — `databricks-llama-4-maverick` (chat), `databricks-gte-large-en` (embeddings, 1024-dim).
- **MCP server** — custom, 6 tools (2 write to Lakebase), deployed as the Databricks App `mcp-stock-research`.
- **Frontend** — a separate Streamlit Databricks App.
- **Deliberately excluded** — Genie, Delta tables, Databricks Vector Search, Agent Bricks.

Two deployables, one repo:

```
stock-research-capstone/
├── mcp-server/          # MCP server app (thin @mcp.tool wrappers + adapters)
│   ├── server.py            # @mcp.tool functions only
│   ├── market_data.py       # Massive + EDGAR HTTP, parsing
│   └── research_store.py    # all Lakebase queries
├── frontend/            # Streamlit app
│   ├── app.py               # UI
│   ├── lakebase.py          # connection layer
│   └── agent.py             # MCP client + system prompt + serving calls
├── notebooks/           # ingestion + eval (run in Databricks)
│   ├── 00_setup.py          # extensions + schema + checks
│   ├── 01_ingest_prices.py  # plain Python, rate-limited
│   ├── 02_ingest_filings.py # Spark
│   └── 03_eval_retrieval.py # graded question set
├── sql/schema.sql       # Lakebase DDL
├── capstone-schema.md   # schema rationale + hybrid-search (RRF) query pattern
└── capstone-scope-stock-research-assistant.md   # full scope
```

---

## Setup (new account)

Do these **in order** — outbound internet is gated behind the first step.

1. **LinkedIn identity verification** in Databricks (unlocks outbound internet).
2. Create the secret scope and store the Massive key:
   ```bash
   databricks secrets create-scope capstone
   databricks secrets put-secret capstone massive_api_key
   ```
3. Create the Lakebase project; confirm compute is **Active**.
4. Pull this repo into a Databricks Git folder.
5. Run `notebooks/00_setup.py` — installs the 3 extensions, runs `sql/schema.sql`,
   confirms both serving models, and prints table row counts.

Deploy the two apps **from workspace** (not from Git source). Sync loop:
push locally → **Pull** in the Databricks Git folder → **Deploy**.

---

## Build status

| Phase | Deliverable | Status |
|---|---|---|
| 0 | Repo bootstrap + schema (`sql/schema.sql`, `00_setup.py`) | ✅ built |
| 1 | Price ingestion (`01_ingest_prices.py`) | ✅ built |
| 2 | EDGAR filing ingestion, Spark (`02_ingest_filings.py`) | ✅ built |
| 3 | Retrieval test (`03_eval_retrieval.py` v1) | ✅ built |
| 4 | Adapter modules (`market_data.py`, `research_store.py`) | ✅ built |
| 5 | MCP server (`server.py`, deploy `mcp-stock-research`) | ✅ built |
| 6 | Agent (`agent.py`) | ✅ built |
| 7 | Streamlit frontend (`app.py`) | ⏳ |
| 8 | Evals (3 retrieval configs + tool-selection rate) | ⏳ |
| 9 | Submission polish | ⏳ |

---

## Design decisions

_Filled in as phases land (why single-store, why hybrid search, why not Agent
Bricks, why Spark for filings but not prices). See `capstone-schema.md` for the
schema rationale and the RRF query pattern._

## Eval results

_Populated in Phase 8: recall@5 + MRR across vector-only / BM25-only / hybrid,
plus tool-selection correct-rate._
