# AI Stock Market Research Assistant — Scope Definition

**Databricks "Rise of the AI Data Engineer" capstone**
Platform: Databricks Free Edition
Last updated: 8 August 2026 (rev 2 — extensions verified, new account, data model,
eval design, repo structure, submission checklist)

---

## 1. Objective

A research assistant that lets a user maintain a watchlist, ask natural-language
questions about what companies have *said* in their SEC filings and how their prices
have *moved*, and have the agent record findings back into persistent state.

**Thesis:** price data tells you *what* happened; filings tell you *why management
thinks* it happened. Combining both in one retrieval layer is the point — neither
alone is interesting.

**Explicitly not:** a trading system, a backtester, or a source of investment advice.
The UI carries a clear disclaimer to that effect.

---

## 2. Capstone requirements → how each is met

| Requirement | Implementation |
|---|---|
| Data pipeline in Spark | EDGAR filing ingestion — fetch, parse sections, chunk, embed, load |
| Third-party API integration | Massive Stocks API (prices) + SEC EDGAR (filings) |
| Processing of unstructured data | 10-K Item 1A risk factors, embedded for semantic retrieval |
| Databricks App with a frontend | Streamlit app |
| An AI agent that does stuff | MCP server exposing 6 tools, 2 of which write to Lakebase |

---

## 3. Architecture

- **Lakebase (Postgres 17)** — single store: user state, prices, filing chunks, embeddings
- **Lakebase Search** — `lakebase_vector` + `lakebase_text` extensions for hybrid retrieval
- **Model serving** — `databricks-llama-4-maverick` (chat), `databricks-gte-large-en` (embeddings, 1024-dim)
- **MCP server** — custom, hosted as a Databricks App named `mcp-stock-research`
- **Frontend** — separate Streamlit Databricks App
- **Secrets** — Massive API key in a Databricks secret scope, never in source or `app.yaml`
- **Deliberately excluded** — Genie, Delta tables, Databricks Vector Search, Agent Bricks

### MCP server module structure

Tool functions stay thin. All HTTP, parsing, and SQL lives in adapter modules —
mirroring the `alpaca_mcp_server.py` / `alpaca_broker.py` split from the Day 3 reference.

```
mcp-stock-research/
├── app.yaml
├── requirements.txt
├── server.py            # @mcp.tool functions only — thin wrappers, docstrings
├── market_data.py       # Massive API + SEC EDGAR: HTTP calls, parsing, returns dicts
└── research_store.py    # All Lakebase queries: retrieval, aggregates, writes
```

No raw `requests` calls inside a `@mcp.tool` function. No raw SQL either.

This is not only a grading requirement — it is what lets the tools be tested as plain
Python functions before any MCP transport exists (build order step 5 before step 6).

### Why not Agent Bricks

The Day 3 reference uses Agent Bricks. Free Edition does not support Knowledge Assistant,
and the availability of other Agent Bricks components is uncertain. The agent is therefore
built directly against `databricks-llama-4-maverick` via the serving endpoint, which also
gives full control over the system prompt and tool-selection logic. Document this choice
in the README rather than leaving it unexplained.

### Secrets handling

```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
api_key = w.secrets.get_secret(scope="capstone", key="massive_api_key")
```

Create the scope once:

```bash
databricks secrets create-scope capstone
databricks secrets put-secret capstone massive_api_key
```

SEC EDGAR needs no key — only a `User-Agent` header with a contact email.

### Why a single store

The agent's most valuable queries combine unstructured retrieval with user state —
for example, *"filings relevant to my watchlist that I haven't written a note on yet."*
Postgres answers that in one SQL statement. Splitting across Delta and Lakebase would
require two round trips and a join in application code.

### Why hybrid search

Pure vector search is weak on rare exact tokens. Financial filings are full of them:
ticker symbols, "Item 1A", specific accounting line items. BM25 catches those; vector
search catches paraphrase and concept. Lakebase Search fuses both in a single query
via reciprocal rank fusion.

---

## 4. Data scope

| | Detail |
|---|---|
| Tickers | 36 S&P 500 large caps across 6 sectors |
| Prices | Daily OHLCV, ~13 months (Massive free tier ceiling), ~3 req/min |
| Filings | Most recent 10-K per ticker, **Item 1A only** at v1 |
| Chunking | ~800 tokens, 100 overlap |
| Embeddings | 1024-dim via `databricks-gte-large-en` |

### Ticker list

```python
TICKERS = [
    # Mega-cap tech
    "AAPL","MSFT","NVDA","GOOGL","AMZN","META","AVGO","ORCL","CRM","ADBE",
    # Semiconductors & hardware
    "AMD","INTC","QCOM","TXN","MU",
    # Financials
    "JPM","BAC","GS","MS","V","MA",
    # Healthcare
    "UNH","JNJ","LLY","PFE","ABBV",
    # Consumer
    "WMT","COST","HD","MCD","NKE",
    # Energy & industrial
    "XOM","CVX","CAT","BA","GE",
]
```

Sector spread is deliberate: it enables cross-sector comparative retrieval
("how do semiconductor firms describe supply-chain risk differently from banks?"),
which is a stronger demonstration than single-ticker lookup.

### Item 7 extensibility

Section is a **column**, not a table. Adding Item 7 (MD&A) later inserts rows —
no migration. The extractor is parameterized by start/end regex markers, so a new
section is a dictionary entry plus a config flag.

### Data model

Eight tables in one Lakebase database. Full DDL, indexes, grants, and the hybrid
search query pattern live in the companion document **`capstone-schema.md`**.

| Table | Grain | Written by |
|---|---|---|
| `companies` | one row per ticker | ingestion |
| `price_history` | one row per ticker per trading day | ingestion |
| `filings` | one row per filing per ticker | ingestion |
| `filing_chunks` | one row per chunk per section per filing | ingestion (Spark) |
| `users` | one row per user | app |
| `watchlist_tickers` | one row per user per ticker | app / agent |
| `research_notes` | one row per note | app / agent |
| `agent_events` | one row per tool invocation | agent — **create in Phase 1** |

`agent_events` is not optional. It is the eval dataset and the seed for Phase 2,
and it cannot be backfilled.

---

## 4a. Repository structure

Two deployables, one repo:

```
stock-research-capstone/
├── mcp-server/
│   ├── app.yaml
│   ├── requirements.txt
│   ├── server.py            # @mcp.tool functions only
│   ├── market_data.py       # Massive + EDGAR HTTP, parsing
│   └── research_store.py    # All Lakebase queries
├── frontend/
│   ├── app.yaml
│   ├── requirements.txt
│   ├── app.py               # Streamlit UI
│   ├── lakebase.py          # connection layer (shared pattern)
│   └── agent.py             # MCP client + system prompt + serving endpoint calls
├── notebooks/
│   ├── 01_ingest_prices.py       # plain Python, rate-limited
│   ├── 02_ingest_filings.py      # Spark
│   └── 03_eval_retrieval.py      # graded question set
├── sql/
│   └── schema.sql
└── README.md
```

Each app's source path points at its subfolder. Deploy **from workspace**, not from
Git — the Git-source path has repeatedly resolved to a Databricks workspace URL and
failed to clone. Sync loop: push locally → Pull in the Databricks Git folder → Deploy.

---

## 5. Agent capabilities

### Read tools — retrieval

| Tool | Purpose |
|---|---|
| `search_filings(query, ticker?, section?, top_k)` | Hybrid semantic + BM25 over filing text |
| `get_price_summary(ticker, period)` | Performance, volatility, drawdown |
| `compare_tickers(tickers, metric)` | Multi-ticker price comparison |

### Read tool — derived judgment

`assess_risk_signal(ticker)` — the one tool that reasons rather than reports.

It combines both data sources instead of passing either through, and it applies
explicit thresholds documented in its own docstring:

```python
def assess_risk_signal(ticker: str) -> dict:
    """Flag whether a stock's recent price action warrants review against the
    risk factors its own management disclosed.

    Combines two sources rather than reporting either:
      - price: drawdown from 90-day peak, and 30-day realized volatility
      - filings: semantic search over this ticker's Item 1A risk factors,
        seeded from the price behaviour observed

    Thresholds (chosen, not derived):
      - drawdown > 15% OR volatility in the ticker's own top quartile -> "elevated"
      - drawdown > 25% -> "high"
      - otherwise -> "normal"

    Returns the signal level, the metrics behind it, and the two or three
    risk-factor passages most relevant to that pattern.
    """
```

This is both a Day 3 requirement (the prediction tool must do more than echo the API)
and the project's best demonstration: it embodies the thesis that price tells you what
happened and filings tell you why management thought it might.

### Write tools

| Tool | Purpose |
|---|---|
| `add_to_watchlist(user_id, ticker)` | Modifies user state |
| `save_research_note(user_id, ticker, note)` | Persists a finding tied to a ticker |

The write tools satisfy the capstone's "agent that does stuff" requirement.
Retrieval alone does not meet the bar.

### Agent system prompt

Must be specific enough that the agent does not fabricate data it did not retrieve.
Cover: what each tool is for and when to prefer one over another; the instruction to
never state a price figure or filing claim not returned by a tool call; behaviour on
tool failure (say so, do not guess); and the standing disclaimer that nothing said
constitutes investment advice.

### Docstrings are load-bearing

Agents select tools from docstrings. `search_filings` and `get_price_summary` both
sound like "get information about a company" to a model. The docstrings must draw
the boundary sharply — one covers *what management wrote about risk and strategy*,
the other covers *numeric price movement*. This is higher leverage than the tool code.

---

## 6. Differentiators

Three things most submissions will not have:

1. **Hybrid retrieval** — BM25 fused with vector search, justified by a real
   problem rather than added as a feature checkbox.
2. **MCP server as a separate deployable** — tools reusable across the Streamlit app,
   the AI Playground, and any future agent. Architecture, not a wrapper.
3. **Eval harness** — measured, with a baseline, reported in the README.

### Eval design

"Fifteen questions" is not a method. Two separate things get measured, because they
fail for different reasons.

**A. Retrieval quality** — 20 questions, each with 1–3 hand-labelled correct chunk IDs.

| Question type | Count | Tests |
|---|---|---|
| Exact-token ("what does MU say about Item 1A cyclicality") | 6 | BM25 strength |
| Paraphrase ("which chipmakers worry about customer concentration") | 6 | Vector strength |
| Cross-sector comparative | 4 | Retrieval breadth |
| Adversarial — answer not in corpus | 4 | Whether it returns nothing rather than noise |

Metrics: recall@5 and MRR. **Run three configurations** — vector-only, BM25-only,
hybrid — and report all three. A single hybrid number proves nothing; the comparison
is the finding, and it either validates the design choice or honestly contradicts it.

**B. Tool selection** — 15 natural-language prompts with the expected tool labelled.
Metric: correct-tool rate. This is where under-specified docstrings show up, and
`agent_events` captures the data automatically.

Report both, including whatever came out worse than hoped. A measured negative result
is stronger evidence of engineering than an unmeasured claim of success.

---

## 7. Out of scope

- Real-time or intraday prices (free tier is end-of-day only)
- Backtesting beyond the 13-month window
- Multi-user authentication (single-user or simple user_id)
- Options, crypto, futures
- Fundamentals, earnings-call transcripts
- Item 7 MD&A at v1

---

## 8. Success criteria

**Minimum viable**
- Prices and Item 1A filings loaded for all 36 tickers
- Semantic search returns relevant passages
- Agent correctly calls at least one read and one write tool
- App deployed and functional

**Complete**
- Hybrid search working, with a documented rationale
- All 6 tools reliable, including `assess_risk_signal`
- Adapter modules cleanly separated from tool definitions
- Massive key in a Databricks secret scope
- Eval results reported
- At least 3 worked examples of natural-language questions with tool calls and answers
- README explains architecture decisions, including why not Agent Bricks

**Stretch**
- Item 7 added
- Cross-ticker comparative retrieval
- Latency benchmark: `lakebase_ann` vs plain HNSW

---

## 9. Verified dependencies

Originally tested 7 Aug 2026 on Free Edition.

**A new Databricks account and a new Massive API key are now in use.** Nothing carries
over between accounts — re-run every check marked ⟳ in the new workspace before building.

| Dependency | Status | Re-verify |
|---|---|---|
| Massive API — daily bars | ✅ 276 trading days, full OHLCV | ⟳ new key |
| Massive — history depth | ⚠️ ~13 months; 3 years returns `403 NOT_AUTHORIZED` | ⟳ new key |
| Massive — rate limit | ⚠️ 429 on 4th request (~3/min, not the documented 5/min) | ⟳ new key |
| SEC EDGAR | ✅ 200, requires `User-Agent` with contact email | ⟳ outbound allowlist is per-account |
| `databricks-llama-4-maverick` | ✅ responds | ⟳ model availability may differ |
| `databricks-gte-large-en` | ✅ 1024 dimensions confirmed | ⟳ |
| Outbound internet | ✅ unlocked by LinkedIn identity verification | ⟳ **do this first** |
| `vector` | ✅ 0.8.0 | ⟳ per-database |
| `lakebase_vector` | ✅ 1.0.0-dev | ⟳ per-database |
| `lakebase_text` | ✅ 0.1.0-dev | ⟳ per-database |

Note the `-dev` version suffixes: these are genuinely early builds, not just beta
branding. Verify index syntax against current docs rather than assuming.

### New-account setup checklist

- [ ] LinkedIn identity verification (unlocks outbound internet — nothing works without it)
- [ ] EDGAR reachability test
- [ ] `w.serving_endpoints.list()` — confirm both models present
- [ ] Lakebase project created, compute Active
- [ ] Three extensions installed and confirmed
- [ ] New Massive key tested for bar count and rate limit
- [ ] Secret scope `capstone` created, key stored
- [ ] `ENDPOINT_NAME` copied from the Computes tab
- [ ] Git credential linked (existing GitHub account is fine)

---

## 10. Known risks

| Risk | Mitigation |
|---|---|
| 10-K section extraction fails on inconsistent HTML | Match the **last** occurrence of the start pattern to skip the table of contents; log extracted length per filing to spot failures early |
| Massive rate limit stalls ingestion | Checkpoint per ticker; exponential backoff starting ~25s |
| Lakebase Search is Beta | It is pgvector-compatible — fall back to HNSW by changing one index type |
| App auto-stops after 24 hours | Redeploy immediately before submitting; record a video walkthrough |
| Free Edition quota shutdown | Small batches; do not leave compute idle |
| SDK version drift | `databricks-sdk>=0.125.0` required for the `w.postgres` API |

---

## 11. Build order

0. **Secret scope** — create `capstone`, store the Massive API key
1. **Lakebase schema** — tables, `vector(1024)` column, `lakebase_ann` + `lakebase_bm25` indexes
2. **Price ingestion** — 36 tickers, backoff, checkpointed (plain Python, not Spark)
3. **EDGAR ingestion (Spark)** — fetch, extract Item 1A, chunk, embed, load
4. **Retrieval test** — validate search quality standalone, before any agent exists
5. **Adapter modules** — `market_data.py` and `research_store.py` as plain Python,
   tested directly against Lakebase
6. **MCP server** — thin `@mcp.tool` wrappers over the working adapters, deploy as
   `mcp-stock-research`
7. **Agent** — system prompt, tool registration against the MCP endpoint
8. **Streamlit frontend** — separate app
9. **Evals** — graded question set, measured, documented

Steps 3 and 9 are the differentiators. Protect time for them.

Step 5 before step 6 matters: debugging retrieval quality through an MCP transport
layer is considerably harder than debugging a function call. The adapter split is what
makes this possible — it is a testing strategy, not just a code-organisation rule.

Prices deliberately do not use Spark. At ~3 requests per minute the job is rate-limited,
not compute-limited, so parallelism buys nothing. Say so in the README — knowing when
not to reach for Spark is worth more than using it everywhere.

---

## 12. Optional Phase 2 — adding an OLAP layer

Not required for submission. Gate it strictly: **do not start until Phase 1 is
complete and tested.** A finished single-store project beats a half-finished
two-store one.

### Design for it now (costs nothing today)

- Every table carries `created_at` / `updated_at` — enables incremental extraction later
- Log every agent interaction from day one:

```sql
CREATE TABLE agent_events (
    event_id     BIGSERIAL PRIMARY KEY,
    user_id      TEXT,
    ts           TIMESTAMPTZ NOT NULL DEFAULT now(),
    user_message TEXT,
    tool_called  TEXT,
    tool_args    JSONB,
    latency_ms   INT,
    result_count INT
);
```

### Why events, and not prices, are the OLAP candidate

10,000 price rows and 2,000 filing chunks do not deserve a lakehouse. Copying them
to Delta would be theatre.

The agent event stream is different: append-only, unbounded, grows with every
interaction, and nobody ever queries a single row of it. That is a genuine analytical
workload — the same shape as clickstream. The split is therefore defensible rather
than decorative, and it mirrors the bootcamp architecture diagram exactly: operational
store on the left, event data flowing right.

### What it would answer

- Which tools does the agent choose, and how often does it choose wrong?
- Is hybrid retrieval beating vector-only, measured over time?
- Latency distribution per tool
- Which tickers and topics users actually ask about
- Whether users save notes after retrieval, or abandon

This merges the eval harness with a real pipeline instead of making them compete for time.

### Mechanism

- Lakebase to Delta via CDF or synced tables
- Spark: bronze events -> silver typed/cleaned -> gold aggregates
- Dashboard on the gold tables
- Optionally write a metric back to Lakebase, closing the loop

Free Edition limits apply: one active pipeline per type, five concurrent job tasks.

---

## 13. Submission checklist

Databricks Apps stop roughly 24 hours after deploy. **Capture everything before the
URL dies, and redeploy immediately before submitting.**

- [ ] Both apps redeployed, confirmed running
- [ ] Frontend URL recorded
- [ ] MCP server URL recorded
- [ ] Video walkthrough (2–4 min): watchlist → chat question → visible tool call →
      answer citing a real filing passage → note saved → refresh proving persistence
- [ ] Screenshot: frontend with data
- [ ] Screenshot: Lakebase tables with row counts
- [ ] Screenshot: AI Playground showing the agent calling MCP tools
- [ ] At least 3 worked natural-language examples with tool calls and final answers
- [ ] Eval results table (three retrieval configs + tool-selection rate)
- [ ] Source zipped: `git archive -o capstone-source.zip HEAD`
- [ ] README covering architecture, why single-store, why hybrid search, why not
      Agent Bricks, why Spark for filings but not prices, limitations, and what
      would come next
- [ ] Disclaimer visible in the UI

The video matters more than usual here. It is the only artifact that survives the app
auto-stopping, and it is what a grader sees if they open the link a day later.

---

## 14. Reference notes

**Endpoint name format** (for `ENDPOINT_NAME` in `app.yaml`):
`projects/<project>/branches/<branch>/endpoints/<endpoint>` — human-readable names, not UUIDs.

**Serving endpoint call syntax** (typed objects, not dicts):

```python
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

r = w.serving_endpoints.query(
    name="databricks-llama-4-maverick",
    messages=[ChatMessage(role=ChatMessageRole.USER, content="...")]
)
print(r.choices[0].message.content)
```

```python
e = w.serving_endpoints.query(
    name="databricks-gte-large-en",
    input=["text to embed"]
)
print(len(e.data[0].embedding))   # 1024
```

**Massive API base:** `https://api.massive.com`, auth via `?apiKey=` or `Authorization: Bearer`.
Daily bars: `/v2/aggs/ticker/{ticker}/range/1/day/{from}/{to}`

**MCP server app names must start with `mcp-`** to be recognized in the AI Playground.
The endpoint lands at `https://<app-url>/mcp`.
