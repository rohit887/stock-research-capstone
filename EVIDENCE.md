# Evidence the system works

Databricks Free Edition apps auto-stop (and hit a daily run limit), so this
consolidates proof captured **from the two deployed apps while they were running**,
plus the measured eval outputs. Screenshots are in [`snapshots/`](snapshots/).

---

## 1. Both apps deployed on Databricks Apps

| App | Screenshot | Evidence |
|---|---|---|
| Streamlit frontend `stock-research-assistant` | [`snapshots/app-frontend.png`](snapshots/app-frontend.png) | deployed from `/Workspace/…/frontend`, deploy history, live URL |
| MCP server `stock-research-mcp-server` | [`snapshots/mcp-server.png`](snapshots/mcp-server.png) | deployed from `/Workspace/…/mcp-server-app`, app resources: Lakebase `databricks_postgres` (branch `production`) + secret, service principal |

---

## 2. Live tool calls in the running app

Seven questions answered by the deployed frontend, each with the **tool-call trace
expanded** — proof the agent actually invokes tools (not hallucination) and grounds
answers in retrieved data. See [`snapshots/question1.png`](snapshots/question1.png)
… `question7.png`.

**Example — `question1.png` (filing retrieval):**
> **Q:** *What does Micron say about DRAM and NAND pricing cyclicality in its risk factors?*
>
> **A:** *"Micron (MU) discusses DRAM and NAND pricing cyclicality in its risk
> factors under Item 1A… significant volatility in average selling prices… DRAM
> average selling prices have ranged from plus low 40% to a minus high 40%… NAND…
> plus low 30% to a minus low 50%… This is research, not investment advice."*
>
> **Tool call shown:** `search_filings · 645 ms`
> `{"query": "DRAM and NAND pricing cyclicality risk factors", "ticker": "MU", "section": "Item 1A"}`

The seven snapshots collectively exercise the tool suite: `search_filings`
(retrieval), `get_price_summary` / `compare_tickers` (price analytics),
`assess_risk_signal` (combined reasoning), and the write tools
(`add_to_watchlist`, `save_research_note`) — the sidebar shows the resulting
watchlist (`AAPL, MU, NVDA`) and saved notes, which **persist across page refresh**
(state written to Lakebase).

---

## 3. Measured eval results

### A. Retrieval quality — recall@5 + MRR (16 answerable questions, 3 configs)
| config | recall@5 | MRR |
|---|---|---|
| **vector-only** | **0.938** | **0.688** |
| bm25-only | 0.312 | 0.200 |
| hybrid (RRF) | 0.812 | 0.591 |

Honest finding: **vector beat hybrid** on this corpus — BM25 helps only on rare
exact tokens (DRAM/NAND, "interest rate"); equal-weight RRF fuses its noise. A
*measured* result, reported as-is (see README). **Adversarial: 0/4** out-of-corpus
questions surfaced a matching passage — no fabricated relevance.

### B. Tool selection — correct-tool rate
**14/15** on 15 natural-language prompts; the single miss was an under-specified
`save_research_note` docstring, since fixed. `agent_events` logs every real tool
call automatically.

---

## 4. Data loaded (pipelines ran end-to-end)

| Table | Rows |
|---|---|
| `companies` | 36 |
| `price_history` | ~9,900 (≈275 trading days × 36) |
| `filings` | 36 (latest 10-K each) |
| `filing_chunks` | 3,377 Item 1A chunks, 1024-dim embeddings |
| `agent_events` | populated (one row per tool invocation) |

Prices via Massive API (plain Python, rate-limited); filings via SEC EDGAR + a
Spark RDD pipeline (fetch → strip HTML → extract Item 1A → chunk) → embedded with
`databricks-gte-large-en`.
