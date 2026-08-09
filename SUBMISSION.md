# Submission checklist

Databricks Apps stop ~24h after deploy. **Capture everything before the URL dies,
and redeploy immediately before submitting.** The video is the only artifact that
survives the app auto-stopping.

## Redeploy + record URLs
- [ ] Redeploy `mcp-stock-research`, confirm running — URL: `__________`
- [ ] Redeploy `stock-research-assistant` (frontend), confirm running — URL: `__________`

## Video walkthrough (2–4 min) — the key artifact
Record this exact loop:
- [ ] Add a ticker to the watchlist (sidebar)
- [ ] Ask a combined question (e.g. *"How has NVDA performed this year, and what
      does it say about supply-chain risk?"*)
- [ ] Expand the **🔧 tool call(s)** trace to show `get_price_summary` + `search_filings`
- [ ] Answer cites a real filing passage
- [ ] Ask the agent to save a note → refresh the page → note + watchlist persist
- [ ] Ask an out-of-corpus question (Tesla) → agent declines rather than fabricating

## Screenshots
- [ ] Frontend with data (chat + sidebar)
- [ ] Lakebase tables with row counts (`companies` 36, `price_history` ~9,900,
      `filings` 36, `filing_chunks` 3,377, `agent_events` > 0)
- [ ] AI Playground showing the agent calling the `mcp-stock-research` MCP tools

## Artifacts already in the repo
- [x] README: architecture, single-store, hybrid-search finding, why-not-Agent-Bricks,
      Spark-for-filings-not-prices, limitations, next steps
- [x] Eval results: recall@5 + MRR across vector / bm25 / hybrid + tool-selection rate
- [x] ≥3 worked NL examples with tool calls (README)
- [x] Disclaimer visible in the UI
- [ ] Source zip: `git archive -o capstone-source.zip HEAD`

## Security TODO (before/after submission)
- [ ] Rotate the `student` Lakebase password (it appeared in git history) and update
      the `lakebase/password` secret. Keep all secrets in Databricks secret scopes.
