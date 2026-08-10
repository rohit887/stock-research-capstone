# Submission checklist

Free Edition apps auto-stop (and have a daily run limit), so proof of a working
system is captured as **screenshots + documentation** — see [`EVIDENCE.md`](EVIDENCE.md).

## Captured evidence (in the repo)
- [x] Frontend app deployed — `snapshots/app-frontend.png`
- [x] MCP server app deployed — `snapshots/mcp-server.png`
- [x] 7 live Q&A screenshots with visible tool-call traces — `snapshots/question1–7.png`
- [x] `EVIDENCE.md` consolidating deployment, transcripts, eval, and data counts
- [x] README: architecture, honest measured eval, design decisions, worked examples, limitations
- [x] Eval: recall@5 + MRR across vector / bm25 / hybrid + tool-selection rate
- [x] Disclaimer visible in the UI (see snapshots)
- [x] No credentials in source (apps read the Postgres URL from a Databricks secret)

## To do at submission time
- [ ] Zip the source: `git archive -o capstone-source.zip HEAD`
      (snapshots are committed, so they're included; git history is **not**, so the
      zip contains no secrets)
- [ ] (Optional but strongest) 2–4 min screen recording of the app loop:
      watchlist → combined question → visible tool trace → cited passage → save note
      → refresh persists → out-of-corpus question declines
- [ ] Record both live app URLs in the submission text (from the snapshots)

## Security note
The current tree is clean of secrets, and the `git archive` zip includes only the
current tree (no history). Separately, **rotate the `student` Lakebase password**
if convenient — an earlier value appeared in git *history* (not in the zip). All
secrets are stored in Databricks secret scopes and referenced by name only.
