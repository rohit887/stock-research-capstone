"""server.py — MCP server for the stock research assistant (`mcp-stock-research`).

Thin `@mcp.tool` wrappers only. All HTTP/SQL lives in the adapter modules
(market_data.py, research_store.py); no raw requests or SQL appear here. The
docstrings are load-bearing: the model selects tools from them, so each one draws
a sharp boundary between *what management wrote about risk/strategy* and *numeric
price movement*.

Served over streamable-HTTP; the MCP endpoint lands at `https://<app-url>/mcp`.
App name must start with `mcp-` to be recognized in the AI Playground.
"""
from __future__ import annotations

import os
import time

from mcp.server.fastmcp import FastMCP


def _derive_pghost() -> None:
    """Resolve PGHOST from the Lakebase instance at startup so it isn't hardcoded."""
    if os.environ.get("PGHOST"):
        return
    inst = os.environ.get("LAKEBASE_INSTANCE")
    if not inst:
        return
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    eps = list(w.postgres.list_endpoints(parent=inst))
    if eps:
        ep = w.postgres.get_endpoint(name=eps[0].name)
        if ep.status and ep.status.hosts:
            os.environ["PGHOST"] = ep.status.hosts.host


_derive_pghost()

import research_store as rs  # noqa: E402 — after PGHOST is resolved

_PORT = int(os.environ.get("DATABRICKS_APP_PORT") or os.environ.get("PORT") or "8000")
mcp = FastMCP("mcp-stock-research", host="0.0.0.0", port=_PORT)


def _log(tool: str, args: dict, t0: float, count):
    rs.log_event(args.get("user_id"), None, tool, args, int((time.time() - t0) * 1000), count)


# ==========================================================================
# Read tools — retrieval
# ==========================================================================
@mcp.tool()
def search_filings(query: str, ticker: str | None = None,
                   section: str | None = None, top_k: int = 5) -> list[dict]:
    """Search what company MANAGEMENT WROTE in their SEC 10-K filings.

    Use this for questions about a company's *narrative disclosures* — the risks,
    strategy, competition, supply chain, regulation, and uncertainties that
    management described in Item 1A (Risk Factors). This is the tool for "what does
    X say about ...", "which companies worry about ...", "how do firms describe ...".
    It does NOT know prices or returns — use get_price_summary for numbers.

    Hybrid semantic + keyword (BM25) retrieval fused by reciprocal rank fusion.
    Args:
        query: natural-language question or topic.
        ticker: optional ticker to restrict to one company (e.g. "MU").
        section: optional filing section (currently only "Item 1A").
        top_k: number of passages to return (default 5).
    Returns passages with the ticker, the text, and a relevance score.
    """
    t0 = time.time()
    rows = rs.search_filings(query, ticker=ticker, section=section, top_k=top_k)
    _log("search_filings", {"query": query, "ticker": ticker, "section": section,
                            "top_k": top_k}, t0, len(rows))
    return rows


@mcp.tool()
def get_price_summary(ticker: str, period: str = "1y") -> dict:
    """Summarize how a stock's PRICE has MOVED (numeric performance).

    Use this for quantitative questions about price action of ONE company — total
    return, volatility, drawdown, high/low over a window. This is the tool for
    "how has X performed", "how volatile is X", "what's X's drawdown". It reports
    numbers only and knows nothing about what management wrote — for disclosures
    and risk narrative use search_filings.

    Args:
        ticker: the ticker (e.g. "NVDA").
        period: one of 1m, 3m, 6m, ytd, 1y, all (default 1y).
    Returns return %, annualized volatility %, max drawdown %, high, low, dates.
    """
    t0 = time.time()
    out = rs.get_price_summary(ticker, period)
    _log("get_price_summary", {"ticker": ticker, "period": period}, t0, None)
    return out


@mcp.tool()
def compare_tickers(tickers: list[str], metric: str = "return", period: str = "1y") -> dict:
    """Rank SEVERAL companies against each other on ONE numeric price metric.

    Use this only when comparing multiple tickers by price behaviour — e.g.
    "which of NVDA, AMD, INTC performed best", "rank these banks by volatility".
    For a single company use get_price_summary; for what management wrote use
    search_filings.

    Args:
        tickers: list of tickers to compare.
        metric: return | volatility | drawdown.
        period: 1m, 3m, 6m, ytd, 1y, all (default 1y).
    Returns a best-first ranking (highest return / lowest vol / smallest drawdown).
    """
    t0 = time.time()
    out = rs.compare_tickers(tickers, metric=metric, period=period)
    _log("compare_tickers", {"tickers": tickers, "metric": metric, "period": period},
         t0, len(out.get("ranking", [])))
    return out


@mcp.tool()
def assess_risk_signal(ticker: str) -> dict:
    """Judge whether a stock's RECENT PRICE ACTION warrants review against the risk
    factors ITS OWN MANAGEMENT DISCLOSED — the one tool that reasons over both
    sources rather than reporting either.

    Use this for "is anything concerning about X", "should I look closer at X",
    "does X's price action line up with its stated risks". It combines price
    (90-day drawdown, 30-day volatility vs the ticker's own history) with a
    targeted search of that ticker's Item 1A, and returns a signal
    (normal / elevated / high) plus the metrics and the most relevant risk
    passages. Not investment advice — a heuristic for where to look.

    Args:
        ticker: the ticker to assess (e.g. "MU").
    """
    t0 = time.time()
    out = rs.assess_risk_signal(ticker)
    _log("assess_risk_signal", {"ticker": ticker}, t0, len(out.get("risk_passages", [])))
    return out


# ==========================================================================
# Write tools — the "agent that does stuff"
# ==========================================================================
@mcp.tool()
def add_to_watchlist(user_id: str, ticker: str) -> dict:
    """Add a ticker to the user's persistent watchlist (writes to user state).

    Use when the user asks to watch / follow / track a company. Idempotent.
    Args:
        user_id: the current user's id.
        ticker: the ticker to add (must be one of the covered companies).
    """
    t0 = time.time()
    out = rs.add_to_watchlist(user_id, ticker)
    _log("add_to_watchlist", {"user_id": user_id, "ticker": ticker}, t0, 1 if out.get("ok") else 0)
    return out


@mcp.tool()
def save_research_note(user_id: str, ticker: str | None, note: str) -> dict:
    """Persist a research note, optionally tied to a ticker (writes to user state).

    Use when the user wants to record / save / remember a finding or thought.
    Args:
        user_id: the current user's id.
        ticker: optional ticker the note is about (or null for a general note).
        note: the note text to save.
    """
    t0 = time.time()
    out = rs.save_research_note(user_id, ticker, note)
    _log("save_research_note", {"user_id": user_id, "ticker": ticker}, t0, 1 if out.get("ok") else 0)
    return out


if __name__ == "__main__":
    mcp.run(transport="streamable-http")
