"""MCP Server for Stock Research Assistant.

Exposes research tools via FastMCP that agents can call to:
- Search SEC 10-K filings (hybrid semantic + BM25)
- Get stock price analytics from ingested historical data
- Compare multiple tickers on risk metrics
- Assess risk signals from price action
- Manage user watchlists and research notes
- Log agent events for observability

All database access and external API calls are delegated to research_store.py
and market_data.py adapter modules in the sibling mcp-server/ directory.
"""
import time
import os
from fastmcp import FastMCP
import research_store as rs

mcp = FastMCP("Stock Research Assistant")


@mcp.tool()
def search_filings(
    query: str,
    ticker: str | None = None,
    section: str | None = None,
    top_k: int = 5
) -> list[dict]:
    """Search SEC 10-K filing chunks using hybrid semantic + BM25 retrieval.
    
    Args:
        query: Natural language search query
        ticker: Optional ticker symbol to filter results (e.g., "AAPL")
        section: Optional section to filter (e.g., "Item 1A")
        top_k: Number of results to return (default 5)
    
    Returns:
        List of matching chunks with ticker, section, text, and relevance score
    """
    start = time.time()
    results = rs.search_filings(query, ticker=ticker, section=section, top_k=top_k)
    latency_ms = int((time.time() - start) * 1000)
    rs.log_event(
        user_id=None,
        user_message=query,
        tool_called="search_filings",
        tool_args={"ticker": ticker, "section": section, "top_k": top_k},
        latency_ms=latency_ms,
        result_count=len(results)
    )
    return results


@mcp.tool()
def get_price_summary(ticker: str, period: str = "1y") -> dict:
    """Get price performance, volatility, and drawdown for a ticker.
    
    Args:
        ticker: Stock ticker symbol (e.g., "NVDA")
        period: Time period - one of: 1m, 3m, 6m, ytd, 1y, all (default "1y")
    
    Returns:
        Dict with return %, annualized volatility %, max drawdown %, high/low, trading days
    """
    start = time.time()
    result = rs.get_price_summary(ticker, period=period)
    latency_ms = int((time.time() - start) * 1000)
    rs.log_event(
        user_id=None,
        user_message=None,
        tool_called="get_price_summary",
        tool_args={"ticker": ticker, "period": period},
        latency_ms=latency_ms,
        result_count=1
    )
    return result


@mcp.tool()
def compare_tickers(
    tickers: list[str],
    metric: str = "return",
    period: str = "1y"
) -> dict:
    """Compare multiple tickers on a single price metric.
    
    Args:
        tickers: List of ticker symbols to compare
        metric: Metric to rank by - "return", "volatility", or "drawdown" (default "return")
        period: Time period - one of: 1m, 3m, 6m, ytd, 1y, all (default "1y")
    
    Returns:
        Dict with ranked tickers (best-first), metric values, and any errors
    """
    start = time.time()
    result = rs.compare_tickers(tickers, metric=metric, period=period)
    latency_ms = int((time.time() - start) * 1000)
    rs.log_event(
        user_id=None,
        user_message=None,
        tool_called="compare_tickers",
        tool_args={"tickers": tickers, "metric": metric, "period": period},
        latency_ms=latency_ms,
        result_count=len(tickers)
    )
    return result


@mcp.tool()
def assess_risk_signal(ticker: str) -> dict:
    """Assess if recent price action warrants review of risk disclosures.
    
    Analyzes 90-day drawdown and 30-day volatility against historical patterns.
    Returns risk signal (normal/elevated/high) with supporting metrics and
    relevant Item 1A passages from the ticker's 10-K.
    
    Args:
        ticker: Stock ticker symbol
    
    Returns:
        Dict with signal level, metrics, risk factor passages, and disclaimer
    """
    start = time.time()
    result = rs.assess_risk_signal(ticker)
    latency_ms = int((time.time() - start) * 1000)
    rs.log_event(
        user_id=None,
        user_message=None,
        tool_called="assess_risk_signal",
        tool_args={"ticker": ticker},
        latency_ms=latency_ms,
        result_count=1
    )
    return result


@mcp.tool()
def add_to_watchlist(user_id: str, ticker: str) -> dict:
    """Add a ticker to a user's watchlist (idempotent).
    
    Args:
        user_id: User identifier
        ticker: Stock ticker symbol to add
    
    Returns:
        Dict with ok status and confirmation
    """
    return rs.add_to_watchlist(user_id, ticker)


@mcp.tool()
def save_research_note(user_id: str, ticker: str | None, note: str) -> dict:
    """Save a research note, optionally associated with a ticker.
    
    Args:
        user_id: User identifier
        ticker: Optional ticker symbol to associate the note with
        note: Note text content
    
    Returns:
        Dict with ok status, note_id, and confirmation
    """
    return rs.save_research_note(user_id, ticker, note)


@mcp.tool()
def get_watchlist(user_id: str) -> list[str]:
    """Get all tickers in a user's watchlist.
    
    Args:
        user_id: User identifier
    
    Returns:
        List of ticker symbols
    """
    return rs.get_watchlist(user_id)


@mcp.tool()
def list_notes(user_id: str, ticker: str | None = None) -> list[dict]:
    """List research notes for a user, optionally filtered by ticker.
    
    Args:
        user_id: User identifier
        ticker: Optional ticker to filter notes by
    
    Returns:
        List of notes with note_id, ticker, text, and timestamp
    """
    return rs.list_notes(user_id, ticker=ticker)


if __name__ == "__main__":
    # For Databricks Apps, use SSE transport on the injected port
    mcp.run(
        transport="sse",
        host="0.0.0.0",
        port=int(os.environ.get("DATABRICKS_APP_PORT", 8000))
    )
