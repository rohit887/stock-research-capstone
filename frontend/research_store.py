"""research_store.py — all Lakebase access for the research assistant.

Every SQL statement and the query-embedding call live here, so the MCP tool
functions in server.py stay thin wrappers with no raw SQL. Testable as plain
Python: import this module in a notebook, set the PG* env vars, and call the
functions directly against Lakebase (build-order step 5, before the MCP server).

Connection config comes from environment variables (12-factor), so the same code
runs in a test notebook and in the deployed Databricks App:

    PGHOST, PGDATABASE (default databricks_postgres), PGUSER (default student),
    PGPASSWORD, PGPORT (default 5432)

Writes and multi-statement work use the native-password `student` role, which has
session affinity on Lakebase Autoscaling (OAuth tokens can route to different
backend instances — see docs/lakebase-authentication-guide.md).
"""
from __future__ import annotations

import os
import re
import math
import json
import time
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import execute_values

EMBED_ENDPOINT = "databricks-gte-large-en"
EMBED_DIM = 1024
RRF_K = 60          # reciprocal-rank-fusion constant
RRF_POOL = 50       # candidates pulled from each retriever before fusion

# Question / structural words dropped so BM25 ORs only discriminating content
# terms. "item"/"1a" appear in every Item 1A chunk, so they are pure noise.
_STOP = {
    "the", "a", "an", "of", "to", "and", "or", "in", "on", "for", "do", "does",
    "did", "what", "how", "is", "are", "was", "were", "be", "been", "about",
    "from", "with", "by", "as", "at", "that", "this", "these", "those", "our",
    "we", "us", "you", "your", "it", "its", "their", "they", "which", "who",
    "when", "where", "why", "can", "could", "should", "would", "may", "might",
    "will", "than", "then", "say", "says", "describe", "worry", "worries",
    "item", "1a", "1b", "1c", "part",
}


# --------------------------------------------------------------------------
# Connection + query embedding
# --------------------------------------------------------------------------
@contextmanager
def get_connection():
    """Yield a psycopg2 connection from Databricks secret.
    
    Fetches the full PostgreSQL connection URL from a Databricks secret at runtime.
    The secret location is specified via LAKEBASE_SECRET_SCOPE and LAKEBASE_SECRET_KEY
    environment variables (set in app.yaml).
    
    The URL is stored plain in the secret; Databricks base64-encodes it at rest,
    so we decode it once here before passing to psycopg2.
    
    Pattern from: Lakebase + Databricks App secrets runbook
    """
    import base64
    import sys
    
    scope = os.environ.get("LAKEBASE_SECRET_SCOPE")
    key = os.environ.get("LAKEBASE_SECRET_KEY")
    
    if not scope or not key:
        raise ValueError(
            "Missing LAKEBASE_SECRET_SCOPE or LAKEBASE_SECRET_KEY environment variables. "
            "Set these in app.yaml to point to the secret containing the PostgreSQL connection URL."
        )
    
    # Fetch the secret via Databricks SDK
    secret = _workspace_client().secrets.get_secret(scope=scope, key=key)
    
    # Decode once (Databricks base64-encodes secrets at rest)
    connection_url = base64.b64decode(secret.value).decode("utf-8")
    
    # Validate it's a proper URL (not double-encoded)
    if not connection_url.startswith("postgresql://"):
        raise ValueError(
            f"Secret does not contain a valid PostgreSQL URL. "
            f"Got: {connection_url[:40]!r}... (may be double-encoded)"
        )
    
    print(f"[DEBUG] Connecting via secret {scope}/{key}", file=sys.stderr, flush=True)
    
    # Connect using the full URL
    conn = psycopg2.connect(connection_url)
    try:
        yield conn
    finally:
        conn.close()


_ws_client = None


def _workspace_client():
    global _ws_client
    if _ws_client is None:
        from databricks.sdk import WorkspaceClient
        _ws_client = WorkspaceClient()
    return _ws_client


def embed_query(text: str) -> list[float]:
    """Embed a query string to a 1024-dim vector via the serving endpoint."""
    resp = _workspace_client().serving_endpoints.query(name=EMBED_ENDPOINT, input=[text])
    return resp.data[0].embedding


def _vec_literal(emb: list[float]) -> str:
    return "[" + ",".join(f"{x:.6f}" for x in emb) + "]"


def _or_tsquery(query: str) -> str:
    """NL query -> OR-ed tsquery of content lexemes (plainto_tsquery ANDs, which
    is too strict and returns nothing for natural-language questions)."""
    seen, uniq = set(), []
    for w in re.findall(r"[a-z0-9]+", query.lower()):
        if len(w) > 1 and w not in _STOP and w not in seen:
            seen.add(w)
            uniq.append(w)
    return " | ".join(uniq)


# --------------------------------------------------------------------------
# Retrieval — hybrid (RRF), the core of search_filings
# --------------------------------------------------------------------------
def search_filings(query: str, ticker: str | None = None, section: str | None = None,
                   top_k: int = 5) -> list[dict]:
    """Hybrid semantic + BM25 retrieval over filing chunks (RRF fusion).

    Returns a list of dicts: chunk_id, ticker, section, chunk_text, score.
    """
    qv = _vec_literal(embed_query(query))
    tsq = _or_tsquery(query)
    sql = """
        WITH filtered AS (
            SELECT chunk_id, ticker, section, chunk_text, embedding
            FROM filing_chunks
            WHERE embedding IS NOT NULL
              AND (%(tk)s IS NULL OR ticker = %(tk)s)
              AND (%(sec)s IS NULL OR section = %(sec)s)
        ),
        vec AS (
            SELECT chunk_id, ROW_NUMBER() OVER (ORDER BY embedding <=> %(qv)s::vector) AS rank
            FROM filtered ORDER BY embedding <=> %(qv)s::vector LIMIT %(pool)s
        ),
        bm25 AS (
            SELECT chunk_id, ROW_NUMBER() OVER (
                       ORDER BY ts_rank_cd(to_tsvector('english', chunk_text),
                                           to_tsquery('english', %(q)s)) DESC) AS rank
            FROM filtered
            WHERE %(q)s <> '' AND to_tsvector('english', chunk_text) @@ to_tsquery('english', %(q)s)
            LIMIT %(pool)s
        ),
        fused AS (
            SELECT chunk_id, SUM(1.0 / (%(k)s + rank)) AS rrf
            FROM (SELECT chunk_id, rank FROM vec
                  UNION ALL
                  SELECT chunk_id, rank FROM bm25) u
            GROUP BY chunk_id
        )
        SELECT f.chunk_id, c.ticker, c.section, c.chunk_text, f.rrf
        FROM fused f JOIN filtered c USING (chunk_id)
        ORDER BY f.rrf DESC
        LIMIT %(topk)s;
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, {"qv": qv, "q": tsq, "tk": ticker, "sec": section,
                          "k": RRF_K, "pool": RRF_POOL, "topk": top_k})
        rows = cur.fetchall()
    return [{"chunk_id": r[0], "ticker": r[1], "section": r[2],
             "chunk_text": r[3], "score": float(r[4])} for r in rows]


# --------------------------------------------------------------------------
# Price analytics (from the ingested price_history — no live API, no rate limit)
# --------------------------------------------------------------------------
_PERIOD_DAYS = {"1m": 21, "3m": 63, "6m": 126, "ytd": None, "1y": 252, "all": None}


def _fetch_closes(cur, ticker: str, period: str):
    """Return [(trade_date, close)] ascending for the requested trailing period."""
    if period == "ytd":
        cur.execute("""
            SELECT trade_date, close FROM price_history
            WHERE ticker = %s AND trade_date >= date_trunc('year', CURRENT_DATE)
            ORDER BY trade_date;""", (ticker,))
    elif period == "all" or _PERIOD_DAYS.get(period) is None:
        cur.execute("""
            SELECT trade_date, close FROM price_history
            WHERE ticker = %s ORDER BY trade_date;""", (ticker,))
    else:
        n = _PERIOD_DAYS[period]
        cur.execute("""
            SELECT trade_date, close FROM (
                SELECT trade_date, close FROM price_history
                WHERE ticker = %s ORDER BY trade_date DESC LIMIT %s
            ) t ORDER BY trade_date;""", (ticker, n))
    return [(d, float(c)) for d, c in cur.fetchall() if c is not None]


def _daily_returns(closes: list[float]) -> list[float]:
    return [(closes[i] / closes[i - 1] - 1.0) for i in range(1, len(closes))
            if closes[i - 1]]


def _volatility(returns: list[float], annualize: bool = True) -> float | None:
    if len(returns) < 2:
        return None
    mean = sum(returns) / len(returns)
    var = sum((r - mean) ** 2 for r in returns) / (len(returns) - 1)
    vol = math.sqrt(var)
    return vol * math.sqrt(252) if annualize else vol


def _max_drawdown(closes: list[float]) -> float:
    peak, mdd = closes[0], 0.0
    for c in closes:
        peak = max(peak, c)
        mdd = min(mdd, c / peak - 1.0)
    return mdd


def get_price_summary(ticker: str, period: str = "1y") -> dict:
    """Performance, volatility, and drawdown for a ticker over `period`.

    period in {1m,3m,6m,ytd,1y,all}. Computed from ingested daily closes.
    """
    ticker = ticker.upper()
    with get_connection() as conn, conn.cursor() as cur:
        rows = _fetch_closes(cur, ticker, period)
    if not rows:
        return {"ticker": ticker, "period": period, "error": "no price data"}
    closes = [c for _, c in rows]
    rets = _daily_returns(closes)
    return {
        "ticker": ticker,
        "period": period,
        "start_date": rows[0][0].isoformat(),
        "end_date": rows[-1][0].isoformat(),
        "start_close": round(closes[0], 2),
        "last_close": round(closes[-1], 2),
        "return_pct": round((closes[-1] / closes[0] - 1.0) * 100, 2),
        "annualized_vol_pct": (round(_volatility(rets) * 100, 2) if _volatility(rets) else None),
        "max_drawdown_pct": round(_max_drawdown(closes) * 100, 2),
        "high": round(max(closes), 2),
        "low": round(min(closes), 2),
        "trading_days": len(closes),
    }


def compare_tickers(tickers: list[str], metric: str = "return", period: str = "1y") -> dict:
    """Rank multiple tickers by one price metric.

    metric in {return, volatility, drawdown}. Returns rows sorted best-first
    (highest return, lowest volatility, smallest drawdown).
    """
    metric = metric.lower()
    out = []
    for t in tickers:
        s = get_price_summary(t, period)
        if "error" in s:
            out.append({"ticker": t.upper(), "error": s["error"]})
            continue
        val = {"return": s["return_pct"], "volatility": s["annualized_vol_pct"],
               "drawdown": s["max_drawdown_pct"]}.get(metric)
        out.append({"ticker": s["ticker"], "value": val, "last_close": s["last_close"]})
    ranked = [r for r in out if r.get("value") is not None]
    reverse = metric == "return"          # higher return better; lower vol/dd better
    ranked.sort(key=lambda r: r["value"], reverse=reverse)
    return {"metric": metric, "period": period, "ranking": ranked,
            "unranked": [r for r in out if r.get("value") is None]}


# --------------------------------------------------------------------------
# Derived judgment — assess_risk_signal (the one tool that reasons)
# --------------------------------------------------------------------------
def _thirty_day_vol_percentile(cur, ticker: str, current_vol: float) -> float | None:
    """Where the current 30-day realized vol sits within this ticker's own history
    of rolling 30-day vols (0..1). Used for the 'own top quartile' threshold."""
    cur.execute("""
        SELECT close FROM price_history WHERE ticker = %s ORDER BY trade_date;""", (ticker,))
    closes = [float(c) for (c,) in cur.fetchall() if c is not None]
    if len(closes) < 60 or current_vol is None:
        return None
    rets = _daily_returns(closes)
    window = 30
    hist = []
    for i in range(window, len(rets)):
        v = _volatility(rets[i - window:i], annualize=True)
        if v is not None:
            hist.append(v)
    if not hist:
        return None
    below = sum(1 for v in hist if v <= current_vol)
    return below / len(hist)


def assess_risk_signal(ticker: str) -> dict:
    """Flag whether recent price action warrants review against the ticker's own
    disclosed risk factors. Combines two sources rather than reporting either.

    Thresholds (chosen, not derived):
      - drawdown > 25%                                   -> "high"
      - drawdown > 15% OR 30-day vol in own top quartile -> "elevated"
      - otherwise                                        -> "normal"
    Returns the signal, the metrics behind it, and the 2-3 most relevant Item 1A
    passages, seeded from the observed price behaviour.
    """
    ticker = ticker.upper()
    with get_connection() as conn, conn.cursor() as cur:
        rows = _fetch_closes(cur, ticker, "3m")           # ~63 trading days ~ 90 cal days
        all_rows = _fetch_closes(cur, ticker, "all")
        if not rows or len(rows) < 20:
            return {"ticker": ticker, "signal": "unknown", "error": "insufficient price data"}
        closes90 = [c for _, c in rows]
        drawdown = _max_drawdown(closes90)                # negative fraction
        closes_all = [c for _, c in all_rows]
        vol30 = _volatility(_daily_returns(closes_all[-31:]))   # 30 trailing daily returns
        pct = _thirty_day_vol_percentile(cur, ticker, vol30)

    top_quartile = pct is not None and pct >= 0.75
    if drawdown <= -0.25:
        signal = "high"
    elif drawdown <= -0.15 or top_quartile:
        signal = "elevated"
    else:
        signal = "normal"

    # Seed the filing search from the observed behaviour.
    seed = {
        "high": "risk factors that could cause a significant decline in the stock price",
        "elevated": "risks related to price volatility, demand weakness, and margin pressure",
        "normal": "principal risk factors affecting the business",
    }[signal]
    passages = search_filings(seed, ticker=ticker, section="Item 1A", top_k=3)

    return {
        "ticker": ticker,
        "signal": signal,
        "metrics": {
            "drawdown_90d_pct": round(drawdown * 100, 2),
            "volatility_30d_pct": round(vol30 * 100, 2) if vol30 else None,
            "vol_percentile_own_history": round(pct, 2) if pct is not None else None,
            "in_own_top_quartile": top_quartile,
        },
        "thresholds": "high: dd>25%; elevated: dd>15% or vol in own top quartile",
        "risk_passages": [{"ticker": p["ticker"], "text": p["chunk_text"][:600],
                           "score": p["score"]} for p in passages],
        "disclaimer": "Signal is a heuristic for review, not investment advice.",
    }


# --------------------------------------------------------------------------
# Writes — the "agent that does stuff" tools
# --------------------------------------------------------------------------
def _ensure_user(cur, user_id: str):
    cur.execute("""
        INSERT INTO users (user_id) VALUES (%s)
        ON CONFLICT (user_id) DO NOTHING;""", (user_id,))


def add_to_watchlist(user_id: str, ticker: str) -> dict:
    """Add a ticker to a user's watchlist (idempotent). Modifies user state."""
    ticker = ticker.upper()
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM companies WHERE ticker = %s;", (ticker,))
        if not cur.fetchone():
            return {"ok": False, "error": f"unknown ticker {ticker}"}
        _ensure_user(cur, user_id)
        cur.execute("""
            INSERT INTO watchlist_tickers (user_id, ticker) VALUES (%s, %s)
            ON CONFLICT (user_id, ticker) DO NOTHING;""", (user_id, ticker))
        conn.commit()
    return {"ok": True, "user_id": user_id, "ticker": ticker}


def save_research_note(user_id: str, ticker: str | None, note: str) -> dict:
    """Persist a research note, optionally tied to a ticker. Modifies user state."""
    ticker = ticker.upper() if ticker else None
    with get_connection() as conn, conn.cursor() as cur:
        _ensure_user(cur, user_id)
        cur.execute("""
            INSERT INTO research_notes (user_id, ticker, note)
            VALUES (%s, %s, %s) RETURNING note_id;""", (user_id, ticker, note))
        note_id = cur.fetchone()[0]
        conn.commit()
    return {"ok": True, "note_id": note_id, "user_id": user_id, "ticker": ticker}


def get_watchlist(user_id: str) -> list[str]:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT ticker FROM watchlist_tickers WHERE user_id = %s ORDER BY ticker;""",
                    (user_id,))
        return [r[0] for r in cur.fetchall()]


def list_notes(user_id: str, ticker: str | None = None) -> list[dict]:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT note_id, ticker, note, created_at FROM research_notes
            WHERE user_id = %s AND (%s IS NULL OR ticker = %s)
            ORDER BY created_at DESC;""",
                    (user_id, ticker.upper() if ticker else None,
                     ticker.upper() if ticker else None))
        return [{"note_id": r[0], "ticker": r[1], "note": r[2],
                 "created_at": r[3].isoformat()} for r in cur.fetchall()]


# --------------------------------------------------------------------------
# Observability — agent_events (eval dataset + Phase-2 OLAP seed)
# --------------------------------------------------------------------------
def log_event(user_id: str | None, user_message: str | None, tool_called: str | None,
              tool_args: dict | None, latency_ms: int | None, result_count: int | None):
    """Append one tool-invocation event. Never raises into the caller."""
    try:
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO agent_events
                    (user_id, user_message, tool_called, tool_args, latency_ms, result_count)
                VALUES (%s, %s, %s, %s, %s, %s);""",
                        (user_id, user_message, tool_called,
                         json.dumps(tool_args) if tool_args is not None else None,
                         latency_ms, result_count))
            conn.commit()
    except Exception:  # noqa: BLE001 — logging must never break a tool call
        pass
