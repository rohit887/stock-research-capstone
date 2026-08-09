"""frontend/agent.py — the research agent (llama-4-maverick + tool calling).

Runs a tool-calling loop against `databricks-llama-4-maverick` over the Databricks
OpenAI-compatible client. Tools are executed in-process via the `research_store`
adapter (imported from the sibling mcp-server/ folder) — the same functions the
deployed MCP server exposes over SSE. One tool layer, two surfaces: the Streamlit
app calls them in-process for reliability; the `mcp-stock-research` app exposes the
identical tools for the AI Playground and any external agent.

Connection + serving auth come from the app environment (the `lakebase` app
resource injects PG* env vars; the app's service principal calls the serving
endpoints), so no secrets live here.
"""
from __future__ import annotations

import os
import sys
import re
import json
import time

# research_store.py is bundled alongside this file (Databricks Apps deploy only the
# app's own folder, so the adapter is copied here rather than imported from a sibling).
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import research_store as rs  # noqa: E402

from databricks.sdk import WorkspaceClient  # noqa: E402

MODEL = "databricks-llama-4-maverick"
MAX_TOOL_ITERS = 6

SYSTEM_PROMPT = """You are a stock research assistant for a fixed universe of 36 \
large-cap US companies. You help the user research what companies SAID in their \
SEC 10-K filings and how their prices MOVED, and you can persist findings.

Tool selection:
- search_filings: what MANAGEMENT WROTE — risks, strategy, competition, supply \
chain (Item 1A). Use for "what does X say about...", "which firms worry about...".
- get_price_summary: NUMERIC price movement for ONE ticker (return, volatility, \
drawdown). Use for "how has X performed / how volatile is X".
- compare_tickers: rank SEVERAL tickers by one price metric.
- assess_risk_signal: combines recent price action with the ticker's own disclosed \
risk factors to flag normal/elevated/high. Use for "is anything concerning about X".
- add_to_watchlist / save_research_note: persist user state when asked to watch a \
company or save a note. get_watchlist / list_notes read that state back.

Rules:
- NEVER state a price figure or a filing claim that a tool did not return. If you \
need a number or a disclosure, call the tool first.
- When you cite a filing, quote or closely paraphrase the returned passage and name \
the ticker; do not invent quotes.
- If a tool returns an error or no data, say so plainly — do not guess or fabricate.
- Prefer the most specific tool; you may call several tools before answering.
- Always include this disclaimer when giving any analysis: "This is research, not \
investment advice." Nothing you say is a recommendation to buy or sell.
"""

# Tool schemas exposed to the model. user_id is NOT exposed — it is injected from
# the session so the model cannot invent an identity for the write tools.
TOOLS = [
    {"type": "function", "function": {
        "name": "search_filings",
        "description": "Search what management wrote in SEC 10-K filings (Item 1A "
                       "risk factors) via hybrid semantic + keyword retrieval.",
        "parameters": {"type": "object", "properties": {
            "query": {"type": "string", "description": "natural-language question/topic"},
            "ticker": {"type": "string", "description": "optional ticker filter, e.g. MU"},
            "section": {"type": "string", "description": "optional section, e.g. 'Item 1A'"},
            "top_k": {"type": "integer", "description": "passages to return", "default": 5},
        }, "required": ["query"]}}},
    {"type": "function", "function": {
        "name": "get_price_summary",
        "description": "Numeric price performance for ONE ticker: return, annualized "
                       "volatility, max drawdown, high/low over a period.",
        "parameters": {"type": "object", "properties": {
            "ticker": {"type": "string"},
            "period": {"type": "string", "enum": ["1m", "3m", "6m", "ytd", "1y", "all"],
                       "default": "1y"},
        }, "required": ["ticker"]}}},
    {"type": "function", "function": {
        "name": "compare_tickers",
        "description": "Rank several tickers by one price metric (best first).",
        "parameters": {"type": "object", "properties": {
            "tickers": {"type": "array", "items": {"type": "string"}},
            "metric": {"type": "string", "enum": ["return", "volatility", "drawdown"],
                       "default": "return"},
            "period": {"type": "string", "enum": ["1m", "3m", "6m", "ytd", "1y", "all"],
                       "default": "1y"},
        }, "required": ["tickers"]}}},
    {"type": "function", "function": {
        "name": "assess_risk_signal",
        "description": "Combine recent price action with the ticker's own disclosed "
                       "risk factors to flag normal/elevated/high with evidence.",
        "parameters": {"type": "object", "properties": {
            "ticker": {"type": "string"}}, "required": ["ticker"]}}},
    {"type": "function", "function": {
        "name": "add_to_watchlist",
        "description": "Add a ticker to the user's watchlist (writes user state).",
        "parameters": {"type": "object", "properties": {
            "ticker": {"type": "string"}}, "required": ["ticker"]}}},
    {"type": "function", "function": {
        "name": "save_research_note",
        "description": "Save or REMEMBER a research note, thought, or reminder, "
                       "optionally tied to a ticker. Use when the user wants to record, "
                       "save, remember, note, or revisit a finding or intention.",
        "parameters": {"type": "object", "properties": {
            "ticker": {"type": "string", "description": "optional ticker"},
            "note": {"type": "string"}}, "required": ["note"]}}},
    {"type": "function", "function": {
        "name": "get_watchlist",
        "description": "List the tickers on the user's watchlist.",
        "parameters": {"type": "object", "properties": {}}}},
    {"type": "function", "function": {
        "name": "list_notes",
        "description": "List the user's saved research notes, optionally by ticker.",
        "parameters": {"type": "object", "properties": {
            "ticker": {"type": "string", "description": "optional ticker filter"}}}}},
]


def _openai_client():
    return WorkspaceClient().serving_endpoints.get_open_ai_client()


def _dispatch(name: str, args: dict, user_id: str):
    """Execute a tool. user_id is injected for the write/read-state tools."""
    if name == "search_filings":
        return rs.search_filings(args["query"], ticker=args.get("ticker"),
                                 section=args.get("section"), top_k=args.get("top_k", 5))
    if name == "get_price_summary":
        return rs.get_price_summary(args["ticker"], period=args.get("period", "1y"))
    if name == "compare_tickers":
        return rs.compare_tickers(args["tickers"], metric=args.get("metric", "return"),
                                  period=args.get("period", "1y"))
    if name == "assess_risk_signal":
        return rs.assess_risk_signal(args["ticker"])
    if name == "add_to_watchlist":
        return rs.add_to_watchlist(user_id, args["ticker"])
    if name == "save_research_note":
        return rs.save_research_note(user_id, args.get("ticker"), args["note"])
    if name == "get_watchlist":
        return rs.get_watchlist(user_id)
    if name == "list_notes":
        return rs.list_notes(user_id, ticker=args.get("ticker"))
    return {"error": f"unknown tool {name}"}


def _result_count(result) -> int:
    if isinstance(result, list):
        return len(result)
    if isinstance(result, dict) and "risk_passages" in result:
        return len(result["risk_passages"])
    return 1


_TOOL_NAMES = {t["function"]["name"] for t in TOOLS}


def _safe_json(s):
    try:
        return json.loads(s or "{}")
    except json.JSONDecodeError:
        return {}


def _coerce(v: str):
    v = v.strip().strip("\"'")
    if re.fullmatch(r"-?\d+", v):
        return int(v)
    if re.fullmatch(r"-?\d+\.\d+", v):
        return float(v)
    low = v.lower()
    if low in ("true", "false"):
        return low == "true"
    if low in ("none", "null", ""):
        return None
    return v


def _parse_text_tool_calls(content: str):
    """Fallback for when the endpoint returns tool calls as TEXT rather than
    structured tool_calls, e.g. Llama's
    `[get_price_summary(ticker=NVDA, period=ytd), search_filings(query=...)]`.
    Only triggers when the content clearly begins with a call/list/python-tag, and
    only recognizes our own tool names (so prose that merely mentions a tool is
    not misparsed)."""
    if not content:
        return []
    s = content.strip()
    if not re.match(r"^\s*(\[|<\|python_tag\|>|[A-Za-z_]\w*\s*\()", s):
        return []
    calls = []
    for i, (name, argstr) in enumerate(re.findall(r"([A-Za-z_]\w*)\s*\(([^)]*)\)", s)):
        if name not in _TOOL_NAMES:
            continue
        args = {}
        for part in argstr.split(","):
            if "=" in part:
                k, v = part.split("=", 1)
                args[k.strip()] = _coerce(v)
        calls.append({"id": f"call_{i}", "name": name, "args": args})
    return calls


def _strip_leaks(text: str) -> str:
    return re.sub(r"<\|[a-z_]+\|>", "", text or "").strip()


def run_agent(user_message: str, history: list | None = None,
              user_id: str = "demo-user", max_iters: int = MAX_TOOL_ITERS) -> dict:
    """Run one turn. Returns {answer, trace, messages}.

    Handles both structured tool_calls and models (llama-4-maverick) that emit tool
    calls as text. `trace` lists the executed tool calls for the UI; `messages` is
    the transcript to feed back as history next turn.
    """
    client = _openai_client()
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    messages += (history or [])
    messages.append({"role": "user", "content": user_message})
    trace = []

    for _ in range(max_iters):
        resp = client.chat.completions.create(
            model=MODEL, messages=messages, tools=TOOLS, tool_choice="auto", temperature=0)
        msg = resp.choices[0].message

        if msg.tool_calls:                                  # structured (preferred)
            calls = [{"id": tc.id, "name": tc.function.name,
                      "args": _safe_json(tc.function.arguments)} for tc in msg.tool_calls]
            assistant_content = msg.content or ""
            from_text = False
        else:                                               # text fallback
            calls = _parse_text_tool_calls(msg.content)
            assistant_content = "" if calls else _strip_leaks(msg.content)
            from_text = True

        if not calls:                                       # final prose answer
            messages.append({"role": "assistant", "content": assistant_content})
            return {"answer": assistant_content, "trace": trace, "messages": messages[1:]}

        messages.append({
            "role": "assistant", "content": assistant_content,
            "tool_calls": [{"id": c["id"], "type": "function",
                            "function": {"name": c["name"], "arguments": json.dumps(c["args"])}}
                           for c in calls],
        })

        for c in calls:
            t0 = time.time()
            try:
                result = _dispatch(c["name"], c["args"], user_id)
            except Exception as e:  # noqa: BLE001 — surface tool failure to the model
                result = {"error": str(e)}
            ms = int((time.time() - t0) * 1000)
            trace.append({"tool": c["name"], "args": c["args"], "result": result, "ms": ms})
            rs.log_event(user_id, user_message, c["name"], c["args"], ms, _result_count(result))
            messages.append({"role": "tool", "tool_call_id": c["id"],
                             "content": json.dumps(result, default=str)})

        if from_text:  # nudge the model to answer in prose, not more tool syntax
            messages.append({"role": "system", "content":
                             "Now answer the user's question in prose using the tool "
                             "results above. Do not emit any tool-call syntax."})

    return {"answer": "I wasn't able to finish within the tool-call limit — please "
                      "narrow the question.", "trace": trace, "messages": messages[1:]}
