"""frontend/app.py — Streamlit UI for the stock research assistant.

Watchlist + chat with visible tool calls + persisted research notes. The chat is
driven by frontend/agent.py (llama-4-maverick tool-calling over the research_store
tools). Lakebase access comes from the app's `lakebase` resource (PG* env vars).
"""
import os
import sys
import json

import streamlit as st

# Self-contained: research_store.py + agent.py are bundled in this folder (Databricks
# Apps deploy only the app's own directory, not sibling folders).
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import research_store as rs          # noqa: E402
import agent                          # noqa: E402  (frontend/agent.py)

st.set_page_config(page_title="Stock Research Assistant", page_icon="📈", layout="wide")

# --------------------------------------------------------------------------
# Session state
# --------------------------------------------------------------------------
if "display" not in st.session_state:
    st.session_state.display = []        # [{role, content, trace?}]
if "agent_history" not in st.session_state:
    st.session_state.agent_history = []  # agent transcript fed back each turn

# --------------------------------------------------------------------------
# Sidebar — identity, watchlist, notes
# --------------------------------------------------------------------------
with st.sidebar:
    st.title("📈 Research Assistant")
    st.caption("SEC 10-K disclosures + price action for 36 large-cap US stocks.")
    user_id = st.text_input("User", value="demo-user")

    st.subheader("Watchlist")
    new_ticker = st.text_input("Add ticker", key="wl_add", placeholder="e.g. NVDA").strip().upper()
    if st.button("Add", use_container_width=True) and new_ticker:
        res = rs.add_to_watchlist(user_id, new_ticker)
        st.success(f"Added {new_ticker}") if res.get("ok") else st.error(res.get("error"))
    try:
        wl = rs.get_watchlist(user_id)
        st.write(", ".join(wl) if wl else "_empty_")
    except Exception as e:  # noqa: BLE001
        st.warning(f"watchlist unavailable: {e}")

    st.subheader("Saved notes")
    try:
        notes = rs.list_notes(user_id)
        if not notes:
            st.write("_none yet_")
        for n in notes[:10]:
            tk = f"[{n['ticker']}] " if n["ticker"] else ""
            st.markdown(f"- {tk}{n['note']}")
    except Exception as e:  # noqa: BLE001
        st.warning(f"notes unavailable: {e}")

    if st.button("Clear chat", use_container_width=True):
        st.session_state.display = []
        st.session_state.agent_history = []
        st.rerun()

st.warning("⚠️ Research and educational tool — **not** investment advice. Nothing here "
           "is a recommendation to buy or sell any security.")

# --------------------------------------------------------------------------
# Chat transcript
# --------------------------------------------------------------------------
for m in st.session_state.display:
    with st.chat_message(m["role"]):
        st.markdown(m["content"])
        if m.get("trace"):
            with st.expander(f"🔧 {len(m['trace'])} tool call(s)"):
                for t in m["trace"]:
                    st.markdown(f"**{t['tool']}**  ·  {t['ms']} ms")
                    st.code(json.dumps(t["args"]), language="json")

prompt = st.chat_input("Ask about filings, prices, or risk — e.g. "
                       "'How has NVDA performed and what does it say about supply-chain risk?'")
if prompt:
    st.session_state.display.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Researching…"):
            try:
                out = agent.run_agent(prompt, history=st.session_state.agent_history,
                                      user_id=user_id)
                answer, trace = out["answer"], out["trace"]
                st.session_state.agent_history = out["messages"]
            except Exception as e:  # noqa: BLE001
                answer, trace = f"Sorry — the agent hit an error: `{e}`", []
        st.markdown(answer)
        if trace:
            with st.expander(f"🔧 {len(trace)} tool call(s)"):
                for t in trace:
                    st.markdown(f"**{t['tool']}**  ·  {t['ms']} ms")
                    st.code(json.dumps(t["args"]), language="json")

    st.session_state.display.append({"role": "assistant", "content": answer, "trace": trace})
