"""market_data.py — third-party HTTP adapters (Massive prices + SEC EDGAR filings).

All outbound HTTP and response parsing lives here so tool functions and ingestion
code never issue raw `requests` calls. Pure functions returning plain dicts/lists,
independently testable.

Config from environment variables:
    MASSIVE_API_KEY       — Massive API key (prices)
    EDGAR_CONTACT_EMAIL   — contact email for the SEC User-Agent (required by SEC)

Prices for the tool layer come from the ingested `price_history` table (see
research_store.get_price_summary) — fast and rate-limit-free. The live fetchers
here power the ingestion pipeline and are available for refreshes.
"""
from __future__ import annotations

import os
import re
import time

import requests

MASSIVE_BASE = "https://api.massive.com"


def _massive_key() -> str:
    key = os.environ.get("MASSIVE_API_KEY")
    if not key:
        raise RuntimeError("MASSIVE_API_KEY not set")
    return key


def _edgar_headers() -> dict:
    email = os.environ.get("EDGAR_CONTACT_EMAIL", "capstone@example.com")
    return {"User-Agent": f"stock-research-capstone {email}"}


# --------------------------------------------------------------------------
# Massive — daily OHLCV
# --------------------------------------------------------------------------
def fetch_daily_bars(ticker: str, from_date: str, to_date: str, max_retries: int = 6) -> list[dict]:
    """Daily OHLCV bars for a ticker over [from_date, to_date] (YYYY-MM-DD).

    Returns a list of bar dicts with keys t (epoch ms), o, h, l, c, v. Exponential
    backoff on 429 starting ~25s. Raises on auth/entitlement errors.
    """
    url = f"{MASSIVE_BASE}/v2/aggs/ticker/{ticker}/range/1/day/{from_date}/{to_date}"
    params = {"apiKey": _massive_key(), "adjusted": "true", "sort": "asc", "limit": 50000}
    backoff = 25
    for attempt in range(1, max_retries + 1):
        r = requests.get(url, params=params, timeout=60)
        if r.status_code == 200:
            return r.json().get("results") or []
        if r.status_code == 429:
            time.sleep(backoff)
            backoff *= 2
            continue
        if r.status_code in (401, 403):
            raise RuntimeError(f"{ticker}: auth/entitlement error {r.status_code}: {r.text[:200]}")
        time.sleep(5)
    raise RuntimeError(f"{ticker}: exhausted {max_retries} retries")


# --------------------------------------------------------------------------
# SEC EDGAR — resolve CIK, find latest 10-K, extract Item 1A
# --------------------------------------------------------------------------
# Reorg edge cases where the ticker map points at an entity lacking 10-K history.
CIK_OVERRIDE = {
    "XOM": ("0000034088", "Exxon Mobil Corp"),  # ticker maps to a new holdco w/o 10-Ks
}


def ticker_cik_map() -> dict[str, tuple[str, str]]:
    """SEC ticker -> (zero-padded 10-digit CIK, company name)."""
    data = requests.get("https://www.sec.gov/files/company_tickers.json",
                        headers=_edgar_headers(), timeout=60).json()
    return {v["ticker"].upper(): (str(v["cik_str"]).zfill(10), v["title"]) for v in data.values()}


def resolve_cik(ticker: str, cik_map: dict | None = None) -> tuple[str, str] | None:
    if ticker.upper() in CIK_OVERRIDE:
        return CIK_OVERRIDE[ticker.upper()]
    cik_map = cik_map if cik_map is not None else ticker_cik_map()
    return cik_map.get(ticker.upper())


def _newest_10k(cols: dict):
    best = None
    for form, acc, doc, fdate, rdate in zip(
        cols["form"], cols["accessionNumber"], cols["primaryDocument"],
        cols["filingDate"], cols["reportDate"],
    ):
        if form == "10-K" and (best is None or (fdate or "") > (best["filing_date"] or "")):
            best = {"accession": acc, "primary": doc,
                    "filing_date": fdate or None, "period_of_report": rdate or None}
    return best


def latest_10k(cik: str) -> dict | None:
    """Most recent 10-K metadata; falls back to overflow submission files for very
    active filers whose 10-K scrolls out of the 1000-row `recent` window."""
    hdr = _edgar_headers()
    r = requests.get(f"https://data.sec.gov/submissions/CIK{cik}.json", headers=hdr, timeout=60)
    r.raise_for_status()
    data = r.json()
    best = _newest_10k(data["filings"]["recent"])
    if best is None:
        for f in data["filings"].get("files", []):
            rr = requests.get(f"https://data.sec.gov/submissions/{f['name']}", headers=hdr, timeout=60)
            time.sleep(0.2)
            if rr.status_code != 200:
                continue
            cand = _newest_10k(rr.json())
            if cand and (best is None or (cand["filing_date"] or "") > (best["filing_date"] or "")):
                best = cand
    if best is None:
        return None
    accnd = best["accession"].replace("-", "")
    best["doc_url"] = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accnd}/{best['primary']}"
    return best


def fetch_filing_html(url: str) -> str:
    return requests.get(url, headers=_edgar_headers(), timeout=90).text


# --- Item 1A extraction (robust to inconsistent / inline-XBRL HTML) ---------
def _loose(s: str) -> str:
    """Char-by-char pattern tolerant of stray whitespace inside words (inline-XBRL
    span breaks produce 'RIS K FACTORS', 'Item 1 A')."""
    return r"\s*".join(re.escape(c) for c in s if not c.isspace())


_START1 = re.compile(_loose("item1a") + r".{0,20}?" + _loose("riskfactors"), re.I)
_RF = re.compile(_loose("riskfactors"), re.I)
_END_STRICT = [re.compile(_loose(x), re.I) for x in ("item1b", "item1c", "item2", "item3", "item4")]
_END_HEADING = _END_STRICT + [re.compile(_loose(x), re.I) for x in (
    "unresolvedstaffcomments", "legalproceedings", "minesafetydisclosures",
    "quantitativeandqualitativedisclosuresaboutmarketrisk")]
_BAD_PRE = re.compile(r'[“"”\')]|\bsee\b|\band\b|\bthe\b|\bour\b|,\s*$', re.I)
_XREF = re.compile(r'^\s*(in this form|on pages?|for (a|further|more)|section (of|in|,)|'
                   r'below|above|and elsewhere|herein|discussed|described|set forth|contained)', re.I)


def html_to_text(html: str) -> str:
    import warnings
    from bs4 import BeautifulSoup
    try:
        from bs4 import XMLParsedAsHTMLWarning
        warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
    except Exception:  # noqa: BLE001
        pass
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style"]):
        tag.decompose()
    return re.sub(r"\s+", " ", soup.get_text(" ")).strip()


def _nearest_end(text: str, s: int, ends) -> int:
    e = len(text)
    for ep in ends:
        m = ep.search(text, s + 80)
        if m:
            e = min(e, m.start())
    return e


def extract_item_1a(text: str) -> str:
    """Two-tier Item 1A extractor. Tier 1 anchors on 'Item 1A ... Risk Factors'
    bounded by strict item markers; tier 2 falls back to bare 'Risk Factors'
    headings for filings that omit an inline Item 1A. Over-extraction is preferred
    to missing the section (callers log length to flag outliers)."""
    best = ""
    for m in _START1.finditer(text):
        if _XREF.match(text[m.end():m.end() + 40]):
            continue
        c = text[m.start():_nearest_end(text, m.start(), _END_STRICT)].strip()
        if len(c) > len(best):
            best = c
    if len(best) >= 3000:
        return best
    best2 = ""
    for m in _RF.finditer(text):
        s = m.start()
        if _BAD_PRE.search(text[max(0, s - 25):s]) or _XREF.match(text[m.end():m.end() + 40]):
            continue
        c = text[s:_nearest_end(text, s, _END_HEADING)].strip()
        if len(c) > len(best2):
            best2 = c
    return best2[:300000] if len(best2) > len(best) else best


def chunk_text(text: str, chunk_tokens: int = 500, overlap_tokens: int = 75) -> list[str]:
    """Word-based chunking approximating token targets (~0.75 words/token)."""
    words = text.split()
    cw = max(1, int(chunk_tokens * 0.75))
    ow = max(0, int(overlap_tokens * 0.75))
    step = max(1, cw - ow)
    out = []
    for i in range(0, len(words), step):
        piece = words[i:i + cw]
        if not piece:
            break
        out.append(" ".join(piece))
        if i + cw >= len(words):
            break
    return out
