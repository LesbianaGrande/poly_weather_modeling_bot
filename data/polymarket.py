"""
data/polymarket.py — Fetches and parses temperature markets from Polymarket's Gamma API.

ROOT CAUSE FIX (v2): The /markets search param doesn't actually filter — it returns
top markets by liquidity regardless of query. Instead we use the /events endpoint and
filter client-side by slug pattern.

Polymarket organises temperature markets as EVENTS, each containing sub-markets for
individual thresholds. Event slugs follow the pattern:
  highest-temperature-in-{city}-on-{month}-{day}-{year}
  lowest-temperature-in-{city}-on-{month}-{day}-{year}

Example event URL:
  https://polymarket.com/event/highest-temperature-in-houston-on-march-24-2026

We:
  1. Paginate through /events, filtering for slugs containing "temperature"
  2. Parse city + date from the event slug (reliable)
  3. Extract each sub-market's question for the threshold
  4. Extract YES price from outcomePrices
"""

import json
import logging
import re
from datetime import date, datetime
from typing import Optional

import requests
from requests.adapters import HTTPAdapter, Retry

logger = logging.getLogger(__name__)

GAMMA_BASE = "https://gamma-api.polymarket.com"

# Slug fragments that identify temperature events
TEMP_SLUG_KEYWORDS = ("highest-temperature", "lowest-temperature", "high-temperature", "low-temperature")

# Month name → number for slug parsing
MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=2.0, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session


# ── Public entry point ────────────────────────────────────────────────────────

def fetch_temperature_markets() -> list[dict]:
    """
    Fetch all active temperature markets from Polymarket via the /events endpoint.

    Returns a list of parsed market dicts with keys:
      market_id, question, city_raw, kind, threshold_f, target_date, yes_price, no_price
    """
    session = _make_session()
    events = _fetch_temperature_events(session)
    logger.info(f"Polymarket: {len(events)} temperature events found")

    parsed: list[dict] = []
    seen_market_ids: set[str] = set()

    for event in events:
        slug = event.get("slug", "")
        title = event.get("title", event.get("name", ""))
        logger.debug(f"Processing event slug='{slug}' title='{title[:80]}'")

        # Parse city + date + kind from slug (preferred — more reliable than question text)
        slug_info = _parse_event_slug(slug)
        if slug_info is None:
            logger.debug(f"  Could not parse slug '{slug}', trying title...")
            slug_info = _parse_event_slug(_title_to_slug(title))
        if slug_info is None:
            logger.debug(f"  Skipping event — could not extract city/date from slug or title")
            continue

        kind = slug_info["kind"]
        city_raw = slug_info["city_raw"]
        target_date = slug_info["target_date"]

        logger.info(f"  EVENT slug='{slug}' → kind={kind} city='{city_raw}' date={target_date}")

        if target_date < date.today():
            logger.info(f"  SKIP past event: slug='{slug}' target_date={target_date}")
            continue

        # Extract sub-markets from the event
        sub_markets = event.get("markets", [])
        logger.debug(f"  Event has {len(sub_markets)} sub-markets")

        if not sub_markets:
            logger.debug(f"  No sub-markets in event, skipping")
            continue

        for mkt in sub_markets:
            market_id = mkt.get("conditionId") or mkt.get("id") or ""
            if not market_id or market_id in seen_market_ids:
                continue
            seen_market_ids.add(market_id)

            question = mkt.get("question") or mkt.get("title") or ""
            raw_prices = mkt.get("outcomePrices") or mkt.get("outcome_prices") or mkt.get("prices")
            logger.info(f"  SUB-MARKET id={market_id[:24]} question='{question[:120]}' raw_prices={raw_prices!r}")

            # Parse band type + bounds from question
            band = _parse_market_band(question)
            if band is None:
                logger.debug(f"    No temperature threshold found in question, skipping")
                continue
            threshold_f  = band["threshold_f"]
            band_type    = band["band_type"]
            threshold_lo = band["threshold_lo"]
            threshold_hi = band["threshold_hi"]

            # Extract YES price
            yes_price = _extract_yes_price(mkt)
            if yes_price is None:
                logger.debug(f"    Could not extract YES price, skipping")
                continue

            result = {
                "market_id": market_id,
                "question": question,
                "city_raw": city_raw,
                "kind": kind,
                "threshold_f": threshold_f,
                "band_type": band_type,
                "threshold_lo": threshold_lo,
                "threshold_hi": threshold_hi,
                "target_date": target_date,
                "yes_price": yes_price,
                "no_price": round(1.0 - yes_price, 4),
            }
            logger.info(
                f"  ACCEPTED {city_raw} {kind} [{band_type}] "
                f"lo={threshold_lo} hi={threshold_hi} mid={threshold_f}°F "
                f"target={target_date} yes={yes_price:.4f} q='{question[:80]}'")
            parsed.append(result)

    logger.info(f"Polymarket: {len(parsed)} temperature sub-markets successfully parsed")
    return parsed


# ── Events fetching ───────────────────────────────────────────────────────────

def _fetch_temperature_events(session: requests.Session) -> list[dict]:
    """
    Paginate through the /events endpoint and return all events whose slug
    contains a temperature keyword.
    """
    events: list[dict] = []
    seen_ids: set[str] = set()

    # tag_slug=weather is the only reliable filter — the search param is ignored
    # by the API and returns all events regardless of query.
    _fetch_events_paginated(session, {"tag_slug": "weather"}, events, seen_ids)

    # Filter client-side to temperature events only
    temp_events = [
        e for e in events
        if any(kw in (e.get("slug") or "").lower() for kw in TEMP_SLUG_KEYWORDS)
        or any(kw.replace("-", " ") in (e.get("title") or e.get("name") or "").lower()
               for kw in TEMP_SLUG_KEYWORDS)
    ]

    logger.info(
        f"_fetch_temperature_events: {len(events)} total events fetched, "
        f"{len(temp_events)} matched temperature keywords"
    )
    return temp_events


def _fetch_events_paginated(
    session: requests.Session,
    extra_params: dict,
    out_events: list,
    seen_ids: set,
    page_size: int = 100,
) -> None:
    """Paginate through /events with given params, appending unique events to out_events."""
    offset = 0
    url = f"{GAMMA_BASE}/events"

    while True:
        params = {
            "active": "true",
            "closed": "false",
            "limit": page_size,
            "offset": offset,
            **extra_params,
        }
        logger.debug(f"Events API | URL={url} params={params}")

        try:
            resp = session.get(url, params=params, timeout=15)
            logger.debug(f"  status={resp.status_code} url={resp.url}")
            resp.raise_for_status()
        except requests.RequestException as exc:
            logger.warning(f"  Events API request failed: {exc}")
            break

        data = resp.json()
        logger.debug(f"  Response type={type(data).__name__} first 400 chars: {str(data)[:400]!r}")

        if isinstance(data, list):
            batch = data
        elif isinstance(data, dict):
            batch = data.get("events", data.get("data", data.get("results", [])))
        else:
            logger.warning(f"  Unexpected response type: {type(data)}")
            break

        if not batch:
            logger.debug(f"  Empty batch at offset={offset}, stopping pagination")
            break

        new_count = 0
        for event in batch:
            eid = event.get("id") or event.get("conditionId") or event.get("slug") or ""
            if eid and eid not in seen_ids:
                seen_ids.add(eid)
                out_events.append(event)
                new_count += 1

        logger.debug(
            f"  offset={offset}: batch_size={len(batch)} new_unique={new_count} total={len(out_events)}"
        )

        if len(batch) < page_size:
            break  # Last page
        offset += page_size


# ── Slug parsing ──────────────────────────────────────────────────────────────

def _parse_event_slug(slug: str) -> Optional[dict]:
    """
    Parse city, kind, and date from an event slug like:
      highest-temperature-in-houston-on-march-24-2026
      lowest-temperature-in-los-angeles-on-april-5-2026

    Returns dict with: kind ('high'/'low'), city_raw (str), target_date (date)
    or None if slug doesn't match.
    """
    slug = slug.lower().strip()
    logger.debug(f"_parse_event_slug: '{slug}'")

    # Determine kind
    if slug.startswith("highest") or "highest-temperature" in slug:
        kind = "high"
    elif slug.startswith("lowest") or "lowest-temperature" in slug:
        kind = "low"
    elif "high-temperature" in slug:
        kind = "high"
    elif "low-temperature" in slug:
        kind = "low"
    else:
        return None

    # Pattern: {kind}-temperature-in-{city-words}-on-{month}-{day}-{year}
    # City words are hyphen-separated, month is a word, day and year are numbers
    m = re.search(
        r"temperature-in-(.+?)-on-([a-z]+)-(\d{1,2})-(\d{4})$",
        slug,
    )
    if not m:
        logger.debug(f"  Slug regex didn't match")
        return None

    city_slug = m.group(1)       # e.g. "los-angeles" or "new-york"
    month_str = m.group(2)       # e.g. "march"
    day = int(m.group(3))
    year = int(m.group(4))

    city_raw = city_slug.replace("-", " ").title()  # "Los Angeles", "New York"

    month_num = MONTH_MAP.get(month_str)
    if not month_num:
        logger.debug(f"  Unknown month '{month_str}' in slug")
        return None

    try:
        target_date = date(year, month_num, day)
    except ValueError as e:
        logger.debug(f"  Invalid date in slug: {e}")
        return None

    logger.debug(f"  Parsed → kind={kind} city='{city_raw}' date={target_date}")
    return {"kind": kind, "city_raw": city_raw, "target_date": target_date}


def _title_to_slug(title: str) -> str:
    """Convert an event title to a slug-like string for parsing."""
    return re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")


# ── Threshold / band extraction from question text ───────────────────────────

def _parse_market_band(question: str) -> Optional[dict]:
    """
    Parse a temperature market question and return a band descriptor dict:
        {
          "band_type":    "above" | "below" | "between",
          "threshold_lo": float | None,   # lower bound (°F)
          "threshold_hi": float | None,   # upper bound (°F)
          "threshold_f":  float,          # midpoint (°F) — for display
        }

    Returns None if no temperature figure can be extracted.

    Handles °F bands ("80-81°F"), single °F ("above 75°F"), °C bands,
    single °C, and bare-number with direction keyword.
    """
    unit_f = r"(?:°\s*[Ff]|degrees?\s*(?:fahrenheit|[Ff])\b)"
    unit_c = r"(?:°\s*[Cc]|degrees?\s*celsius)"
    num    = r"\d+(?:\.\d+)?"
    dash   = r"\s*[-–]\s*"

    def _valid(v: float) -> bool:
        return -60 <= v <= 140

    def _c2f(c: float) -> float:
        return c * 9 / 5 + 32

    q = question  # shorthand

    # ── Band °F: "80-81°F" or "between 80 and 81°F" ──────────────────────────
    m = re.search(rf"({num}){dash}({num})\s*{unit_f}", q, re.IGNORECASE)
    if m:
        lo, hi = float(m.group(1)), float(m.group(2))
        mid = (lo + hi) / 2
        if _valid(mid):
            logger.debug(f"  Band °F between {lo}-{hi}: mid={mid}")
            return {"band_type": "between", "threshold_lo": lo,
                    "threshold_hi": hi, "threshold_f": mid}

    # ── Band °C: "24-25°C" ────────────────────────────────────────────────────
    m = re.search(rf"({num}){dash}({num})\s*{unit_c}", q, re.IGNORECASE)
    if m:
        lo, hi = _c2f(float(m.group(1))), _c2f(float(m.group(2)))
        mid = (lo + hi) / 2
        if _valid(mid):
            logger.debug(f"  Band °C→°F between {lo:.1f}-{hi:.1f}: mid={mid:.1f}")
            return {"band_type": "between", "threshold_lo": lo,
                    "threshold_hi": hi, "threshold_f": mid}

    # ── Single °F with direction ──────────────────────────────────────────────
    # "above/over/higher than/exceed X°F"  → above
    # "below/under/not exceed X°F" / "X°F or below/lower/under" → below
    # "X°F or above/higher/more" / "at least X°F" → above
    m = re.search(rf"(?<!\d)(-?{num})\s*{unit_f}", q, re.IGNORECASE)
    if m:
        val = float(m.group(1))
        if _valid(val):
            # Determine direction from surrounding words
            q_lower = q.lower()
            if re.search(r"\bor\s+(?:below|lower|under)\b|\b(?:below|under|not\s+exceed)\b",
                         q_lower):
                logger.debug(f"  Single °F below {val}")
                return {"band_type": "below", "threshold_lo": None,
                        "threshold_hi": val, "threshold_f": val}
            elif re.search(r"\bor\s+(?:above|higher|more|over)\b|\b(?:above|over|exceed|higher\s+than|at\s+least)\b",
                           q_lower):
                logger.debug(f"  Single °F above {val}")
                return {"band_type": "above", "threshold_lo": val,
                        "threshold_hi": None, "threshold_f": val}
            else:
                # Default: treat bare single value as "above" (most common Polymarket phrasing)
                logger.debug(f"  Single °F (no direction keyword) → defaulting to above {val}")
                return {"band_type": "above", "threshold_lo": val,
                        "threshold_hi": None, "threshold_f": val}

    # ── Single °C with direction ──────────────────────────────────────────────
    m = re.search(rf"(?<!\d)(-?{num})\s*{unit_c}", q, re.IGNORECASE)
    if m:
        val = _c2f(float(m.group(1)))
        if _valid(val):
            q_lower = q.lower()
            if re.search(r"\bor\s+(?:below|lower|under)\b|\b(?:below|under)\b", q_lower):
                return {"band_type": "below", "threshold_lo": None,
                        "threshold_hi": val, "threshold_f": val}
            else:
                return {"band_type": "above", "threshold_lo": val,
                        "threshold_hi": None, "threshold_f": val}

    # ── Bare number with direction keyword ────────────────────────────────────
    m_above = re.search(rf"(?:above|exceed|over|higher\s+than)\s+({num})", q, re.IGNORECASE)
    if m_above:
        val = float(m_above.group(1))
        if _valid(val):
            return {"band_type": "above", "threshold_lo": val,
                    "threshold_hi": None, "threshold_f": val}

    m_below = re.search(rf"(?:below|under|not\s+exceed)\s+({num})", q, re.IGNORECASE)
    if m_below:
        val = float(m_below.group(1))
        if _valid(val):
            return {"band_type": "below", "threshold_lo": None,
                    "threshold_hi": val, "threshold_f": val}

    logger.debug(f"  _parse_market_band: no temperature found in: {q[:100]!r}")
    return None


def _extract_threshold(question: str) -> Optional[float]:
    """Backward-compat shim — returns midpoint only. Use _parse_market_band for full info."""
    band = _parse_market_band(question)
    return band["threshold_f"] if band else None


# ── YES price extraction ──────────────────────────────────────────────────────

def _extract_yes_price(market: dict) -> Optional[float]:
    """
    Extract the YES outcome price (0–1) from a market dict.
    Polymarket returns outcomePrices as a JSON-encoded list of strings, e.g. '["0.72","0.28"]'.
    """
    # outcomePrices: most common field, often a JSON-string list
    for field in ("outcomePrices", "outcome_prices", "prices"):
        val = market.get(field)
        if val is None:
            continue
        if isinstance(val, str):
            try:
                val = json.loads(val)
            except Exception:
                pass
        if isinstance(val, list) and len(val) >= 1:
            try:
                price = float(val[0])
                # Could be 0–1 or 0–100
                if price > 1.0:
                    price /= 100.0
                if 0.0 <= price <= 1.0:
                    logger.debug(f"  YES price from '{field}': {price:.4f}")
                    return price
            except (ValueError, TypeError):
                pass

    # tokens / outcomes array
    for arr_field in ("tokens", "outcomes"):
        tokens = market.get(arr_field) or []
        if not isinstance(tokens, list):
            continue
        for token in tokens:
            if not isinstance(token, dict):
                continue
            outcome_name = (token.get("outcome") or token.get("name") or "").lower()
            if "yes" in outcome_name or outcome_name == "y":
                for pf in ("price", "lastPrice", "last_price", "midPrice"):
                    p = token.get(pf)
                    if p is not None:
                        try:
                            price = float(p)
                            price = price / 100.0 if price > 1.0 else price
                            if 0.0 <= price <= 1.0:
                                logger.debug(f"  YES price from {arr_field}[].{pf}: {price:.4f}")
                                return price
                        except (ValueError, TypeError):
                            pass

    # Single scalar price field
    for field in ("price", "lastPrice", "last_price", "bestBid", "best_bid", "midPrice"):
        val = market.get(field)
        if val is not None:
            try:
                price = float(val)
                price = price / 100.0 if price > 1.0 else price
                if 0.0 <= price <= 1.0:
                    logger.debug(f"  YES price from '{field}': {price:.4f}")
                    return price
            except (ValueError, TypeError):
                pass

    logger.debug(f"  YES price: no valid field found. Market keys={list(market.keys())}")
    return None
