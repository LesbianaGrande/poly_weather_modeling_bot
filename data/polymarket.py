"""
data/polymarket.py — Fetches and parses temperature markets from Polymarket's Gamma API.

Gamma API base: https://gamma-api.polymarket.com
We search for active weather/temperature markets and parse the question text to extract:
  - City name
  - 'high' or 'low'
  - Temperature threshold (°F)
  - Target date
  - YES implied probability (from outcome prices)

Market prices on Polymarket are 0–1 representing the probability.
"""

import logging
import re
from datetime import date, datetime
from typing import Optional

import requests
from requests.adapters import HTTPAdapter, Retry

logger = logging.getLogger(__name__)

GAMMA_BASE = "https://gamma-api.polymarket.com"


def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=2.0, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session


def fetch_temperature_markets() -> list[dict]:
    """
    Fetch all active temperature markets from Polymarket.

    Tries multiple search strategies and deduplicates by market conditionId.
    Returns a list of parsed market dicts (see _parse_market for schema).
    """
    session = _make_session()
    raw_markets: list[dict] = []
    seen_ids: set[str] = set()

    search_strategies = [
        {"search": "temperature high"},
        {"search": "temperature low"},
        {"search": "high temperature"},
        {"search": "low temperature"},
        {"tag_slug": "weather"},
        {"search": "daily high"},
        {"search": "daily low"},
        {"search": "°F"},
    ]

    for strategy in search_strategies:
        params = {
            "active": "true",
            "closed": "false",
            "limit": 100,
            **strategy,
        }
        url = f"{GAMMA_BASE}/markets"
        logger.debug(f"Polymarket search | URL={url} params={params}")

        try:
            resp = session.get(url, params=params, timeout=15)
            logger.debug(f"  Response status={resp.status_code} url={resp.url}")
            resp.raise_for_status()
        except requests.RequestException as exc:
            logger.warning(f"  Polymarket request failed for strategy={strategy}: {exc}")
            continue

        data = resp.json()

        # Handle both list responses and paginated dict responses
        if isinstance(data, list):
            markets_batch = data
        elif isinstance(data, dict):
            markets_batch = data.get("markets", data.get("data", data.get("results", [])))
            logger.debug(f"  Response is dict with keys: {list(data.keys())}")
        else:
            logger.warning(f"  Unexpected response type: {type(data)}")
            continue

        logger.debug(
            f"  strategy={strategy} → {len(markets_batch)} markets returned. "
            f"First 500 chars of raw: {str(data)[:500]!r}"
        )

        for m in markets_batch:
            mid = m.get("conditionId") or m.get("id") or m.get("marketMakerAddress", "")
            if mid and mid not in seen_ids:
                seen_ids.add(mid)
                raw_markets.append(m)

    logger.info(
        f"Polymarket: {len(raw_markets)} unique raw markets found across all search strategies"
    )

    parsed: list[dict] = []
    for m in raw_markets:
        result = _parse_market(m)
        if result:
            parsed.append(result)
        else:
            q = m.get("question", m.get("title", ""))
            logger.debug(f"  Could not parse market: '{q[:120]}'")

    logger.info(f"Polymarket: {len(parsed)} markets successfully parsed as temperature markets")
    return parsed


def _parse_market(market: dict) -> Optional[dict]:
    """
    Parse a raw Polymarket market dict into a structured temperature market.

    Returns dict with keys:
      market_id, question, city_raw, kind ('high'/'low'),
      threshold_f (°F float), target_date (date),
      yes_price (0–1 float), no_price (0–1 float)

    Returns None if this isn't a parseable temperature market.
    """
    question = market.get("question") or market.get("title") or market.get("name") or ""
    market_id = (
        market.get("conditionId")
        or market.get("id")
        or market.get("marketMakerAddress")
        or ""
    )

    logger.debug(f"Parsing market | id={market_id!r} question={question[:120]!r}")

    if not question:
        return None

    # ── Temperature threshold ──────────────────────────────────────────────────
    # Match patterns like "75°F", "75 °F", "75F", "-5°F", "100 degrees"
    threshold_match = re.search(
        r"(-?\d+(?:\.\d+)?)\s*(?:°F|°f|°\s*F|degrees?\s+(?:fahrenheit|F)|F\b)",
        question,
        re.IGNORECASE,
    )
    if not threshold_match:
        # Try Celsius: "25°C" — convert to °F
        c_match = re.search(r"(-?\d+(?:\.\d+)?)\s*(?:°C|°c|degrees?\s+celsius)", question, re.IGNORECASE)
        if c_match:
            threshold_f = float(c_match.group(1)) * 9 / 5 + 32
            logger.debug(f"  Found Celsius threshold {c_match.group(1)}°C → {threshold_f:.1f}°F")
        else:
            return None
    else:
        threshold_f = float(threshold_match.group(1))

    # Sanity check: plausible temperature range (-60 to 140°F)
    if not (-60 <= threshold_f <= 140):
        logger.debug(f"  Threshold {threshold_f}°F out of plausible range, skipping")
        return None

    # ── High or Low ───────────────────────────────────────────────────────────
    q_lower = question.lower()
    if any(w in q_lower for w in ["high", "maximum", "max temp", "hottest"]):
        kind = "high"
    elif any(w in q_lower for w in ["low", "minimum", "min temp", "coldest"]):
        kind = "low"
    else:
        return None

    # ── City ──────────────────────────────────────────────────────────────────
    city_raw = _extract_city(question)
    if not city_raw:
        return None

    # ── Target date ───────────────────────────────────────────────────────────
    target_date = _extract_date(question, market)
    if not target_date:
        return None

    # Skip markets that have already resolved (target date in the past)
    if target_date < date.today():
        logger.debug(f"  Market target_date={target_date} is in the past, skipping")
        return None

    # ── YES price → implied probability ───────────────────────────────────────
    yes_price = _extract_yes_price(market)
    if yes_price is None:
        logger.debug(f"  Could not extract YES price for market_id={market_id}")
        return None

    no_price = round(1.0 - yes_price, 4)

    parsed = {
        "market_id": market_id,
        "question": question,
        "city_raw": city_raw,
        "kind": kind,
        "threshold_f": threshold_f,
        "target_date": target_date,
        "yes_price": yes_price,
        "no_price": no_price,
    }
    logger.debug(f"  Parsed OK: {parsed}")
    return parsed


def _extract_city(question: str) -> Optional[str]:
    """
    Extract a city name from the market question text.

    Patterns: "in [City]", "for [City]", "[City]'s", "at [City]"
    We return the raw string; cities.py handles the lookup.
    """
    patterns = [
        r"(?:in|for|at)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})",  # "in New York City"
        r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})'s\s+(?:daily\s+)?(?:high|low)",  # "Chicago's high"
        r"(?:high|low)\s+(?:temperature\s+)?(?:in|for|at)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})",
    ]
    for pat in patterns:
        m = re.search(pat, question)
        if m:
            city = m.group(1).strip()
            logger.debug(f"  City extracted: '{city}' via pattern '{pat[:40]}'")
            return city
    logger.debug(f"  No city found in: '{question[:100]}'")
    return None


def _extract_date(question: str, market: dict) -> Optional[date]:
    """
    Extract the target date from market question or market metadata.
    Tries several formats.
    """
    # Try structured metadata fields first
    for field in ["endDate", "end_date", "expirationDate", "expiration_date", "resolutionDate"]:
        raw = market.get(field)
        if raw:
            try:
                dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                logger.debug(f"  Date from field '{field}': {dt.date()}")
                return dt.date()
            except (ValueError, AttributeError):
                pass

    current_year = date.today().year

    # Try text patterns in the question
    # "May 10, 2026" or "May 10 2026"
    m = re.search(
        r"(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
        r"Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
        r"\s+(\d{1,2})(?:st|nd|rd|th)?,?\s*(\d{4})?",
        question,
        re.IGNORECASE,
    )
    if m:
        month_str, day_str, year_str = m.group(1), m.group(2), m.group(3)
        year = int(year_str) if year_str else current_year
        try:
            dt = datetime.strptime(f"{month_str[:3].capitalize()} {day_str} {year}", "%b %d %Y")
            logger.debug(f"  Date from question text (long month): {dt.date()}")
            return dt.date()
        except ValueError:
            pass

    # "2026-05-10" or "05/10/2026"
    for pattern, fmt in [
        (r"\b(\d{4}-\d{2}-\d{2})\b", "%Y-%m-%d"),
        (r"\b(\d{2}/\d{2}/\d{4})\b", "%m/%d/%Y"),
    ]:
        m2 = re.search(pattern, question)
        if m2:
            try:
                dt = datetime.strptime(m2.group(1), fmt)
                logger.debug(f"  Date from question numeric pattern: {dt.date()}")
                return dt.date()
            except ValueError:
                pass

    logger.debug(f"  Could not extract date from question='{question[:100]}'")
    return None


def _extract_yes_price(market: dict) -> Optional[float]:
    """
    Extract the YES outcome price (0–1) from a market dict.
    Tries multiple field name variants used by Polymarket's API.
    """
    # Direct price fields
    for field in ["outcomePrices", "outcome_prices", "prices"]:
        val = market.get(field)
        if val:
            # Could be a list ["0.72", "0.28"] or a JSON string
            if isinstance(val, str):
                try:
                    import json
                    val = json.loads(val)
                except Exception:
                    pass
            if isinstance(val, list) and len(val) >= 1:
                try:
                    price = float(val[0])
                    if 0.0 <= price <= 1.0:
                        logger.debug(f"  YES price from '{field}[0]': {price}")
                        return price
                    # Might be in cents (0–100)
                    if 0.0 <= price <= 100.0:
                        logger.debug(f"  YES price from '{field}[0]' (cents→frac): {price/100:.4f}")
                        return price / 100.0
                except (ValueError, TypeError):
                    pass

    # Tokens list
    tokens = market.get("tokens") or market.get("outcomes") or []
    if isinstance(tokens, list):
        for token in tokens:
            if isinstance(token, dict):
                outcome = (token.get("outcome") or token.get("name") or "").lower()
                if "yes" in outcome or outcome == "y":
                    for pf in ["price", "lastPrice", "last_price", "midPrice"]:
                        p = token.get(pf)
                        if p is not None:
                            try:
                                price = float(p)
                                price = price / 100.0 if price > 1.0 else price
                                logger.debug(f"  YES price from tokens[].{pf}: {price}")
                                return price
                            except (ValueError, TypeError):
                                pass

    # Single price field (binary market)
    for field in ["price", "lastPrice", "last_price", "bestBid", "best_bid", "midPrice"]:
        val = market.get(field)
        if val is not None:
            try:
                price = float(val)
                price = price / 100.0 if price > 1.0 else price
                if 0.0 <= price <= 1.0:
                    logger.debug(f"  YES price from '{field}': {price}")
                    return price
            except (ValueError, TypeError):
                pass

    logger.debug(f"  YES price: tried all fields, none worked. Market keys: {list(market.keys())}")
    return None
