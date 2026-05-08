"""
trading/scanner.py â Main scan loop.

On each run:
  1. Fetch all active temperature markets from Polymarket
  2. For each parsed market:
       a. Resolve city â coordinates + MOS station
       b. Fetch ensemble forecast (Open-Meteo)
       c. Fetch MOS bias correction (Iowa Mesonet) â skipped for international cities
       d. Fetch climatological prior (Open-Meteo archive)
       e. Blend into a probability
       f. Compare with market price â Kelly sizing
       g. Open paper position if edge is sufficient and no existing position,
          subject to per-city-per-scan and all-time same-trade caps
  3. Check existing open positions for resolution (price near 0 or 1)
  4. Print summary table
"""

import logging
from datetime import date, datetime, timezone

import config
import cities as city_db
from data.weather import fetch_ensemble_members
from data.mos import fetch_mos_prediction
from data.historical import fetch_climatology
from data.polymarket import fetch_temperature_markets, _extract_yes_price
from models.ensemble import blend_all
from models.probability import compute_probability, distribution_summary
from models.kelly import kelly_bet
from trading import paper_trader as pt

logger = logging.getLogger(__name__)

# A market is considered resolved if YES price is >= this (YES won) or <= (1 - this) (NO won)
RESOLUTION_THRESHOLD = 0.95


def run_scan() -> None:
    """
    Main scan: discover markets â model â decide â trade.
    Safe to call repeatedly; catches per-market exceptions.
    """
    run_start = datetime.now(tz=timezone.utc)
    logger.info(f"{'='*70}")
    logger.info(f"  SCAN STARTED at {run_start.isoformat()}")
    logger.info(f"{'='*70}")

    # --- 1. Fetch markets ---
    try:
        markets = fetch_temperature_markets()
    except Exception as exc:
        logger.error(f"Fatal error fetching markets: {exc}", exc_info=True)
        return

    logger.info(f"Scan: {len(markets)} parseable temperature markets found")

    trades_opened = 0
    trades_skipped = 0
    errors = 0

    # Track how many trades have been opened per city this scan
    city_trades_this_scan: dict[str, int] = {}

    for mkt in markets:
        try:
            result = _evaluate_market(mkt, city_trades_this_scan)
            if result == "opened":
                trades_opened += 1
            elif result == "skipped":
                trades_skipped += 1
        except Exception as exc:
            errors += 1
            logger.error(
                f"Error evaluating market '{mkt.get('question','?')[:80]}': {exc}",
                exc_info=True,
            )

    # --- 2. Check resolutions ---
    resolved = check_resolutions()

    # --- 3. Summary ---
    logger.info(
        f"Scan complete | opened={trades_opened} skipped={trades_skipped} "
        f"errors={errors} resolved={resolved}"
    )
    pt.print_summary_table()


def _evaluate_market(mkt: dict, city_trades_this_scan: dict) -> str:
    """
    Evaluate a single market and optionally open a paper position.

    city_trades_this_scan: mutable dict {city_name: count} tracking how many
    trades have been opened for each city in this scan. Modified in-place when
    a trade is opened.

    Returns: 'opened', 'skipped', or 'passed'
    """
    market_id    = mkt["market_id"]
    question     = mkt["question"]
    city_raw     = mkt["city_raw"]
    kind         = mkt["kind"]
    threshold_f  = mkt["threshold_f"]
    band_type    = mkt.get("band_type", "above")
    threshold_lo = mkt.get("threshold_lo")
    threshold_hi = mkt.get("threshold_hi")
    target_date  = mkt["target_date"]
    yes_price    = mkt["yes_price"]

    logger.info(f"--- Market: '{question[:100]}'")
    logger.debug(
        f"  id={market_id} city_raw='{city_raw}' kind={kind} "
        f"threshold={threshold_f}Â°F target={target_date} yes_price={yes_price:.4f}"
    )

    # --- City lookup ---
    city_info = city_db.lookup_city(city_raw)
    if city_info is None:
        logger.warning(f"  SKIP: city '{city_raw}' not in database")
        return "skipped"

    city_name = city_info["display_name"]
    lat, lon = city_info["lat"], city_info["lon"]
    mos_station = city_info.get("mos_station")  # None for international cities
    lead_days = (target_date - date.today()).days

    logger.info(
        f"  City={city_name} ({lat},{lon}) station={mos_station or 'N/A'} lead={lead_days}d "
        f"band={band_type} lo={threshold_lo} hi={threshold_hi} threshold={threshold_f}Â°F"
    )

    if lead_days < 0:
        logger.info("  SKIP: target date already passed")
        return "skipped"

    # --- Skip if already have a position ---
    if pt.position_exists(market_id):
        logger.info(f"  SKIP: already have open position for market {market_id[:30]}")
        return "skipped"

    # --- Per-city scan limit (max 2 trades per city per scan) ---
    city_count_this_scan = city_trades_this_scan.get(city_name, 0)
    max_per_city = config.MAX_TRADES_PER_CITY_PER_SCAN
    if city_count_this_scan >= max_per_city:
        logger.info(
            f"  SKIP: already {city_count_this_scan} trades for {city_name} "
            f"this scan (max {max_per_city})"
        )
        return "skipped"

    # --- All-time same-trade cap (max 5 identical city+date+threshold combos) ---
    same_trade_count = pt.count_same_trades(city_name, target_date.isoformat(), threshold_f)
    max_same = config.MAX_SAME_TRADE_ALL_TIME
    if same_trade_count >= max_same:
        logger.info(
            f"  SKIP: {same_trade_count} existing positions for "
            f"{city_name} {target_date} {threshold_f}Â°F (max {max_same})"
        )
        return "skipped"

    # --- Ensemble forecast ---
    logger.info(f"  Fetching ensemble forecast...")
    try:
        ensemble_members = fetch_ensemble_members(lat, lon, target_date, kind=kind)
    except Exception as exc:
        logger.error(f"  Ensemble fetch failed: {exc}", exc_info=True)
        ensemble_members = []

    logger.info(f"  Ensemble: {len(ensemble_members)} members")

    # --- MOS bias correction (US cities only; skip for international) ---
    mos_pred = None
    mos_correction = None
    if mos_station is None:
        logger.info(f"  MOS: no station configured for {city_name} (international city) â skipping")
    else:
        logger.info(f"  Fetching MOS for station={mos_station}...")
        try:
            mos_data = fetch_mos_prediction(mos_station, target_date)
            if mos_data:
                mos_pred = mos_data.get("high") if kind == "high" else mos_data.get("low")
                if mos_pred is not None:
                    ens_mean = sum(ensemble_members) / len(ensemble_members) if ensemble_members else None
                    mos_correction = mos_pred - ens_mean if ens_mean is not None else None
                    logger.info(
                        f"  MOS {kind}={mos_pred:.1f}Â°F | "
                        f"ens_mean={ens_mean:.1f}Â°F | correction={mos_correction:+.1f}Â°F"
                        if ens_mean is not None else f"  MOS {kind}={mos_pred:.1f}Â°F (no ens mean)"
                    )
                else:
                    logger.info(f"  MOS data available but no '{kind}' field: {mos_data}")
        except Exception as exc:
            logger.warning(f"  MOS fetch failed (continuing without): {exc}", exc_info=True)

    # --- Climatological prior ---
    logger.info(f"  Fetching climatology for ({lat},{lon})...")
    try:
        clim_samples = fetch_climatology(lat, lon, target_date, kind=kind)
    except Exception as exc:
        logger.error(f"  Climatology fetch failed: {exc}", exc_info=True)
        clim_samples = []

    logger.info(f"  Climatology: {len(clim_samples)} historical samples")

    if not ensemble_members and not clim_samples:
        logger.error("  SKIP: no data at all (no ensemble, no climatology)")
        return "skipped"

    # --- Blend ---
    blended = blend_all(ensemble_members, mos_pred, clim_samples, target_date, kind)
    dist = distribution_summary(blended)
    logger.info(f"  Blended distribution: {dist}")

    # Compute blended mean (our predicted temperature)
    blended_mean = sum(blended) / len(blended) if blended else None
    if blended_mean is not None:
        offset = blended_mean - threshold_f
        logger.info(
            f"  Prediction: {blended_mean:.1f}Â°F vs threshold {threshold_f}Â°F "
            f"(offset={offset:+.1f}Â°F)"
        )

    # --- Probability ---
    our_prob = compute_probability(
        blended, threshold_f, kind,
        band_type=band_type,
        threshold_lo=threshold_lo,
        threshold_hi=threshold_hi,
    )
    market_prob = yes_price  # YES price IS the market's implied probability

    logger.info(
        f"  our_prob={our_prob:.4f} market_prob={market_prob:.4f} "
        f"edge={our_prob - market_prob:+.4f}"
    )

    # --- Kelly sizing ---
    bankroll = pt.get_bankroll()
    kelly = kelly_bet(our_prob, market_prob, bankroll)
    logger.info(f"  Kelly â {kelly['reason']}")

    # --- Log model run regardless of trade ---
    pt.log_model_run(
        market_id=market_id,
        lead_days=lead_days,
        our_prob=our_prob,
        market_prob=market_prob,
        edge=kelly["edge"],
        kelly_fraction=kelly["kelly_frac"],
        dollar_amount=kelly["dollar_amount"],
        action_taken=kelly["action"],
        n_ensemble=len(ensemble_members),
        n_clim=len(clim_samples),
        mos_correction=mos_correction,
        notes=kelly["reason"],
        city=city_name,
        kind=kind,
        threshold_f=threshold_f,
        target_date=target_date.isoformat(),
        blended_mean=blended_mean,
    )

    # --- Open position ---
    if kelly["action"] == "pass":
        logger.info("  â PASS (no trade)")
        return "passed"

    direction = kelly["action"]  # 'yes' or 'no'
    entry_price = market_prob if direction == "yes" else (1.0 - market_prob)

    pos_id = pt.open_position(
        market_id=market_id,
        question=question,
        city=city_name,
        kind=kind,
        direction=direction,
        target_date=target_date,
        threshold_f=threshold_f,
        entry_price=entry_price,
        dollar_amount=kelly["dollar_amount"],
    )

    # Update per-city scan counter
    city_trades_this_scan[city_name] = city_count_this_scan + 1

    logger.info(
        f"  â TRADE OPENED id={pos_id} | {city_name} {kind.upper()} "
        f"{direction.upper()} ${kelly['dollar_amount']:.2f} @ {entry_price:.4f} "
        f"| target={target_date} threshold={threshold_f}Â°F "
        f"| city_trades_this_scan={city_trades_this_scan[city_name]}/{max_per_city} "
        f"same_trade_total={same_trade_count+1}/{max_same}"
    )
    return "opened"


def check_resolutions() -> int:
    """
    For all open positions, fetch current YES price from Polymarket.
    If price is near 0 or 1 (>= RESOLUTION_THRESHOLD), close the position.

    Returns: number of positions closed.
    """
    logger.info("check_resolutions: checking open positions...")
    open_positions = pt.get_open_positions()
    if not open_positions:
        logger.info("  No open positions to check.")
        return 0

    import requests
    from requests.adapters import HTTPAdapter, Retry

    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))

    closed_count = 0
    GAMMA_BASE = "https://gamma-api.polymarket.com"

    for pos in open_positions:
        market_id = pos["market_id"]
        logger.debug(f"  Checking resolution for market_id={market_id[:40]}")

        # Fetch current market state
        url = f"{GAMMA_BASE}/markets/{market_id}"
        try:
            resp = session.get(url, timeout=10)
            logger.debug(f"  Resolution check | status={resp.status_code} url={resp.url}")
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.warning(f"  Could not fetch market {market_id[:30]}: {exc}")
            continue

        # Also try fetching by conditionId as a query param
        yes_price = _extract_yes_price(data if isinstance(data, dict) else {})

        if yes_price is None:
            logger.debug(f"  Could not extract price for {market_id[:30]}, skipping")
            continue

        logger.debug(f"  Current YES price: {yes_price:.4f}")

        exit_price = None
        if yes_price >= RESOLUTION_THRESHOLD:
            # YES resolved (market HIGH exceeded threshold, or LOW dropped below)
            exit_price = 1.0 if pos["direction"] == "yes" else 0.0
            logger.info(
                f"  Market {market_id[:30]} â YES resolved. "
                f"Our direction={pos['direction']} â exit_price={exit_price}"
            )
        elif yes_price <= (1.0 - RESOLUTION_THRESHOLD):
            # NO resolved
            exit_price = 0.0 if pos["direction"] == "yes" else 1.0
            logger.info(
                f"  Market {market_id[:30]} â NO resolved. "
                f"Our direction={pos['direction']} â exit_price={exit_price}"
            )

        if exit_price is not None:
            closed = pt.close_position(market_id, exit_price)
            closed_count += len(closed)

    logger.info(f"check_resolutions done | {closed_count} positions closed")
    return closed_count
