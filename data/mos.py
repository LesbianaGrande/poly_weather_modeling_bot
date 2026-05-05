"""
data/mos.py — Fetches Model Output Statistics (MOS) from Iowa Environmental Mesonet.

MOS are statistically post-processed NWS model forecasts that correct for known
model biases. We use them as a bias-correction signal on top of raw ensemble output.

Iowa Mesonet MOS API: https://mesonet.agron.iastate.edu/mos/
CSV endpoint: https://mesonet.agron.iastate.edu/mos/csv.php
"""

import logging
from datetime import date, datetime, timezone

import requests
from requests.adapters import HTTPAdapter, Retry

logger = logging.getLogger(__name__)

MOS_URL = "https://mesonet.agron.iastate.edu/mos/csv.php"

# MOS models to try in order of preference
MOS_MODELS = ["gfs", "nam", "mex"]


def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session


def _latest_runtime() -> str:
    """
    Return the most recent MOS runtime string in YYYYMMDDhh format.
    MOS runs are published at 00Z and 12Z; we pick the most recent.
    """
    now = datetime.now(tz=timezone.utc)
    if now.hour >= 12:
        runtime_dt = now.replace(hour=12, minute=0, second=0, microsecond=0)
    else:
        runtime_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
    runtime_str = runtime_dt.strftime("%Y%m%d%H")
    logger.debug(f"MOS latest runtime string: {runtime_str}")
    return runtime_str


def fetch_mos_prediction(
    station: str,
    target_date: date,
    models: list[str] = MOS_MODELS,
) -> dict | None:
    """
    Fetch MOS forecast for a station and return the predicted high and low
    temperatures (°F) for the target_date.

    Returns dict with keys 'high' and/or 'low' (floats, °F), or None if unavailable.
    The Iowa Mesonet MOS CSV has columns including 'n_x_t' (max temp) and 'n_i_t' (min temp).

    NOTE: Column names in Iowa Mesonet MOS CSV differ by model. Common temp columns:
      mx   — daily max temp (°F)
      mn   — daily min temp (°F)
      tmp  — 3-hourly temp (°F)
    We try multiple column name variants.
    """
    session = _make_session()
    runtime = _latest_runtime()

    for model in models:
        params = {
            "station": station.upper(),
            "runtime": runtime,
            "mos": model,
        }
        logger.debug(f"MOS request | station={station} model={model} runtime={runtime} URL={MOS_URL} params={params}")

        try:
            resp = session.get(MOS_URL, params=params, timeout=15)
            logger.debug(f"  MOS response status={resp.status_code} url={resp.url}")
            resp.raise_for_status()
        except requests.RequestException as exc:
            logger.warning(f"  MOS request failed for station={station} model={model}: {exc}")
            continue

        text = resp.text
        logger.info(f"  MOS raw response ({len(text)} chars):\n{text[:1500]}")

        if not text.strip() or "No data" in text or len(text.strip().splitlines()) < 2:
            logger.warning(f"  MOS: empty or no-data response for station={station} model={model}")
            continue

        result = _parse_mos_csv(text, target_date, station, model)
        if result:
            logger.info(
                f"MOS forecast | station={station} model={model} date={target_date} → {result}"
            )
            return result
        else:
            logger.debug(f"  MOS parse found nothing for target_date={target_date} model={model}")

    logger.warning(
        f"MOS: no prediction available for station={station} date={target_date} "
        f"(tried models: {models})"
    )
    return None


def _parse_mos_csv(text: str, target_date: date, station: str, model: str) -> dict | None:
    """
    Parse Iowa Mesonet MOS CSV and extract high/low temps for target_date.

    The CSV has a header row + data rows, one row per forecast valid time.
    Date/time columns vary; we look for a 'ftime' or 'valid' column.
    Temp columns: 'mx' (max), 'mn' (min), or 'tmp'.
    """
    lines = [l for l in text.strip().splitlines() if l.strip()]
    if len(lines) < 2:
        return None

    header = [h.strip().lower() for h in lines[0].split(",")]
    logger.debug(f"  MOS CSV headers: {header}")

    # Find date column
    date_col = None
    for candidate in ["ftime", "valid", "utcvalid", "date"]:
        if candidate in header:
            date_col = header.index(candidate)
            break
    if date_col is None:
        logger.debug(f"  MOS CSV: no date column found in {header}")
        return None

    # Find temp columns
    max_col = next((header.index(c) for c in ["mx", "maxt", "tmax", "mx_t"] if c in header), None)
    min_col = next((header.index(c) for c in ["mn", "mint", "tmin", "mn_t"] if c in header), None)
    tmp_col = header.index("tmp") if "tmp" in header else None

    logger.debug(f"  MOS CSV col indices | date={date_col} max={max_col} min={min_col} tmp={tmp_col}")

    target_str = target_date.isoformat()  # "2026-05-10"
    highs, lows = [], []

    for line in lines[1:]:
        cols = [c.strip() for c in line.split(",")]
        if len(cols) <= date_col:
            continue
        row_date_raw = cols[date_col]
        # Date may be "2026-05-10 12:00" or "2026051012" etc.
        row_date_iso = row_date_raw[:10].replace("/", "-")  # normalise
        if row_date_iso != target_str:
            logger.debug(f"  skip row date={row_date_iso!r} (want {target_str!r})")
            continue
        logger.info(f"  MOS MATCH row: {dict(zip(header, cols))}")

        # Extract max temp
        if max_col is not None and max_col < len(cols):
            try:
                val = float(cols[max_col])
                highs.append(val)
            except ValueError:
                pass

        # Extract min temp
        if min_col is not None and min_col < len(cols):
            try:
                val = float(cols[min_col])
                lows.append(val)
            except ValueError:
                pass

        # Fallback: 3-hourly temps to derive high/low
        if tmp_col is not None and tmp_col < len(cols):
            try:
                val = float(cols[tmp_col])
                highs.append(val)
                lows.append(val)
            except ValueError:
                pass

    result = {}
    if highs:
        result["high"] = max(highs)
    if lows:
        result["low"] = min(lows)

    return result if result else None
