"""
data/historical.py — Fetches historical daily temperature data from Open-Meteo Archive API.

Used to build the climatological prior: "what does the temperature distribution look like
for this location on roughly this calendar date, based on the past N years?"

Archive API docs: https://open-meteo.com/en/docs/historical-weather-api
All temps are requested in °F.
"""

import logging
from datetime import date, timedelta
from typing import Literal

import requests
from requests.adapters import HTTPAdapter, Retry

logger = logging.getLogger(__name__)

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
YEARS_OF_HISTORY = 10        # How many years back to fetch
CALENDAR_WINDOW_DAYS = 20    # ±20 days around target calendar date


def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session


def fetch_climatology(
    lat: float,
    lon: float,
    target_date: date,
    kind: Literal["high", "low"] = "high",
    years: int = YEARS_OF_HISTORY,
    window_days: int = CALENDAR_WINDOW_DAYS,
) -> list[float]:
    """
    Return a list of historical daily high (or low) temperatures in °F for the
    same ±window_days calendar window across the past `years` years.

    Example: target_date=2026-05-10, window=20 → pulls all historical daily highs
    between Apr 20 and May 30, for 2016–2025.

    Args:
        lat, lon:       Location coordinates
        target_date:    Reference date (used for calendar window only)
        kind:           'high' → temperature_2m_max, 'low' → temperature_2m_min
        years:          Number of years of history to fetch
        window_days:    Half-width of calendar window in days

    Returns:
        List of float temps (°F). May be empty on API failure.
    """
    # Date range: go back `years` years from yesterday
    end_date = date.today() - timedelta(days=1)
    start_date = end_date.replace(year=end_date.year - years)

    variable = "temperature_2m_max" if kind == "high" else "temperature_2m_min"

    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "daily": variable,
        "temperature_unit": "fahrenheit",
        "timezone": "UTC",
    }

    logger.debug(
        f"fetch_climatology | lat={lat} lon={lon} kind={kind} "
        f"target_date={target_date} range={start_date}→{end_date} "
        f"window=±{window_days}d years={years}"
    )
    logger.debug(f"  Archive URL={ARCHIVE_URL} params={params}")

    session = _make_session()
    try:
        resp = session.get(ARCHIVE_URL, params=params, timeout=30)
        logger.debug(f"  Archive response status={resp.status_code} url={resp.url}")
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.error(f"  Archive API request failed: {exc}")
        return []

    data = resp.json()
    logger.debug(f"  Archive response keys: {list(data.keys())}")

    daily = data.get("daily", {})
    times = daily.get("time", [])
    values = daily.get(variable, [])

    logger.debug(f"  Archive returned {len(times)} daily records")

    if not times or not values:
        logger.warning("  Archive API returned empty daily data")
        return []

    # Filter to calendar window: keep only records where the month-day falls
    # within ±window_days of the target date's month-day
    target_mmdd = (target_date.month, target_date.day)
    samples: list[float] = []

    for time_str, val in zip(times, values):
        if val is None:
            continue
        try:
            record_date = date.fromisoformat(time_str)
        except ValueError:
            continue

        # Compute day-of-year distance (handles year boundary wrap-around)
        record_doy = record_date.timetuple().tm_yday
        target_doy = target_date.timetuple().tm_yday
        diff = abs(record_doy - target_doy)
        diff = min(diff, 365 - diff)  # wrap around year boundary

        if diff <= window_days:
            samples.append(float(val))

    logger.info(
        f"fetch_climatology DONE | kind={kind} calendar_window=±{window_days}d "
        f"samples_kept={len(samples)} out of {len(times)} records"
    )
    if samples:
        mean = sum(samples) / len(samples)
        logger.debug(
            f"  Clim stats | mean={mean:.1f}°F "
            f"min={min(samples):.1f}°F max={max(samples):.1f}°F"
        )

    return samples
