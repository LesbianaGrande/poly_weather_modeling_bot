"""
data/weather.py — Fetches multi-model ensemble temperature forecasts from Open-Meteo.

Ensemble API docs: https://open-meteo.com/en/docs/ensemble-api
All temps are requested in °F (temperature_unit=fahrenheit).

For a given location + target date, returns a list of daily high (or low) temps,
one per ensemble member across all requested models.
"""

import logging
from datetime import date, timedelta
from typing import Literal

import requests
from requests.adapters import HTTPAdapter, Retry

logger = logging.getLogger(__name__)

# Models available for free on Open-Meteo ensemble API
ENSEMBLE_MODELS = ["gfs_seamless", "icon_seamless", "gem_global"]

ENSEMBLE_URL = "https://ensemble-api.open-meteo.com/v1/ensemble"


def _make_session() -> requests.Session:
    """Create a requests.Session with retry logic."""
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_ensemble_members(
    lat: float,
    lon: float,
    target_date: date,
    kind: Literal["high", "low"] = "high",
    models: list[str] = ENSEMBLE_MODELS,
    timezone: str = "America/New_York",
) -> list[float]:
    """
    Fetch ensemble temperature forecasts and return a list of daily max (kind='high')
    or daily min (kind='low') temperatures in °F, one per ensemble member.

    Args:
        lat, lon:      Location coordinates
        target_date:   The date we want the high/low for
        kind:          'high' → daily max, 'low' → daily min
        models:        List of Open-Meteo ensemble model names

    Returns:
        List of temps (floats, °F). May be empty if all models fail.
    """
    session = _make_session()
    all_members: list[float] = []

    # We request a range ending a couple days past target to be safe
    today = date.today()
    forecast_days = (target_date - today).days + 3
    forecast_days = max(1, min(forecast_days, 16))  # API cap is 16 days

    logger.debug(
        f"fetch_ensemble_members | lat={lat} lon={lon} date={target_date} "
        f"kind={kind} forecast_days={forecast_days} models={models}"
    )

    for model in models:
        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": "temperature_2m",
            "models": model,
            "temperature_unit": "fahrenheit",
            "wind_speed_unit": "mph",
            "forecast_days": forecast_days,
            "timezone": timezone,  # local calendar day slicing
        }
        logger.debug(f"  Open-Meteo request | model={model} URL={ENSEMBLE_URL} params={params}")

        try:
            resp = session.get(ENSEMBLE_URL, params=params, timeout=20)
            logger.debug(f"  Response status={resp.status_code} url={resp.url}")
            resp.raise_for_status()
        except requests.RequestException as exc:
            logger.warning(f"  Open-Meteo request failed for model={model}: {exc}")
            continue

        data = resp.json()
        logger.debug(f"  Raw response keys: {list(data.keys())}")

        hourly = data.get("hourly", {})
        times = hourly.get("time", [])
        logger.debug(f"  Hourly time steps returned: {len(times)}")

        # Member columns look like: temperature_2m_member01, temperature_2m_member02, ...
        member_keys = [k for k in hourly.keys() if k.startswith("temperature_2m_member")]
        logger.debug(f"  model={model} member columns found: {len(member_keys)} → {member_keys[:5]}...")

        if not member_keys:
            # Some models return just 'temperature_2m' (single deterministic run)
            if "temperature_2m" in hourly:
                member_keys = ["temperature_2m"]
                logger.debug(f"  Falling back to single deterministic column for model={model}")
            else:
                logger.warning(f"  No temperature columns found for model={model}, skipping")
                continue

        # Filter hours for target_date. Timestamps are in the requested timezone,
        # so a simple date-prefix match gives the correct local calendar day.
        target_str = target_date.isoformat()
        target_indices = [i for i, t in enumerate(times) if t.startswith(target_str)]
        logger.info(
            f"  model={model} target={target_str} (tz={timezone}): {len(target_indices)} hours matched"
        )

        if not target_indices:
            logger.warning(
                f"  model={model}: no hours found for date {target_str}. "
                f"Available dates: {sorted(set(t[:10] for t in times))}"
            )
            continue

        model_member_temps: list[float] = []
        for col in member_keys:
            hourly_vals = hourly[col]
            day_vals = [hourly_vals[i] for i in target_indices if hourly_vals[i] is not None]
            if not day_vals:
                logger.debug(f"    col={col} had no valid values for target date, skipping")
                continue
            if kind == "high":
                member_temp = max(day_vals)
            else:
                member_temp = min(day_vals)
            model_member_temps.append(member_temp)

        logger.info(
            f"  model={model}: {len(model_member_temps)} members | "
            f"mean={sum(model_member_temps)/len(model_member_temps):.1f}°F "
            f"min={min(model_member_temps):.1f}°F max={max(model_member_temps):.1f}°F"
            if model_member_temps else f"  model={model}: 0 members"
        )
        all_members.extend(model_member_temps)

    logger.info(
        f"fetch_ensemble_members DONE | total members={len(all_members)} "
        f"overall_mean={sum(all_members)/len(all_members):.1f}°F"
        if all_members else "fetch_ensemble_members DONE | total members=0"
    )
    return all_members
