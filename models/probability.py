"""
models/probability.py — Compute temperature threshold probabilities from a sample distribution.

Given a list of temperature samples (the blended ensemble+clim output) and a threshold,
computes the fraction of samples that exceed (or fall below) the threshold.

Also computes distribution summary stats for logging/debugging.
"""

import logging
import math
from typing import Literal, Optional

logger = logging.getLogger(__name__)


def compute_probability(
    samples: list[float],
    threshold_f: float,
    kind: Literal["high", "low"],
    band_type: str = "above",
    threshold_lo: Optional[float] = None,
    threshold_hi: Optional[float] = None,
) -> float:
    """
    Compute P(YES) for a temperature market.

    band_type controls the market structure:
      "above"   — YES resolves if temp >= threshold_lo  (e.g. "85°F or higher")
      "below"   — YES resolves if temp <= threshold_hi  (e.g. "53°F or below")
      "between" — YES resolves if threshold_lo <= temp <= threshold_hi  (e.g. "80-81°F band")

    For 'high' markets, samples are daily-max temperatures.
    For 'low'  markets, samples are daily-min temperatures.

    Returns:
        Probability as a float in [0, 1].
        Returns 0.5 (maximum uncertainty) if samples is empty.
    """
    if not samples:
        logger.warning(
            "compute_probability | no samples provided, returning 0.5 (max uncertainty)"
        )
        return 0.5

    n = len(samples)

    lo = threshold_lo if threshold_lo is not None else threshold_f
    hi = threshold_hi if threshold_hi is not None else threshold_f

    if band_type == "between":
        # P(lo <= temp <= hi) — narrow band market
        count = sum(1 for s in samples if lo <= s <= hi)
    elif band_type == "below":
        # P(temp <= hi) — "X or below" market
        count = sum(1 for s in samples if s <= hi)
    else:
        # "above" (default) — "X or higher" market: YES if temp >= threshold
        count = sum(1 for s in samples if s >= lo)

    probability = count / n

    # --- Distribution summary ---
    sorted_s = sorted(samples)
    mean = sum(samples) / n
    variance = sum((s - mean) ** 2 for s in samples) / n
    std = math.sqrt(variance)

    def percentile(p: float) -> float:
        idx = p / 100 * (n - 1)
        lo, hi = int(idx), min(int(idx) + 1, n - 1)
        frac = idx - lo
        return sorted_s[lo] * (1 - frac) + sorted_s[hi] * frac

    p10 = percentile(10)
    p25 = percentile(25)
    p50 = percentile(50)
    p75 = percentile(75)
    p90 = percentile(90)

    logger.info(
        f"compute_probability | kind={kind} band={band_type} "
        f"lo={lo}°F hi={hi}°F "
        f"n_samples={n} count={count} probability={probability:.4f}"
    )
    logger.debug(
        f"  Distribution | mean={mean:.1f}°F std={std:.1f}°F "
        f"p10={p10:.1f} p25={p25:.1f} p50={p50:.1f} p75={p75:.1f} p90={p90:.1f}"
    )

    # Sanity clamp
    probability = max(0.001, min(0.999, probability))
    return probability


def distribution_summary(samples: list[float]) -> dict:
    """Return a dict of summary statistics for a sample list (for logging/debugging)."""
    if not samples:
        return {"n": 0}
    n = len(samples)
    mean = sum(samples) / n
    variance = sum((s - mean) ** 2 for s in samples) / n
    std = math.sqrt(variance)
    sorted_s = sorted(samples)

    def pct(p):
        idx = p / 100 * (n - 1)
        lo = int(idx)
        hi = min(lo + 1, n - 1)
        return sorted_s[lo] * (1 - (idx - lo)) + sorted_s[hi] * (idx - lo)

    return {
        "n": n,
        "mean": round(mean, 2),
        "std": round(std, 2),
        "min": round(sorted_s[0], 2),
        "p10": round(pct(10), 2),
        "p25": round(pct(25), 2),
        "p50": round(pct(50), 2),
        "p75": round(pct(75), 2),
        "p90": round(pct(90), 2),
        "max": round(sorted_s[-1], 2),
    }
