"""
models/probability.py — Compute temperature threshold probabilities from a sample distribution.

Given a list of temperature samples (the blended ensemble+clim output) and a threshold,
computes the fraction of samples that exceed (or fall below) the threshold.

Also computes distribution summary stats for logging/debugging.
"""

import logging
import math
from typing import Literal

logger = logging.getLogger(__name__)


def compute_probability(
    samples: list[float],
    threshold_f: float,
    kind: Literal["high", "low"],
    band_type: str = "above",
    threshold_lo: float | None = None,
    threshold_hi: float | None = None,
) -> float:
    """
    Compute P(YES) for a Polymarket temperature band market.

    band_type controls the semantics:
      "above"   — YES if daily stat > threshold_lo   (e.g. "72°F or higher")
      "below"   — YES if daily stat <= threshold_hi  (e.g. "33°F or below")
      "between" — YES if threshold_lo <= stat <= threshold_hi  (e.g. "54-55°F")

    The 'kind' parameter (high/low) tells us which daily stat the samples represent.
    Samples are already the correct stat (daily max for high markets, daily min for low).
    """
    if not samples:
        logger.warning("compute_probability | no samples, returning 0.5")
        return 0.5

    n = len(samples)

    lo = threshold_lo if threshold_lo is not None else threshold_f
    hi = threshold_hi if threshold_hi is not None else threshold_f

    if band_type == "between":
        count = sum(1 for s in samples if lo <= s <= hi)
        desc = f"between {lo}-{hi}°F"
    elif band_type == "below":
        count = sum(1 for s in samples if s <= hi)
        desc = f"<= {hi}°F"
    else:  # "above" — also the legacy default
        count = sum(1 for s in samples if s > lo)
        desc = f"> {lo}°F"

    probability = count / n

    sorted_s = sorted(samples)
    mean = sum(samples) / n
    variance = sum((s - mean) ** 2 for s in samples) / n
    std = math.sqrt(variance)

    def percentile(p: float) -> float:
        idx = p / 100 * (n - 1)
        i_lo, i_hi = int(idx), min(int(idx) + 1, n - 1)
        frac = idx - i_lo
        return sorted_s[i_lo] * (1 - frac) + sorted_s[i_hi] * frac

    logger.info(
        f"compute_probability | kind={kind} band={band_type} [{desc}] "
        f"n={n} count={count} P(YES)={probability:.4f}"
    )
    logger.info(
        f"  Distribution | mean={mean:.1f}°F std={std:.1f}°F "
        f"p10={percentile(10):.1f} p25={percentile(25):.1f} p50={percentile(50):.1f} "
        f"p75={percentile(75):.1f} p90={percentile(90):.1f}"
    )

    return max(0.001, min(0.999, probability))


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
