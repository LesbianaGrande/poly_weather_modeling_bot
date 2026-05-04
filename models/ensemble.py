"""
models/ensemble.py — Ensemble blending, MOS bias correction, and climatological prior mixing.

Pipeline:
  1. Raw ensemble members (°F) from Open-Meteo (GFS, ICON, GEM)
  2. MOS bias correction: shift ensemble by (MOS_predicted - ensemble_mean)
  3. Blend corrected ensemble with climatological samples, weighted by lead time:
       >7 days  → 60% clim / 40% ensemble
       3–7 days → 25% clim / 75% ensemble
       1–3 days → 10% clim / 90% ensemble
       <1 day   →  5% clim / 95% ensemble

The output is a single list of temperature samples from which probabilities are computed.
"""

import logging
import math
from datetime import date
from typing import Literal, Optional

logger = logging.getLogger(__name__)


def lead_time_weights(target_date: date) -> tuple[float, float]:
    """
    Return (clim_weight, ensemble_weight) based on days until target_date.
    Weights sum to 1.0.
    """
    lead_days = (target_date - date.today()).days
    logger.debug(f"lead_time_weights | target={target_date} lead_days={lead_days}")

    if lead_days > 7:
        w_clim, w_ens = 0.60, 0.40
    elif lead_days >= 3:
        w_clim, w_ens = 0.25, 0.75
    elif lead_days >= 1:
        w_clim, w_ens = 0.10, 0.90
    else:
        w_clim, w_ens = 0.05, 0.95

    logger.debug(f"  lead_days={lead_days} → clim_weight={w_clim} ensemble_weight={w_ens}")
    return w_clim, w_ens


def apply_mos_correction(
    ensemble_members: list[float],
    mos_prediction: Optional[float],
    kind: Literal["high", "low"],
) -> list[float]:
    """
    Apply MOS bias correction by shifting all ensemble members by the difference
    between the MOS prediction and the raw ensemble mean.

    If MOS is unavailable, returns ensemble unchanged.

    Args:
        ensemble_members:  Raw ensemble temp samples (°F)
        mos_prediction:    MOS predicted high or low (°F), or None
        kind:              'high' or 'low' (just for logging)

    Returns:
        Bias-corrected list of floats (same length as input).
    """
    if not ensemble_members:
        logger.debug("apply_mos_correction | empty ensemble, returning []")
        return []

    ensemble_mean = sum(ensemble_members) / len(ensemble_members)
    logger.debug(
        f"apply_mos_correction | kind={kind} n_members={len(ensemble_members)} "
        f"ensemble_mean={ensemble_mean:.2f}°F mos_prediction={mos_prediction}"
    )

    if mos_prediction is None:
        logger.info(
            "  MOS prediction not available — using raw ensemble without bias correction"
        )
        return ensemble_members

    correction = mos_prediction - ensemble_mean
    logger.info(
        f"  MOS correction | ensemble_mean={ensemble_mean:.2f}°F "
        f"mos={mos_prediction:.2f}°F correction={correction:+.2f}°F"
    )

    corrected = [t + correction for t in ensemble_members]
    new_mean = sum(corrected) / len(corrected)
    logger.debug(
        f"  After correction | new_mean={new_mean:.2f}°F "
        f"min={min(corrected):.2f}°F max={max(corrected):.2f}°F"
    )
    return corrected


def blend_samples(
    ensemble_members: list[float],
    clim_samples: list[float],
    target_date: date,
) -> list[float]:
    """
    Blend ensemble and climatological samples using lead-time-based weights.

    Rather than mixing the raw lists directly (which would be sensitive to list sizes),
    we resample each component to a normalised target size proportional to its weight,
    then concatenate.

    TARGET_N_SAMPLES controls how many blended samples we work with (~1000 is fine).

    Args:
        ensemble_members:  Bias-corrected ensemble temps (°F)
        clim_samples:      Historical climatology temps (°F)
        target_date:       Used to determine lead time weights

    Returns:
        Combined list of temperature samples (°F).
    """
    TARGET_N = 1000
    w_clim, w_ens = lead_time_weights(target_date)

    n_ens = len(ensemble_members)
    n_clim = len(clim_samples)
    logger.debug(
        f"blend_samples | n_ensemble={n_ens} n_clim={n_clim} "
        f"w_ens={w_ens} w_clim={w_clim} TARGET_N={TARGET_N}"
    )

    blended: list[float] = []

    # --- Ensemble contribution ---
    if n_ens > 0 and w_ens > 0:
        n_take_ens = max(1, round(TARGET_N * w_ens))
        ens_resampled = _resample(ensemble_members, n_take_ens)
        blended.extend(ens_resampled)
        logger.debug(f"  Ensemble resampled: {len(ens_resampled)} samples")
    else:
        logger.warning("  No ensemble members available — using climatology only")
        w_clim = 1.0

    # --- Climatology contribution ---
    if n_clim > 0 and w_clim > 0:
        n_take_clim = max(1, round(TARGET_N * w_clim))
        clim_resampled = _resample(clim_samples, n_take_clim)
        blended.extend(clim_resampled)
        logger.debug(f"  Climatology resampled: {len(clim_resampled)} samples")
    elif w_clim > 0:
        logger.warning("  No climatology samples available — blend is ensemble-only")

    blended_mean = sum(blended) / len(blended) if blended else float("nan")
    logger.info(
        f"blend_samples DONE | total_samples={len(blended)} "
        f"blended_mean={blended_mean:.2f}°F "
        f"min={min(blended):.2f}°F max={max(blended):.2f}°F"
        if blended else "blend_samples DONE | no samples!"
    )
    return blended


def _resample(samples: list[float], n: int) -> list[float]:
    """
    Resample a list to exactly n samples using modulo indexing (no randomness needed —
    we want a deterministic, reproducible blend).
    """
    if not samples:
        return []
    return [samples[i % len(samples)] for i in range(n)]


def blend_all(
    ensemble_members: list[float],
    mos_prediction: Optional[float],
    clim_samples: list[float],
    target_date: date,
    kind: Literal["high", "low"],
) -> list[float]:
    """
    Full pipeline: MOS correction → lead-time blend → return samples.

    This is the single entry point used by scanner.py.
    """
    logger.info(
        f"=== blend_all | kind={kind} target={target_date} "
        f"n_ensemble={len(ensemble_members)} n_clim={len(clim_samples)} "
        f"mos={mos_prediction} ==="
    )

    corrected = apply_mos_correction(ensemble_members, mos_prediction, kind)
    blended = blend_samples(corrected, clim_samples, target_date)
    return blended
