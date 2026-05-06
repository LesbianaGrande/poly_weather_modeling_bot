"""
models/kelly.py — Kelly criterion position sizing.

Given our model probability p, the market's implied probability q (= YES price),
and a fractional Kelly multiplier, compute the optimal bet size.

Formula (binary market):
  b = (1 / q) - 1       # net odds for YES: gain per $1 risked
  f* = (p*b - (1-p)) / b  # full Kelly fraction of bankroll

We use fractional Kelly (typically 0.25×) to reduce variance.

Edge check: if |p - q| < MIN_EDGE, we skip the trade (not worth the noise).
"""

import logging
from typing import Literal

import config

logger = logging.getLogger(__name__)


def kelly_bet(
    our_prob: float,
    market_prob: float,
    bankroll: float,
    kind: Literal["yes", "no"] = "yes",
) -> dict:
    """
    Compute Kelly-optimal bet size.

    Args:
        our_prob:     Our model's probability that YES resolves (0–1)
        market_prob:  Market's implied YES probability (= current YES price, 0–1)
        bankroll:     Current paper bankroll ($)
        kind:         'yes' if we're buying YES, 'no' if buying NO

    Returns dict with keys:
        action:         'yes', 'no', or 'pass'
        our_prob:       (echo)
        market_prob:    (echo)
        edge:           our_prob - market_prob (signed)
        kelly_full:     Full Kelly fraction (before multiplier)
        kelly_frac:     Fractional Kelly fraction
        dollar_amount:  Dollar amount to bet (capped at MAX_POSITION_FRACTION * bankroll)
        reason:         Human-readable explanation
    """
    edge = our_prob - market_prob
    logger.debug(
        f"kelly_bet | our_prob={our_prob:.4f} market_prob={market_prob:.4f} "
        f"edge={edge:+.4f} bankroll=${bankroll:.2f}"
    )

    result_base = {
        "our_prob": our_prob,
        "market_prob": market_prob,
        "edge": edge,
        "kelly_full": 0.0,
        "kelly_frac": 0.0,
        "dollar_amount": 0.0,
    }

    # ── Relative edge (important for narrow-band markets) ────────────────────────────────────────────────────────────────────
    # For 1-2°F bands, YES price can be 0.5-3% while our prob is 3-8%.
    # Absolute edge stays small even with strong conviction, so also check
    # whether our probability is >= REL_EDGE_MIN better than the market.
    REL_EDGE_MIN = 0.25  # we must be at least 25% relatively better than the market

    rel_edge_yes = (our_prob / market_prob - 1.0) if market_prob > 0 else 0.0
    rel_edge_no  = ((1.0 - our_prob) / (1.0 - market_prob) - 1.0) if market_prob < 1.0 else 0.0

    def _qualifies(abs_e: float, rel_e: float) -> bool:
        return abs_e >= config.MIN_EDGE or rel_e >= REL_EDGE_MIN

    # ── Determine trade direction ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
    if edge > 0 and _qualifies(edge, rel_edge_yes):
        # Our probability is higher than market → buy YES
        action = "yes"
        p = our_prob          # P(YES resolves)
        q = market_prob       # cost per YES share
    elif edge < 0 and _qualifies(-edge, rel_edge_no):
        # Our probability is lower than market → buy NO
        action = "no"
        p = 1.0 - our_prob    # P(NO resolves)
        q = 1.0 - market_prob # cost per NO share
    else:
        logger.info(
            f"kelly_bet PASS | abs_edge={edge:+.4f} "
            f"rel_yes={rel_edge_yes:+.2f} rel_no={rel_edge_no:+.2f} "
            f"MIN_EDGE={config.MIN_EDGE} REL_MIN={REL_EDGE_MIN}"
        )
        return {
            **result_base,
            "action": "pass",
            "reason": (
                f"Edge abs={edge:+.4f} rel_yes={rel_edge_yes:+.2f} rel_no={rel_edge_no:+.2f} "
                f"below thresholds (abs>={config.MIN_EDGE} or rel>={REL_EDGE_MIN})"
            ),
        }

    # ── Kelly formula ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
    # b = net odds: if you bet $1 on YES and win, you receive $1/q - 1 net
    if q <= 0 or q >= 1:
        logger.warning(f"kelly_bet | invalid q={q:.4f}, returning pass")
        return {**result_base, "action": "pass", "reason": f"Invalid market price q={q}"}

    b = (1.0 / q) - 1.0
    numerator = p * b - (1.0 - p)
    kelly_full = numerator / b if b > 0 else 0.0
    kelly_full = max(0.0, kelly_full)  # clamp negative Kelly to 0

    kelly_frac = kelly_full * config.KELLY_FRACTION
    raw_dollar = kelly_frac * bankroll

    # Cap at MAX_POSITION_FRACTION
    cap_dollar = config.MAX_POSITION_FRACTION * bankroll
    dollar_amount = min(raw_dollar, cap_dollar)
    dollar_amount = round(dollar_amount, 2)

    logger.info(
        f"kelly_bet | action={action} edge={edge:+.4f} "
        f"b={b:.4f} kelly_full={kelly_full:.4f} "
        f"kelly_frac_({config.KELLY_FRACTION}x)={kelly_frac:.4f} "
        f"raw=${raw_dollar:.2f} capped=${dollar_amount:.2f} "
        f"(cap={config.MAX_POSITION_FRACTION*100:.0f}% of ${bankroll:.2f})"
    )

    if dollar_amount < config.MIN_TRADE_DOLLARS:
        logger.info(
            f"kelly_bet PASS | dollar_amount=${dollar_amount:.2f} < MIN_TRADE_DOLLARS=${config.MIN_TRADE_DOLLARS}"
        )
        return {
            **result_base,
            "action": "pass",
            "kelly_full": kelly_full,
            "kelly_frac": kelly_frac,
            "dollar_amount": dollar_amount,
            "reason": f"Bet size ${dollar_amount:.2f} below minimum ${config.MIN_TRADE_DOLLARS}",
        }

    return {
        "action": action,
        "our_prob": our_prob,
        "market_prob": market_prob,
        "edge": edge,
        "b": b,
        "kelly_full": kelly_full,
        "kelly_frac": kelly_frac,
        "dollar_amount": dollar_amount,
        "reason": (
            f"Edge {edge:+.4f} | b={b:.3f} | "
            f"full_Kelly={kelly_full:.4f} | "
            f"{config.KELLY_FRACTION}x_Kelly={kelly_frac:.4f} | "
            f"bet=${dollar_amount:.2f}"
        ),
    }
