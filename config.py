"""
config.py — All settings sourced from environment variables with sensible defaults.
Load this module first; it also sets up the root logger.
"""

import os
import logging
from dotenv import load_dotenv

load_dotenv()

# ── Kelly / betting ────────────────────────────────────────────────────────────
KELLY_FRACTION = float(os.getenv("KELLY_FRACTION", "0.25"))          # quarter-Kelly
MIN_EDGE = float(os.getenv("MIN_EDGE", "0.03"))                       # 3 % minimum edge
MAX_POSITION_FRACTION = float(os.getenv("MAX_POSITION_FRACTION", "0.10"))  # 10 % of bankroll cap
MIN_TRADE_DOLLARS = float(os.getenv("MIN_TRADE_DOLLARS", "1.0"))

# ── Paper trading ──────────────────────────────────────────────────────────────
STARTING_BANKROLL = float(os.getenv("STARTING_BANKROLL", "1000.0"))
DB_PATH = os.getenv("DB_PATH", "./paper_trades.db")

# ── Scheduler ─────────────────────────────────────────────────────────────────
SCAN_INTERVAL_HOURS = int(os.getenv("SCAN_INTERVAL_HOURS", "6"))

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG")


def setup_logging() -> None:
    """Configure the root logger. Call once at startup."""
    level = getattr(logging, LOG_LEVEL.upper(), logging.DEBUG)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Suppress noisy third-party loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("apscheduler").setLevel(logging.INFO)

    logger = logging.getLogger(__name__)
    logger.info("=== Config loaded ===")
    logger.info(f"  KELLY_FRACTION       = {KELLY_FRACTION}")
    logger.info(f"  MIN_EDGE             = {MIN_EDGE}")
    logger.info(f"  MAX_POSITION_FRACTION= {MAX_POSITION_FRACTION}")
    logger.info(f"  MIN_TRADE_DOLLARS    = {MIN_TRADE_DOLLARS}")
    logger.info(f"  STARTING_BANKROLL    = ${STARTING_BANKROLL:.2f}")
    logger.info(f"  DB_PATH              = {DB_PATH}")
    logger.info(f"  SCAN_INTERVAL_HOURS  = {SCAN_INTERVAL_HOURS}")
    logger.info(f"  LOG_LEVEL            = {LOG_LEVEL}")
