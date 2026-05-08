"""
config.py â All settings sourced from environment variables with sensible defaults.
Load this module first; it also sets up the root logger.
"""

import os
import logging
from dotenv import load_dotenv

load_dotenv()

# ââ Kelly / betting ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
KELLY_FRACTION = float(os.getenv("KELLY_FRACTION", "0.25"))          # quarter-Kelly
MIN_EDGE = float(os.getenv("MIN_EDGE", "0.03"))                       # 3 % minimum edge
MAX_POSITION_FRACTION = float(os.getenv("MAX_POSITION_FRACTION", "0.10"))  # 10 % of bankroll cap
MIN_TRADE_DOLLARS = float(os.getenv("MIN_TRADE_DOLLARS", "1.0"))

# ââ Paper trading ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
STARTING_BANKROLL = float(os.getenv("STARTING_BANKROLL", "10000.0"))
# Set FORCE_BANKROLL > 0 to override the DB bankroll on startup (e.g. to reset after a run)
FORCE_BANKROLL = float(os.getenv("FORCE_BANKROLL", "0"))
DB_PATH = os.getenv("DB_PATH", "./paper_trades.db")

# ââ Trade limits âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
MAX_TRADES_PER_CITY_PER_SCAN = int(os.getenv("MAX_TRADES_PER_CITY_PER_SCAN", "2"))
MAX_SAME_TRADE_ALL_TIME = int(os.getenv("MAX_SAME_TRADE_ALL_TIME", "5"))

# ââ Scheduler âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
SCAN_INTERVAL_HOURS = int(os.getenv("SCAN_INTERVAL_HOURS", "6"))

# ââ Logging âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
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
    logger.info(f"  KELLY_FRACTION            = {KELLY_FRACTION}")
    logger.info(f"  MIN_EDGE                  = {MIN_EDGE}")
    logger.info(f"  MAX_POSITION_FRACTION     = {MAX_POSITION_FRACTION}")
    logger.info(f"  MIN_TRADE_DOLLARS         = {MIN_TRADE_DOLLARS}")
    logger.info(f"  STARTING_BANKROLL         = ${STARTING_BANKROLL:.2f}")
    logger.info(f"  FORCE_BANKROLL            = ${FORCE_BANKROLL:.2f}")
    logger.info(f"  DB_PATH                   = {DB_PATH}")
    logger.info(f"  MAX_TRADES_PER_CITY_SCAN  = {MAX_TRADES_PER_CITY_PER_SCAN}")
    logger.info(f"  MAX_SAME_TRADE_ALL_TIME   = {MAX_SAME_TRADE_ALL_TIME}")
    logger.info(f"  SCAN_INTERVAL_HOURS       = {SCAN_INTERVAL_HOURS}")
    logger.info(f"  LOG_LEVEL                 = {LOG_LEVEL}")
