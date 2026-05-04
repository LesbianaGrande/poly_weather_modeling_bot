"""
main.py — Entry point for the Polymarket weather paper trading bot.

Starts APScheduler with two jobs:
  - run_scan()          every SCAN_INTERVAL_HOURS (default 6h, aligned to GFS runs)
  - check_resolutions() every hour

On startup: initialises DB, prints config, runs one immediate scan.
"""

import logging
import signal
import sys
import time

import config

# Set up logging before any other imports that might log
config.setup_logging()
logger = logging.getLogger(__name__)

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

from trading import paper_trader as pt
from trading.scanner import run_scan, check_resolutions


def main() -> None:
    logger.info("=" * 70)
    logger.info("  Polymarket Weather Paper Trading Bot")
    logger.info("  Strategy: Ensemble Blend + MOS Correction + Kelly Sizing")
    logger.info("=" * 70)

    # Initialise database
    pt.init_db()

    # Immediate first run
    logger.info("Running initial scan on startup...")
    try:
        run_scan()
    except Exception as exc:
        logger.error(f"Initial scan failed: {exc}", exc_info=True)

    # --- Scheduler ---
    scheduler = BlockingScheduler(timezone="UTC")

    scheduler.add_job(
        run_scan,
        trigger=IntervalTrigger(hours=config.SCAN_INTERVAL_HOURS),
        id="market_scan",
        name="Market scan + trade",
        max_instances=1,
        misfire_grace_time=600,
    )
    logger.info(f"Scheduled market_scan every {config.SCAN_INTERVAL_HOURS}h")

    scheduler.add_job(
        check_resolutions,
        trigger=IntervalTrigger(hours=1),
        id="resolution_check",
        name="Check resolutions",
        max_instances=1,
        misfire_grace_time=300,
    )
    logger.info("Scheduled resolution_check every 1h")

    # Graceful shutdown on SIGINT/SIGTERM
    def _shutdown(signum, frame):
        logger.info(f"Received signal {signum}, shutting down scheduler...")
        scheduler.shutdown(wait=False)
        pt.print_summary_table()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    logger.info("Scheduler started. Press Ctrl+C to stop.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped.")
        pt.print_summary_table()


if __name__ == "__main__":
    main()
