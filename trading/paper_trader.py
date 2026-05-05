"""
trading/paper_trader.py — SQLite-backed paper trading engine.

Tracks virtual positions and running bankroll. No real money, no real orders.

Tables:
  positions   — every trade opened/closed
  model_runs  — every model evaluation (even when no trade is taken)
  settings    — key/value store (bankroll, etc.)
"""

import logging
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime, timezone

import config

logger = logging.getLogger(__name__)


@contextmanager
def _conn():
    """Context manager: open DB connection, commit on success, rollback on error."""
    con = sqlite3.connect(config.DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    try:
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def init_db() -> None:
    """Create tables if they don't exist. Safe to call on every startup."""
    logger.info(f"Initialising DB at {config.DB_PATH}")
    with _conn() as con:
        con.executescript("""
            CREATE TABLE IF NOT EXISTS positions (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id       TEXT NOT NULL,
                question        TEXT,
                city            TEXT,
                kind            TEXT,           -- 'high' or 'low'
                direction       TEXT NOT NULL,  -- 'yes' or 'no'
                target_date     TEXT,
                threshold_f     REAL,
                entry_price     REAL NOT NULL,  -- price paid per share (0-1)
                shares          REAL NOT NULL,  -- number of shares
                dollar_amount   REAL NOT NULL,  -- total dollars committed
                status          TEXT DEFAULT 'open',  -- 'open' or 'closed'
                entry_time      TEXT NOT NULL,
                exit_price      REAL,
                exit_time       TEXT,
                pnl             REAL
            );

            CREATE TABLE IF NOT EXISTS model_runs (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id       TEXT NOT NULL,
                run_time        TEXT NOT NULL,
                city            TEXT,
                kind            TEXT,           -- 'high' or 'low'
                threshold_f     REAL,           -- market threshold in °F
                target_date     TEXT,
                lead_days       INTEGER,
                blended_mean    REAL,           -- our predicted temperature (°F)
                our_prob        REAL,           -- our model probability
                market_prob     REAL,           -- market implied probability
                edge            REAL,
                kelly_fraction  REAL,
                dollar_amount   REAL,
                action_taken    TEXT,           -- 'yes', 'no', 'pass'
                n_ensemble      INTEGER,
                n_clim          INTEGER,
                mos_correction  REAL,
                notes           TEXT
            );

            CREATE TABLE IF NOT EXISTS settings (
                key     TEXT PRIMARY KEY,
                value   TEXT NOT NULL
            );
        """)

    # Migrate existing DBs: add new columns if they don't exist yet
    _migrate_model_runs_columns()
    logger.info("DB initialised OK")
    _ensure_bankroll()


def _migrate_model_runs_columns() -> None:
    """Add columns introduced in v2 to model_runs if this is an existing DB."""
    new_cols = [
        ("city",         "TEXT"),
        ("kind",         "TEXT"),
        ("threshold_f",  "REAL"),
        ("target_date",  "TEXT"),
        ("blended_mean", "REAL"),
    ]
    with _conn() as con:
        existing = {row[1] for row in con.execute("PRAGMA table_info(model_runs)").fetchall()}
        for col_name, col_type in new_cols:
            if col_name not in existing:
                con.execute(f"ALTER TABLE model_runs ADD COLUMN {col_name} {col_type}")
                logger.info(f"  Migrated model_runs: added column '{col_name}'")



def _ensure_bankroll() -> None:
    """Set starting bankroll if not already present."""
    with _conn() as con:
        row = con.execute("SELECT value FROM settings WHERE key='bankroll'").fetchone()
        if row is None:
            con.execute(
                "INSERT INTO settings (key, value) VALUES ('bankroll', ?)",
                (str(config.STARTING_BANKROLL),),
            )
            logger.info(f"Bankroll initialised to ${config.STARTING_BANKROLL:.2f}")
        else:
            logger.info(f"Bankroll loaded: ${float(row['value']):.2f}")


def get_bankroll() -> float:
    with _conn() as con:
        row = con.execute("SELECT value FROM settings WHERE key='bankroll'").fetchone()
        val = float(row["value"]) if row else config.STARTING_BANKROLL
    logger.debug(f"get_bankroll → ${val:.2f}")
    return val


def set_bankroll(amount: float) -> None:
    with _conn() as con:
        con.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES ('bankroll', ?)",
            (str(amount),),
        )
    logger.debug(f"set_bankroll → ${amount:.2f}")


def open_position(
    market_id: str,
    question: str,
    city: str,
    kind: str,
    direction: str,
    target_date: date,
    threshold_f: float,
    entry_price: float,
    dollar_amount: float,
) -> int:
    """
    Open a new paper position. Deducts dollar_amount from bankroll.
    Returns the new position id.
    """
    shares = dollar_amount / entry_price if entry_price > 0 else 0
    now = datetime.now(tz=timezone.utc).isoformat()

    bankroll = get_bankroll()
    new_bankroll = bankroll - dollar_amount
    if new_bankroll < 0:
        logger.warning(
            f"open_position | insufficient bankroll ${bankroll:.2f} for ${dollar_amount:.2f}"
        )
        new_bankroll = 0.0

    with _conn() as con:
        cur = con.execute(
            """INSERT INTO positions
               (market_id, question, city, kind, direction, target_date, threshold_f,
                entry_price, shares, dollar_amount, status, entry_time)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?)""",
            (
                market_id, question, city, kind, direction,
                target_date.isoformat(), threshold_f,
                entry_price, shares, dollar_amount, now,
            ),
        )
        pos_id = cur.lastrowid

    set_bankroll(new_bankroll)
    logger.info(
        f"open_position | id={pos_id} market={market_id[:20]} {direction.upper()} "
        f"@ {entry_price:.3f} shares={shares:.2f} cost=${dollar_amount:.2f} "
        f"bankroll: ${bankroll:.2f} → ${new_bankroll:.2f}"
    )
    return pos_id


def close_position(market_id: str, exit_price: float) -> list[dict]:
    """
    Close all open positions for a market_id at exit_price.
    Adds proceeds to bankroll.
    Returns list of closed position dicts.
    """
    now = datetime.now(tz=timezone.utc).isoformat()
    closed = []

    with _conn() as con:
        rows = con.execute(
            "SELECT * FROM positions WHERE market_id=? AND status='open'", (market_id,)
        ).fetchall()

        for row in rows:
            shares = row["shares"]
            entry_price = row["entry_price"]
            direction = row["direction"]
            dollar_amount = row["dollar_amount"]

            # Proceeds = shares * exit_price (exit_price is the final resolution value)
            proceeds = shares * exit_price
            pnl = proceeds - dollar_amount

            con.execute(
                """UPDATE positions
                   SET status='closed', exit_price=?, exit_time=?, pnl=?
                   WHERE id=?""",
                (exit_price, now, pnl, row["id"]),
            )
            closed.append(dict(row) | {"exit_price": exit_price, "pnl": pnl, "proceeds": proceeds})

            bankroll = get_bankroll()
            new_bankroll = bankroll + proceeds
            set_bankroll(new_bankroll)

            logger.info(
                f"close_position | id={row['id']} market={market_id[:20]} "
                f"{direction.upper()} exit_price={exit_price:.3f} "
                f"proceeds=${proceeds:.2f} pnl=${pnl:+.2f} "
                f"bankroll: ${bankroll:.2f} → ${new_bankroll:.2f}"
            )

    return closed


def position_exists(market_id: str) -> bool:
    """Return True if there is already an open position for this market."""
    with _conn() as con:
        row = con.execute(
            "SELECT id FROM positions WHERE market_id=? AND status='open'", (market_id,)
        ).fetchone()
    exists = row is not None
    logger.debug(f"position_exists | market={market_id[:30]} → {exists}")
    return exists


def get_open_positions() -> list[dict]:
    with _conn() as con:
        rows = con.execute("SELECT * FROM positions WHERE status='open' ORDER BY entry_time").fetchall()
    result = [dict(r) for r in rows]
    logger.debug(f"get_open_positions | {len(result)} open")
    return result


def log_model_run(
    market_id: str,
    lead_days: int,
    our_prob: float,
    market_prob: float,
    edge: float,
    kelly_fraction: float,
    dollar_amount: float,
    action_taken: str,
    n_ensemble: int = 0,
    n_clim: int = 0,
    mos_correction: float | None = None,
    notes: str = "",
    city: str = "",
    kind: str = "",
    threshold_f: float | None = None,
    target_date: str = "",
    blended_mean: float | None = None,
) -> None:
    now = datetime.now(tz=timezone.utc).isoformat()
    with _conn() as con:
        con.execute(
            """INSERT INTO model_runs
               (market_id, run_time, city, kind, threshold_f, target_date,
                lead_days, blended_mean, our_prob, market_prob, edge,
                kelly_fraction, dollar_amount, action_taken, n_ensemble, n_clim,
                mos_correction, notes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                market_id, now, city, kind, threshold_f, target_date,
                lead_days, blended_mean, our_prob, market_prob, edge,
                kelly_fraction, dollar_amount, action_taken,
                n_ensemble, n_clim, mos_correction, notes,
            ),
        )
    logger.debug(
        f"log_model_run | {city} {kind} thr={threshold_f}°F "
        f"pred={blended_mean:.1f}°F action={action_taken} "
        f"our={our_prob:.4f} mkt={market_prob:.4f} edge={edge:+.4f}"
        if blended_mean else
        f"log_model_run | market={market_id[:30]} action={action_taken} "
        f"our={our_prob:.4f} mkt={market_prob:.4f} edge={edge:+.4f}"
    )


def get_summary() -> dict:
    """Return a summary dict: bankroll, total PnL, win rate, position counts."""
    bankroll = get_bankroll()
    with _conn() as con:
        all_closed = con.execute(
            "SELECT pnl FROM positions WHERE status='closed'"
        ).fetchall()
        n_open = con.execute(
            "SELECT COUNT(*) FROM positions WHERE status='open'"
        ).fetchone()[0]

    pnls = [r["pnl"] for r in all_closed if r["pnl"] is not None]
    total_pnl = sum(pnls)
    n_closed = len(pnls)
    wins = sum(1 for p in pnls if p > 0)
    win_rate = wins / n_closed if n_closed else 0.0

    summary = {
        "bankroll": bankroll,
        "starting_bankroll": config.STARTING_BANKROLL,
        "total_pnl": total_pnl,
        "return_pct": (bankroll - config.STARTING_BANKROLL) / config.STARTING_BANKROLL * 100,
        "n_open": n_open,
        "n_closed": n_closed,
        "win_rate": win_rate,
        "wins": wins,
        "losses": n_closed - wins,
    }
    logger.info(
        f"Summary | bankroll=${bankroll:.2f} ({summary['return_pct']:+.1f}%) "
        f"PnL=${total_pnl:+.2f} open={n_open} closed={n_closed} "
        f"win_rate={win_rate:.1%}"
    )
    return summary


def print_summary_table() -> None:
    """Log a formatted summary table for easy reading in logs."""
    s = get_summary()
    lines = [
        "=" * 60,
        "  PAPER TRADING SUMMARY",
        "=" * 60,
        f"  Bankroll:       ${s['bankroll']:.2f}  ({s['return_pct']:+.1f}% vs start)",
        f"  Total PnL:      ${s['total_pnl']:+.2f}",
        f"  Open positions: {s['n_open']}",
        f"  Closed:         {s['n_closed']}  (W:{s['wins']} / L:{s['losses']})  win_rate={s['win_rate']:.1%}",
        "=" * 60,
    ]
    for line in lines:
        logger.info(line)

    # Also log open positions
    open_pos = get_open_positions()
    if open_pos:
        logger.info("  OPEN POSITIONS:")
        for p in open_pos:
            logger.info(
                f"    [{p['id']}] {p['city']} {p['kind'].upper()} "
                f"{p['direction'].upper()} @ {p['entry_price']:.3f} "
                f"${p['dollar_amount']:.2f}  target={p['target_date']}"
            )
    else:
        logger.info("  No open positions.")
