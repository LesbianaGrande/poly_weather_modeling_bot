"""
server.py — Minimal HTTP status dashboard for the paper trading bot.

Runs in a background thread alongside the APScheduler.
Serves a live HTML status page at / showing bankroll, open positions, and recent trades.

Port is read from the PORT env var (Railway sets this automatically) or defaults to 8080.
"""

import json
import logging
import os
import sqlite3
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import config

logger = logging.getLogger(__name__)

PORT = int(os.getenv("PORT", "8080"))


def _query(sql: str, params: tuple = ()) -> list[dict]:
    """Run a read-only query against the paper trades DB."""
    try:
        con = sqlite3.connect(config.DB_PATH)
        con.row_factory = sqlite3.Row
        rows = con.execute(sql, params).fetchall()
        con.close()
        return [dict(r) for r in rows]
    except Exception as exc:
        logger.warning(f"Status DB query failed: {exc}")
        return []


def _get_status() -> dict:
    """Collect all data needed to render the status page."""
    bankroll_row = _query("SELECT value FROM settings WHERE key='bankroll'")
    bankroll = float(bankroll_row[0]["value"]) if bankroll_row else config.STARTING_BANKROLL

    open_positions = _query(
        "SELECT * FROM positions WHERE status='open' ORDER BY entry_time DESC"
    )
    closed_positions = _query(
        "SELECT * FROM positions WHERE status='closed' ORDER BY exit_time DESC LIMIT 20"
    )
    recent_runs = _query(
        "SELECT * FROM model_runs ORDER BY run_time DESC LIMIT 30"
    )

    pnls = [r["pnl"] for r in closed_positions if r.get("pnl") is not None]
    total_pnl = sum(pnls)
    wins = sum(1 for p in pnls if p > 0)
    n_closed = len(pnls)
    win_rate = wins / n_closed if n_closed else 0.0

    return {
        "bankroll": bankroll,
        "starting_bankroll": config.STARTING_BANKROLL,
        "total_pnl": total_pnl,
        "return_pct": (bankroll - config.STARTING_BANKROLL) / config.STARTING_BANKROLL * 100,
        "open_positions": open_positions,
        "closed_positions": closed_positions,
        "recent_runs": recent_runs,
        "n_closed": n_closed,
        "win_rate": win_rate,
        "wins": wins,
        "losses": n_closed - wins,
        "generated_at": datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
    }


def _render_html(s: dict) -> str:
    pnl_color = "#22c55e" if s["total_pnl"] >= 0 else "#ef4444"
    ret_color = "#22c55e" if s["return_pct"] >= 0 else "#ef4444"

    def row_color(pnl):
        if pnl is None:
            return ""
        return "background:#f0fdf4" if pnl > 0 else ("background:#fef2f2" if pnl < 0 else "")

    open_rows = ""
    for p in s["open_positions"]:
        open_rows += f"""
        <tr>
          <td>{p.get('city','?')}</td>
          <td>{p.get('kind','?').upper()}</td>
          <td>{p.get('direction','?').upper()}</td>
          <td>{p.get('threshold_f','?')}°F</td>
          <td>{p.get('target_date','?')}</td>
          <td>${p.get('dollar_amount',0):.2f} @ {p.get('entry_price',0):.3f}</td>
          <td>{p.get('entry_time','?')[:16]}</td>
        </tr>"""

    closed_rows = ""
    for p in s["closed_positions"]:
        pnl = p.get("pnl")
        pnl_str = f"${pnl:+.2f}" if pnl is not None else "—"
        closed_rows += f"""
        <tr style="{row_color(pnl)}">
          <td>{p.get('city','?')}</td>
          <td>{p.get('kind','?').upper()}</td>
          <td>{p.get('direction','?').upper()}</td>
          <td>{p.get('threshold_f','?')}°F</td>
          <td>{p.get('target_date','?')}</td>
          <td>{p.get('exit_price','?')}</td>
          <td><b>{pnl_str}</b></td>
          <td>{(p.get('exit_time') or '')[:16]}</td>
        </tr>"""

    run_rows = ""
    for r in s["recent_runs"]:
        action = r.get("action_taken", "?")
        action_badge = {
            "yes": '<span style="color:#16a34a;font-weight:bold">BUY YES</span>',
            "no": '<span style="color:#dc2626;font-weight:bold">BUY NO</span>',
            "pass": '<span style="color:#6b7280">PASS</span>',
        }.get(action, action)
        run_rows += f"""
        <tr>
          <td>{r.get('run_time','?')[:16]}</td>
          <td style="font-size:0.75em;max-width:260px;overflow:hidden">{r.get('market_id','?')[:32]}...</td>
          <td>{r.get('lead_days','?')}d</td>
          <td>{r.get('our_prob',0):.3f}</td>
          <td>{r.get('market_prob',0):.3f}</td>
          <td>{r.get('edge',0):+.3f}</td>
          <td>${r.get('dollar_amount',0):.2f}</td>
          <td>{action_badge}</td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="60">
  <title>Polymarket Weather Bot — Status</title>
  <style>
    body {{ font-family: system-ui, sans-serif; margin: 0; padding: 20px; background: #f8fafc; color: #1e293b; }}
    h1 {{ margin: 0 0 4px; font-size: 1.4em; }}
    .subtitle {{ color: #64748b; font-size: 0.85em; margin-bottom: 20px; }}
    .cards {{ display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 24px; }}
    .card {{ background: white; border-radius: 10px; padding: 16px 20px; box-shadow: 0 1px 3px rgba(0,0,0,.08); min-width: 140px; }}
    .card .label {{ font-size: 0.75em; color: #64748b; text-transform: uppercase; letter-spacing: .05em; }}
    .card .value {{ font-size: 1.6em; font-weight: 700; margin-top: 2px; }}
    section {{ margin-bottom: 28px; }}
    h2 {{ font-size: 1em; color: #475569; border-bottom: 1px solid #e2e8f0; padding-bottom: 6px; margin-bottom: 10px; }}
    table {{ width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,.06); font-size: 0.85em; }}
    th {{ background: #f1f5f9; padding: 8px 10px; text-align: left; font-size: 0.75em; color: #64748b; text-transform: uppercase; }}
    td {{ padding: 7px 10px; border-top: 1px solid #f1f5f9; }}
    .empty {{ color: #94a3b8; font-style: italic; padding: 12px 10px; }}
  </style>
</head>
<body>
  <h1>🌡️ Polymarket Weather Bot</h1>
  <div class="subtitle">Paper trading · Auto-refreshes every 60s · {s['generated_at']}</div>

  <div class="cards">
    <div class="card">
      <div class="label">Bankroll</div>
      <div class="value">${s['bankroll']:.2f}</div>
    </div>
    <div class="card">
      <div class="label">Total P&amp;L</div>
      <div class="value" style="color:{pnl_color}">${s['total_pnl']:+.2f}</div>
    </div>
    <div class="card">
      <div class="label">Return</div>
      <div class="value" style="color:{ret_color}">{s['return_pct']:+.1f}%</div>
    </div>
    <div class="card">
      <div class="label">Open</div>
      <div class="value">{len(s['open_positions'])}</div>
    </div>
    <div class="card">
      <div class="label">Closed</div>
      <div class="value">{s['n_closed']}</div>
    </div>
    <div class="card">
      <div class="label">Win Rate</div>
      <div class="value">{s['win_rate']:.0%}</div>
    </div>
    <div class="card">
      <div class="label">W / L</div>
      <div class="value">{s['wins']} / {s['losses']}</div>
    </div>
  </div>

  <section>
    <h2>Open Positions ({len(s['open_positions'])})</h2>
    {'<table><thead><tr><th>City</th><th>Kind</th><th>Dir</th><th>Threshold</th><th>Target Date</th><th>Size @ Price</th><th>Opened</th></tr></thead><tbody>' + open_rows + '</tbody></table>' if s['open_positions'] else '<div class="empty">No open positions.</div>'}
  </section>

  <section>
    <h2>Recent Closed Trades (last 20)</h2>
    {'<table><thead><tr><th>City</th><th>Kind</th><th>Dir</th><th>Threshold</th><th>Target Date</th><th>Exit Price</th><th>P&amp;L</th><th>Closed</th></tr></thead><tbody>' + closed_rows + '</tbody></table>' if s['closed_positions'] else '<div class="empty">No closed trades yet.</div>'}
  </section>

  <section>
    <h2>Recent Model Runs (last 30)</h2>
    {'<table><thead><tr><th>Time</th><th>Market ID</th><th>Lead</th><th>Our P</th><th>Mkt P</th><th>Edge</th><th>Size</th><th>Action</th></tr></thead><tbody>' + run_rows + '</tbody></table>' if s['recent_runs'] else '<div class="empty">No model runs yet.</div>'}
  </section>
</body>
</html>"""


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/", "/status"):
            try:
                s = _get_status()
                body = _render_html(s).encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception as exc:
                logger.error(f"Status page error: {exc}", exc_info=True)
                self.send_response(500)
                self.end_headers()
                self.wfile.write(b"Internal error")
        elif self.path == "/health":
            body = b'{"status":"ok"}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, fmt, *args):
        # Route access logs through our logger at DEBUG level
        logger.debug(f"HTTP {fmt % args}")


def start_server() -> None:
    """Start the status HTTP server in a daemon thread."""
    httpd = ThreadingHTTPServer(("0.0.0.0", PORT), _Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    logger.info(f"Status dashboard running on http://0.0.0.0:{PORT}")
