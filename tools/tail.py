"""tail.py — live view of events_archive while we don't have a dashboard.

Polls Postgres every 2 seconds and prints the most recent N events.
Updates by clearing the screen with ANSI escapes (works on Git Bash, modern
PowerShell, and any UNIX terminal).

Usage:
    python tools/tail.py                       # last 20 rows, refresh every 2s
    python tools/tail.py --limit 50            # last 50 rows
    python tools/tail.py --source cnbc_rss     # filter by source
    python tools/tail.py --min-score 7         # only events scoring >= 7
"""
from __future__ import annotations
import argparse
import os
import sys
import time
from datetime import datetime

# Make `services.*` importable when running from repo root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.shared.db import connect

CLEAR = "\x1b[2J\x1b[H"   # ANSI clear screen + cursor home


def _ensure_env():
    os.environ.setdefault(
        "POSTGRES_URL",
        "postgresql://rates:changeme@localhost:5432/rates",
    )


def _fetch_rows(limit: int, source: str | None, min_score: int | None,
                status: str | None) -> list[tuple]:
    where = []
    params: list = []
    if source:
        where.append("source = %s")
        params.append(source)
    if status:
        where.append("status = %s")
        params.append(status)
    if min_score is not None:
        where.append("score >= %s")
        params.append(min_score)
    where_clause = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
        SELECT ts_ingested, source, status, score, direction, confidence, headline
        FROM events_archive
        {where_clause}
        ORDER BY ts_ingested DESC
        LIMIT %s
    """
    params.append(limit)
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


def _render(rows) -> str:
    if not rows:
        return "(no events yet — is the ingestor running?)"
    header = f"{'time':19s}  {'source':10s}  {'status':8s}  {'sc':>2s}  {'dir':12s}  {'conf':>4s}  headline"
    sep = "-" * 110
    out = [header, sep]
    for ts, source, status, score, direction, confidence, headline in rows:
        when = ts.strftime("%Y-%m-%d %H:%M:%S") if isinstance(ts, datetime) else str(ts)
        sc_str = f"{score:2d}" if score is not None else " -"
        dir_str = (direction or "-")[:12]
        conf_str = f"{float(confidence):.2f}" if confidence is not None else " -  "
        head = (headline or "")[:60]
        out.append(f"{when}  {source:10s}  {status:8s}  {sc_str}  {dir_str:12s}  {conf_str}  {head}")
    return "\n".join(out)


def main() -> int:
    p = argparse.ArgumentParser(description="Live tail of events_archive.")
    p.add_argument("--limit", type=int, default=20)
    p.add_argument("--source", default=None)
    p.add_argument("--status", default=None)
    p.add_argument("--min-score", type=int, default=None)
    p.add_argument("--interval", type=float, default=2.0,
                   help="Refresh interval in seconds (default: 2.0)")
    args = p.parse_args()

    _ensure_env()
    try:
        while True:
            rows = _fetch_rows(args.limit, args.source, args.min_score, args.status)
            sys.stdout.write(CLEAR)
            sys.stdout.write(_render(rows))
            sys.stdout.write(f"\n\n(refreshing every {args.interval:.1f}s — Ctrl-C to exit)\n")
            sys.stdout.flush()
            time.sleep(args.interval)
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
