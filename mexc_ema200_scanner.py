#!/usr/bin/env python3
"""
MEXC 200-EMA "cross & retest" scanner (4h chart)
================================================

Scans MEXC spot USDT pairs on the 4-hour timeframe and flags bullish setups
where price:

  1. RECLAIMED the 200 EMA  (closed back above it after being below), then
  2. RETESTED it            (pulled back down and tagged the EMA as support), then
  3. HELD / is confirming    (closed back above the EMA, EMA sloping up).

This is a trading-idea screener, not financial advice. Always confirm on the
chart yourself.

Usage
-----
    python3 mexc_ema200_scanner.py                # scan all spot USDT pairs
    python3 mexc_ema200_scanner.py --csv out.csv  # also write results to CSV
    python3 mexc_ema200_scanner.py --quote USDT --interval 4h
    python3 mexc_ema200_scanner.py --top 40 --workers 12

Tunable thresholds (see the CONFIG section / --help) control how "fresh" the
cross must be, how close the retest has to come to the EMA, and how much slack
the pullback is allowed before it counts as a failed reclaim.

Only the standard library + `requests` are required:
    pip install requests
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict

try:
    import requests
except ImportError:
    sys.exit("This script needs the 'requests' package.  Install it with:\n"
             "    pip install requests")

BASE = "https://api.mexc.com"

# ----------------------------------------------------------------------------
# CONFIG (defaults; overridable via CLI)
# ----------------------------------------------------------------------------
EMA_PERIOD      = 200     # the EMA we care about
KLINE_LIMIT     = 1000    # candles pulled per symbol (MEXC max ~1000)
LOOKBACK        = 30      # search this many recent candles for the setup
RETEST_TOL      = 0.020   # a pullback within this % of the EMA counts as a retest (2.0%)
BREAK_TOL       = 0.005   # a close this far *below* the EMA voids the reclaim (0.5%)
MAX_ABOVE_NOW   = 0.08    # ignore names already >8% above EMA (retest is stale/extended)
MIN_SLOPE       = 0.0     # require EMA slope >= this over the lookback (0 = flat-or-up)


# ----------------------------------------------------------------------------
# Indicators
# ----------------------------------------------------------------------------
def ema(values: list[float], period: int) -> list[float]:
    """Exponential moving average. Seeded with an SMA of the first `period`
    values (standard TradingView-style seeding). Returns a list the same length
    as `values`; entries before the seed are None."""
    n = len(values)
    out: list[float | None] = [None] * n
    if n < period:
        return out
    k = 2.0 / (period + 1.0)
    seed = sum(values[:period]) / period
    out[period - 1] = seed
    prev = seed
    for i in range(period, n):
        prev = values[i] * k + prev * (1.0 - k)
        out[i] = prev
    return out


# ----------------------------------------------------------------------------
# Setup detection
# ----------------------------------------------------------------------------
@dataclass
class Hit:
    symbol: str
    price: float
    ema: float
    pct_above_ema: float   # how far current close sits above the EMA (%)
    bars_since_cross: int  # candles since the reclaim
    retest_gap_pct: float  # how close the retest low came to the EMA (%)
    score: float           # higher = cleaner / fresher setup


def detect_cross_and_retest(
    highs: list[float], lows: list[float], closes: list[float],
    *, ema_period: int = EMA_PERIOD, lookback: int = LOOKBACK,
    retest_tol: float = RETEST_TOL, break_tol: float = BREAK_TOL,
    max_above_now: float = MAX_ABOVE_NOW, min_slope: float = MIN_SLOPE,
) -> tuple[bool, dict]:
    """
    Return (is_hit, details) for the cross-and-retest pattern on CLOSED candles.

    Pattern, evaluated over the last `lookback` closed candles:
      * a reclaim: close crosses from below the EMA to at/above it,
      * a retest after the reclaim: a candle's LOW tags the EMA (within retest_tol)
        while its CLOSE stays at/above the EMA (held as support),
      * no candle closed decisively (> break_tol) below the EMA after the reclaim,
      * the latest close is above the EMA but not overextended (<= max_above_now),
      * EMA slope over the window is >= min_slope.
    """
    e = ema(closes, ema_period)
    n = len(closes)
    if n < ema_period + 2 or e[-1] is None:
        return False, {}

    last = n - 1
    ema_now = e[last]
    close_now = closes[last]

    # Must currently be above the EMA, and not overextended.
    if close_now < ema_now:
        return False, {}
    pct_above = (close_now - ema_now) / ema_now
    if pct_above > max_above_now:
        return False, {}

    # EMA slope over the lookback window (rise / value).
    j = max(ema_period - 1, last - lookback)
    if e[j] is None or e[j] == 0:
        return False, {}
    slope = (ema_now - e[j]) / e[j]
    if slope < min_slope:
        return False, {}

    # Find the most recent reclaim (cross up) inside the lookback window.
    cross_idx = None
    start = max(ema_period, last - lookback)
    for i in range(last, start, -1):
        if e[i] is None or e[i - 1] is None:
            continue
        if closes[i - 1] < e[i - 1] and closes[i] >= e[i]:
            cross_idx = i
            break
    if cross_idx is None:
        return False, {}

    # After the reclaim, price must not have closed decisively back below the EMA.
    for i in range(cross_idx, last + 1):
        if e[i] is None:
            continue
        if closes[i] < e[i] * (1.0 - break_tol):
            return False, {}

    # Look for a retest between the reclaim and now: low tags the EMA band,
    # close holds above it. (The reclaim candle itself doesn't count.)
    best_gap = None
    retest_idx = None
    for i in range(cross_idx + 1, last + 1):
        if e[i] is None:
            continue
        low_gap = (lows[i] - e[i]) / e[i]            # how far the low sat above EMA
        if low_gap <= retest_tol and closes[i] >= e[i]:
            if best_gap is None or low_gap < best_gap:
                best_gap = low_gap
                retest_idx = i
    if retest_idx is None:
        return False, {}

    bars_since_cross = last - cross_idx
    # Score: reward a tight retest, a fresh cross, and price sitting near the EMA.
    freshness = max(0.0, 1.0 - bars_since_cross / lookback)
    tightness = max(0.0, 1.0 - best_gap / retest_tol)
    proximity = max(0.0, 1.0 - pct_above / max_above_now)
    score = round(100 * (0.45 * tightness + 0.35 * freshness + 0.20 * proximity), 1)

    return True, {
        "price": close_now,
        "ema": ema_now,
        "pct_above_ema": round(pct_above * 100, 2),
        "bars_since_cross": bars_since_cross,
        "retest_gap_pct": round(best_gap * 100, 2),
        "score": score,
    }


# ----------------------------------------------------------------------------
# MEXC API
# ----------------------------------------------------------------------------
def get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "ema200-scanner/1.0"})
    return s


def list_symbols(sess: requests.Session, quote: str) -> list[str]:
    r = sess.get(f"{BASE}/api/v3/exchangeInfo", timeout=30)
    r.raise_for_status()
    data = r.json()
    syms = []
    for s in data.get("symbols", []):
        if s.get("quoteAsset") != quote:
            continue
        # status is "1"/"ENABLED" for tradable spot symbols
        if str(s.get("status")) not in ("1", "ENABLED", "TRADING"):
            continue
        # skip leveraged tokens (3L/3S/5L/5S etc.) — noisy for this scan
        base = s.get("baseAsset", "")
        if base.endswith(("3L", "3S", "5L", "5S", "2L", "2S", "4L", "4S")):
            continue
        syms.append(s["symbol"])
    return sorted(set(syms))


def fetch_klines(sess: requests.Session, symbol: str, interval: str,
                 limit: int) -> list[list] | None:
    for attempt in range(4):
        try:
            r = sess.get(
                f"{BASE}/api/v3/klines",
                params={"symbol": symbol, "interval": interval, "limit": limit},
                timeout=30,
            )
            if r.status_code == 429:            # rate limited — back off
                time.sleep(1.5 * (attempt + 1))
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException:
            time.sleep(0.6 * (attempt + 1))
    return None


def scan_symbol(sess: requests.Session, symbol: str, interval: str,
                cfg: dict) -> Hit | None:
    raw = fetch_klines(sess, symbol, interval, cfg["kline_limit"])
    if not raw or len(raw) < EMA_PERIOD + 2:
        return None
    # MEXC kline row: [openTime, open, high, low, close, volume, closeTime, ...]
    # Drop the last (still-forming) candle so we work with closed bars only.
    rows = raw[:-1]
    try:
        highs = [float(x[2]) for x in rows]
        lows = [float(x[3]) for x in rows]
        closes = [float(x[4]) for x in rows]
    except (ValueError, IndexError):
        return None

    ok, d = detect_cross_and_retest(
        highs, lows, closes,
        ema_period=EMA_PERIOD, lookback=cfg["lookback"],
        retest_tol=cfg["retest_tol"], break_tol=cfg["break_tol"],
        max_above_now=cfg["max_above_now"], min_slope=cfg["min_slope"],
    )
    if not ok:
        return None
    return Hit(symbol=symbol, **d)


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
def main() -> None:
    p = argparse.ArgumentParser(description="MEXC 4h 200-EMA cross & retest scanner")
    p.add_argument("--quote", default="USDT", help="quote asset (default USDT)")
    p.add_argument("--interval", default="4h", help="kline interval (default 4h)")
    p.add_argument("--workers", type=int, default=10, help="concurrent requests")
    p.add_argument("--top", type=int, default=0, help="show only the top N by score (0 = all)")
    p.add_argument("--csv", default="", help="also write results to this CSV path")
    p.add_argument("--kline-limit", type=int, default=KLINE_LIMIT)
    p.add_argument("--lookback", type=int, default=LOOKBACK)
    p.add_argument("--retest-tol", type=float, default=RETEST_TOL)
    p.add_argument("--break-tol", type=float, default=BREAK_TOL)
    p.add_argument("--max-above", type=float, default=MAX_ABOVE_NOW)
    p.add_argument("--min-slope", type=float, default=MIN_SLOPE)
    args = p.parse_args()

    cfg = {
        "kline_limit": args.kline_limit, "lookback": args.lookback,
        "retest_tol": args.retest_tol, "break_tol": args.break_tol,
        "max_above_now": args.max_above, "min_slope": args.min_slope,
    }

    sess = get_session()
    print(f"Fetching {args.quote} spot symbols from MEXC ...", file=sys.stderr)
    try:
        symbols = list_symbols(sess, args.quote)
    except requests.RequestException as e:
        sys.exit(f"Could not reach MEXC: {e}")
    print(f"Scanning {len(symbols)} pairs on the {args.interval} chart "
          f"(200 EMA cross & retest) ...", file=sys.stderr)

    hits: list[Hit] = []
    done = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(scan_symbol, sess, s, args.interval, cfg): s
                for s in symbols}
        for fut in as_completed(futs):
            done += 1
            if done % 100 == 0:
                print(f"  ...{done}/{len(symbols)}", file=sys.stderr)
            h = fut.result()
            if h:
                hits.append(h)

    hits.sort(key=lambda h: h.score, reverse=True)
    if args.top > 0:
        hits = hits[:args.top]

    # Pretty table
    if not hits:
        print("\nNo cross-and-retest setups found right now.")
    else:
        print(f"\n{'SYMBOL':<14}{'PRICE':>14}{'EMA200':>14}"
              f"{'%>EMA':>8}{'BARS':>6}{'RETEST%':>9}{'SCORE':>7}")
        print("-" * 72)
        for h in hits:
            print(f"{h.symbol:<14}{h.price:>14.8g}{h.ema:>14.8g}"
                  f"{h.pct_above_ema:>8.2f}{h.bars_since_cross:>6}"
                  f"{h.retest_gap_pct:>9.2f}{h.score:>7.1f}")
        print(f"\n{len(hits)} setup(s).  BARS = candles since the reclaim; "
              f"RETEST% = how close the pullback came to the EMA.")

    if args.csv:
        with open(args.csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(asdict(hits[0]).keys())
                               if hits else ["symbol"])
            w.writeheader()
            for h in hits:
                w.writerow(asdict(h))
        print(f"Wrote {len(hits)} rows to {args.csv}", file=sys.stderr)


if __name__ == "__main__":
    main()
