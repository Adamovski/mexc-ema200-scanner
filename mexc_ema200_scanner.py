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
    sl: float              # suggested stop-loss (below structure / EMA)
    tp1: float | None      # 1st target: nearest overhead resistance (None = blue sky)
    tp2: float | None      # 2nd target: next resistance above
    tp3: float | None      # 3rd target: next resistance above that
    rr: float | None       # reward:risk to TP1, entry = current price


def resistances_above(highs: list[float], upto: int, price: float,
                      left: int = 3, right: int = 3, window: int = 300,
                      max_n: int = 3) -> list[float]:
    """The nearest swing-high pivots ABOVE `price`, ascending — the overhead
    resistance ceilings price would run into, in order.

    A pivot high is a candle whose high is >= the `left` highs before it and the
    `right` highs after it. Near-equal levels (within 0.5%) are merged so the
    targets are meaningfully separated. Returns up to `max_n` levels."""
    start = max(left, upto - window)
    res = []
    for i in range(start, upto - right + 1):
        h = highs[i]
        if all(h >= highs[i - d] for d in range(1, left + 1)) and \
           all(h >= highs[i + d] for d in range(1, right + 1)) and h > price:
            res.append(h)
    res.sort()
    out: list[float] = []
    for h in res:
        if not out or h > out[-1] * 1.005:
            out.append(h)
        if len(out) >= max_n:
            break
    return out


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

    # Look for a retest between the reclaim and now: the low must TAG the EMA
    # band — i.e. come within +/- retest_tol of the EMA — while the candle still
    # CLOSES at/above the EMA (held as support). Requiring the low to be within
    # the band on BOTH sides rejects violent downside wicks that plunge far
    # below the EMA and merely close back above (common noise on illiquid names).
    # We keep the retest whose low sits CLOSEST to the EMA (the cleanest tag).
    best_gap = None
    retest_idx = None
    for i in range(cross_idx + 1, last + 1):
        if e[i] is None:
            continue
        low_gap = (lows[i] - e[i]) / e[i]            # low vs EMA (+above / -below)
        if abs(low_gap) <= retest_tol and closes[i] >= e[i]:
            if best_gap is None or abs(low_gap) < abs(best_gap):
                best_gap = low_gap
                retest_idx = i
    if retest_idx is None:
        return False, {}

    bars_since_cross = last - cross_idx
    # Score, 0-100: reward a tight retest, a fresh cross, and price sitting near
    # the EMA. Each component is clamped to [0,1] so the total never exceeds 100.
    freshness = max(0.0, 1.0 - bars_since_cross / lookback)
    tightness = max(0.0, min(1.0, 1.0 - abs(best_gap) / retest_tol))
    proximity = max(0.0, min(1.0, 1.0 - pct_above / max_above_now))
    score = round(100 * (0.45 * tightness + 0.35 * freshness + 0.20 * proximity), 1)

    # --- suggested trade levels (technical estimate, NOT advice) -------------
    # Entry is taken at the current price. The setup is invalidated on a close
    # back below the reclaimed EMA, so the stop sits just under the structural
    # support: the lower of the EMA and the lowest low since the reclaim (the
    # retest low), minus a small buffer.
    entry = close_now                       # entry = current price
    swing_low = min(lows[cross_idx:last + 1])
    sl = min(swing_low, ema_now) * (1.0 - 0.003)
    # Targets = the nearest overhead resistances (prior swing highs above price),
    # in order. If price is in blue sky some/all will be None.
    res = resistances_above(highs, last, entry, max_n=3)
    tp1 = res[0] if len(res) > 0 else None
    tp2 = res[1] if len(res) > 1 else None
    tp3 = res[2] if len(res) > 2 else None
    rr = round((tp1 - entry) / (entry - sl), 2) if (tp1 and entry > sl) else None

    return True, {
        "price": close_now,
        "ema": ema_now,
        "pct_above_ema": round(pct_above * 100, 2),
        "bars_since_cross": bars_since_cross,
        "retest_gap_pct": round(best_gap * 100, 2),
        "score": score,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "rr": rr,
    }


# ----------------------------------------------------------------------------
# Bull-flag detection
# ----------------------------------------------------------------------------
@dataclass
class FlagHit:
    symbol: str
    price: float
    pole_gain_pct: float    # size of the flagpole (impulse) move, %
    flag_bars: int          # length of the consolidation, in candles
    pullback_pct: float     # how deep the flag retraced the pole, %
    vol_contraction: float  # flag avg volume / pole avg volume (<1 = drying up)
    breakout: float         # breakout trigger (top of the flag)
    sl: float               # stop below the flag
    tp1: float              # measured move (breakout + 1.0x pole height)
    tp2: float              # 1.618x pole extension
    tp3: float              # 2.0x pole extension
    rr: float               # reward:risk to TP1, entry = current price
    score: float            # overall quality 0-100


def detect_bull_flag(
    highs: list[float], lows: list[float], closes: list[float],
    volumes: list[float], *,
    pole_min_gain: float = 0.15, pole_max_len: int = 8,
    flag_min_bars: int = 3, flag_max_bars: int = 12,
    max_retrace: float = 0.5, search: int = 30,
) -> tuple[bool, dict]:
    """
    Detect a classic bull flag on closed candles: a sharp impulse up (the
    flagpole) followed by a shallow, tightening pullback (the flag), ideally on
    declining volume, that hasn't broken down or fully broken out yet.

    Returns (is_hit, details). Scans recent candles for the best-scoring flag.
    """
    n = len(closes)
    if n < search + pole_max_len + 2:
        return False, {}
    last = n - 1

    best = None
    # Try each possible flag length ending on the latest candle.
    for flag_bars in range(flag_min_bars, flag_max_bars + 1):
        flag_start = last - flag_bars + 1
        if flag_start - pole_max_len < 1:
            continue
        # Pole = the run into the flag. Pole top is the high just before the flag.
        pole_top = max(highs[flag_start - pole_max_len:flag_start])
        pole_low = min(lows[flag_start - pole_max_len:flag_start])
        if pole_low <= 0:
            continue
        pole_gain = (pole_top - pole_low) / pole_low
        if pole_gain < pole_min_gain:
            continue

        flag_hi = max(highs[flag_start:last + 1])
        flag_lo = min(lows[flag_start:last + 1])
        close_now = closes[last]

        # Flag must consolidate BELOW the pole top (not already run away), and
        # must not have broken down (price still above the flag low / mid-pole).
        if flag_hi > pole_top * 1.03:
            continue
        if close_now <= flag_lo:
            continue
        # Shallow pullback: the flag low shouldn't retrace more than max_retrace
        # of the pole.
        retrace = (pole_top - flag_lo) / (pole_top - pole_low)
        if retrace > max_retrace:
            continue

        pole_vol = sum(volumes[flag_start - pole_max_len:flag_start]) / pole_max_len
        flag_vol = sum(volumes[flag_start:last + 1]) / flag_bars
        vol_contraction = (flag_vol / pole_vol) if pole_vol > 0 else 1.0

        # Score components, each 0-1.
        pole_s = max(0.0, min(1.0, (pole_gain - pole_min_gain) / (0.6 - pole_min_gain)))
        tight_s = max(0.0, min(1.0, 1.0 - retrace / max_retrace))
        vol_s = max(0.0, min(1.0, 1.0 - vol_contraction / 1.0))   # lower vol = better
        # Position: reward price sitting near the top of the flag (coiled to break)
        rng = max(flag_hi - flag_lo, 1e-12)
        pos_s = max(0.0, min(1.0, (close_now - flag_lo) / rng))
        score = 100 * (0.35 * pole_s + 0.25 * tight_s + 0.25 * vol_s + 0.15 * pos_s)

        if best is None or score > best[0]:
            best = (score, flag_bars, pole_gain, retrace, vol_contraction,
                    flag_hi, flag_lo, pole_top, pole_low, close_now)

    if best is None:
        return False, {}

    (score, flag_bars, pole_gain, retrace, vol_contraction,
     flag_hi, flag_lo, pole_top, pole_low, close_now) = best

    entry = close_now                    # entry = current price
    breakout = flag_hi
    sl = flag_lo * (1.0 - 0.003)
    pole_height = pole_top - pole_low
    # Continuation targets projected off the breakout: the classic measured move
    # (1.0x pole) then the common 1.618x and 2.0x extensions.
    tp1 = breakout + 1.000 * pole_height
    tp2 = breakout + 1.618 * pole_height
    tp3 = breakout + 2.000 * pole_height
    rr = round((tp1 - entry) / (entry - sl), 2) if entry > sl else 0.0

    return True, {
        "price": close_now,
        "pole_gain_pct": round(pole_gain * 100, 1),
        "flag_bars": flag_bars,
        "pullback_pct": round(retrace * 100, 1),
        "vol_contraction": round(vol_contraction, 2),
        "breakout": breakout,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "rr": rr,
        "score": round(score, 1),
    }


# ----------------------------------------------------------------------------
# MEXC API
# ----------------------------------------------------------------------------
def get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "ema200-scanner/1.0"})
    return s


# Stablecoins / pegged assets — a "200-EMA cross" on these is meaningless
# (they hover around $1), so we drop them from the crypto scan.
STABLE_BASES = {
    "USDT", "USDC", "USDD", "DAI", "TUSD", "FDUSD", "USDE", "USDP", "PYUSD",
    "GUSD", "BUSD", "EURT", "EURS", "EUR", "USD1", "FRAX", "LUSD", "SUSD",
    "USDJ", "USDX", "CUSD", "USTC", "AEUR", "XUSD",
}


FUTURES_BASE = "https://contract.mexc.com"


def list_futures_bases(sess: requests.Session) -> set[str]:
    """Base coins that have a USDT-settled perpetual on MEXC futures.
    Used to restrict the spot scan to coins that also trade on futures."""
    last_err = None
    for attempt in range(3):
        try:
            r = sess.get(f"{FUTURES_BASE}/api/v1/contract/detail", timeout=30)
            r.raise_for_status()
            data = r.json().get("data", [])
            bases = {str(c.get("baseCoin", "")).upper()
                     for c in data
                     if c.get("quoteCoin") == "USDT" and not c.get("isHidden")}
            if bases:
                return bases
        except requests.RequestException as e:
            last_err = e
            time.sleep(0.8 * (attempt + 1))
    raise requests.RequestException(
        f"could not load MEXC futures contract list: {last_err}")


def list_symbols(sess: requests.Session, quote: str,
                 futures_only: bool = False) -> list[str]:
    r = sess.get(f"{BASE}/api/v3/exchangeInfo", timeout=30)
    r.raise_for_status()
    data = r.json()
    # When futures_only, keep only coins that also have a USDT perpetual.
    fut_bases = list_futures_bases(sess) if futures_only else None
    syms = []
    for s in data.get("symbols", []):
        if s.get("quoteAsset") != quote:
            continue
        # status is "1"/"ENABLED" for tradable spot symbols
        if str(s.get("status")) not in ("1", "ENABLED", "TRADING"):
            continue
        base = s.get("baseAsset", "")
        # skip leveraged tokens (3L/3S/5L/5S etc.) — noisy for this scan
        if base.endswith(("3L", "3S", "5L", "5S", "2L", "2S", "4L", "4S")):
            continue
        # skip stablecoin/pegged bases (no meaningful trend on a $1 peg)
        if base.upper() in STABLE_BASES:
            continue
        # skip TOKENIZED STOCKS / ETFs (crypto-only scan). MEXC now lists Ondo
        # tokenized equities on spot (e.g. TSLAON, AAPLON, NOCON, EFAON). They
        # all carry the tokenized-equity category tags conceptPlateIds 51 & 56,
        # which real crypto never has, so this is a precise, future-proof filter.
        # (A "(Ondo)" full name is a secondary backstop.)
        plates = set(s.get("conceptPlateIds") or [])
        if {51, 56} <= plates:
            continue
        if "(ondo)" in str(s.get("fullName", "")).lower():
            continue
        # keep only coins that also trade on MEXC USDT-perp futures
        if fut_bases is not None and base.upper() not in fut_bases:
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


def scan_symbol_multi(sess: requests.Session, symbol: str, interval: str,
                      cfg: dict) -> tuple[Hit | None, FlagHit | None]:
    """Fetch klines ONCE and run both the 200-EMA and bull-flag detectors.
    Used by the dashboard so a two-scan page still costs one request per symbol."""
    raw = fetch_klines(sess, symbol, interval, cfg["kline_limit"])
    if not raw or len(raw) < EMA_PERIOD + 2:
        return None, None
    rows = raw[:-1]                       # drop the still-forming candle
    try:
        highs = [float(x[2]) for x in rows]
        lows = [float(x[3]) for x in rows]
        closes = [float(x[4]) for x in rows]
        vols = [float(x[5]) for x in rows]
    except (ValueError, IndexError):
        return None, None

    ema_hit = None
    ok, d = detect_cross_and_retest(
        highs, lows, closes,
        ema_period=EMA_PERIOD, lookback=cfg["lookback"],
        retest_tol=cfg["retest_tol"], break_tol=cfg["break_tol"],
        max_above_now=cfg["max_above_now"], min_slope=cfg["min_slope"],
    )
    if ok:
        ema_hit = Hit(symbol=symbol, **d)

    flag_hit = None
    ok2, f = detect_bull_flag(
        highs, lows, closes, vols,
        pole_min_gain=cfg.get("pole_min_gain", 0.15),
        max_retrace=cfg.get("flag_max_retrace", 0.5),
    )
    if ok2:
        flag_hit = FlagHit(symbol=symbol, **f)

    return ema_hit, flag_hit


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
    p.add_argument("--include-spot-only", action="store_true",
                   help="also scan coins NOT listed on MEXC futures "
                        "(default: futures-listed coins only)")
    args = p.parse_args()

    cfg = {
        "kline_limit": args.kline_limit, "lookback": args.lookback,
        "retest_tol": args.retest_tol, "break_tol": args.break_tol,
        "max_above_now": args.max_above, "min_slope": args.min_slope,
    }

    sess = get_session()
    futures_only = not args.include_spot_only
    print(f"Fetching {args.quote} spot symbols from MEXC "
          f"({'futures-listed only' if futures_only else 'all spot'}) ...",
          file=sys.stderr)
    try:
        symbols = list_symbols(sess, args.quote, futures_only=futures_only)
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
