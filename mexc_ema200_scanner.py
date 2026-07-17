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
import os
import sys
import time

# Which MEXC universe to scan:
#   "crypto" (default) — normal crypto perps, tokenized stocks/commodities excluded
#   "tradfi"           — ONLY the tokenized stocks / ETFs / forex / metals / energy
#                        (gold, silver, oil, Apple, etc.) — the "stocks & commodities" site
#   "all"              — everything
SCAN_UNIVERSE = os.environ.get("SCAN_UNIVERSE", "crypto").strip().lower()
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


def atr(highs: list[float], lows: list[float], closes: list[float],
        period: int = 14) -> float:
    """Average True Range over the last `period` candles — a volatility measure
    used to size stop-loss buffers so they adapt to each coin instead of a flat %."""
    n = len(closes)
    if n < 2:
        return 0.0
    trs = []
    for i in range(1, n):
        trs.append(max(highs[i] - lows[i],
                       abs(highs[i] - closes[i - 1]),
                       abs(lows[i] - closes[i - 1])))
    recent = trs[-period:] if len(trs) >= period else trs
    return sum(recent) / len(recent) if recent else 0.0


def supertrend(highs: list[float], lows: list[float], closes: list[float],
               period: int = 10, mult: float = 3.0) -> tuple[float | None, str | None]:
    """Supertrend (ATR trend-following) — returns (line_value, direction) at the
    latest closed candle. direction 'up' = the line sits BELOW price and acts as a
    trailing SUPPORT; 'down' = it sits ABOVE price and acts as RESISTANCE."""
    n = len(closes)
    if n < period + 2:
        return None, None
    tr = [0.0] * n
    for i in range(1, n):
        tr[i] = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]),
                    abs(lows[i] - closes[i - 1]))
    atr_s: list[float | None] = [None] * n
    atr_s[period] = sum(tr[1:period + 1]) / period            # Wilder ATR seed
    for i in range(period + 1, n):
        atr_s[i] = (atr_s[i - 1] * (period - 1) + tr[i]) / period
    fu = [0.0] * n
    fl = [0.0] * n
    st = [0.0] * n
    d = [1] * n
    for i in range(period, n):
        hl2 = (highs[i] + lows[i]) / 2
        bu = hl2 + mult * atr_s[i]
        bl = hl2 - mult * atr_s[i]
        if i == period:
            fu[i], fl[i], st[i], d[i] = bu, bl, bl, 1
            continue
        fu[i] = bu if (bu < fu[i - 1] or closes[i - 1] > fu[i - 1]) else fu[i - 1]
        fl[i] = bl if (bl > fl[i - 1] or closes[i - 1] < fl[i - 1]) else fl[i - 1]
        if closes[i] > fu[i - 1]:
            d[i] = 1
        elif closes[i] < fl[i - 1]:
            d[i] = -1
        else:
            d[i] = d[i - 1]
        st[i] = fl[i] if d[i] == 1 else fu[i]
    return st[n - 1], ("up" if d[n - 1] == 1 else "down")


def rel_volume(volumes: list[float], lookback: int = 20) -> float | None:
    """Relative volume of the latest candle vs the prior `lookback`-candle average.
    >1 = the current bar is trading on above-average volume (confirmation)."""
    if len(volumes) < lookback + 2:
        return None
    base = volumes[-lookback - 1:-1]
    avg = sum(base) / len(base)
    return round(volumes[-1] / avg, 2) if avg > 0 else None


def rsi(closes: list[float], period: int = 14) -> float | None:
    """Relative Strength Index (0-100). <30 oversold, >70 overbought."""
    if len(closes) < period + 1:
        return None
    gains = losses = 0.0
    for i in range(len(closes) - period, len(closes)):
        ch = closes[i] - closes[i - 1]
        if ch >= 0:
            gains += ch
        else:
            losses -= ch
    avg_gain, avg_loss = gains / period, losses / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - 100 / (1 + rs), 1)


def rsi_series(closes: list[float], period: int = 14) -> list:
    """RSI at every candle (Wilder-smoothed), so we can compare RSI at successive
    price swings to spot divergences. First `period` entries are None (warm-up)."""
    n = len(closes)
    if n < period + 1:
        return [None] * n
    out = [None] * period
    gains = losses = 0.0
    for i in range(1, period + 1):
        ch = closes[i] - closes[i - 1]
        gains += ch if ch > 0 else 0.0
        losses += -ch if ch < 0 else 0.0
    ag, al = gains / period, losses / period

    def _r(ag, al):
        if al == 0:
            return 100.0
        return 100 - 100 / (1 + ag / al)

    out.append(round(_r(ag, al), 2))
    for i in range(period + 1, n):
        ch = closes[i] - closes[i - 1]
        ag = (ag * (period - 1) + (ch if ch > 0 else 0.0)) / period
        al = (al * (period - 1) + (-ch if ch < 0 else 0.0)) / period
        out.append(round(_r(ag, al), 2))
    return out


def swing_points(highs: list[float], lows: list[float], left: int = 3,
                 right: int = 3, window: int = 80):
    """Return (swing_highs, swing_lows) as lists of (index, price), most recent
    last — the pivots used for market-structure analysis."""
    n = len(highs)
    start = max(left, n - window)
    sh, sl = [], []
    for i in range(start, n - right):
        h = highs[i]
        if all(h >= highs[i - d] for d in range(1, left + 1)) and \
           all(h >= highs[i + d] for d in range(1, right + 1)):
            sh.append((i, h))
        l = lows[i]
        if all(l <= lows[i - d] for d in range(1, left + 1)) and \
           all(l <= lows[i + d] for d in range(1, right + 1)):
            sl.append((i, l))
    return sh, sl


def detect_rsi_divergence(highs, lows, closes, rsi_vals=None, window: int = 60,
                          recent: int = 18):
    """RSI divergence between the last two price swings — a leading momentum signal.
      • REGULAR bullish  = price lower-low  but RSI higher-low  → downtrend weakening (reversal up).
      • REGULAR bearish  = price higher-high but RSI lower-high → uptrend weakening (reversal down).
      • HIDDEN  bullish  = price higher-low  but RSI lower-low  → uptrend continuation (buy the dip).
      • HIDDEN  bearish  = price lower-high  but RSI higher-high→ downtrend continuation (sell the rip).
    Returns the most RECENT qualifying divergence as {kind, dir, label, note} or None."""
    if rsi_vals is None:
        rsi_vals = rsi_series(closes)
    n = len(closes)
    if n < 30 or not rsi_vals:
        return None
    sh, sl = swing_points(highs, lows, left=3, right=3, window=window)

    def _rsi_at(i):
        return rsi_vals[i] if 0 <= i < len(rsi_vals) and rsi_vals[i] is not None else None

    cands = []
    # --- lows → bullish signals ---
    if len(sl) >= 2:
        (i1, p1), (i2, p2) = sl[-2], sl[-1]
        r1, r2 = _rsi_at(i1), _rsi_at(i2)
        if r1 is not None and r2 is not None and (n - 1 - i2) <= recent and abs(r2 - r1) >= 3:
            if p2 < p1 * 0.999 and r2 > r1 + 2:
                cands.append((i2, {"kind": "regular_bull", "dir": "bullish",
                    "label": "Regular bullish RSI divergence",
                    "note": "price made a lower low but RSI made a higher low — selling momentum is fading, a reversal UP is likely."}))
            elif p2 > p1 * 1.001 and r2 < r1 - 2:
                cands.append((i2, {"kind": "hidden_bull", "dir": "bullish",
                    "label": "Hidden bullish RSI divergence",
                    "note": "price made a higher low while RSI made a lower low — an uptrend pullback that usually resumes UP (buy-the-dip continuation)."}))
    # --- highs → bearish signals ---
    if len(sh) >= 2:
        (i1, p1), (i2, p2) = sh[-2], sh[-1]
        r1, r2 = _rsi_at(i1), _rsi_at(i2)
        if r1 is not None and r2 is not None and (n - 1 - i2) <= recent and abs(r2 - r1) >= 3:
            if p2 > p1 * 1.001 and r2 < r1 - 2:
                cands.append((i2, {"kind": "regular_bear", "dir": "bearish",
                    "label": "Regular bearish RSI divergence",
                    "note": "price made a higher high but RSI made a lower high — buying momentum is fading, a reversal DOWN is likely."}))
            elif p2 < p1 * 0.999 and r2 > r1 + 2:
                cands.append((i2, {"kind": "hidden_bear", "dir": "bearish",
                    "label": "Hidden bearish RSI divergence",
                    "note": "price made a lower high while RSI made a higher high — a downtrend bounce that usually resumes DOWN (sell-the-rip continuation)."}))
    if not cands:
        return None
    cands.sort(key=lambda c: c[0])           # most recent second-pivot wins
    return cands[-1][1]


def market_structure(highs: list[float], lows: list[float],
                     closes: list[float]) -> dict:
    """Classify trend structure and detect a Change of Character (CHoCH).

    Uptrend = higher highs + higher lows; downtrend = lower highs + lower lows.
    A bullish CHoCH is when price (in a downtrend) closes above the last swing
    high — the first sign the character is flipping up; bearish is the mirror."""
    sh, sl = swing_points(highs, lows)
    price = closes[-1]
    structure = "range"
    if len(sh) >= 2 and len(sl) >= 2:
        hh = sh[-1][1] > sh[-2][1]
        hl = sl[-1][1] > sl[-2][1]
        lh = sh[-1][1] < sh[-2][1]
        ll = sl[-1][1] < sl[-2][1]
        if hh and hl:
            structure = "uptrend"
        elif lh and ll:
            structure = "downtrend"
    choch = None
    if sh and price > sh[-1][1] and structure != "uptrend":
        choch = "bullish"          # broke the last lower-high → flipping up
    elif sl and price < sl[-1][1] and structure != "downtrend":
        choch = "bearish"
    return {
        "structure": structure,
        "choch": choch,
        "last_swing_high": sh[-1][1] if sh else None,
        "last_swing_low": sl[-1][1] if sl else None,
    }


def volume_profile(volumes: list[float], closes: list[float],
                   lookback: int = 20) -> dict:
    """Recent volume vs its longer average, and whether up-candles or down-candles
    carry more volume (buy vs sell pressure)."""
    if len(volumes) < lookback + 5:
        return {"vol_trend": "n/a", "vol_ratio": None, "pressure": "n/a"}
    recent = volumes[-lookback:]
    base = volumes[-lookback * 3:-lookback] or volumes[:-lookback]
    ravg = sum(recent) / len(recent)
    bavg = (sum(base) / len(base)) if base else ravg
    ratio = round(ravg / bavg, 2) if bavg else None
    vtrend = "rising" if ratio and ratio > 1.15 else ("falling" if ratio and ratio < 0.85 else "steady")
    up = dn = 0.0
    for i in range(len(closes) - lookback, len(closes)):
        if i <= 0:
            continue
        if closes[i] >= closes[i - 1]:
            up += volumes[i]
        else:
            dn += volumes[i]
    pressure = "buyers" if up > dn * 1.15 else ("sellers" if dn > up * 1.15 else "balanced")
    return {"vol_trend": vtrend, "vol_ratio": ratio, "pressure": pressure}


def _line_slope(pivots):
    """Slope of a line through the first & last pivot (index, price)."""
    (i0, p0), (i1, p1) = pivots[0], pivots[-1]
    return (p1 - p0) / (i1 - i0) if i1 != i0 else None


def detect_chart_patterns(highs: list[float], lows: list[float],
                          closes: list[float], volumes: list[float], *,
                          window: int = 60) -> list[dict]:
    """Classify the chart formation(s) currently in play from swing-pivot geometry:
    wedges, triangles (ascending / descending / symmetrical = pennant), channels,
    rectangles, flags/pennants after a pole, and double tops/bottoms. Returns a list
    of {name, bias, note}, most salient first (empty if nothing clean)."""
    n = len(closes)
    if n < 25:
        return []
    last = n - 1
    price = closes[last] or 1.0
    sh, sl = swing_points(highs, lows, window=window)
    out: list[dict] = []

    def add(name, bias, note):
        out.append({"name": name, "bias": bias, "note": note})

    # --- Flag / pennant after a strong pole (momentum continuation) ---
    look = min(len(closes) - 1, 24)
    if look >= 6:
        seg = closes[-look:]
        lo_i = seg.index(min(seg))
        hi_i = seg.index(max(seg))
        run_up = (max(seg) - seg[0]) / seg[0] if seg[0] else 0
        run_dn = (seg[0] - min(seg)) / seg[0] if seg[0] else 0
        tail = closes[-5:]
        tail_range = (max(tail) - min(tail)) / price
        if run_up > 0.18 and hi_i < look - 2 and tail_range < 0.10 and price > seg[0] * 1.05:
            add("Bull flag / pennant", "bullish",
                "A strong up-impulse (pole) followed by a tight pullback — a continuation setup that tends to break upward.")
        elif run_dn > 0.18 and lo_i < look - 2 and tail_range < 0.10 and price < seg[0] * 0.95:
            add("Bear flag / pennant", "bearish",
                "A sharp drop (pole) followed by a shallow bounce — a continuation setup that tends to break downward.")

    # --- Trendline geometry: wedges / triangles / channels / rectangle ---
    if len(sh) >= 2 and len(sl) >= 2:
        ph = sh[-3:] if len(sh) >= 3 else sh[-2:]
        pl = sl[-3:] if len(sl) >= 3 else sl[-2:]
        us, ls = _line_slope(ph), _line_slope(pl)
        if us is not None and ls is not None:
            un, ln = us / price, ls / price               # per-bar % slopes
            flat = 0.0010
            w_start = ph[0][1] - pl[0][1]
            w_end = ph[-1][1] - pl[-1][1]
            converging = 0 < w_end < w_start * 0.92
            diverging = w_end > w_start * 1.12

            def cls(s):
                return "up" if s > flat else ("down" if s < -flat else "flat")
            uc, lc = cls(un), cls(ln)
            if uc == "down" and lc == "down" and converging:
                add("Falling wedge", "bullish",
                    "Both boundaries slope down and converge (highs falling faster) — a bullish reversal that usually breaks up.")
            elif uc == "up" and lc == "up" and converging:
                add("Rising wedge", "bearish",
                    "Both boundaries slope up and converge (lows rising faster) — a bearish reversal that usually breaks down.")
            elif uc == "flat" and lc == "up" and converging:
                add("Ascending triangle", "bullish",
                    "Flat resistance with rising support — buyers pressing up into a ceiling; bullish breakout bias.")
            elif uc == "down" and lc == "flat" and converging:
                add("Descending triangle", "bearish",
                    "Falling resistance on flat support — sellers pressing down onto a floor; bearish breakdown bias.")
            elif uc == "down" and lc == "up" and converging:
                add("Symmetrical triangle (pennant)", "neutral",
                    "Highs falling and lows rising into an apex — a coil that can break either way; trade the break.")
            elif uc == "up" and lc == "up" and not converging and not diverging:
                add("Ascending channel", "bullish",
                    "Parallel rising trendlines — an orderly uptrend; buy the lower rail, watch the upper.")
            elif uc == "down" and lc == "down" and not converging and not diverging:
                add("Descending channel", "bearish",
                    "Parallel falling trendlines — an orderly downtrend; sell the upper rail.")
            elif uc == "flat" and lc == "flat":
                add("Rectangle / range", "neutral",
                    "Flat highs and lows — sideways range between horizontal support and resistance.")
            elif diverging and uc == "up" and lc == "down":
                add("Broadening formation", "neutral",
                    "Widening highs and lows — rising volatility and indecision; unstable, prone to whipsaws.")

    # --- Double top / double bottom (fallbacks — only if nothing else matched) ---
    if not out and len(sh) >= 2:
        h1, h2 = sh[-2][1], sh[-1][1]
        if abs(h1 - h2) / max(h1, h2) < 0.03 and price < min(h1, h2) * 0.995:
            add("Double top", "bearish",
                "Two peaks at a similar level with price rolling over — a reversal pattern; a break of the middle trough confirms.")
    if not out and len(sl) >= 2:
        l1, l2 = sl[-2][1], sl[-1][1]
        if abs(l1 - l2) / max(l1, l2) < 0.03 and price > max(l1, l2) * 1.005:
            add("Double bottom", "bullish",
                "Two troughs at a similar level with price turning up — a reversal pattern; a break of the middle peak confirms.")

    return out[:3]


def primary_pattern(highs, lows, closes, volumes) -> dict | None:
    """The single most salient chart pattern (or None) — used for a compact badge."""
    pats = detect_chart_patterns(highs, lows, closes, volumes)
    return pats[0] if pats else None


# How "noteworthy" each formation is — reversal/continuation patterns rank above
# generic ranges, so the badge shows the most meaningful one across timeframes.
_PAT_SALIENCE = {
    "Falling wedge": 3, "Rising wedge": 3, "Ascending triangle": 3,
    "Descending triangle": 3, "Symmetrical triangle (pennant)": 3,
    "Bull flag / pennant": 3, "Bear flag / pennant": 3,
    "Double top": 2, "Double bottom": 2,
    "Ascending channel": 2, "Descending channel": 2,
    "Broadening formation": 1, "Rectangle / range": 1,
}
_TF_RANK = {"1W": 3, "1D": 2, "4h": 1, "1h": 0}


def primary_pattern_mtf(rows, highs, lows, closes, volumes) -> list[dict]:
    """Primary chart pattern on EACH timeframe (4h from the base candles, Daily and
    Weekly aggregated from them). Returns a list of {name,bias,note,tf}."""
    out = []
    p = primary_pattern(highs, lows, closes, volumes)
    if p:
        out.append({**p, "tf": "4h"})
    for gd, lbl in ((1, "1D"), (7, "1W")):
        h, l, c, v = _agg_series(rows, gd)
        if len(c) >= 30:
            pp = primary_pattern(h, l, c, v)
            if pp:
                out.append({**pp, "tf": lbl})
    return out


def best_pattern(pats: list[dict]) -> dict | None:
    """Most salient pattern across timeframes (ties → higher timeframe)."""
    if not pats:
        return None
    return max(pats, key=lambda p: (_PAT_SALIENCE.get(p.get("name"), 0),
                                    _TF_RANK.get(p.get("tf"), 0)))


def pct_returns(closes: list[float]) -> list[float]:
    """Bar-over-bar percentage returns — the series used for BTC correlation."""
    out = []
    for i in range(1, len(closes)):
        p = closes[i - 1]
        out.append((closes[i] - p) / p if p else 0.0)
    return out


def pearson(a: list[float], b: list[float], min_n: int = 20) -> float | None:
    """Pearson correlation of two aligned return series (most-recent aligned).
    Returns None if there isn't enough overlapping history."""
    n = min(len(a), len(b))
    if n < min_n:
        return None
    a, b = a[-n:], b[-n:]
    ma, mb = sum(a) / n, sum(b) / n
    num = sum((a[i] - ma) * (b[i] - mb) for i in range(n))
    da = (sum((x - ma) ** 2 for x in a)) ** 0.5
    db = (sum((x - mb) ** 2 for x in b)) ** 0.5
    if da == 0 or db == 0:
        return None
    return num / (da * db)


CORR_WINDOW = 60          # ~10 days of 4h bars for the BTC-correlation estimate


# ----------------------------------------------------------------------------
# Setup detection
# ----------------------------------------------------------------------------
# Scan results are plain dicts (each detector returns its own fields plus the
# shared level bundle from level_bundle(): sl_tight, sl_wide, tp1..tp5,
# rr1..rr5 [to tight stop], rrw1..rrw5 [to wide stop]).


def resistances_above(highs: list[float], upto: int, price: float,
                      left: int = 3, right: int = 3, window: int = 300,
                      max_n: int = 3, min_gap: float = 0.0) -> list[float]:
    """The nearest swing-high pivots ABOVE `price`, ascending — the overhead
    resistance ceilings price would run into, in order.

    A pivot high is a candle whose high is >= the `left` highs before it and the
    `right` highs after it. Levels closer than `min_gap` above price are skipped
    (trivially-close ceilings make for meaningless targets), and near-equal levels
    (within 0.5%) are merged. Returns up to `max_n` levels."""
    floor = price * (1.0 + min_gap)
    start = max(left, upto - window)
    res = []
    for i in range(start, upto - right + 1):
        h = highs[i]
        if all(h >= highs[i - d] for d in range(1, left + 1)) and \
           all(h >= highs[i + d] for d in range(1, right + 1)) and h > floor:
            res.append(h)
    res.sort()
    out: list[float] = []
    for h in res:
        if not out or h > out[-1] * 1.005:
            out.append(h)
        if len(out) >= max_n:
            break
    return out


def _apply_stop_floor(entry: float, sl_tight: float, sl_wide: float, atr=None,
                      plan_entry: float = None):
    """Enforce a volatility floor so a stop never sits inside normal noise.

    A 1%-away stop is far too tight for a high-conviction swing: it gets wicked
    out on ordinary chop AND it artificially inflates R:R (a tiny risk
    denominator makes any target look like a huge ratio). So we require the
    TIGHT stop to be at least max(1.5 x ATR, 2.5%) away from the fill.

    Crucially the distance is measured from the level the trade is actually
    entered at — `plan_entry` (the optimal / retest fill) when given, else the
    current price. This keeps the stop sensibly BELOW a long's pullback entry
    (or ABOVE a short's), instead of ending up on the wrong side of it.

    Structure-based stops that already clear that floor are left untouched; only
    the too-tight ones are pushed out. The wide stop is kept at least as far as
    the (possibly widened) tight stop."""
    if not entry or sl_tight is None:
        return sl_tight, sl_wide
    long_side = sl_tight < entry           # stop below entry => long
    # Reference the fill the trader actually uses. For a long that's the LOWER
    # of price / optimal-entry (a dip); for a short the HIGHER (a rally).
    ref = entry
    if plan_entry and plan_entry > 0:
        ref = min(entry, plan_entry) if long_side else max(entry, plan_entry)
    floor_dist = 0.025 * ref               # 2.5% hard minimum
    if atr:
        floor_dist = max(floor_dist, 1.5 * atr)
    if long_side:
        sl_tight = min(sl_tight, ref - floor_dist)
        sl_wide = sl_tight if sl_wide is None else min(sl_wide, sl_tight)
    else:
        sl_tight = max(sl_tight, ref + floor_dist)
        sl_wide = sl_tight if sl_wide is None else max(sl_wide, sl_tight)
    return round(sl_tight, 10), round(sl_wide, 10)


def level_bundle(entry: float, sl_tight: float, sl_wide: float,
                 tps: list, atr=None,
                 sl_tight_basis: str = None, sl_wide_basis: str = None,
                 plan_entry: float = None) -> dict:
    """Package two stop scenarios and up to 5 targets, with R:R (to the tight
    stop) for each. R:R = (TP - entry) / (entry - stop) = profit / loss.

    Each element of `tps` may be a bare number, or a `(level, basis)` /
    `(level, basis, is_ema)` tuple. `basis` is a short per-coin explanation of
    what that target IS (a swing high, a Fib extension, the 200-EMA reclaim …);
    `is_ema` flags the 200-EMA reclaim target so the UI can highlight it. The
    emitted dict carries tp{i}_basis and tp{i}_ema alongside each tp{i}.

    `sl_tight_basis` / `sl_wide_basis` are per-coin explanations of each stop.

    `atr` (if given) activates a volatility floor on the tight stop so thin
    stops can't inflate R:R — see _apply_stop_floor."""
    floored = _apply_stop_floor(entry, sl_tight, sl_wide, atr, plan_entry)
    sl_tight, sl_wide = floored
    def rr(tp, sl):
        # works for longs (tp/sl on the bullish side) and shorts (mirrored) —
        # the ratio is sign-invariant: profit/loss.
        return round((tp - entry) / (entry - sl), 2) if (tp and entry != sl) else None
    floor_note = (" It's held at least 1.5×ATR / 2.5% from entry so a "
                  "normal wick can't trip it.")
    st_basis = (sl_tight_basis or "The setup's immediate invalidation level, "
                "buffered by ~0.5×ATR.") + floor_note
    sw_basis = (sl_wide_basis or "A deeper structural level for more room, "
                "buffered by ~1×ATR.")
    out = {"sl_tight": sl_tight, "sl_wide": sl_wide,
           "sl_tight_basis": st_basis, "sl_wide_basis": sw_basis}
    norm = []
    for t in tps[:5]:
        if isinstance(t, (tuple, list)):
            lvl = t[0]
            basis = t[1] if len(t) > 1 else None
            is_ema = bool(t[2]) if len(t) > 2 else False
        else:
            lvl, basis, is_ema = t, None, False
        norm.append((lvl, basis, is_ema))
    padded = norm + [(None, None, False)] * (5 - len(norm))
    for i, (tp, basis, is_ema) in enumerate(padded, 1):
        out[f"tp{i}"] = tp
        out[f"tp{i}_basis"] = basis
        out[f"tp{i}_ema"] = is_ema
        out[f"rr{i}"] = rr(tp, sl_tight)
        out[f"rrw{i}"] = rr(tp, sl_wide)   # R:R against the wider stop
    return out


def _res_targets(levels, kind: str = "a prior swing high the move must clear"):
    """Wrap overhead-resistance target levels with a per-coin basis string."""
    return [(r, f"Overhead resistance at {r:.6g} — {kind}.", False) for r in levels]


def _sup_targets(levels, kind: str = "a prior swing low (downside target)"):
    """Wrap support target levels (for shorts) with a per-coin basis string."""
    return [(s, f"Support at {s:.6g} — {kind}.", False) for s in levels]


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

    # EMA slope over a fixed recent window (trend filter, independent of how long
    # ago the reclaim was).
    j = max(ema_period - 1, last - 20)
    if e[j] is None or e[j] == 0:
        return False, {}
    slope = (ema_now - e[j]) / e[j]
    if slope < min_slope:
        return False, {}

    # Find the reclaim that STARTED the current run above the EMA — the most
    # recent close-below -> close-at/above cross, searching all the way back (not
    # capped to a fixed window). This keeps a reclaim on the board for as long as
    # price holds above the EMA, rather than expiring after N candles.
    cross_idx = None
    for i in range(last, ema_period, -1):
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
    a = atr(highs, lows, closes)
    # Optimal entry: the lower-risk fill is a pullback that retests the reclaimed
    # EMA, so the ideal entry sits at/just above the EMA rather than chasing.
    optimal_entry = round(ema_now * 1.002, 10)
    # Sensible, volatility-aware stops (ATR buffer below real structure):
    #  - tight: just under the retest low / EMA (whichever is lower) - 0.5 ATR
    #  - wide:  under the deeper swing low of the whole move            - 1.0 ATR
    tight_struct = min(min(lows[retest_idx:last + 1]), ema_now)
    wide_struct = min(lows[cross_idx:last + 1])
    sl_tight = tight_struct - 0.5 * a
    sl_wide = min(wide_struct - 1.0 * a, sl_tight)
    # Up to 5 overhead resistances (skip trivially-close ceilings) as targets.
    res = resistances_above(highs, last, entry, max_n=5, min_gap=0.008)
    bundle = level_bundle(
        entry, sl_tight, sl_wide, _res_targets(res), atr=a, plan_entry=optimal_entry,
        sl_tight_basis=(f"Just below the reclaimed 200 EMA / retest low at "
                        f"{tight_struct:.6g}, buffered by ~0.5×ATR. A close back "
                        f"below the EMA voids the reclaim."),
        sl_wide_basis=(f"Below the deeper swing low of the whole reclaim move at "
                       f"{wide_struct:.6g}, ~1×ATR buffer."))

    return True, {
        "price": close_now,
        "ema": ema_now,
        "pct_above_ema": round(pct_above * 100, 2),
        "bars_since_cross": bars_since_cross,
        "fresh": bars_since_cross <= 6,          # reclaimed within ~24h (6× 4h bars)
        "retest_gap_pct": round(best_gap * 100, 2),
        "score": score,
        "entry": entry,
        "optimal_entry": optimal_entry,
        **bundle,
    }


# ----------------------------------------------------------------------------
# Bull-flag detection
# ----------------------------------------------------------------------------
def detect_bull_flag(
    highs: list[float], lows: list[float], closes: list[float],
    volumes: list[float], *,
    pole_min_gain: float = 0.15, pole_max_len: int = 8,
    flag_min_bars: int = 3, flag_max_bars: int = 20,
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
    a = atr(highs, lows, closes)
    pole_height = pole_top - pole_low
    # Optimal entry for a continuation is the break of the flag high (enter on
    # strength); a pullback to the flag low is the alternative lower-risk fill.
    optimal_entry = round(breakout * 1.001, 10)
    # Stops (ATR-buffered below real structure):
    #  - tight: just under the flag low          - 0.5 ATR
    #  - wide:  under the whole pole's base       - 0.5 ATR (much more room)
    sl_tight = flag_lo - 0.5 * a
    sl_wide = min(pole_low - 0.5 * a, sl_tight)
    # Five continuation targets: measured move then Fib extensions of the pole,
    # so proper breakouts have higher targets to run to.
    _mults = (1.0, 1.618, 2.0, 2.618, 3.0)
    tps = [(breakout + m * pole_height,
            (f"Measured move — the flag breakout at {breakout:.6g} plus 1× the "
             f"pole height ({pole_height:.6g}), the classic flag projection."
             if m == 1.0 else
             f"{m:g}× Fibonacci extension of the pole projected from the breakout."),
            False) for m in _mults]
    bundle = level_bundle(
        entry, sl_tight, sl_wide, tps, atr=a, plan_entry=optimal_entry,
        sl_tight_basis=(f"Just below the flag low at {flag_lo:.6g}, buffered by "
                        f"~0.5×ATR. A close below the flag breaks the pattern."),
        sl_wide_basis=(f"Below the pole's base at {pole_low:.6g} — full "
                       f"invalidation of the flag structure."))

    return True, {
        "price": close_now,
        "pole_gain_pct": round(pole_gain * 100, 1),
        "flag_bars": flag_bars,
        "pullback_pct": round(retrace * 100, 1),
        "vol_contraction": round(vol_contraction, 2),
        "breakout": breakout,
        "entry": entry,
        "optimal_entry": optimal_entry,
        "score": round(score, 1),
        **bundle,
    }


# ----------------------------------------------------------------------------
# Narrow CPR (Central Pivot Range) detection
# ----------------------------------------------------------------------------
def detect_narrow_cpr(rows: list, *, cpr_max_width_pct: float = 0.75,
                      max_ext_above: float = 0.08) -> tuple[bool, dict]:
    """Narrow Central Pivot Range on the DAILY frame (from 4h candles).

    CPR uses the previous day's High/Low/Close:
        Pivot P = (H+L+C)/3,  BC = (H+L)/2,  TC = 2P - BC.
    A NARROW CPR (small TC-BC relative to price) signals compressed, coiled
    price — often a precursor to a trending / breakout move. We flag coins whose
    latest CPR is narrow and price is at/above it (bullish side), and attach a
    trade plan (entry, two stops, five targets).
    """
    if len(rows) < 30:
        return False, {}
    try:
        times = [int(float(r[0])) for r in rows]
        highs = [float(r[2]) for r in rows]
        lows = [float(r[3]) for r in rows]
        closes = [float(r[4]) for r in rows]
    except (ValueError, IndexError):
        return False, {}

    # group candle indices by UTC day
    days: dict[int, list[int]] = {}
    for i, t in enumerate(times):
        days.setdefault(t // 86400000, []).append(i)
    keys = list(days.keys())
    if len(keys) < 2:
        return False, {}

    prev = days[keys[-2]]                      # last fully-closed day = "yesterday"
    dh = max(highs[i] for i in prev)
    dl = min(lows[i] for i in prev)
    dc = closes[prev[-1]]
    P = (dh + dl + dc) / 3.0
    BC = (dh + dl) / 2.0
    TC = 2 * P - BC
    top, bot = max(TC, BC), min(TC, BC)
    width = top - bot
    price = closes[-1]
    if price <= 0:
        return False, {}
    width_pct = width / price * 100.0
    if width_pct > cpr_max_width_pct:
        return False, {}

    if price > top:
        position = "above"
    elif price < bot:
        position = "below"
    else:
        position = "inside"
    if position == "below":                    # broken down — not a long setup
        return False, {}
    if price > top * (1.0 + max_ext_above):    # already flown too far past CPR
        return False, {}

    ref = closes[max(0, len(closes) - 31)]
    slope = (price - ref) / ref if ref else 0.0
    trend = "up" if slope > 0.01 else ("down" if slope < -0.01 else "flat")

    narrow_s = max(0.0, min(1.0, 1.0 - width_pct / cpr_max_width_pct))
    pos_s = 1.0 if position == "above" else 0.5
    trend_s = 1.0 if trend == "up" else (0.5 if trend == "flat" else 0.0)
    score = round(100 * (0.5 * narrow_s + 0.3 * pos_s + 0.2 * trend_s), 1)

    entry = price
    a = atr(highs, lows, closes)
    optimal_entry = round(top * 1.001, 10)     # break above the CPR top
    sl_tight = BC - 0.5 * a                     # below the CPR bottom
    sl_wide = min(dl - 0.5 * a, sl_tight)       # below yesterday's low
    res = resistances_above(highs, len(closes) - 1, entry, max_n=5, min_gap=0.005)
    bundle = level_bundle(
        entry, sl_tight, sl_wide, _res_targets(res), atr=a, plan_entry=optimal_entry,
        sl_tight_basis=(f"Below the CPR bottom (BC) at {BC:.6g}, buffered by "
                        f"~0.5×ATR — losing the pivot range breaks the setup."),
        sl_wide_basis=(f"Below yesterday's low at {dl:.6g} — the full daily range "
                       f"is void."))

    return True, {
        "price": price,
        "pivot": P, "tc": TC, "bc": BC,
        "cpr_width_pct": round(width_pct, 3),
        "position": position, "trend": trend,
        "entry": entry, "optimal_entry": optimal_entry,
        "score": score,
        **bundle,
    }


# ----------------------------------------------------------------------------
# Support-bounce detection (multi-timeframe: 4h / 1D / 1W)
# ----------------------------------------------------------------------------
def htf_swing_lows(rows: list, group_days: int, left: int = 2, right: int = 2,
                   window: int = 60) -> list[float]:
    """Aggregate 4h candles into a higher timeframe (group_days=1 -> daily,
    7 -> weekly) by UTC period and return that timeframe's swing-low pivots."""
    buckets: dict[int, list] = {}
    span = 86400000 * group_days
    for r in rows:
        try:
            buckets.setdefault(int(float(r[0])) // span, []).append(r)
        except (ValueError, IndexError):
            continue
    keys = sorted(buckets)
    L = [min(float(x[3]) for x in buckets[k]) for k in keys]
    if len(L) < left + right + 1:
        return []
    out = []
    start = max(left, len(L) - window)
    for i in range(start, len(L) - right):
        if all(L[i] <= L[i - d] for d in range(1, left + 1)) and \
           all(L[i] <= L[i + d] for d in range(1, right + 1)):
            out.append(L[i])
    return out


def htf_swing_highs(rows: list, group_days: int, left: int = 2, right: int = 2,
                    window: int = 60) -> list[float]:
    """Higher-timeframe swing-HIGH pivots (daily=1, weekly=7) — resistances."""
    buckets: dict[int, list] = {}
    span = 86400000 * group_days
    for r in rows:
        try:
            buckets.setdefault(int(float(r[0])) // span, []).append(r)
        except (ValueError, IndexError):
            continue
    keys = sorted(buckets)
    H = [max(float(x[2]) for x in buckets[k]) for k in keys]
    if len(H) < left + right + 1:
        return []
    out = []
    start = max(left, len(H) - window)
    for i in range(start, len(H) - right):
        if all(H[i] >= H[i - d] for d in range(1, left + 1)) and \
           all(H[i] >= H[i + d] for d in range(1, right + 1)):
            out.append(H[i])
    return out


def key_resistances(rows: list, highs: list[float], price: float) -> dict:
    """Nearest resistance ABOVE price on each timeframe (4h / daily / weekly)."""
    r4 = resistances_above(highs, len(highs) - 1, price, max_n=1, min_gap=0.003)
    dh = [p for p in htf_swing_highs(rows, 1) if p > price]
    wh = [p for p in htf_swing_highs(rows, 7) if p > price]
    return {
        "res_4h": r4[0] if r4 else None,
        "res_1d": min(dh) if dh else None,
        "res_1w": min(wh) if wh else None,
    }


def key_supports(rows: list, lows: list[float], price: float) -> dict:
    """Nearest support BELOW price on each timeframe: 4h swing low, daily swing
    low, weekly swing low — the safety nets under a trade."""
    s4 = supports_below(lows, len(lows) - 1, price, max_n=1, min_gap=0.003)
    dl = [p for p in htf_swing_lows(rows, 1) if p < price]
    wl = [p for p in htf_swing_lows(rows, 7) if p < price]
    return {
        "sup_4h": s4[0] if s4 else None,
        "sup_1d": max(dl) if dl else None,
        "sup_1w": max(wl) if wl else None,
    }


def tf_levels_for(sess, symbol: str, price: float, cfg: dict) -> dict:
    """Nearest swing-low support BELOW price and swing-high resistance ABOVE price
    on EACH timeframe (1h / 4h / 1d / 1w), each computed from that timeframe's own
    candles. This is what lets the entry engine reason across timeframes: a support
    confirmed on the Daily/Weekly chart is STRONGER and gets hit FIRST on a pullback,
    so price usually turns there before reaching a deeper lower-timeframe level — and
    a smart long should buy that higher-timeframe level rather than a fantasy dip
    below it that may never fill. Returns {tf: {sup, res}} with None where a coin
    lacks history (common on weekly for newer alts)."""
    mkt = cfg.get("market", "futures")
    out: dict[str, dict] = {}
    for tf in ("5m", "15m", "1h", "4h", "1d", "1w"):
        rr = fetch_candles(sess, symbol, tf, 400, mkt)
        if not rr or len(rr) < 30:
            out[tf] = {"sup": None, "res": None}
            continue
        rws = rr[:-1]
        try:
            H = [float(x[2]) for x in rws]
            L = [float(x[3]) for x in rws]
        except (ValueError, IndexError):
            out[tf] = {"sup": None, "res": None}
            continue
        n = len(L) - 1
        sdn = supports_below(L, n, price, max_n=1, min_gap=0.004)
        rup = resistances_above(H, n, price, max_n=1, min_gap=0.004)
        out[tf] = {"sup": (sdn[0] if sdn else None),
                   "res": (rup[0] if rup else None)}
    return out


def detect_support_bounce(rows: list, highs: list[float], lows: list[float],
                          closes: list[float], volumes: list[float], *,
                          zone_tol: float = 0.015, touch_tol: float = 0.015,
                          recent: int = 5, min_touches: int = 2,
                          window: int = 90) -> tuple[bool, dict]:
    """Coin bouncing off a strong horizontal support, considering 4h, DAILY and
    WEEKLY supports. Swing lows from all three timeframes are clustered into
    zones; a level confirmed on a higher timeframe (or tested repeatedly) is
    stronger. Flags where price has just tagged the nearest strong zone and is
    turning back up."""
    n = len(closes)
    if n < window:
        return False, {}
    last = n - 1
    price = closes[last]

    _, sl4 = swing_points(highs, lows, window=window)
    tagged_lows = [("4h", p) for _, p in sl4]
    tagged_lows += [("1d", p) for p in htf_swing_lows(rows, 1)]
    tagged_lows += [("1w", p) for p in htf_swing_lows(rows, 7)]
    if len(tagged_lows) < 2:
        return False, {}

    rank = {"4h": 0, "1d": 1, "1w": 2}
    zones: list[dict] = []
    for tf, p in tagged_lows:
        for z in zones:
            if abs(p - z["level"]) / z["level"] <= zone_tol:
                z["prices"].append(p)
                z["level"] = sum(z["prices"]) / len(z["prices"])
                z["tfs"].add(tf)
                break
        else:
            zones.append({"level": p, "prices": [p], "tfs": {tf}})

    def best_tf(z):
        return max(z["tfs"], key=lambda t: rank[t])

    # A zone qualifies if tested 2+ times OR confirmed on a higher timeframe.
    cands = [z for z in zones if z["level"] <= price * 1.005 and
             (len(z["prices"]) >= min_touches or z["tfs"] & {"1d", "1w"})]
    if not cands:
        cands = [z for z in zones if z["level"] <= price * 1.005]
    if not cands:
        return False, {}
    support = max(cands, key=lambda z: z["level"])   # nearest support below price
    slevel = support["level"]
    touches = len(support["prices"])
    tf = best_tf(support)

    tagged = any(lows[i] <= slevel * (1 + touch_tol) and
                 lows[i] >= slevel * (1 - 2 * touch_tol)
                 for i in range(max(0, last - recent + 1), last + 1))
    if not tagged:
        return False, {}
    if price < slevel * (1 - 0.01):                  # decisively broke support
        return False, {}

    turning = closes[last] > closes[last - 1] or (
        last - 2 >= 0 and closes[last] > closes[last - 2])
    r14 = rsi(closes) or 50.0
    res = resistances_above(highs, last, price, max_n=5, min_gap=0.005)
    dist = abs(price - slevel) / price

    tf_s = {"1w": 1.0, "1d": 0.7, "4h": 0.4}[tf]     # higher timeframe = stronger
    touch_s = min(1.0, (touches - 1) / 3.0)
    fresh_s = 1.0 if dist < 0.03 else max(0.0, 1.0 - dist / 0.08)
    room_s = min(1.0, ((res[-1] - price) / price) / 0.30) if res else 0.0
    rsi_s = 1.0 if r14 < 45 else (0.5 if r14 < 60 else 0.0)
    turn_s = 1.0 if turning else 0.3
    score = round(100 * (0.22 * tf_s + 0.22 * touch_s + 0.2 * fresh_s +
                         0.18 * room_s + 0.1 * rsi_s + 0.08 * turn_s), 1)

    entry = price
    a = atr(highs, lows, closes)
    optimal_entry = round(slevel * 1.005, 10)        # buy near the support
    sl_tight = slevel - 0.5 * a
    lower = [z["level"] for z in zones if z["level"] < slevel * 0.99]
    wide_anchor = max(lower) if lower else slevel
    sl_wide = min(wide_anchor - 1.0 * a, sl_tight)
    bundle = level_bundle(
        entry, sl_tight, sl_wide, _res_targets(res), atr=a, plan_entry=optimal_entry,
        sl_tight_basis=(f"Just below the {tf} swing-low support zone at "
                        f"{slevel:.6g} (the level being defended), ~0.5×ATR "
                        f"buffer. A close below means support failed."),
        sl_wide_basis=(f"Below the next support zone down at {wide_anchor:.6g}, "
                       f"~1×ATR buffer."))

    tf_label = {"1w": "Weekly", "1d": "Daily", "4h": "4h"}[tf]
    ms = market_structure(highs, lows, closes)
    if ms["choch"] == "bullish":
        bias = "Bullish CHoCH"
    elif ms["structure"] == "uptrend":
        bias = "Bullish"
    elif ms["structure"] == "downtrend":
        bias = "Bearish"
    else:
        bias = "Range"
    return True, {
        "price": price,
        "support": slevel,
        "tf": tf_label,
        "method": "swing-low pivot zone",   # how the support level was identified
        "bias": bias,
        "choch": ms["choch"],
        "touches": touches,
        "dist_to_support_pct": round(dist * 100, 2),
        "rsi": r14,
        "turning_up": bool(turning),
        "entry": entry,
        "optimal_entry": optimal_entry,
        "score": score,
        **bundle,
    }


# ----------------------------------------------------------------------------
# Falling-wedge detection (bullish reversal) — multi-timeframe
# ----------------------------------------------------------------------------
def _agg_series(rows: list, group_days: int):
    """Aggregate 4h rows into a higher timeframe OHLCV (group_days=1 daily,
    7 weekly) by UTC period. Returns (highs, lows, closes, volumes)."""
    buckets: dict[int, list] = {}
    span = 86400000 * group_days
    for r in rows:
        try:
            buckets.setdefault(int(float(r[0])) // span, []).append(r)
        except (ValueError, IndexError):
            continue
    keys = sorted(buckets)
    highs = [max(float(x[2]) for x in buckets[k]) for k in keys]
    lows = [min(float(x[3]) for x in buckets[k]) for k in keys]
    closes = [float(buckets[k][-1][4]) for k in keys]
    vols = [sum(float(x[5]) for x in buckets[k]) for k in keys]
    return highs, lows, closes, vols


def detect_falling_wedge(highs: list[float], lows: list[float],
                         closes: list[float], volumes: list[float], *,
                         window: int = 60, near_tol: float = 0.03,
                         min_conv: float = 0.15) -> tuple[bool, dict]:
    """Falling wedge: a bullish reversal where price coils inside two DOWN-sloping,
    CONVERGING trendlines (lower highs + lower lows, highs falling faster). The
    setup fires when price is coiling near the apex or has just broken out above
    the upper (descending resistance) line. Returns (is_hit, details)."""
    n = len(closes)
    if n < 30:
        return False, {}
    last = n - 1
    price = closes[last]
    sh, sl = swing_points(highs, lows, window=window)
    if len(sh) < 2 or len(sl) < 2:
        return False, {}
    ph, pl = sh[-3:], sl[-3:]

    def line(pivots):
        (i0, p0), (i1, p1) = pivots[0], pivots[-1]
        if i1 == i0:
            return None
        slope = (p1 - p0) / (i1 - i0)
        return slope, p0 - slope * i0            # value(idx) = slope*idx + b

    up, lo = line(ph), line(pl)
    if not up or not lo:
        return False, {}
    us, ub = up
    ls, lb = lo
    # Both lines must fall, and price must make lower highs AND lower lows.
    if us >= 0 or ls >= 0:
        return False, {}
    if ph[-1][1] >= ph[0][1] or pl[-1][1] >= pl[0][1]:
        return False, {}

    start_idx = min(ph[0][0], pl[0][0])
    upper_now = us * last + ub
    lower_now = ls * last + lb
    w_start = (us * start_idx + ub) - (ls * start_idx + lb)
    w_now = upper_now - lower_now
    if w_start <= 0 or w_now <= 0 or w_now >= w_start:
        return False, {}                          # must be converging
    conv = 1.0 - w_now / w_start                  # 0..1, higher = tighter apex
    if conv < min_conv:
        return False, {}

    broke = price > upper_now
    near = lower_now * (1 - near_tol) <= price <= upper_now * (1 + near_tol)
    if not (broke or near):
        return False, {}

    touches = len(ph) + len(pl)
    rv = rel_volume(volumes) or 1.0
    vol_s = min(1.0, max(0.0, (rv - 1.0) / 1.5)) if broke else 0.3
    apex_gap = abs(price - upper_now) / price
    apex_s = max(0.0, 1.0 - apex_gap / 0.06)
    touch_s = min(1.0, (touches - 4) / 4.0) if touches > 4 else 0.3
    score = round(100 * (0.4 * conv + 0.25 * apex_s +
                         0.2 * vol_s + 0.15 * touch_s), 1)
    if broke:
        score = round(min(100.0, score + 8), 1)   # confirmed breakout nudge

    entry = price
    a = atr(highs, lows, closes)
    wedge_lo = min(lows[start_idx:last + 1])
    optimal_entry = round(upper_now, 10)          # ideal fill: retest of the line
    sl_tight = min(lower_now, wedge_lo) - 0.5 * a
    sl_wide = wedge_lo - 1.0 * a
    if sl_wide > sl_tight:
        sl_wide = sl_tight
    mm = upper_now + w_start                       # measured move = wedge height
    res = resistances_above(highs, last, entry, max_n=4, min_gap=0.008)
    labelled = ([(upper_now * 1.002,
                  f"The wedge's upper trendline at {upper_now:.6g} — the breakout "
                  f"level itself, first objective.", False)]
                + _res_targets(res, "a prior swing high above the wedge")
                + [(mm, f"Measured move at {mm:.6g} — breakout plus the wedge's "
                        f"height ({w_start:.6g}), the classic wedge target.", False)])
    labelled = [t for t in labelled if t[0] > price]
    seen, dedup = set(), []
    for t in sorted(labelled, key=lambda x: x[0]):
        k = round(t[0], 8)
        if k not in seen:
            seen.add(k)
            dedup.append(t)
    bundle = level_bundle(
        entry, sl_tight, sl_wide, dedup[:5], atr=a, plan_entry=optimal_entry,
        sl_tight_basis=(f"Just below the wedge's lower line / recent low at "
                        f"{min(lower_now, wedge_lo):.6g}, ~0.5×ATR buffer. A close "
                        f"below breaks the wedge."),
        sl_wide_basis=(f"Below the wedge low at {wedge_lo:.6g} — full pattern "
                       f"invalidation."))
    ms = market_structure(highs, lows, closes)
    return True, {
        "price": price,
        "pattern": "falling_wedge",
        "broken_out": bool(broke),
        "upper": upper_now,
        "lower": lower_now,
        "conv_pct": round(conv * 100, 1),
        "touches": touches,
        "rvol": round(rv, 2),
        "choch": ms["choch"],
        "entry": entry,
        "optimal_entry": optimal_entry,
        "score": score,
        **bundle,
    }


def detect_falling_wedge_mtf(rows: list, highs: list[float], lows: list[float],
                             closes: list[float], volumes: list[float]
                             ) -> tuple[bool, dict]:
    """Run the falling-wedge check on 4h, DAILY and WEEKLY series and return the
    strongest hit, tagged with its timeframe."""
    best = None
    frames = [("4h", highs, lows, closes, volumes)]
    for gd, lbl in ((1, "Daily"), (7, "Weekly")):
        h, l, c, v = _agg_series(rows, gd)
        if len(c) >= 30:
            frames.append((lbl, h, l, c, v))
    for lbl, h, l, c, v in frames:
        ok, d = detect_falling_wedge(h, l, c, v)
        if ok:
            d = {**d, "tf": lbl}
            # Prefer a confirmed breakout, then higher score.
            key = (1 if d["broken_out"] else 0, d["score"])
            if best is None or key > best[0]:
                best = (key, d)
    return (True, best[1]) if best else (False, {})


# ----------------------------------------------------------------------------
# Breakdown-and-retest (bearish mirror of the 200-EMA reclaim) — SHORTS
# ----------------------------------------------------------------------------
def detect_breakdown_and_retest(
    highs: list[float], lows: list[float], closes: list[float],
    *, ema_period: int = EMA_PERIOD, lookback: int = LOOKBACK,
    retest_tol: float = RETEST_TOL, break_tol: float = BREAK_TOL,
    max_below_now: float = MAX_ABOVE_NOW, max_slope: float = -MIN_SLOPE,
) -> tuple[bool, dict]:
    """Bearish mirror of cross-and-retest: price breaks DOWN through the 200 EMA,
    then retests it FROM BELOW (EMA acting as resistance) and rolls over — a short
    setup. Stops sit ABOVE the EMA; targets are supports below."""
    e = ema(closes, ema_period)
    n = len(closes)
    if n < ema_period + 2 or e[-1] is None:
        return False, {}
    last = n - 1
    ema_now = e[last]
    close_now = closes[last]
    if close_now > ema_now:                        # must be below the EMA now
        return False, {}
    pct_below = (ema_now - close_now) / ema_now
    if pct_below > max_below_now:                  # too far below = overextended
        return False, {}

    j = max(ema_period - 1, last - 20)
    if e[j] is None or e[j] == 0:
        return False, {}
    slope = (ema_now - e[j]) / e[j]
    if slope > max_slope:                          # EMA must be falling
        return False, {}

    # Most recent close-above -> close-below cross (the breakdown that started
    # the current run below the EMA), searching back unbounded.
    cross_idx = None
    for i in range(last, ema_period, -1):
        if e[i] is None or e[i - 1] is None:
            continue
        if closes[i - 1] > e[i - 1] and closes[i] <= e[i]:
            cross_idx = i
            break
    if cross_idx is None:
        return False, {}

    # After the breakdown, price must not have closed decisively back above.
    for i in range(cross_idx, last + 1):
        if e[i] is None:
            continue
        if closes[i] > e[i] * (1.0 + break_tol):
            return False, {}

    # Retest from below: a HIGH tags the EMA band while the candle CLOSES at/below
    # the EMA (held as resistance). Keep the cleanest tag.
    best_gap = None
    retest_idx = None
    for i in range(cross_idx + 1, last + 1):
        if e[i] is None:
            continue
        high_gap = (highs[i] - e[i]) / e[i]
        if abs(high_gap) <= retest_tol and closes[i] <= e[i]:
            if best_gap is None or abs(high_gap) < abs(best_gap):
                best_gap = high_gap
                retest_idx = i
    if retest_idx is None:
        return False, {}

    bars_since_cross = last - cross_idx
    freshness = max(0.0, 1.0 - bars_since_cross / lookback)
    tightness = max(0.0, min(1.0, 1.0 - abs(best_gap) / retest_tol))
    proximity = max(0.0, min(1.0, 1.0 - pct_below / max_below_now))
    score = round(100 * (0.45 * tightness + 0.35 * freshness + 0.20 * proximity), 1)

    entry = close_now
    a = atr(highs, lows, closes)
    optimal_entry = round(ema_now * 0.998, 10)     # ideal short: rally to the EMA
    tight_struct = max(max(highs[retest_idx:last + 1]), ema_now)
    wide_struct = max(highs[cross_idx:last + 1])
    sl_tight = tight_struct + 0.5 * a              # stop ABOVE structure
    sl_wide = max(wide_struct + 1.0 * a, sl_tight)
    sup = supports_below(lows, last, entry, max_n=5, min_gap=0.008)
    bundle = level_bundle(   # rr sign-invariant (mirrored for shorts)
        entry, sl_tight, sl_wide,
        _sup_targets(sup, "a prior swing low — a downside cover target"), atr=a, plan_entry=optimal_entry,
        sl_tight_basis=(f"Just ABOVE the rejected 200 EMA / retest high at "
                        f"{tight_struct:.6g}, ~0.5×ATR buffer. A close back above "
                        f"invalidates the short."),
        sl_wide_basis=(f"Above the deeper swing high of the move at "
                       f"{wide_struct:.6g}, ~1×ATR buffer."))
    return True, {
        "price": close_now,
        "ema": ema_now,
        "pct_below_ema": round(pct_below * 100, 2),
        "bars_since_cross": bars_since_cross,
        "fresh": bars_since_cross <= 6,
        "retest_gap_pct": round(best_gap * 100, 2),
        "side": "short",
        "score": score,
        "entry": entry,
        "optimal_entry": optimal_entry,
        **bundle,
    }


# ----------------------------------------------------------------------------
# Supertrend support bounce — bouncing off the Supertrend line as support,
# considered across 4h / Daily / Weekly.
# ----------------------------------------------------------------------------
def detect_supertrend_bounce(rows: list, highs: list[float], lows: list[float],
                             closes: list[float], volumes: list[float], *,
                             near_tol: float = 0.04, recent: int = 6
                             ) -> tuple[bool, dict]:
    """Price bouncing off its Supertrend line used as SUPPORT, checked on 4h,
    DAILY and WEEKLY. On each timeframe where Supertrend is in 'up' mode the line
    sits below price and acts as a trailing support; the setup fires when price is
    holding just above the nearest such line, has recently tagged it, and is
    turning back up. A higher-timeframe Supertrend support is stronger."""
    n = len(closes)
    if n < 60:
        return False, {}
    last = n - 1
    price = closes[last]

    frames = [("4h", highs, lows, closes)]
    for gd, lbl in ((1, "Daily"), (7, "Weekly")):
        h, l, c, _v = _agg_series(rows, gd)
        if len(c) >= 15:
            frames.append((lbl, h, l, c))

    rank = {"4h": 0, "Daily": 1, "Weekly": 2}
    tf_strength = {"4h": 0.45, "Daily": 0.72, "Weekly": 1.0}
    up_count = 0
    supports = []                       # (tf_label, st_value) where ST is a support
    for lbl, h, l, c in frames:
        v, d = supertrend(h, l, c)
        if v is None:
            continue
        if d == "up":
            up_count += 1
            if v <= price * 1.005:      # line sits at/below price → support
                supports.append((lbl, v))
    if not supports:
        return False, {}

    # Nearest Supertrend support below price (highest value ≤ price); ties → higher TF.
    slabel, slevel = max(supports, key=lambda x: (x[1], rank[x[0]]))
    dist = (price - slevel) / price
    if dist > near_tol:                 # too far above the line = not a fresh bounce
        return False, {}
    if price < slevel * (1 - 0.005):    # closed below the Supertrend support = broken
        return False, {}

    tagged = any(lows[i] <= slevel * (1 + 0.012)
                 for i in range(max(0, last - recent + 1), last + 1))
    turning = closes[last] > closes[last - 1] or (
        last - 2 >= 0 and closes[last] > closes[last - 2])
    r14 = rsi(closes) or 50.0
    res = resistances_above(highs, last, price, max_n=5, min_gap=0.005)

    tf_s = tf_strength[slabel]
    fresh_s = 1.0 if dist < 0.01 else max(0.0, 1.0 - dist / near_tol)
    align_s = min(1.0, up_count / 3.0)          # how many TFs are Supertrend-up
    turn_s = 1.0 if turning else 0.3
    tag_s = 1.0 if tagged else 0.4
    room_s = min(1.0, ((res[-1] - price) / price) / 0.30) if res else 0.0
    rsi_s = 1.0 if r14 < 45 else (0.5 if r14 < 60 else 0.0)
    score = round(100 * (0.24 * tf_s + 0.2 * fresh_s + 0.18 * align_s +
                         0.12 * tag_s + 0.1 * turn_s + 0.08 * room_s + 0.08 * rsi_s), 1)

    entry = price
    a = atr(highs, lows, closes)
    optimal_entry = round(slevel * 1.003, 10)   # buy near the Supertrend line
    sl_tight = slevel - 0.5 * a                  # stop just below the line
    sl_wide = slevel - 1.5 * a
    bundle = level_bundle(
        entry, sl_tight, sl_wide, _res_targets(res), atr=a, plan_entry=optimal_entry,
        sl_tight_basis=(f"Just below the {slabel} Supertrend line at {slevel:.6g} "
                        f"(the support being bounced), ~0.5×ATR buffer. A close "
                        f"below flips the Supertrend and voids the setup."),
        sl_wide_basis=(f"Further below the {slabel} Supertrend line at "
                       f"{slevel:.6g} (~1.5×ATR) for more room."))
    ms = market_structure(highs, lows, closes)
    return True, {
        "price": price,
        "supertrend": slevel,
        "tf": slabel,
        "tf_up": up_count,
        "dist_to_st_pct": round(dist * 100, 2),
        "tagged": bool(tagged),
        "turning_up": bool(turning),
        "rsi": r14,
        "choch": ms["choch"],
        "entry": entry,
        "optimal_entry": optimal_entry,
        "score": score,
        **bundle,
    }


# ----------------------------------------------------------------------------
# Early / potential setups — accumulation BEFORE the confirmation fires
# ----------------------------------------------------------------------------
def detect_early_setup(rows: list, highs: list[float], lows: list[float],
                       closes: list[float], volumes: list[float]
                       ) -> tuple[bool, dict]:
    """Catch coins EARLY — while they're still basing, before the 200-EMA reclaim /
    Supertrend flip confirms. Looks for: a beaten-down coin (well off its recent
    high) that is COILING (volatility contraction / squeeze) on a strong, defended
    higher-timeframe support, still below the 200 EMA, and oversold or carving a
    higher low. Higher risk / earlier entry than the confirmed scans — tag it as
    'unconfirmed'."""
    n = len(closes)
    if n < 80:
        return False, {}
    last = n - 1
    price = closes[last]
    e = ema(closes, EMA_PERIOD)
    ema_now = e[last]
    if ema_now is None or ema_now <= 0:
        return False, {}

    # 1. Beaten down — at least ~22% off the recent (120-bar) high.
    hi = max(highs[-120:]) if n >= 120 else max(highs)
    drawdown = (hi - price) / hi if hi else 0.0
    if drawdown < 0.22:
        return False, {}
    # 2. EARLY — still below / around the 200 EMA (not yet a confirmed reclaim).
    if price > ema_now * 1.03:
        return False, {}
    # 3. Volatility contraction (a coil): recent ATR well under its prior average.
    a_recent = atr(highs[-20:], lows[-20:], closes[-20:])
    a_base = atr(highs[-60:-20], lows[-60:-20], closes[-60:-20]) \
        if n >= 60 else a_recent
    contraction = (a_recent / a_base) if a_base else 1.0
    if contraction >= 0.8:                     # not coiling tightly enough
        return False, {}
    # 4. On a strong, still-defended support (daily/weekly swing low or base low).
    ksup = key_supports(rows, lows, price)
    base_low = min(lows[-30:])
    cands = [s for s in (ksup.get("sup_1d"), ksup.get("sup_1w"), base_low)
             if s and s <= price * 1.02]
    support = max(cands) if cands else None
    if support is None:
        return False, {}
    dist_sup = (price - support) / price
    if dist_sup > 0.12 or price < base_low * 0.985:   # extended, or broke the base
        return False, {}
    # 5. Bottoming — oversold or a bullish change-of-character (higher low / break).
    r14 = rsi(closes) or 50.0
    ms = market_structure(highs, lows, closes)
    if not (r14 < 50 or ms["choch"] == "bullish"):
        return False, {}

    c01 = lambda v: max(0.0, min(1.0, v))
    contr_s = c01(1.0 - contraction)
    sup_s = 1.0 if dist_sup < 0.03 else c01(1.0 - dist_sup / 0.12)
    rsi_s = 1.0 if r14 < 35 else (0.6 if r14 < 45 else 0.3)
    dd_s = c01(drawdown / 0.5)
    turn_s = 1.0 if ms["choch"] == "bullish" else 0.4
    score = round(100 * (0.30 * contr_s + 0.25 * sup_s + 0.20 * rsi_s +
                         0.15 * dd_s + 0.10 * turn_s), 1)

    entry = price
    a = atr(highs, lows, closes)
    optimal_entry = round(support * 1.005, 10)
    sl_tight = support - 0.5 * a
    sl_wide = base_low - 1.5 * a
    if sl_wide > sl_tight:
        sl_wide = sl_tight
    # Per-coin stop explanations (which support / base this level rides).
    _sup_src = ("the weekly swing-low support" if support == ksup.get("sup_1w")
                else "the daily swing-low support" if support == ksup.get("sup_1d")
                else "the 30-bar base low")
    st_basis = (f"Just below {_sup_src} at {support:.6g} that the coil is "
                f"holding, buffered by ~0.5×ATR. A close below breaks the base "
                f"and voids the early thesis.")
    sw_basis = (f"Below the 30-bar base low at {base_low:.6g} with a ~1.5×ATR "
                f"buffer — full invalidation of the accumulation range.")
    # Targets, each with its own per-coin basis. The 200-EMA reclaim is the
    # mean-reversion thesis, so it's ALWAYS included and flagged (is_ema=True).
    res = resistances_above(highs, last, price, max_n=5, min_gap=0.01)
    tps: list = []
    for r in res:                                   # nearest resistances first
        if price * 1.01 < r < ema_now * 0.999:      # rungs on the way up to the EMA
            tps.append((r, f"Overhead resistance at {r:.6g} — a prior swing high "
                           f"the bounce has to clear on the way up.", False))
        if len([1 for x in tps if not x[2]]) >= 3:
            break
    if ema_now > price * 1.01:
        tps.append((ema_now, f"200-EMA reclaim at {ema_now:.6g} — the mean-reversion "
                             f"target that confirms the early setup. This is the "
                             f"'(EMA)' target.", True))
    for r in res:                                   # stretch targets beyond the EMA
        if r > ema_now * 1.001:
            tps.append((r, f"Overhead resistance at {r:.6g} above the 200 EMA — an "
                           f"extended target once the reclaim holds.", False))
    tps.sort(key=lambda x: x[0])
    seen, ded = set(), []
    for t in tps:
        k = round(t[0], 10)
        if k not in seen:
            seen.add(k)
            ded.append(t)
    bundle = level_bundle(entry, sl_tight, sl_wide, ded[:5], atr=a, plan_entry=optimal_entry,
                          sl_tight_basis=st_basis, sl_wide_basis=sw_basis)
    # Explicit mean-reversion target: the 200-EMA reclaim on the base (4h)
    # timeframe. Always surfaced as its own column so it's visible even when
    # several structural rungs sit between price and the EMA. R:R uses the
    # floored tight stop from the bundle.
    _slt = bundle["sl_tight"]
    ema_target_rr = (round((ema_now - entry) / (entry - _slt), 2)
                     if (ema_now > entry and entry != _slt) else None)
    # Recommended REVERSAL take-profit: the realistic "good" target for this
    # accumulation/reversal — the best reward:risk among the 200-EMA reclaim and
    # the overhead resistances, weighted by reachability (a generous horizon,
    # since a proper reversal can travel), and only if it clears 1.5:1 R:R.
    _risk = entry - _slt
    _atrp = (a / price * 100.0) if price else 0.0
    rev_tp = rev_rr = rev_pct = None
    if _risk > 0:
        _cands = ([ema_now] if ema_now > entry * 1.01 else []) + \
                 [r for r in res if r > entry * 1.015]
        _best_ev = -1.0
        for _lvl in _cands:
            _rr = (_lvl - entry) / _risk
            if _rr < 1.5:
                continue
            _move = (_lvl - entry) / entry
            _datr = (_move * 100.0 / _atrp) if _atrp else 4.0
            _reach = 1.0 / (1.0 + (_datr / 9.0) ** 1.8)   # generous — reversals run
            _ev = min(_rr, 8.0) * _reach
            if _ev > _best_ev:
                _best_ev = _ev
                rev_tp = round(_lvl, 10)
                rev_rr = round(min(_rr, 8.0), 2)
                rev_pct = round(_move * 100.0, 1)
    return True, {
        "price": price,
        "support": support,
        "drawdown_pct": round(drawdown * 100, 1),
        "contraction": round(contraction, 2),
        "pct_below_ema": round((ema_now - price) / ema_now * 100, 2),
        "dist_to_support_pct": round(dist_sup * 100, 2),
        "rsi": r14,
        "choch": ms["choch"],
        "entry": entry,
        "optimal_entry": optimal_entry,
        "ema_target": round(ema_now, 10),
        "ema_target_tf": "4h",
        "rev_tp": rev_tp, "rev_tp_rr": rev_rr, "rev_tp_pct": rev_pct,
        "ema_target_pct": round((ema_now - price) / price * 100, 1),
        "ema_target_rr": ema_target_rr,
        "score": score,
        **bundle,
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
                     if c.get("quoteCoin") == "USDT" and not c.get("isHidden")
                     and not _is_tradfi_contract(c)}
            if bases:
                return bases
        except requests.RequestException as e:
            last_err = e
            time.sleep(0.8 * (attempt + 1))
    raise requests.RequestException(
        f"could not load MEXC futures contract list: {last_err}")


def list_symbols(sess: requests.Session, quote: str,
                 futures_only: bool = False, market: str = "spot") -> list[str]:
    if market == "futures":
        return list_futures_symbols(sess, quote)   # every USDT perp
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


def to_contract(display_symbol: str, quote: str = "USDT") -> str:
    """'BTCUSDT' -> 'BTC_USDT' (MEXC futures contract symbol)."""
    if display_symbol.endswith(quote) and "_" not in display_symbol:
        return display_symbol[:-len(quote)] + "_" + quote
    return display_symbol


FUT_INTERVAL = {"1m": "Min1", "5m": "Min5", "15m": "Min15", "30m": "Min30",
                "1h": "Min60", "4h": "Hour4", "8h": "Hour8", "1d": "Day1",
                "1w": "Week1"}
_FUT_SECS = {"Min1": 60, "Min5": 300, "Min15": 900, "Min30": 1800, "Min60": 3600,
             "Hour4": 14400, "Hour8": 28800, "Day1": 86400, "Week1": 604800}


# MEXC tags tokenized stocks / ETFs / other TradFi perps (Amazon, Apple, iShares
# ETFs, etc.) with contract type==2 and these "concept plate" zones. They are NOT
# crypto, barely move in USDT terms, and pollute the scan — so we drop them.
_TRADFI_PLATES = {
    "mc-trade-zone-Stock", "mc-trade-zone-tradfi", "mc-trade-zone-ETF",
    "mc-trade-zone-stockindex", "mc-trade-zone-Forex", "mc-trade-zone-commodities",
}


def _is_tradfi_contract(c: dict) -> bool:
    """True for tokenized stocks / ETFs / forex / commodities (non-crypto)."""
    if c.get("type") == 2:                    # MEXC marks TradFi contracts as type 2
        return True
    plates = c.get("conceptPlate") or []
    if any(p in _TRADFI_PLATES for p in plates):
        return True
    base = str(c.get("baseCoin", "")).upper()
    return base.endswith("STOCK")             # belt-and-suspenders for stock tokens


def list_futures_symbols(sess: requests.Session, quote: str = "USDT") -> list[str]:
    """Tradable USDT-perp contracts as DISPLAY symbols ('BTCUSDT'). The universe
    depends on SCAN_UNIVERSE: 'crypto' (default) drops tokenized stocks/ETFs/
    commodities; 'tradfi' keeps ONLY those (gold/silver/oil/stocks); 'all' keeps
    everything. Stablecoins are always dropped."""
    r = sess.get(f"{FUTURES_BASE}/api/v1/contract/detail", timeout=30)
    r.raise_for_status()
    out = []
    for c in r.json().get("data", []):
        if c.get("quoteCoin") != quote or c.get("isHidden"):
            continue
        base = str(c.get("baseCoin", "")).upper()
        if base in STABLE_BASES or not base:
            continue
        is_tf = _is_tradfi_contract(c)
        if SCAN_UNIVERSE == "crypto" and is_tf:
            continue                            # drop non-crypto (stocks/ETFs/etc.)
        if SCAN_UNIVERSE == "tradfi" and not is_tf:
            continue                            # keep ONLY stocks/commodities/forex
        out.append(base + quote)
    return sorted(set(out))


def fetch_futures_klines(sess: requests.Session, symbol: str, interval: str,
                         limit: int) -> list[list] | None:
    """Fetch futures (perp) klines and return them in the same ROW shape as the
    spot endpoint: [openTime_ms, open, high, low, close, vol, closeTime_ms, ...]."""
    cs = to_contract(symbol)
    iv = FUT_INTERVAL.get(interval, "Hour4")
    start = int(time.time()) - limit * _FUT_SECS.get(iv, 14400)
    for attempt in range(4):
        try:
            r = sess.get(f"{FUTURES_BASE}/api/v1/contract/kline/{cs}",
                         params={"interval": iv, "start": start}, timeout=30)
            if r.status_code == 429:
                time.sleep(1.5 * (attempt + 1))
                continue
            r.raise_for_status()
            d = r.json().get("data") or {}
            t = d.get("time") or []
            if not t:
                return None
            o, h, l, c, v = d["open"], d["high"], d["low"], d["close"], d["vol"]
            return [[t[i] * 1000, float(o[i]), float(h[i]), float(l[i]),
                     float(c[i]), float(v[i]), t[i] * 1000, 0, 0, 0, 0, 0]
                    for i in range(len(t))]
        except (requests.RequestException, KeyError, ValueError):
            time.sleep(0.6 * (attempt + 1))
    return None


def fetch_futures_klines_deep(sess: requests.Session, symbol: str, interval: str,
                              target: int) -> list[list] | None:
    """Deep history via PAGINATION — the contract kline endpoint caps how many candles it
    returns per call, so to reach 1–2 YEARS of data we walk backward in windows (newest first),
    stitch them, de-dup by open-time and return the last `target` candles. Used by the backtester
    so the higher timeframes can be measured over a proper multi-regime span, not a lucky slice."""
    cs = to_contract(symbol)
    iv = FUT_INTERVAL.get(interval, "Hour4")
    secs = _FUT_SECS.get(iv, 14400)
    win = 1800                                        # candles requested per page (endpoint caps ~2000)
    end = int(time.time())
    got = {}
    for _page in range(8):                            # up to 8 pages back
        start = end - win * secs
        ok = False
        for attempt in range(3):
            try:
                r = sess.get(f"{FUTURES_BASE}/api/v1/contract/kline/{cs}",
                             params={"interval": iv, "start": start, "end": end}, timeout=30)
                if r.status_code == 429:
                    time.sleep(1.2 * (attempt + 1)); continue
                r.raise_for_status()
                d = r.json().get("data") or {}
                t = d.get("time") or []
                if not t:
                    ok = True; break
                o, h, l, c, v = d["open"], d["high"], d["low"], d["close"], d["vol"]
                for i in range(len(t)):
                    got[t[i]] = [t[i] * 1000, float(o[i]), float(h[i]), float(l[i]),
                                 float(c[i]), float(v[i]), t[i] * 1000, 0, 0, 0, 0, 0]
                end = min(t) - secs                   # step the window further back
                ok = True; break
            except (requests.RequestException, KeyError, ValueError):
                time.sleep(0.5 * (attempt + 1))
        if not ok or len(got) >= target:
            break
    if not got:
        return None
    rows = [got[k] for k in sorted(got)]
    return rows[-target:]


def fetch_candles_deep(sess: requests.Session, symbol: str, interval: str,
                       target: int, market: str) -> list[list] | None:
    """Deep (multi-year) candles for the backtester. Futures paginates for real depth; if that
    comes back stale/empty, fall back to the normal (capped) fetch so a coin still contributes."""
    if market == "futures":
        rows = fetch_futures_klines_deep(sess, symbol, interval, target)
        if rows and _klines_fresh(rows, interval):
            return rows
        return fetch_candles(sess, symbol, interval, min(target, 1000), market)
    return fetch_klines(sess, symbol, interval, min(target, 1000))


def _klines_fresh(rows: list[list] | None, interval: str) -> bool:
    """True if the most recent candle is recent enough (guards against MEXC's
    futures kline REST endpoint serving stale/frozen data for some contracts)."""
    if not rows:
        return False
    try:
        last_ts = float(rows[-1][0]) / 1000.0        # ms → s
    except (ValueError, IndexError, TypeError):
        return False
    secs = _FUT_SECS.get(FUT_INTERVAL.get(interval, "Hour4"), 14400)
    return (time.time() - last_ts) < secs * 3        # within ~3 candles of now


def fetch_candles(sess: requests.Session, symbol: str, interval: str,
                  limit: int, market: str) -> list[list] | None:
    """Dispatch to the futures or spot kline endpoint (unified row shape).
    If the futures endpoint returns stale/frozen data (as MEXC sometimes does for
    certain contracts), fall back to the spot klines, which stay current."""
    if market == "futures":
        rows = fetch_futures_klines(sess, symbol, interval, limit)
        if _klines_fresh(rows, interval):
            return rows
        spot = fetch_klines(sess, symbol, interval, limit)   # fresher fallback
        if spot:
            return spot
        return rows                                          # nothing better
    return fetch_klines(sess, symbol, interval, limit)


def bias_on_tf(sess: requests.Session, symbol: str, interval: str,
               market: str) -> str | None:
    """Market-structure bias ('bullish' / 'bearish' / 'neutral') on one timeframe —
    used to enrich hits with a 1h read (which can't be aggregated from 4h candles)."""
    rr = fetch_candles(sess, symbol, interval, 400, market)
    if not rr or len(rr) < 15:
        return None
    rws = rr[:-1]
    try:
        H = [float(x[2]) for x in rws]
        L = [float(x[3]) for x in rws]
        C = [float(x[4]) for x in rws]
    except (ValueError, IndexError):
        return None
    m = market_structure(H, L, C)
    if m["structure"] == "uptrend" or m["choch"] == "bullish":
        return "bullish"
    if m["structure"] == "downtrend" or m["choch"] == "bearish":
        return "bearish"
    return "neutral"


# Fixed basket of liquid alts used to gauge broad "alt-season" breadth cheaply
# (one daily fetch each) without scanning the whole universe again.
ALT_BASKET = ["ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT",
              "AVAXUSDT", "LINKUSDT", "TONUSDT", "TRXUSDT", "DOTUSDT", "MATICUSDT",
              "NEARUSDT", "APTUSDT", "SUIUSDT", "LTCUSDT"]


def _tf_read(sess, symbol, interval, market):
    """One-timeframe health read for a symbol: price vs its 200-EMA (or the longest
    EMA the history allows), the 20/50/200 EMA stack, RSI(14), Bollinger squeeze
    percentile, and last-closed-candle % change. Returns a dict or None."""
    rr = fetch_candles(sess, symbol, interval, 400, market)
    if not rr or len(rr) < 30:
        return None
    rws = rr[:-1]
    try:
        C = [float(x[4]) for x in rws]
    except (ValueError, IndexError):
        return None
    if len(C) < 30:
        return None
    px = C[-1]
    per_long = 200 if len(C) >= 210 else (100 if len(C) >= 110 else 50)
    e_l = ema(C, per_long)[-1]
    e20 = ema(C, 20)[-1]
    e50 = ema(C, 50)[-1] if len(C) >= 60 else None
    rs = rsi_series(C)
    rsi = next((v for v in reversed(rs) if v is not None), None)
    sq = bbw_squeeze_pct(C)
    chg = (C[-1] / C[-2] - 1.0) * 100 if len(C) >= 2 and C[-2] else 0.0
    px_vs_long = ((px / e_l - 1.0) * 100) if e_l else None
    # Directional score in [-1,+1] from EMA side, EMA stack and RSI.
    s = 0.0
    if px_vs_long is not None:
        s += 0.5 if px_vs_long > 0 else -0.5
    if e20 and e_l:
        s += 0.25 if e20 > e_l else -0.25
    if e20 and px:
        s += 0.15 if px > e20 else -0.15
    if rsi is not None:
        s += 0.10 if rsi >= 55 else (-0.10 if rsi <= 45 else 0.0)
    s = max(-1.0, min(1.0, s))
    bias = "bull" if s >= 0.35 else ("bear" if s <= -0.35 else "neutral")
    if e20 and e_l:
        stack = "stacked up" if (px > e20 > e_l) else ("stacked down" if (px < e20 < e_l) else "mixed")
    else:
        stack = "mixed"
    return {"tf": interval, "px_vs_ema": round(px_vs_long, 2) if px_vs_long is not None else None,
            "ema_stack": stack, "rsi": round(rsi, 1) if rsi is not None else None,
            "squeeze": sq, "chg": round(chg, 2), "score": round(s, 3), "bias": bias,
            "ema_len": per_long}


def compute_market_context(sess: requests.Session, market: str) -> dict:
    """Big-picture read: is it a good day/week to be hunting longs or shorts?
    Combines a multi-timeframe BTC health check (15m→1W) with alt-market breadth
    (share of a liquid alt basket above its 200-/50-day EMAs, plus up/down count).
    Returns a self-describing dict the dashboard renders as the Market tab."""
    out = {"asof": time.time(), "btc": None, "alts": None, "day": None, "week": None}
    # --- BTC across timeframes ---
    tfs = ["15m", "1h", "4h", "1d", "1w"]
    reads = []
    with ThreadPoolExecutor(max_workers=5) as ex:
        futs = {ex.submit(_tf_read, sess, "BTCUSDT", tf, market): tf for tf in tfs}
        got = {}
        for f in as_completed(futs):
            try:
                got[futs[f]] = f.result()
            except Exception:
                got[futs[f]] = None
    reads = [got.get(tf) for tf in tfs]
    reads = [r for r in reads if r]
    btc_px = None
    try:
        _b = fetch_candles(sess, "BTCUSDT", "1h", 3, market)
        if _b:
            btc_px = float(_b[-1][4])
    except Exception:
        pass
    if reads:
        # Weight higher timeframes more for the overall trend read.
        w = {"15m": 0.5, "1h": 0.8, "4h": 1.2, "1d": 1.6, "1w": 1.4}
        tot = sum(w.get(r["tf"], 1.0) for r in reads)
        score = sum(r["score"] * w.get(r["tf"], 1.0) for r in reads) / tot if tot else 0.0
        verdict = "bullish" if score >= 0.3 else ("bearish" if score <= -0.3 else "mixed")
        out["btc"] = {"price": btc_px, "tfs": reads, "score": round(score, 3),
                      "verdict": verdict}
        # Day read = intraday TFs; Week read = swing TFs.
        def _sub(names):
            rs = [r for r in reads if r["tf"] in names]
            if not rs:
                return None
            tw = sum(w.get(r["tf"], 1.0) for r in rs)
            return sum(r["score"] * w.get(r["tf"], 1.0) for r in rs) / tw if tw else 0.0
        day_s = _sub({"15m", "1h", "4h"})
        week_s = _sub({"4h", "1d", "1w"})
    else:
        score = day_s = week_s = 0.0
    # --- Alt breadth (daily) ---
    alt_reads = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(_tf_read, sess, s, "1d", market): s for s in ALT_BASKET}
        for f in as_completed(futs):
            try:
                r = f.result()
            except Exception:
                r = None
            if r:
                r["symbol"] = futs[f]
                alt_reads.append(r)
    alts = None
    breadth_s = 0.0
    if alt_reads:
        n = len(alt_reads)
        above = sum(1 for r in alt_reads if (r.get("px_vs_ema") or 0) > 0)
        up = sum(1 for r in alt_reads if (r.get("chg") or 0) > 0)
        above_20 = sum(1 for r in alt_reads if r.get("ema_stack") == "stacked up")
        avg_chg = sum((r.get("chg") or 0) for r in alt_reads) / n
        pct_above = round(above / n * 100)
        breadth_s = (above / n - 0.5) * 2.0  # -1..+1
        bverdict = "risk-on" if pct_above >= 60 else ("risk-off" if pct_above <= 40 else "mixed")
        alts = {"n": n, "pct_above_200ema": pct_above,
                "pct_stacked_up": round(above_20 / n * 100),
                "up": up, "down": n - up, "avg_chg": round(avg_chg, 2),
                "verdict": bverdict,
                "members": sorted(alt_reads, key=lambda r: (r.get("px_vs_ema") or -999),
                                  reverse=True)}
    out["alts"] = alts

    # --- Intraday alt breadth (4h) --- a SHORTER-timeframe breadth read so the "day"
    # verdict reflects how alts are participating RIGHT NOW (last few hours), not just
    # where they sit on the daily. Same basket, one extra 4h read each (bounded cost).
    alt4_reads = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(_tf_read, sess, s, "4h", market): s for s in ALT_BASKET}
        for f in as_completed(futs):
            try:
                r = f.result()
            except Exception:
                r = None
            if r:
                r["symbol"] = futs[f]
                alt4_reads.append(r)
    alts_4h = None
    breadth_4h = 0.0
    if alt4_reads:
        n4 = len(alt4_reads)
        above4 = sum(1 for r in alt4_reads if (r.get("px_vs_ema") or 0) > 0)
        up4 = sum(1 for r in alt4_reads if (r.get("chg") or 0) > 0)
        avg_chg4 = sum((r.get("chg") or 0) for r in alt4_reads) / n4
        pct_above4 = round(above4 / n4 * 100)
        pct_up4 = round(up4 / n4 * 100)
        breadth_4h = (above4 / n4 - 0.5) * 2.0
        # Lean off the freshest thing: how many alts are GREEN on the 4h right now.
        v4 = "risk-on" if pct_up4 >= 60 else ("risk-off" if pct_up4 <= 40 else "mixed")
        alts_4h = {"n": n4, "pct_above_200ema": pct_above4, "pct_up": pct_up4,
                   "up": up4, "down": n4 - up4, "avg_chg": round(avg_chg4, 2),
                   "verdict": v4}
    out["alts_4h"] = alts_4h

    # A quick "right now" intraday lean from the fastest reads (BTC 15m/1h + 4h alt breadth).
    now_s = 0.7 * (day_s or 0.0) + 0.3 * breadth_4h
    out["now"] = {"score": round(now_s, 3),
                  "lean": "long" if now_s >= 0.22 else ("short" if now_s <= -0.22 else "neutral"),
                  "note": ("alts leaning green on the 4h" if breadth_4h > 0.2
                           else "alts leaning red on the 4h" if breadth_4h < -0.2
                           else "alts mixed on the 4h")}

    # --- Combined day/week verdicts ---
    # The DAY read blends BTC intraday trend with the 4h alt breadth (fresh participation);
    # the WEEK read blends BTC swing trend with the daily alt breadth (structural).
    def _combine(trend_s, horizon, breadth):
        c = 0.7 * (trend_s or 0.0) + 0.3 * (breadth or 0.0)
        if c >= 0.28:
            longs, shorts = "favorable", "avoid"
            head = f"Good {horizon} to look for LONGS — BTC is trending up and alt breadth is supportive."
        elif c <= -0.28:
            longs, shorts = "avoid", "favorable"
            head = f"Good {horizon} to look for SHORTS — BTC is trending down and alt breadth is weak."
        else:
            longs, shorts = "cautious", "cautious"
            head = f"Mixed {horizon} — no clear edge either way; be selective and trade the cleanest setups only."
        return {"score": round(c, 3), "longs": longs, "shorts": shorts, "headline": head}
    out["day"] = _combine(day_s, "day", breadth_4h)
    out["week"] = _combine(week_s, "week", breadth_s)

    # --- Volatility regime (BTC 4h): compression/chop vs expansion. Drives which KIND of
    # trade the tape rewards: in compression, mean-reversion bounces off levels work and
    # breakouts fake out; in expansion, momentum/trend & breakouts follow through. ---
    vol_regime = None
    try:
        braw = fetch_candles(sess, "BTCUSDT", "4h", 300, market)
        if braw and len(braw) > 60:
            bh = [float(x[2]) for x in braw[:-1]]
            bl = [float(x[3]) for x in braw[:-1]]
            bc = [float(x[4]) for x in braw[:-1]]
            trs = [max(bh[i] - bl[i], abs(bh[i] - bc[i - 1]), abs(bl[i] - bc[i - 1]))
                   for i in range(1, len(bc))]
            win = 14
            atrs = [sum(trs[i - win:i]) / win for i in range(win, len(trs) + 1)]
            if atrs:
                cur = atrs[-1]
                look = atrs[-120:] if len(atrs) >= 120 else atrs
                pct = round(sum(1 for x in look if x <= cur) / len(look) * 100)
                if pct >= 70:
                    st, note = "expansion", ("BTC volatility is EXPANDING — momentum, trend "
                        "and breakouts tend to follow through; counter-trend fades are riskier.")
                elif pct <= 30:
                    st, note = "compression", ("BTC volatility is COMPRESSED (chop) — bounces "
                        "off strong levels work best; breakouts often fail/fake until it expands.")
                else:
                    st, note = "normal", "BTC volatility is around normal — no strong tilt either way."
                vol_regime = {"state": st, "atr_pctile": pct,
                              "atr_pct": round(cur / bc[-1] * 100, 2), "note": note}
    except Exception:
        vol_regime = None
    out["vol_regime"] = vol_regime
    return out


def backfill_market_history(sess: requests.Session, market: str, days: int = 120) -> list:
    """Reconstruct the market-regime chart for the PAST `days` from real daily candles
    (no waiting for live scans to accumulate). For each historical day we recompute the
    daily-timeframe BTC trend score (price vs 200/20 EMA + RSI) and the alt breadth
    (share of the basket above its own 200-day EMA that day). Returns points matching
    the live market-history schema, tagged backfill=True."""
    need = days + 220
    braw = fetch_candles(sess, "BTCUSDT", "1d", min(need, 1000), market)
    if not braw or len(braw) < 230:
        return []
    bc = [float(x[4]) for x in braw[:-1]]
    bt = [int(x[0]) // 1000 for x in braw[:-1]]
    e200, e20 = ema(bc, 200), ema(bc, 20)
    rsis = rsi_series(bc)
    N = len(bc)
    # Alt basket daily closes + their 200-EMA, aligned to BTC by index-from-end.
    alt = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(fetch_candles, sess, s, "1d", min(need, 1000), market): s
                for s in ALT_BASKET}
        for f in as_completed(futs):
            try:
                r = f.result()
            except Exception:
                r = None
            if r and len(r) >= 210:
                c = [float(x[4]) for x in r[:-1]]
                alt.append((c, ema(c, 200)))
    pts = []
    start = max(210, N - days)
    for i in range(start, N):
        el, e2, rs, px = e200[i], e20[i], rsis[i], bc[i]
        if el is None:
            continue
        s = 0.5 if (px / el - 1) > 0 else -0.5
        if e2 and el:
            s += 0.25 if e2 > el else -0.25
        if e2:
            s += 0.15 if px > e2 else -0.15
        if rs is not None:
            s += 0.10 if rs >= 55 else (-0.10 if rs <= 45 else 0.0)
        s = max(-1.0, min(1.0, s))
        verdict = "bullish" if s >= 0.3 else ("bearish" if s <= -0.3 else "mixed")
        above = tot = 0
        for c, e2h in alt:
            j = len(c) - (N - i)                 # same calendar day, index-from-end
            if 0 <= j < len(c) and e2h[j] is not None:
                tot += 1
                if c[j] > e2h[j]:
                    above += 1
        pct = round(above / tot * 100) if tot else None
        av = ("risk-on" if (pct or 0) >= 60 else "risk-off" if (pct or 0) <= 40 else "mixed")
        pts.append({"t": bt[i], "btc": round(s, 3), "btc_v": verdict, "btc_px": px,
                    "day": None, "day_longs": None, "week": None, "week_longs": None,
                    "alt_above": pct, "alt_v": av, "alt_chg": None, "backfill": True})
    return pts


def enrich_1h(sess: requests.Session, symbol: str, market: str) -> tuple:
    """One 1h fetch → (bias_label, primary_1h_pattern). Used to enrich flagged
    coins with a 1h read (bias + formation) that can't be aggregated from 4h."""
    rr = fetch_candles(sess, symbol, "1h", 400, market)
    if not rr or len(rr) < 15:
        return None, None
    rws = rr[:-1]
    try:
        H = [float(x[2]) for x in rws]
        L = [float(x[3]) for x in rws]
        C = [float(x[4]) for x in rws]
        V = [float(x[5]) for x in rws]
    except (ValueError, IndexError):
        return None, None
    m = market_structure(H, L, C)
    bias = ("bullish" if (m["structure"] == "uptrend" or m["choch"] == "bullish")
            else "bearish" if (m["structure"] == "downtrend" or m["choch"] == "bearish")
            else "neutral")
    pat = primary_pattern(H, L, C, V)
    if pat:
        pat = {**pat, "tf": "1h"}
    return bias, pat


def _futures_live_price(sess: requests.Session, symbol: str) -> float | None:
    r = sess.get(f"{FUTURES_BASE}/api/v1/contract/ticker",
                 params={"symbol": to_contract(symbol)}, timeout=15)
    r.raise_for_status()
    d = r.json().get("data")
    if isinstance(d, list):
        d = d[0] if d else {}
    return float(d["lastPrice"]) if d and d.get("lastPrice") else None


def _spot_live_price(sess: requests.Session, symbol: str) -> float | None:
    r = sess.get(f"{BASE}/api/v3/ticker/price",
                 params={"symbol": symbol}, timeout=15)
    r.raise_for_status()
    p = r.json().get("price")
    return float(p) if p else None


# ---------------------------------------------------------------------------
# Coinalyze — free derivatives-history API (open interest, funding rate,
# long/short ratio, liquidations). MEXC's own API only returns a LIVE open-
# interest snapshot, so we use Coinalyze to get the HISTORY needed for a real
# price-vs-OI divergence read plus funding/positioning context.
#
# Entirely optional and self-contained: set COINALYZE_API_KEY in the environment
# to enable it. Without a key (or on any error) every function returns None and
# the app behaves exactly as before. Free tier = 40 calls/min, and each symbol in
# a request counts as one call, so this is used ON DEMAND (Analyze a coin), not
# across the whole universe.
# ---------------------------------------------------------------------------
# Tolerate a key pasted WITH surrounding quotes (a very common mistake when copying
# from a .env line) — strip quotes/whitespace so the header is the raw token.
COINALYZE_KEY = os.environ.get("COINALYZE_API_KEY", "").strip().strip('"').strip("'").strip()
COINALYZE_BASE = "https://api.coinalyze.net/v1"
_CX_SESS = requests.Session()
_cx_symbol_map: dict | None = None      # BASE asset (e.g. 'BTC') -> coinalyze perp symbol
_cx_map_ts = 0.0


def _cx_get(path: str, params: dict, timeout: int = 15):
    if not COINALYZE_KEY:
        return None
    p = dict(params)
    p["api_key"] = COINALYZE_KEY
    try:
        r = _CX_SESS.get(f"{COINALYZE_BASE}{path}", params=p, timeout=timeout)
        if r.status_code != 200:
            return None
        return r.json()
    except (requests.RequestException, ValueError):
        return None


def coinalyze_status() -> dict:
    """Non-secret diagnostic of the Coinalyze integration: is a key present, does a
    test call succeed, and did the symbol map build? Never returns the key itself."""
    out = {"key_present": bool(COINALYZE_KEY), "key_len": len(COINALYZE_KEY)}
    if not COINALYZE_KEY:
        return out
    try:
        r = _CX_SESS.get(f"{COINALYZE_BASE}/future-markets",
                         params={"api_key": COINALYZE_KEY}, timeout=12)
        out["test_status"] = r.status_code
        try:
            j = r.json()
            out["markets"] = len(j) if isinstance(j, list) else None
        except Exception:
            out["body"] = (r.text or "")[:140]
    except Exception as e:
        out["error"] = str(e)[:140]
    try:
        m = _cx_build_symbol_map()
        out["map_size"] = len(m)
        out["btc_symbol"] = m.get("BTC")
    except Exception as e:
        out["map_error"] = str(e)[:140]
    return out


def _cx_build_symbol_map() -> dict:
    """Map BASE asset -> Coinalyze perpetual symbol, preferring MEXC's own USDT
    perp so the OI/funding we read matches the exchange the user trades; any other
    exchange fills gaps. Cached ~6h; {} if the API is unavailable."""
    global _cx_symbol_map, _cx_map_ts
    now = time.time()
    if _cx_symbol_map is not None and (now - _cx_map_ts) < 6 * 3600:
        return _cx_symbol_map
    markets = _cx_get("/future-markets", {})
    if not isinstance(markets, list):
        _cx_symbol_map = {}
        _cx_map_ts = now
        return _cx_symbol_map
    exch = _cx_get("/exchanges", {})
    mexc_code = None
    if isinstance(exch, list):
        for e in exch:
            if "mexc" in (e.get("name", "") or "").lower():
                mexc_code = e.get("code")
                break
    mexc_map, any_map = {}, {}
    for mk in markets:
        if not mk.get("is_perpetual"):
            continue
        if (mk.get("quote_asset") or "").upper() not in ("USDT", "USD"):
            continue
        base = (mk.get("base_asset") or "").upper()
        sym = mk.get("symbol")
        if not base or not sym:
            continue
        if mexc_code and mk.get("exchange") == mexc_code:
            mexc_map.setdefault(base, sym)
        else:
            any_map.setdefault(base, sym)
    m = dict(any_map)
    m.update(mexc_map)                       # MEXC wins where present
    _cx_symbol_map = m
    _cx_map_ts = now
    return m


def cx_symbol_for(display_symbol: str):
    """MEXC display symbol 'BTCUSDT' -> Coinalyze perp symbol, or None."""
    s = (display_symbol or "").upper()
    base = s
    for q in ("USDT", "USD"):
        if s.endswith(q):
            base = s[:-len(q)]
            break
    return _cx_build_symbol_map().get(base)


def fetch_derivatives(display_symbol: str, bars: int = 24) -> dict | None:
    """Derivatives history + a price-vs-OI divergence read for ONE coin, from
    Coinalyze. Pulls hourly open-interest, funding-rate, long/short-ratio and
    OHLCV over the last `bars` hours. Returns a dict (or None when there's no key
    / no match). ~4 API calls — on-demand use only."""
    if not COINALYZE_KEY:
        return None
    sym = cx_symbol_for(display_symbol)
    if not sym:
        return None
    now = int(time.time())
    frm = now - (bars + 2) * 3600

    def hist(path):
        d = _cx_get(path, {"symbols": sym, "interval": "1hour", "from": frm, "to": now})
        if isinstance(d, list) and d and isinstance(d[0].get("history"), list):
            return d[0]["history"]
        return None

    oi = hist("/open-interest-history")           # {t,o,h,l,c} — c = OI at close
    fr = hist("/funding-rate-history")            # c = funding rate
    lsr = hist("/long-short-ratio-history")       # r = long/short ratio
    ohlcv = hist("/ohlcv-history")                # {t,o,h,l,c,v,...}
    liq = hist("/liquidation-history")            # {t,l,s} — l=long liqs, s=short liqs ($)
    out: dict = {"source": "coinalyze"}

    # Recent liquidation FLOW — which side just got flushed. Heavy LONG liquidations =
    # longs capitulating (often near a local bottom / bounce); heavy SHORT liquidations =
    # shorts squeezed (often near a local top / fade). A read, not a resting-liq heatmap.
    if liq:
        ll = sum((x.get("l") or 0) for x in liq)
        sl = sum((x.get("s") or 0) for x in liq)
        out["liq_long"], out["liq_short"], out["liq_total"] = ll, sl, (ll + sl)
        if (ll + sl) > 0:
            share = ll / (ll + sl)
            if share >= 0.70:
                out["liq_side"] = "long"
                out["liq_note"] = ("longs were just liquidated hard — forced selling that "
                                   "often flushes into a local bottom (watch for a bounce)")
            elif share <= 0.30:
                out["liq_side"] = "short"
                out["liq_note"] = ("shorts were just squeezed hard — forced buying that "
                                   "often marks a local top (fade-the-rip risk)")
            else:
                out["liq_side"] = "balanced"
                out["liq_note"] = "liquidations roughly balanced both sides — no one-sided flush"

    if oi and len(oi) >= 4:
        oi_now = oi[-1].get("c")
        oi_then = oi[max(0, len(oi) - bars)].get("c")
        out["oi_now"] = oi_now
        if oi_now and oi_then:
            out["oi_chg_pct"] = round((oi_now / oi_then - 1) * 100, 1)
    if fr:
        out["funding"] = fr[-1].get("c")          # latest funding rate (fraction)
        vals = [x.get("c") for x in fr if x.get("c") is not None]
        if vals:
            out["funding_avg"] = sum(vals) / len(vals)
    if lsr:
        out["long_short"] = lsr[-1].get("r")
    if ohlcv and len(ohlcv) >= 4 and out.get("oi_chg_pct") is not None:
        p_now = ohlcv[-1].get("c")
        p_then = ohlcv[max(0, len(ohlcv) - bars)].get("c")
        if p_now and p_then:
            out["price_chg_pct"] = round((p_now / p_then - 1) * 100, 1)

    def _div_label(pc, oc):
        """Classify a price-vs-OI divergence from a % price change and % OI change."""
        if pc is None or oc is None:
            return (None, None)
        pu, pd = pc > 0.7, pc < -0.7
        ou, od = oc > 2, oc < -2
        if pu and ou:
            return ("real_up", "price and open interest both rising — new money behind the "
                    "move (real, conviction up-move)")
        if pu and od:
            return ("fake_up", "price up but open interest FALLING — short-covering / a thin "
                    "rally that can fade (fake-pump risk, confirm before chasing)")
        if pd and ou:
            return ("real_down", "price down with open interest rising — fresh shorts, "
                    "conviction selling (real down-move)")
        if pd and od:
            return ("exhaust_down", "price and open interest both falling — longs unwinding, "
                    "the selloff may be exhausting (watch for a bounce)")
        return ("neutral", "open interest roughly flat vs price — no clear divergence")

    pc, oc = out.get("price_chg_pct"), out.get("oi_chg_pct")
    dv, dn = _div_label(pc, oc)
    if dv:
        out["divergence"], out["divergence_note"] = dv, dn

    # PER-TIMEFRAME derivatives read — the same OI/price divergence over several windows
    # (1h / 4h / 12h / 24h) computed from the hourly series (no extra API calls), so you
    # can see whether the OI story agrees with the timeframe you're actually trading.
    def _chg(series, hrs, key):
        if not series or len(series) < 2:
            return None
        a = series[-1].get(key)
        b = series[max(0, len(series) - 1 - hrs)].get(key)
        return round((a / b - 1) * 100, 1) if (a and b) else None

    oi_tf = []
    for hrs, lab in ((1, "1h"), (4, "4h"), (12, "12h"), (24, "24h")):
        _oc = _chg(oi, hrs, "c")
        if _oc is None:
            continue
        _pc = _chg(ohlcv, hrs, "c")
        _dv, _dn = _div_label(_pc, _oc)
        oi_tf.append({"tf": lab, "oi_chg": _oc, "price_chg": _pc,
                      "divergence": _dv, "divergence_note": _dn})
    if oi_tf:
        out["oi_tf"] = oi_tf
    return out if len(out) > 1 else None


def estimate_liq_zones(price: float) -> dict | None:
    """Approximate liquidation MAGNETS around price. We don't have an order-book heatmap
    (that needs a paid source), so we estimate where over-leveraged positions get
    liquidated by common leverage tiers: longs get liquidated BELOW price (magnets that
    can pull price down to hunt stops), shorts ABOVE. Clearly an estimate, not exact
    resting liquidity — but the clusters are where price often gets 'magneted'."""
    if not price or price <= 0:
        return None
    levs = [100, 50, 25, 10]     # nearest → furthest
    below = [{"lev": lv, "level": round(price * (1 - 1.0 / lv), 10), "pct": round(-100.0 / lv, 1)}
             for lv in levs]     # long liquidations below
    above = [{"lev": lv, "level": round(price * (1 + 1.0 / lv), 10), "pct": round(100.0 / lv, 1)}
             for lv in levs]     # short liquidations above
    return {"below_long_liqs": below, "above_short_liqs": above,
            "note": "Estimated leverage-liquidation magnets (10×–100×). Price often reaches "
                    "toward these clusters to trigger stops — use as context, not exact levels."}


def fetch_deriv_series(display_symbol: str, bars: int = 168) -> dict | None:
    """Raw derivatives TIME-SERIES for one coin from Coinalyze — hourly open
    interest, funding rate and price over the last `bars` hours. Used by the
    History store to backload real historical context on startup and append the
    newest points thereafter. Returns {'oi':[[t,c],…],'funding':[[t,c],…],
    'price':[[t,c],…]} (epoch-seconds timestamps) or None without a key/match."""
    if not COINALYZE_KEY:
        return None
    sym = cx_symbol_for(display_symbol)
    if not sym:
        return None
    now = int(time.time())
    frm = now - (bars + 2) * 3600

    def hist(path):
        d = _cx_get(path, {"symbols": sym, "interval": "1hour", "from": frm, "to": now})
        if isinstance(d, list) and d and isinstance(d[0].get("history"), list):
            return d[0]["history"]
        return None

    oi = hist("/open-interest-history") or []
    fr = hist("/funding-rate-history") or []
    px = hist("/ohlcv-history") or []

    def pairs(rows):
        out = []
        for x in rows:
            t, c = x.get("t"), x.get("c")
            if t is not None and c is not None:
                out.append([int(t), float(c)])
        return out

    o, f, p = pairs(oi), pairs(fr), pairs(px)
    if not (o or f or p):
        return None
    return {"oi": o, "funding": f, "price": p}


def fetch_open_interest(sess: requests.Session, symbol: str) -> dict | None:
    """Current open interest for a MEXC perp from the contract ticker.
    Returns {oi, price, oi_usd, chg24} — oi is holdVol (contracts), oi_usd an
    approximate notional (holdVol × last price), chg24 the 24h price move %
    (for the price-vs-OI divergence read). None if unavailable."""
    try:
        r = sess.get(f"{FUTURES_BASE}/api/v1/contract/ticker",
                     params={"symbol": to_contract(symbol)}, timeout=12)
        r.raise_for_status()
        d = r.json().get("data")
        if isinstance(d, list):
            d = d[0] if d else {}
        if not d or d.get("holdVol") is None:
            return None
        oi = float(d["holdVol"])
        price = float(d["lastPrice"]) if d.get("lastPrice") else None
        chg = float(d["riseFallRate"]) * 100 if d.get("riseFallRate") is not None else None
        return {"oi": oi, "price": price,
                "oi_usd": (oi * price) if (oi and price) else None,
                "chg24": chg}
    except (requests.RequestException, ValueError, KeyError, TypeError):
        return None


def fetch_live_price(sess: requests.Session, symbol: str,
                     market: str) -> float | None:
    """The current LIVE last-traded price for one symbol — independent of the
    chart timeframe, so 'current price' is always up to the second.

    Retries on transient errors and, if the primary market's ticker fails,
    falls back to the other market so the Analyze card never silently drops to
    a stale per-timeframe candle close."""
    order = ([_futures_live_price, _spot_live_price] if market == "futures"
             else [_spot_live_price, _futures_live_price])
    for fn in order:
        for attempt in range(3):
            try:
                p = fn(sess, symbol)
                if p:
                    return p
                break  # valid response but no price here — try the other market
            except requests.RequestException:
                time.sleep(0.6 * (attempt + 1))   # transient — back off and retry
            except (KeyError, ValueError, TypeError):
                break
    return None


_SPOT_IV = {"1h": "60m", "1w": "1W"}   # normalize to MEXC spot interval names


def fetch_klines(sess: requests.Session, symbol: str, interval: str,
                 limit: int) -> list[list] | None:
    interval = _SPOT_IV.get(interval, interval)
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


def _round_number(price, above):
    """Nearest psychological round-number level just above/below price (traders cluster
    orders there, so it acts as a magnet). Scales to the coin's magnitude."""
    import math
    if not price or price <= 0:
        return None
    step = 10 ** (math.floor(math.log10(price)) - 1)
    if step <= 0:
        return None
    lv = (math.floor(price / step) + 1) * step if above else math.floor(price / step) * step
    return lv if lv > 0 else None


def _revert_live(side, closes, highs, lows, ema200_now, atr_val, rsis, e200s, e20s,
                 tf_label, btc_trend):
    """The LIVE version of the backtested trend-aligned reversion edge, evaluated on the
    CURRENT bar. Long = an oversold flush that's snapping back inside an uptrend; short = an
    overbought pop rolling over inside a downtrend; target the 20-EMA mean; stop beyond the
    flush extreme. Refuses to fight BTC (no long while BTC is in a downtrend, no short while
    BTC rips). Returns a ready-to-trade plan dict (same shape the boards expect) or None.
    This is what the whole-universe backtest proved wins on the higher timeframes."""
    n = len(closes)
    if n < 210 or not atr_val or atr_val <= 0:
        return None
    price = closes[-1]
    rs = rsis[-1] if rsis else None
    el = ema200_now
    e2 = e20s[-1] if e20s else None
    if rs is None or el is None or e2 is None or price <= 0:
        return None
    prev200 = e200s[-21] if (len(e200s) > 21 and e200s[-21] is not None) else None
    long = side == "long"
    if long:
        slope_up = (prev200 is None) or (el > prev200)
        if not (price > el and slope_up):        # must be an uptrend
            return None
        if _btc_block(True, tf_label or "4h", btc_trend, None):   # smart don't-fight-BTC gate
            return None
        # MOMENTUM continuation (dip-buying loses in crypto) — ride strength on a fresh breakout.
        if not (52 <= rs <= 72):                 # strong, not blown off
            return None
        prior_high = max(highs[-21:-1]) if len(highs) >= 21 else max(highs[:-1] or highs)
        if closes[-1] <= prior_high:             # need a fresh 20-bar breakout close
            return None
        entry = price
        base_low = min(lows[-20:])
        swing_low = min(lows[-10:])
        stop = min(swing_low, entry - 1.4 * atr_val)
        risk = entry - stop
        if risk <= 0:
            return None
        # Structural target (measured move) capped tight at ~1.9R — 2yr data: R:R<2 beats ≥2.
        measured = prior_high - base_low
        target = min(entry + max(measured, 1.5 * risk), entry + 1.9 * risk)
        rr = (target - entry) / risk
        if rr < 1.2:
            return None
        tp2 = entry + (target - entry) * 1.6
        tps = [{"lvl": round(target, 10), "rr": round(rr, 2), "basis": "measured move — base height projected up"},
               {"lvl": round(tp2, 10), "rr": round(rr * 1.6, 2), "basis": "runner — let the trend extend"}]
        return {"entry": round(entry, 10), "entry_basis": "breakout / trend-continuation (momentum)",
                "entry_tf": tf_label, "stop": round(stop, 10),
                "stop_basis": "below the breakout base / 1.4×ATR", "stop_tf": tf_label,
                "target": round(target, 10), "rr": round(rr, 2), "rr_max": 3.0, "tps": tps,
                "revert": True, "kind": "momentum"}
    else:
        slope_dn = (prev200 is None) or (el < prev200)
        if not (price < el and slope_dn):        # must be a downtrend
            return None
        if _btc_block(False, tf_label or "4h", btc_trend, None):  # smart don't-fight-BTC gate
            return None
        if rs <= 55:                             # overbought-ish pop (loosened to catch big caps)
            return None
        if closes[-1] >= closes[-2]:             # roll-over must have started (red tick)
            return None
        flush_high = max(highs[-4:])
        entry = price
        risk = (flush_high + 0.5 * atr_val - entry) * 1.3   # WIDER stop (~30%) — losers kept hitting target after being stopped
        stop = entry + risk
        if risk <= 0:
            return None
        recent_low = min(lows[-20:])
        struct = min(e2, recent_low) if recent_low < entry * 0.997 else e2
        target = max(min(struct, entry - 1.3 * risk), entry - 1.9 * risk)   # capped tight (R:R<2 wins)
        if target <= 0:
            return None
        rr = (entry - target) / risk
        if rr < 0.8:
            return None
        tp2 = entry - (entry - target) * 1.6
        tps = [{"lvl": round(target, 10), "rr": round(rr, 2), "basis": "structure below — 20-EMA mean / recent swing low"},
               {"lvl": round(tp2, 10), "rr": round(rr * 1.6, 2), "basis": "runner — let the downtrend extend"}]
        return {"entry": round(entry, 10), "entry_basis": "overbought pop fading in a downtrend (reversion)",
                "entry_tf": tf_label, "stop": round(stop, 10),
                "stop_basis": "above the pop high + widened ATR buffer", "stop_tf": tf_label,
                "target": round(target, 10), "rr": round(rr, 2), "rr_max": 2.0, "tps": tps,
                "revert": True, "kind": "reversion"}


def leaderboard_setups(symbol, highs, lows, closes, vols, ema_now, tfb, bias,
                       ms, ksup, kres, rv, detectors, btc_trend=None, tf_label=None) -> tuple:
    """Grade the BEST LONG and BEST SHORT for this coin from data already computed
    in the scan (no extra API calls). Returns (long_setup, short_setup) — each a
    0-100 composite with a plain-English `why`, so the Top-setups tabs can rank the
    whole universe instead of only the handful of pattern hits. Clicking a row opens
    the full cross-timeframe Analyze card.

    Blends: market structure, multi-timeframe bias agreement, 200-EMA side,
    momentum (rate of change), relative volume, pattern-detector confluence, and
    proximity to the nearest support (long) / resistance (short) for entry quality."""
    price = closes[-1]
    atr_val = atr(highs, lows, closes)
    atr_pct = round(atr_val / price * 100, 2) if price else None
    roc = (price / closes[-11] - 1) if len(closes) > 11 else 0.0
    struct = ms.get("structure")
    choch = ms.get("choch")
    bull_tf = sum(1 for v in tfb.values() if v == "bullish")
    bear_tf = sum(1 for v in tfb.values() if v == "bearish")
    n_tf = sum(1 for v in tfb.values() if v)
    # nearest support below / resistance above (closest of the per-TF levels)
    sups = [s for s in (ksup.get("sup_4h"), ksup.get("sup_1d"), ksup.get("sup_1w"))
            if s and s < price]
    ress = [r for r in (kres.get("res_4h"), kres.get("res_1d"), kres.get("res_1w"))
            if r and r > price]
    near_sup = max(sups) if sups else None
    near_res = min(ress) if ress else None
    gap_sup = (price - near_sup) / price if near_sup else None
    gap_res = (near_res - price) / price if near_res else None
    bull_dets = [k for k in ("ema", "flag", "cpr", "bounce", "wedge", "stb", "early")
                 if detectors.get(k)]
    bear_dets = [k for k in ("short",) if detectors.get(k)]

    def _tf_label(cnt, tot, word):
        return f"{cnt}/{tot} timeframes {word}" if tot else None

    # ---- LONG ----
    ls, lw = 0.0, []
    if struct == "uptrend":
        ls += 25; lw.append("Uptrend structure")
    elif choch == "bullish":
        ls += 22; lw.append("Bullish CHoCH (structure just flipped up)")
    elif struct == "range":
        ls += 8; lw.append("Ranging (no clear trend)")
    if bull_tf:
        ls += min(18, bull_tf * 6)
        lbl = _tf_label(bull_tf, n_tf, "bullish")
        if lbl:
            lw.append(lbl)
    if ema_now and price > ema_now:
        if price > ema_now * 1.25:
            ls += 4; lw.append("Extended above 200-EMA (chasing risk)")
        else:
            ls += 10; lw.append("Above 200-EMA")
    if roc > 0:
        ls += min(12, roc * 80)
        if roc > 0.03:
            lw.append(f"+{roc*100:.1f}% momentum (10 candles)")
    if rv and rv > 1:
        ls += min(8, (rv - 1) * 8)
        if rv > 1.3:
            lw.append(f"{rv:.1f}× relative volume")
    if bull_dets:
        ls += min(20, len(bull_dets) * 5)
        lw.append("Pattern confluence: " + ", ".join(bull_dets))
    if gap_sup is not None and gap_sup < 0.08:
        ls += 10 * max(0.0, 1 - gap_sup / 0.08)
        lw.append(f"Near support ({gap_sup*100:.1f}% away) — clean entry")
    ls = round(max(0.0, min(100.0, ls)), 1)

    # ---- SHORT ----
    ss, sw = 0.0, []
    if struct == "downtrend":
        ss += 25; sw.append("Downtrend structure")
    elif choch == "bearish":
        ss += 22; sw.append("Bearish CHoCH (structure just flipped down)")
    elif struct == "range":
        ss += 8; sw.append("Ranging (no clear trend)")
    if bear_tf:
        ss += min(18, bear_tf * 6)
        lbl = _tf_label(bear_tf, n_tf, "bearish")
        if lbl:
            sw.append(lbl)
    if ema_now and price < ema_now:
        if price < ema_now * 0.75:
            ss += 4; sw.append("Extended below 200-EMA (chasing risk)")
        else:
            ss += 10; sw.append("Below 200-EMA")
    if roc < 0:
        ss += min(12, -roc * 80)
        if roc < -0.03:
            sw.append(f"{roc*100:.1f}% momentum (10 candles)")
    if rv and rv > 1:
        ss += min(8, (rv - 1) * 8)
        if rv > 1.3:
            sw.append(f"{rv:.1f}× relative volume")
    if bear_dets:
        ss += min(20, len(bear_dets) * 12)
        sw.append("Breakdown/retest confirmed")
    if gap_res is not None and gap_res < 0.08:
        ss += 10 * max(0.0, 1 - gap_res / 0.08)
        sw.append(f"Near resistance ({gap_res*100:.1f}% away) — clean entry")
    ss = round(max(0.0, min(100.0, ss)), 1)

    # Compact recommended trade for the leaderboard row — a pullback entry on the
    # nearest structural level, a stop beyond the next level, target at the next level
    # the other way, and the resulting R:R. It's a quick preview; the ⚲ click opens the
    # full cross-timeframe engine (which can refine all three across timeframes).
    atr_frac = (atr_val / price) if price else 0.02
    buf = max(0.015, 1.2 * atr_frac)
    _sup_map = [(ksup.get("sup_4h"), "4h"), (ksup.get("sup_1d"), "Daily"),
                (ksup.get("sup_1w"), "Weekly")]
    _res_map = [(kres.get("res_4h"), "4h"), (kres.get("res_1d"), "Daily"),
                (kres.get("res_1w"), "Weekly")]

    def _tf_of(lvl, mp):
        if lvl is None:
            return None
        for v, lbl in mp:
            if v is not None and abs(v - lvl) / max(abs(lvl), 1e-12) < 1e-6:
                return lbl
        return None

    # Recent swing for projection-based targets (Fib extensions / measured moves).
    _sw = 40 if len(closes) >= 40 else len(closes)
    _hi = max(highs[-_sw:]) if _sw >= 5 else price
    _lo = min(lows[-_sw:]) if _sw >= 5 else price
    _rng = max(_hi - _lo, price * 0.005, 1e-9)

    def _smart_tps(long, entry, risk):
        """Target ladder built from REAL levels price is actually pulled toward: multi-TF
        swing highs/lows, Fibonacci extensions of the recent swing, leverage-liquidation
        magnets and round numbers — each labelled with its 'why'. Arbitrary 'R measured
        moves' are a LAST-RESORT fallback only (added later if fewer than 2 real levels)."""
        cands = []
        if long:
            for r in sorted([x for x in ress if x > entry * 1.005]):
                tf = _tf_of(r, _res_map)
                cands.append((r, f"{tf} swing-high resistance" if tf else "overhead resistance"))
            for k, lab in ((0.272, "0.27"), (0.618, "0.62"), (1.0, "1.0"), (1.618, "1.62")):
                lv = _hi + k * _rng
                if lv > entry * 1.01:
                    cands.append((lv, f"{lab}× Fib extension of the swing"))
            rn = _round_number(entry, True)
            if rn and rn > entry * 1.01:
                cands.append((rn, "round-number magnet"))
            for z in _liq_above:
                if z > entry * 1.01:
                    cands.append((z, "short-liquidation magnet (est.)"))
            cands = [c for c in cands if c[0] > entry * 1.005]
            cands.sort(key=lambda c: c[0])
        else:
            for s in sorted([x for x in sups if x < entry * 0.995], reverse=True):
                tf = _tf_of(s, _sup_map)
                cands.append((s, f"{tf} swing-low support" if tf else "support below"))
            for k, lab in ((0.272, "0.27"), (0.618, "0.62"), (1.0, "1.0"), (1.618, "1.62")):
                lv = _lo - k * _rng
                if 0 < lv < entry * 0.99:
                    cands.append((lv, f"{lab}× Fib extension of the swing"))
            rn = _round_number(entry, False)
            if rn and 0 < rn < entry * 0.99:
                cands.append((rn, "round-number magnet"))
            for z in _liq_below:
                if 0 < z < entry * 0.99:
                    cands.append((z, "long-liquidation magnet (est.)"))
            cands = [c for c in cands if 0 < c[0] < entry * 0.995]
            cands.sort(key=lambda c: -c[0])
        tps = []
        for lvl, basis in cands:
            if tps and abs(lvl - tps[-1]["lvl"]) / max(tps[-1]["lvl"], 1e-9) < 0.006:
                continue
            tps.append({"lvl": round(lvl, 10), "rr": round(abs(lvl - entry) / risk, 2), "basis": basis})
            if len(tps) >= 4:
                break
        k = 2
        while len(tps) < 2:
            lvl = entry + k * risk if long else entry - k * risk
            if lvl > 0:
                tps.append({"lvl": round(lvl, 10), "rr": float(k), "basis": f"{k}R measured move"})
            k += 1
        return tps

    # Estimated leverage-liquidation magnets around price (used as target context).
    _liq_below = [price * (1 - 1.0 / lv) for lv in (25, 10)]     # long liqs below
    _liq_above = [price * (1 + 1.0 / lv) for lv in (25, 10)]     # short liqs above

    def _plan(long):
        if long:
            ins = sorted(sups, reverse=True)                 # supports below, nearest first
            outs = sorted(ress)                              # resistances above, nearest first
            near = ins[0] if (ins and (price - ins[0]) / price <= 0.06) else None
            # BREAKOUT entry: with real up-momentum and a resistance just overhead — and
            # price NOT already sitting on support — the smart entry is the BREAK above
            # that resistance (a limit ABOVE current price), stopped if the break fails.
            brk = next((r for r in outs if (r - price) / price <= 0.045), None)
            use_brk = (brk is not None and roc > 0.008
                       and (struct == "uptrend" or choch == "bullish")
                       and (near is None or (price - near) / price > 0.025))
            if use_brk:
                entry = round(brk * 1.0015, 10)
                entry_tf = _tf_of(brk, _res_map)
                entry_basis = ((f"break above the {entry_tf} resistance" if entry_tf
                                else "break above overhead resistance")
                               + " — momentum entry (buy-stop ABOVE price)")
                stop = round(brk * (1 - max(0.006, 0.8 * buf)), 10)
                _bs = [s for s in ins if s < stop]
                if _bs:
                    stop = _bs[0]
                stop_tf = _tf_of(stop, _sup_map)
                stop_basis = ("back below the broken level"
                              + (f" ({stop_tf} swing low)" if stop_tf else "") + " — break failed")
            else:
                entry = near if near is not None else price
                entry_tf = _tf_of(entry, _sup_map)
                if entry > price * 1.001:
                    # Price has already fallen BELOW this support → not a pullback but a
                    # RECLAIM (a buy-stop above the current price; only a long once reclaimed).
                    entry_basis = (f"reclaim of the {entry_tf} level — buy-stop ABOVE price"
                                   if entry_tf else "reclaim entry — buy-stop above price")
                else:
                    entry_basis = (f"{entry_tf} swing-low support — buy the pullback"
                                   if entry_tf else "current price (nearest support is too far to wait for)")
                below = [s for s in ins if s < entry * 0.999]
                # Stop sized to THIS coin's volatility — ~1.4×ATR below the support with a
                # small 0.8% floor so a 4h swing isn't wicked out, but the distance stays
                # individual (a quiet coin gets a tighter stop than a volatile one).
                _min = max(0.011, 1.85 * atr_frac)   # wider stop — backtest: stops too tight
                if below:
                    stop = round(min(below[0] - 0.25 * atr_val, entry * (1 - _min)), 10)
                    stop_tf = _tf_of(below[0], _sup_map)
                    stop_basis = (f"below the {stop_tf} swing low (ATR-buffered)" if stop_tf
                                  else "below support, ATR-buffered")
                else:
                    stop = round(entry * (1 - buf), 10)
                    stop_tf = None
                    stop_basis = "≈1.2×ATR below entry (volatility stop)"
            risk = entry - stop
            if risk <= 0:
                return {}
            tps = _smart_tps(True, entry, risk)
            return {"entry": round(entry, 10), "entry_basis": entry_basis, "entry_tf": entry_tf,
                    "stop": round(stop, 10), "stop_basis": stop_basis, "stop_tf": stop_tf,
                    "breakout": bool(use_brk),
                    "target": tps[0]["lvl"], "rr": tps[0]["rr"],
                    "rr_max": tps[-1]["rr"], "tps": tps}
        ins = sorted(ress)                                   # resistances above, nearest first
        outs = sorted(sups, reverse=True)                    # supports below, nearest first
        near = ins[0] if (ins and (ins[0] - price) / price <= 0.06) else None
        # BREAKDOWN entry (mirror): down-momentum + support just below → SHORT the break
        # below that support (a sell-stop BELOW current price).
        brk = next((s for s in outs if (price - s) / price <= 0.045), None)
        use_brk = (brk is not None and roc < -0.008
                   and (struct == "downtrend" or choch == "bearish")
                   and (near is None or (near - price) / price > 0.025))
        if use_brk:
            entry = round(brk * 0.9985, 10)
            entry_tf = _tf_of(brk, _sup_map)
            entry_basis = ((f"break below the {entry_tf} support" if entry_tf
                            else "break below support")
                           + " — momentum entry (sell-stop BELOW price)")
            stop = round(brk * (1 + max(0.006, 0.8 * buf)), 10)
            _bs = [r for r in ins if r > stop]
            if _bs:
                stop = _bs[0]
            stop_tf = _tf_of(stop, _res_map)
            stop_basis = ("back above the broken level"
                          + (f" ({stop_tf} swing high)" if stop_tf else "") + " — break failed")
        else:
            entry = near if near is not None else price
            entry_tf = _tf_of(entry, _res_map)
            entry_basis = (f"{entry_tf} swing-high resistance — sell into strength"
                           if entry_tf else "current price (nearest resistance is too far to wait for)")
            above = [r for r in ins if r > entry * 1.001]
            # Stop sized to THIS coin's volatility — ~1.4×ATR above the resistance, 0.8% floor.
            _min = max(0.011, 1.85 * atr_frac)   # wider stop — backtest: stops too tight
            if above:
                stop = round(max(above[0] + 0.25 * atr_val, entry * (1 + _min)), 10)
                stop_tf = _tf_of(above[0], _res_map)
                stop_basis = (f"above the {stop_tf} swing high (ATR-buffered)" if stop_tf
                              else "above resistance, ATR-buffered")
            else:
                stop = round(entry * (1 + buf), 10)
                stop_tf = None
                stop_basis = "≈1.2×ATR above entry (volatility stop)"
        risk = stop - entry
        if risk <= 0:
            return {}
        tps = _smart_tps(False, entry, risk)
        return {"entry": round(entry, 10), "entry_basis": entry_basis, "entry_tf": entry_tf,
                "stop": round(stop, 10), "stop_basis": stop_basis, "stop_tf": stop_tf,
                "breakout": bool(use_brk),
                "target": (tps[0]["lvl"] if tps else None), "rr": (tps[0]["rr"] if tps else None),
                "rr_max": (tps[-1]["rr"] if tps else None), "tps": tps}

    lp, sp = _plan(True), _plan(False)

    # ---- PROVEN EDGE: trend-aligned reversion (from the whole-universe backtest) ----------
    # If the CURRENT bar is a BTC-aligned oversold snap-back in an uptrend (long) / overbought
    # roll-over in a downtrend (short), REPLACE the generic level plan with the reversion plan
    # and boost the grade — this is the mechanic the backtest proved wins, so it should lead
    # the board. Non-reversion setups still appear (graded normally) so the board never empties.
    _e200s = ema(closes, 200)
    _e20s = ema(closes, 20)
    _rsis = rsi_series(closes)
    _rl = _revert_live("long", closes, highs, lows, ema_now, atr_val, _rsis, _e200s, _e20s, tf_label, btc_trend)
    _rs = _revert_live("short", closes, highs, lows, ema_now, atr_val, _rsis, _e200s, _e20s, tf_label, btc_trend)
    if _rl:
        lp = _rl
        ls = round(min(100.0, ls + 22), 1)
        lw.insert(0, "Trend-aligned reversion — oversold snap-back in an uptrend (backtest-proven edge)")
    if _rs:
        sp = _rs
        ss = round(min(100.0, ss + 22), 1)
        sw.insert(0, "Trend-aligned reversion — overbought roll-over in a downtrend (backtest-proven edge)")

    # A great setup needs BOTH conviction AND a tradeable reward:risk. A strong trend
    # with no room to run (target already at resistance → poor R:R) is NOT a top setup.
    # Final rank score = conviction × an R:R multiplier, so junk-R:R coins sink.
    def _quality(conv, plan):
        rr = plan.get("rr")
        if rr is None:
            mult = 0.45
        else:
            # Reward a solid first target AND real ROOM TO RUN (a distant runner) — an
            # amazing trade has both a clean R:R and space to a far structural target, so
            # tight scalp-range coins sink and coins with real move potential rise.
            rmax = plan.get("rr_max") or rr
            eff = 0.7 * rr + 0.3 * min(rmax, 6.0)
            mult = max(0.40, min(1.45, 0.45 + 0.26 * min(eff, 3.6)))
        return round(max(0.0, min(100.0, conv * mult)), 1)

    base = {"symbol": symbol, "price": price, "atr_pct": atr_pct, "rvol": rv,
            "bias": bias, "tf_bias": tfb,
            "pct_vs_ema": (round((price / ema_now - 1) * 100, 2) if ema_now else None)}
    # Flag CMP / momentum entries (no clean pullback level to limit at). The backtest
    # proved these lose money vs limit-at-a-level entries, so the gate drops them (except
    # deliberate breakouts, which are buy/sell-stops by design).
    for _pl in (lp, sp):
        if _pl:
            _pl["entry_cmp"] = (("current price" in (_pl.get("entry_basis") or ""))
                                and not _pl.get("breakout"))
    long_setup = {**base, "side": "long", "score": _quality(ls, lp), "conviction": ls,
                  "why": lw, "near_level": near_sup, "near_kind": "support", **lp}
    short_setup = {**base, "side": "short", "score": _quality(ss, sp), "conviction": ss,
                   "why": sw, "near_level": near_res, "near_kind": "resistance", **sp}
    return long_setup, short_setup


def bbw_squeeze_pct(closes, per: int = 20, window: int = 120):
    """How squeezed a close series is RIGHT NOW: the percentile of the current
    Bollinger-band width within its own recent range. Returns 0-100 (100 = tightest
    it's been in `window` bars) or None if there isn't enough history. Reusable across
    timeframes so we can say WHICH timeframes are coiled."""
    n = len(closes)
    if n < max(40, per + 20):
        return None

    def _std(xs):
        m = sum(xs) / len(xs)
        return (sum((x - m) ** 2 for x in xs) / len(xs)) ** 0.5

    bbw = []
    for i in range(per, n + 1):
        w = closes[i - per:i]
        m = sum(w) / per
        if m > 0:
            bbw.append(4 * _std(w) / m)
    if len(bbw) < 30:
        return None
    cur = bbw[-1]
    recent = bbw[-window:]
    rank = sum(1 for x in recent if x < cur) / len(recent)
    return round((1.0 - rank) * 100)


def squeeze_setup(symbol, highs, lows, closes, tfb, bias, atr_pct, ksup, kres):
    """Grade how COILED a coin is — how likely it is to make a big move SOON — from a
    volatility squeeze: Bollinger-band width compressed to a low of its own recent
    range, ATR contracting vs its prior baseline, and price coiling in a tight range.
    A market that has been quiet for a while tends to expand; this finds the quiet ones
    that are wound tightest. Returns a 0-100 'coil' score with a directional lean and
    the break-up / break-down trigger levels. No extra API calls — all from the scan's
    own candles."""
    n = len(closes)
    if n < 60:
        return None
    price = closes[-1]
    if not price:
        return None

    def _std(xs):
        m = sum(xs) / len(xs)
        return (sum((x - m) ** 2 for x in xs) / len(xs)) ** 0.5

    per = 20
    bbw = []                                   # Bollinger-band width (as % of price) series
    for i in range(per, n + 1):
        w = closes[i - per:i]
        m = sum(w) / per
        if m > 0:
            bbw.append(4 * _std(w) / m)        # (upper − lower)/mid = 4σ/mid
    if len(bbw) < 40:
        return None
    cur = bbw[-1]
    recent = bbw[-120:]
    rank = sum(1 for x in recent if x < cur) / len(recent)   # 0 = tightest in the window
    squeeze = 1.0 - rank                                     # 1 = maximally squeezed
    tighter_than = round((1.0 - rank) * 100)

    rATR = atr(highs[-20:], lows[-20:], closes[-20:])
    pATR = (atr(highs[-120:-20], lows[-120:-20], closes[-120:-20])
            if n >= 120 else atr(highs[:-20], lows[:-20], closes[:-20]))
    ratio = (rATR / pATR) if pATR else 1.0
    contr = max(0.0, min(1.0, (1.0 - ratio) / 0.5))

    tight_raw = _std(closes[-12:]) / price
    tight = max(0.0, min(1.0, (0.03 - tight_raw) / 0.03))

    score = round(100 * (0.60 * squeeze + 0.25 * contr + 0.15 * tight), 1)

    bull = sum(1 for v in tfb.values() if v == "bullish")
    bear = sum(1 for v in tfb.values() if v == "bearish")
    lean = "bullish" if bull > bear else "bearish" if bear > bull else "neutral"
    near_res = min([r for r in (kres.get("res_4h"), kres.get("res_1d"), kres.get("res_1w"))
                    if r and r > price], default=None)
    near_sup = max([s for s in (ksup.get("sup_4h"), ksup.get("sup_1d"), ksup.get("sup_1w"))
                    if s and s < price], default=None)

    # WHICH timeframes are coiled — squeeze % on 4h (native) plus Daily/Weekly
    # downsampled from the 4h closes (cheap). 15m/1h are filled in a second pass for
    # the top coils (they need finer candles). A coil confirmed on MULTIPLE timeframes
    # is a bigger, more explosive setup, so we bonus the score for TF confluence.
    coiled_tfs = {"4h": tighter_than}
    _d_cl = closes[5::6]          # ~Daily closes from 4h candles
    _w_cl = closes[41::42]        # ~Weekly closes from 4h candles
    _pd = bbw_squeeze_pct(_d_cl)
    if _pd is not None:
        coiled_tfs["1d"] = _pd
    _pw = bbw_squeeze_pct(_w_cl)
    if _pw is not None:
        coiled_tfs["1w"] = _pw
    _n_coiled = sum(1 for v in coiled_tfs.values() if v >= 70)
    if _n_coiled >= 2:
        score = round(min(100.0, score + 4 * (_n_coiled - 1)), 1)   # multi-TF confluence bonus

    why = []
    if squeeze >= 0.55:
        why.append(f"Bollinger squeeze — band width tighter than {tighter_than}% of its recent range")
    if contr > 0.15:
        why.append(f"Volatility contracting — ATR {round((1 - ratio) * 100)}% below its prior baseline")
    if tight > 0.3:
        why.append(f"Coiling in a tight {round(tight_raw * 100, 1)}% range")
    if lean == "bullish" and near_res:
        why.append("Leaning bullish — watch for a break UP through resistance")
    elif lean == "bearish" and near_sup:
        why.append("Leaning bearish — watch for a break DOWN through support")
    else:
        why.append("Direction unresolved — trade the break, either way")

    # Full breakout plan off the coil, BOTH ways. LONG on a break above the range top,
    # SHORT below the bottom. Two limit entries (a retest limit at the edge + a break-
    # confirm just beyond), a tight stop back INSIDE the range (break failed), and a
    # THREE-target ladder — a 1× measured move, a 1.618× Fibonacci extension, and a 2×
    # / next higher-timeframe level — so the R:R runs well past 2 into the real move.
    band = max((atr_pct or 1.5) / 100 * 2, 0.012)
    top = near_res if near_res else price * (1 + band)
    bot = near_sup if near_sup else price * (1 - band)
    plan_long = plan_short = None
    if top > bot:
        rng = top - bot
        mid = (top + bot) / 2.0

        def _tp_row(lvl, entry, stop, long, basis):
            risk = (entry - stop) if long else (stop - entry)
            rr = round((lvl - entry) / risk, 2) if long and risk > 0 else \
                 round((entry - lvl) / risk, 2) if (not long) and risk > 0 else None
            return {"lvl": round(lvl, 10), "rr": rr, "basis": basis}

        _top_tf = ("4h" if near_res == kres.get("res_4h") else
                   "Daily" if near_res == kres.get("res_1d") else
                   "Weekly" if near_res == kres.get("res_1w") else None)
        _bot_tf = ("4h" if near_sup == ksup.get("sup_4h") else
                   "Daily" if near_sup == ksup.get("sup_1d") else
                   "Weekly" if near_sup == ksup.get("sup_1w") else None)
        # LONG break-up
        l_stop = mid
        _res_far = sorted([r for r in (kres.get("res_1d"), kres.get("res_1w"))
                           if r and r > top + 1.9 * rng])
        _l_far_basis = ("Daily/Weekly resistance overhead" if _res_far else "2× measured move")
        l_lvls = [(top + rng, "1× measured move (range height projected)"),
                  (top + 1.618 * rng, "1.618× Fibonacci extension"),
                  ((_res_far[0] if _res_far else top + 2.5 * rng), _l_far_basis)]
        plan_long = {
            "entry": round(top, 10),                       # retest limit at the break level
            "entry_break": round(top * 1.002, 10),         # break-confirm entry
            "entry_basis": (f"break above the range top ({_top_tf} resistance)" if _top_tf
                            else "break above the range top"),
            "stop": round(l_stop, 10),
            "stop_basis": "mid-range — a close back inside the range = the break failed",
            "tps": [_tp_row(x, top, l_stop, True, b) for x, b in l_lvls],
            "rr": _tp_row(l_lvls[-1][0], top, l_stop, True, "")["rr"],
        }
        # SHORT break-down
        s_stop = mid
        _sup_far = sorted([s for s in (ksup.get("sup_1d"), ksup.get("sup_1w"))
                           if s and s < bot - 1.9 * rng], reverse=True)
        _s_far_basis = ("Daily/Weekly support below" if _sup_far else "2× measured move")
        s_lvls = [(bot - rng, "1× measured move (range height projected)"),
                  (bot - 1.618 * rng, "1.618× Fibonacci extension"),
                  ((_sup_far[0] if _sup_far else bot - 2.5 * rng), _s_far_basis)]
        plan_short = {
            "entry": round(bot, 10),
            "entry_break": round(bot * 0.998, 10),
            "entry_basis": (f"break below the range bottom ({_bot_tf} support)" if _bot_tf
                            else "break below the range bottom"),
            "stop": round(s_stop, 10),
            "stop_basis": "mid-range — a close back inside the range = the break failed",
            "tps": [_tp_row(x, bot, s_stop, False, b) for x, b in s_lvls if x > 0],
            "rr": _tp_row(s_lvls[-1][0], bot, s_stop, False, "")["rr"] if s_lvls[-1][0] > 0 else None,
        }
    rec_side = "long" if lean == "bullish" else "short" if lean == "bearish" else "either"

    return {"symbol": symbol, "side": lean, "score": score, "why": why,
            "price": price, "atr_pct": atr_pct, "tf_bias": tfb, "bias": bias,
            "squeeze_pct": tighter_than, "coiled_tfs": coiled_tfs,
            "near_res": near_res, "near_sup": near_sup,
            "plan_long": plan_long, "plan_short": plan_short, "rec_side": rec_side}


def coil_tfs_finer(sess: requests.Session, symbol: str, market: str) -> dict:
    """5m / 15m / 1h squeeze % for one coin — the finer timeframes the 4h universe
    scan can't derive. Called only for the top coils (cheap), to complete the 5m→1W
    coiled-timeframes picture."""
    out = {}
    for iv, key in (("5m", "5m"), ("15m", "15m"), ("1h", "1h")):
        try:
            rr = fetch_candles(sess, symbol, iv, 400, market)
            if rr and len(rr) > 41:
                p = bbw_squeeze_pct([float(x[4]) for x in rr[:-1]])
                if p is not None:
                    out[key] = p
        except (requests.RequestException, ValueError, IndexError, TypeError):
            pass
    return out


def scalp_setup(sess, symbol, market, side, htf_conv, htf_tf_bias):
    """A MULTI-TIMEFRAME scalp: the higher timeframes set the DIRECTION, and the lower
    timeframes give the trigger — the 5m provides the precise, TIGHT entry/stop, the
    15m the structure and the bigger targets. Only taken WITH the higher-timeframe
    trend, and only when 5m + 15m agree with it. Second-pass fetch (5m + 15m)."""
    def _series(iv, lim):
        rr = fetch_candles(sess, symbol, iv, lim, market)
        if not rr or len(rr) < 40:
            return None
        rws = rr[:-1]
        try:
            return ([float(x[2]) for x in rws], [float(x[3]) for x in rws],
                    [float(x[4]) for x in rws], [float(x[5]) for x in rws])
        except (ValueError, IndexError):
            return None
    s5 = _series("5m", 500)
    s15 = _series("15m", 400)
    if not s5 and not s15:
        return None
    # Entry timeframe = 15m — 5m is too noisy for crypto (stops get wicked out on ordinary
    # chop). The 15m gives a cleaner structure and a stop that can actually breathe. 5m is
    # only a fallback if the 15m fetch failed.
    (H, L, C, Vv), etf = (s15, "15m") if s15 else (s5, "5m")
    price = C[-1]
    if not price:
        return None
    a = atr(H, L, C)
    atrp = round(a / price * 100, 2)
    ms_e = market_structure(H, L, C)
    r14 = rsi(C)
    rvv = rel_volume(Vv)
    last = len(L) - 1

    def _bias_of(s):
        if not s:
            return None
        m = market_structure(s[0], s[1], s[2])
        return ("bullish" if m["structure"] == "uptrend" or m["choch"] == "bullish"
                else "bearish" if m["structure"] == "downtrend" or m["choch"] == "bearish"
                else "neutral")
    bias5, bias15 = _bias_of(s5), _bias_of(s15)
    long = side == "long"

    # Fine-TF swing levels for the tight entry/stop; blend in 15m levels for targets.
    fine_sup = [x for x in supports_below(L, last, price, max_n=4, min_gap=0.002) if x < price]
    fine_res = [x for x in resistances_above(H, last, price, max_n=4, min_gap=0.002) if x > price]
    cand_up = [(x, etf) for x in fine_res]
    cand_dn = [(x, etf) for x in fine_sup]
    if s15 and etf != "15m":
        l15 = len(s15[1]) - 1
        cand_up += [(x, "15m") for x in resistances_above(s15[0], l15, price, max_n=3, min_gap=0.003) if x > price]
        cand_dn += [(x, "15m") for x in supports_below(s15[1], l15, price, max_n=3, min_gap=0.003) if x < price]

    if long:
        near = fine_sup[0] if fine_sup else price * (1 - 0.004)
        entry = near if (price - near) / price <= 0.008 else price
        entry_basis = (f"{etf} swing-low support — buy the dip" if (price - near) / price <= 0.008
                       else f"current price ({etf} momentum entry)")
        below = fine_sup[1] if len(fine_sup) > 1 else near * 0.998
        # Stop must clear ordinary 5m noise — a 0.3×ATR stop gets wicked out before the
        # target ever prints. Floor it at max(1.2×ATR, 1.0%) beyond the entry.
        _sfloor = max(1.2 * a, entry * 0.010)
        stop = min(below - 0.4 * a, entry - _sfloor)
        stop_basis = f"below the next {etf} swing low, ≥1.2×ATR to clear 5m noise"
        risk = entry - stop
        if risk <= 0:
            return None
        outs = sorted({round(x, 10): tf for x, tf in cand_up if x > entry * 1.0015}.items())
        tps = []
        for lvl, tf in outs:
            if not tps or lvl > tps[-1]["lvl"] * 1.0015:
                tps.append({"lvl": lvl, "rr": round((lvl - entry) / risk, 2),
                            "basis": f"{tf} swing-high resistance"})
            if len(tps) >= 3:
                break
        k = 2
        while len(tps) < 2:
            tps.append({"lvl": round(entry + k * risk, 10), "rr": float(k), "basis": f"{k}R measured move"})
            k += 1
    else:
        near = fine_res[0] if fine_res else price * (1 + 0.004)
        entry = near if (near - price) / price <= 0.008 else price
        entry_basis = (f"{etf} swing-high resistance — sell the rip" if (near - price) / price <= 0.008
                       else f"current price ({etf} momentum entry)")
        above = fine_res[1] if len(fine_res) > 1 else near * 1.002
        # Same noise floor for shorts — a 0.3×ATR stop is inside normal 5m chop.
        _sfloor = max(1.2 * a, entry * 0.010)
        stop = max(above + 0.4 * a, entry + _sfloor)
        stop_basis = f"above the next {etf} swing high, ≥1.2×ATR to clear 5m noise"
        risk = stop - entry
        if risk <= 0:
            return None
        outs = sorted({round(x, 10): tf for x, tf in cand_dn if x < entry * 0.9985}.items(), reverse=True)
        tps = []
        for lvl, tf in outs:
            if not tps or lvl < tps[-1]["lvl"] * 0.9985:
                tps.append({"lvl": lvl, "rr": round((entry - lvl) / risk, 2),
                            "basis": f"{tf} swing-low support"})
            if len(tps) >= 3:
                break
        k = 2
        while len(tps) < 2 and entry - k * risk > 0:
            tps.append({"lvl": round(entry - k * risk, 10), "rr": float(k), "basis": f"{k}R measured move"})
            k += 1

    # A scalp doesn't need a tight ladder — TPs 0.2% apart are just noise and fees. Keep
    # ONE clean primary target and add a runner ONLY if it's meaningfully further out
    # (≥1.6× the first target's reward AND ≥0.8% beyond it). Otherwise, take 100% at TP1.
    if tps:
        _prim = tps[0]
        _runner = next((t for t in tps[1:]
                        if (t.get("rr") or 0) >= (_prim.get("rr") or 0) * 1.6
                        and abs(t["lvl"] - _prim["lvl"]) / entry >= 0.008), None)
        tps = [_prim] + ([_runner] if _runner else [])

    risk_frac = risk / entry
    if risk_frac > 0.04 or not tps:            # too wide to be a scalp
        return None
    rr_base = tps[0]["rr"]
    mult_rr = max(0.4, min(1.3, 0.45 + 0.28 * min(rr_base, 3.0)))
    tight = max(0.0, min(1.0, (0.03 - risk_frac) / 0.025))
    # Alignment: penalise if 5m or 15m is trending AGAINST the HTF direction.
    want = "bullish" if long else "bearish"
    opp5 = bias5 is not None and bias5 == ("bearish" if long else "bullish")
    opp15 = bias15 is not None and bias15 == ("bearish" if long else "bullish")
    align = 1.0 - (0.25 if opp5 else 0) - (0.25 if opp15 else 0)
    agree = sum(1 for b in (bias5, bias15) if b == want)
    score = round(max(0.0, min(100.0, (htf_conv or 50) * mult_rr * align * (0.8 + 0.2 * tight))), 1)

    _hb = [v for v in (htf_tf_bias or {}).values()]
    _nal = sum(1 for v in _hb if v == want)
    why = [f"HTF {want} — {_nal}/{len(_hb) or 1} higher timeframes aligned"]
    why.append(f"LTF trigger: 5m {bias5 or '—'}, 15m {bias15 or '—'}"
               + (" (both with the trend)" if agree == 2 else " ⚠ (an LTF is against)" if (opp5 or opp15) else ""))
    why.append(f"Tight {risk_frac*100:.1f}% stop on the {etf}")
    why.append(f"R:R {rr_base} to the first target")
    if rvv and rvv > 1.3:
        why.append(f"{rvv:.1f}× {etf} relative volume")

    tf_bias = dict(htf_tf_bias or {})
    if bias5:
        tf_bias["5m"] = bias5
    if bias15:
        tf_bias["15m"] = bias15

    return {"symbol": symbol, "side": side, "score": score, "why": why,
            "price": price, "atr_pct": atrp, "tf_bias": tf_bias, "ltf5": bias5, "ltf15": bias15,
            "entry_tf": etf, "entry": round(entry, 10), "entry_basis": entry_basis,
            "stop": round(stop, 10), "stop_basis": stop_basis,
            "stop_pct": round(risk_frac * 100, 2),
            "target": tps[0]["lvl"], "rr": rr_base, "rr_max": tps[-1]["rr"], "tps": tps,
            "rsi": r14}


def bounce_scalp_setup(sess, symbol, market, side):
    """A COUNTER-TREND (or with-trend) BOUNCE scalp: a quick trade off a STRONG, tested
    lower-timeframe level — buy a wash-out into multi-touch support (long), or fade a
    pop into multi-touch resistance (short). Unlike scalp_setup this does NOT require
    higher-timeframe alignment: the edge is the level + an oversold/overbought snap-back,
    not the trend. Graded on how strong the level is (touches), how stretched RSI is,
    how tight the stop can sit, and whether price is actually snapping back. This is what
    keeps a good scalp on the board even when there's no clean trend setup: 'there is
    always a bounce off strong support/resistance somewhere.' Second-pass fetch (5m+15m)."""
    def _series(iv, lim):
        rr = fetch_candles(sess, symbol, iv, lim, market)
        if not rr or len(rr) < 40:
            return None
        rws = rr[:-1]
        try:
            return ([float(x[2]) for x in rws], [float(x[3]) for x in rws],
                    [float(x[4]) for x in rws], [float(x[5]) for x in rws])
        except (ValueError, IndexError):
            return None
    s5 = _series("5m", 500)
    s15 = _series("15m", 400)
    if not s5 and not s15:
        return None
    # 15m base — 5m is too noisy for crypto; the 15m level + snap is a far cleaner bounce.
    (H, L, C, Vv), etf = (s15, "15m") if s15 else (s5, "5m")
    price = C[-1]
    if not price or len(C) < 30:
        return None
    a = atr(H, L, C)
    if not a or a <= 0:
        return None
    atrp = round(a / price * 100, 2)
    r5 = rsi(C)
    rvv = rel_volume(Vv)
    last = len(L) - 1
    long = side == "long"
    tol = max(0.35 * a, price * 0.0015)          # "at the level" tolerance

    def _touches(level, arr):
        return sum(1 for x in arr if abs(x - level) <= tol)

    def _bias_of(s):
        if not s:
            return None
        m = market_structure(s[0], s[1], s[2])
        return ("bullish" if m["structure"] == "uptrend" or m["choch"] == "bullish"
                else "bearish" if m["structure"] == "downtrend" or m["choch"] == "bearish"
                else "neutral")
    bias5, bias15 = _bias_of(s5), _bias_of(s15)

    if long:
        sups = [x for x in supports_below(L, last, price, max_n=4, min_gap=0.0015) if x < price]
        if not sups:
            return None
        level = sups[0]                          # nearest support below
        dist = (price - level) / price
        if dist > min(0.02, max(0.010, 1.3 * a / price)):   # price must be NEAR support (cap 2%)
            return None
        touches = _touches(level, L)
        # Snap-back confirmation: a green tick off the low, or a clear lower wick that held.
        rng = max(H[-1] - L[-1], 1e-9)
        wick = (C[-1] - L[-1]) / rng
        reversal = (C[-1] >= C[-2]) or (wick >= 0.45 and L[-1] <= level * 1.0015)
        oversold = r5 is not None and r5 < 45
        if not (reversal or oversold):
            return None
        if touches < 2 and not (oversold and reversal):
            return None                          # weak level AND no clear snap = skip
        # Prefer entering at MARKET when the snap-back is already underway (a limit at the
        # level just sits there and goes MISSED as price lifts off). CMP if we're close OR
        # a reversal candle has already confirmed within a wider band.
        use_cmp = dist <= 0.008 or (reversal and dist <= 0.013)
        entry = price if use_cmp else round(level * 1.002, 10)
        entry_basis = (f"bounce underway off {etf} support — enter at market (CMP)"
                       if use_cmp else f"limit at {etf} support {level:.6g} ({touches}× tested)")
        # Stop must sit BELOW the level far enough to clear noise, not hug it: ≥1.2×ATR / 1%.
        stop = level - max(1.2 * a, level * 0.010)
        stop_basis = f"below the {etf} support that's holding, ≥1.2×ATR to clear noise"
        risk = entry - stop
        if risk <= 0:
            return None
        res = [x for x in resistances_above(H, last, price, max_n=4, min_gap=0.002) if x > entry * 1.0015]
        if s15 and etf != "15m":
            l15 = len(s15[0]) - 1
            res += [x for x in resistances_above(s15[0], l15, price, max_n=3, min_gap=0.003) if x > entry * 1.0015]
        res = sorted(set(round(x, 10) for x in res))
        mean_t = round((max(H[-40:]) + min(L[-40:])) / 2, 10)   # snap-back-to-mean target
        if mean_t > entry * 1.002:
            res.append(mean_t)
        res = sorted(set(res))
        tps = []
        for lvl in res:
            if lvl > entry and (not tps or lvl > tps[-1]["lvl"] * 1.002):
                tps.append({"lvl": lvl, "rr": round((lvl - entry) / risk, 2),
                            "basis": "snap-back to mean" if abs(lvl - mean_t) < 1e-9 else f"{etf} resistance"})
            if len(tps) >= 2:
                break
    else:
        ress = [x for x in resistances_above(H, last, price, max_n=4, min_gap=0.0015) if x > price]
        if not ress:
            return None
        level = ress[0]
        dist = (level - price) / price
        if dist > min(0.02, max(0.010, 1.3 * a / price)):
            return None
        touches = _touches(level, H)
        rng = max(H[-1] - L[-1], 1e-9)
        wick = (H[-1] - C[-1]) / rng
        reversal = (C[-1] <= C[-2]) or (wick >= 0.45 and H[-1] >= level * 0.9985)
        overbought = r5 is not None and r5 > 55
        if not (reversal or overbought):
            return None
        if touches < 2 and not (overbought and reversal):
            return None
        use_cmp = dist <= 0.008 or (reversal and dist <= 0.013)
        entry = price if use_cmp else round(level * 0.998, 10)
        entry_basis = (f"rejection underway off {etf} resistance — enter at market (CMP)"
                       if use_cmp else f"limit at {etf} resistance {level:.6g} ({touches}× tested)")
        stop = level + max(1.2 * a, level * 0.010)
        stop_basis = f"above the {etf} resistance that's holding, ≥1.2×ATR to clear noise"
        risk = stop - entry
        if risk <= 0:
            return None
        sup = [x for x in supports_below(L, last, price, max_n=4, min_gap=0.002) if x < entry * 0.9985]
        if s15 and etf != "15m":
            l15 = len(s15[1]) - 1
            sup += [x for x in supports_below(s15[1], l15, price, max_n=3, min_gap=0.003) if x < entry * 0.9985]
        sup = sorted(set(round(x, 10) for x in sup), reverse=True)
        mean_t = round((max(H[-40:]) + min(L[-40:])) / 2, 10)
        if mean_t < entry * 0.998:
            sup.append(mean_t); sup.sort(reverse=True)
        tps = []
        for lvl in sup:
            if lvl < entry and (not tps or lvl < tps[-1]["lvl"] * 0.998):
                tps.append({"lvl": lvl, "rr": round((entry - lvl) / risk, 2),
                            "basis": "snap-back to mean" if abs(lvl - mean_t) < 1e-9 else f"{etf} support"})
            if len(tps) >= 2:
                break

    if not tps:
        return None
    # One clean target + a runner only if it's meaningfully further (≥1.6× reward & ≥0.8%).
    _prim = tps[0]
    _runner = next((t for t in tps[1:]
                    if (t.get("rr") or 0) >= (_prim.get("rr") or 0) * 1.6
                    and abs(t["lvl"] - _prim["lvl"]) / entry >= 0.008), None)
    tps = [_prim] + ([_runner] if _runner else [])
    rr_base = tps[0]["rr"]
    risk_frac = risk / entry
    if risk_frac > 0.05 or rr_base < 1.2:        # too wide, or not enough room to the level
        return None

    # Grade on the merits of a bounce: level strength, RSI stretch, R:R, tightness, snap.
    q_touch = min(1.0, touches / 4.0)
    q_rsi = (max(0.0, (45 - r5) / 25.0) if long else max(0.0, (r5 - 55) / 25.0)) if r5 is not None else 0.3
    q_rsi = min(1.0, q_rsi)
    q_rr = min(1.0, rr_base / 2.5)
    q_tight = max(0.0, min(1.0, (0.045 - risk_frac) / 0.035))
    q_rev = 1.0 if reversal else 0.5
    q_vol = min(1.0, (rvv or 1.0) / 2.0)
    raw = 0.28 * q_touch + 0.24 * q_rsi + 0.20 * q_rr + 0.12 * q_tight + 0.10 * q_rev + 0.06 * q_vol
    score = round(max(0.0, min(100.0, 100.0 * raw)), 1)

    want = "bullish" if long else "bearish"
    against = "bearish" if long else "bullish"
    counter = (bias15 == against) or (bias5 == against and bias15 in (None, "neutral"))
    why = [(f"↩ Counter-trend bounce — {etf} at strong {'support' if long else 'resistance'} "
            f"{level:.6g} ({touches}× tested), fading a {bias15 or bias5 or 'weak'} LTF trend")
           if counter else
           (f"Bounce off {etf} {'support' if long else 'resistance'} {level:.6g} ({touches}× tested)")]
    if r5 is not None:
        why.append(f"RSI {r5:.0f} — {'oversold snap-back' if long else 'overbought rejection'}")
    why.append(f"{'Green tick off the low' if (long and reversal) else 'Rejection candle' if reversal else 'Level reaction'} confirms the turn")
    why.append(f"Tight {risk_frac*100:.1f}% stop just {'below' if long else 'above'} the level")
    why.append(f"R:R {rr_base} to the {'mean/resistance' if long else 'mean/support'}")
    if rvv and rvv > 1.3:
        why.append(f"{rvv:.1f}× {etf} relative volume")

    tf_bias = {}
    if bias5:
        tf_bias["5m"] = bias5
    if bias15:
        tf_bias["15m"] = bias15
    return {"symbol": symbol, "side": side, "score": score, "why": why,
            "price": price, "atr_pct": atrp, "tf_bias": tf_bias, "ltf5": bias5, "ltf15": bias15,
            "entry_tf": etf, "entry": round(entry, 10), "entry_basis": entry_basis,
            "stop": round(stop, 10), "stop_basis": stop_basis,
            "stop_pct": round(risk_frac * 100, 2),
            "target": tps[0]["lvl"], "rr": rr_base, "rr_max": tps[-1]["rr"], "tps": tps,
            "rsi": r5, "kind": "bounce", "counter_trend": bool(counter),
            "level": round(level, 10), "touches": touches}


def _btc_regime_ctx(rows):
    """Per-bar BTC BEHAVIOUR keyed by candle open-time (ms): its TREND (up / down / range)
    from the 200-EMA and the EMA's slope, and its VOLATILITY (hi / lo) from ATR% vs a rolling
    median. Built once per timeframe from BTC's own candles, then used to tag every backtest
    trade with what the market leader was doing at that exact moment — so expectancy can be
    sliced by BTC regime, and the strategy can refuse to fight a strong opposing BTC tape."""
    try:
        H = [float(x[2]) for x in rows]
        L = [float(x[3]) for x in rows]
        C = [float(x[4]) for x in rows]
    except (ValueError, IndexError, TypeError):
        return {}
    n = len(C)
    if n < 60:
        return {}
    e200 = ema(C, 200)
    atrp = [0.0] * n
    for t in range(n):
        a = atr(H[max(0, t - 20):t + 1], L[max(0, t - 20):t + 1], C[max(0, t - 20):t + 1])
        atrp[t] = (a / C[t]) if (a and C[t]) else 0.0
    ctx = {}
    for t in range(n):
        el = e200[t]
        if el is None:
            continue
        prev = e200[t - 20] if t >= 20 else None
        if C[t] > el and (prev is None or el > prev):
            trend = "up"
        elif C[t] < el and (prev is None or el < prev):
            trend = "down"
        else:
            trend = "range"
        win = sorted(atrp[max(0, t - 100):t + 1])
        med = win[len(win) // 2] if win else 0.0
        ctx[int(rows[t][0])] = {"t": trend, "v": ("hi" if atrp[t] > med else "lo")}
    return ctx


def _market_ctx(rows_by, btc_sym="BTCUSDT", lookback=20, min_syms=8):
    """Per-bar MARKET BREADTH & DOMINANCE across the whole basket, keyed by candle open-time.
    breadth = fraction of the basket trading above its OWN 200-EMA at that bar (broad alt
    participation); dom = who led over the last `lookback` bars, BTC or the median alt — a cheap,
    self-contained BTC-dominance proxy (no external BTC.D feed needed). Buckets: risk_on (breadth
    >=55%), risk_off (<=40%), mixed. Used to TAG and GATE every backtest trade with the market
    ENVIRONMENT, so longs can be limited to risk-on breadth instead of firing into broad alt bleed."""
    above = {}      # ts -> [total, above_count]
    btc_ret = {}    # ts -> BTC lookback return
    alt_rets = {}   # ts -> [alt lookback returns]
    for sym, rows in rows_by.items():
        if not rows or len(rows) < 210:
            continue
        try:
            C = [float(x[4]) for x in rows]
            T = [int(x[0]) for x in rows]
        except (ValueError, IndexError, TypeError):
            continue
        e200 = ema(C, 200)
        for i in range(len(C)):
            if e200[i] is None:
                continue
            t = T[i]
            cell = above.setdefault(t, [0, 0])
            cell[0] += 1
            if C[i] > e200[i]:
                cell[1] += 1
            if i >= lookback and C[i - lookback] > 0:
                r = C[i] / C[i - lookback] - 1.0
                if sym == btc_sym:
                    btc_ret[t] = r
                else:
                    alt_rets.setdefault(t, []).append(r)
    ctx = {}
    for t, (tot, ab) in above.items():
        if tot < min_syms:
            ctx[t] = {"br": None, "bpct": (round(ab / tot * 100) if tot else None), "dom": None}
            continue
        b = ab / tot
        br = "risk_on" if b >= 0.55 else ("risk_off" if b <= 0.40 else "mixed")
        dom = None
        ar = alt_rets.get(t)
        if ar and t in btc_ret:
            a_sorted = sorted(ar)
            med = a_sorted[len(a_sorted) // 2]
            dom = "alt" if med > btc_ret[t] else "btc"
        ctx[t] = {"br": br, "bpct": round(b * 100), "dom": dom}
    return ctx


def _bt_by_breadth(trades):
    """Expectancy / win-rate sliced by the market-breadth regime the trade was taken in
    (risk_on / mixed / risk_off) and by dominance (alts vs BTC leading) — the clean read on
    WHICH environment this strategy/side actually makes money in."""
    def blk(pred):
        a = [x for x in trades if pred(x)]
        if not a:
            return None
        w = sum(1 for x in a if x.get("outcome") == "win")
        sm = sum(x.get("r") or 0 for x in a)
        return {"n": len(a), "winrate": round(w / len(a) * 100, 1),
                "exp": round(sm / len(a), 3), "sumR": round(sm, 1)}
    out = {"breadth": {}, "dom": {}}
    for k in ("risk_on", "mixed", "risk_off"):
        r = blk(lambda x, k=k: x.get("breadth") == k)
        if r:
            out["breadth"][k] = r
    for k in ("alt", "btc"):
        r = blk(lambda x, k=k: x.get("dom") == k)
        if r:
            out["dom"][k] = r
    return out



def _btc_block(long, tf, btrend, bvol):
    """The SMART don't-fight-BTC gate (replaces the old blunt block that starved longs). On the
    LOWER timeframes (15m/1h) reversion is noisy, so only trade when the macro tape AGREES —
    long only in a BTC uptrend, short only in a BTC downtrend. On the HIGHER timeframes we're
    happy to trade against a calm BTC drift and only step aside for the VOLATILE opposing tape
    (a BTC dump against a long / a BTC rip against a short). Returns True to SKIP the trade."""
    if not btrend:
        return False
    want = "up" if long else "down"
    opp = "down" if long else "up"
    if tf in ("15m", "1h"):
        return btrend != want
    return btrend == opp and bvol == "hi"


def _bt_revert(rows, side, fees_bps=5.0, horizon=24, warmup=210, btc_ctx=None, tf="4h", mkt_ctx=None):
    """TREND-ALIGNED MEAN REVERSION — the high-win-rate candidate. Only trades WITH the
    higher-timeframe trend (price on the right side of a SLOPING 200-EMA), enters on an
    OVERSOLD (long) / OVERBOUGHT (short) snap-back that has just reversed, and targets the
    nearby mean (20-EMA) for a quick, high-probability bounce. The stop sits beyond the
    flush extreme. In plain terms: buy panic in an uptrend / sell euphoria in a downtrend,
    and take the snap-back to the mean. No look-ahead; net of fees.

    OPTIMIZATION — don't fight BTC: when btc_ctx is supplied (per-bar BTC behaviour keyed by
    candle open-time), skip a LONG while BTC is in a confirmed downtrend and a SHORT while
    BTC is ripping — the alt's own trend is fragile against a strong opposing BTC tape. Every
    kept trade is tagged with the time-of-day session and what BTC was doing, so the analysis
    can slice expectancy by time / BTC regime / BTC volatility."""
    try:
        H = [float(x[2]) for x in rows]
        L = [float(x[3]) for x in rows]
        C = [float(x[4]) for x in rows]
        V = [float(x[5]) for x in rows]
    except (ValueError, IndexError, TypeError):
        return []
    n = len(C)
    if n < warmup + horizon + 5:
        return []
    e200 = ema(C, 200)
    e20 = ema(C, 20)
    rsis = rsi_series(C)
    long = side == "long"
    fee_frac = fees_bps / 10000.0
    _dv = sorted(C[i] * V[i] for i in range(len(C)))
    dvol = _dv[len(_dv) // 2] if _dv else 0.0
    # LEARNED FILTER (from the 2yr segments): calm entries beat choppy ones on both sides. Skip
    # a bar when the coin's ATR% is above its own median — trade the calm, not the chop.
    _atrp = [((atr(H[max(0, i - 20):i + 1], L[max(0, i - 20):i + 1], C[max(0, i - 20):i + 1]) or 0.0) / C[i])
             if C[i] else 0.0 for i in range(n)]
    _asort = sorted(x for x in _atrp if x > 0)
    _atrp_med = _asort[len(_asort) // 2] if _asort else 0.0
    trades = []
    t = warmup
    while t < n - horizon - 1:
        el = e200[t]; e2 = e20[t]; rs = rsis[t]
        if el is None or e2 is None or rs is None:
            t += 1; continue
        price = C[t]
        a = atr(H[max(0, t - 20):t + 1], L[max(0, t - 20):t + 1], C[max(0, t - 20):t + 1])
        if not a or a <= 0:
            t += 1; continue
        prev200 = e200[t - 20] if t >= 20 else None
        ts = int(rows[t][0]) if rows[t] else 0          # candle open-time → BTC state + time-of-day
        _bs = btc_ctx.get(ts) if btc_ctx else None
        btrend = _bs.get("t") if _bs else None
        bvol = _bs.get("v") if _bs else None
        _ms = mkt_ctx.get(ts) if mkt_ctx else None
        mbr = _ms.get("br") if _ms else None
        mdom = _ms.get("dom") if _ms else None
        mbpct = _ms.get("bpct") if _ms else None
        # LEARNED GATES (2yr segments): only trade when BTC is CALM and the coin itself is calm.
        if bvol == "hi":                                 # volatile BTC lost heavily on both sides
            t += 1; continue
        if _atrp_med and _atrp[t] > _atrp_med * 1.15:    # choppy entry — skip, calm beats chop
            t += 1; continue
        entry = stop = target = risk = rr = None
        outcome = rbar = None; mfe = 0.0; mae = 0.0; stp = False
        if long:
            slope_up = (prev200 is None) or (el > prev200)
            if not (price > el and slope_up):           # must be an uptrend
                t += 1; continue
            if _btc_block(True, tf, btrend, bvol):       # smart don't-fight-BTC gate
                t += 1; continue
            if mbr == "risk_off":                        # broad breadth risk-off — don't long alt bleed
                t += 1; continue
            # MOMENTUM continuation — dip-buying LOSES in crypto (2yr backtest), so ride STRENGTH
            # instead: strong-but-not-blown-off, breaking out of the recent range with the trend.
            if not (52 <= rs <= 72):                     # strong momentum, not an exhausted blow-off
                t += 1; continue
            prior_high = max(H[max(0, t - 20):t])        # highest high of the prior 20 bars
            if C[t] <= prior_high:                       # need a fresh breakout close
                t += 1; continue
            entry = price
            base_low = min(L[max(0, t - 20):t + 1])
            swing_low = min(L[max(0, t - 10):t + 1])
            stop = min(swing_low, entry - 1.4 * a)       # below the breakout base
            risk = entry - stop
            if risk <= 0:
                t += 1; continue
            # Individual structural target (measured move) BUT capped tight — the 2yr segments
            # proved R:R < 2 beats R:R ≥ 2, so we take the structural objective only up to ~1.9R.
            measured = prior_high - base_low
            target = min(entry + max(measured, 1.5 * risk), entry + 1.9 * risk)
            rr = (target - entry) / risk
            if rr < 1.2:
                t += 1; continue
            f = t + 1
            for j in range(f, min(n, f + horizon + 1)):
                mfe = max(mfe, (H[j] - entry) / risk)
                mae = max(mae, (entry - L[j]) / risk)      # worst drawdown while open (in R)
                if L[j] <= stop:
                    outcome, rbar = "loss", j; break
                if H[j] >= target:
                    outcome, rbar = "win", j; break
            if outcome is None:
                t = f + 1; continue
            stp = outcome == "loss" and any(H[j] >= target for j in range(rbar, min(n, f + horizon + 1)))
        else:
            slope_dn = (prev200 is None) or (el < prev200)
            if not (price < el and slope_dn):           # must be a downtrend
                t += 1; continue
            if _btc_block(False, tf, btrend, bvol):      # smart don't-fight-BTC gate
                t += 1; continue
            if rs <= 55:                                 # overbought-ish pop (loosened to catch big caps)
                t += 1; continue
            if C[t] >= C[t - 1]:                         # need the reversal down to have started
                t += 1; continue
            entry = price
            flush_high = max(H[max(0, t - 3):t + 1])
            risk = (flush_high + 0.5 * a - entry) * 1.3      # WIDER stop (~30%): losers kept hitting target after being stopped
            stop = entry + risk
            if risk <= 0:
                t += 1; continue
            # Structural target (20-EMA mean / recent swing low) BUT capped tight at ~1.9R —
            # the 2yr segments proved tighter R:R (<2) beats wider on the short side too.
            recent_low = min(L[max(0, t - 20):t])
            struct = min(e2, recent_low) if recent_low < entry * 0.997 else e2
            target = max(min(struct, entry - 1.3 * risk), entry - 1.9 * risk)
            if target <= 0:
                t += 1; continue
            rr = (entry - target) / risk
            if rr < 0.8:
                t += 1; continue
            f = t + 1
            for j in range(f, min(n, f + horizon + 1)):
                mfe = max(mfe, (entry - L[j]) / risk)
                mae = max(mae, (H[j] - entry) / risk)      # worst drawdown while open (in R)
                if H[j] >= stop:
                    outcome, rbar = "loss", j; break
                if L[j] <= target:
                    outcome, rbar = "win", j; break
            if outcome is None:
                t = f + 1; continue
            stp = outcome == "loss" and any(L[j] <= target for j in range(rbar, min(n, f + horizon + 1)))
        rr = round(rr, 2)
        fee_R = fee_frac / (risk / entry)
        R = (rr - fee_R) if outcome == "win" else (-1.0 - fee_R)
        _hour = (ts // 3600000) % 24                      # UTC hour → crypto session
        _session = "Asia" if _hour < 8 else ("EU" if _hour < 14 else "US")
        trades.append({"r": round(R, 3), "rr": rr, "outcome": outcome,
                       "gross": rr if outcome == "win" else -1.0,
                       "mfe": round(min(mfe, rr), 2), "stop_then_tp": bool(stp),
                       "cmp": False, "bars": rbar - f,
                       "extd": round(abs((entry - el) / el) * 100, 2) if el else 0.0,
                       "atrp": round(a / entry * 100, 2), "dvol": dvol,
                       "stopw": round(risk / entry * 100, 2),   # stop width, % of entry
                       "tppct": round(abs(target - entry) / entry * 100, 2),  # target distance, % of entry
                       "mae": round(mae, 2),                     # max drawdown while open, in R
                       "ts": ts,                                 # entry candle time (for the equity curve)
                       "entry": round(entry, 10), "stop": round(stop, 10),
                       "target": round(target, 10), "rsi": round(rs, 1),
                       "kind": ("momentum" if long else "reversion"),
                       "session": _session, "btc_trend": btrend, "btc_vol": bvol,
                       "btc_align": (btrend == ("up" if long else "down")) if btrend else None,
                       "btc_state": ("BTC " + btrend) if btrend else None,
                       "breadth": mbr, "dom": mdom, "bpct": mbpct})
        t = rbar + 1
    return trades


def _st_series(H, L, C, period=10, mult=3.0):
    """Per-bar Supertrend (value, direction) — causal, no look-ahead — for backtesting reactions
    to the trailing ATR trend line. dir 1 = line is SUPPORT below price; -1 = RESISTANCE above."""
    n = len(C)
    st = [None] * n; d = [0] * n
    if n < period + 2:
        return st, d
    tr = [0.0] * n
    for i in range(1, n):
        tr[i] = max(H[i] - L[i], abs(H[i] - C[i - 1]), abs(L[i] - C[i - 1]))
    atr_s = [None] * n
    atr_s[period] = sum(tr[1:period + 1]) / period
    for i in range(period + 1, n):
        atr_s[i] = (atr_s[i - 1] * (period - 1) + tr[i]) / period
    fu = [0.0] * n; fl = [0.0] * n
    for i in range(period, n):
        if atr_s[i] is None:
            continue
        hl2 = (H[i] + L[i]) / 2; bu = hl2 + mult * atr_s[i]; bl = hl2 - mult * atr_s[i]
        if st[i - 1] is None:
            fu[i], fl[i], st[i], d[i] = bu, bl, bl, 1; continue
        fu[i] = bu if (bu < fu[i - 1] or C[i - 1] > fu[i - 1]) else fu[i - 1]
        fl[i] = bl if (bl > fl[i - 1] or C[i - 1] < fl[i - 1]) else fl[i - 1]
        d[i] = 1 if C[i] > fu[i - 1] else (-1 if C[i] < fl[i - 1] else d[i - 1])
        st[i] = fl[i] if d[i] == 1 else fu[i]
    return st, d


def _bt_sim(rows, C, H, L, e200, dvol, fee_frac, horizon, n, btc_ctx, kind, long, t, entry, stop, target, a, rs=None, mkt_ctx=None):
    """Shared forward simulator: resolve one trade (stop/target first-touch, stop-first on a tie),
    net of fees, tagged with the full context every backtest metric needs. Returns (trade, rbar)."""
    risk = (entry - stop) if long else (stop - entry)
    if risk <= 0 or entry <= 0:
        return None, None
    rr = (((target - entry) if long else (entry - target)) / risk)
    if rr <= 0:
        return None, None
    ts = int(rows[t][0]) if rows[t] else 0
    _bs = btc_ctx.get(ts) if btc_ctx else None
    btrend = _bs.get("t") if _bs else None
    bvol = _bs.get("v") if _bs else None
    _ms = mkt_ctx.get(ts) if mkt_ctx else None
    mbr = _ms.get("br") if _ms else None
    mdom = _ms.get("dom") if _ms else None
    mbpct = _ms.get("bpct") if _ms else None
    if long and mbr == "risk_off":          # don't buy alts when broad breadth is risk-off
        return None, None
    f = t + 1; outcome = rbar = None; mfe = 0.0; mae = 0.0
    for j in range(f, min(n, f + horizon + 1)):
        if long:
            mfe = max(mfe, (H[j] - entry) / risk); mae = max(mae, (entry - L[j]) / risk)
            if L[j] <= stop:
                outcome, rbar = "loss", j; break
            if H[j] >= target:
                outcome, rbar = "win", j; break
        else:
            mfe = max(mfe, (entry - L[j]) / risk); mae = max(mae, (H[j] - entry) / risk)
            if H[j] >= stop:
                outcome, rbar = "loss", j; break
            if L[j] <= target:
                outcome, rbar = "win", j; break
    if outcome is None:
        return None, None
    if long:
        stp = outcome == "loss" and any(H[j] >= target for j in range(rbar, min(n, f + horizon + 1)))
    else:
        stp = outcome == "loss" and any(L[j] <= target for j in range(rbar, min(n, f + horizon + 1)))
    rr = round(rr, 2); fee_R = fee_frac / (risk / entry)
    R = (rr - fee_R) if outcome == "win" else (-1.0 - fee_R)
    _hr = (ts // 3600000) % 24
    _sess = "Asia" if _hr < 8 else ("EU" if _hr < 14 else "US")
    el = e200[t]
    tr = {"r": round(R, 3), "rr": rr, "outcome": outcome, "gross": rr if outcome == "win" else -1.0,
          "mfe": round(min(mfe, rr), 2), "mae": round(mae, 2), "stop_then_tp": bool(stp), "cmp": False,
          "bars": rbar - f, "extd": round(abs((entry - el) / el) * 100, 2) if el else 0.0,
          "atrp": round(a / entry * 100, 2) if entry else 0.0, "dvol": dvol,
          "stopw": round(risk / entry * 100, 2), "tppct": round(abs(target - entry) / entry * 100, 2),
          "ts": ts, "entry": round(entry, 10), "stop": round(stop, 10), "target": round(target, 10),
          "rsi": round(rs, 1) if rs is not None else None, "kind": kind,
          "session": _sess, "btc_trend": btrend, "btc_vol": bvol,
          "btc_align": (btrend == ("up" if long else "down")) if btrend else None,
          "btc_state": ("BTC " + btrend) if btrend else None,
          "breadth": mbr, "dom": mdom, "bpct": mbpct}
    return tr, rbar


def _bt_supertrend(rows, side, fees_bps=5.0, horizon=24, warmup=210, btc_ctx=None, tf="4h", mkt_ctx=None):
    """SUPERTREND-PULLBACK — a trend-FOLLOWING engine (different from RSI reversion). In an
    uptrend (price above the 200-EMA, Supertrend in 'up' mode) buy the PULLBACK that tests the
    rising Supertrend line and closes back above it; mirror for shorts. Stop just beyond the ST
    line, tight structural target. Calm-BTC only. This is the fresh idea for the LONG side."""
    try:
        H = [float(x[2]) for x in rows]; L = [float(x[3]) for x in rows]; C = [float(x[4]) for x in rows]
        V = [float(x[5]) for x in rows]
    except (ValueError, IndexError, TypeError):
        return []
    n = len(C)
    if n < warmup + horizon + 5:
        return []
    e200 = ema(C, 200); st, d = _st_series(H, L, C)
    _dv = sorted(C[i] * V[i] for i in range(n)); dvol = _dv[len(_dv) // 2] if _dv else 0.0
    fee_frac = fees_bps / 10000.0; long = side == "long"; out = []; t = warmup
    while t < n - horizon - 1:
        el = e200[t]; sv = st[t]
        if el is None or sv is None:
            t += 1; continue
        price = C[t]
        a = atr(H[max(0, t - 20):t + 1], L[max(0, t - 20):t + 1], C[max(0, t - 20):t + 1])
        if not a or a <= 0:
            t += 1; continue
        _bs = btc_ctx.get(int(rows[t][0])) if btc_ctx else None
        if _bs and _bs.get("v") == "hi":                 # calm BTC only
            t += 1; continue
        if long:
            # uptrend + ST support, price dipped to the line this bar and closed back above it
            if not (d[t] == 1 and price > el):
                t += 1; continue
            if not (L[t] <= sv * 1.004 and price > sv):
                t += 1; continue
            entry = price; stop = sv - 0.6 * a; risk = entry - stop
            if risk <= 0:
                t += 1; continue
            target = entry + min(max(1.4 * risk, (max(H[max(0, t - 20):t]) - entry)), 1.9 * risk)
        else:
            if not (d[t] == -1 and price < el):
                t += 1; continue
            if not (H[t] >= sv * 0.996 and price < sv):
                t += 1; continue
            entry = price; stop = sv + 0.6 * a; risk = stop - entry
            if risk <= 0:
                t += 1; continue
            target = entry - min(max(1.4 * risk, (entry - min(L[max(0, t - 20):t]))), 1.9 * risk)
        tr, rbar = _bt_sim(rows, C, H, L, e200, dvol, fee_frac, horizon, n, btc_ctx, "supertrend", long, t, entry, stop, target, a, mkt_ctx=mkt_ctx)
        if tr is None:
            t += 1; continue
        out.append(tr); t = rbar + 1
    return out


def _bt_cpr(rows, side, fees_bps=5.0, horizon=24, warmup=210, btc_ctx=None, tf="4h", mkt_ctx=None):
    """CPR / PIVOT REACTION — trade reactions at the rolling Central Pivot Range (pivot, BC, TC
    from the prior 20 bars). In an uptrend, buy a dip that holds the pivot/BC support and closes
    back above the pivot; in a downtrend, short a pop rejected at the pivot/TC. Tight target,
    calm-BTC only. A structurally different, level-based idea to test head-to-head."""
    try:
        H = [float(x[2]) for x in rows]; L = [float(x[3]) for x in rows]; C = [float(x[4]) for x in rows]
        V = [float(x[5]) for x in rows]
    except (ValueError, IndexError, TypeError):
        return []
    n = len(C)
    if n < warmup + horizon + 5:
        return []
    e200 = ema(C, 200)
    _dv = sorted(C[i] * V[i] for i in range(n)); dvol = _dv[len(_dv) // 2] if _dv else 0.0
    fee_frac = fees_bps / 10000.0; long = side == "long"; out = []; t = warmup; pk = 20
    while t < n - horizon - 1:
        el = e200[t]
        if el is None:
            t += 1; continue
        price = C[t]
        a = atr(H[max(0, t - 20):t + 1], L[max(0, t - 20):t + 1], C[max(0, t - 20):t + 1])
        if not a or a <= 0:
            t += 1; continue
        Hp = max(H[t - pk:t]); Lp = min(L[t - pk:t]); Cp = C[t - 1]
        P = (Hp + Lp + Cp) / 3.0; BC = (Hp + Lp) / 2.0; TC = 2 * P - BC
        lo_p, hi_p = min(BC, TC), max(BC, TC)
        _bs = btc_ctx.get(int(rows[t][0])) if btc_ctx else None
        if _bs and _bs.get("v") == "hi":
            t += 1; continue
        if long:
            if not (price > el):
                t += 1; continue
            if not (L[t] <= P * 1.004 and price > lo_p and C[t] > C[t - 1]):
                t += 1; continue
            entry = price; stop = min(lo_p, L[t]) - 0.4 * a; risk = entry - stop
            if risk <= 0:
                t += 1; continue
            target = entry + min(max(1.4 * risk, (hi_p - entry)), 1.9 * risk)
        else:
            if not (price < el):
                t += 1; continue
            if not (H[t] >= P * 0.996 and price < hi_p and C[t] < C[t - 1]):
                t += 1; continue
            entry = price; stop = max(hi_p, H[t]) + 0.4 * a; risk = stop - entry
            if risk <= 0:
                t += 1; continue
            target = entry - min(max(1.4 * risk, (entry - lo_p)), 1.9 * risk)
        tr, rbar = _bt_sim(rows, C, H, L, e200, dvol, fee_frac, horizon, n, btc_ctx, "cpr", long, t, entry, stop, target, a, mkt_ctx=mkt_ctx)
        if tr is None:
            t += 1; continue
        out.append(tr); t = rbar + 1
    return out


def _bt_mix(rows, side, fees_bps=5.0, horizon=24, warmup=210, btc_ctx=None, tf="4h", mkt_ctx=None):
    """MIX / CONFLUENCE — only trade when all three agree: the 200-EMA trend, the Supertrend
    direction, AND price reacting at the rolling pivot. Stricter = fewer, hopefully higher-quality
    trades. Tests whether stacking the three edges beats any one alone."""
    try:
        H = [float(x[2]) for x in rows]; L = [float(x[3]) for x in rows]; C = [float(x[4]) for x in rows]
        V = [float(x[5]) for x in rows]
    except (ValueError, IndexError, TypeError):
        return []
    n = len(C)
    if n < warmup + horizon + 5:
        return []
    e200 = ema(C, 200); st, d = _st_series(H, L, C)
    _dv = sorted(C[i] * V[i] for i in range(n)); dvol = _dv[len(_dv) // 2] if _dv else 0.0
    fee_frac = fees_bps / 10000.0; long = side == "long"; out = []; t = warmup; pk = 20
    while t < n - horizon - 1:
        el = e200[t]; sv = st[t]
        if el is None or sv is None:
            t += 1; continue
        price = C[t]
        a = atr(H[max(0, t - 20):t + 1], L[max(0, t - 20):t + 1], C[max(0, t - 20):t + 1])
        if not a or a <= 0:
            t += 1; continue
        _bs = btc_ctx.get(int(rows[t][0])) if btc_ctx else None
        if _bs and _bs.get("v") == "hi":
            t += 1; continue
        Hp = max(H[t - pk:t]); Lp = min(L[t - pk:t]); Cp = C[t - 1]
        P = (Hp + Lp + Cp) / 3.0
        if long:
            if not (price > el and d[t] == 1 and L[t] <= max(sv, P) * 1.004 and price > P and C[t] > C[t - 1]):
                t += 1; continue
            entry = price; stop = min(sv, P) - 0.5 * a; risk = entry - stop
            if risk <= 0:
                t += 1; continue
            target = entry + min(max(1.4 * risk, Hp - entry), 1.9 * risk)
        else:
            if not (price < el and d[t] == -1 and H[t] >= min(sv, P) * 0.996 and price < P and C[t] < C[t - 1]):
                t += 1; continue
            entry = price; stop = max(sv, P) + 0.5 * a; risk = stop - entry
            if risk <= 0:
                t += 1; continue
            target = entry - min(max(1.4 * risk, entry - Lp), 1.9 * risk)
        tr, rbar = _bt_sim(rows, C, H, L, e200, dvol, fee_frac, horizon, n, btc_ctx, "mix", long, t, entry, stop, target, a, mkt_ctx=mkt_ctx)
        if tr is None:
            t += 1; continue
        out.append(tr); t = rbar + 1
    return out


def _bt_btc_monday(rows, side, fees_bps=5.0, horizon=42, warmup=60, btc_ctx=None, tf="4h", mkt_ctx=None):
    """BTC MONDAY-RANGE — the weekly opening range. Monday (UTC) prints a high/low; for the rest
    of the week we fade the edges: LONG a dip that holds the Monday LOW and turns back up (target
    the Monday high), SHORT a pop rejected at the Monday HIGH (target the Monday low). A structural
    idea (weekly reference levels traders watch) — run on BTC/majors only."""
    import datetime as _dt
    try:
        H = [float(x[2]) for x in rows]; L = [float(x[3]) for x in rows]; C = [float(x[4]) for x in rows]
        V = [float(x[5]) for x in rows]
    except (ValueError, IndexError, TypeError):
        return []
    n = len(C)
    if n < warmup + horizon + 5:
        return []
    e200 = ema(C, 200)
    _dv = sorted(C[i] * V[i] for i in range(n)); dvol = _dv[len(_dv) // 2] if _dv else 0.0
    fee_frac = fees_bps / 10000.0; long = side == "long"; out = []
    wd = [(_dt.datetime.utcfromtimestamp(int(rows[i][0]) / 1000).weekday()) for i in range(n)]
    t = warmup
    mHigh = mLow = None; seen_mon = False
    while t < n - horizon - 1:
        # (Re)build the Monday range as Monday bars stream in; freeze it for the week after.
        if wd[t] == 0:
            if not seen_mon or (wd[t - 1] != 0):
                mHigh, mLow, seen_mon = H[t], L[t], True
            else:
                mHigh = max(mHigh, H[t]); mLow = min(mLow, L[t])
            t += 1; continue
        if mHigh is None:
            t += 1; continue
        price = C[t]
        a = atr(H[max(0, t - 20):t + 1], L[max(0, t - 20):t + 1], C[max(0, t - 20):t + 1])
        if not a or a <= 0:
            t += 1; continue
        rng = mHigh - mLow
        if rng <= 0:
            t += 1; continue
        if long:
            if not (L[t] <= mLow * 1.002 and price > mLow and C[t] > C[t - 1]):
                t += 1; continue
            entry = price; stop = mLow - 0.6 * a; risk = entry - stop
            if risk <= 0:
                t += 1; continue
            target = min(mHigh, entry + 2.5 * risk)              # ride toward the Monday high
        else:
            if not (H[t] >= mHigh * 0.998 and price < mHigh and C[t] < C[t - 1]):
                t += 1; continue
            entry = price; stop = mHigh + 0.6 * a; risk = stop - entry
            if risk <= 0:
                t += 1; continue
            target = max(mLow, entry - 2.5 * risk)               # ride toward the Monday low
        if abs(target - entry) / entry < 0.002:
            t += 1; continue
        tr, rbar = _bt_sim(rows, C, H, L, e200, dvol, fee_frac, horizon, n, btc_ctx, "monday", long, t, entry, stop, target, a, mkt_ctx=mkt_ctx)
        if tr is None:
            t += 1; continue
        out.append(tr); t = rbar + 1
    return out


def _bt_ema200pb(rows, side, fees_bps=5.0, horizon=24, warmup=210, btc_ctx=None, tf="4h", mkt_ctx=None):
    """200-EMA PULLBACK / RIP — the simplest trend trade, with a DELIBERATELY WIDE stop.
    LONG: in an uptrend (price above a RISING 200-EMA) buy the pullback that tags the 200-EMA
    (a wick into it) and closes back above it — the classic 'buy the dip to the mean in an
    uptrend'. SHORT: in a downtrend (price below a FALLING 200-EMA) sell the rip that tags the
    200-EMA from below (uses it as resistance) and closes back under it. The stop sits WIDE —
    ~1.8x ATR beyond the EMA — so a normal wick through the line does not knock you out; the
    trade is only wrong if price CLOSES decisively through the mean. Target a measured 2R move
    (capped ~3R). No look-ahead; stop-first on ties; net of fees; regime-tagged/gated via _bt_sim."""
    try:
        H = [float(x[2]) for x in rows]
        L = [float(x[3]) for x in rows]
        C = [float(x[4]) for x in rows]
        V = [float(x[5]) for x in rows]
    except (ValueError, IndexError, TypeError):
        return []
    n = len(C)
    if n < warmup + horizon + 5:
        return []
    e200 = ema(C, 200)
    long = side == "long"
    fee_frac = fees_bps / 10000.0
    _dv = sorted(C[i] * V[i] for i in range(n))
    dvol = _dv[len(_dv) // 2] if _dv else 0.0
    out = []
    t = warmup
    while t < n - horizon - 1:
        el = e200[t]
        if el is None:
            t += 1; continue
        prev = e200[t - 20] if t >= 20 else None
        a = atr(H[max(0, t - 20):t + 1], L[max(0, t - 20):t + 1], C[max(0, t - 20):t + 1])
        if not a or a <= 0:
            t += 1; continue
        price = C[t]
        entry = stop = target = None
        if long:
            slope_up = (prev is None) or (el > prev)
            if not (price > el and slope_up):                     # uptrend only
                t += 1; continue
            tagged = min(L[max(0, t - 3):t + 1]) <= max(el + 0.60 * a, el * 1.012)  # pullback reached the mean band
            if not tagged:
                t += 1; continue
            if not (C[t] > el and C[t] > C[t - 1]):               # closes back above, turning up
                t += 1; continue
            entry = price
            stop = min(el, price) - 1.8 * a                        # WIDE, below the EMA
            risk = entry - stop
            if risk <= 0:
                t += 1; continue
            prior_high = max(H[max(0, t - 20):t])
            target = min(max(prior_high, entry + 2.0 * risk), entry + 3.0 * risk)
        else:
            slope_dn = (prev is None) or (el < prev)
            if not (price < el and slope_dn):                     # downtrend only
                t += 1; continue
            tagged = max(H[max(0, t - 3):t + 1]) >= min(el - 0.60 * a, el * 0.988)  # rip reached the mean band
            if not tagged:
                t += 1; continue
            if not (C[t] < el and C[t] < C[t - 1]):               # closes back below, turning down
                t += 1; continue
            entry = price
            stop = max(el, price) + 1.8 * a                        # WIDE, above the EMA
            risk = stop - entry
            if risk <= 0:
                t += 1; continue
            prior_low = min(L[max(0, t - 20):t])
            target = max(min(prior_low, entry - 2.0 * risk), entry - 3.0 * risk)
        if abs(target - entry) / entry < 0.002:
            t += 1; continue
        tr, rbar = _bt_sim(rows, C, H, L, e200, dvol, fee_frac, horizon, n, btc_ctx,
                           "ema200pb", long, t, entry, stop, target, a, mkt_ctx=mkt_ctx)
        if tr is None:
            t += 1; continue
        out.append(tr); t = rbar + 1
    return out


def _bt_goldencross(rows, side, fees_bps=5.0, horizon=60, warmup=210, btc_ctx=None, tf="4h", mkt_ctx=None):
    """GOLDEN / DEATH CROSS — the classic long-term trend flip. A Golden Cross = the faster 50-EMA
    crossing ABOVE the slow 200-EMA (long-term uptrend begins); a Death Cross = 50-EMA crossing
    BELOW the 200-EMA (downtrend begins). LONG on a fresh Golden Cross, SHORT on a fresh Death
    Cross — entered at the close of the cross bar. Because it's a slow, structural signal it uses a
    LONGER horizon and a WIDE stop (below/above the 200-EMA by ~2x ATR, or the recent swing) and a
    larger measured target (~3R), riding the new trend. Trades are rarer per coin but add up across
    the basket and timeframes. No look-ahead; stop-first on ties; net of fees; regime-tagged."""
    try:
        H = [float(x[2]) for x in rows]
        L = [float(x[3]) for x in rows]
        C = [float(x[4]) for x in rows]
        V = [float(x[5]) for x in rows]
    except (ValueError, IndexError, TypeError):
        return []
    n = len(C)
    if n < warmup + horizon + 5:
        return []
    e50 = ema(C, 50)
    e200 = ema(C, 200)
    long = side == "long"
    fee_frac = fees_bps / 10000.0
    _dv = sorted(C[i] * V[i] for i in range(n))
    dvol = _dv[len(_dv) // 2] if _dv else 0.0
    out = []
    t = warmup
    while t < n - horizon - 1:
        ef, es = e50[t], e200[t]
        efp, esp = e50[t - 1], e200[t - 1]
        if None in (ef, es, efp, esp):
            t += 1; continue
        a = atr(H[max(0, t - 20):t + 1], L[max(0, t - 20):t + 1], C[max(0, t - 20):t + 1])
        if not a or a <= 0:
            t += 1; continue
        price = C[t]
        golden = efp <= esp and ef > es          # fresh 50>200 cross this bar
        death = efp >= esp and ef < es           # fresh 50<200 cross this bar
        entry = stop = target = None
        if long:
            if not golden:
                t += 1; continue
            entry = price
            swing_low = min(L[max(0, t - 20):t + 1])
            stop = min(swing_low, es - 2.0 * a)   # WIDE — below the 200-EMA / recent swing
            risk = entry - stop
            if risk <= 0:
                t += 1; continue
            target = entry + 3.0 * risk            # ride the new uptrend (~3R)
        else:
            if not death:
                t += 1; continue
            entry = price
            swing_high = max(H[max(0, t - 20):t + 1])
            stop = max(swing_high, es + 2.0 * a)
            risk = stop - entry
            if risk <= 0:
                t += 1; continue
            target = entry - 3.0 * risk
        if abs(target - entry) / entry < 0.002:
            t += 1; continue
        tr, rbar = _bt_sim(rows, C, H, L, e200, dvol, fee_frac, horizon, n, btc_ctx,
                           "goldencross", long, t, entry, stop, target, a, mkt_ctx=mkt_ctx)
        if tr is None:
            t += 1; continue
        out.append(tr); t = rbar + 1
    return out


def _bt_highwr(rows, side, fees_bps=5.0, horizon=24, warmup=210, btc_ctx=None, tf="4h", mkt_ctx=None):
    """HIGH WIN-RATE mean reversion — engineered for HIT-RATE, not big R. Only trades WITH the
    higher-timeframe trend (sloping 200-EMA), only in a FAVOURABLE breadth regime (long in risk-on,
    short in risk-off), only when BTC and the coin are CALM, and only on an oversold/overbought
    snap that has already turned. The take-profit is placed NEAR (~1.3x ATR, and never past the
    20-EMA mean) so it is banked frequently; the stop is placed WIDE (~2.0x ATR beyond the flush)
    so a normal wick does not take you out. Reward:risk is deliberately < 1 (~0.65) — the point is
    a high win-rate. Breakeven WR at that R:R is ~61%, so it only truly wins if the hit-rate clears
    that; the lab reports both so you can judge honestly. No look-ahead; net of fees; regime-gated."""
    try:
        H = [float(x[2]) for x in rows]; L = [float(x[3]) for x in rows]
        C = [float(x[4]) for x in rows]; V = [float(x[5]) for x in rows]
    except (ValueError, IndexError, TypeError):
        return []
    n = len(C)
    if n < warmup + horizon + 5:
        return []
    e200 = ema(C, 200); e20 = ema(C, 20); rsis = rsi_series(C)
    long = side == "long"; fee_frac = fees_bps / 10000.0
    _dv = sorted(C[i] * V[i] for i in range(n)); dvol = _dv[len(_dv) // 2] if _dv else 0.0
    _atrp = [((atr(H[max(0, i - 20):i + 1], L[max(0, i - 20):i + 1], C[max(0, i - 20):i + 1]) or 0.0) / C[i])
             if C[i] else 0.0 for i in range(n)]
    _as = sorted(x for x in _atrp if x > 0); _amed = _as[len(_as) // 2] if _as else 0.0
    out = []; t = warmup
    while t < n - horizon - 1:
        el = e200[t]; e2 = e20[t]; rs = rsis[t]
        if el is None or e2 is None or rs is None:
            t += 1; continue
        a = atr(H[max(0, t - 20):t + 1], L[max(0, t - 20):t + 1], C[max(0, t - 20):t + 1])
        if not a or a <= 0:
            t += 1; continue
        prev = e200[t - 20] if t >= 20 else None
        ts = int(rows[t][0]) if rows[t] else 0
        _bs = btc_ctx.get(ts) if btc_ctx else None
        btrend = _bs.get("t") if _bs else None; bvol = _bs.get("v") if _bs else None
        _ms = mkt_ctx.get(ts) if mkt_ctx else None
        mbr = _ms.get("br") if _ms else None; mdom = _ms.get("dom") if _ms else None
        mbpct = _ms.get("bpct") if _ms else None
        if bvol == "hi":                                   # calm BTC only
            t += 1; continue
        if _amed and _atrp[t] > _amed * 1.10:              # calm coin only
            t += 1; continue
        price = C[t]; entry = stop = target = None
        if long:
            if not (price > el and (prev is None or el > prev)):
                t += 1; continue
            if mbr == "risk_off":                          # long only in supportive breadth
                t += 1; continue
            if _btc_block(True, tf, btrend, bvol):
                t += 1; continue
            if not (38 <= rs <= 55 and C[t] > C[t - 1]):   # oversold-ish snap that has turned up
                t += 1; continue
            entry = price
            flush_low = min(L[max(0, t - 3):t + 1])
            stop = min(flush_low - 0.2 * a, entry - 1.8 * a)   # WIDE (but not so wide R:R collapses)
            risk = entry - stop
            if risk <= 0:
                t += 1; continue
            mean_gap = (e2 - entry) if e2 > entry else 1.1 * a
            target = entry + min(max(1.1 * a, 0.7 * mean_gap), 1.5 * a)   # NEAR — ~0.75 R:R
        else:
            if not (price < el and (prev is None or el < prev)):
                t += 1; continue
            if mbr == "risk_on":                           # short only in weak breadth
                t += 1; continue
            if _btc_block(False, tf, btrend, bvol):
                t += 1; continue
            if not (45 <= rs <= 62 and C[t] < C[t - 1]):   # overbought-ish pop rolling over
                t += 1; continue
            entry = price
            flush_high = max(H[max(0, t - 3):t + 1])
            stop = max(flush_high + 0.2 * a, entry + 1.8 * a)
            risk = stop - entry
            if risk <= 0:
                t += 1; continue
            mean_gap = (entry - e2) if e2 < entry else 1.1 * a
            target = entry - min(max(1.1 * a, 0.7 * mean_gap), 1.5 * a)
        if target is None or abs(target - entry) / entry < 0.0015:
            t += 1; continue
        tr, rbar = _bt_sim(rows, C, H, L, e200, dvol, fee_frac, horizon, n, btc_ctx,
                           "highwr", long, t, entry, stop, target, a, rs=rs, mkt_ctx=mkt_ctx)
        if tr is None:
            t += 1; continue
        out.append(tr); t = rbar + 1
    return out


def _bt_pullback20(rows, side, fees_bps=5.0, horizon=24, warmup=210, btc_ctx=None, tf="4h", mkt_ctx=None):
    """20-EMA PULLBACK CONTINUATION — a high-win-rate trend-continuation. In an uptrend (price above
    a rising 200-EMA, favourable breadth, calm tape) buy the shallow pullback that tags the fast
    20-EMA and closes back above it; mirror for shorts. The fast mean gets reclaimed often inside a
    trend, so the hit-rate is high; the target is kept NEAR (a fraction of the swing, ~<=1.3x ATR)
    and the stop moderately WIDE (below the 20-EMA by ~1.5x ATR). No look-ahead; net of fees; gated."""
    try:
        H = [float(x[2]) for x in rows]; L = [float(x[3]) for x in rows]
        C = [float(x[4]) for x in rows]; V = [float(x[5]) for x in rows]
    except (ValueError, IndexError, TypeError):
        return []
    n = len(C)
    if n < warmup + horizon + 5:
        return []
    e200 = ema(C, 200); e20 = ema(C, 20); rsis = rsi_series(C)
    long = side == "long"; fee_frac = fees_bps / 10000.0
    _dv = sorted(C[i] * V[i] for i in range(n)); dvol = _dv[len(_dv) // 2] if _dv else 0.0
    _atrp = [((atr(H[max(0, i - 20):i + 1], L[max(0, i - 20):i + 1], C[max(0, i - 20):i + 1]) or 0.0) / C[i])
             if C[i] else 0.0 for i in range(n)]
    _as = sorted(x for x in _atrp if x > 0); _amed = _as[len(_as) // 2] if _as else 0.0
    out = []; t = warmup
    while t < n - horizon - 1:
        el = e200[t]; e2 = e20[t]; rs = rsis[t]
        if el is None or e2 is None:
            t += 1; continue
        a = atr(H[max(0, t - 20):t + 1], L[max(0, t - 20):t + 1], C[max(0, t - 20):t + 1])
        if not a or a <= 0:
            t += 1; continue
        prev = e200[t - 20] if t >= 20 else None
        ts = int(rows[t][0]) if rows[t] else 0
        _bs = btc_ctx.get(ts) if btc_ctx else None
        btrend = _bs.get("t") if _bs else None; bvol = _bs.get("v") if _bs else None
        _ms = mkt_ctx.get(ts) if mkt_ctx else None
        mbr = _ms.get("br") if _ms else None; mdom = _ms.get("dom") if _ms else None
        mbpct = _ms.get("bpct") if _ms else None
        if bvol == "hi" or (_amed and _atrp[t] > _amed * 1.15):
            t += 1; continue
        price = C[t]; entry = stop = target = None
        if long:
            if not (price > el and (prev is None or el > prev)):
                t += 1; continue
            if mbr == "risk_off" or _btc_block(True, tf, btrend, bvol):
                t += 1; continue
            tagged = min(L[max(0, t - 2):t + 1]) <= e2 * 1.004        # pullback tagged the fast mean
            if not (tagged and C[t] > e2 and C[t] > C[t - 1]):        # reclaimed, turning up
                t += 1; continue
            entry = price
            stop = min(min(L[max(0, t - 3):t + 1]) - 0.2 * a, e2 - 1.5 * a)
            risk = entry - stop
            if risk <= 0:
                t += 1; continue
            prior_high = max(H[max(0, t - 12):t])
            gap = (prior_high - entry) if prior_high > entry else 1.0 * a
            target = entry + min(max(0.9 * a, 0.6 * gap), 1.3 * a)
        else:
            if not (price < el and (prev is None or el < prev)):
                t += 1; continue
            if mbr == "risk_on" or _btc_block(False, tf, btrend, bvol):
                t += 1; continue
            tagged = max(H[max(0, t - 2):t + 1]) >= e2 * 0.996
            if not (tagged and C[t] < e2 and C[t] < C[t - 1]):
                t += 1; continue
            entry = price
            stop = max(max(H[max(0, t - 3):t + 1]) + 0.2 * a, e2 + 1.5 * a)
            risk = stop - entry
            if risk <= 0:
                t += 1; continue
            prior_low = min(L[max(0, t - 12):t])
            gap = (entry - prior_low) if prior_low < entry else 1.0 * a
            target = entry - min(max(0.9 * a, 0.6 * gap), 1.3 * a)
        if target is None or abs(target - entry) / entry < 0.0015:
            t += 1; continue
        tr, rbar = _bt_sim(rows, C, H, L, e200, dvol, fee_frac, horizon, n, btc_ctx,
                           "pullback20", long, t, entry, stop, target, a, rs=rs, mkt_ctx=mkt_ctx)
        if tr is None:
            t += 1; continue
        out.append(tr); t = rbar + 1
    return out


def backtest_symbol(rows, side, fees_bps=4.0, horizon=40, warmup=210, strategy="level", btc_ctx=None, tf="4h", mkt_ctx=None):
    """Replay a strategy's entry/stop/target MECHANICS over one coin's historical candles and
    return a list of resolved trades (net R after fees). WITHOUT look-ahead: every decision at
    bar t uses only candles up to t, and the outcome is whichever of stop/target is reached
    first (stop assumed first on a tie). strategy='level' = pullback/rip to a structural level;
    'revert' = trend-aligned oversold/overbought snap-back to the mean (the high-win-rate one);
    'spot' = the SAME trend-aligned reversion but LONG-ONLY and with spot fees (no funding, no
    leverage) — i.e. buy an oversold dip in an uptrend and hold to the mean, as a cash buyer."""
    if strategy == "spot":
        # Spot: long-only, cash. Charge realistic spot round-trip fees (default ~20 bps if the
        # caller didn't pass a heavier number) and never short — you can't short on spot.
        return _bt_revert(rows, "long", fees_bps=max(fees_bps, 20.0), warmup=warmup, btc_ctx=btc_ctx, tf=tf, mkt_ctx=mkt_ctx)
    if strategy == "revert":
        return _bt_revert(rows, side, fees_bps=fees_bps, warmup=warmup, btc_ctx=btc_ctx, tf=tf, mkt_ctx=mkt_ctx)
    if strategy == "supertrend":
        return _bt_supertrend(rows, side, fees_bps=fees_bps, warmup=warmup, btc_ctx=btc_ctx, tf=tf, mkt_ctx=mkt_ctx)
    if strategy == "cpr":
        return _bt_cpr(rows, side, fees_bps=fees_bps, warmup=warmup, btc_ctx=btc_ctx, tf=tf, mkt_ctx=mkt_ctx)
    if strategy == "mix":
        return _bt_mix(rows, side, fees_bps=fees_bps, warmup=warmup, btc_ctx=btc_ctx, tf=tf, mkt_ctx=mkt_ctx)
    if strategy == "ema200pb":
        return _bt_ema200pb(rows, side, fees_bps=fees_bps, warmup=warmup, btc_ctx=btc_ctx, tf=tf, mkt_ctx=mkt_ctx)
    if strategy == "goldencross":
        return _bt_goldencross(rows, side, fees_bps=fees_bps, warmup=warmup, btc_ctx=btc_ctx, tf=tf, mkt_ctx=mkt_ctx)
    if strategy == "highwr":
        return _bt_highwr(rows, side, fees_bps=fees_bps, warmup=warmup, btc_ctx=btc_ctx, tf=tf, mkt_ctx=mkt_ctx)
    if strategy == "pullback20":
        return _bt_pullback20(rows, side, fees_bps=fees_bps, warmup=warmup, btc_ctx=btc_ctx, tf=tf, mkt_ctx=mkt_ctx)
    if strategy == "monday":
        return _bt_btc_monday(rows, side, fees_bps=fees_bps, btc_ctx=btc_ctx, tf=tf, mkt_ctx=mkt_ctx)
    try:
        H = [float(x[2]) for x in rows]
        L = [float(x[3]) for x in rows]
        C = [float(x[4]) for x in rows]
        V = [float(x[5]) for x in rows]
    except (ValueError, IndexError, TypeError):
        return []
    n = len(C)
    if n < warmup + horizon + 5:
        return []
    e200 = ema(C, 200)
    long = side == "long"
    W = 120
    # Per-coin liquidity proxy: median USD volume over the window (close × volume). Every
    # trade on this coin is tagged with it, so the analysis can segment by liquidity TREND
    # (liquid large-caps vs thin low-caps) rather than by individual coin name.
    _dv = sorted(C[i] * V[i] for i in range(len(C)))
    dvol = _dv[len(_dv) // 2] if _dv else 0.0
    fee_frac = fees_bps / 10000.0
    trades = []
    t = warmup
    while t < n - horizon - 1:
        el = e200[t]
        if el is None:
            t += 1; continue
        price = C[t]
        lo = max(0, t - W)
        Hw, Lw, Cw = H[lo:t + 1], L[lo:t + 1], C[lo:t + 1]
        a = atr(H[max(0, t - 20):t + 1], L[max(0, t - 20):t + 1], C[max(0, t - 20):t + 1])
        if not a or a <= 0:
            t += 1; continue
        ms = market_structure(Hw, Lw, Cw)
        up = ms["structure"] == "uptrend" or ms["choch"] == "bullish"
        dn = ms["structure"] == "downtrend" or ms["choch"] == "bearish"
        last = len(Lw) - 1
        entry = stop = tp = risk = rr = None
        prox = max(0.02, 1.5 * a / price)              # "near a level" band for a limit entry
        if long:
            if not (price > el and up):
                t += 1; continue
            sup = [s for s in supports_below(Lw, last, price, max_n=3, min_gap=0.004) if s < price]
            near = sup[0] if sup else None
            is_cmp = near is None or (price - near) / price > prox
            if is_cmp:                    # limit-only: backtest proved CMP momentum entries lose
                t += 1; continue
            entry = near           # CMP momentum entry, or limit at support
            _min = max(0.011, 1.85 * a / entry)   # wider stop (~30%) — backtest: too many losers hit TP after stop
            below = (sup[1] if len(sup) > 1 else (near * 0.996 if near else entry * 0.985))
            stop = min((below - 0.4 * a) if (near and not is_cmp) else entry * (1 - _min),
                       entry * (1 - _min))
            risk = entry - stop
            if risk <= 0:
                t += 1; continue
            res = [r for r in resistances_above(Hw, last, price, max_n=3, min_gap=0.005) if r > entry * 1.01]
            tp = res[0] if res else entry + 2 * risk
            rr = (tp - entry) / risk
            if rr < 1.3:
                t += 1; continue
            if is_cmp:
                f = t + 1
            else:
                f = next((j for j in range(t + 1, min(n, t + 9)) if L[j] <= entry), None)
                if f is None:
                    t += 1; continue
            outcome = rbar = None
            mfe = 0.0
            for j in range(f, min(n, f + horizon + 1)):
                mfe = max(mfe, (H[j] - entry) / risk)     # best move toward target so far (R)
                if L[j] <= stop:
                    outcome, rbar = "loss", j; break
                if H[j] >= tp:
                    outcome, rbar = "win", j; break
            if outcome is None:
                t = f + 1; continue
            stop_then_tp = (outcome == "loss" and
                            any(H[j] >= tp for j in range(rbar, min(n, f + horizon + 1))))
        else:
            if not (price < el and dn):
                t += 1; continue
            res = [r for r in resistances_above(Hw, last, price, max_n=3, min_gap=0.004) if r > price]
            near = res[0] if res else None
            is_cmp = near is None or (near - price) / price > prox
            if is_cmp:                    # limit-only: backtest proved CMP momentum entries lose
                t += 1; continue
            entry = near
            _min = max(0.011, 1.85 * a / entry)   # wider stop (~30%) — backtest: too many losers hit TP after stop
            above = (res[1] if len(res) > 1 else (near * 1.004 if near else entry * 1.015))
            stop = max((above + 0.4 * a) if (near and not is_cmp) else entry * (1 + _min),
                       entry * (1 + _min))
            risk = stop - entry
            if risk <= 0:
                t += 1; continue
            sup = [s for s in supports_below(Lw, last, price, max_n=3, min_gap=0.005) if s < entry * 0.99]
            tp = sup[0] if sup else entry - 2 * risk
            if tp <= 0:
                t += 1; continue
            rr = (entry - tp) / risk
            if rr < 1.3:
                t += 1; continue
            if is_cmp:
                f = t + 1
            else:
                f = next((j for j in range(t + 1, min(n, t + 9)) if H[j] >= entry), None)
                if f is None:
                    t += 1; continue
            outcome = rbar = None
            mfe = 0.0
            for j in range(f, min(n, f + horizon + 1)):
                mfe = max(mfe, (entry - L[j]) / risk)
                if H[j] >= stop:
                    outcome, rbar = "loss", j; break
                if L[j] <= tp:
                    outcome, rbar = "win", j; break
            if outcome is None:
                t = f + 1; continue
            stop_then_tp = (outcome == "loss" and
                            any(L[j] <= tp for j in range(rbar, min(n, f + horizon + 1))))
        rr = round(rr, 2)
        fee_R = fee_frac / (risk / entry)
        R = (rr - fee_R) if outcome == "win" else (-1.0 - fee_R)
        trades.append({"r": round(R, 3), "rr": rr, "outcome": outcome,
                       "gross": rr if outcome == "win" else -1.0,
                       "mfe": round(min(mfe, rr), 2), "stop_then_tp": bool(stop_then_tp),
                       "cmp": bool(is_cmp), "bars": rbar - f,
                       "extd": round(abs((entry - el) / el) * 100, 2) if el else 0.0,
                       "atrp": round(a / entry * 100, 2), "dvol": dvol})
        t = rbar + 1
    return trades


def _bt_insights(trades):
    """Diagnostics that turn a backtest into ACTIONABLE ideas. The headline one:
    'stop_then_tp' = the share of LOSING trades whose target was later reached after the
    stop was hit — a direct measure of stops being too tight."""
    n = len(trades)
    if not n:
        return {}
    losers = [x for x in trades if x["outcome"] == "loss"]
    nl = len(losers) or 1
    stp = round(sum(1 for x in losers if x.get("stop_then_tp")) / nl * 100, 1)
    loser_mfe = round(sum(x.get("mfe", 0) for x in losers) / nl, 2)
    cmp_share = round(sum(1 for x in trades if x.get("cmp")) / n * 100)
    avg_bars = round(sum(x.get("bars", 0) for x in trades) / n, 1)
    return {"stop_then_tp_pct": stp, "loser_mfe": loser_mfe,
            "cmp_share": cmp_share, "avg_bars": avg_bars, "n_losers": len(losers)}


def _bt_findings(trades):
    """Segment the trades by context (entry type, distance-from-EMA, volatility, target
    width) and surface the splits with the biggest expectancy gap — i.e. WHAT WORKED, so
    the logic can be biased toward it."""
    def seg(pred):
        a = [x for x in trades if pred(x)]
        b = [x for x in trades if not pred(x)]
        ma = (round(sum(x["r"] for x in a) / len(a), 3), len(a)) if a else (None, 0)
        mb = (round(sum(x["r"] for x in b) / len(b), 3), len(b)) if b else (None, 0)
        return ma, mb
    out = []

    def add(label, aname, bname, pred, minn=15):
        (ea, na), (eb, nb) = seg(pred)
        if na >= minn and nb >= minn and ea is not None and eb is not None and abs(ea - eb) >= 0.1:
            out.append({"label": label, "a": {"name": aname, "exp": ea, "n": na},
                        "b": {"name": bname, "exp": eb, "n": nb},
                        "better": aname if ea > eb else bname, "gap": round(abs(ea - eb), 2)})

    def add_cat(label, keyfn, minn=20):
        """Categorical (multi-value) segment: find the single value whose expectancy differs
        most from the rest, and surface it as best-value-vs-the-field. Used for time-of-day
        and BTC-regime, where there are more than two buckets."""
        vals = {}
        for x in trades:
            k = keyfn(x)
            if k is not None:
                vals.setdefault(k, []).append(x)
        best = None
        for k, a in vals.items():
            if len(a) < minn:
                continue
            b = [x for x in trades if keyfn(x) is not None and keyfn(x) != k]
            if len(b) < minn:
                continue
            ea = sum(x["r"] for x in a) / len(a)
            eb = sum(x["r"] for x in b) / len(b)
            gap = abs(ea - eb)
            if gap >= 0.1 and (best is None or gap > best["gap"]):
                best = {"label": label, "a": {"name": str(k), "exp": round(ea, 3), "n": len(a)},
                        "b": {"name": "the rest", "exp": round(eb, 3), "n": len(b)},
                        "better": str(k) if ea > eb else "the rest", "gap": round(gap, 2)}
        if best:
            out.append(best)
    add("Entry type", "limit at a level", "market (CMP)", lambda x: not x.get("cmp"))
    add("Distance from 200-EMA", "near (≤5%)", "extended (>5%)", lambda x: (x.get("extd") or 0) <= 5)
    atrs = sorted((x.get("atrp") or 0) for x in trades)
    med = atrs[len(atrs) // 2] if atrs else 0
    add("Volatility at entry", f"calmer (≤{med:.1f}% ATR)", f"choppier (>{med:.1f}%)",
        lambda x: (x.get("atrp") or 0) <= med)
    add("Target width", "tighter R:R (<2)", "wider R:R (≥2)", lambda x: (x.get("rr") or 0) < 2)
    # Coin liquidity (median $volume) — split at the median so the finding reads as a TREND
    # ("liquid names vs thin"), the coin-character segment the strategy should gate on.
    dvs = sorted((x.get("dvol") or 0) for x in trades)
    dmed = dvs[len(dvs) // 2] if dvs else 0
    _fmt = lambda v: (f"${v/1e6:.0f}M" if v >= 1e6 else f"${v/1e3:.0f}K")
    add("Coin liquidity ($vol)", f"liquid (≥{_fmt(dmed)})", f"thin (<{_fmt(dmed)})",
        lambda x: (x.get("dvol") or 0) >= dmed)
    # NEW dimensions the user asked for: time-of-day, BTC behaviour, BTC alignment & volatility.
    add_cat("Time of day (UTC)", lambda x: x.get("session"))
    add_cat("BTC regime", lambda x: x.get("btc_state"))
    add("BTC alignment", "BTC trending with the trade", "BTC flat / ranging",
        lambda x: x.get("btc_align") is True, minn=20)
    add("BTC volatility", "BTC calm", "BTC volatile", lambda x: x.get("btc_vol") == "lo", minn=20)
    add_cat("Market breadth", lambda x: x.get("breadth"))
    add("Dominance", "alts leading (BTC.D falling)", "BTC leading (BTC.D rising)",
        lambda x: x.get("dom") == "alt", minn=20)
    out.sort(key=lambda f: f["gap"], reverse=True)
    return out[:8]


# A broad liquid basket the backtester runs over by default (~60 majors + mid-caps).
BACKTEST_BASKET = [c + "USDT" for c in (
    "BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "AVAX", "LINK", "SUI", "APT", "ARB",
    "OP", "LTC", "NEAR", "INJ", "TRX", "DOT", "MATIC", "ATOM", "UNI", "AAVE", "FIL", "ETC",
    "XLM", "HBAR", "ICP", "IMX", "RUNE", "GRT", "ALGO", "SAND", "MANA", "AXS", "FTM", "EGLD",
    "THETA", "FLOW", "CHZ", "CRV", "LDO", "SNX", "DYDX", "GMX", "SEI", "TIA", "PYTH", "JTO",
    "WIF", "PEPE", "BONK", "FLOKI", "ORDI", "STX", "RNDR", "FET", "AR", "KAS", "TON", "WLD")]


def backtest_board(sess, side, market, tf="4h", limit=1000, fees_bps=4.0, symbols=None):
    """Run backtest_symbol across a basket and aggregate — win-rate, expectancy (net of
    fees), gross vs net, avg win/loss and a per-symbol breakdown. Fetch AND compute are
    parallelised so a big basket (or the whole universe) stays fast. `symbols` are full
    pair names (e.g. 'BTCUSDT'); defaults to the broad liquid basket."""
    syms = symbols or BACKTEST_BASKET

    def _one(sym):
        try:
            rows = fetch_candles(sess, sym, tf, limit, market)
        except Exception:
            return sym, None
        if not rows:
            return sym, None
        return sym, backtest_symbol(rows, side, fees_bps=fees_bps)

    results = {}
    with ThreadPoolExecutor(max_workers=12) as ex:
        for f in as_completed([ex.submit(_one, s) for s in syms]):
            try:
                sym, tr = f.result()
            except Exception:
                sym, tr = None, None
            if sym:
                results[sym] = tr
    return _bt_aggregate(results, syms, tf, side, fees_bps)


def _bt_portfolio(trades, start=10000.0, min_margin=100.0, lev=10.0):
    """Turn the net-R trade stream into a $ PORTFOLIO curve. Sizing rule (your spec): $10k start,
    risk 1% of equity as margin BUT never less than $100 margin, always at 10× — so the minimum
    trade is $1,000 notional (the book can hold ~100 such margin slots). A trade's P&L in $ =
    net-R × (notional × stop-width%). Two scenarios: COMPOUNDING (margin = max(1% of the growing
    equity, $100)) and FIXED ($100 margin every trade). We report each scenario's end equity,
    return %, and the worst peak-to-trough drawdown in BOTH % and $. Idealised single sequential
    stream — see 'max concurrent' for how many positions would actually be open at once."""
    seq = sorted((x for x in trades if x.get("ts") is not None and x.get("stopw") is not None),
                 key=lambda x: x["ts"])
    if not seq:
        return None

    def run(compound):
        eq = start; peak = start; maxdd = 0.0; maxdd_abs = 0.0
        for x in seq:
            stopw = (x.get("stopw") or 0.0) / 100.0
            margin = max(0.01 * eq, min_margin) if compound else min_margin
            notional = margin * lev
            eq += (x.get("r") or 0.0) * notional * stopw
            if eq < 0:
                eq = 0.0
            peak = max(peak, eq)
            dd = peak - eq
            if dd > maxdd_abs:
                maxdd_abs = dd
            if peak > 0:
                maxdd = max(maxdd, dd / peak)
        return {"end": round(eq), "ret_pct": round((eq / start - 1) * 100, 1),
                "max_dd_pct": round(maxdd * 100, 1), "max_dd_abs": round(maxdd_abs)}
    return {"start": round(start), "n": len(seq), "min_margin": round(min_margin), "lev": round(lev),
            "min_notional": round(min_margin * lev),
            "compound": run(True), "fixed": run(False)}


_TF_SECS = {"15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}


def _bt_monthly(trades):
    """Win-rate + expectancy per calendar month, so you can see WHEN the edge showed up and
    whether it's steady or lumpy (e.g. all the short profit came from a couple of dump months)."""
    by = {}
    for x in trades:
        ts = x.get("ts")
        if not ts:
            continue
        import datetime as _dt
        m = _dt.datetime.utcfromtimestamp(ts / 1000).strftime("%Y-%m")
        by.setdefault(m, []).append(x)
    out = []
    for m in sorted(by):
        a = by[m]; nn = len(a)
        wr = round(sum(1 for x in a if x["outcome"] == "win") / nn * 100, 1)
        ex = round(sum(x["r"] for x in a) / nn, 3)
        out.append({"m": m, "n": nn, "winrate": wr, "exp": ex, "sumR": round(sum(x["r"] for x in a), 1)})
    return out


def _bt_max_concurrent(trades, tf):
    """The most positions that would be OPEN AT ONCE if you took every signal — a reality check
    on the portfolio sim (which runs trades sequentially). High = you'd need lots of margin slots
    and couldn't actually take them all on a small account."""
    secs = _TF_SECS.get(tf, 14400) * 1000
    events = []
    for x in trades:
        ts = x.get("ts")
        if not ts:
            continue
        end = ts + (x.get("bars") or 1) * secs
        events.append((ts, 1)); events.append((end, -1))
    events.sort()
    cur = mx = 0
    for _, d in events:
        cur += d
        if cur > mx:
            mx = cur
    return mx


def _bt_aggregate(results, syms, tf, side, fees_bps):
    """Aggregate per-symbol trade lists into a board summary + actionable analysis."""
    allt, per = [], []
    for s in syms:
        tr = results.get(s)
        if tr:
            m = len(tr); sm = sum(x["r"] for x in tr)
            w = sum(1 for x in tr if x["outcome"] == "win")
            lz = [x for x in tr if x["outcome"] == "loss"]
            _sw = [x.get("stopw") for x in tr if x.get("stopw") is not None]
            _md = [x.get("mae") for x in tr if x.get("mae") is not None]
            _tkeys = ("ts", "r", "outcome", "rsi", "btc_trend", "btc_vol", "session",
                      "stopw", "tppct", "mae", "entry", "stop", "target", "rr", "bars", "kind")
            _recent = sorted(tr, key=lambda z: z.get("ts") or 0, reverse=True)[:8]
            per.append({"symbol": s, "n": m, "winrate": round(w / m * 100, 1),
                        "exp": round(sm / m, 3), "sumR": round(sm, 2),
                        # per-coin diagnostics: stops-too-tight rate + how far losers ran
                        "stp": round(sum(1 for x in lz if x.get("stop_then_tp")) / (len(lz) or 1) * 100),
                        "cmp": round(sum(1 for x in tr if x.get("cmp")) / m * 100),
                        "stopw": round(sum(_sw) / len(_sw), 2) if _sw else None,
                        "mae": round(sum(_md) / len(_md), 2) if _md else None,
                        # this coin's most-recent trades (for the expandable row)
                        "trades": [{k: x.get(k) for k in _tkeys} for x in _recent]})
            allt += tr
    n = len(allt)
    if not n:
        return {"n": 0, "tf": tf, "side": side, "fees_bps": fees_bps, "coins": 0}
    w = sum(1 for x in allt if x["outcome"] == "win")
    sm = sum(x["r"] for x in allt); gsm = sum(x["gross"] for x in allt)
    wins = [x["r"] for x in allt if x["outcome"] == "win"]
    losses = [x["r"] for x in allt if x["outcome"] == "loss"]
    # Breakeven win-rate given the realised avg win/loss (so a finding can say how far off).
    aw = (sum(wins) / len(wins)) if wins else 0
    al = (-sum(losses) / len(losses)) if losses else 1
    breakeven = round(al / (aw + al) * 100, 1) if (aw + al) else None
    _tpp = [x.get("tppct") for x in allt if x.get("tppct") is not None]
    _wmfe = [x.get("mfe") for x in allt if x["outcome"] == "win" and x.get("mfe") is not None]
    # Split max-drawdown-while-open by OUTCOME: winners that barely dipped = clean entries;
    # winners that went deep underwater first = the entry could be tighter (better RR).
    _wmae = [x.get("mae") for x in allt if x["outcome"] == "win" and x.get("mae") is not None]
    _lmae = [x.get("mae") for x in allt if x["outcome"] == "loss" and x.get("mae") is not None]
    _allmae = [x.get("mae") for x in allt if x.get("mae") is not None]
    return {"n": n, "winrate": round(w / n * 100, 1), "exp": round(sm / n, 3),
            "sumR": round(sm, 2), "gross_exp": round(gsm / n, 3),
            "fee_drag": round((gsm - sm) / n, 3),
            "avg_win": round(aw, 2) if wins else None,
            "avg_loss": round(-al, 2) if losses else None,
            "breakeven_wr": breakeven,
            "avg_mae": round(sum(_allmae) / len(_allmae), 2) if _allmae else None,
            "avg_win_mae": round(sum(_wmae) / len(_wmae), 2) if _wmae else None,   # winners' avg drawdown
            "avg_loss_mae": round(sum(_lmae) / len(_lmae), 2) if _lmae else None,
            "avg_tp_pct": round(sum(_tpp) / len(_tpp), 2) if _tpp else None,
            "avg_win_mfe": round(sum(_wmfe) / len(_wmfe), 2) if _wmfe else None,
            "max_concurrent": _bt_max_concurrent(allt, tf),
            "monthly": _bt_monthly(allt),
            "insights": _bt_insights(allt), "findings": _bt_findings(allt),
            "by_breadth": _bt_by_breadth(allt),
            "portfolio": _bt_portfolio(allt),
            "sample": _bt_sample(results, syms),
            "per_symbol": sorted(per, key=lambda p: p["exp"], reverse=True),
            "tf": tf, "fees_bps": fees_bps, "side": side, "coins": len(per)}


def _bt_sample(results, syms, k=40):
    """A readable LOG of individual trades — the most recent k across all coins — each carrying
    its date, the coin, entry/stop/target, net-R result, and the MARKET ENVIRONMENT it was taken
    in (BTC regime + volatility, time-of-day session) plus the RSI that triggered it, so you can
    see exactly WHAT was going on and WHY each trade fired, not just the aggregate."""
    keys = ("ts", "r", "outcome", "rsi", "btc_trend", "btc_vol", "btc_state", "session",
            "stopw", "tppct", "mae", "entry", "stop", "target", "rr", "bars")
    rows = []
    for s in syms:
        tr = results.get(s)
        if not tr:
            continue
        for x in tr:
            rows.append({**{kk: x.get(kk) for kk in keys}, "symbol": s})
    rows.sort(key=lambda z: z.get("ts") or 0, reverse=True)
    return rows[:k]


def backtest_all(sess, market, tfs=("15m", "1h", "4h", "1d"), limit=1000,
                 fees_bps=5.0, symbols=None, strategy="revert", sides=("long", "short")):
    """Sweep the whole basket across EVERY timeframe and the requested SIDES in one pass —
    fetching each coin's candles once per TF and running the given strategy on them. Returns
    {tf: {side: agg, ...}}. Run in the background so the app shows a full TF × side edge
    matrix without the user triggering anything. Pass strategy='spot', sides=('long',) for the
    cash/spot sweep (long-only reversion, spot fees)."""
    syms = symbols or BACKTEST_BASKET
    out = {}
    for tf in tfs:
        # BTC behaviour for THIS timeframe — fetched once, shared across every coin so each
        # trade can be tagged with (and gated on) what the market leader was doing.
        # PHASE 1 — fetch every coin's candles ONCE (parallel), keep them so we can build a
        # market-wide breadth/dominance context before running any strategy.
        rows_by = {}
        def _fetch(sym):
            try:
                return sym, fetch_candles_deep(sess, sym, tf, limit, market)
            except Exception:
                return sym, None
        with ThreadPoolExecutor(max_workers=12) as ex:
            for fut in as_completed([ex.submit(_fetch, s) for s in syms]):
                try:
                    sym, r = fut.result()
                except Exception:
                    sym, r = None, None
                if sym and r:
                    rows_by[sym] = r
        # BTC own-regime context (fetch BTC separately if it's not in this basket, e.g. odd baskets)
        _brows = rows_by.get("BTCUSDT")
        if not _brows:
            try:
                _brows = fetch_candles_deep(sess, "BTCUSDT", tf, limit, market)
            except Exception:
                _brows = None
        btc_ctx = _btc_regime_ctx(_brows) if _brows else {}
        # Market breadth + dominance across the whole basket (per bar) — the environment gate.
        mkt_ctx = _market_ctx(rows_by, "BTCUSDT")

        # PHASE 2 — run every strategy/side on the pre-fetched candles.
        def _one(sym):
            rows = rows_by.get(sym)
            if not rows:
                return sym, None
            return sym, {sd: backtest_symbol(rows, sd, fees_bps=fees_bps, strategy=strategy,
                                             btc_ctx=btc_ctx, mkt_ctx=mkt_ctx, tf=tf)
                         for sd in sides}
        per = {sd: {} for sd in sides}
        with ThreadPoolExecutor(max_workers=12) as ex:
            for fut in as_completed([ex.submit(_one, s) for s in syms]):
                try:
                    sym, res = fut.result()
                except Exception:
                    sym, res = None, None
                if sym and res:
                    for sd in sides:
                        per[sd][sym] = res.get(sd)
        out[tf] = {sd: _bt_aggregate(per[sd], syms, tf, sd, fees_bps) for sd in sides}
    return out


def scan_symbol(sess: requests.Session, symbol: str, interval: str,
                cfg: dict) -> dict | None:
    return scan_symbol_multi(sess, symbol, interval, cfg)[0]


def retest_level(highs: list[float], lows: list[float], price: float,
                 side: str = "long", ema_now: float | None = None,
                 st_val: float | None = None) -> float | None:
    """A proper RETEST entry — a pullback you wait for rather than chasing the
    candle. For a long it sits BELOW price (nearest swing-low support / a reclaimed
    EMA or Supertrend below price); for a short it sits ABOVE. Returns None only if
    there's nothing sensible, in which case callers fall back to a small % pullback."""
    last = len(highs) - 1
    if side == "short":
        cand = [r for r in resistances_above(highs, last, price, max_n=3, min_gap=0.004)
                if r > price]
        if ema_now and ema_now > price:
            cand.append(ema_now)
        if st_val and st_val > price:
            cand.append(st_val)
        return round(min(cand) * 0.998, 10) if cand else round(price * 1.02, 10)
    cand = [s for s in supports_below(lows, last, price, max_n=3, min_gap=0.004)
            if s < price]
    if ema_now and ema_now < price:
        cand.append(ema_now)
    if st_val and st_val < price:
        cand.append(st_val)
    return round(max(cand) * 1.002, 10) if cand else round(price * 0.98, 10)


def scan_symbol_multi(sess: requests.Session, symbol: str, interval: str,
                      cfg: dict) -> tuple:
    """Fetch klines ONCE and run every detector (200-EMA reclaim, bull flag,
    narrow CPR, support bounce, falling wedge, bearish breakdown/retest). One
    request per symbol powers every scan. Returns an 11-tuple: the 8 detector dicts
    (each with 'symbol') or None — (ema, flag, cpr, bounce, wedge, short, st_bounce,
    early) — plus a graded best-long and best-short setup, and a 'coil' (squeeze /
    imminent-move) setup for the coin, all for the universe-wide leaderboards."""
    raw = fetch_candles(sess, symbol, interval, cfg["kline_limit"],
                        cfg.get("market", "spot"))
    if not raw or len(raw) < EMA_PERIOD + 2:
        return (None,) * 11
    # MEXC kline row: [openTime, open, high, low, close, volume, closeTime, ...]
    rows = raw[:-1]                       # drop the still-forming candle
    try:
        highs = [float(x[2]) for x in rows]
        lows = [float(x[3]) for x in rows]
        closes = [float(x[4]) for x in rows]
        vols = [float(x[5]) for x in rows]
    except (ValueError, IndexError):
        return (None,) * 8

    rv = rel_volume(vols)                       # relative volume of the latest candle
    ksup = key_supports(rows, lows, closes[-1])  # next 4h / daily / weekly support
    kres = key_resistances(rows, highs, closes[-1])  # next 4h / daily / weekly resist.
    _stale = not _klines_fresh(raw, interval)   # frozen on both futures & spot
    # BTC correlation over the recent window (how much this coin just mirrors BTC).
    _btc_ret = cfg.get("btc_returns")
    _corr = None
    if _btc_ret:
        _corr = pearson(pct_returns(closes)[-CORR_WINDOW:], _btc_ret)
    _pats = primary_pattern_mtf(rows, highs, lows, closes, vols)  # 4h/1D/1W formations
    _pp = best_pattern(_pats)                          # most salient across TFs
    _ms = market_structure(highs, lows, closes)  # for a per-setup bias label

    def _tf_bias(H, L, C):
        if len(C) < 12:
            return None
        m = market_structure(H, L, C)
        if m["structure"] == "uptrend" or m["choch"] == "bullish":
            return "bullish"
        if m["structure"] == "downtrend" or m["choch"] == "bearish":
            return "bearish"
        return "neutral"
    # Multi-timeframe bias: 4h from the base candles, Daily/Weekly aggregated from
    # them (cheap — no extra API calls).
    _tfb = {"4h": _tf_bias(highs, lows, closes)}
    for _gd, _lbl in ((1, "1d"), (7, "1w")):
        _hh, _ll, _cc, _vv = _agg_series(rows, _gd)
        _tfb[_lbl] = _tf_bias(_hh, _ll, _cc)
    if _ms["choch"] == "bullish":
        _bias = "Bullish CHoCH"
    elif _ms["structure"] == "uptrend":
        _bias = "Bullish"
    elif _ms["structure"] == "downtrend":
        _bias = "Bearish"
    else:
        _bias = "Range"
    _dir = ("bullish" if _bias in ("Bullish", "Bullish CHoCH")
            else "bearish" if _bias == "Bearish" else "neutral")
    _e = ema(closes, EMA_PERIOD)
    _ema_now = _e[-1] if _e else None
    _stv, _std = supertrend(highs, lows, closes)          # for a retest anchor

    def confirmed(d: dict, boost: bool) -> dict:
        """Attach relative volume, multi-timeframe supports, a market-structure
        bias label, a proper (non-chasing) retest entry, and — for momentum setups —
        a volume-confirmation score nudge."""
        _side = d.get("side", "long")
        _rt = retest_level(highs, lows, closes[-1], _side, _ema_now, _stv)
        d = {"symbol": symbol, "rvol": rv, "bias": _bias, "bias_dir": _dir,
             "choch": _ms["choch"],
             "btc_corr": (round(_corr, 2) if _corr is not None else None),
             "pattern": _pp, "patterns_mtf": _pats,
             "data_stale": _stale, "tf_bias": _tfb, "retest_entry": _rt,
             **ksup, **d}
        if boost and rv:
            factor = 1.0 + 0.15 * max(0.0, min(1.0, (rv - 1.0) / 1.5))
            d["score"] = round(min(100.0, d["score"] * factor), 1)
        return d

    ema_hit = flag_hit = cpr_hit = bounce_hit = None
    ok, d = detect_cross_and_retest(
        highs, lows, closes,
        ema_period=EMA_PERIOD, lookback=cfg["lookback"],
        retest_tol=cfg["retest_tol"], break_tol=cfg["break_tol"],
        max_above_now=cfg["max_above_now"], min_slope=cfg["min_slope"],
    )
    if ok:
        ema_hit = confirmed(d, boost=True)

    ok2, f = detect_bull_flag(
        highs, lows, closes, vols,
        pole_min_gain=cfg.get("pole_min_gain", 0.15),
        max_retrace=cfg.get("flag_max_retrace", 0.5),
    )
    if ok2:
        flag_hit = confirmed(f, boost=False)     # flags want volume to DRY UP

    ok3, c = detect_narrow_cpr(
        rows, cpr_max_width_pct=cfg.get("cpr_max_width_pct", 0.75))
    if ok3:
        cpr_hit = confirmed(c, boost=True)

    ok4, b = detect_support_bounce(rows, highs, lows, closes, vols)
    if ok4:
        bounce_hit = confirmed(b, boost=True)

    wedge_hit = short_hit = None
    ok5, w = detect_falling_wedge_mtf(rows, highs, lows, closes, vols)
    if ok5:
        wedge_hit = confirmed(w, boost=w.get("broken_out", False))

    ok6, s = detect_breakdown_and_retest(
        highs, lows, closes,
        ema_period=EMA_PERIOD, lookback=cfg["lookback"],
        retest_tol=cfg["retest_tol"], break_tol=cfg["break_tol"],
        max_below_now=cfg["max_above_now"], max_slope=-cfg["min_slope"],
    )
    if ok6:
        short_hit = confirmed(s, boost=True)
        short_hit["bias_dir"] = "bearish"        # a short is bearish by definition

    st_bounce_hit = None
    ok7, sb = detect_supertrend_bounce(rows, highs, lows, closes, vols)
    if ok7:
        st_bounce_hit = confirmed(sb, boost=True)

    early_hit = None
    ok8, el = detect_early_setup(rows, highs, lows, closes, vols)
    if ok8:
        early_hit = confirmed(el, boost=False)   # early = coiling, volume not required

    # Universe-wide leaderboard: grade the best long AND best short for EVERY coin
    # (not just pattern hits) so Top-setups can rank all ~500 pairs. Uses only data
    # already computed above — no extra API calls.
    _dets = {"ema": ema_hit, "flag": flag_hit, "cpr": cpr_hit, "bounce": bounce_hit,
             "wedge": wedge_hit, "short": short_hit, "stb": st_bounce_hit,
             "early": early_hit}
    long_setup, short_setup = leaderboard_setups(
        symbol, highs, lows, closes, vols, _ema_now, _tfb, _bias, _ms,
        ksup, kres, rv, _dets, btc_trend=cfg.get("btc_trend"), tf_label=interval)
    # Attach BTC correlation so the boards can tell a coin that just follows BTC from one
    # trading on its own — a decorrelated coin's setup shouldn't be judged by BTC's regime.
    _bc = round(_corr, 2) if _corr is not None else None
    long_setup["btc_corr"] = short_setup["btc_corr"] = _bc
    coil_setup = squeeze_setup(symbol, highs, lows, closes, _tfb, _bias,
                               (round(atr(highs, lows, closes) / closes[-1] * 100, 2)
                                if closes[-1] else None), ksup, kres)
    if coil_setup:
        coil_setup["btc_corr"] = _bc
    if _stale:
        long_setup["data_stale"] = short_setup["data_stale"] = True
        if coil_setup:
            coil_setup["data_stale"] = True

    return (ema_hit, flag_hit, cpr_hit, bounce_hit, wedge_hit, short_hit,
            st_bounce_hit, early_hit, long_setup, short_setup, coil_setup)


def supports_below(lows: list[float], upto: int, price: float,
                   left: int = 3, right: int = 3, window: int = 300,
                   max_n: int = 3, min_gap: float = 0.0) -> list[float]:
    """Nearest swing-low pivots BELOW `price`, nearest first — support levels."""
    ceil = price * (1.0 - min_gap)
    start = max(left, upto - window)
    res = []
    for i in range(start, upto - right + 1):
        l = lows[i]
        if all(l <= lows[i - d] for d in range(1, left + 1)) and \
           all(l <= lows[i + d] for d in range(1, right + 1)) and l < ceil:
            res.append(l)
    res.sort(reverse=True)                       # nearest below first
    out: list[float] = []
    for l in res:
        if not out or l < out[-1] * 0.995:
            out.append(l)
        if len(out) >= max_n:
            break
    return out


def normalize_symbol(raw: str, quote: str = "USDT") -> str:
    """Turn user input like 'btc', 'BTC/USDT', ' eth ' into 'BTCUSDT'."""
    s = raw.strip().upper().replace("/", "").replace("-", "").replace(" ", "")
    if not s:
        return ""
    if s.endswith(quote) or "_" in s:
        return s
    return s + quote


def analyze_symbol(sess: requests.Session, symbol: str, interval: str,
                   cfg: dict) -> dict:
    """On-demand technical read of one coin on the 4h chart: trend vs the 200 EMA,
    support/resistance, a suggested entry / stop / three targets with R:R, and
    whether it currently matches either scan. Technical estimate, NOT advice."""
    mkt = cfg.get("market", "futures")
    raw = fetch_candles(sess, symbol, interval, cfg.get("kline_limit", 1000), mkt)
    if not raw or len(raw) < EMA_PERIOD + 2:
        n = len(raw) if raw else 0
        need = EMA_PERIOD + 2
        lower = {"1w": "Daily / 4h", "1d": "4h / 1h", "4h": "1h / 15m",
                 "1h": "15m", "15m": ""}.get(interval, "a lower timeframe")
        tip = f" Try {lower}." if lower else ""
        return {"error": f"Not enough {interval} history for '{symbol}' — it has only "
                         f"{n} {interval} candle(s), but the 200 EMA needs {need}. "
                         f"This coin is likely too new to analyse on the {interval} "
                         f"chart.{tip} (Or check it's listed on MEXC {mkt}.)"}
    data_stale = not _klines_fresh(raw, interval)   # both futures & spot came back old
    rows = raw[:-1]
    try:
        highs = [float(x[2]) for x in rows]
        lows = [float(x[3]) for x in rows]
        closes = [float(x[4]) for x in rows]
        vols = [float(x[5]) for x in rows]
    except (ValueError, IndexError):
        return {"error": "Could not parse candles."}

    e = ema(closes, EMA_PERIOD)
    last = len(closes) - 1
    # Current price = the LIVE last-traded price (same on every timeframe),
    # falling back to the last closed candle if the ticker call fails.
    live = fetch_live_price(sess, symbol, mkt)
    price = live if live else closes[last]
    # Open interest context (perps only) — is the move backed by real positioning?
    oi_data = fetch_open_interest(sess, symbol) if mkt == "futures" else None
    # Deeper derivatives HISTORY (OI divergence, funding, long/short) from Coinalyze
    # — only when a COINALYZE_API_KEY is configured; otherwise stays None.
    deriv = fetch_derivatives(symbol) if mkt == "futures" else None
    ema_now = e[last]
    if ema_now is None:
        return {"error": "Not enough data to compute the 200 EMA."}

    j = max(EMA_PERIOD - 1, last - 20)
    slope = (ema_now - e[j]) / e[j] if e[j] else 0.0
    trend = "up" if slope > 0.003 else ("down" if slope < -0.003 else "flat")
    above = price >= ema_now
    pct_vs_ema = (price - ema_now) / ema_now * 100

    sup = supports_below(lows, last, price, max_n=6, min_gap=0.005)
    # Pull MORE overhead swing highs (not just the nearest few) so the target
    # ladder doesn't leave a big gap between the near-term ceilings and the
    # Fibonacci measured-move targets — it should surface the mid-range shelves
    # (e.g. a prior consolidation zone well above price) too.
    res = resistances_above(highs, last, price, max_n=12, min_gap=0.008)
    ksup = key_supports(rows, lows, price)       # next 4h / daily / weekly support
    kres = key_resistances(rows, highs, price)   # next 4h / daily / weekly resistance
    # Per-timeframe nearest support/resistance (each from its own candles) so the
    # entry engine can prefer a stronger higher-TF level price reaches first.
    tf_levels = tf_levels_for(sess, symbol, price, cfg)
    a = atr(highs, lows, closes)

    # 200 EMA on the daily & weekly frames (fetched separately; None if the coin
    # doesn't have enough history — common for newer alts, esp. weekly).
    def _tf_ema(iv):
        rr = fetch_candles(sess, symbol, iv, 1000, cfg.get("market", "spot"))
        if not rr or len(rr) < EMA_PERIOD + 2:
            return None
        ev = ema([float(x[4]) for x in rr[:-1]], EMA_PERIOD)
        return ev[-1]
    ema_1d = _tf_ema("1d")
    ema_1w = _tf_ema("1w")

    # Chart formations on each timeframe (4h / Daily / Weekly).
    def _tf_patterns(iv):
        rr = fetch_candles(sess, symbol, iv, 400, cfg.get("market", "futures"))
        if not rr or len(rr) < 30:
            return []
        rws = rr[:-1]
        try:
            H = [float(x[2]) for x in rws]
            L = [float(x[3]) for x in rws]
            C = [float(x[4]) for x in rws]
            V = [float(x[5]) for x in rws]
        except (ValueError, IndexError):
            return []
        return detect_chart_patterns(H, L, C, V)
    patterns = {"5m": _tf_patterns("5m"),
                "15m": _tf_patterns("15m"),
                "1h": _tf_patterns("1h"),
                "4h": detect_chart_patterns(highs, lows, closes, vols)
                if interval == "4h" else _tf_patterns("4h"),
                "1d": _tf_patterns("1d"),
                "1w": _tf_patterns("1w")}

    # Multi-timeframe bias (bullish / bearish / neutral) on 1h / 4h / Daily / Weekly.
    def _bias_of(iv, base=None):
        H = L = C = None
        if base is not None:
            H, L, C = base
        else:
            rr = fetch_candles(sess, symbol, iv, 400, cfg.get("market", "futures"))
            if not rr or len(rr) < 15:
                return None
            rws = rr[:-1]
            try:
                H = [float(x[2]) for x in rws]
                L = [float(x[3]) for x in rws]
                C = [float(x[4]) for x in rws]
            except (ValueError, IndexError):
                return None
        m = market_structure(H, L, C)
        if m["structure"] == "uptrend" or m["choch"] == "bullish":
            return "bullish"
        if m["structure"] == "downtrend" or m["choch"] == "bearish":
            return "bearish"
        return "neutral"
    tf_bias = {"5m": _bias_of("5m"),
               "15m": _bias_of("15m"),
               "1h": _bias_of("1h"),
               "4h": _bias_of("4h", (highs, lows, closes) if interval == "4h" else None),
               "1d": _bias_of("1d"),
               "1w": _bias_of("1w")}

    atr_pct = round(a / price * 100, 2) if price else None
    # BTC correlation over the recent window (independence vs "just follows BTC").
    btc_corr = None
    try:
        braw = fetch_candles(sess, "BTCUSDT", interval, cfg.get("kline_limit", 1000), mkt)
        if braw and len(braw) > 2:
            bcl = [float(x[4]) for x in braw[:-1]]
            btc_corr = pearson(pct_returns(closes)[-CORR_WINDOW:],
                               pct_returns(bcl)[-CORR_WINDOW:])
    except (requests.RequestException, ValueError, IndexError, TypeError):
        btc_corr = None
    r14 = rsi(closes)
    rsi_div = detect_rsi_divergence(highs, lows, closes)       # regular / hidden RSI divergence
    squeeze_pct = bbw_squeeze_pct(closes)                       # Bollinger-band squeeze / coil (0-100)
    st_val, st_dir = supertrend(highs, lows, closes)          # 4h Supertrend (or the selected TF)
    st_role = None
    if st_val is not None:
        st_role = "support" if st_dir == "up" else "resistance"
    ms = market_structure(highs, lows, closes)
    vp = volume_profile(vols, closes)
    rv = rel_volume(vols)
    win = closes[-120:] if len(closes) >= 120 else closes
    hh = max(highs[-120:]) if len(highs) >= 120 else max(highs)
    ll = min(lows[-120:]) if len(lows) >= 120 else min(lows)
    range_pos = round((price - ll) / (hh - ll) * 100, 1) if hh > ll else None

    entry = price
    okE, dE = detect_cross_and_retest(
        highs, lows, closes, ema_period=EMA_PERIOD,
        lookback=cfg.get("lookback", 30), retest_tol=cfg.get("retest_tol", 0.02),
        break_tol=cfg.get("break_tol", 0.005),
        max_above_now=cfg.get("max_above_now", 0.08),
        min_slope=cfg.get("min_slope", 0.0))
    okF, dF = detect_bull_flag(
        highs, lows, closes, vols,
        pole_min_gain=cfg.get("pole_min_gain", 0.15),
        max_retrace=cfg.get("flag_max_retrace", 0.5))
    okB, dB = detect_support_bounce(rows, highs, lows, closes, vols)

    if above and trend == "up":
        bias = "bullish"
    elif not above and trend == "down":
        bias = "bearish"
    else:
        bias = "neutral"

    # Directional lean: weigh the bullish vs bearish evidence into Long/Short/Wait.
    long_pts = short_pts = 0
    long_pts += 2 if (above and trend == "up") else 0
    short_pts += 2 if ((not above) and trend == "down") else 0
    if ms["choch"] == "bullish":
        long_pts += 1
    if ms["choch"] == "bearish":
        short_pts += 1
    if okB:
        long_pts += 1
    if okE or okF:
        long_pts += 1
    if r14 is not None and r14 < 30:
        long_pts += 1        # oversold — bounce potential
    if r14 is not None and r14 > 70:
        short_pts += 1        # overbought — pullback risk
    if rsi_div:              # momentum divergence is a leading signal — weight it
        if rsi_div["dir"] == "bullish":
            long_pts += 1
        elif rsi_div["dir"] == "bearish":
            short_pts += 1
    # Liquidation flush is contrarian: a heavy one-sided flush near an RSI extreme often
    # marks exhaustion — longs flushed into weakness = bounce; shorts squeezed into
    # strength = fade. Only nudge when the extreme confirms (avoids catching a knife).
    if deriv:
        _lqs = deriv.get("liq_side")
        if _lqs == "long" and r14 is not None and r14 < 42:
            long_pts += 1        # capitulation flush — bounce potential
        elif _lqs == "short" and r14 is not None and r14 > 58:
            short_pts += 1       # short squeeze into strength — fade potential
    net = long_pts - short_pts
    direction = "Long" if net >= 2 else ("Short" if net <= -2 else "Neutral")

    # --- hover explanations for the bias / direction / structure pills ---
    bias_reason = (f"{bias.upper()}: price is {abs(pct_vs_ema):.1f}% "
                   f"{'above' if above else 'below'} a {trend}-sloping 200 EMA.")
    choch_note = (f" A {ms['choch']} CHoCH means the {ms['structure']} just printed "
                  f"its first opposite structure break — an early turn signal."
                  if ms["choch"] else "")
    struct_reason = (f"Market structure is {ms['structure']} on this timeframe "
                     f"(from swing highs/lows).{choch_note}")
    dir_reason = (f"{direction.upper()}: {long_pts} bullish vs {short_pts} bearish "
                  f"signals across the 200-EMA side, market structure & CHoCH, RSI, "
                  f"and active setups — the stronger side wins.")

    # --- DIRECTIONAL trade plans. Build BOTH a long and a short plan so the UI
    # can toggle perspective: a coin that's currently bearish still gets a valid
    # reversal-LONG plan (buy support, targets up), and a bullish coin gets a
    # SHORT plan (sell resistance, targets down). Each plan carries its own
    # entries, stops, target ladder and R:R. (short = stops above, targets below)
    def _build_plan(side):
        if side == "short":
            r0 = res[0] if res else (ema_now if not above else price * 1.03)
            optimal_entry = r0                       # sell into the nearest resistance
            sl_tight = r0 + 0.5 * a                   # stop ABOVE resistance
            r1 = res[1] if len(res) > 1 else r0
            sl_wide = max(r1 + 1.0 * a, sl_tight)
        else:
            if sup:
                optimal_entry = sup[0]
            elif above:
                optimal_entry = round(ema_now * 1.002, 10)
            else:
                optimal_entry = price
            near_support = sup[0] if sup else (ema_now if above else price * 0.97)
            sl_tight = near_support - 0.5 * a
            deep_support = sup[1] if len(sup) > 1 else (sup[0] if sup else near_support)
            sl_wide = min(deep_support - 1.0 * a, sl_tight)

        # Volatility floor on the tight stop, measured from the intended fill so
        # the stop stays sensibly beyond the entry and out of the noise.
        sl_tight, sl_wide = _apply_stop_floor(entry, sl_tight, sl_wide, a,
                                              plan_entry=optimal_entry)

        # Candidate stop-loss levels, each labelled by what defines it. For a long
        # these sit BELOW price; for a short ABOVE. ATR-buffered.
        stop_levels = []

        def _add_stop(level, basis):
            if level is None or level <= 0:
                return
            if side == "short" and level <= price:
                return
            if side != "short" and level >= price:
                return
            stop_levels.append({"level": round(level, 10), "basis": basis,
                                "pct": round((level - price) / price * 100, 2)})

        if side == "short":
            if res:
                _add_stop(res[0] + 0.3 * a, "Above the nearest swing high")
                if len(res) > 1:
                    _add_stop(res[1] + 0.5 * a, "Above a deeper swing high")
            if st_val and st_role == "resistance":
                _add_stop(st_val + 0.2 * a, f"Above the {interval} Supertrend line")
            if not above:
                _add_stop(ema_now + 0.2 * a, f"Above the 200 EMA ({interval})")
            if kres.get("res_1d"):
                _add_stop(kres["res_1d"] + 0.3 * a, "Above the Daily swing-high resistance")
            # Cross-timeframe structural stops — a swing high on ANY chart is a real
            # invalidation level, so the stop can be anchored to the best one across
            # timeframes, not just the viewed chart. (deduped below)
            _sn = {"1h": "1h", "4h": "4h", "1d": "Daily", "1w": "Weekly"}
            for _tf in ("1h", "4h", "1d", "1w"):
                _lv = (tf_levels.get(_tf) or {}).get("res")
                if _lv and _lv > price:
                    _add_stop(_lv + 0.3 * a, f"Above the {_sn[_tf]} swing high")
        else:
            if sup:
                _add_stop(sup[0] - 0.3 * a, "Below the nearest swing low")
                if len(sup) > 1:
                    _add_stop(sup[1] - 0.5 * a, "Below a deeper swing low")
            if st_val and st_role == "support":
                _add_stop(st_val - 0.2 * a, f"Below the {interval} Supertrend line")
            if above:
                _add_stop(ema_now - 0.2 * a, f"Below the 200 EMA ({interval})")
            if ksup.get("sup_1d"):
                _add_stop(ksup["sup_1d"] - 0.3 * a, "Below the Daily swing-low support")
            if ksup.get("sup_1w"):
                _add_stop(ksup["sup_1w"] - 0.3 * a, "Below the Weekly swing-low support")
            # Cross-timeframe structural stops — a swing low on ANY chart is a real
            # invalidation level, so the stop can be anchored to the best one across
            # timeframes, not just the viewed chart. (deduped below)
            _sn = {"1h": "1h", "4h": "4h", "1d": "Daily", "1w": "Weekly"}
            for _tf in ("1h", "4h", "1d", "1w"):
                _lv = (tf_levels.get(_tf) or {}).get("sup")
                if _lv and _lv < price:
                    _add_stop(_lv - 0.3 * a, f"Below the {_sn[_tf]} swing low")
        # Always-available candidates so the menu is never bare (e.g. a coin sitting
        # below all its higher-TF supports): the recent range extreme plus two
        # volatility (ATR) stops giving graduated risk choices.
        _base_lo = min(lows[-30:]) if len(lows) >= 30 else min(lows)
        _base_hi = max(highs[-30:]) if len(highs) >= 30 else max(highs)
        if side == "short":
            _add_stop(_base_hi + 0.5 * a, "Above the recent 30-bar range high")
            _add_stop(price + 1.5 * a, "≈1.5× ATR above price (volatility stop)")
            _add_stop(price + 2.5 * a, "≈2.5× ATR above price (wider volatility stop)")
        else:
            _add_stop(_base_lo - 0.5 * a, "Below the recent 30-bar range low")
            _add_stop(price - 1.5 * a, "≈1.5× ATR below price (volatility stop)")
            _add_stop(price - 2.5 * a, "≈2.5× ATR below price (wider volatility stop)")
        stop_levels.sort(key=lambda s: abs(s["level"] - price))
        _sd, _seen = [], []
        for s in stop_levels:
            if all(abs(s["level"] - x) / price > 0.004 for x in _seen):
                _sd.append(s)
                _seen.append(s["level"])
        stop_levels = _sd[:6]

        # Target ladder — structural levels blended with Fibonacci extensions,
        # deduped, up to 8. ALWAYS yields targets (falls back to measured % steps).
        # Take-profits are pulled from EVERY timeframe's chart (1h/4h/Daily/Weekly),
        # not just the one being viewed — a Daily or Weekly swing high/low is a real
        # ceiling/floor worth targeting, so it belongs on the ladder too.
        _tfn = {"1h": "1h", "4h": "4h", "1d": "Daily", "1w": "Weekly"}
        rng = hh - ll if hh > ll else 0.0
        if side == "short":                          # targets go DOWN
            cand = [(lvl, "support (prior swing low)") for lvl in sup]
            for _tf in ("1h", "4h", "1d", "1w"):     # cross-timeframe supports below price
                _s = (tf_levels.get(_tf) or {}).get("sup")
                if _s and _s < price:
                    cand.append((_s, f"{_tfn[_tf]} chart support (swing low)"))
            if ksup.get("sup_1d") and ksup["sup_1d"] < price:
                cand.append((ksup["sup_1d"], "Daily swing-low support"))
            if ksup.get("sup_1w") and ksup["sup_1w"] < price:
                cand.append((ksup["sup_1w"], "Weekly swing-low support"))
            for ratio in (0.272, 0.414, 0.618, 1.0, 1.618, 2.0):
                lvl = ll - rng * ratio
                if 0 < lvl < price:
                    cand.append((lvl, f"{ratio:g}× Fibonacci downside extension"))
            cand.sort(key=lambda x: -x[0])
            picked = []
            for lvl, kind in cand:
                if lvl >= price:
                    continue
                if not picked or lvl < picked[-1][0] * 0.995:
                    picked.append((lvl, kind))
            if not picked:
                picked = [(round(entry * (1 - p), 10), f"{int(p*100)}% measured drop")
                          for p in (0.1, 0.2, 0.3, 0.4, 0.5)]
        else:                                        # targets go UP
            cand = [(lvl, "overhead resistance (prior swing high)") for lvl in res]
            for _tf in ("1h", "4h", "1d", "1w"):     # cross-timeframe resistances above price
                _r = (tf_levels.get(_tf) or {}).get("res")
                if _r and _r > price:
                    cand.append((_r, f"{_tfn[_tf]} chart resistance (swing high)"))
            if kres.get("res_1d") and kres["res_1d"] > price:
                cand.append((kres["res_1d"], "Daily swing-high resistance"))
            if kres.get("res_1w") and kres["res_1w"] > price:
                cand.append((kres["res_1w"], "Weekly swing-high resistance"))
            for ratio in (0.272, 0.414, 0.618, 1.0, 1.618, 2.0):
                lvl = hh + rng * ratio
                if lvl > price:
                    cand.append((lvl, f"{ratio:g}× Fibonacci extension of the recent range"))
            cand.sort(key=lambda x: x[0])
            picked = []
            for lvl, kind in cand:
                if lvl <= price:
                    continue
                if not picked or lvl > picked[-1][0] * 1.005:
                    picked.append((lvl, kind))
            if not picked:
                picked = [(round(entry * (1 + p), 10), f"{int(p*100)}% measured move")
                          for p in (0.1, 0.2, 0.3, 0.4, 0.5)]
        picked = picked[:8]
        # Breakout "runner" targets — a coin basing far below its prior major highs
        # can run a long way (2x/3x) if it breaks out of the base. Append the prior
        # major high (and a halfway rung) as ambitious runner targets so the ladder
        # shows that upside; they're the furthest, so add them AFTER the 8-cap, which
        # would otherwise drop them. Low reach / high R:R — a bag to hold into strength.
        if side != "short":
            _hi_lb = highs[-500:] if len(highs) >= 500 else highs
            _lo_lb = lows[-500:] if len(lows) >= 500 else lows
            _major_hi = max(_hi_lb) if _hi_lb else 0.0
            _major_lo = min(_lo_lb) if _lo_lb else 0.0
            _runners = []
            # The most sensible runner targets are the REAL higher-timeframe resistance
            # levels above price — the prior Daily/Weekly swing highs a recovery has to
            # clear on the way up. We blend those with the Fibonacci levels of the whole
            # decline (golden pocket / prior high / 1.618 extension) as measured-move
            # projections. Low reach / high R:R — a bag to hold into strength.
            for _p in htf_swing_highs(rows, 1):        # Daily swing highs
                if _p > price * 1.12:
                    _runners.append((_p, "Daily swing-high resistance — breakout runner"))
            for _p in htf_swing_highs(rows, 7):        # Weekly swing highs
                if _p > price * 1.12:
                    _runners.append((_p, "Weekly swing-high resistance — breakout runner"))
            if _major_hi > price * 1.25 and _major_hi > _major_lo > 0:
                _rng = _major_hi - _major_lo
                for _f, _lbl in ((0.618, "0.618 Fib — golden pocket of the decline"),
                                 (1.0,   "prior major high (full retrace of the decline)"),
                                 (1.618, "1.618 Fib extension beyond the prior high")):
                    _lv = _major_lo + _rng * _f
                    if _lv > price * 1.08:
                        _runners.append((_lv, f"{_lbl} — breakout runner"))
            # Dedupe among themselves + against existing rungs; keep the nearest few so
            # the ladder isn't flooded with far targets.
            _runners.sort(key=lambda x: x[0])
            _radd = 0
            for lvl, kind in _runners:
                if lvl <= price * 1.05:
                    continue
                if any(abs(lvl - p0) / lvl <= 0.012 for p0, _ in picked):
                    continue
                picked.append((lvl, kind))
                _radd += 1
                if _radd >= 5:
                    break
            picked.sort(key=lambda x: x[0])
            picked = picked[:12]
        target_ladder = [{"level": lvl, "kind": kind,
                          "pct": round((lvl - entry) / entry * 100, 1),
                          "rr": round((lvl - entry) / (entry - sl_tight), 2)
                                 if entry != sl_tight else None,
                          "runner": ("runner" in kind)}
                         for lvl, kind in picked]
        plan_tps = [(t["level"],
                     f"{t['kind']} at {t['level']:.6g}.",
                     abs(t["level"] - ema_now) / ema_now < 0.002 if ema_now else False)
                    for t in target_ladder[:5]]
        _st_basis = (f"Above the nearest resistance / rejected level at {sl_tight:.6g}"
                     if side == "short" else
                     f"Below the nearest support / structure at {sl_tight:.6g}")
        bundle = level_bundle(
            entry, sl_tight, sl_wide, plan_tps,
            sl_tight_basis=_st_basis + ", ATR-buffered.",
            sl_wide_basis=(f"A deeper structural level at {sl_wide:.6g} for more room, "
                           f"ATR-buffered."))
        retest = retest_level(highs, lows, price, side, ema_now,
                              st_val if st_role else None)
        return {"side": side, "optimal_entry": optimal_entry, "retest_entry": retest,
                "stop_levels": stop_levels, "target_ladder": target_ladder, **bundle}

    plan_long = _build_plan("long")
    plan_short = _build_plan("short")
    default_side = "short" if direction == "Short" else "long"
    active = plan_short if default_side == "short" else plan_long
    # Expose the default-side plan at the top level (backward compatible), and
    # ship BOTH plans so the UI can switch perspective without a re-fetch.
    side = default_side
    optimal_entry = active["optimal_entry"]
    stop_levels = active["stop_levels"]
    target_ladder = active["target_ladder"]
    _active_retest = active["retest_entry"]
    bundle = {k: v for k, v in active.items()
              if k not in ("side", "optimal_entry", "retest_entry",
                           "stop_levels", "target_ladder")}

    notes = []
    if data_stale:
        notes.append("⚠ MEXC's kline history for this coin looks stale (the latest "
                     "candle isn't recent), so the indicators below may be out of "
                     "date — confirm on the chart before trusting them.")
    notes.append(f"Price is {abs(pct_vs_ema):.1f}% {'above' if above else 'below'} "
                 f"the 200 EMA, which is sloping {trend} — {bias} trend bias.")
    notes.append(f"Market structure is {ms['structure']}"
                 + (f", with a fresh {ms['choch']} CHoCH (change of character)."
                    if ms['choch'] else "."))
    if r14 is not None:
        state = ("oversold" if r14 < 30 else "overbought" if r14 > 70 else "neutral")
        notes.append(f"RSI(14) is {r14} ({state}).")
    if rsi_div:
        _di = "✅" if rsi_div["dir"] == "bullish" else "⚠"
        notes.append(f"{_di} {rsi_div['label']} — {rsi_div['note']} (factored into the directional lean below.)")
    else:
        notes.append(f"No RSI divergence between the last two swings on the {interval} — "
                     f"momentum is confirming price rather than diverging from it.")
    if squeeze_pct is not None and squeeze_pct >= 70:
        notes.append(f"🚀 Volatility squeeze — the Bollinger bands are tighter than {squeeze_pct}% of "
                     f"their recent range on the {interval} (a narrow, coiled range). Compressed markets "
                     f"tend to EXPAND soon, so expect a bigger move; trade the break of the range.")
    elif squeeze_pct is not None and squeeze_pct <= 25:
        notes.append(f"Bollinger bands are wide (squeeze {squeeze_pct}%) — volatility is already elevated "
                     f"on the {interval}, so this is less a coiled setup and more likely mid-move.")
    if st_val is not None:
        notes.append(
            f"Supertrend ({interval}) is at <b>{st_val:.6g}</b>, "
            f"{'below' if st_role == 'support' else 'above'} price — trend is "
            f"{'up, so it acts as a trailing <b>support</b>' if st_role == 'support' else 'down, so it acts as <b>resistance</b>'}; "
            f"a flip through it would signal the {interval} trend changing.")
    notes.append(f"Volume is {vp['vol_trend']} vs its average"
                 + (f" (x{vp['vol_ratio']})" if vp['vol_ratio'] else "")
                 + f", with {vp['pressure']} in control of recent candles"
                 + (f"; the latest candle is at {rv}x average volume"
                    + (" — confirming" if rv and rv > 1.3 else "") if rv else "")
                 + ".")
    if range_pos is not None:
        notes.append(f"Price sits {range_pos}% up its recent range "
                     f"(0% = range low, 100% = range high); ATR ≈ {atr_pct}% of price.")
    if sup:
        notes.append("Nearest supports: " + ", ".join(f"{s:.6g}" for s in sup[:3]) + ".")
    if res:
        notes.append("Nearest resistances: " + ", ".join(f"{s:.6g}" for s in res[:3]) + ".")
    def _dd(sv):
        return f"{sv:.6g} (-{(price - sv) / price * 100:.1f}%)"
    tf_parts = []
    if ksup["sup_4h"]:
        tf_parts.append("4h " + _dd(ksup["sup_4h"]))
    if ksup["sup_1d"]:
        tf_parts.append("Daily " + _dd(ksup["sup_1d"]))
    if ksup["sup_1w"]:
        tf_parts.append("Weekly " + _dd(ksup["sup_1w"]))
    if tf_parts:
        notes.append("Next major support (drawdown from here) — "
                     + ", ".join(tf_parts) + ".")
    matched = []
    if okE:
        matched.append(f"200-EMA reclaim (score {dE['score']})")
    if okF:
        matched.append(f"bull flag (pole {dF['pole_gain_pct']}%, score {dF['score']})")
    if okB:
        matched.append(f"support bounce off {dB['tf']} support at {dB['support']:.6g} "
                       f"({dB['touches']} touches, score {dB['score']})")
    notes.append("Active setups: " + (", ".join(matched) if matched else "none right now") + ".")
    if oi_data and oi_data.get("oi_usd"):
        _m = oi_data["oi_usd"]
        _oi_s = (f"${_m/1e9:.2f}B" if _m >= 1e9 else f"${_m/1e6:.1f}M" if _m >= 1e6 else f"${_m/1e3:.0f}K")
        _c = oi_data.get("chg24")
        notes.append(f"Open interest: ~{_oi_s} notional"
                     + (f" · price {'+' if _c >= 0 else ''}{_c:.1f}% (24h)" if _c is not None else "")
                     + ". Price rising WITH open interest = new money backing the move; "
                       "price up while OI is flat or falling = a weaker, short-covering move "
                       "(possible fake pump — confirm before chasing).")
    if deriv:
        if deriv.get("divergence_note"):
            _oc = deriv.get("oi_chg_pct")
            _icon = "✅" if deriv.get("divergence") in ("real_up", "real_down") else \
                    "⚠" if deriv.get("divergence") in ("fake_up", "exhaust_down") else "•"
            notes.append(f"{_icon} OI divergence (24h): {deriv['divergence_note']}"
                         + (f" — open interest {'+' if _oc >= 0 else ''}{_oc:.1f}% vs price "
                            f"{'+' if deriv.get('price_chg_pct',0) >= 0 else ''}{deriv.get('price_chg_pct'):.1f}%."
                            if _oc is not None and deriv.get('price_chg_pct') is not None else "."))
        _f = deriv.get("funding")
        if _f is not None:
            _fp = _f * 100
            if _fp >= 0.03:
                notes.append(f"Funding is high positive ({_fp:+.3f}%) — longs are paying shorts, "
                             "positioning is crowded long (raises squeeze-DOWN risk / long-side cost).")
            elif _fp <= -0.03:
                notes.append(f"Funding is negative ({_fp:+.3f}%) — shorts are paying longs, "
                             "positioning is crowded short (squeeze-UP fuel).")
            else:
                notes.append(f"Funding is roughly neutral ({_fp:+.3f}%) — balanced positioning.")
        _ls = deriv.get("long_short")
        if _ls is not None:
            _bias = ("more longs than shorts" if _ls > 1.05 else
                     "more shorts than longs" if _ls < 0.95 else "balanced")
            notes.append(f"Long/short accounts ratio: {_ls:.2f} ({_bias}).")
        if deriv.get("liq_note"):
            _lqic = "🩸" if deriv.get("liq_side") in ("long", "short") else "•"
            notes.append(f"{_lqic} Liquidations (24h): {deriv['liq_note']}.")
    if direction == "Long":
        notes.append(f"Directional lean: <b>LONG</b> — bullish signals outweigh "
                     f"bearish ({long_pts} vs {short_pts}).")
    elif direction == "Short":
        notes.append(f"Directional lean: <b>SHORT</b> — bearish signals outweigh "
                     f"bullish ({short_pts} vs {long_pts}).")
    else:
        notes.append(f"Directional lean: <b>NEUTRAL</b> — signals are mixed "
                     f"({long_pts} long vs {short_pts} short); better to wait.")

    return {
        "symbol": symbol,
        "interval": interval,
        "price": price,
        "ema": ema_now,
        "pct_vs_ema": round(pct_vs_ema, 2),
        "ema_1d": ema_1d, "ema_1w": ema_1w,
        "dist_ema_1d": round((price - ema_1d) / ema_1d * 100, 2) if ema_1d else None,
        "dist_ema_1w": round((price - ema_1w) / ema_1w * 100, 2) if ema_1w else None,
        "res_4h": kres["res_4h"], "res_1d": kres["res_1d"], "res_1w": kres["res_1w"],
        "target_ladder": target_ladder,
        "trend": trend,
        "above_ema": above,
        "bias": bias,
        "direction": direction,
        "side": side,
        "dir_long_pts": long_pts,
        "dir_short_pts": short_pts,
        "bias_reason": bias_reason,
        "dir_reason": dir_reason,
        "struct_reason": struct_reason,
        "rsi": r14,
        "rsi_div": rsi_div,
        "squeeze_pct": squeeze_pct,
        "atr_pct": atr_pct,
        "open_interest": oi_data,
        "derivatives": deriv,
        "liq_zones": estimate_liq_zones(price),
        "btc_corr": (round(btc_corr, 2) if btc_corr is not None else None),
        "supertrend": st_val,
        "supertrend_dir": st_dir,
        "supertrend_role": st_role,
        "data_stale": data_stale,
        "patterns": patterns,
        "tf_bias": tf_bias,
        "stop_levels": stop_levels,
        "range_pos": range_pos,
        "structure": ms["structure"],
        "choch": ms["choch"],
        "vol_trend": vp["vol_trend"],
        "vol_ratio": vp["vol_ratio"],
        "rvol": rv,
        "pressure": vp["pressure"],
        "resistances": res[:4],
        "support_bounce": bool(okB),
        "support_bounce_score": dB.get("score") if okB else None,
        "support_bounce_tf": dB.get("tf") if okB else None,
        "support_bounce_support": dB.get("support") if okB else None,
        "support_bounce_touches": dB.get("touches") if okB else None,
        "support_bounce_method": dB.get("method") if okB else None,
        "sup_4h": ksup["sup_4h"], "sup_1d": ksup["sup_1d"], "sup_1w": ksup["sup_1w"],
        "tf_levels": tf_levels,
        "entry": entry,
        "optimal_entry": optimal_entry,
        "retest_entry": _active_retest,
        "auto_side": default_side,
        "plans": {"long": plan_long, "short": plan_short},
        "supports": sup,
        "ema_reclaim": bool(okE),
        "ema_reclaim_score": dE.get("score") if okE else None,
        "bull_flag": bool(okF),
        "bull_flag_score": dF.get("score") if okF else None,
        "notes": notes,
        **bundle,
    }


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
                   help="spot market: also scan coins NOT on futures")
    p.add_argument("--market", default="futures", choices=["futures", "spot"],
                   help="which MEXC market to scan (default: futures/perps)")
    args = p.parse_args()

    cfg = {
        "kline_limit": args.kline_limit, "lookback": args.lookback,
        "retest_tol": args.retest_tol, "break_tol": args.break_tol,
        "max_above_now": args.max_above, "min_slope": args.min_slope,
        "pole_min_gain": 0.15, "flag_max_retrace": 0.5, "cpr_max_width_pct": 0.75,
        "market": args.market,
    }

    sess = get_session()
    futures_only = not args.include_spot_only
    print(f"Fetching {args.quote} {args.market} symbols from MEXC ...",
          file=sys.stderr)
    try:
        symbols = list_symbols(sess, args.quote, futures_only=futures_only,
                               market=args.market)
    except requests.RequestException as e:
        sys.exit(f"Could not reach MEXC: {e}")
    print(f"Scanning {len(symbols)} pairs on the {args.interval} chart "
          f"(200 EMA cross & retest) ...", file=sys.stderr)

    hits: list[dict] = []
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

    hits.sort(key=lambda h: h["score"], reverse=True)
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
            print(f"{h['symbol']:<14}{h['price']:>14.8g}{h['ema']:>14.8g}"
                  f"{h['pct_above_ema']:>8.2f}{h['bars_since_cross']:>6}"
                  f"{h['retest_gap_pct']:>9.2f}{h['score']:>7.1f}")
        print(f"\n{len(hits)} setup(s).  BARS = candles since the reclaim; "
              f"RETEST% = how close the pullback came to the EMA.")

    if args.csv:
        with open(args.csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(hits[0].keys())
                               if hits else ["symbol"])
            w.writeheader()
            w.writerows(hits)
        print(f"Wrote {len(hits)} rows to {args.csv}", file=sys.stderr)


if __name__ == "__main__":
    main()
