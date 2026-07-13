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
    request per symbol powers every scan. Returns an 8-tuple of dicts (each with
    'symbol') or None: (ema, flag, cpr, bounce, wedge, short, st_bounce, early)."""
    raw = fetch_candles(sess, symbol, interval, cfg["kline_limit"],
                        cfg.get("market", "spot"))
    if not raw or len(raw) < EMA_PERIOD + 2:
        return (None,) * 8
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

    return (ema_hit, flag_hit, cpr_hit, bounce_hit, wedge_hit, short_hit,
            st_bounce_hit, early_hit)


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
        return {"error": f"Not enough 4h data for '{symbol}'. Check it's a coin "
                         f"listed on MEXC {mkt} (e.g. BTC, SOL, ETHUSDT)."}
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
    patterns = {"1h": _tf_patterns("1h"),
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
    tf_bias = {"1h": _bias_of("1h"),
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
        rng = hh - ll if hh > ll else 0.0
        if side == "short":                          # targets go DOWN
            cand = [(lvl, "support (prior swing low)") for lvl in sup]
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
        target_ladder = [{"level": lvl, "kind": kind,
                          "pct": round((lvl - entry) / entry * 100, 1),
                          "rr": round((lvl - entry) / (entry - sl_tight), 2)
                                 if entry != sl_tight else None}
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
        "atr_pct": atr_pct,
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
