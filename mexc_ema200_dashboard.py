#!/usr/bin/env python3
"""
MEXC 200-EMA "cross & retest" LIVE DASHBOARD (4h chart)
=======================================================

Runs the scanner on a continuous loop and serves an auto-refreshing web
dashboard on your machine. Start it once and leave it running:

    pip install requests
    python3 mexc_ema200_dashboard.py

Then open  http://localhost:8000  in your browser. The page auto-updates; the
server keeps re-scanning MEXC in the background forever (default every 15 min,
which lines up nicely with the 4h close cadence).

Options:
    python3 mexc_ema200_dashboard.py --port 8000 --scan-every 15
    python3 mexc_ema200_dashboard.py --interval 4h --quote USDT
    # detection knobs (same meaning as the CLI scanner):
    python3 mexc_ema200_dashboard.py --lookback 30 --retest-tol 0.02 \
        --break-tol 0.005 --max-above 0.08 --min-slope 0.0

This file reuses the detection logic in mexc_ema200_scanner.py, so keep both
files in the same folder.

Screener only — not financial advice. Always confirm on the chart.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

try:
    import requests
except ImportError:
    sys.exit("This script needs 'requests'.  Install it with:  pip install requests")

# Reuse the tested scanner core.
try:
    from mexc_ema200_scanner import (
        list_symbols, scan_symbol_multi, get_session, Hit, FlagHit, EMA_PERIOD,
        analyze_symbol, normalize_symbol,
    )
except ImportError:
    sys.exit("Could not import mexc_ema200_scanner.py — keep both files in the "
             "same folder.")


# ----------------------------------------------------------------------------
# Shared state (updated by the scan loop, read by the HTTP handler)
# ----------------------------------------------------------------------------
class State:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.lock = threading.Lock()
        self.hits: list[dict] = []
        self.prev_symbols: set[str] | None = None  # symbols from the previous scan
        self.new_symbols: list[str] = []           # setups new in the latest scan
        self.flag_hits: list[dict] = []            # bull-flag setups
        self.prev_flag_symbols: set[str] | None = None
        self.new_flag_symbols: list[str] = []
        self.both_symbols: list[str] = []          # appear on BOTH scans (confluence)
        self.last_scan: float | None = None      # epoch seconds
        self.next_scan: float | None = None
        self.scanning: bool = False
        self.progress: tuple[int, int] = (0, 0)  # (done, total)
        self.universe: int = 0
        self.error: str = ""

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "hits": list(self.hits),
                "last_scan": self.last_scan,
                "next_scan": self.next_scan,
                "scanning": self.scanning,
                "progress": self.progress,
                "universe": self.universe,
                "new_symbols": list(self.new_symbols),
                "flag_hits": list(self.flag_hits),
                "flag_new_symbols": list(self.new_flag_symbols),
                "both_symbols": list(self.both_symbols),
                "error": self.error,
                "cfg": {
                    "interval": self.cfg["interval"],
                    "quote": self.cfg["quote"],
                    "scan_every": self.cfg["scan_every"],
                    "ema_period": EMA_PERIOD,
                    "futures_only": self.cfg.get("futures_only", True),
                },
            }


def run_one_scan(state: State) -> None:
    cfg = state.cfg
    sess = get_session()
    with state.lock:
        state.scanning = True
        state.error = ""
    try:
        symbols = list_symbols(sess, cfg["quote"],
                               futures_only=cfg.get("futures_only", True))
        with state.lock:
            state.universe = len(symbols)
            state.progress = (0, len(symbols))
    except requests.RequestException as e:
        with state.lock:
            state.error = f"symbol fetch failed: {e}"
            state.scanning = False
        return

    hits: list[Hit] = []
    flags: list[FlagHit] = []
    done = 0
    scan_cfg = {k: cfg[k] for k in
                ("kline_limit", "lookback", "retest_tol", "break_tol",
                 "max_above_now", "min_slope", "pole_min_gain", "flag_max_retrace")}
    with ThreadPoolExecutor(max_workers=cfg["workers"]) as ex:
        futs = {ex.submit(scan_symbol_multi, sess, s, cfg["interval"], scan_cfg): s
                for s in symbols}
        for fut in as_completed(futs):
            done += 1
            if done % 25 == 0:
                with state.lock:
                    state.progress = (done, len(symbols))
            try:
                h, f = fut.result()
            except Exception:
                h, f = None, None
            if h:
                hits.append(h)
            if f:
                flags.append(f)

    hits.sort(key=lambda h: h.score, reverse=True)
    flags.sort(key=lambda h: h.score, reverse=True)

    def tag_new(items, prev):
        """Return (rows, new_syms, cur_syms) with an is_new flag per row."""
        cur = {it.symbol for it in items}
        new = set() if prev is None else cur - prev
        rows = []
        for it in items:
            d = asdict(it)
            d["is_new"] = it.symbol in new
            rows.append(d)
        return rows, [it.symbol for it in items if it.symbol in new], cur

    # Confluence: coins that show up on BOTH scans (reclaim + bull flag).
    both = {h.symbol for h in hits} & {h.symbol for h in flags}

    with state.lock:
        rows, new_syms, cur = tag_new(hits, state.prev_symbols)
        frows, fnew, fcur = tag_new(flags, state.prev_flag_symbols)
        for d in rows + frows:
            d["both"] = d["symbol"] in both
        state.hits = rows
        state.new_symbols = new_syms
        state.prev_symbols = cur
        state.flag_hits = frows
        state.new_flag_symbols = fnew
        state.prev_flag_symbols = fcur
        state.both_symbols = sorted(both)

        state.last_scan = time.time()
        state.progress = (done, len(symbols))
        state.scanning = False


def scan_loop(state: State) -> None:
    every = state.cfg["scan_every"] * 60
    while True:
        run_one_scan(state)
        with state.lock:
            state.next_scan = time.time() + every
        time.sleep(every)


# ----------------------------------------------------------------------------
# HTTP handler
# ----------------------------------------------------------------------------
PAGE = """<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>MEXC 200-EMA cross &amp; retest</title>
<style>
  :root{--bg:#0b0e14;--panel:#141924;--line:#232a38;--txt:#e6edf3;--dim:#8b98ad;
        --accent:#3fb950;--warn:#d29922;--head:#1b2130;}
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--txt);
       font:14px/1.45 -apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif}
  header{padding:16px 22px;border-bottom:1px solid var(--line);
         display:flex;align-items:baseline;gap:14px;flex-wrap:wrap}
  h1{font-size:16px;margin:0;font-weight:650}
  .sub{color:var(--dim);font-size:12.5px}
  .status{padding:10px 22px;border-bottom:1px solid var(--line);color:var(--dim);
          display:flex;gap:22px;flex-wrap:wrap;font-size:12.5px}
  .dot{display:inline-block;width:8px;height:8px;border-radius:50%;
       background:var(--dim);margin-right:6px;vertical-align:middle}
  .dot.live{background:var(--accent);box-shadow:0 0 0 0 rgba(63,185,80,.6);
            animation:pulse 1.6s infinite}
  @keyframes pulse{0%{box-shadow:0 0 0 0 rgba(63,185,80,.5)}
                   70%{box-shadow:0 0 0 7px rgba(63,185,80,0)}
                   100%{box-shadow:0 0 0 0 rgba(63,185,80,0)}}
  .wrap{padding:14px 22px 40px}
  table{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums}
  th,td{padding:8px 12px;text-align:right;border-bottom:1px solid var(--line);
        white-space:nowrap}
  th{position:sticky;top:0;background:var(--head);color:var(--dim);
     font-weight:600;cursor:pointer;user-select:none;font-size:12px}
  th:first-child,td:first-child{text-align:left}
  tbody tr:hover{background:#0f1420}
  td.sym a{color:var(--txt);text-decoration:none;font-weight:600}
  td.sym a:hover{color:var(--accent)}
  .score{color:var(--accent);font-weight:650}
  .empty{color:var(--dim);padding:40px 0;text-align:center}
  .pill{background:var(--panel);border:1px solid var(--line);border-radius:20px;
        padding:2px 10px;color:var(--dim)}
  /* new-coins alert banner */
  .banner{display:none;margin:12px 22px 0;padding:11px 16px;border-radius:10px;
          background:rgba(63,185,80,.10);border:1px solid rgba(63,185,80,.45);
          color:var(--txt);font-size:13px;line-height:1.5}
  .banner.show{display:block}
  .banner b{color:var(--accent)}
  .banner .chip{display:inline-block;background:rgba(63,185,80,.16);
          border:1px solid rgba(63,185,80,.5);border-radius:6px;padding:1px 7px;
          margin:2px 4px 2px 0;font-weight:600}
  .banner .chip a{color:var(--accent);text-decoration:none}
  /* tabs */
  .tabs{display:flex;gap:4px;padding:10px 22px 0}
  .tab{padding:7px 16px;border:1px solid var(--line);border-bottom:none;
       border-radius:8px 8px 0 0;background:var(--panel);color:var(--dim);
       cursor:pointer;font-size:13px;font-weight:600}
  .tab.active{background:var(--head);color:var(--txt)}
  .view{display:none}
  .view.active{display:block}
  /* NEW badge */
  .newbadge{display:inline-block;background:var(--accent);color:#04140a;
       font-size:10px;font-weight:700;border-radius:5px;padding:1px 5px;margin-left:7px;
       vertical-align:middle;letter-spacing:.03em}
  tr.isnew td{background:rgba(63,185,80,.06)}
  /* confluence: appears on both scans */
  .bothbadge{display:inline-block;background:#f0b429;color:#231a00;
       font-size:10px;font-weight:800;border-radius:5px;padding:1px 6px;margin-left:7px;
       vertical-align:middle;letter-spacing:.03em}
  tr.both td{background:rgba(240,180,41,.10)}
  tr.both td:first-child{box-shadow:inset 3px 0 0 #f0b429}
  /* info panel */
  .info{max-width:900px;color:var(--txt);font-size:14px;line-height:1.65}
  .info h2{font-size:15px;margin:22px 0 8px;color:var(--txt)}
  .info h2:first-child{margin-top:6px}
  .info p{color:#c3ccd8;margin:8px 0}
  .info code{background:var(--panel);border:1px solid var(--line);border-radius:4px;
       padding:1px 5px;font-size:12.5px;color:var(--txt)}
  .info table.def{border-collapse:collapse;margin:6px 0 12px;width:100%}
  .info table.def td{border-bottom:1px solid var(--line);padding:7px 10px;
       text-align:left;vertical-align:top;font-size:13px}
  .info table.def td:first-child{color:var(--accent);font-weight:600;
       white-space:nowrap;width:170px}
  .info .warn{color:var(--warn)}
  /* analyze tab */
  .azbar{display:flex;gap:8px;max-width:520px;margin:4px 0 8px}
  .azbar input{flex:1;background:var(--panel);border:1px solid var(--line);
       border-radius:8px;color:var(--txt);padding:10px 12px;font-size:14px;outline:none}
  .azbar input:focus{border-color:var(--accent)}
  .azbar button{background:var(--accent);color:#04140a;border:none;border-radius:8px;
       padding:0 18px;font-weight:700;font-size:14px;cursor:pointer}
  .azbar button:disabled{opacity:.5;cursor:default}
  .azhint{color:var(--dim);font-size:12.5px;max-width:760px}
  .azresult{max-width:820px}
  .azcard{background:var(--panel);border:1px solid var(--line);border-radius:12px;
       padding:16px 18px;margin:6px 0 14px}
  .azhead{display:flex;align-items:baseline;gap:12px;flex-wrap:wrap;margin-bottom:4px}
  .azhead .sym{font-size:18px;font-weight:700}
  .azhead a{color:var(--accent);text-decoration:none;font-size:12.5px}
  .biaspill{border-radius:20px;padding:2px 12px;font-weight:700;font-size:12px}
  .bias-bullish{background:rgba(63,185,80,.15);color:var(--accent);border:1px solid rgba(63,185,80,.5)}
  .bias-bearish{background:rgba(248,81,73,.15);color:#f85149;border:1px solid rgba(248,81,73,.5)}
  .bias-neutral{background:rgba(139,152,173,.15);color:var(--dim);border:1px solid var(--line)}
  .azgrid{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));
       gap:10px;margin:12px 0}
  .azcell{background:var(--bg);border:1px solid var(--line);border-radius:8px;padding:9px 11px}
  .azcell .k{color:var(--dim);font-size:11px;text-transform:uppercase;letter-spacing:.04em}
  .azcell .v{font-size:15px;font-weight:650;font-variant-numeric:tabular-nums;margin-top:2px}
  .azcell .rr{color:var(--dim);font-size:11.5px;font-weight:500}
  .aznotes{margin:10px 0 2px;padding-left:18px}
  .aznotes li{margin:4px 0;color:#c3ccd8}
  .aztags{display:flex;gap:8px;flex-wrap:wrap;margin-top:8px}
  .aztag{font-size:11.5px;border-radius:6px;padding:2px 8px;border:1px solid var(--line);color:var(--dim)}
  .aztag.on{background:rgba(63,185,80,.14);color:var(--accent);border-color:rgba(63,185,80,.5);font-weight:600}
  .azerr{color:var(--warn);padding:10px 0}
</style></head>
<body>
<header>
  <h1>MEXC · 200-EMA cross &amp; retest</h1>
  <span class="sub" id="meta"></span>
</header>
<div class="banner" id="banner"></div>
<div class="tabs">
  <div class="tab active" id="tabSetups" onclick="showTab('setups')">200-EMA reclaim</div>
  <div class="tab" id="tabFlags" onclick="showTab('flags')">Bull flags</div>
  <div class="tab" id="tabAnalyze" onclick="showTab('analyze')">Analyze a coin</div>
  <div class="tab" id="tabInfo" onclick="showTab('info')">Info</div>
</div>

<div class="view active" id="viewSetups">
<div class="status">
  <span><span class="dot" id="dot"></span><span id="scanState">starting…</span></span>
  <span id="lastScan">last scan: —</span>
  <span id="nextScan">next scan: —</span>
  <span id="count"></span>
  <span id="err" style="color:var(--warn)"></span>
</div>
<div class="wrap">
  <table id="tbl">
    <thead><tr>
      <th data-k="symbol">Symbol</th>
      <th data-k="price">Price</th>
      <th data-k="ema">EMA200</th>
      <th data-k="pct_above_ema">% &gt; EMA</th>
      <th data-k="bars_since_cross">Bars since cross</th>
      <th data-k="retest_gap_pct">Retest %</th>
      <th data-k="sl">Stop (SL)</th>
      <th data-k="tp1">TP1</th>
      <th data-k="rr1">R:R 1</th>
      <th data-k="tp2">TP2</th>
      <th data-k="rr2">R:R 2</th>
      <th data-k="tp3">TP3</th>
      <th data-k="rr3">R:R 3</th>
      <th data-k="score">Score</th>
    </tr></thead>
    <tbody id="rows"></tbody>
  </table>
  <div class="empty" id="empty" style="display:none">No cross-and-retest setups right now. The loop keeps scanning…</div>
</div>
</div>

<div class="view" id="viewFlags">
<div class="status">
  <span>Bull flags — impulse + tight pullback, ranked by strength &amp; volume dry-up</span>
  <span id="flagCount"></span>
</div>
<div class="wrap">
  <table id="ftbl">
    <thead><tr>
      <th data-fk="symbol">Symbol</th>
      <th data-fk="price">Price</th>
      <th data-fk="pole_gain_pct">Pole %</th>
      <th data-fk="flag_bars">Flag bars</th>
      <th data-fk="pullback_pct">Pullback %</th>
      <th data-fk="vol_contraction">Vol ratio</th>
      <th data-fk="breakout">Breakout</th>
      <th data-fk="sl">Stop (SL)</th>
      <th data-fk="tp1">TP1</th>
      <th data-fk="rr1">R:R 1</th>
      <th data-fk="tp2">TP2</th>
      <th data-fk="rr2">R:R 2</th>
      <th data-fk="tp3">TP3</th>
      <th data-fk="rr3">R:R 3</th>
      <th data-fk="score">Score</th>
    </tr></thead>
    <tbody id="frows"></tbody>
  </table>
  <div class="empty" id="fempty" style="display:none">No bull flags right now. The loop keeps scanning…</div>
</div>
</div>

<div class="view" id="viewAnalyze">
<div class="wrap">
  <div class="azbar">
    <input id="azInput" type="text" placeholder="Enter a ticker, e.g. BTC or SOLUSDT"
           autocomplete="off" spellcheck="false" onkeydown="if(event.key==='Enter')analyze()">
    <button id="azBtn" onclick="analyze()">Analyze</button>
  </div>
  <div id="azResult" class="azresult"></div>
  <p class="azhint">4h chart read from live MEXC data: trend vs the 200 EMA, support/resistance,
     a suggested entry, stop, and three targets with R:R. A technical estimate to speed up
     your own analysis — not financial advice.</p>
</div>
</div>

<div class="view" id="viewInfo">
<div class="wrap"><div class="info">
  <h2>What this scanner looks for</h2>
  <p>It watches <b>crypto</b> pairs on MEXC quoted in USDT that are also listed on
  MEXC <b>futures</b> (USDT perpetuals), and flags one specific bullish pattern on
  the <b>4-hour</b> chart: a <b>200-EMA cross &amp; retest</b>. Leveraged tokens,
  stablecoin pairs, tokenized stocks/ETFs (Tesla, Apple, iShares, etc.), and any
  spot coin without a futures contract are all filtered out.</p>
  <p>The 200-period exponential moving average is a widely watched trend line.
  The pattern has three parts, all confirmed on <i>closed</i> candles:</p>
  <p><b>1. Reclaim</b> — price was trading below the 200 EMA, then a candle closes
  back above it (a fresh cross up).<br>
  <b>2. Retest</b> — price pulls back down and a candle's low <i>tags</i> the EMA
  (comes within the retest tolerance) while still closing above it, i.e. the EMA
  held as support.<br>
  <b>3. Hold / confirm</b> — price never closes decisively back below the EMA
  after the reclaim, the latest candle is above the EMA but not overextended, and
  the EMA is sloping up.</p>
  <p>The idea: a reclaim that gets bought on the retest is a cleaner, more
  reliable trend change than a price that just spikes across the average.</p>

  <h2>The columns</h2>
  <table class="def">
    <tr><td>Symbol</td><td>The MEXC pair. Click it to open the chart on TradingView.</td></tr>
    <tr><td>Price</td><td>Last closed 4h price.</td></tr>
    <tr><td>EMA200</td><td>The 200-period EMA value on the 4h chart.</td></tr>
    <tr><td>% &gt; EMA</td><td>How far the current price sits above the EMA. Smaller = closer to the line = a tighter entry.</td></tr>
    <tr><td>Bars since cross</td><td>How many 4h candles ago the reclaim happened. Fewer = fresher.</td></tr>
    <tr><td>Retest %</td><td>How close the pullback's low came to the EMA. Near 0 = a clean tag; a small negative means the wick dipped just below the line before closing back above.</td></tr>
    <tr><td>Stop (SL)</td><td>Suggested stop-loss — just below the setup's support (the lower of the EMA and the retest low). A close below here would invalidate the reclaim.</td></tr>
    <tr><td>TP1 / TP2 / TP3</td><td>Three take-profit targets at successive overhead resistances — the nearest prior swing highs above price, in order. Scale out as each is reached. "—" means no more resistance above (blue sky).</td></tr>
    <tr><td>R:R 1 / 2 / 3</td><td>Reward-to-risk to each target = (TP − entry) ÷ (entry − stop), i.e. potential profit ÷ potential loss, with <b>entry = current price</b>. Shown per target so you can see the payoff of holding for TP1 vs TP2 vs TP3. Nearer targets have lower R:R; further ones higher.</td></tr>
    <tr><td>★ BOTH</td><td>Confluence flag — this coin also appears on the other scan (see below). Rows are highlighted gold.</td></tr>
    <tr><td>Score</td><td>Overall quality, 0–100 (see below).</td></tr>
  </table>

  <h2>How the score works (0–100)</h2>
  <p>The score is a weighted blend of three things, each measured 0 to 1 and then
  scaled to 100. Higher means a cleaner, fresher, better-positioned setup:</p>
  <table class="def">
    <tr><td>Retest tightness — 45%</td><td>How close the pullback low tagged the EMA. A low sitting right on the line scores 1; one that only came within the full tolerance (default 2%) scores 0.</td></tr>
    <tr><td>Freshness — 35%</td><td>How recent the reclaim is. A cross on the latest candle scores near 1; one near the edge of the lookback window (default 30 bars) scores near 0.</td></tr>
    <tr><td>Proximity — 20%</td><td>How close current price still is to the EMA. Right at the line scores 1; already extended (near the max-above cap, default 8%) scores 0.</td></tr>
  </table>
  <p>So a high score = price reclaimed the EMA recently, pulled back and kissed it
  precisely, and is still hugging the line rather than having run away. It ranks
  setups; it does not predict outcomes.</p>

  <h2>Stop, target &amp; R:R</h2>
  <p>For each setup the scanner sketches a <b>potential</b> trade, purely from
  chart structure:</p>
  <p><b>Entry</b> is the current price. <b>Stop (SL)</b> sits just below the
  support that would invalidate the reclaim — the lower of the 200 EMA and the
  <i>retest</i> low, with a small buffer (deliberately not the pre-cross low,
  which sat below the EMA and would inflate the risk). <b>TP1/TP2/TP3</b> are the
  next three meaningful overhead resistances, and <b>R:R 1/2/3</b> is the
  reward-to-risk to each — potential profit ÷ potential loss from the current
  price. Reclaim setups often have low R:R to the nearest target because you're
  buying close to support with resistance overhead; the further targets show the
  fuller payoff.</p>
  <p>These are mechanical estimates to save you eyeballing the chart, not
  recommendations. A high R:R only means the geometry is favourable, not that the
  trade will work — size and manage risk yourself.</p>

  <h2>The "just triggered" banner</h2>
  <p>Each scan is compared with the one before it. Any pair that shows up now but
  wasn't in the previous scan is a <b>brand-new setup</b> — it gets called out in
  the green banner at the top and tagged <span class="newbadge">NEW</span> in the
  table, so you can spot fresh signals at a glance.</p>

  <h2>The Bull flags tab</h2>
  <p>A second, independent scan of the same crypto universe looks for
  <b>bull flags</b>: a sharp impulsive rally (the <b>flagpole</b>) followed by a
  shallow, orderly pullback or sideways drift (the <b>flag</b>) — ideally on
  fading volume — that hasn't broken down or fully broken out yet. It's a
  continuation pattern: the market pauses to catch its breath before (often)
  pushing higher.</p>
  <table class="def">
    <tr><td>Pole %</td><td>Size of the flagpole — how far price ran up into the flag. Bigger impulse = stronger setup.</td></tr>
    <tr><td>Flag bars</td><td>How many candles the consolidation has lasted.</td></tr>
    <tr><td>Pullback %</td><td>How deeply the flag retraced the pole. Shallower (well under 50%) is healthier.</td></tr>
    <tr><td>Vol ratio</td><td>Average flag volume ÷ average pole volume. Below 1 means volume is drying up during the pause — the classic, constructive flag signature.</td></tr>
    <tr><td>Breakout</td><td>The trigger level — the top of the flag. A move above it confirms the pattern.</td></tr>
    <tr><td>Stop / TPs / R:R</td><td>Stop sits just below the flag low. TP1 is the classic <b>measured move</b> (breakout + 1.0× the pole's height); TP2 and TP3 are the 1.618× and 2.0× pole extensions. R:R 1/2/3 is reward ÷ risk to each target, entry = current price.</td></tr>
    <tr><td>Score</td><td>0–100, weighting pole strength (35%), pullback tightness (25%), volume contraction (25%), and how coiled near the breakout price is (15%).</td></tr>
  </table>
  <p>The two tabs are complementary: the 200-EMA tab catches trend <i>reclaims</i>,
  the bull-flag tab catches <i>continuations</i> mid-trend.</p>

  <h2>Confluence — coins on both scans</h2>
  <p>When a coin shows up on <b>both</b> the 200-EMA reclaim and the bull-flag scan
  at the same time, it's flagged with a gold <span class="bothbadge">★ BOTH</span>
  badge and its row is highlighted gold on both tabs. The header also shows a
  running "★ N on both" count. These are the highest-conviction names — a trend
  reclaim and a continuation pattern lining up on the same chart — so they're
  worth looking at first.</p>

  <h2>The Analyze a coin tab</h2>
  <p>Type any MEXC ticker (e.g. <code>BTC</code>, <code>SOL</code>,
  <code>SOLUSDT</code>) and it pulls that coin's live 4h candles on demand and
  returns a read: its <b>bias</b> (trend vs the 200 EMA), whether it currently
  matches the reclaim or bull-flag setups, a suggested <b>entry</b> (now) and a
  lower-risk <b>pullback entry</b> at the nearest support, a <b>stop</b> below
  support, and three resistance <b>targets</b> each with their R:R. It's the same
  math the scanners use, pointed at one coin of your choosing.</p>

  <h2>How often it updates</h2>
  <p>The server re-scans all pairs on a loop (the cadence is shown in the header)
  and the page refreshes itself every few seconds — no need to reload.</p>

  <h2 class="warn">Important</h2>
  <p class="warn">This is a screener to speed up chart work, not financial advice
  and not a signal to trade. It can produce false positives, especially on thin,
  low-liquidity coins. Always confirm the setup on the chart and manage your own
  risk before acting.</p>
</div></div>
</div>
<script>
let sortKey="score", sortDir=-1, latest=[];
let fSortKey="score", fSortDir=-1, flatest=[];
let activeTab="setups", lastData=null;
function fmtNum(n){ if(n===null||n===undefined) return "—";
  const a=Math.abs(n); if(a!==0&&a<0.001) return n.toExponential(2);
  return (+n).toLocaleString(undefined,{maximumSignificantDigits:8}); }
function ago(ts){ if(!ts) return "—"; const s=Math.max(0,Math.floor(Date.now()/1000-ts));
  if(s<60) return s+"s ago"; const m=Math.floor(s/60); if(m<60) return m+"m "+(s%60)+"s ago";
  return Math.floor(m/60)+"h "+(m%60)+"m ago"; }
function until(ts){ if(!ts) return "—"; const s=Math.floor(ts-Date.now()/1000);
  if(s<=0) return "due"; const m=Math.floor(s/60); return m>0? m+"m "+(s%60)+"s" : s+"s"; }
function tvLink(sym){ return "https://www.tradingview.com/chart/?symbol=MEXC:"+sym; }
function badges(h){
  let s="";
  if(h.is_new) s+='<span class="newbadge">NEW</span>';
  if(h.both) s+='<span class="bothbadge" title="Appears on BOTH scans — confluence">★ BOTH</span>';
  return s;
}
function rowClass(h){ return (h.is_new?"isnew ":"")+(h.both?"both":""); }
function tpCell(v){ return `<td>${v==null?'—':fmtNum(v)}</td>`; }
function rrCell(v){ return `<td>${v==null?'—':(+v).toFixed(2)}</td>`; }
function showTab(which){
  activeTab=which;
  for(const [t,v] of [["setups","Setups"],["flags","Flags"],["analyze","Analyze"],["info","Info"]]){
    document.getElementById("tab"+v).classList.toggle("active", t===which);
    document.getElementById("view"+v).classList.toggle("active", t===which);
  }
  renderBanner();  // banner follows the active scan tab
}
async function analyze(){
  const inp=document.getElementById("azInput");
  const btn=document.getElementById("azBtn");
  const box=document.getElementById("azResult");
  const sym=inp.value.trim();
  if(!sym){ box.innerHTML='<div class="azerr">Enter a ticker, e.g. BTC.</div>'; return; }
  btn.disabled=true; const t=btn.textContent; btn.textContent="…";
  box.innerHTML='<div class="azhint">Fetching 4h candles from MEXC and analyzing…</div>';
  try{
    const r=await fetch("/analyze?symbol="+encodeURIComponent(sym),{cache:"no-store"});
    const d=await r.json();
    box.innerHTML = d.error ? '<div class="azerr">'+d.error+'</div>' : azCard(d);
  }catch(e){ box.innerHTML='<div class="azerr">Analysis failed — try again.</div>'; }
  btn.disabled=false; btn.textContent=t;
}
function azCard(d){
  const cell=(k,v)=>`<div class="azcell"><div class="k">${k}</div><div class="v">${v}</div></div>`;
  const tp=(v,rr)=> v==null?'—':`${fmtNum(v)}${rr!=null?` <span class="rr">R:R ${(+rr).toFixed(2)}</span>`:''}`;
  const notes=(d.notes||[]).map(n=>`<li>${n}</li>`).join("");
  return `<div class="azcard">
    <div class="azhead">
      <span class="sym">${d.symbol}</span>
      <span class="biaspill bias-${d.bias}">${d.bias.toUpperCase()}</span>
      <span>${fmtNum(d.price)}</span>
      <span style="color:var(--dim)">EMA200 ${fmtNum(d.ema)} · ${d.pct_vs_ema>=0?'+':''}${d.pct_vs_ema}% · trend ${d.trend}</span>
      <a href="${tvLink(d.symbol)}" target="_blank" rel="noopener">open chart ↗</a>
    </div>
    <div class="aztags">
      <span class="aztag ${d.ema_reclaim?'on':''}">200-EMA reclaim${d.ema_reclaim?' · '+d.ema_reclaim_score:''}</span>
      <span class="aztag ${d.bull_flag?'on':''}">Bull flag${d.bull_flag?' · '+d.bull_flag_score:''}</span>
    </div>
    <div class="azgrid">
      ${cell("Entry (now)", fmtNum(d.entry))}
      ${cell("Pullback entry", fmtNum(d.pullback_entry))}
      ${cell("Stop (SL)", fmtNum(d.sl))}
      ${cell("TP1", tp(d.tp1,d.rr1))}
      ${cell("TP2", tp(d.tp2,d.rr2))}
      ${cell("TP3", tp(d.tp3,d.rr3))}
    </div>
    <ul class="aznotes">${notes}</ul>
  </div>`;
}
function renderBanner(){
  const b=document.getElementById("banner");
  let newSyms=[], what="";
  if(activeTab==="setups"){ newSyms=(lastData&&lastData.new_symbols)||[]; what="200-EMA reclaim"; }
  else if(activeTab==="flags"){ newSyms=(lastData&&lastData.flag_new_symbols)||[]; what="bull flag"; }
  if(!newSyms.length){ b.classList.remove("show"); b.innerHTML=""; return; }
  const chips=newSyms.map(s=>`<span class="chip"><a href="${tvLink(s)}" target="_blank" rel="noopener">${s}</a></span>`).join("");
  const label=newSyms.length===1?`new ${what} just triggered`:`new ${what} setups just triggered`;
  b.innerHTML=`<b>🆕 ${newSyms.length} ${label}</b> since the last scan: ${chips}`;
  b.classList.add("show");
}
function renderFlags(){
  const rows=[...flatest].sort((a,b)=>{
    const x=a[fSortKey],y=b[fSortKey];
    if(typeof x==="string") return fSortDir*x.localeCompare(y);
    return fSortDir*((x??0)-(y??0));
  });
  const tb=document.getElementById("frows"); tb.innerHTML="";
  document.getElementById("fempty").style.display = rows.length? "none":"block";
  for(const h of rows){
    const tr=document.createElement("tr");
    tr.className=rowClass(h);
    tr.innerHTML =
      `<td class="sym"><a href="${tvLink(h.symbol)}" target="_blank" rel="noopener">${h.symbol}</a>${badges(h)}</td>`+
      `<td>${fmtNum(h.price)}</td>`+
      `<td>${(+h.pole_gain_pct).toFixed(1)}</td>`+
      `<td>${h.flag_bars}</td>`+
      `<td>${(+h.pullback_pct).toFixed(1)}</td>`+
      `<td>${(+h.vol_contraction).toFixed(2)}</td>`+
      `<td>${fmtNum(h.breakout)}</td>`+
      `<td>${fmtNum(h.sl)}</td>`+
      tpCell(h.tp1)+rrCell(h.rr1)+tpCell(h.tp2)+rrCell(h.rr2)+tpCell(h.tp3)+rrCell(h.rr3)+
      `<td class="score">${(+h.score).toFixed(1)}</td>`;
    tb.appendChild(tr);
  }
}
function render(){
  const rows=[...latest].sort((a,b)=>{
    const x=a[sortKey],y=b[sortKey];
    if(typeof x==="string") return sortDir*x.localeCompare(y);
    return sortDir*((x??0)-(y??0));
  });
  const tb=document.getElementById("rows"); tb.innerHTML="";
  document.getElementById("empty").style.display = rows.length? "none":"block";
  for(const h of rows){
    const tr=document.createElement("tr");
    tr.className=rowClass(h);
    tr.innerHTML =
      `<td class="sym"><a href="${tvLink(h.symbol)}" target="_blank" rel="noopener">${h.symbol}</a>${badges(h)}</td>`+
      `<td>${fmtNum(h.price)}</td>`+
      `<td>${fmtNum(h.ema)}</td>`+
      `<td>${(+h.pct_above_ema).toFixed(2)}</td>`+
      `<td>${h.bars_since_cross}</td>`+
      `<td>${(+h.retest_gap_pct).toFixed(2)}</td>`+
      `<td>${fmtNum(h.sl)}</td>`+
      tpCell(h.tp1)+rrCell(h.rr1)+tpCell(h.tp2)+rrCell(h.rr2)+tpCell(h.tp3)+rrCell(h.rr3)+
      `<td class="score">${(+h.score).toFixed(1)}</td>`;
    tb.appendChild(tr);
  }
}
document.querySelectorAll("th[data-k]").forEach(th=>th.addEventListener("click",()=>{
  const k=th.dataset.k; if(k===sortKey) sortDir*=-1; else {sortKey=k; sortDir=(k==="symbol")?1:-1;}
  render();
}));
document.querySelectorAll("th[data-fk]").forEach(th=>th.addEventListener("click",()=>{
  const k=th.dataset.fk; if(k===fSortKey) fSortDir*=-1; else {fSortKey=k; fSortDir=(k==="symbol")?1:-1;}
  renderFlags();
}));
async function poll(){
  try{
    const r=await fetch("/data",{cache:"no-store"}); const d=await r.json();
    lastData=d;
    latest=d.hits||[]; render();
    flatest=d.flag_hits||[]; renderFlags();
    renderBanner();
    const nboth=(d.both_symbols||[]).length;
    const bothTxt=nboth?` · ★ ${nboth} on both`:"";
    document.getElementById("flagCount").textContent = `${flatest.length} flag(s) · ${d.universe} pairs${bothTxt}`;
    document.getElementById("meta").textContent =
      `${d.cfg.interval} chart · ${d.cfg.quote} ${d.cfg.futures_only?'· futures-listed':'spot'} · EMA${d.cfg.ema_period} · rescans every ${d.cfg.scan_every}m`;
    const dot=document.getElementById("dot");
    dot.className = "dot" + (d.scanning? " live":"");
    document.getElementById("scanState").textContent =
      d.scanning ? `scanning… ${d.progress[0]}/${d.progress[1]}` : "idle";
    document.getElementById("lastScan").textContent = "last scan: "+ago(d.last_scan);
    document.getElementById("nextScan").textContent =
      d.scanning ? "next scan: —" : "next scan: "+until(d.next_scan);
    document.getElementById("count").textContent =
      `${latest.length} setup(s) · ${d.universe} pairs${bothTxt}`;
    document.getElementById("err").textContent = d.error||"";
  }catch(e){ document.getElementById("err").textContent="dashboard offline?"; }
}
poll(); setInterval(poll, 3000);
</script>
</body></html>"""


def make_handler(state: State):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):  # quiet
            pass

        def _send(self, code, body: bytes, ctype: str):
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path.startswith("/data"):
                body = json.dumps(state.snapshot()).encode()
                self._send(200, body, "application/json")
            elif self.path.startswith("/analyze"):
                self._analyze()
            elif self.path in ("/", "/index.html"):
                self._send(200, PAGE.encode(), "text/html; charset=utf-8")
            else:
                self._send(404, b"not found", "text/plain")

        def _analyze(self):
            from urllib.parse import urlparse, parse_qs
            q = parse_qs(urlparse(self.path).query)
            raw = (q.get("symbol") or [""])[0]
            sym = normalize_symbol(raw, state.cfg.get("quote", "USDT"))
            if not sym:
                self._send(200, json.dumps({"error": "Enter a ticker, e.g. BTC."}).encode(),
                           "application/json")
                return
            try:
                sess = get_session()
                out = analyze_symbol(sess, sym, state.cfg["interval"], state.cfg)
            except Exception as e:
                out = {"error": f"Analysis failed: {e}"}
            self._send(200, json.dumps(out).encode(), "application/json")

    return Handler


# ----------------------------------------------------------------------------
def main() -> None:
    p = argparse.ArgumentParser(description="MEXC 4h 200-EMA cross & retest live dashboard")
    # Hosts like Render/Railway/Fly inject the port via the PORT env var.
    p.add_argument("--port", type=int, default=int(os.environ.get("PORT", 8000)))
    p.add_argument("--scan-every", type=int,
                   default=int(os.environ.get("SCAN_EVERY", 15)),
                   help="minutes between scans")
    p.add_argument("--quote", default="USDT")
    p.add_argument("--interval", default="4h")
    p.add_argument("--workers", type=int, default=10)
    p.add_argument("--kline-limit", type=int, default=1000)
    p.add_argument("--lookback", type=int, default=30)
    p.add_argument("--retest-tol", type=float, default=0.020)
    p.add_argument("--break-tol", type=float, default=0.005)
    p.add_argument("--max-above", type=float, default=0.08)
    p.add_argument("--min-slope", type=float, default=0.0)
    p.add_argument("--pole-min-gain", type=float, default=0.15,
                   help="bull flag: min flagpole rise (0.15 = 15%%)")
    p.add_argument("--flag-max-retrace", type=float, default=0.5,
                   help="bull flag: max pullback of the pole (0.5 = 50%%)")
    p.add_argument("--include-spot-only", action="store_true",
                   help="also scan coins NOT listed on MEXC futures "
                        "(default: futures-listed coins only)")
    args = p.parse_args()

    cfg = {
        "port": args.port, "scan_every": args.scan_every, "quote": args.quote,
        "interval": args.interval, "workers": args.workers,
        "kline_limit": args.kline_limit, "lookback": args.lookback,
        "retest_tol": args.retest_tol, "break_tol": args.break_tol,
        "max_above_now": args.max_above, "min_slope": args.min_slope,
        "pole_min_gain": args.pole_min_gain, "flag_max_retrace": args.flag_max_retrace,
        "futures_only": not args.include_spot_only,
    }
    state = State(cfg)

    t = threading.Thread(target=scan_loop, args=(state,), daemon=True)
    t.start()

    srv = ThreadingHTTPServer(("0.0.0.0", args.port), make_handler(state))
    url = f"http://localhost:{args.port}"
    print(f"\n  MEXC 200-EMA cross & retest dashboard")
    print(f"  scanning {cfg['quote']} spot on the {cfg['interval']} chart, "
          f"every {cfg['scan_every']} min")
    print(f"  open  {url}   (Ctrl+C to stop)\n")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\n  stopped.")


if __name__ == "__main__":
    main()
