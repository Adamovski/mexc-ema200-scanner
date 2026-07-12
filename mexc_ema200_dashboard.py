#!/usr/bin/env python3
# build: per-tab filters + robust live prices (retry/fallback) — auto-deploy trigger
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
        list_symbols, scan_symbol_multi, get_session, EMA_PERIOD,
        analyze_symbol, normalize_symbol, fetch_candles, pct_returns, CORR_WINDOW,
    )
except ImportError:
    sys.exit("Could not import mexc_ema200_scanner.py — keep both files in the "
             "same folder.")


# ----------------------------------------------------------------------------
# Telegram push alerts (optional — set TELEGRAM_TOKEN + TELEGRAM_CHAT_ID)
# ----------------------------------------------------------------------------
def send_telegram(cfg: dict, text: str) -> None:
    tok = cfg.get("telegram_token")
    chat = cfg.get("telegram_chat")
    if not tok or not chat:
        return
    try:
        requests.get(f"https://api.telegram.org/bot{tok}/sendMessage",
                     params={"chat_id": chat, "text": text, "parse_mode": "HTML",
                             "disable_web_page_preview": "true"}, timeout=15)
    except Exception:
        pass


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
        self.flag_by_symbol: dict[str, dict] = {}  # latest forming flag per symbol
        self.broken_flags: dict[str, dict] = {}    # flags that have broken out (kept a while)
        self.cpr_hits: list[dict] = []             # narrow-CPR setups
        self.prev_cpr_symbols: set[str] | None = None
        self.new_cpr_symbols: list[str] = []
        self.bounce_hits: list[dict] = []          # support-bounce setups
        self.prev_bounce_symbols: set[str] | None = None
        self.new_bounce_symbols: list[str] = []
        self.wedge_hits: list[dict] = []           # falling-wedge setups (multi-TF)
        self.prev_wedge_symbols: set[str] | None = None
        self.new_wedge_symbols: list[str] = []
        self.short_hits: list[dict] = []           # bearish breakdown/retest (shorts)
        self.prev_short_symbols: set[str] | None = None
        self.new_short_symbols: list[str] = []
        self.both_symbols: list[str] = []          # appear on 2+ scans (confluence)
        self.prev_both: set[str] | None = None     # confluence set from previous scan
        self.watch: dict[str, float] = {}          # symbol -> flag breakout level (armed)
        self.watch_fired: set[str] = set()         # already-alerted breakouts
        self.breakout_events: list[dict] = []      # recent triggered breakouts
        self.live_prices: dict[str, float] = {}    # symbol -> live last price (refreshed ~20s)
        self.last_scan: float | None = None      # epoch seconds
        self.next_scan: float | None = None
        self.scanning: bool = False
        self.progress: tuple[int, int] = (0, 0)  # (done, total)
        self.universe: int = 0
        self.error: str = ""

    def snapshot(self) -> dict:
        with self.lock:
            lp = self.live_prices

            def withlive(rows):
                """Copy each row and attach the current LIVE price so the tables
                show up-to-the-second prices between 15-min rescans."""
                out = []
                for r in rows:
                    d = dict(r)
                    p = lp.get(d.get("symbol"))
                    d["live"] = p if p else d.get("price")
                    out.append(d)
                return out

            return {
                "hits": withlive(self.hits),
                "last_scan": self.last_scan,
                "next_scan": self.next_scan,
                "scanning": self.scanning,
                "progress": self.progress,
                "universe": self.universe,
                "new_symbols": list(self.new_symbols),
                "flag_hits": withlive(self.flag_hits),
                "flag_new_symbols": list(self.new_flag_symbols),
                "cpr_hits": withlive(self.cpr_hits),
                "cpr_new_symbols": list(self.new_cpr_symbols),
                "bounce_hits": withlive(self.bounce_hits),
                "bounce_new_symbols": list(self.new_bounce_symbols),
                "wedge_hits": withlive(self.wedge_hits),
                "wedge_new_symbols": list(self.new_wedge_symbols),
                "short_hits": withlive(self.short_hits),
                "short_new_symbols": list(self.new_short_symbols),
                "both_symbols": list(self.both_symbols),
                "breakout_events": list(self.breakout_events),
                "error": self.error,
                "cfg": {
                    "interval": self.cfg["interval"],
                    "quote": self.cfg["quote"],
                    "scan_every": self.cfg["scan_every"],
                    "ema_period": EMA_PERIOD,
                    "futures_only": self.cfg.get("futures_only", True),
                    "market": self.cfg.get("market", "futures"),
                    "telegram": bool(self.cfg.get("telegram_token") and self.cfg.get("telegram_chat")),
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
                               futures_only=cfg.get("futures_only", True),
                               market=cfg.get("market", "futures"))
        with state.lock:
            state.universe = len(symbols)
            state.progress = (0, len(symbols))
    except requests.RequestException as e:
        with state.lock:
            state.error = f"symbol fetch failed: {e}"
            state.scanning = False
        return

    hits: list[dict] = []
    flags: list[dict] = []
    cprs: list[dict] = []
    bounces: list[dict] = []
    wedges: list[dict] = []
    shorts: list[dict] = []
    done = 0
    scan_cfg = {k: cfg[k] for k in
                ("kline_limit", "lookback", "retest_tol", "break_tol",
                 "max_above_now", "min_slope", "pole_min_gain", "flag_max_retrace",
                 "cpr_max_width_pct")}
    scan_cfg["market"] = cfg.get("market", "futures")
    # BTC's recent returns — computed ONCE, shared by every worker so each coin's
    # correlation to BTC can be measured (to flag coins that just follow BTC).
    try:
        _braw = fetch_candles(sess, "BTCUSDT", cfg["interval"], cfg["kline_limit"],
                              scan_cfg["market"])
        if _braw and len(_braw) > 2:
            _bcl = [float(x[4]) for x in _braw[:-1]]
            scan_cfg["btc_returns"] = pct_returns(_bcl)[-CORR_WINDOW:]
    except Exception:
        pass
    with ThreadPoolExecutor(max_workers=cfg["workers"]) as ex:
        futs = {ex.submit(scan_symbol_multi, sess, s, cfg["interval"], scan_cfg): s
                for s in symbols}
        for fut in as_completed(futs):
            done += 1
            if done % 25 == 0:
                with state.lock:
                    state.progress = (done, len(symbols))
            try:
                h, f, c, b, w, sh = fut.result()
            except Exception:
                h, f, c, b, w, sh = None, None, None, None, None, None
            if h:
                hits.append(h)
            if f:
                flags.append(f)
            if c:
                cprs.append(c)
            if b:
                bounces.append(b)
            if w:
                wedges.append(w)
            if sh:
                shorts.append(sh)

    # Persist recently broken-out flags on the list (tagged) so the Bull-flags tab
    # doesn't "reset" the moment a flag triggers. The forming flags are recorded
    # for the breakout watcher to promote to 'broken out' when they fire.
    now = time.time()
    with state.lock:
        state.flag_by_symbol = {f["symbol"]: f for f in flags}
        broken = {s: v for s, v in state.broken_flags.items()
                  if now - v.get("broke_time", 0) < 12 * 3600}
        state.broken_flags = broken
    cur_flag_syms = {f["symbol"] for f in flags}
    for f in flags:
        f["broken_out"] = False
    for s, v in broken.items():
        if s not in cur_flag_syms:
            flags.append(dict(v))

    hits.sort(key=lambda h: h["score"], reverse=True)
    flags.sort(key=lambda h: h["score"], reverse=True)
    cprs.sort(key=lambda h: h["score"], reverse=True)
    bounces.sort(key=lambda h: h["score"], reverse=True)
    wedges.sort(key=lambda h: h["score"], reverse=True)
    shorts.sort(key=lambda h: h["score"], reverse=True)

    def tag_new(items, prev):
        """Return (rows, new_syms, cur_syms) with an is_new flag per row."""
        cur = {it["symbol"] for it in items}
        new = set() if prev is None else cur - prev
        rows = [dict(it) for it in items]
        for d in rows:
            d["is_new"] = d["symbol"] in new
        return rows, [s for s in (it["symbol"] for it in items) if s in new], cur

    # Confluence: coins that appear on 2 or more of the three scans.
    scan_members = {
        "200-EMA reclaim": {h["symbol"] for h in hits},
        "Bull flag": {h["symbol"] for h in flags},
        "Narrow CPR": {h["symbol"] for h in cprs},
        "Support bounce": {h["symbol"] for h in bounces},
        "Falling wedge": {h["symbol"] for h in wedges},
    }
    conf: dict[str, list[str]] = {}          # symbol -> list of scan labels it's in
    for label, members in scan_members.items():
        for s in members:
            conf.setdefault(s, []).append(label)
    both = {s for s, labels in conf.items() if len(labels) >= 2}

    with state.lock:
        rows, new_syms, cur = tag_new(hits, state.prev_symbols)
        frows, fnew, fcur = tag_new(flags, state.prev_flag_symbols)
        crows, cnew, ccur = tag_new(cprs, state.prev_cpr_symbols)
        brows, bnew, bcur = tag_new(bounces, state.prev_bounce_symbols)
        wrows, wnew, wcur = tag_new(wedges, state.prev_wedge_symbols)
        srows, snew, scur = tag_new(shorts, state.prev_short_symbols)
        for d in rows + frows + crows + brows + wrows + srows:
            labels = conf.get(d["symbol"], [])
            d["both"] = len(labels) >= 2
            d["both_count"] = len(labels)
            d["both_in"] = labels
        state.hits, state.new_symbols, state.prev_symbols = rows, new_syms, cur
        state.flag_hits, state.new_flag_symbols, state.prev_flag_symbols = frows, fnew, fcur
        state.cpr_hits, state.new_cpr_symbols, state.prev_cpr_symbols = crows, cnew, ccur
        state.bounce_hits, state.new_bounce_symbols, state.prev_bounce_symbols = brows, bnew, bcur
        state.wedge_hits, state.new_wedge_symbols, state.prev_wedge_symbols = wrows, wnew, wcur
        state.short_hits, state.new_short_symbols, state.prev_short_symbols = srows, snew, scur
        state.both_symbols = sorted(both)

        # Arm breakout alerts for the current bull flags (their breakout level).
        new_watch = {f["symbol"]: f["breakout"] for f in flags if f.get("breakout")}
        state.watch_fired = {s for s in state.watch_fired if s in new_watch}
        state.watch = new_watch

        # newly-formed confluence coins (for Telegram) — skip the first scan
        prev_both = state.prev_both
        new_both = set() if prev_both is None else (both - prev_both)
        state.prev_both = both

        state.last_scan = time.time()
        state.progress = (done, len(symbols))
        state.scanning = False

    for s in sorted(new_both):
        labels = conf.get(s, [])
        send_telegram(cfg, f"⭐ <b>{s}</b> confluence — now on {len(labels)} "
                           f"scans: {', '.join(labels)}")


def scan_loop(state: State) -> None:
    every = state.cfg["scan_every"] * 60
    while True:
        run_one_scan(state)
        with state.lock:
            state.next_scan = time.time() + every
        time.sleep(every)


MEXC_PRICE_URL = "https://api.mexc.com/api/v3/ticker/price"
MEXC_FUT_TICKER = "https://contract.mexc.com/api/v1/contract/ticker"


def fetch_all_prices(sess, market: str) -> dict:
    """All last-prices in ONE request, keyed by DISPLAY symbol (BTCUSDT)."""
    if market == "futures":
        r = sess.get(MEXC_FUT_TICKER, timeout=20)
        r.raise_for_status()
        return {d["symbol"].replace("_", ""): float(d["lastPrice"])
                for d in r.json().get("data", [])}
    r = sess.get(MEXC_PRICE_URL, timeout=20)
    r.raise_for_status()
    return {d["symbol"]: float(d["price"]) for d in r.json()}


def breakout_watcher(state: State) -> None:
    """Every ~45s, pull ALL prices in one request and fire an alert the moment a
    watched bull flag trades above its breakout level. Cheap: one call per tick."""
    sess = get_session()
    market = state.cfg.get("market", "futures")
    while True:
        time.sleep(20)
        try:
            prices = fetch_all_prices(sess, market)
        except Exception:
            continue
        # Always publish the fresh price map so every scan table shows live prices.
        with state.lock:
            state.live_prices = prices
            watch = dict(state.watch)
            fired = set(state.watch_fired)
        if not watch:
            continue
        events = []
        for sym, bo in watch.items():
            p = prices.get(sym)
            if p is not None and p >= bo * 1.0005 and sym not in fired:
                events.append({"symbol": sym, "breakout": bo, "price": p,
                               "time": time.time()})
                fired.add(sym)
        if events:
            with state.lock:
                fbs = dict(state.flag_by_symbol)
                bf = dict(state.broken_flags)
            for e in events:
                fd = fbs.get(e["symbol"])
                if fd:
                    bf[e["symbol"]] = {**fd, "broken_out": True,
                                       "broke_time": e["time"], "price": e["price"]}
            with state.lock:
                state.watch_fired = fired
                state.breakout_events = (state.breakout_events + events)[-20:]
                state.broken_flags = bf
            for e in events:
                send_telegram(state.cfg,
                    f"🚀 <b>{e['symbol']}</b> BULL-FLAG BREAKOUT\n"
                    f"crossed {e['breakout']:.6g} — now {e['price']:.6g}")


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
  .alertbtn{margin-left:auto;background:var(--panel);border:1px solid var(--line);
       color:var(--txt);border-radius:8px;padding:7px 14px;font-size:12.5px;
       font-weight:600;cursor:pointer}
  .alertbtn.on{background:rgba(63,185,80,.15);border-color:rgba(63,185,80,.6);color:var(--accent)}
  .bkbanner{display:none;margin:12px 22px 0;padding:13px 18px;border-radius:10px;
       background:rgba(248,81,73,.12);border:1px solid #f85149;color:var(--txt);
       font-size:14px;animation:bkflash 1s ease-in-out infinite}
  .bkbanner.show{display:block}
  .bkbanner b{color:#ff7b72}
  .bkbanner .chip{display:inline-block;background:rgba(248,81,73,.18);
       border:1px solid rgba(248,81,73,.5);border-radius:6px;padding:1px 8px;margin:2px 4px 2px 0;font-weight:600}
  .bkbanner .chip a{color:#ff9e96;text-decoration:none}
  @keyframes bkflash{0%,100%{box-shadow:0 0 0 0 rgba(248,81,73,.5)}
                     50%{box-shadow:0 0 14px 2px rgba(248,81,73,.55)}}
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
  .wrap{padding:14px 22px 40px;overflow-x:auto}
  table{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums}
  td .rr{color:var(--dim);font-size:11px;margin-left:3px}
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
  .filterbar{padding:8px 22px;border-bottom:1px solid var(--line);color:var(--dim);
       font-size:12.5px;display:flex;gap:8px;align-items:center;flex-wrap:wrap}
  .fbtn{padding:3px 12px;border:1px solid var(--line);border-radius:14px;cursor:pointer;color:var(--dim)}
  .fbtn.active{background:var(--accent);color:#04140a;border-color:var(--accent);font-weight:700}
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
  .bothbadge{cursor:help}
  .supbadge{cursor:help;margin-left:6px;font-size:12px;opacity:.85}
  .freshbadge{display:inline-block;background:#2f81f7;color:#fff;font-size:10px;font-weight:700;
       border-radius:5px;padding:1px 5px;margin-left:7px;vertical-align:middle;letter-spacing:.03em;cursor:help}
  .brokebadge{display:inline-block;background:rgba(248,81,73,.2);color:#ff7b72;border:1px solid #f85149;
       font-size:10px;font-weight:700;border-radius:5px;padding:1px 5px;margin-left:7px;vertical-align:middle;cursor:help}
  #tip{position:fixed;z-index:9999;display:none;pointer-events:none;max-width:300px;
       background:#0b0e14;border:1px solid #f0b429;color:var(--txt);padding:7px 11px;
       border-radius:7px;font-size:12px;line-height:1.4;box-shadow:0 6px 20px rgba(0,0,0,.6)}
  /* timeframe pill on the support tab */
  .tfpill{display:inline-block;border-radius:5px;padding:1px 7px;font-size:11px;
       font-weight:700;border:1px solid var(--line);color:var(--dim)}
  .tfpill.tf-weekly{background:rgba(240,180,41,.16);color:#f0b429;border-color:rgba(240,180,41,.5)}
  .tfpill.tf-daily{background:rgba(63,185,80,.14);color:var(--accent);border-color:rgba(63,185,80,.5)}
  .tfpill.tf-4h{background:var(--panel)}
  /* bias pill on the support-bounce tab */
  .biaspill2{display:inline-block;border-radius:5px;padding:1px 7px;font-size:11px;font-weight:700;
       border:1px solid var(--line);color:var(--dim);white-space:nowrap}
  .biaspill2.b-bullishchoch{background:rgba(240,180,41,.16);color:#f0b429;border-color:rgba(240,180,41,.55)}
  .biaspill2.b-bullish{background:rgba(63,185,80,.14);color:var(--accent);border-color:rgba(63,185,80,.5)}
  .biaspill2.b-bearish{background:rgba(248,81,73,.14);color:#f85149;border-color:rgba(248,81,73,.5)}
  .biaspill2.b-range{background:var(--panel)}
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
  .aztfs{display:flex;gap:6px;align-items:center;color:var(--dim);font-size:12.5px;margin:2px 0 10px}
  .tfbtn{padding:3px 12px;border:1px solid var(--line);border-radius:14px;cursor:pointer;color:var(--dim)}
  .tfbtn.active{background:var(--accent);color:#04140a;border-color:var(--accent);font-weight:700}
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
  .dirpill{border-radius:20px;padding:3px 14px;font-weight:800;font-size:13px;cursor:help;letter-spacing:.03em}
  .dir-long{background:rgba(63,185,80,.2);color:var(--accent);border:1px solid var(--accent)}
  .dir-short{background:rgba(248,81,73,.2);color:#f85149;border:1px solid #f85149}
  .dir-neutral{background:rgba(139,152,173,.15);color:var(--dim);border:1px solid var(--line)}
  .azcell[data-tip]{cursor:help}
  .azsec[data-tip]{cursor:help}
  td[data-tip]{cursor:help}
  .azladder{display:flex;flex-wrap:wrap;gap:8px;margin:10px 0 4px}
  .ladchip{background:var(--bg);border:1px solid var(--line);border-radius:8px;
       padding:7px 11px;font-size:13px;font-weight:600;font-variant-numeric:tabular-nums;cursor:help}
  .azsec{font-size:12px;font-weight:700;letter-spacing:.05em;text-transform:uppercase;
       color:var(--accent);margin:18px 0 2px;border-bottom:1px solid var(--line);padding-bottom:5px}
  .azsec .azsub{color:var(--dim);font-weight:500;text-transform:none;letter-spacing:0;font-size:11.5px}
  .azgrid{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));
       gap:10px;margin:10px 0 4px}
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
  .grade{display:inline-block;border-radius:6px;padding:1px 8px;font-weight:800;font-size:12.5px}
  .g-a{background:rgba(63,185,80,.22);color:var(--accent);border:1px solid rgba(63,185,80,.6)}
  .g-b{background:rgba(88,166,255,.16);color:#58a6ff;border:1px solid rgba(88,166,255,.5)}
  .g-c{background:rgba(210,153,34,.16);color:#d29922;border:1px solid rgba(210,153,34,.5)}
  .g-d{background:rgba(139,152,173,.14);color:var(--dim);border:1px solid var(--line)}
  .gscore{color:var(--dim);font-size:11.5px;font-variant-numeric:tabular-nums;margin-left:4px}
  .whycell{color:#c3ccd8;font-size:12.5px;max-width:340px}
  .corrbadge{border-radius:6px;padding:0 6px;font-size:10.5px;font-weight:700;border:1px solid var(--line);margin-left:5px;font-variant-numeric:tabular-nums;cursor:help}
  .corr-hi{background:rgba(210,153,34,.16);color:#d29922;border-color:rgba(210,153,34,.45)}
  .corr-mid{background:rgba(139,152,173,.12);color:var(--dim)}
  .corr-lo{background:rgba(63,185,80,.16);color:var(--accent);border-color:rgba(63,185,80,.45)}
  .phasepill{border-radius:6px;padding:1px 7px;font-size:11px;font-weight:700;border:1px solid var(--line)}
  .phase-broke{background:rgba(63,185,80,.16);color:var(--accent);border-color:rgba(63,185,80,.5)}
  .phase-form{background:rgba(139,152,173,.12);color:var(--dim)}
</style></head>
<body>
<header>
  <h1>MEXC · 200-EMA cross &amp; retest</h1>
  <span class="sub" id="meta"></span>
  <button id="alertBtn" class="alertbtn" onclick="enableAlerts()">🔔 Enable breakout alerts</button>
</header>
<div id="tip"></div>
<div class="bkbanner" id="bkbanner"></div>
<div class="banner" id="banner"></div>
<div class="tabs">
  <div class="tab active" id="tabSetups" onclick="showTab('setups')">200-EMA reclaim</div>
  <div class="tab" id="tabFlags" onclick="showTab('flags')">Bull flags</div>
  <div class="tab" id="tabCpr" onclick="showTab('cpr')">Narrow CPR</div>
  <div class="tab" id="tabBounce" onclick="showTab('bounce')">Support bounce</div>
  <div class="tab" id="tabWedge" onclick="showTab('wedge')">Falling wedge</div>
  <div class="tab" id="tabShorts" onclick="showTab('shorts')">Shorts</div>
  <div class="tab" id="tabTop" onclick="showTab('top')">⭐ Top setups</div>
  <div class="tab" id="tabAnalyze" onclick="showTab('analyze')">Analyze a coin</div>
  <div class="tab" id="tabInfo" onclick="showTab('info')">Info</div>
</div>
<div class="filterbar" id="filterbar"></div>

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
      <th data-k="pct_above_ema">% &gt; EMA</th>
      <th data-k="bars_since_cross">Bars</th>
      <th data-k="bias">Bias</th>
      <th data-k="optimal_entry">Optimal entry</th>
      <th data-k="sl_tight">SL tight</th>
      <th data-k="sl_wide">SL wide</th>
      <th data-k="tp1">TP1</th>
      <th data-k="tp2">TP2</th>
      <th data-k="tp3">TP3</th>
      <th data-k="tp4">TP4</th>
      <th data-k="tp5">TP5</th>
      <th data-k="rvol">RVol</th>
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
      <th data-fk="bias">Bias</th>
      <th data-fk="optimal_entry">Breakout entry</th>
      <th data-fk="sl_tight">SL tight</th>
      <th data-fk="sl_wide">SL wide</th>
      <th data-fk="tp1">TP1</th>
      <th data-fk="tp2">TP2</th>
      <th data-fk="tp3">TP3</th>
      <th data-fk="tp4">TP4</th>
      <th data-fk="tp5">TP5</th>
      <th data-fk="rvol">RVol</th>
      <th data-fk="score">Score</th>
    </tr></thead>
    <tbody id="frows"></tbody>
  </table>
  <div class="empty" id="fempty" style="display:none">No bull flags right now. The loop keeps scanning…</div>
</div>
</div>

<div class="view" id="viewCpr">
<div class="status">
  <span>Narrow CPR — compressed daily pivot range, a coiled-for-breakout signal</span>
  <span id="cprCount"></span>
</div>
<div class="wrap">
  <table id="ctbl">
    <thead><tr>
      <th data-ck="symbol">Symbol</th>
      <th data-ck="price">Price</th>
      <th data-ck="cpr_width_pct">CPR width %</th>
      <th data-ck="position">Pos</th>
      <th data-ck="bias">Bias</th>
      <th data-ck="tc">TC</th>
      <th data-ck="bc">BC</th>
      <th data-ck="optimal_entry">Breakout entry</th>
      <th data-ck="sl_tight">SL tight</th>
      <th data-ck="sl_wide">SL wide</th>
      <th data-ck="tp1">TP1</th>
      <th data-ck="tp2">TP2</th>
      <th data-ck="tp3">TP3</th>
      <th data-ck="tp4">TP4</th>
      <th data-ck="tp5">TP5</th>
      <th data-ck="rvol">RVol</th>
      <th data-ck="score">Score</th>
    </tr></thead>
    <tbody id="crows"></tbody>
  </table>
  <div class="empty" id="cempty" style="display:none">No narrow-CPR setups right now. The loop keeps scanning…</div>
</div>
</div>

<div class="view" id="viewBounce">
<div class="status">
  <span>Support bounce — price reversing off a strong, multi-tested support with room above</span>
  <span id="bounceCount"></span>
</div>
<div class="wrap">
  <table id="btbl">
    <thead><tr>
      <th data-bk="symbol">Symbol</th>
      <th data-bk="price">Price</th>
      <th data-bk="support">Support</th>
      <th data-bk="tf">TF</th>
      <th data-bk="touches">Touches</th>
      <th data-bk="dist_to_support_pct">Dist %</th>
      <th data-bk="rsi">RSI</th>
      <th data-bk="bias">Bias</th>
      <th data-bk="optimal_entry">Optimal entry</th>
      <th data-bk="sl_tight">SL tight</th>
      <th data-bk="sl_wide">SL wide</th>
      <th data-bk="tp1">TP1</th>
      <th data-bk="tp2">TP2</th>
      <th data-bk="tp3">TP3</th>
      <th data-bk="tp4">TP4</th>
      <th data-bk="tp5">TP5</th>
      <th data-bk="rvol">RVol</th>
      <th data-bk="score">Score</th>
    </tr></thead>
    <tbody id="brows"></tbody>
  </table>
  <div class="empty" id="bempty" style="display:none">No support-bounce setups right now. The loop keeps scanning…</div>
</div>
</div>

<div class="view" id="viewWedge">
<div class="status">
  <span>Falling wedge — price coiling in a converging downtrend (bullish reversal), on 4h / Daily / Weekly. Fires near the apex or on the breakout.</span>
  <span id="wedgeCount"></span>
</div>
<div class="wrap">
  <table id="wtbl">
    <thead><tr>
      <th data-wk="symbol">Symbol</th>
      <th data-wk="price">Price</th>
      <th data-wk="tf">TF</th>
      <th data-wk="phase">Phase</th>
      <th data-wk="conv_pct">Converge %</th>
      <th data-wk="touches">Touches</th>
      <th data-wk="bias">Bias</th>
      <th data-wk="optimal_entry">Breakout entry</th>
      <th data-wk="sl_tight">SL tight</th>
      <th data-wk="sl_wide">SL wide</th>
      <th data-wk="tp1">TP1</th>
      <th data-wk="tp2">TP2</th>
      <th data-wk="tp3">TP3</th>
      <th data-wk="tp4">TP4</th>
      <th data-wk="tp5">TP5</th>
      <th data-wk="rvol">RVol</th>
      <th data-wk="score">Score</th>
    </tr></thead>
    <tbody id="wrows"></tbody>
  </table>
  <div class="empty" id="wempty" style="display:none">No falling-wedge setups right now. The loop keeps scanning…</div>
</div>
</div>

<div class="view" id="viewShorts">
<div class="status">
  <span>Shorts — only coins worth shorting: a bearish breakdown &amp; retest of a <b>falling</b> 200 EMA (price broke below it and rejected it from underneath). Stops sit ABOVE, targets BELOW. Ranked by a composite <b>conviction rating</b>; hover it for the reasons.</span>
  <span id="shortCount"></span>
</div>
<div class="wrap">
  <table id="stbl">
    <thead><tr>
      <th data-sk="symbol">Symbol</th>
      <th data-tip="Composite short-conviction rating (A+ → D, 0–100): blends the detector score (30%), market-structure alignment to the downside (20%), volume (15%), reward:risk (10%), plus a base for a confirmed breakdown+retest. Higher = a cleaner short.">Rating</th>
      <th data-sk="price">Price</th>
      <th data-sk="pct_below_ema">% &lt; EMA</th>
      <th data-sk="bars_since_cross">Bars</th>
      <th data-sk="bias">Bias</th>
      <th data-tip="BTC correlation ρ over ~10 days. Low/negative = its own move; ≥0.85 = largely just following BTC.">BTC ρ</th>
      <th data-tip="Plain-English reasons this qualifies as a short.">Why</th>
      <th data-sk="optimal_entry">Optimal entry</th>
      <th data-sk="sl_tight">SL tight</th>
      <th data-sk="sl_wide">SL wide</th>
      <th data-sk="tp1">TP1</th>
      <th data-sk="tp2">TP2</th>
      <th data-sk="tp3">TP3</th>
      <th data-sk="tp4">TP4</th>
      <th data-sk="tp5">TP5</th>
      <th data-sk="rvol">RVol</th>
      <th data-sk="score">Score</th>
    </tr></thead>
    <tbody id="srows"></tbody>
  </table>
  <div class="empty" id="sempty" style="display:none">No short setups right now. The loop keeps scanning…</div>
</div>
</div>

<div class="view" id="viewTop">
<div class="status">
  <span>Top setups — only the highest-conviction <b>LONG</b> opportunities. Aggregated across every bullish scan, then <b>filtered for quality</b>: each must clear a real reward:risk bar and earn a solid composite <b>conviction rating</b> (A+ → D). Hover the rating or "Why" for the full reasoning.</span>
  <span id="topCount"></span>
</div>
<div class="wrap">
  <table id="tbtbl">
    <thead><tr>
      <th>Symbol</th>
      <th data-tip="Composite conviction rating (A+ → D, 0–100). Blends: multi-scan confluence (30%), realistic reward:risk (26%), the best detector's own score (18%), volume confirmation via RVol (13%), and market-structure alignment (13%). Only setups clearing a real R:R + rating bar are shown.">Rating</th>
      <th data-tip="Which bullish scans this coin currently appears on. Appearing on 2+ (★) is strong confluence.">Setups</th>
      <th data-tip="Live last-traded price (updates ~every 20s).">Price</th>
      <th data-tip="Market-structure bias from swing highs/lows.">Bias</th>
      <th data-tip="BTC correlation ρ over ~10 days. Low/negative = an independent mover (preferred); ≥0.85 = mostly just BTC beta and gets docked in the rating.">BTC ρ</th>
      <th data-tip="Lower-risk entry — a pullback to the setup's support / EMA / breakout level.">Entry</th>
      <th data-tip="Tight stop-loss for the best setup on this coin.">SL tight</th>
      <th data-tip="The meaningful profit target (≥~2% away) used to judge the reward:risk.">Target</th>
      <th data-tip="Realistic reward:risk to that target (capped at 8:1). This is the quality bar the setup had to clear.">R:R</th>
      <th data-tip="Plain-English summary of what earns this coin its rating.">Why it's a top setup</th>
    </tr></thead>
    <tbody id="tbrows"></tbody>
  </table>
  <div class="empty" id="tbempty" style="display:none">No setups clear the quality bar right now — the loop keeps scanning. (Better to show nothing than a mediocre trade.)</div>
</div>
</div>

<div class="view" id="viewAnalyze">
<div class="wrap">
  <div class="azbar">
    <input id="azInput" type="text" placeholder="Enter a ticker, e.g. BTC or SOLUSDT"
           autocomplete="off" spellcheck="false" onkeydown="if(event.key==='Enter')analyze()">
    <button id="azBtn" onclick="analyze()">Analyze</button>
  </div>
  <div class="aztfs">Timeframe:
    <span class="tfbtn" data-tf="1h" onclick="setAzTf('1h')">1h</span>
    <span class="tfbtn active" data-tf="4h" onclick="setAzTf('4h')">4h</span>
    <span class="tfbtn" data-tf="1d" onclick="setAzTf('1d')">1D</span>
    <span class="tfbtn" data-tf="1w" onclick="setAzTf('1w')">1W</span>
  </div>
  <div id="azResult" class="azresult"></div>
  <p class="azhint">Live 4h read from MEXC: a Long/Short lean, trend vs the 200 EMA (4h/1D/1W),
     market structure &amp; CHoCH, RSI, volume, multi-timeframe support/resistance, and a full
     entry / two-stop / target-ladder plan with R:R. Hover any box for what it means.
     A technical estimate to speed up your own analysis — not financial advice.</p>
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
    <tr><td>Bars</td><td>How many 4h candles ago the reclaim happened. Fewer = fresher.</td></tr>
    <tr><td>Optimal entry</td><td>The lower-risk fill — a pullback that retests the reclaimed EMA — rather than chasing the current price.</td></tr>
    <tr><td>SL tight / SL wide</td><td>Two stop scenarios, both ATR-buffered (scaled to the coin's volatility). <b>Tight</b> sits just under the retest low / EMA; <b>wide</b> sits under the deeper swing low of the whole move — more breathing room, bigger risk.</td></tr>
    <tr><td>TP1 … TP5</td><td>Up to five take-profit targets at successive overhead resistances (nearest prior swing highs above price). Each cell shows the price and, in grey, its <b>R:R</b> to the <i>tight</i> stop. Scale out as each is hit; "—" = no more resistance (blue sky).</td></tr>
    <tr><td>R:R (grey on each TP)</td><td>(TP − entry) ÷ (entry − tight stop) = potential profit ÷ potential loss, entry = current price. Nearer targets = lower R:R, further = higher. Using the wide stop lowers each R:R.</td></tr>
    <tr><td>★ BOTH</td><td>Confluence — this coin also appears on another scan (see below). Rows are highlighted gold.</td></tr>
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

  <h2>Entries, stops, targets &amp; R:R</h2>
  <p>For each setup the scanner sketches a <b>potential</b> trade from chart
  structure. Every tab (and the Analyze tab) shows the same anatomy:</p>
  <p><b>Entry</b> is the current price; <b>optimal entry</b> is a lower-risk fill
  (a pullback to the reclaimed EMA / flag or CPR breakout level / nearest support,
  depending on the tab). There are <b>two stop scenarios</b>, both buffered by
  <b>ATR</b> (the coin's own volatility) rather than a flat %: <b>SL tight</b> sits
  just under the immediate structure, <b>SL wide</b> under a deeper swing low for
  more breathing room. There are up to <b>five targets</b> (TP1–TP5) at successive
  resistances (or, for flags, measured-move and Fibonacci pole extensions so
  proper breakouts have room to run), and each shows its <b>R:R</b> to the tight
  stop — profit ÷ loss from the current price. Nearer targets have lower R:R,
  further ones higher.</p>
  <p>These are mechanical estimates to save you eyeballing the chart, not
  recommendations. A high R:R only means the geometry is favourable, not that the
  trade will work — size and manage risk yourself.</p>

  <h2>Next support 🛟 (4h / Daily / Weekly)</h2>
  <p>Every setup row has a small <b>🛟 buoy</b> next to the symbol — <b>hover it</b>
  to see the next major support below current price on all three timeframes:
  <b>4h</b>, <b>Daily</b> and <b>Weekly</b>. It's your safety-net map: where price
  is likely to find a floor if the trade goes against you. (The Analyze tab shows
  the same three levels as a dedicated field.)</p>

  <h2>RVol — volume confirmation</h2>
  <p>The <b>RVol</b> column is the latest candle's volume divided by its recent
  20-bar average. Above <b>1.0×</b> means the signal is printing on
  above-average volume — <b>confirmation</b> that real participation is behind the
  move (values ≥ 1.5× are highlighted green). A reclaim, CPR break or support
  bounce on high volume is more trustworthy, so those setups get a small score
  boost when RVol is elevated. (Bull flags are the exception — they're healthiest
  when volume <i>dries up</i> during the flag, which the Vol ratio column tracks.)</p>

  <h2>Telegram push alerts 📲</h2>
  <p>Get alerts on your phone 24/7, even with the tab closed. When the header
  shows "📲 Telegram on", the server pushes a message whenever a <b>bull flag
  breaks out</b> or a coin <b>newly becomes confluence</b> (lands on 2+ scans).
  To enable it: message <code>@BotFather</code> on Telegram → <code>/newbot</code>
  → copy the <b>token</b>; then message <code>@userinfobot</code> to get your
  <b>chat ID</b>; and add both as environment variables
  (<code>TELEGRAM_TOKEN</code>, <code>TELEGRAM_CHAT_ID</code>) on the host, then
  redeploy.</p>

  <h2>Bull-flag breakout alerts 🔔</h2>
  <p>Click <b>Enable breakout alerts</b> (top-right) once to turn on sound and
  desktop notifications. Every bull flag currently on the board is "armed" at its
  breakout level, and a background watcher checks live prices every ~45 seconds —
  so the moment a flag actually trades <b>above its breakout trigger</b>, you get
  a loud triple beep, a browser notification, and a flashing red banner naming the
  coin. (Browsers require that one click before they'll allow sound/notifications;
  keep the tab open to receive them.)</p>

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

  <h2>The Narrow CPR tab</h2>
  <p>A third scan built on the <b>Central Pivot Range</b> (CPR), computed on the
  <b>daily</b> frame from the 4h candles using the previous day's High/Low/Close:
  Pivot P = (H+L+C)/3, BC = (H+L)/2, TC = 2P − BC. When the range between TC and
  BC is <b>narrow</b> relative to price, it means the market is coiled and
  compressed — often the calm before a trending / breakout move. The scan flags
  coins whose latest CPR is narrow and price is sitting at or above it (bullish
  side), and shows the CPR width %, where price sits (Pos: above / inside), the TC
  and BC levels, a breakout entry, both stops, and five targets.</p>

  <h2>The Support bounce tab</h2>
  <p>A fourth scan for reversals: coins <b>bouncing off a strong horizontal
  support</b>, looked for across <b>three timeframes</b> — 4-hour, <b>Daily</b>
  and <b>Weekly</b> (the 4h candles are aggregated up). It finds swing-low pivots
  on each, clusters nearby lows into support <b>zones</b> (confirmed on a higher
  timeframe, or tested repeatedly = stronger), then flags where price has just
  tagged the nearest strong zone and is turning back up, with room above.</p>
  <table class="def">
    <tr><td>Support</td><td>The support level price is bouncing from.</td></tr>
    <tr><td>TF</td><td>The strongest timeframe that support sits on — <b>Weekly</b> (gold) &gt; <b>Daily</b> (green) &gt; 4h. Higher-timeframe supports are more significant and score higher.</td></tr>
    <tr><td>Touches</td><td>How many times that level has been tested. More touches = a more significant, better-respected support.</td></tr>
    <tr><td>Dist %</td><td>How far price currently sits above the support. Smaller = a fresher bounce, tighter entry.</td></tr>
    <tr><td>RSI</td><td>Relative Strength Index (0–100). Bouncing from oversold (&lt;30–45) scores higher — more room to rally.</td></tr>
    <tr><td>Score</td><td>0–100, weighting touches (30%), how fresh the bounce is (25%), room to the resistance above (20%), RSI (15%), and whether price is turning up (10%).</td></tr>
  </table>
  <p>Entry/optimal entry (near the support), the two ATR stops (below the zone),
  and the five targets work exactly like the other tabs.</p>

  <h2>Confluence — coins on 2+ scans</h2>
  <p>When a coin shows up on <b>two or more</b> of the four scans (200-EMA
  reclaim, bull flag, narrow CPR, support bounce) at once, it's flagged with a
  gold <span class="bothbadge">★ 2</span> badge showing <b>how many scans</b> it's
  on (★ 2 / ★ 3 / ★ 4) — <b>hover the badge to see exactly which scans</b>. The
  row is highlighted gold on every tab, with a running "★ N confluence" count in
  the header. These are the highest-conviction names — multiple bullish signals
  lining up on the same chart — so they're worth a look first.</p>

  <h2>The Analyze a coin tab</h2>
  <p>Type any MEXC ticker (e.g. <code>BTC</code>, <code>SOL</code>,
  <code>SOLUSDT</code>) and it pulls that coin's live 4h candles on demand and
  returns a full read. As well as the trade plan (entry, optimal entry, both
  stops, five targets with R:R), it reports:</p>
  <table class="def">
    <tr><td>Bias</td><td>Bullish / bearish / neutral, from price vs the 200 EMA and the EMA's slope.</td></tr>
    <tr><td>Structure &amp; CHoCH</td><td>Market structure (uptrend = higher highs &amp; higher lows, downtrend = lower highs &amp; lows, or range) and any <b>Change of Character</b> — the first structural break signalling the trend may be flipping.</td></tr>
    <tr><td>RSI (14)</td><td>Momentum, 0–100. &lt;30 oversold, &gt;70 overbought.</td></tr>
    <tr><td>Volume</td><td>Whether recent volume is rising/steady/falling vs its average, and whether <b>buyers or sellers</b> control recent candles (up-candle vs down-candle volume).</td></tr>
    <tr><td>Range position</td><td>Where price sits in its recent 120-bar range (0% = range low, 100% = high), plus ATR as a % of price (its volatility).</td></tr>
    <tr><td>Supports &amp; resistances</td><td>The nearest levels above and below, and which of the four setups (if any) the coin currently matches.</td></tr>
  </table>
  <p>Everything is spelled out in plain English in the notes under the card.</p>

  <h2>Indicators &amp; terms — glossary</h2>
  <table class="def">
    <tr><td>200 EMA</td><td>200-period exponential moving average — a heavily-watched long-term trend line. Above it = bullish regime, below = bearish.</td></tr>
    <tr><td>ATR</td><td>Average True Range — the coin's typical candle range, i.e. its volatility. Stops are buffered by a fraction of ATR so they fit each coin instead of a flat %.</td></tr>
    <tr><td>RSI</td><td>Relative Strength Index — momentum oscillator, 0–100; flags oversold/overbought.</td></tr>
    <tr><td>Swing high / low (pivot)</td><td>A local peak/trough — a candle higher/lower than the few candles either side. These define support, resistance and structure.</td></tr>
    <tr><td>Support / resistance</td><td>Prior swing lows (floors) / swing highs (ceilings). A level tested repeatedly is stronger.</td></tr>
    <tr><td>Market structure</td><td>The sequence of highs and lows: higher-high/higher-low = uptrend; lower-high/lower-low = downtrend.</td></tr>
    <tr><td>CHoCH</td><td>Change of Character — the first break of that structure (e.g. a downtrend making a higher high), an early reversal cue.</td></tr>
    <tr><td>CPR</td><td>Central Pivot Range — Pivot, BC and TC from the prior day's H/L/C. A narrow CPR = compressed, coiled price.</td></tr>
    <tr><td>Measured move</td><td>Projecting a pattern's own size from its breakout (a bull flag typically runs ~the height of its pole); Fib extensions (1.618×, 2×) give the further targets.</td></tr>
    <tr><td>R:R</td><td>Reward-to-risk = potential profit ÷ potential loss = (target − entry) ÷ (entry − stop), entry = current price.</td></tr>
    <tr><td>Confluence</td><td>The same coin appearing on 2+ scans at once — stacked signals, higher conviction.</td></tr>
  </table>

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
let cSortKey="score", cSortDir=-1, clatest=[];
let bSortKey="score", bSortDir=-1, blatest=[];
let wSortKey="score", wSortDir=-1, wlatest=[];
let sSortKey="score", sSortDir=-1, slatest=[];
let activeTab="setups", lastData=null;
// Per-tab filters so a filter on one tab never hides rows on another.
const FILT={ setups:{bias:"all",fresh:false}, flags:{bias:"all",phase:"all"},
             cpr:{bias:"all"}, bounce:{bias:"all"},
             wedge:{bias:"all",phase:"all"}, shorts:{bias:"all"}, top:{indep:false} };
function biasOk(h,tab){ const b=(FILT[tab]||{}).bias; return !b||b==="all"||h.bias_dir===b; }
function renderFilterBar(){
  const bar=document.getElementById("filterbar"); if(!bar) return;
  const t=activeTab;
  if(!FILT[t]){ bar.style.display="none"; return; }
  bar.style.display="flex";
  const f=FILT[t]; let h="";
  if('bias' in f){ h+="Bias: ";
    for(const [k,l] of [["all","All"],["bullish","Bullish"],["bearish","Bearish"],["neutral","Neutral"]])
      h+=`<span class="fbtn ${f.bias===k?'active':''}" onclick="setF('${t}','bias','${k}')">${l}</span>`;
  }
  if(t==="top"){
    h+=`<span class="fbtn ${f.indep?'active':''}" onclick="setF('top','indep',${!f.indep})" data-tip="Hide coins that just follow BTC — show only setups with BTC correlation ρ below 0.6 (their own movers).">🧭 Independent movers only (ρ&lt;0.6)</span>`;
  }
  if(t==="setups"){
    h+='<span style="color:var(--line)">|</span>';
    h+=`<span class="fbtn ${f.fresh?'active':''}" onclick="setF('setups','fresh',${!f.fresh})" data-tip="Show only reclaims that crossed the 200 EMA within the last ~24h (≤6 4h-candles).">🔵 Fresh reclaims only</span>`;
  }
  if(t==="flags"){
    h+='<span style="color:var(--line)">|</span>';
    for(const [k,l] of [["all","All flags"],["forming","Forming only"],["broken","Broken out only"]])
      h+=`<span class="fbtn ${f.phase===k?'active':''}" onclick="setF('flags','phase','${k}')">${l}</span>`;
  }
  if(t==="wedge"){
    h+='<span style="color:var(--line)">|</span>';
    for(const [k,l] of [["all","All wedges"],["forming","Coiling only"],["broken","Broken out only"]])
      h+=`<span class="fbtn ${f.phase===k?'active':''}" onclick="setF('wedge','phase','${k}')">${l}</span>`;
  }
  bar.innerHTML=h;
}
function setF(tab,key,val){ FILT[tab][key]=val; renderFilterBar();
  ({setups:render,flags:renderFlags,cpr:renderCPR,bounce:renderBounce,
    wedge:renderWedge,shorts:renderShorts,top:renderTop}[tab]||(()=>{}))();
}
let alertsOn=false, audioCtx=null, seenBreak=Math.floor(Date.now()/1000);
function enableAlerts(){
  try{ if(!audioCtx) audioCtx=new (window.AudioContext||window.webkitAudioContext)();
       if(audioCtx.state==="suspended") audioCtx.resume(); }catch(e){}
  if("Notification" in window && Notification.permission==="default") Notification.requestPermission();
  alertsOn=true;
  const b=document.getElementById("alertBtn"); b.textContent="🔔 Breakout alerts ON"; b.classList.add("on");
  beep(1);
}
function beep(times){
  if(!audioCtx) return;
  let t=audioCtx.currentTime;
  for(let i=0;i<times;i++){
    const o=audioCtx.createOscillator(), g=audioCtx.createGain();
    o.type="square"; o.frequency.value=880;
    g.gain.setValueAtTime(0.0001,t); g.gain.exponentialRampToValueAtTime(0.35,t+0.02);
    g.gain.exponentialRampToValueAtTime(0.0001,t+0.28);
    o.connect(g); g.connect(audioCtx.destination); o.start(t); o.stop(t+0.3);
    t+=0.34;
  }
}
function fireBreakout(evs){
  beep(3);
  if("Notification" in window && Notification.permission==="granted"){
    for(const e of evs){ try{ new Notification("🚀 "+e.symbol+" broke out!",
      {body:"Above flag breakout "+(+e.breakout).toPrecision(6)+" — now "+(+e.price).toPrecision(6)}); }catch(x){} }
  }
  const bb=document.getElementById("bkbanner");
  const chips=evs.map(e=>`<span class="chip"><a href="${tvLink(e.symbol)}" target="_blank" rel="noopener">${e.symbol}</a> @ ${fmtNum(e.price)}</span>`).join("");
  bb.innerHTML=`<b>🚀 BULL FLAG BREAKOUT</b> — price crossed the flag trigger on: ${chips}`;
  bb.classList.add("show");
  clearTimeout(window._bkT); window._bkT=setTimeout(()=>bb.classList.remove("show"), 90000);
}
function fmtNum(n){ if(n===null||n===undefined) return "—";
  n=+n; if(!isFinite(n)) return "—";
  const a=Math.abs(n);
  if(a!==0 && a<1){   // small prices: plain decimals, never scientific notation
    const decimals=Math.min(12, Math.max(2, 3 - Math.floor(Math.log10(a))));
    return n.toFixed(decimals).replace(/0+$/,'').replace(/\.$/,'');
  }
  return n.toLocaleString(undefined,{maximumFractionDigits:a>=1000?2:4}); }
function ago(ts){ if(!ts) return "—"; const s=Math.max(0,Math.floor(Date.now()/1000-ts));
  if(s<60) return s+"s ago"; const m=Math.floor(s/60); if(m<60) return m+"m "+(s%60)+"s ago";
  return Math.floor(m/60)+"h "+(m%60)+"m ago"; }
function until(ts){ if(!ts) return "—"; const s=Math.floor(ts-Date.now()/1000);
  if(s<=0) return "due"; const m=Math.floor(s/60); return m>0? m+"m "+(s%60)+"s" : s+"s"; }
function tvLink(sym){ const perp=(lastData&&lastData.cfg&&lastData.cfg.market==="futures")?".P":"";
  return "https://www.tradingview.com/chart/?symbol=MEXC:"+sym+perp; }
function badges(h){
  let s="";
  if(h.is_new) s+='<span class="newbadge">NEW</span>';
  if(h.fresh) s+='<span class="freshbadge" data-tip="Fresh reclaim — price crossed back above the 200 EMA within the last ~24h (≤6 of the 4h candles). Older reclaims still show while price holds above the EMA, just without this badge.">FRESH</span>';
  if(h.broken_out) s+='<span class="brokebadge" data-tip="This bull flag already broke out above its trigger — kept on the list for ~12h so it doesn\\'t vanish the moment it fires.">🚀 BROKEN OUT</span>';
  if(h.both){
    const inList=(h.both_in||[]).join(", ");
    s+=`<span class="bothbadge" data-tip="On ${h.both_count} scans: ${inList}" title="On ${h.both_count} scans: ${inList}">★ ${h.both_count}</span>`;
  }
  const P=h.live!=null?h.live:h.price;
  const dd=v=> (P&&v)? " (-"+(((P-v)/P)*100).toFixed(1)+"%)" : "";
  const sp=[];
  if(h.sup_4h!=null) sp.push("4h "+fmtNum(h.sup_4h)+dd(h.sup_4h));
  if(h.sup_1d!=null) sp.push("Daily "+fmtNum(h.sup_1d)+dd(h.sup_1d));
  if(h.sup_1w!=null) sp.push("Weekly "+fmtNum(h.sup_1w)+dd(h.sup_1w));
  if(sp.length) s+=`<span class="supbadge" data-tip="Next support below (drawdown) — ${sp.join("  ·  ")}">🛟</span>`;
  s+=corrPill(h.btc_corr);
  return s;
}
// BTC-correlation pill: how much this coin just mirrors BTC over the last ~10 days.
function corrPill(c){
  if(c==null) return '';
  const cls = c>=0.85?'corr-hi' : c>=0.55?'corr-mid' : 'corr-lo';
  const lbl = c>=0.85?'moves with BTC (just follows it)'
            : c>=0.55?'partly BTC-linked'
            : (c<0.2?'independent of BTC':'loosely BTC-linked');
  return `<span class="corrbadge ${cls}" data-tip="BTC correlation ρ=${c.toFixed(2)} over ~10 days (60×4h bars) — ${lbl}. High ρ (≥0.85) means the move is really just BTC beta; low or negative ρ means the coin is moving on its own story.">ρ${c.toFixed(2)}</span>`;
}
// instant custom tooltip for the ★ confluence badge (reliable, no native delay)
document.addEventListener("mouseover",e=>{
  const b=e.target.closest("[data-tip]"); if(!b) return;
  const t=document.getElementById("tip"); if(!t) return;
  t.textContent=b.getAttribute("data-tip")||""; t.style.display="block";
});
document.addEventListener("mousemove",e=>{
  const t=document.getElementById("tip");
  if(t&&t.style.display==="block"){ t.style.left=(e.clientX+14)+"px"; t.style.top=(e.clientY+16)+"px"; }
});
document.addEventListener("mouseout",e=>{
  if(e.target.closest&&e.target.closest("[data-tip]")){ const t=document.getElementById("tip"); if(t) t.style.display="none"; }
});
function rowClass(h){ return (h.is_new?"isnew ":"")+(h.both?"both":""); }
function tpCell(v,rr){ if(v==null) return '<td>—</td>';
  const tip=`Take-profit target at ${fmtNum(v)} — an overhead resistance level (on the Bull-flags tab, a measured-move / Fib extension of the pole). Grey number is the reward:risk to the tight stop${rr!=null?` (${(+rr).toFixed(2)})`:''}.`;
  return `<td data-tip="${tip}">${fmtNum(v)}${rr!=null?`<span class="rr">R${(+rr).toFixed(1)}</span>`:''}</td>`; }
function biasPill(h){ return `<td><span class="biaspill2 b-${(h.bias||'').toLowerCase().replace(/[^a-z]/g,'')}">${h.bias||'—'}</span></td>`; }
function rvCell(v){ return v==null?'<td>—</td>'
  :`<td${(+v)>=1.5?' style="color:var(--accent);font-weight:600"':''}>${(+v).toFixed(2)}×</td>`; }
function showTab(which){
  activeTab=which;
  for(const [t,v] of [["setups","Setups"],["flags","Flags"],["cpr","Cpr"],["bounce","Bounce"],["wedge","Wedge"],["shorts","Shorts"],["top","Top"],["analyze","Analyze"],["info","Info"]]){
    document.getElementById("tab"+v).classList.toggle("active", t===which);
    document.getElementById("view"+v).classList.toggle("active", t===which);
  }
  renderBanner();  // banner follows the active scan tab
  renderFilterBar();  // filters are per-tab
}
let azTf="4h";
function setAzTf(tf){ azTf=tf;
  document.querySelectorAll(".tfbtn").forEach(x=>x.classList.toggle("active",x.dataset.tf===tf));
  if(document.getElementById("azInput").value.trim()) analyze();
}
async function analyze(){
  const inp=document.getElementById("azInput");
  const btn=document.getElementById("azBtn");
  const box=document.getElementById("azResult");
  const sym=inp.value.trim();
  if(!sym){ box.innerHTML='<div class="azerr">Enter a ticker, e.g. BTC.</div>'; return; }
  btn.disabled=true; const t=btn.textContent; btn.textContent="…";
  box.innerHTML=`<div class="azhint">Fetching ${azTf} candles from MEXC and analyzing…</div>`;
  try{
    const r=await fetch("/analyze?symbol="+encodeURIComponent(sym)+"&interval="+azTf,{cache:"no-store"});
    const d=await r.json();
    box.innerHTML = d.error ? '<div class="azerr">'+d.error+'</div>' : azCard(d);
  }catch(e){ box.innerHTML='<div class="azerr">Analysis failed — try again.</div>'; }
  btn.disabled=false; btn.textContent=t;
}
function azCard(d){
  const cell=(k,v,tip)=>`<div class="azcell"${tip?` data-tip="${tip}"`:''}><div class="k">${k}</div><div class="v">${v}</div></div>`;
  const pct=(v,sign)=> d.price&&v!=null? ` <span class="rr">${sign}${(Math.abs((v-d.price)/d.price)*100).toFixed(1)}%</span>`:'';
  const tp=(v,rr)=> v==null?'—':`${fmtNum(v)}${rr!=null?` <span class="rr">R:R ${(+rr).toFixed(2)}</span>`:''}`;
  const notes=(d.notes||[]).map(n=>`<li>${n}</li>`).join("");
  return `<div class="azcard">
    <div class="azhead">
      <span class="sym">${d.symbol}</span>
      <span style="color:var(--dim);font-size:12px;border:1px solid var(--line);border-radius:6px;padding:1px 7px">${(d.interval||'4h')} chart</span>
      <span class="dirpill dir-${(d.direction||'neutral').toLowerCase()}" data-tip="${d.dir_reason||''}">${(d.direction||'—').toUpperCase()}${d.direction==='Long'?' ▲':d.direction==='Short'?' ▼':''}</span>
      <span class="biaspill bias-${d.bias}" data-tip="${d.bias_reason||''}">${d.bias.toUpperCase()}</span>
      <span>${fmtNum(d.price)}</span>
      <span style="color:var(--dim)">EMA200 ${fmtNum(d.ema)} · ${d.pct_vs_ema>=0?'+':''}${d.pct_vs_ema}% · trend ${d.trend}</span>
      <a href="${tvLink(d.symbol)}" target="_blank" rel="noopener">open chart ↗</a>
    </div>
    <div class="aztags">
      <span class="aztag ${d.ema_reclaim?'on':''}">200-EMA reclaim${d.ema_reclaim?' · '+d.ema_reclaim_score:''}</span>
      <span class="aztag ${d.bull_flag?'on':''}">Bull flag${d.bull_flag?' · '+d.bull_flag_score:''}</span>
      <span class="aztag ${d.support_bounce?'on':''}">Support bounce${d.support_bounce?` · ${d.support_bounce_tf} support · `+d.support_bounce_score:''}</span>
    </div>
    <div class="azsec">Market read</div>
    <div class="azgrid">
      ${cell("Structure", (d.structure||'—')+(d.choch?` · ${d.choch} CHoCH`:''), d.struct_reason||"Market structure from swing highs/lows. CHoCH = the first break the other way — an early reversal cue that can appear inside a trend.")}
      ${cell("RSI (14)", d.rsi==null?'—':(+d.rsi).toFixed(0)+(d.rsi<30?' oversold':d.rsi>70?' overbought':''), "Relative Strength Index (0-100) — momentum. Below 30 = oversold (bounce potential), above 70 = overbought (pullback risk).")}
      ${cell("Volume", (d.vol_trend||'—')+(d.vol_ratio?` ×${d.vol_ratio}`:''), `Recent volume is ${d.vol_trend||'—'} vs its average. Rising volume in an uptrend = buyers committed (bullish confirmation); rising volume in a downtrend = sellers in control (bearish confirmation). Falling volume during a move usually means momentum is fading — expect consolidation or a possible reversal. Here the trend is ${d.trend||'flat'}.`)}
      ${cell("Pressure", d.pressure||'—', `Buyers vs sellers over recent candles (volume on up-candles vs down-candles). '${d.pressure||'—'}' are in control. Buyers-in-control backs a long; sellers-in-control backs a short; balanced = indecision.`)}
      ${cell("Rel volume", d.rvol==null?'—':(+d.rvol).toFixed(2)+'× latest bar', "The latest candle's volume ÷ its 20-bar average. Above 1× = the current move is happening on above-average participation = stronger confirmation. Below 1× = quiet, less conviction.")}
      ${cell("Range position", d.range_pos==null?'—':d.range_pos+'%', `Where price sits in its recent 120-candle range on the ${d.interval||'4h'} timeframe (0% = range low, 100% = range high).`)}
      ${cell("ATR", d.atr_pct==null?'—':d.atr_pct+'%', "Average True Range as a % of price — the coin's volatility. Stops are buffered by a fraction of this.")}
      ${cell("BTC correlation", d.btc_corr==null?'—':('ρ '+(+d.btc_corr).toFixed(2)+(d.btc_corr>=0.85?' · just follows BTC':d.btc_corr<0.5?' · independent':' · partly linked')), "How closely this coin's 4h returns tracked BTC over the last ~10 days (Pearson ρ, −1 to +1). ρ≥0.85 means the move is largely just BTC beta — a 'breakout' here may only be BTC pulling it up. Low or negative ρ means the coin is trading on its own story, which is usually what you want for an independent setup.")}
      ${cell("Supertrend ("+(d.interval||'4h')+")", d.supertrend==null?'—':(fmtNum(d.supertrend)+' · '+(d.supertrend_role==='support'?'SUPPORT':'RESISTANCE')+pct(d.supertrend, d.supertrend_role==='support'?'-':'+')), `Supertrend (ATR 10×3) on the ${d.interval||'4h'} chart. When price is ABOVE the line the trend is up and the line acts as a trailing SUPPORT; when price is BELOW it the trend is down and it acts as RESISTANCE. Here it's ${d.supertrend_role||'—'} at ${d.supertrend==null?'—':fmtNum(d.supertrend)} — a level to watch for the trend flipping.`)}
      ${cell("Supports (distance)", (d.supports||[]).slice(0,3).map(v=>fmtNum(v)+pct(v,'-')).join(' · ')||'—', "Nearest support levels below price, with how far (%) each sits below the current price.")}
      ${cell("Resistances (distance)", (d.resistances||[]).slice(0,3).map(v=>fmtNum(v)+pct(v,'+')).join(' · ')||'—', "Nearest resistance levels above price, with how far (%) each sits above the current price.")}
      ${cell("Next support 4h·1D·1W (drawdown)", [d.sup_4h,d.sup_1d,d.sup_1w].map(v=>v==null?'—':fmtNum(v)+pct(v,'-')).join(' · '), "The next major support on the 4h, Daily and Weekly charts — your safety-net levels — with the % drawdown to each.")}
      ${cell("Next resistance 4h·1D·1W (upside)", [d.res_4h,d.res_1d,d.res_1w].map(v=>v==null?'—':fmtNum(v)+pct(v,'+')).join(' · '), "The next major resistance on the 4h, Daily and Weekly charts — likely ceilings — with the % upside to each.")}
      ${cell("Dist. from 200 EMA (4h·1D·1W)", `4h ${d.pct_vs_ema>=0?'+':''}${d.pct_vs_ema}% · 1D ${d.dist_ema_1d==null?'—':(d.dist_ema_1d>=0?'+':'')+d.dist_ema_1d+'%'} · 1W ${d.dist_ema_1w==null?'—':(d.dist_ema_1w>=0?'+':'')+d.dist_ema_1w+'%'}`, "How far price sits above/below the 200 EMA on each timeframe. Above on all three = a strong multi-timeframe uptrend regime. '—' = not enough history for that EMA.")}
    </div>
    <div class="azsec">Trade plan <span class="azsub">${(d.side||'long')==='short'?'SHORT — sell resistance, stops ABOVE, targets BELOW':'LONG — buy support, stops BELOW, targets ABOVE'} → 5 targets (with R:R)</span></div>
    <div class="azgrid">
      ${cell("Entry (now)", fmtNum(d.entry), "The current price — your immediate entry.")}
      ${cell("Optimal entry", fmtNum(d.optimal_entry), (d.side||'long')==='short'?"A better short fill: sell into the nearest resistance rather than shorting here.":"A lower-risk long fill: buy a pullback to the nearest support / EMA instead of chasing.")}
      ${cell("SL tight", fmtNum(d.sl_tight), (d.side||'long')==='short'?"Tighter stop just ABOVE the nearest resistance (ATR-buffered) — a break above invalidates the short.":"Tighter stop just BELOW the immediate structure (ATR-buffered).")}
      ${cell("SL wide", fmtNum(d.sl_wide), (d.side||'long')==='short'?"Wider stop above a higher resistance — more room, bigger risk.":"Wider stop below a deeper swing low — more room, bigger risk.")}
      ${cell("TP1", tp(d.tp1,d.rr1), (d.side||'long')==='short'?"First downside target (nearest support) with its R:R to the tight stop.":"First target (nearest resistance) with its R:R to the tight stop.")}
      ${cell("TP2", tp(d.tp2,d.rr2), "Second target with its R:R to the tight stop.")}
      ${cell("TP3", tp(d.tp3,d.rr3), "Third target with its R:R to the tight stop.")}
      ${cell("TP4", tp(d.tp4,d.rr4), "Fourth target with its R:R to the tight stop.")}
      ${cell("TP5", tp(d.tp5,d.rr5), "Fifth target with its R:R to the tight stop.")}
    </div>
    <div class="azsec" data-tip="A fuller ladder of profit targets: overhead resistance levels blended with Fibonacci extensions of the recent range, in order. Each shows % upside and R:R to the tight stop.">Target ladder <span class="azsub">resistances + Fibonacci extensions (hover for more)</span></div>
    <div class="azladder">
      ${(d.target_ladder||[]).map((t,i)=>`<span class="ladchip" data-tip="Target ${i+1}: ${t.kind} at ${fmtNum(t.level)} — ${t.pct>=0?'+':''}${t.pct}% move, R:R ${t.rr!=null?t.rr:'—'} to the tight stop.">T${i+1} ${fmtNum(t.level)} <span class="rr">${t.pct>=0?'+':''}${t.pct}%${t.rr!=null?` · R${t.rr}`:''}</span></span>`).join('') || '<span style="color:var(--dim)">No further targets that side.</span>'}
    </div>
    <div class="azsec">In plain English</div>
    <ul class="aznotes">${notes}</ul>
  </div>`;
}
function renderBanner(){
  const b=document.getElementById("banner");
  let newSyms=[], what="";
  if(activeTab==="setups"){ newSyms=(lastData&&lastData.new_symbols)||[]; what="200-EMA reclaim"; }
  else if(activeTab==="flags"){ newSyms=(lastData&&lastData.flag_new_symbols)||[]; what="bull flag"; }
  else if(activeTab==="cpr"){ newSyms=(lastData&&lastData.cpr_new_symbols)||[]; what="narrow CPR"; }
  else if(activeTab==="bounce"){ newSyms=(lastData&&lastData.bounce_new_symbols)||[]; what="support bounce"; }
  else if(activeTab==="wedge"){ newSyms=(lastData&&lastData.wedge_new_symbols)||[]; what="falling wedge"; }
  else if(activeTab==="shorts"){ newSyms=(lastData&&lastData.short_new_symbols)||[]; what="short setup"; }
  if(!newSyms.length){ b.classList.remove("show"); b.innerHTML=""; return; }
  const chips=newSyms.map(s=>`<span class="chip"><a href="${tvLink(s)}" target="_blank" rel="noopener">${s}</a></span>`).join("");
  const label=newSyms.length===1?`new ${what} just triggered`:`new ${what} setups just triggered`;
  b.innerHTML=`<b>🆕 ${newSyms.length} ${label}</b> since the last scan: ${chips}`;
  b.classList.add("show");
}
function renderFlags(){
  const ph=FILT.flags.phase;
  const rows=[...flatest].filter(h=>biasOk(h,"flags")
      &&(ph==="all"||(ph==="broken"?h.broken_out:!h.broken_out))).sort((a,b)=>{
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
      `<td data-tip="Live last-traded price — refreshes ~every 20s and is independent of the chart timeframe.">${fmtNum(h.live!=null?h.live:h.price)}</td>`+
      `<td>${(+h.pole_gain_pct).toFixed(1)}</td>`+
      `<td>${h.flag_bars}</td>`+
      `<td>${(+h.pullback_pct).toFixed(1)}</td>`+
      `<td>${(+h.vol_contraction).toFixed(2)}</td>`+
      biasPill(h)+
      `<td data-tip="Optimal entry — the lower-risk fill: a pullback to the setup's support / EMA / breakout level rather than chasing the current price.">${fmtNum(h.optimal_entry)}</td>`+
      `<td data-tip="Tight stop — just below the setup's immediate structure (retest low / flag low / support / CPR bottom), buffered by ATR. A close below invalidates the setup.">${fmtNum(h.sl_tight)}</td>`+
      `<td data-tip="Wide stop — below a deeper swing low. More breathing room but larger risk per unit.">${fmtNum(h.sl_wide)}</td>`+
      tpCell(h.tp1,h.rr1)+tpCell(h.tp2,h.rr2)+tpCell(h.tp3,h.rr3)+tpCell(h.tp4,h.rr4)+tpCell(h.tp5,h.rr5)+
      rvCell(h.rvol)+
      `<td class="score">${(+h.score).toFixed(1)}</td>`;
    tb.appendChild(tr);
  }
}
function render(){
  const rows=[...latest].filter(h=>biasOk(h,"setups")&&(!FILT.setups.fresh||h.fresh)).sort((a,b)=>{
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
      `<td data-tip="Live last-traded price — refreshes ~every 20s and is independent of the chart timeframe.">${fmtNum(h.live!=null?h.live:h.price)}</td>`+
      `<td>${(+h.pct_above_ema).toFixed(2)}</td>`+
      `<td>${h.bars_since_cross}</td>`+
      biasPill(h)+
      `<td data-tip="Optimal entry — the lower-risk fill: a pullback to the setup's support / EMA / breakout level rather than chasing the current price.">${fmtNum(h.optimal_entry)}</td>`+
      `<td data-tip="Tight stop — just below the setup's immediate structure (retest low / flag low / support / CPR bottom), buffered by ATR. A close below invalidates the setup.">${fmtNum(h.sl_tight)}</td>`+
      `<td data-tip="Wide stop — below a deeper swing low. More breathing room but larger risk per unit.">${fmtNum(h.sl_wide)}</td>`+
      tpCell(h.tp1,h.rr1)+tpCell(h.tp2,h.rr2)+tpCell(h.tp3,h.rr3)+tpCell(h.tp4,h.rr4)+tpCell(h.tp5,h.rr5)+
      rvCell(h.rvol)+
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
document.querySelectorAll("th[data-ck]").forEach(th=>th.addEventListener("click",()=>{
  const k=th.dataset.ck; if(k===cSortKey) cSortDir*=-1; else {cSortKey=k; cSortDir=(k==="symbol"||k==="position")?1:-1;}
  renderCPR();
}));
document.querySelectorAll("th[data-bk]").forEach(th=>th.addEventListener("click",()=>{
  const k=th.dataset.bk; if(k===bSortKey) bSortDir*=-1; else {bSortKey=k; bSortDir=(k==="symbol")?1:-1;}
  renderBounce();
}));
function renderBounce(){
  const rows=[...blatest].filter(h=>biasOk(h,"bounce")).sort((a,b)=>{
    const x=a[bSortKey],y=b[bSortKey];
    if(typeof x==="string") return bSortDir*x.localeCompare(y);
    return bSortDir*((x??0)-(y??0));
  });
  const tb=document.getElementById("brows"); tb.innerHTML="";
  document.getElementById("bempty").style.display = rows.length? "none":"block";
  for(const h of rows){
    const tr=document.createElement("tr");
    tr.className=rowClass(h);
    tr.innerHTML =
      `<td class="sym"><a href="${tvLink(h.symbol)}" target="_blank" rel="noopener">${h.symbol}</a>${badges(h)}</td>`+
      `<td data-tip="Live last-traded price — refreshes ~every 20s and is independent of the chart timeframe.">${fmtNum(h.live!=null?h.live:h.price)}</td>`+
      `<td>${fmtNum(h.support)}</td>`+
      `<td><span class="tfpill tf-${(h.tf||'').toLowerCase()}">${h.tf||'—'}</span></td>`+
      `<td>${h.touches}</td>`+
      `<td>${(+h.dist_to_support_pct).toFixed(2)}</td>`+
      `<td>${h.rsi==null?'—':(+h.rsi).toFixed(0)}</td>`+
      `<td><span class="biaspill2 b-${(h.bias||'').toLowerCase().replace(/[^a-z]/g,'')}">${h.bias||'—'}</span></td>`+
      `<td data-tip="Optimal entry — the lower-risk fill: a pullback to the setup's support / EMA / breakout level rather than chasing the current price.">${fmtNum(h.optimal_entry)}</td>`+
      `<td data-tip="Tight stop — just below the setup's immediate structure (retest low / flag low / support / CPR bottom), buffered by ATR. A close below invalidates the setup.">${fmtNum(h.sl_tight)}</td>`+
      `<td data-tip="Wide stop — below a deeper swing low. More breathing room but larger risk per unit.">${fmtNum(h.sl_wide)}</td>`+
      tpCell(h.tp1,h.rr1)+tpCell(h.tp2,h.rr2)+tpCell(h.tp3,h.rr3)+tpCell(h.tp4,h.rr4)+tpCell(h.tp5,h.rr5)+
      rvCell(h.rvol)+
      `<td class="score">${(+h.score).toFixed(1)}</td>`;
    tb.appendChild(tr);
  }
}
function renderCPR(){
  const rows=[...clatest].filter(h=>biasOk(h,"cpr")).sort((a,b)=>{
    const x=a[cSortKey],y=b[cSortKey];
    if(typeof x==="string") return cSortDir*x.localeCompare(y);
    return cSortDir*((x??0)-(y??0));
  });
  const tb=document.getElementById("crows"); tb.innerHTML="";
  document.getElementById("cempty").style.display = rows.length? "none":"block";
  for(const h of rows){
    const tr=document.createElement("tr");
    tr.className=rowClass(h);
    tr.innerHTML =
      `<td class="sym"><a href="${tvLink(h.symbol)}" target="_blank" rel="noopener">${h.symbol}</a>${badges(h)}</td>`+
      `<td data-tip="Live last-traded price — refreshes ~every 20s and is independent of the chart timeframe.">${fmtNum(h.live!=null?h.live:h.price)}</td>`+
      `<td>${(+h.cpr_width_pct).toFixed(3)}</td>`+
      `<td>${h.position}</td>`+
      biasPill(h)+
      `<td>${fmtNum(h.tc)}</td>`+
      `<td>${fmtNum(h.bc)}</td>`+
      `<td data-tip="Optimal entry — the lower-risk fill: a pullback to the setup's support / EMA / breakout level rather than chasing the current price.">${fmtNum(h.optimal_entry)}</td>`+
      `<td data-tip="Tight stop — just below the setup's immediate structure (retest low / flag low / support / CPR bottom), buffered by ATR. A close below invalidates the setup.">${fmtNum(h.sl_tight)}</td>`+
      `<td data-tip="Wide stop — below a deeper swing low. More breathing room but larger risk per unit.">${fmtNum(h.sl_wide)}</td>`+
      tpCell(h.tp1,h.rr1)+tpCell(h.tp2,h.rr2)+tpCell(h.tp3,h.rr3)+tpCell(h.tp4,h.rr4)+tpCell(h.tp5,h.rr5)+
      rvCell(h.rvol)+
      `<td class="score">${(+h.score).toFixed(1)}</td>`;
    tb.appendChild(tr);
  }
}
document.querySelectorAll("th[data-wk]").forEach(th=>th.addEventListener("click",()=>{
  const k=th.dataset.wk; if(k===wSortKey) wSortDir*=-1; else {wSortKey=k; wSortDir=(k==="symbol"||k==="tf"||k==="phase")?1:-1;}
  renderWedge();
}));
document.querySelectorAll("th[data-sk]").forEach(th=>th.addEventListener("click",()=>{
  const k=th.dataset.sk; if(k===sSortKey) sSortDir*=-1; else {sSortKey=k; sSortDir=(k==="symbol")?1:-1;}
  renderShorts();
}));
function wedgePhase(h){ return h.broken_out
  ? '<span class="phasepill phase-broke">🚀 Broke out</span>'
  : '<span class="phasepill phase-form">Coiling</span>'; }
function renderWedge(){
  const ph=FILT.wedge.phase;
  const rows=[...wlatest].filter(h=>biasOk(h,"wedge")
      &&(ph==="all"||(ph==="broken"?h.broken_out:!h.broken_out))).sort((a,b)=>{
    const x=a[wSortKey],y=b[wSortKey];
    if(typeof x==="string") return wSortDir*x.localeCompare(y);
    return wSortDir*((x??0)-(y??0));
  });
  const tb=document.getElementById("wrows"); tb.innerHTML="";
  document.getElementById("wempty").style.display = rows.length? "none":"block";
  for(const h of rows){
    const tr=document.createElement("tr");
    tr.className=rowClass(h);
    tr.innerHTML =
      `<td class="sym"><a href="${tvLink(h.symbol)}" target="_blank" rel="noopener">${h.symbol}</a>${badges(h)}</td>`+
      `<td data-tip="Live last-traded price — refreshes ~every 20s and is independent of the chart timeframe.">${fmtNum(h.live!=null?h.live:h.price)}</td>`+
      `<td><span class="tfpill tf-${(h.tf||'').toLowerCase()}">${h.tf||'4h'}</span></td>`+
      `<td>${wedgePhase(h)}</td>`+
      `<td data-tip="How far the two wedge lines have converged toward the apex. Higher = tighter coil, closer to resolution.">${h.conv_pct==null?'—':(+h.conv_pct).toFixed(1)}</td>`+
      `<td>${h.touches==null?'—':h.touches}</td>`+
      `<td><span class="biaspill2 b-${(h.bias||'').toLowerCase().replace(/[^a-z]/g,'')}">${h.bias||'—'}</span></td>`+
      `<td data-tip="Breakout entry — a break/retest of the upper (descending) wedge line rather than chasing.">${fmtNum(h.optimal_entry)}</td>`+
      `<td data-tip="Tight stop — just below the wedge low, ATR-buffered. A close below invalidates the wedge.">${fmtNum(h.sl_tight)}</td>`+
      `<td data-tip="Wide stop — below a deeper swing low.">${fmtNum(h.sl_wide)}</td>`+
      tpCell(h.tp1,h.rr1)+tpCell(h.tp2,h.rr2)+tpCell(h.tp3,h.rr3)+tpCell(h.tp4,h.rr4)+tpCell(h.tp5,h.rr5)+
      rvCell(h.rvol)+
      `<td class="score">${(+h.score).toFixed(1)}</td>`;
    tb.appendChild(tr);
  }
}
// ---- Conviction rating: a composite 0–100 grade with a plain-English "why". ----
const clamp01=v=>Math.max(0,Math.min(1,v));
function gradeOf(s){ return s>=82?'A+':s>=72?'A':s>=62?'B':s>=48?'C':'D'; }
function gradeClass(s){ return s>=72?'g-a':s>=62?'g-b':s>=48?'g-c':'g-d'; }
function esc(t){ return (''+t).replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;'); }
function ratingCell(score,why){
  const g=gradeOf(score);
  const tip=esc(`Rated ${g} (${score}/100). ${why.join(' · ')}`);
  return `<td data-tip="${tip}"><span class="grade ${gradeClass(score)}">${g}</span><span class="gscore">${score}</span></td>`;
}
// A *realistic* reward:risk for a setup: the best R:R among its targets that sit a
// meaningful distance away (>=2% move), capped at 8 so measured-move moonshots on
// the flag/wedge scans can't dominate. Returns {rr, tp, move}.
function realisticRR(h){
  const P=h.live!=null?h.live:h.price;
  let best={rr:0,tp:null,move:0};
  for(let i=1;i<=5;i++){
    const tp=h['tp'+i], rr=h['rr'+i];
    if(tp==null||rr==null||rr<=0||!P) continue;
    const move=Math.abs((tp-P)/P);
    if(move>=0.02){ const c=Math.min(rr,8); if(c>best.rr) best={rr:c,tp:tp,move:move}; }
  }
  if(best.rr===0){ for(let i=1;i<=5;i++){ const tp=h['tp'+i],rr=h['rr'+i];
    if(rr>0){ best={rr:Math.min(rr,8),tp:tp,move:P&&tp?Math.abs((tp-P)/P):0}; break; } } }
  return best;
}
// Per-setup quality used to pick the single best setup for a coin (blends the
// detector's own score with its realistic reward:risk).
function setupQuality(h){ return 0.55*clamp01((h.score||0)/100)+0.45*clamp01(realisticRR(h).rr/3); }
// Independence nudge: reward coins moving on their own, dock ones just tracking BTC.
function indepAdj(corr){ if(corr==null) return 0; if(corr>=0.85) return -5; if(corr<=0.4) return 4; return 0; }
function corrTd(c){ if(c==null) return '<td>—</td>';
  const cls=c>=0.85?'corr-hi':c>=0.55?'corr-mid':'corr-lo';
  return `<td data-tip="BTC correlation ρ over ~10 days. ≥0.85 = largely just follows BTC; low or negative = its own mover."><span class="corrbadge ${cls}">${c.toFixed(2)}</span></td>`; }
function bullConviction(f){
  const conf = f.nScans>=3?1 : f.nScans===2?0.72 : 0.35;               // confluence
  const rr   = clamp01((f.rrq||0)/3);                                  // realistic R:R (3:1 = full)
  const qual = clamp01((f.bestScore||0)/100);                          // detector quality
  const vol  = f.rvol? clamp01((f.rvol-1)/1.5) : 0.3;                  // volume confirmation
  const struct = (f.bias==='Bullish'||f.bias==='Bullish CHoCH')?1 : (f.choch==='bullish'?0.6:0.3);
  return Math.round(100*(0.30*conf+0.26*rr+0.18*qual+0.13*vol+0.13*struct));
}
function bearConviction(f){
  const rr   = clamp01((f.rrq||0)/3);
  const qual = clamp01((f.bestScore||0)/100);
  const vol  = f.rvol? clamp01((f.rvol-1)/1.5) : 0.3;
  const struct = (f.bias==='Bearish')?1 : (f.choch==='bearish'?0.6:0.3);
  const base = 0.6;                                                     // confirmed breakdown+retest
  return Math.round(100*(0.22*base+0.28*rr+0.25*qual+0.15*struct+0.10*vol));
}
function bullWhy(f){
  const w=[];
  w.push(f.nScans>=2 ? `Confluence: on ${f.nScans} scans (${f.setups})`
                     : `Single setup: ${f.setups}`);
  if(f.rrq) w.push(`Realistic R:R ${f.rrq.toFixed(2)}${f.rrMove?` to a +${(f.rrMove*100).toFixed(1)}% target`:''}`);
  w.push(`Detector score ${Math.round(f.bestScore)}/100`);
  if(f.rvol) w.push(`Volume ${f.rvol.toFixed(2)}× ${f.rvol>=1.5?'(strong confirmation)':f.rvol>=1?'(above average)':'(light)'}`);
  w.push(`Structure: ${f.bias||'—'}${f.choch==='bullish'?' + bullish CHoCH':''}`);
  if(f.fresh) w.push('Fresh trigger this scan');
  return w;
}
function bearWhy(f){
  const w=[];
  w.push('Broke below a falling 200 EMA and retested it from below');
  if(f.rrq) w.push(`Realistic R:R ${f.rrq.toFixed(2)}${f.rrMove?` to a −${(f.rrMove*100).toFixed(1)}% target`:''}`);
  w.push(`Detector score ${Math.round(f.bestScore)}/100`);
  w.push(`Structure: ${f.bias||'—'}${f.choch==='bearish'?' + bearish CHoCH':''}`);
  if(f.rvol) w.push(`Volume ${f.rvol.toFixed(2)}×`);
  if(f.fresh) w.push('Fresh breakdown this scan');
  return w;
}
function renderShorts(){
  const rows=[...slatest].map(h=>{
    const rr=realisticRR(h);
    const f={bestScore:h.score||0,rvol:h.rvol,bias:h.bias,choch:h.choch,rrq:rr.rr,rrMove:rr.move,fresh:h.is_new};
    return {...h, _rating:bearConviction(f), _why:bearWhy(f)};
  }).filter(h=>biasOk(h,"shorts")).sort((a,b)=>{
    if(sSortKey==="score"){ return sSortDir*((a._rating)-(b._rating)); }  // sort by rating on the Score/default
    const x=a[sSortKey],y=b[sSortKey];
    if(typeof x==="string") return sSortDir*x.localeCompare(y);
    return sSortDir*((x??0)-(y??0));
  });
  const tb=document.getElementById("srows"); tb.innerHTML="";
  document.getElementById("sempty").style.display = rows.length? "none":"block";
  for(const h of rows){
    const tr=document.createElement("tr");
    tr.className=rowClass(h);
    tr.innerHTML =
      `<td class="sym"><a href="${tvLink(h.symbol)}" target="_blank" rel="noopener">${h.symbol}</a>${badges(h)}</td>`+
      ratingCell(h._rating,h._why)+
      `<td data-tip="Live last-traded price — refreshes ~every 20s and is independent of the chart timeframe.">${fmtNum(h.live!=null?h.live:h.price)}</td>`+
      `<td>${(+h.pct_below_ema).toFixed(2)}</td>`+
      `<td>${h.bars_since_cross}</td>`+
      `<td><span class="biaspill2 b-${(h.bias||'').toLowerCase().replace(/[^a-z]/g,'')}">${h.bias||'—'}</span></td>`+
      corrTd(h.btc_corr)+
      `<td class="whycell" data-tip="${esc(h._why.join(' · '))}">${h._why[0]}</td>`+
      `<td data-tip="Optimal short entry — a rally back up to the 200 EMA (resistance) rather than shorting into support.">${fmtNum(h.optimal_entry)}</td>`+
      `<td data-tip="Tight stop — just ABOVE the EMA / retest high, ATR-buffered. A close back above invalidates the short.">${fmtNum(h.sl_tight)}</td>`+
      `<td data-tip="Wide stop — above a deeper swing high.">${fmtNum(h.sl_wide)}</td>`+
      tpCell(h.tp1,h.rr1)+tpCell(h.tp2,h.rr2)+tpCell(h.tp3,h.rr3)+tpCell(h.tp4,h.rr4)+tpCell(h.tp5,h.rr5)+
      rvCell(h.rvol)+
      `<td class="score">${(+h.score).toFixed(1)}</td>`;
    tb.appendChild(tr);
  }
}
function renderTop(){
  if(!lastData) return;
  const srcB=[["200-EMA reclaim",lastData.hits],["Bull flag",lastData.flag_hits],
              ["Narrow CPR",lastData.cpr_hits],["Support bounce",lastData.bounce_hits],
              ["Falling wedge",lastData.wedge_hits]];
  const m={};
  for(const [lbl,list] of srcB) for(const h of (list||[])){
    const k=h.symbol, q=setupQuality(h);
    if(!m[k]) m[k]={row:null,bestQ:-1,best:0,setups:new Set(),rvol:0,rr:{rr:0,tp:null,move:0},
                    fresh:false,bias:h.bias,choch:h.choch};
    const o=m[k]; o.setups.add(lbl);
    o.best=Math.max(o.best,h.score||0);
    if(h.rvol&&h.rvol>o.rvol) o.rvol=h.rvol;
    if(h.is_new||h.fresh) o.fresh=true;
    if(q>o.bestQ){ o.bestQ=q; o.row=h; o.rr=realisticRR(h); o.bias=h.bias; o.choch=h.choch; }
  }
  let items=Object.values(m).map(o=>{
    const f={nScans:o.setups.size,bestScore:o.best,rvol:o.rvol,bias:o.bias,choch:o.choch,
             rrq:o.rr.rr,rrMove:o.rr.move,fresh:o.fresh,setups:[...o.setups].join(', ')};
    o.corr=o.row.btc_corr;
    o.rating=Math.max(0,Math.min(100, bullConviction(f)+indepAdj(o.corr)));
    o.why=bullWhy(f);
    if(o.corr!=null) o.why.push(o.corr>=0.85?`⚠ Highly BTC-correlated (ρ${o.corr.toFixed(2)}) — mostly BTC beta`
                              : o.corr<0.5?`Independent of BTC (ρ${o.corr.toFixed(2)}) — its own move`
                              : `Moderate BTC correlation (ρ${o.corr.toFixed(2)})`);
    o.f=f; return o;
  });
  // QUALITY GATE — only genuinely good longs: a real reward:risk AND a solid grade,
  // backed by either multi-scan confluence or a strong standalone setup. Strong
  // 3-scan confluence can pass with a slightly lower R:R.
  items=items.filter(o=>
      o.rating>=52 &&
      (o.rr.rr>=1.5 || (o.f.nScans>=3 && o.rr.rr>=1.1)) &&
      (o.f.nScans>=2 || o.best>=68)
  );
  if(FILT.top.indep) items=items.filter(o=> o.corr!=null && o.corr<0.6);
  items=items.sort((a,b)=> b.rating-a.rating || b.rr.rr-a.rr.rr).slice(0,30);
  const tb=document.getElementById('tbrows'); tb.innerHTML="";
  document.getElementById('tbempty').style.display = items.length? "none":"block";
  const topEl=document.getElementById('topCount');
  if(topEl) topEl.textContent = `${items.length} high-conviction long setup(s)`;
  for(const o of items){
    const h=o.row, P=h.live!=null?h.live:h.price;
    const shortWhy = (o.f.nScans>=2? `★ ${o.f.nScans} scans (${o.f.setups})` : o.f.setups)
                   + (o.rvol? ` · vol ${o.rvol.toFixed(1)}×`:'');
    const tgtTip = o.rr.move? esc(`Target ${fmtNum(o.rr.tp)} — a +${(o.rr.move*100).toFixed(1)}% move, the basis for the R:R.`):'';
    const tr=document.createElement('tr'); tr.className=rowClass(h);
    tr.innerHTML =
      `<td class="sym"><a href="${tvLink(h.symbol)}" target="_blank" rel="noopener">${h.symbol}</a>${badges(h)}</td>`+
      ratingCell(o.rating,o.why)+
      `<td data-tip="${esc(o.f.setups)}">${o.f.nScans>1?'★ ':''}${o.f.nScans}</td>`+
      `<td>${fmtNum(P)}</td>`+
      `<td><span class="biaspill2 b-${(h.bias||'').toLowerCase().replace(/[^a-z]/g,'')}">${h.bias||'—'}</span></td>`+
      corrTd(o.corr)+
      `<td>${fmtNum(h.optimal_entry)}</td>`+
      `<td>${fmtNum(h.sl_tight)}</td>`+
      `<td data-tip="${tgtTip}">${o.rr.tp!=null?fmtNum(o.rr.tp):'—'}</td>`+
      `<td data-tip="Realistic reward:risk — to a target at least ~2% away, capped at 8:1 so measured-move projections don't inflate it.">${o.rr.rr?('<b>'+o.rr.rr.toFixed(2)+'</b>'):'—'}</td>`+
      `<td class="whycell" data-tip="${esc(o.why.join(' · '))}">${shortWhy}</td>`;
    tb.appendChild(tr);
  }
}
async function poll(){
  try{
    const r=await fetch("/data",{cache:"no-store"}); const d=await r.json();
    lastData=d;
    latest=d.hits||[]; render();
    flatest=d.flag_hits||[]; renderFlags();
    clatest=d.cpr_hits||[]; renderCPR();
    blatest=d.bounce_hits||[]; renderBounce();
    wlatest=d.wedge_hits||[]; renderWedge();
    slatest=d.short_hits||[]; renderShorts();
    renderTop();
    const evs=(d.breakout_events||[]).filter(e=>e.time>seenBreak);
    if(evs.length){ seenBreak=Math.max(seenBreak, ...evs.map(e=>e.time)); if(alertsOn) fireBreakout(evs); }
    renderBanner();
    const nboth=(d.both_symbols||[]).length;
    const bothTxt=nboth?` · ★ ${nboth} confluence`:"";
    document.getElementById("flagCount").textContent = `${flatest.length} flag(s) · ${d.universe} pairs${bothTxt}`;
    document.getElementById("cprCount").textContent = `${clatest.length} narrow-CPR · ${d.universe} pairs${bothTxt}`;
    document.getElementById("bounceCount").textContent = `${blatest.length} bounce(s) · ${d.universe} pairs${bothTxt}`;
    document.getElementById("wedgeCount").textContent = `${wlatest.length} wedge(s) · ${d.universe} pairs${bothTxt}`;
    document.getElementById("shortCount").textContent = `${slatest.length} short setup(s) · ${d.universe} pairs`;
    // topCount is set inside renderTop() (it knows the deduped long count).
    document.getElementById("meta").textContent =
      `${d.cfg.interval} chart · MEXC ${d.cfg.market==='futures'?'perps ⚡':'spot'} · ${d.cfg.quote} · EMA${d.cfg.ema_period} · rescans every ${d.cfg.scan_every}m${d.cfg.telegram?' · 📲 Telegram on':''}`;
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
const HDR_TIPS={
  symbol:"The coin. Click a column header to sort by it; click a symbol to open its TradingView chart. The ★ badge = confluence (hover it), the 🛟 = next 4h/Daily/Weekly support (hover it).",
  price:"Last closed price on the scan timeframe.",
  pct_above_ema:"How far price sits above the 200 EMA (%). Smaller = closer to the line = tighter entry.",
  bars_since_cross:"How many candles ago the EMA was reclaimed. Fewer = fresher signal.",
  retest_gap_pct:"How close the pullback's low tagged the EMA. Near 0 = a clean retest.",
  bias:"Market-structure bias of the setup: Bullish, Bullish CHoCH (fresh reversal), Bearish, or Range.",
  optimal_entry:"Lower-risk entry — a pullback to the setup's support / EMA / breakout level rather than chasing.",
  sl_tight:"Tighter stop-loss, ATR-buffered just below the immediate structure. Smaller risk, easier to get stopped.",
  sl_wide:"Wider stop-loss below a deeper swing low. More room, bigger risk per unit.",
  tp1:"First profit target with its reward:risk to the tight stop.",
  tp2:"Second profit target (R:R to tight stop).",
  tp3:"Third profit target (R:R to tight stop).",
  tp4:"Fourth profit target (R:R to tight stop).",
  tp5:"Fifth profit target (R:R to tight stop).",
  rvol:"Relative volume — latest candle vs its 20-bar average. Above 1× = above-average activity = confirmation.",
  score:"Overall setup quality, 0–100 (higher = cleaner/stronger).",
  pole_gain_pct:"Flagpole size — how far price ran up into the flag (%). Bigger impulse = stronger.",
  flag_bars:"How many candles the flag consolidation has lasted.",
  pullback_pct:"How deep the flag retraced the pole. Shallower (well under 50%) is healthier.",
  vol_contraction:"Flag average volume ÷ pole average volume. Below 1 = volume drying up = the classic constructive flag.",
  breakout:"The breakout trigger — the top of the flag / CPR. A move above confirms.",
  cpr_width_pct:"Central Pivot Range width as % of price. Narrower = more compressed/coiled.",
  position:"Where price sits vs the CPR — above (breakout side) or inside.",
  tc:"CPR top (Top Central).", bc:"CPR bottom (Bottom Central).",
  support:"The horizontal support level price is bouncing from.",
  tf:"Strongest timeframe the support sits on — Weekly (gold) > Daily (green) > 4h. Higher = more significant.",
  touches:"How many times that support level has been tested. More = stronger.",
  dist_to_support_pct:"How far price currently sits above the support (%). Smaller = fresher bounce.",
  rsi:"RSI(14) momentum, 0–100. Below 30 = oversold (bounce potential), above 70 = overbought.",
  conv_pct:"How far the two wedge lines have converged toward the apex. Higher = tighter coil, nearer resolution.",
  phase:"Coiling = still forming inside the wedge. Broke out = price has pushed above the upper (descending) line.",
  pct_below_ema:"How far price sits BELOW the falling 200 EMA (%). This is a short: closer to the line = tighter entry."
};
function applyHeaderTips(){
  document.querySelectorAll("th[data-k],th[data-fk],th[data-ck],th[data-bk],th[data-wk],th[data-sk]").forEach(th=>{
    const k=th.dataset.k||th.dataset.fk||th.dataset.ck||th.dataset.bk||th.dataset.wk||th.dataset.sk;
    if(HDR_TIPS[k]) th.setAttribute("data-tip",HDR_TIPS[k]);
  });
}
applyHeaderTips();
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
            iv = (q.get("interval") or [state.cfg["interval"]])[0]
            if iv not in ("1h", "4h", "1d", "1w"):
                iv = "4h"
            sym = normalize_symbol(raw, state.cfg.get("quote", "USDT"))
            if not sym:
                self._send(200, json.dumps({"error": "Enter a ticker, e.g. BTC."}).encode(),
                           "application/json")
                return
            try:
                sess = get_session()
                out = analyze_symbol(sess, sym, iv, state.cfg)
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
    p.add_argument("--cpr-max-width", type=float, default=0.75,
                   help="narrow CPR: max CPR width as %% of price (0.75 = 0.75%%)")
    p.add_argument("--include-spot-only", action="store_true",
                   help="spot market: also scan coins NOT on futures")
    p.add_argument("--market", default=os.environ.get("MARKET", "futures"),
                   choices=["futures", "spot"],
                   help="which MEXC market to scan (default: futures/perps)")
    args = p.parse_args()

    cfg = {
        "port": args.port, "scan_every": args.scan_every, "quote": args.quote,
        "interval": args.interval, "workers": args.workers,
        "kline_limit": args.kline_limit, "lookback": args.lookback,
        "retest_tol": args.retest_tol, "break_tol": args.break_tol,
        "max_above_now": args.max_above, "min_slope": args.min_slope,
        "pole_min_gain": args.pole_min_gain, "flag_max_retrace": args.flag_max_retrace,
        "cpr_max_width_pct": args.cpr_max_width,
        "futures_only": not args.include_spot_only,
        "market": args.market,
        "telegram_token": os.environ.get("TELEGRAM_TOKEN", "").strip(),
        "telegram_chat": os.environ.get("TELEGRAM_CHAT_ID", "").strip(),
    }
    state = State(cfg)

    if cfg["telegram_token"] and cfg["telegram_chat"]:
        print("  Telegram alerts: ON")
        send_telegram(cfg, "✅ MEXC scanner is live — breakout &amp; confluence "
                           "alerts are on.")
    else:
        print("  Telegram alerts: off (set TELEGRAM_TOKEN and TELEGRAM_CHAT_ID)")

    t = threading.Thread(target=scan_loop, args=(state,), daemon=True)
    t.start()
    threading.Thread(target=breakout_watcher, args=(state,), daemon=True).start()

    srv = ThreadingHTTPServer(("0.0.0.0", args.port), make_handler(state))
    url = f"http://localhost:{args.port}"
    print(f"\n  MEXC 200-EMA cross & retest dashboard")
    print(f"  scanning MEXC {cfg['market']} ({cfg['quote']}) on the "
          f"{cfg['interval']} chart, every {cfg['scan_every']} min")
    print(f"  open  {url}   (Ctrl+C to stop)\n")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\n  stopped.")


if __name__ == "__main__":
    main()
