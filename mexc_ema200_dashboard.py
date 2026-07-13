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
        bias_on_tf, enrich_1h, best_pattern,
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
        self.stb_hits: list[dict] = []             # supertrend support bounce (multi-TF)
        self.prev_stb_symbols: set[str] | None = None
        self.new_stb_symbols: list[str] = []
        self.early_hits: list[dict] = []           # early / pre-breakout accumulation
        self.prev_early_symbols: set[str] | None = None
        self.new_early_symbols: list[str] = []
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
                "stb_hits": withlive(self.stb_hits),
                "stb_new_symbols": list(self.new_stb_symbols),
                "early_hits": withlive(self.early_hits),
                "early_new_symbols": list(self.new_early_symbols),
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
    st_bounces: list[dict] = []
    earlies: list[dict] = []
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
                h, f, c, b, w, sh, sb, el = fut.result()
            except Exception:
                h, f, c, b, w, sh, sb, el = (None,) * 8
            if el:
                earlies.append(el)
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
            if sb:
                st_bounces.append(sb)

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
    st_bounces.sort(key=lambda h: h["score"], reverse=True)
    earlies.sort(key=lambda h: h["score"], reverse=True)

    # Enrich ONLY the flagged coins with a 1h read (bias + formation) — a single 1h
    # fetch each, cheap since it's just the few dozen hits (not the ~500 universe).
    # This completes the 1h/4h/1D/1W bias strip AND the multi-timeframe pattern set.
    all_rows = (hits + flags + cprs + bounces + wedges + shorts + st_bounces + earlies)
    hit_syms = sorted({r["symbol"] for r in all_rows})
    if hit_syms:
        mkt = cfg.get("market", "futures")
        one: dict[str, tuple] = {}
        with ThreadPoolExecutor(max_workers=min(12, cfg["workers"])) as ex:
            futs = {ex.submit(enrich_1h, sess, s, mkt): s for s in hit_syms}
            for fut in as_completed(futs):
                try:
                    one[futs[fut]] = fut.result()
                except Exception:
                    one[futs[fut]] = (None, None)
        for r in all_rows:
            b1h, p1h = one.get(r["symbol"], (None, None))
            tb = r.get("tf_bias")
            if isinstance(tb, dict):
                tb["1h"] = b1h
            if p1h:
                mtf = [x for x in (r.get("patterns_mtf") or []) if x.get("tf") != "1h"]
                mtf = [p1h] + mtf
                r["patterns_mtf"] = mtf
                r["pattern"] = best_pattern(mtf)          # may now include 1h

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
        "Supertrend bounce": {h["symbol"] for h in st_bounces},
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
        sbrows, sbnew, sbcur = tag_new(st_bounces, state.prev_stb_symbols)
        elrows, elnew, elcur = tag_new(earlies, state.prev_early_symbols)
        for d in rows + frows + crows + brows + wrows + srows + sbrows + elrows:
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
        state.stb_hits, state.new_stb_symbols, state.prev_stb_symbols = sbrows, sbnew, sbcur
        state.early_hits, state.new_early_symbols, state.prev_early_symbols = elrows, elnew, elcur
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
<title>Apex — MEXC Futures Scanner</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@500;600&display=swap" rel="stylesheet">
<style>
  :root{--bg:#0a0d13;--bg2:#0d1119;--panel:#141a26;--panel2:#171e2c;--line:#222b3b;
        --line2:#2c3648;--txt:#eaf0f7;--dim:#8593a8;--dim2:#5f6b80;
        --accent:#3fb950;--accent-d:#2ea043;--blue:#58a6ff;--red:#f85149;--warn:#d29922;
        --head:#151c29;--radius:12px;
        --mono:"JetBrains Mono",ui-monospace,SFMono-Regular,Menlo,monospace;
        --shadow:0 10px 40px rgba(0,0,0,.45);}
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
  td.ematp{background:rgba(88,166,255,.13);box-shadow:inset 2px 0 0 #58a6ff}
  td.revtp{background:rgba(63,185,80,.12);box-shadow:inset 2px 0 0 var(--accent);font-weight:600}
  .emastar{color:#58a6ff;font-weight:800;margin-left:3px;cursor:help}
  .azbtn{cursor:pointer;margin-left:6px;color:var(--dim);font-size:13px;user-select:none;vertical-align:middle;border:1px solid var(--line);border-radius:5px;padding:0 5px}
  .azbtn:hover{color:var(--accent);border-color:var(--accent)}
  .wstar{cursor:pointer;margin-right:6px;color:var(--dim);font-size:15px;user-select:none;vertical-align:middle}
  .wstar:hover{color:#f0b429}
  .wstar.on{color:#f0b429}
  .wlbtn{background:var(--bg);border:1px solid var(--line);border-radius:6px;padding:3px 9px;font-size:12px;font-weight:600;color:#e6edf3;cursor:pointer;margin-right:6px}
  .wlbtn:hover{border-color:var(--accent)}
  .azladder{display:flex;flex-wrap:wrap;gap:8px;margin:10px 0 4px}
  .azrec{margin:10px 0 2px;font-size:14px;color:#e6edf3}
  .azrec b{color:var(--accent)}
  .azstop b{color:#f85149}
  .ladrec{border-color:var(--accent)!important;background:rgba(63,185,80,.12)!important;box-shadow:0 0 0 1px rgba(63,185,80,.35)}
  .ladrecstop{border-color:#f85149!important;background:rgba(248,81,73,.12)!important;box-shadow:0 0 0 1px rgba(248,81,73,.35)}
  .sidetog{display:inline-flex;gap:0;border:1px solid var(--line);border-radius:8px;overflow:hidden;margin-left:8px}
  .sidetog button{background:var(--bg);color:var(--dim);border:0;padding:4px 12px;font-size:12px;font-weight:700;cursor:pointer}
  .sidetog button.on{color:#fff}
  .sidetog button.on.long{background:rgba(63,185,80,.35)}
  .sidetog button.on.short{background:rgba(248,81,73,.35)}
  .sidenote{color:var(--dim);font-size:12px;margin:2px 0 8px}
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
  .patbadge{border-radius:6px;padding:0 7px;font-size:10.5px;font-weight:700;border:1px solid var(--line);margin-left:5px;cursor:help;white-space:nowrap}
  .pat-bull{background:rgba(63,185,80,.14);color:var(--accent);border-color:rgba(63,185,80,.4)}
  .pat-bear{background:rgba(248,81,73,.14);color:#f85149;border-color:rgba(248,81,73,.4)}
  .pat-neu{background:rgba(139,152,173,.12);color:var(--dim)}
  .tfbias{border-radius:5px;padding:0 5px;font-size:10px;font-weight:700;border:1px solid var(--line);margin-left:4px;cursor:help;font-variant-numeric:tabular-nums}
  .patwrap{display:flex;flex-direction:column;gap:6px;margin:8px 0 2px}
  .patrow{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
  .pattf{min-width:54px;color:var(--dim);font-size:12px;font-weight:700}
  .phasepill{border-radius:6px;padding:1px 7px;font-size:11px;font-weight:700;border:1px solid var(--line)}
  .phase-broke{background:rgba(63,185,80,.16);color:var(--accent);border-color:rgba(63,185,80,.5)}
  .phase-form{background:rgba(139,152,173,.12);color:var(--dim)}
  /* ================= PREMIUM DESIGN SYSTEM (overrides above) ================= */
  html{-webkit-font-smoothing:antialiased;-moz-osx-font-smoothing:grayscale;text-rendering:optimizeLegibility}
  body{font-family:"Inter",-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;
       background:
         radial-gradient(1100px 520px at 82% -12%,rgba(63,185,80,.07),transparent 55%),
         radial-gradient(900px 500px at 8% -8%,rgba(88,166,255,.05),transparent 55%),
         linear-gradient(180deg,var(--bg2),var(--bg));
       letter-spacing:.005em}
  ::selection{background:rgba(63,185,80,.30);color:#fff}
  *{scrollbar-width:thin;scrollbar-color:#2b3446 transparent}
  *::-webkit-scrollbar{width:11px;height:11px}
  *::-webkit-scrollbar-thumb{background:#2b3446;border-radius:9px;border:3px solid transparent;background-clip:padding-box}
  *::-webkit-scrollbar-thumb:hover{background:#3b4761;background-clip:padding-box}
  @media(prefers-reduced-motion:reduce){*{transition:none!important;animation:none!important}}
  a,button,.tab,.fbtn,.tfbtn,.azbtn,.wstar,.wlbtn,.alertbtn,.ladchip,.azcell,.chip a,td.sym a,.biaspill2,tbody tr{
       transition:background .16s ease,color .16s ease,border-color .16s ease,transform .13s cubic-bezier(.2,.7,.3,1),box-shadow .16s ease,filter .16s ease}
  /* ---- header: glassy brand bar ---- */
  header{padding:14px 26px;background:linear-gradient(180deg,rgba(15,20,30,.82),rgba(15,20,30,.35));
       backdrop-filter:blur(10px);-webkit-backdrop-filter:blur(10px);
       border-bottom:1px solid var(--line);position:sticky;top:0;z-index:70;align-items:center}
  h1{font-size:17px;font-weight:800;letter-spacing:-.02em;display:flex;align-items:center;gap:10px}
  h1::before{content:"";width:13px;height:13px;border-radius:4px;
       background:linear-gradient(135deg,var(--accent),#2f81f7);box-shadow:0 0 14px rgba(63,185,80,.7)}
  .brandsub{font-size:11.5px;font-weight:600;color:var(--dim);letter-spacing:.02em;
       padding-left:11px;margin-left:2px;border-left:1px solid var(--line2)}
  .sub{color:var(--dim);font-size:12px;font-variant-numeric:tabular-nums}
  .alertbtn{border-radius:9px;font-weight:600}
  .alertbtn:hover{border-color:var(--accent);color:var(--accent);transform:translateY(-1px)}
  /* ---- tabs: segmented nav ---- */
  .tabs{gap:6px;padding:14px 26px 0;flex-wrap:wrap}
  .tab{border:1px solid var(--line);border-radius:10px;background:rgba(21,28,41,.7);
       padding:8px 15px;font-weight:600;font-size:12.5px;color:var(--dim)}
  .tab:hover{color:var(--txt);border-color:var(--line2);transform:translateY(-1px);background:rgba(28,36,52,.8)}
  .tab.active{color:#eafff0;border-color:rgba(63,185,80,.55);
       background:linear-gradient(180deg,rgba(63,185,80,.22),rgba(63,185,80,.07));
       box-shadow:0 4px 16px rgba(63,185,80,.18),inset 0 1px 0 rgba(255,255,255,.06)}
  /* ---- filter bar ---- */
  .filterbar{padding:11px 26px}
  .fbtn{border-radius:16px;padding:4px 13px}
  .fbtn:hover{border-color:var(--accent);color:var(--txt);transform:translateY(-1px)}
  .tfbtn{border-radius:16px}
  .tfbtn:hover{border-color:var(--accent);color:var(--txt)}
  /* ---- status strip ---- */
  .status{padding:11px 26px;font-size:12px}
  /* ---- tables ---- */
  .wrap{padding:16px 26px 70px}
  table{border-spacing:0}
  tbody td{font-family:var(--mono);font-size:12.5px;font-weight:500}
  td.sym{font-family:"Inter",sans-serif}
  th,td{padding:10px 14px}
  thead th{top:0;z-index:40;background:rgba(21,28,41,.92);backdrop-filter:blur(8px);
       -webkit-backdrop-filter:blur(8px);border-bottom:1px solid var(--line2);
       box-shadow:0 4px 14px rgba(0,0,0,.4);padding-top:11px;padding-bottom:11px;
       text-transform:uppercase;letter-spacing:.05em;font-size:10.5px;font-weight:700;color:var(--dim2)}
  thead th:hover{color:var(--txt)}
  tbody tr:nth-child(even) td{background:rgba(255,255,255,.012)}
  tbody tr:hover td{background:rgba(63,185,80,.07)}
  tbody tr:hover td:first-child{box-shadow:inset 3px 0 0 var(--accent)}
  td .rr{font-family:var(--mono);color:var(--dim);font-size:10.5px;margin-left:4px}
  td.sym a{font-weight:700;letter-spacing:.01em}
  td.sym a:hover{color:var(--accent)}
  .score{font-family:var(--mono);font-weight:700}
  /* freeze the Symbol column so you never lose track of which coin a row is */
  td.sym{position:sticky;left:0;z-index:30;background:var(--bg2);
       box-shadow:8px 0 12px -8px rgba(0,0,0,.55)}
  tbody tr:hover td.sym{background:#121a28}
  thead th:first-child{position:sticky;left:0;z-index:46;background:rgba(21,28,41,.98)}
  th[data-sort]::after{margin-left:6px;color:var(--accent);font-size:9px;vertical-align:middle}
  th[data-sort="asc"]::after{content:"▲"}
  th[data-sort="desc"]::after{content:"▼"}
  /* ---- badges / pills ---- */
  .biaspill2,.tfpill,.pill,.phasepill{border-radius:6px;font-weight:700;letter-spacing:.02em}
  tr:hover .biaspill2{transform:translateY(-1px)}
  .newbadge,.bothbadge,.freshbadge{box-shadow:0 2px 8px rgba(0,0,0,.35)}
  .patbadge{border-radius:6px;font-weight:600}
  /* ---- banners ---- */
  .banner{border-radius:12px;box-shadow:var(--shadow);backdrop-filter:blur(6px)}
  /* ---- empty state ---- */
  .empty{font-size:13.5px;opacity:.85;padding:56px 0}
  .empty::before{content:"◎";display:block;font-size:30px;color:var(--line2);margin-bottom:10px}
  /* ---- analyze: search + tf ---- */
  .azbar{max-width:560px}
  .azbar input{border-radius:11px;padding:12px 14px;background:var(--panel2);font-size:14px}
  .azbar input:focus{border-color:var(--accent);box-shadow:0 0 0 3px rgba(63,185,80,.16)}
  .azbar button{border-radius:11px;font-weight:700;box-shadow:0 4px 14px rgba(63,185,80,.25)}
  .azbar button:hover:not(:disabled){filter:brightness(1.08);transform:translateY(-1px)}
  .tfbtn.active,.fbtn.active{box-shadow:0 3px 12px rgba(63,185,80,.28)}
  /* ---- analyze card ---- */
  .azcard{border-radius:16px;border:1px solid var(--line2);padding:20px 22px;
       background:linear-gradient(180deg,#171e2c,#131824);box-shadow:var(--shadow);position:relative;overflow:hidden}
  .azcard::before{content:"";position:absolute;inset:0 0 auto 0;height:2px;
       background:linear-gradient(90deg,var(--accent),transparent 60%);opacity:.7}
  .azhead .sym{font-size:20px;font-weight:800;letter-spacing:-.01em}
  /* verdict bar — the bottom line, up top */
  .azverdict{display:flex;flex-wrap:wrap;align-items:center;gap:8px 16px;margin:12px 0 4px;
       padding:13px 16px;border-radius:13px;border:1px solid var(--line2);
       background:linear-gradient(90deg,rgba(63,185,80,.12),rgba(20,25,36,.35))}
  .azverdict.short{background:linear-gradient(90deg,rgba(248,81,73,.12),rgba(20,25,36,.35))}
  .azverdict.none{background:linear-gradient(90deg,rgba(227,179,65,.12),rgba(20,25,36,.35))}
  .vbadge{font-weight:800;padding:5px 13px;border-radius:9px;font-size:13px;letter-spacing:.02em}
  .v-long{background:rgba(63,185,80,.22);color:var(--accent);border:1px solid rgba(63,185,80,.55)}
  .v-short{background:rgba(248,81,73,.22);color:#ff6b63;border:1px solid rgba(248,81,73,.55)}
  .v-none{background:rgba(227,179,65,.22);color:var(--warn);border:1px solid rgba(227,179,65,.55)}
  .vgrade{font-weight:800;font-size:13px;color:#fff;background:rgba(255,255,255,.06);
       border:1px solid var(--line2);border-radius:8px;padding:4px 10px}
  .vitem{font-size:13.5px;color:var(--txt);font-family:var(--mono);font-weight:600}
  .vitem i{font-style:normal;color:var(--dim2);font-size:9.5px;text-transform:uppercase;
       letter-spacing:.06em;margin-right:5px;font-family:"Inter",sans-serif;font-weight:600}
  .vitem b{color:var(--accent);font-weight:700}
  .tfsrc{font-family:"Inter",sans-serif;font-size:9.5px;font-weight:700;letter-spacing:.03em;
       color:var(--accent);background:rgba(63,185,80,.13);border:1px solid rgba(63,185,80,.35);
       border-radius:5px;padding:0 5px;margin-left:5px;vertical-align:middle;cursor:help}
  .vsep{width:1px;height:20px;background:var(--line2)}
  .azsec{border-top:1px solid var(--line);margin-top:18px;padding-top:14px;
       font-weight:800;color:var(--txt);letter-spacing:.02em;font-size:13px;text-transform:uppercase}
  .azsec .azsub{text-transform:none;font-weight:500}
  .azsec:first-of-type{border-top:none;margin-top:6px}
  .azgrid{gap:12px;grid-template-columns:repeat(auto-fit,minmax(165px,1fr))}
  .azcell{border-radius:11px;border:1px solid var(--line);background:var(--panel2);padding:12px 14px}
  .azcell:hover{border-color:var(--line2);background:#151d2b;transform:translateY(-1px)}
  .azcell .k{font-size:10px;letter-spacing:.05em;text-transform:uppercase;color:var(--dim2);font-weight:600}
  /* values are Inter (readable for mixed text like "downtrend · bullish CHoCH"),
     with tabular figures so pure numbers still align. Mono is ONLY for tables. */
  .azcell .v{font-family:"Inter",sans-serif;font-weight:700;font-size:14.5px;line-height:1.45;
       margin-top:5px;font-variant-numeric:tabular-nums;word-break:normal;overflow-wrap:anywhere;color:var(--txt)}
  .azcell .v .rr{font-family:var(--mono);font-size:11px}
  .azrec{border-radius:11px;padding:11px 14px;margin:9px 0;background:rgba(63,185,80,.06);
       border:1px solid rgba(63,185,80,.18)}
  .azrec.azstop{background:rgba(248,81,73,.06);border-color:rgba(248,81,73,.2)}
  .azrec b{font-family:var(--mono)}
  .sidenote{border-radius:10px;padding:9px 13px;background:rgba(255,255,255,.02);border:1px solid var(--line);margin:7px 0}
  .ladchip{border-radius:9px}
  .ladchip:hover{border-color:var(--accent);color:var(--txt);transform:translateY(-1px);background:rgba(63,185,80,.06)}
  .ladchip .rr,.azcell .rr{font-family:var(--mono)}
  /* ---- tooltip ---- */
  #tip{border-color:var(--line2);box-shadow:0 14px 40px rgba(0,0,0,.6);border-radius:11px;
       background:rgba(13,17,25,.97);backdrop-filter:blur(6px);font-size:12px;line-height:1.5;padding:9px 12px}
  /* ---- info tab ---- */
  .info h2{font-weight:800;letter-spacing:.01em}
  .info code{font-family:var(--mono);font-size:12px}
  /* ---- watchlist buttons ---- */
  .wlbtn{border-radius:8px}
  .wlbtn:hover{transform:translateY(-1px)}
  /* ---- responsive ---- */
  @media(max-width:680px){
    header,.status,.tabs,.filterbar,.wrap{padding-left:14px;padding-right:14px}
    .tabs{gap:5px;padding-top:10px}.tab{padding:7px 11px;font-size:12px}
    th,td{padding:8px 9px}.wrap{padding-bottom:60px}
  }
</style></head>
<body>
<header>
  <h1>Apex<span class="brandsub">MEXC Futures Scanner</span></h1>
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
  <div class="tab" id="tabStb" onclick="showTab('stb')">Supertrend support bounce</div>
  <div class="tab" id="tabShorts" onclick="showTab('shorts')">Shorts</div>
  <div class="tab" id="tabEarly" onclick="showTab('early')">⏳ Early</div>
  <div class="tab" id="tabTop" onclick="showTab('top')">⭐ Top setups</div>
  <div class="tab" id="tabWatch" onclick="showTab('watch')">📌 Watchlist</div>
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

<div class="view" id="viewStb">
<div class="status">
  <span>Supertrend support bounce — price bouncing off its <b>Supertrend</b> line used as support, checked on 4h / Daily / Weekly. Fires when price holds just above the nearest up-trending Supertrend line, has tagged it, and is turning back up. A higher-timeframe line is stronger.</span>
  <span id="stbCount"></span>
</div>
<div class="wrap">
  <table id="sbtbl">
    <thead><tr>
      <th data-xk="symbol">Symbol</th>
      <th data-xk="price">Price</th>
      <th data-xk="supertrend">Supertrend</th>
      <th data-xk="tf">TF</th>
      <th data-xk="tf_up">TFs up</th>
      <th data-xk="dist_to_st_pct">Dist %</th>
      <th data-xk="rsi">RSI</th>
      <th data-xk="bias">Bias</th>
      <th data-tip="BTC correlation ρ over ~10 days. Low/negative = its own mover.">BTC ρ</th>
      <th data-xk="optimal_entry">Optimal entry</th>
      <th data-xk="sl_tight">SL tight</th>
      <th data-xk="sl_wide">SL wide</th>
      <th data-xk="tp1">TP1</th>
      <th data-xk="tp2">TP2</th>
      <th data-xk="tp3">TP3</th>
      <th data-xk="tp4">TP4</th>
      <th data-xk="tp5">TP5</th>
      <th data-xk="rvol">RVol</th>
      <th data-xk="score">Score</th>
    </tr></thead>
    <tbody id="sbrows"></tbody>
  </table>
  <div class="empty" id="sbempty" style="display:none">No Supertrend support bounces right now. The loop keeps scanning…</div>
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

<div class="view" id="viewEarly">
<div class="status">
  <span>⏳ Early / potential — <b>pre-breakout accumulation</b>. Beaten-down coins that are <b>coiling</b> (volatility contraction) on a strong higher-timeframe support, still below the 200 EMA, oversold or carving a higher low — <i>before</i> the reclaim/Supertrend flip confirms. Earlier entry, higher risk: treat as <b>unconfirmed</b>. TP1–3 are the nearer structural rungs; the <b>◎ 200 EMA (4h)</b> column is the mean-reversion goal that confirms the setup. All indicators here are on the 4h chart.</span>
  <span id="earlyCount"></span>
</div>
<div class="wrap">
  <table id="etbl">
    <thead><tr>
      <th data-ek="symbol">Symbol</th>
      <th data-ek="price">Price</th>
      <th data-ek="support">Support</th>
      <th data-ek="drawdown_pct">Off high %</th>
      <th data-ek="contraction">Coil</th>
      <th data-ek="pct_below_ema" data-tip="How far below the 200 EMA (on the 4h chart) the price is trading. The scanner runs on 4h candles, so this — and the EMA target — are the 4h 200 EMA.">% &lt; EMA (4h)</th>
      <th data-ek="rsi">RSI</th>
      <th data-ek="bias">Bias</th>
      <th data-tip="BTC correlation ρ over ~10 days. Low/negative = its own mover.">BTC ρ</th>
      <th data-ek="optimal_entry">Optimal entry</th>
      <th data-ek="sl_tight">SL tight</th>
      <th data-ek="sl_wide">SL wide</th>
      <th data-ek="tp1" data-tip="Nearer structural targets in order — prior swing highs the bounce must clear on the way up to the 200 EMA. The dedicated EMA column at right is the mean-reversion goal.">TP1</th>
      <th data-ek="tp2">TP2</th>
      <th data-ek="tp3">TP3</th>
      <th data-ek="ema_target" data-tip="The 200-EMA reclaim on the 4h chart — the mean-reversion target that confirms this early setup. Always shown here (◎) even when several nearer resistances sit below it. Grey = reward:risk to the tight stop.">◎ 200 EMA (4h)</th>
      <th data-ek="rev_tp_rr" data-tip="Recommended REVERSAL take-profit — the realistic 'good' target for this accumulation/reversal: the best reward:risk (expected value) among the 200-EMA reclaim and the overhead resistances, only if it clears 1.5:1. Click to rank by this R:R.">🎯 Best TP</th>
      <th data-ek="rvol">RVol</th>
      <th data-ek="score">Score</th>
    </tr></thead>
    <tbody id="erows"></tbody>
  </table>
  <div class="empty" id="eempty" style="display:none">No early/accumulation setups right now. The loop keeps scanning…</div>
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
      <th data-tip="Retest entry — a proper pullback level to wait for (nearest swing-low support / reclaimed EMA / Supertrend below price), rather than chasing the current candle. The R:R is measured from here over the tight stop.">Retest entry</th>
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

<div class="view" id="viewWatch">
<div class="status">
  <span>📌 Watchlist — coins you've starred. Add/remove from any tab (☆/★) or Analyze. Saved in this browser. Each coin auto-pulls its <b>LONG</b> plan — bias, active setups, recommended entry, stop and best take-profit. Click a row's ⚲ / Analyze for the full breakdown.</span>
  <span id="watchCount"></span>
  <span class="wlbtn" onclick="loadWatch(true)" style="cursor:pointer">↻ Refresh plans</span>
</div>
<div class="wrap">
  <table id="wltbl">
    <thead><tr>
      <th>Symbol</th>
      <th data-tip="Live last-traded price.">Price</th>
      <th data-tip="Market-structure bias (auto direction) on the 4h chart.">Bias</th>
      <th data-tip="Active scanner setups + the strongest chart pattern found.">Setups</th>
      <th data-tip="Recommended LONG entry — a proper pullback fill, not chasing.">🎯 Entry</th>
      <th data-tip="Recommended stop-loss (the level that invalidates the long), with ×ATR distance.">🛑 Stop</th>
      <th data-tip="Best realistic take-profit by expected value, with R:R and a plan grade.">⭐ Best TP</th>
      <th data-tip="BTC correlation ρ over ~10 days. Low/negative = its own mover.">BTC ρ</th>
      <th>Actions</th>
    </tr></thead>
    <tbody id="wlrows"></tbody>
  </table>
  <div class="empty" id="wlempty" style="display:none">Your watchlist is empty. Click the ☆ next to any coin's symbol (on any tab or in Analyze) to add it here.</div>
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
let xSortKey="score", xSortDir=-1, xlatest=[];
let eeSortKey="score", eeSortDir=-1, eelatest=[];
let activeTab="setups", lastData=null;
let topSyms=null, topNew=[];   // track coins newly entering Top setups
// Per-tab filters so a filter on one tab never hides rows on another.
const FILT={ setups:{bias:"all",biasTf:"4h",fresh:false}, flags:{bias:"all",biasTf:"4h",phase:"all"},
             cpr:{bias:"all",biasTf:"4h"}, bounce:{bias:"all",biasTf:"4h"}, stb:{bias:"all",biasTf:"4h"},
             wedge:{bias:"all",biasTf:"4h",phase:"all"}, shorts:{bias:"all",biasTf:"4h"},
             early:{bias:"all",biasTf:"4h"}, top:{indep:false} };
function biasOk(h,tab){ const f=FILT[tab]||{}; const b=f.bias;
  if(!b||b==="all") return true;
  const tf=f.biasTf||"4h";
  const lbl=(h.tf_bias&&h.tf_bias[tf]) || (tf==="4h"?h.bias_dir:null);
  return lbl===b; }
function renderFilterBar(){
  const bar=document.getElementById("filterbar"); if(!bar) return;
  const t=activeTab;
  if(!FILT[t]){ bar.style.display="none"; return; }
  bar.style.display="flex";
  const f=FILT[t]; let h="";
  if('bias' in f){ h+="Bias: ";
    for(const [k,l] of [["all","All"],["bullish","Bullish"],["bearish","Bearish"],["neutral","Neutral"]])
      h+=`<span class="fbtn ${f.bias===k?'active':''}" onclick="setF('${t}','bias','${k}')">${l}</span>`;
    if(f.bias!=="all"){
      h+='<span style="color:var(--dim);font-size:11px">on</span>';
      for(const [k,l] of [["1h","1h"],["4h","4h"],["1d","1D"],["1w","1W"]])
        h+=`<span class="fbtn ${f.biasTf===k?'active':''}" onclick="setF('${t}','biasTf','${k}')" data-tip="Judge the bias on the ${l} timeframe (market structure). Lets you find coins that are, say, bullish on the 1D even if the 4h is choppy.">${l}</span>`;
    }
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
  bar.innerHTML=h;
}
function setF(tab,key,val){ FILT[tab][key]=val; renderFilterBar();
  ({setups:render,flags:renderFlags,cpr:renderCPR,bounce:renderBounce,
    shorts:renderShorts,top:renderTop,stb:renderStb,
    early:renderEarly}[tab]||(()=>{}))();
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
// Some contracts are listed on TradingView under a different ticker than MEXC's
// futures symbol — map those so the chart link resolves (e.g. AIGENSYN = AI).
const TV_ALIAS={AIGENSYNUSDT:"AIUSDT"};
// Some coins are listed under a different ticker than MEXC's futures symbol —
// show the ticker people actually know (e.g. AIGENSYN is "AI" on MEXC).
const SYM_ALIAS={AIGENSYNUSDT:"AIUSDT"};
function dispSym(s){ return SYM_ALIAS[s]||s; }
function tvLink(sym){ const s=TV_ALIAS[sym]||sym; const perp=(lastData&&lastData.cfg&&lastData.cfg.market==="futures")?".P":"";
  return "https://www.tradingview.com/chart/?symbol=MEXC:"+s+perp; }
// ---- Watchlist (saved in this browser via localStorage) --------------------
let WATCH=new Set();
try{ WATCH=new Set(JSON.parse(localStorage.getItem('mexcWatch')||'[]')); }catch(e){}
function saveWatch(){ try{ localStorage.setItem('mexcWatch', JSON.stringify([...WATCH])); }catch(e){} }
function toggleWatch(sym, ev){ if(ev){ ev.stopPropagation(); ev.preventDefault(); }
  if(WATCH.has(sym)) WATCH.delete(sym); else WATCH.add(sym);
  saveWatch();
  // refresh the star everywhere + the watchlist tab + its count
  document.querySelectorAll('.wstar[data-sym="'+sym+'"]').forEach(el=>{
    const on=WATCH.has(sym); el.classList.toggle('on',on); el.textContent=on?'★':'☆';
    el.title=on?'Remove from watchlist':'Add to watchlist'; });
  renderWatch(); loadWatch();
  const wc=document.getElementById('tabWatch'); if(wc) wc.textContent=`📌 Watchlist${WATCH.size?' ('+WATCH.size+')':''}`;
}
function watchStar(sym){ const on=WATCH.has(sym);
  return `<span class="wstar${on?' on':''}" data-sym="${sym}" title="${on?'Remove from watchlist':'Add to watchlist'}" onclick="toggleWatch('${sym}',event)">${on?'★':'☆'}</span>`; }
function goAnalyze(sym, ev){ if(ev){ ev.stopPropagation(); ev.preventDefault(); }
  const inp=document.getElementById('azInput'); if(inp){ inp.value=sym; } showTab('analyze'); analyze(); }
function analyzeFromWatch(sym){ goAnalyze(sym); }
// A little "analyse this coin" button for any row — opens the Analyze tab for it.
function analyzeBtn(sym){ return `<span class="azbtn" title="Analyze ${dispSym(sym)} — full trade plan" onclick="goAnalyze('${sym}',event)">⚲</span>`; }
// Look up the freshest row we have for a symbol across all scan lists (for price/bias).
function watchLookup(sym){
  const lists=[latest,flatest,clatest,blatest,xlatest,slatest,eelatest];
  let found=null; const on=[];
  const names=[[latest,'200-EMA reclaim'],[flatest,'Bull flag'],[clatest,'Narrow CPR'],[blatest,'Support bounce'],[xlatest,'Supertrend bounce'],[slatest,'Short'],[eelatest,'Early']];
  for(const [lst,nm] of names){ for(const h of (lst||[])){ if(h.symbol===sym){ if(!found) found=h; on.push(nm); break; } } }
  return {row:found, on};
}
// Full analyze data per watched coin, fetched on demand and cached.
let watchData={};   // sym -> {d, ts}
async function loadWatch(force){
  const syms=[...WATCH];
  for(const sym of syms){
    const c=watchData[sym];
    if(!force && c && (Date.now()-c.ts)<120000) continue;   // fresh enough
    try{ const r=await fetch("/analyze?symbol="+encodeURIComponent(sym)+"&interval=4h",{cache:"no-store"});
      const d=await r.json();
      if(!d.error){ watchData[sym]={d, ts:Date.now()}; renderWatch(); }
    }catch(e){}
  }
}
// The recommended LONG plan for a watched coin (uses the same engine as Analyze).
function watchRec(d){
  const dd=(d.plans&&d.plans.long)?Object.assign({},d,d.plans.long):d;
  const be=pickEntry(dd);
  const recE=be?be.level:(dd.retest_entry!=null?dd.retest_entry:(dd.optimal_entry!=null?dd.optimal_entry:dd.entry));
  const rstop=be?be.rs:recStop(dd,recE);
  const rtg=be?be.rt:recTargets(dd,recE,rstop?rstop.level:dd.sl_tight);
  const rec=(rtg&&rtg.primary)?rtg.primary:null;
  const grade=rec?(rec.rr>=3&&rec.p>=.45?'A+':rec.rr>=2.5&&rec.p>=.35?'A':rec.rr>=2&&rec.p>=.3?'B':'C'):'—';
  return {recE, rstop, rec, grade};
}
function setupsCell(d, on){
  const tags=[];
  if(d){ if(d.ema_reclaim) tags.push('200-EMA reclaim'); if(d.bull_flag) tags.push('Bull flag');
    if(d.support_bounce) tags.push('Support bounce'); }
  for(const nm of on){ if(!tags.includes(nm)) tags.push(nm); }
  const tfs=[['1h','1h'],['4h','4h'],['1d','1D'],['1w','1W']];
  const t=tags.length?tags.slice(0,3).join(', ')+(tags.length>3?` +${tags.length-3}`:''):'<span style="color:var(--dim2)">— no active scan setup —</span>';
  // Chart patterns across EVERY timeframe (1h / 4h / Daily / Weekly), not just 4h.
  let pats='';
  if(d&&d.patterns){ const parts=[];
    for(const [k,lbl] of tfs){ const a=d.patterns[k]||[]; if(a.length){
      const b=a[0].bias, c=b==='bullish'?'var(--accent)':b==='bearish'?'#f85149':'var(--dim)';
      parts.push(`<span style="color:${c}">${a[0].name}·${lbl}</span>`); } }
    if(parts.length) pats=`<div style="font-size:11px;margin-top:3px;color:var(--dim)">◈ ${parts.join(' · ')}</div>`;
  }
  // Multi-timeframe bias strip.
  let bstrip='';
  if(d&&d.tf_bias){ const sy={bullish:'▲',bearish:'▼',neutral:'–'},
        co={bullish:'var(--accent)',bearish:'#f85149',neutral:'var(--dim2)'}; const parts=[];
    for(const [k,lbl] of tfs){ const b=d.tf_bias[k]; if(b) parts.push(`<span style="color:${co[b]||'var(--dim2)'}">${lbl}${sy[b]||'–'}</span>`); }
    if(parts.length) bstrip=`<div style="font-size:11px;margin-top:3px;font-family:var(--mono);letter-spacing:.02em">${parts.join('  ')}</div>`;
  }
  return `<td style="text-align:left;white-space:normal;max-width:280px;line-height:1.4">${t}${pats}${bstrip}</td>`;
}
function renderWatch(){
  const tb=document.getElementById('wlrows'); if(!tb) return;
  const syms=[...WATCH].sort();
  document.getElementById('wlempty').style.display = syms.length? 'none':'block';
  document.getElementById('watchCount').textContent = syms.length? `${syms.length} coin(s)`:'';
  tb.innerHTML='';
  for(const sym of syms){
    const {row,on}=watchLookup(sym);
    const cache=watchData[sym]; const d=cache?cache.d:null;
    const price=(d&&d.live!=null)?d.live:(d?d.price:(row?(row.live!=null?row.live:row.price):null));
    const bias=d?(d.bias||'—'):(row?(row.bias||'—'):'—');
    const corr=d?d.btc_corr:(row?row.btc_corr:null);
    let entryC='<td><span style="color:var(--dim2)">loading…</span></td>', stopC='<td>—</td>', tpC='<td>—</td>';
    if(d){
      const R=watchRec(d);
      entryC=`<td>${R.recE!=null?fmtNum(R.recE):'—'}</td>`;
      stopC=R.rstop?`<td>${fmtNum(R.rstop.level)}<span class="rr">${R.rstop.atrx?R.rstop.atrx.toFixed(1)+'×':''}</span></td>`:'<td>—</td>';
      tpC=R.rec?`<td class="revtp">${fmtNum(R.rec.lvl)}<span class="rr">R${R.rec.rr.toFixed(1)}·${R.grade}</span></td>`
                :'<td><span style="color:var(--warn)">no ≥1.5 R:R</span></td>';
    }
    const tr=document.createElement('tr');
    tr.innerHTML =
      `<td class="sym">${watchStar(sym)}<a href="${tvLink(sym)}" target="_blank" rel="noopener">${dispSym(sym)}</a>${analyzeBtn(sym)}</td>`+
      `<td>${price!=null?fmtNum(price):'<span style="color:var(--dim)">—</span>'}</td>`+
      `<td><span class="biaspill2 b-${(bias||'').toLowerCase().replace(/[^a-z]/g,'')}">${bias}</span></td>`+
      setupsCell(d,on)+ entryC+ stopC+ tpC+ corrTd(corr)+
      `<td><span class="wlbtn" onclick="analyzeFromWatch('${sym}')">Analyze</span><span class="wlbtn" onclick="toggleWatch('${sym}',event)">Remove</span></td>`;
    tb.appendChild(tr);
  }
}
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
  s+=patPill(h.pattern, h.patterns_mtf);
  s+=tfBiasStrip(h.tf_bias);
  if(h.data_stale) s+='<span class="patbadge pat-bear" data-tip="MEXC\\'s kline history for this coin looks frozen (both futures and spot came back stale). The indicators may be out of date — confirm on the chart.">⚠ stale</span>';
  if(stopBreached(h)) s+='<span class="patbadge pat-bear" data-tip="The live price has already moved through this setup\\'s tight stop — the ~10-min-old levels are stale and the setup is effectively invalidated. Wait for a fresh signal.">⚠ stop hit</span>';
  return s;
}
// Compact per-timeframe bias strip (▲ bullish / ▼ bearish / – neutral on 4h·1D·1W).
function tfBiasStrip(tb){
  if(!tb) return '';
  const sym={bullish:'▲',bearish:'▼',neutral:'–'};
  const cls={bullish:'pat-bull',bearish:'pat-bear',neutral:'pat-neu'};
  let out='';
  for(const [k,lbl] of [['1h','1h'],['4h','4h'],['1d','1D'],['1w','1W']]){
    const b=tb[k]; if(!b) continue;
    out+=`<span class="tfbias ${cls[b]}" data-tip="Market-structure bias on the ${lbl} chart: ${b} (from swing highs/lows + CHoCH).">${lbl}${sym[b]}</span>`;
  }
  return out;
}
// Chart-formation pill — shows the most salient formation across 1h/4h/1D/1W and
// which timeframe it's on; the hover lists the pattern on every timeframe.
function patPill(p,mtf){
  if(!p||!p.name) return '';
  const cls = p.bias==='bullish'?'pat-bull':p.bias==='bearish'?'pat-bear':'pat-neu';
  const all=(mtf&&mtf.length)? mtf.map(x=>`${x.tf}: ${x.name}`).join(' · ') : '';
  const tip=`Chart formation — strongest is on the ${p.tf||'4h'}: ${p.name} (${p.bias}). ${(''+(p.note||''))}`
          + (all? `  ||  All timeframes → ${all}` : '');
  return `<span class="patbadge ${cls}" data-tip="${(''+tip).replace(/"/g,'&quot;')}">◈ ${p.name} · ${p.tf||'4h'}</span>`;
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
function esc(s){ return (''+s).replace(/"/g,'&quot;'); }
function tpCell(v,rr,basis,isEma){ if(v==null) return '<td>—</td>';
  const what = basis || 'an overhead resistance level (prior swing high).';
  const tip=`Take-profit at ${fmtNum(v)} — ${what} Grey number is the reward:risk to the tight stop${rr!=null?` (${(+rr).toFixed(2)})`:''}.`;
  const star=isEma?'<span class="emastar" data-tip="This target IS the 200-EMA reclaim — the mean-reversion objective that confirms the setup.">◎</span>':'';
  return `<td class="${isEma?'ematp':''}" data-tip="${esc(tip)}">${fmtNum(v)}${star}${rr!=null?`<span class="rr">R${(+rr).toFixed(1)}</span>`:''}</td>`; }
// Render TP1..TPn cells for a hit, each with its own per-coin basis + EMA flag.
function tpsCells(h,n){ let s=''; for(let i=1;i<=n;i++){ s+=tpCell(h['tp'+i],h['rr'+i],h['tp'+i+'_basis'],h['tp'+i+'_ema']); } return s; }
// The explicit 200-EMA (4h) reclaim target for Early setups — always visible.
function emaTargetCell(h){
  if(h.ema_target==null) return '<td>—</td>';
  const pct=(h.ema_target_pct!=null)?`${h.ema_target_pct>=0?'+':''}${h.ema_target_pct}%`:'';
  const tip=`200-EMA reclaim on the 4h chart at ${fmtNum(h.ema_target)} — the mean-reversion target that confirms this early setup${pct?` (${pct} away)`:''}. Grey = reward:risk to the tight stop${h.ema_target_rr!=null?` (${(+h.ema_target_rr).toFixed(2)})`:''}.`;
  return `<td class="ematp" data-tip="${esc(tip)}"><span class="emastar">◎</span>${fmtNum(h.ema_target)}${h.ema_target_rr!=null?`<span class="rr">R${(+h.ema_target_rr).toFixed(1)}</span>`:''}</td>`;
}
// The recommended realistic reversal target for an Early setup (best EV target).
function revTpCell(h){
  if(h.rev_tp==null) return '<td><span style="color:var(--dim)">—</span></td>';
  const tip=`Recommended reversal target: ${fmtNum(h.rev_tp)} — the best realistic reward:risk (R:R ${h.rev_tp_rr}) among the 200-EMA reclaim and overhead resistances, ${h.rev_tp_pct>=0?'+':''}${h.rev_tp_pct}% away. A proper reversal target, not just the nearest rung.`;
  return `<td class="revtp" data-tip="${esc(tip)}">🎯 ${fmtNum(h.rev_tp)}${h.rev_tp_rr!=null?`<span class="rr">R${(+h.rev_tp_rr).toFixed(1)}</span>`:''}</td>`;
}
// Stop cells driven by the per-coin basis the backend computed.
function slTightCell(h){ const b=h.sl_tight_basis||"The setup's immediate invalidation level, ATR-buffered."; return `<td data-tip="${esc('Tight stop at '+fmtNum(h.sl_tight)+' — '+b)}">${fmtNum(h.sl_tight)}</td>`; }
function slWideCell(h){ const b=h.sl_wide_basis||"A deeper structural level for more room, ATR-buffered."; return `<td data-tip="${esc('Wide stop at '+fmtNum(h.sl_wide)+' — '+b)}">${fmtNum(h.sl_wide)}</td>`; }
function biasPill(h){ return `<td><span class="biaspill2 b-${(h.bias||'').toLowerCase().replace(/[^a-z]/g,'')}">${h.bias||'—'}</span></td>`; }
function rvCell(v){ return v==null?'<td>—</td>'
  :`<td${(+v)>=1.5?' style="color:var(--accent);font-weight:600"':''}>${(+v).toFixed(2)}×</td>`; }
function showTab(which){
  activeTab=which;
  for(const [t,v] of [["setups","Setups"],["flags","Flags"],["cpr","Cpr"],["bounce","Bounce"],["stb","Stb"],["shorts","Shorts"],["early","Early"],["top","Top"],["watch","Watch"],["analyze","Analyze"],["info","Info"]]){
    document.getElementById("tab"+v).classList.toggle("active", t===which);
    document.getElementById("view"+v).classList.toggle("active", t===which);
  }
  if(which==="watch"){ renderWatch(); loadWatch(); }
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
    azLast = d.error ? null : d; azSide = null;   // reset perspective to the coin's own lean
    box.innerHTML = d.error ? '<div class="azerr">'+d.error+'</div>' : azCard(d);
  }catch(e){ box.innerHTML='<div class="azerr">Analysis failed — try again.</div>'; }
  btn.disabled=false; btn.textContent=t;
}
// Long/Short perspective toggle for the Analyze card. null = the coin's own lean.
let azLast=null, azSide=null;
function setAzSide(s){
  azSide = (azSide===s) ? null : s;   // click the active side again to snap back to auto
  if(azLast) document.getElementById("azResult").innerHTML=azCard(azLast);
}
function stopsSection(list,side,recLevel){
  if(!list||!list.length) return '';
  const isR=(lvl)=> recLevel!=null && lvl!=null && Math.abs(lvl-recLevel)/(recLevel||1) < 0.004;
  const chips=list.map(s=>`<span class="ladchip${isR(s.level)?' ladrecstop':''}" data-tip="${esc((''+s.basis)+' — a '+(side==='short'?'short':'long')+' stop here risks '+Math.abs(s.pct).toFixed(1)+'% from the current price.'+(isR(s.level)?' 🛑 Recommended stop — the nearest real structure beyond the noise (best balance of safety and reward:risk for this chart).':''))}">${isR(s.level)?'🛑 ':''}${fmtNum(s.level)} <span class="rr">${s.pct>=0?'+':''}${s.pct}% · ${s.basis}</span></span>`).join('');
  return `<div class="azsec" data-tip="Candidate stop-loss levels, each anchored to a specific structure (swing high/low, the Supertrend line, the 200 EMA, or a higher-timeframe support/resistance) and buffered by a fraction of ATR. Pick the one that fits your risk and thesis — tighter = smaller risk but easier to get wicked out; wider = more room. 🛑 = recommended.">Stop-loss options <span class="azsub">— ${side==='short'?'above':'below'} price, each with what it\\'s based on · 🛑 = recommended</span></div>`
    + `<div class="azladder">${chips}</div>`;
}
// P(a stop at `riskATR` ATRs away survives normal noise without being wicked
// out before the target). Near the wick range (~<1x ATR) it's very likely to be
// tripped; by ~2.5-3x ATR it's comfortably clear. A smooth S-curve.
function surviveProb(riskATR){
  if(riskATR==null) riskATR=1.5;
  return Math.max(0.05, Math.min(0.96, 1/(1+Math.exp(-(riskATR-1.5)*1.7))));
}
// Evaluate an ENTRY: from the actual stop-loss OPTIONS on the menu, choose the
// stop that maximises TRADE QUALITY = EV(best target from this entry over this
// stop) × P(the stop survives normal noise). This is the whole game: a tight
// stop gives a fat R:R but gets wicked out (low survival); a wide stop survives
// but shrinks R:R — the winner is the sweet spot in between, decided per chart
// by its own volatility. Real structure gets a small nudge over a blunt ATR
// stop, and in a clean trend the trend line (Supertrend/EMA) gets a small nudge
// too — nudges, not mandates. Returns {stop, rtg, sc} or null. The chosen stop
// is always one of the menu chips, so it can be highlighted there.
function evalEntry(d, E){
  const side=(d.side||'long'), atr=d.atr_pct||0, list=(d.stop_levels||[]);
  if(E==null) return null;
  const onSide=(lvl)=> side==='short'? lvl>E : lvl<E;
  const distE=(lvl)=> Math.abs((lvl-E)/E)*100;
  const atrx=(lvl)=> atr? distE(lvl)/atr : distE(lvl)/2.5;
  const isVol=(b)=> /ATR/i.test(b||'');
  const isTrend=(b)=> /supertrend/i.test(b||'') || /200 EMA/i.test(b||'');
  const trending = side==='short'? d.trend==='down' : d.trend==='up';
  const onSideAll=list.filter(s=> onSide(s.level));
  if(!onSideAll.length) return null;
  const band=onSideAll.filter(s=> atrx(s.level)>=0.9 && atrx(s.level)<=6);
  const pool=band.length?band:onSideAll;
  const mk=(s)=>{ const ax=atrx(s.level);
    const note = (isTrend(s.basis)&&trending) ? 'the trend line that has carried this move — a close through it flips the trend, the true invalidation here'
      : (ax>3.5 ? 'a wider structural level that comfortably clears the wick/noise range' : 'just below real structure, beyond the recent wick range');
    return {level:s.level, basis:s.basis, pct:distE(s.level), atrx:ax, note}; };
  let best=null;
  for(const s of pool){
    const rt=recTargets(d, E, s.level);
    const ev=(rt&&rt.primary)? rt.primary.ev : 0;
    if(ev<=0) continue;                                  // must yield a >=1.5 R:R trade
    let sc=ev*surviveProb(atrx(s.level));
    if(!isVol(s.basis)) sc*=1.08;                        // real structure over a blunt ATR stop
    if(trending && isTrend(s.basis)) sc*=1.08;           // trend line nudge in a trend
    if(!best || sc>best.sc) best={sc, rtg:rt, stop:mk(s)};
  }
  if(best) return best;
  // No stop produced a >=1.5 R:R trade — return the nearest sensible structural
  // level (beyond ~1.2x ATR) for reference; the TP logic will flag "no trade".
  const struct=pool.filter(s=> !isVol(s.basis) && atrx(s.level)>=1.2);
  const refPool=struct.length?struct:pool;
  const s=refPool.reduce((a,b)=> atrx(a.level)<=atrx(b.level)?a:b);
  return {sc:0, rtg:recTargets(d, E, s.level), stop:mk(s)};
}
// Thin wrapper kept for callers that just want the stop for a given entry.
function recStop(d, entry){
  const E=(entry!=null)?entry:(d.retest_entry!=null?d.retest_entry:(d.optimal_entry!=null?d.optimal_entry:d.price));
  const e=evalEntry(d, E); return e? e.stop : null;
}
// How much momentum / trend / volume backs a SUSTAINED move in `side` — a 0..1
// "potential" score. High potential legitimately stretches how far a target can
// realistically run (a far level isn't unrealistic if the trend is strong).
function potentialScore(d, side){
  let s=0.5; const long=side!=='short';
  const trend=d.trend, rsi=d.rsi, vt=(d.vol_trend||''), pr=(d.pressure||'').toLowerCase(),
        struct=(d.structure||'').toLowerCase();
  if(long){ if(trend==='up') s+=0.15; if(trend==='down') s-=0.12;
            if(struct.indexOf('up')>=0) s+=0.08; if(struct.indexOf('down')>=0) s-=0.06; }
  else    { if(trend==='down') s+=0.15; if(trend==='up') s-=0.12;
            if(struct.indexOf('down')>=0) s+=0.08; if(struct.indexOf('up')>=0) s-=0.06; }
  if(rsi!=null){ if(long){ if(rsi>55&&rsi<72) s+=0.10; else if(rsi>=72) s+=0.02; else if(rsi<40) s-=0.03; }
                 else    { if(rsi<45&&rsi>28) s+=0.10; else if(rsi<=28) s+=0.02; else if(rsi>60) s-=0.03; } }
  if(vt==='rising'){ if(long&&pr.indexOf('buyer')>=0) s+=0.10; if(!long&&pr.indexOf('seller')>=0) s+=0.10; }
  if(long&&pr.indexOf('buyer')>=0) s+=0.04; if(!long&&pr.indexOf('seller')>=0) s+=0.04;
  if(d.choch==='bullish'&&long) s+=0.05; if(d.choch==='bearish'&&!long) s+=0.05;
  if(d.btc_corr!=null && d.btc_corr>=0.85) s-=0.03;   // "breakout" may just be BTC beta
  return Math.max(0.05, Math.min(1, s));
}
// Reachability of a target: decays with distance measured in ATR units, but the
// "reachable horizon" widens with potential — so strong setups keep far targets
// viable, weak ones don't. Not a true probability, a sensible 0.05..0.98 proxy.
function reachProb(distATR, pot){
  if(distATR==null) distATR=4;
  const horizon = 3 + (pot||0.5)*9;    // ATRs comfortably reachable: ~3 (weak) .. ~12 (strong)
  return Math.max(0.05, Math.min(0.98, 1/(1+Math.pow(distATR/horizon, 1.8))));
}
// Rank the profit targets by EXPECTED VALUE (reward:risk x reach probability),
// not raw R:R — so we don't recommend a far pie-in-the-sky target just because
// its ratio is big. Returns a primary (best EV, R:R>=1.2) plus a scale-out plan:
// Secure (nearest bankable), Base (=primary), Stretch (ambitious, for strong runs).
function recTargets(d, entry, stopLevel){
  const side=(d.side||'long'), long=side!=='short', price=d.price, atr=d.atr_pct||0;
  const risk=(entry!=null&&stopLevel!=null)?Math.abs(entry-stopLevel):null;
  if(risk==null||risk===0) return null;
  const pot=potentialScore(d, side);
  const cand=[]; const seen=[];
  const push=(lvl,kind)=>{
    if(lvl==null) return; if(long? lvl<=entry : lvl>=entry) return;
    const move=Math.abs((lvl-entry)/entry); if(move<0.015) return;
    if(seen.some(x=>Math.abs(x-lvl)/lvl<0.004)) return; seen.push(lvl);
    const rr=Math.min(Math.abs(lvl-entry)/risk,8);
    const dATR=atr? (move*100)/atr : null;
    const p=reachProb(dATR, pot);
    cand.push({lvl,kind,move,rr,p,dATR,ev:rr*p});
  };
  if(d.target_ladder&&d.target_ladder.length){ for(const t of d.target_ladder) push(t.level,t.kind); }
  else { for(let i=1;i<=5;i++) push(d['tp'+i], d['tp'+i+'_basis']); }
  if(!cand.length) return null;
  cand.sort((a,b)=>a.move-b.move);
  const MINRR=1.5;                       // in crypto, a sub-1.5 R:R "trade" is not worth taking
  const bestRR=cand.reduce((m,c)=>Math.max(m,c.rr),0);
  const eligible=cand.filter(c=>c.rr>=MINRR);
  // Recommended = best expected value AMONG targets that clear the 1.5 R:R floor.
  // If nothing clears it, there is NO recommended trade (don't manufacture one).
  const primary=eligible.length? eligible.reduce((a,b)=> a.ev>=b.ev?a:b) : null;
  const secure=primary? (cand.filter(c=>c.rr>=MINRR&&c.move<=primary.move).sort((a,b)=>a.move-b.move)[0]||primary) : null;
  const farPool=primary? cand.filter(c=>c.rr>=2.5 && c.move>primary.move && c.p>=0.18) : [];
  const stretch=farPool.length?farPool[farPool.length-1]:null;
  return {primary, secure, stretch, pot, bestRR, minRR:MINRR, all:cand};
}
// Pick the best value-area ENTRY. A long doesn't have to buy near current price —
// if price is extended or correcting, a deeper pullback (a support / EMA /
// Supertrend retest) can be the smarter fill; a short can sell into a higher
// rally. We score each candidate entry by the trade it produces (expected value
// of the best >=1.5 R:R target from there) with a mild preference for fills that
// are more likely to actually print (closer, in ATR terms).
// Timeframe strength ladder — a level confirmed higher up is more reliable and
// gets hit first on a pullback. Used to (a) nudge the entry score toward stronger
// TFs and (b) build the "shield": price should turn at the nearest strong HTF
// level before reaching anything deeper.
const TFRANK={'1h':1,'4h':2,'1d':3,'1w':4};
const TFNAME={'1h':'1h','4h':'4h','1d':'Daily','1w':'Weekly'};
function pickEntry(d){
  const side=(d.side||'long'), long=side!=='short', price=d.price, atr=d.atr_pct||0;
  if(price==null) return null;
  const viewTf=d.interval||'4h', viewRank=TFRANK[viewTf]||2;
  const cands=[];
  const add=(lvl,basis,tf)=>{ if(lvl==null||lvl<=0) return; if(long? lvl>=price : lvl<=price) return;
    if(cands.some(c=>Math.abs(c.level-lvl)/lvl<0.003)) return; cands.push({level:lvl,basis,tf:tf||viewTf}); };
  add(d.retest_entry,'retest level',viewTf); add(d.optimal_entry,'optimal fill',viewTf);
  // Cross-timeframe swing levels — each from its own chart (1h / 4h / Daily / Weekly).
  const tl=d.tf_levels||{};
  ['1h','4h','1d','1w'].forEach(tf=>{ const L=tl[tf]; if(!L) return;
    if(long){ if(L.sup!=null) add(L.sup, TFNAME[tf]+' swing-low support', tf); }
    else    { if(L.res!=null) add(L.res, TFNAME[tf]+' swing-high resistance', tf); } });
  if(long){ (d.supports||[]).slice(0,3).forEach(s=>add(s,'swing-low support',viewTf));
            add(d.sup_1d,'Daily swing-low support','1d'); add(d.sup_1w,'Weekly swing-low support','1w');
            if(d.ema!=null&&d.ema<price) add(d.ema*1.002,'200-EMA retest',viewTf);
            if(d.supertrend!=null&&d.supertrend_role==='support'&&d.supertrend<price) add(d.supertrend*1.003,'Supertrend retest',viewTf); }
  else    { (d.resistances||[]).slice(0,3).forEach(s=>add(s,'swing-high resistance',viewTf));
            add(d.res_1d,'Daily swing-high resistance','1d'); add(d.res_1w,'Weekly swing-high resistance','1w');
            if(d.ema!=null&&d.ema>price) add(d.ema*0.998,'200-EMA retest',viewTf);
            if(d.supertrend!=null&&d.supertrend_role==='resistance'&&d.supertrend>price) add(d.supertrend*0.997,'Supertrend retest',viewTf); }
  if(!cands.length) return null;
  // The SHIELD: the nearest STRONG level (a TF at least as high as the one being
  // viewed, and Daily+ counts everywhere) that price hits first on the pullback.
  // Price tends to turn there, so a deeper entry beyond it is unlikely to fill —
  // this is exactly "if the Daily support is higher than the 4h one, take the
  // Daily", generalised across every timeframe.
  let shield=null, shieldTf=null;
  for(const c of cands){ const rank=TFRANK[c.tf]||2;
    if(rank<Math.max(3,viewRank)) continue;                   // Daily/Weekly (or >= the viewed TF if viewing high)
    if(long? (shield==null||c.level>shield) : (shield==null||c.level<shield)){ shield=c.level; shieldTf=c.tf; } }
  const pot=potentialScore(d, side);
  let best=null;
  for(const c of cands){
    const ee=evalEntry(d, c.level);                           // its best stop + targets (survival-weighted)
    if(!ee || !ee.rtg || !ee.rtg.primary) continue;           // must yield a real >=1.5 R:R trade w/ a menu stop
    const distATR=atr? Math.abs((c.level-price)/price*100)/atr : 0;
    if(distATR > 8) continue;                                 // absurdly far — not part of this move
    const rank=TFRANK[c.tf]||2;
    // A DEEP pullback can be the right entry — in a strong trend, or at a major
    // level (200 EMA / HTF support / Supertrend), waiting for it is smart, not
    // wrong. So instead of a hard depth cap, widen the "realistic fill" horizon
    // with potential and for major levels; shallow entries still win when the
    // setup is weak or the level is minor.
    const major=/200[- ]?EMA|Daily|Weekly|Supertrend/i.test(c.basis||'') || rank>=3;
    const horizon=2.5 + pot*3.5 + (major?1.5:0);             // ATRs of pullback that's realistic to wait for
    const fill=1/(1+Math.pow(distATR/horizon,1.8));
    // Higher-TF levels are more reliable → a modest score boost (never enough to
    // override a genuinely better R:R, just to break ties toward the stronger TF).
    const tfBoost=1 + 0.06*(rank-2);
    // Beyond the shield: this entry sits past a strong HTF level price should turn
    // at first, so it probably never fills — penalise by how far past (in ATR).
    let shieldPen=1;
    if(shield!=null){ const past = long ? (c.level < shield*0.999) : (c.level > shield*1.001);
      if(past){ const beyond=atr? Math.abs((c.level-shield)/price*100)/atr : 0;
        shieldPen=Math.max(0.12, 1/(1+Math.pow(beyond/0.75,1.6))); } }
    const score=ee.rtg.primary.ev*surviveProb(ee.stop.atrx)*(0.35+0.65*fill)*tfBoost*shieldPen;
    if(!best || score>best.score) best={level:c.level, basis:c.basis, tf:c.tf, score, rt:ee.rtg, rs:ee.stop,
        distPct:Math.abs((c.level-price)/price*100), distATR,
        shieldTf:(shield!=null&&Math.abs(c.level-shield)/shield<0.003)?shieldTf:null};
  }
  return (best&&best.score>0)?best:null;
}
function patternsSection(p){
  if(!p) return '';
  const one=x=>`<span class="patbadge ${x.bias==='bullish'?'pat-bull':x.bias==='bearish'?'pat-bear':'pat-neu'}" data-tip="${(''+(x.note||'')).replace(/"/g,'&quot;')}">${x.name}</span>`;
  const row=(k,lbl)=>{ const l=(p[k]||[]);
    const chips=l.length? l.map(one).join(' ') : '<span style="color:var(--dim)">— none clear —</span>';
    return `<div class="patrow"><span class="pattf">${lbl}</span>${chips}</div>`; };
  return `<div class="azsec" data-tip="Chart formations detected from swing-pivot geometry on each timeframe — wedges, triangles, pennants, channels, flags, double tops/bottoms. Green = bullish, red = bearish, grey = neutral (trade the break).">Chart patterns <span class="azsub">— formations on 1h / 4h / Daily / Weekly (hover each for what it means)</span></div>`
    + `<div class="patwrap">${row('1h','1h')}${row('4h','4h')}${row('1d','Daily')}${row('1w','Weekly')}</div>`;
}
function tfBiasSection(tb){
  if(!tb) return '';
  const one=(k,lbl)=>{ const b=tb[k]; if(!b) return '';
    const c=b==='bullish'?'pat-bull':b==='bearish'?'pat-bear':'pat-neu';
    const sym=b==='bullish'?'▲':b==='bearish'?'▼':'–';
    return `<span class="patbadge ${c}" data-tip="Market-structure bias on the ${lbl} chart (from swing highs/lows + CHoCH).">${lbl} ${b} ${sym}</span>`; };
  return `<div class="azsec" data-tip="The trend bias on each timeframe from market structure. Alignment across timeframes (all green or all red) is a stronger signal than a single timeframe.">Bias by timeframe <span class="azsub">— 1h / 4h / Daily / Weekly</span></div>`
    + `<div class="patrow" style="margin:8px 0 2px">${one('1h','1h')}${one('4h','4h')}${one('1d','Daily')}${one('1w','Weekly')}</div>`;
}
function azCard(d0){
  // Perspective: default to the coin's own lean (auto_side), or the user's
  // Long/Short toggle. Merge the chosen plan's entries/stops/targets over the
  // side-independent read so the whole trade plan reflects that perspective.
  const auto=d0.auto_side||d0.side||'long';
  const activeSide=(azSide==='long'||azSide==='short')?azSide:auto;
  const d=(d0.plans&&d0.plans[activeSide])?Object.assign({},d0,d0.plans[activeSide]):d0;
  const cell=(k,v,tip)=>`<div class="azcell"${tip?` data-tip="${tip}"`:''}><div class="k">${k}</div><div class="v">${v}</div></div>`;
  const pct=(v,sign)=> d.price&&v!=null? ` <span class="rr">${sign}${(Math.abs((v-d.price)/d.price)*100).toFixed(1)}%</span>`:'';
  const tp=(v,rr)=> v==null?'—':`${fmtNum(v)}${rr!=null?` <span class="rr">R:R ${(+rr).toFixed(2)}</span>`:''}`;
  const notes=(d.notes||[]).map(n=>`<li>${n}</li>`).join("");
  // --- Recommended trade. Pick the best value-area ENTRY (may be a deeper
  // pullback for a long / higher rally for a short, not just the nearest level),
  // then the context-aware stop and the expected-value take-profit.
  const be=pickEntry(d);
  let recE, rstop, rtg;
  if(be){ recE=be.level; rstop=be.rs; rtg=be.rt; }
  else { recE=(d.retest_entry!=null?d.retest_entry:(d.optimal_entry!=null?d.optimal_entry:d.entry));
         const ee=evalEntry(d, recE);
         rstop=ee?ee.stop:null; rtg=ee?ee.rtg:recTargets(d, recE, rstop?rstop.level:d.sl_tight); }
  // When the winning entry comes from a HIGHER timeframe than the one being viewed,
  // say so — that's the whole "take the Daily support over the deeper 4h dip" idea.
  const viewRankAz=TFRANK[d.interval||'4h']||2;
  const beHigherTf=(be&&be.tf&&(TFRANK[be.tf]||2)>viewRankAz);
  const crossTfTag=beHigherTf?` <span class="tfsrc" data-tip="This entry level is the ${TFNAME[be.tf]} chart's support — a higher, stronger timeframe than the ${d.interval||'4h'} you're viewing. Price should turn there first, so it's the smarter fill.">${TFNAME[be.tf]}</span>`:'';
  const crossTfNote=beHigherTf?` · <b style="color:var(--accent)">from the ${TFNAME[be.tf]} chart</b> — a stronger level price should reach first, so it beats waiting for a deeper ${d.interval||'4h'} dip that may never fill`:'';
  const rec=(rtg&&rtg.primary)?{tp:rtg.primary.lvl, rr:rtg.primary.rr, move:rtg.primary.move, p:rtg.primary.p}:{tp:null};
  const isRec=(lvl)=> rec.tp!=null && lvl!=null && Math.abs(lvl-rec.tp)/(rec.tp||1) < 0.004;
  const sgn=(d.side||'long')==='short'?'+':'−';         // stop/entry sign relative to price for this side
  // Distance of a level in ATR units (the honest "how tight/far" for THIS coin).
  const atrxOf=(lvl,ref)=>{ const r=(ref!=null)?ref:d.price; if(lvl==null||r==null||!d.atr_pct) return null;
    return Math.abs((lvl-r)/r*100)/d.atr_pct; };
  const slInfo=(lvl)=>{ const p=(d.price&&lvl!=null)?Math.abs((lvl-d.price)/d.price*100):null; const a=atrxOf(lvl,d.price);
    return (p!=null?`${p.toFixed(1)}%`:'')+(a!=null?` · ${a.toFixed(1)}×ATR`:''); };
  const planGrade=(rec.tp==null)?'—':(rec.rr>=3&&rec.p>=0.45?'A+':rec.rr>=2.5&&rec.p>=0.35?'A':rec.rr>=2&&rec.p>=0.3?'B':'C');
  // R:R to the recommended (base) target over the recommended stop, from ANY
  // chosen entry — lets each entry cell show its own R:R and lets us compare
  // entering now at market vs waiting for the recommended pullback.
  const rrAt=(en)=>{ if(en==null||rec.tp==null||!rstop) return null; const stop=rstop.level;
    const long=(d.side||'long')!=='short';
    if(long?(rec.tp<=en||stop>=en):(rec.tp>=en||stop<=en)) return null;
    return Math.min(Math.abs(rec.tp-en)/Math.abs(en-stop),8); };
  const rrTag=(en)=>{ const r=rrAt(en); return r!=null?` <span class="rr">R:R ${r.toFixed(2)}</span>`:''; };
  const cmpE=(d.live!=null?d.live:d.price);   // current market price ("enter now")
  // Reward:risk of a target measured from the retest entry over a given stop
  // (side-aware, only counts targets on the right side, capped at 8:1).
  const rrOn=(tpv,stop)=>{ if(tpv==null||recE==null||stop==null||recE===stop) return null;
    const long=(d.side||'long')!=='short'; if(long? tpv<=recE : tpv>=recE) return null;
    return Math.min(Math.abs(tpv-recE)/Math.abs(recE-stop),8); };
  const recRR=(tpv)=> rrOn(tpv, rstop?rstop.level:d.sl_tight);
  const tightRR=(tpv)=> rrOn(tpv, d.sl_tight);
  // R:R if you enter NOW at the current market price (over the recommended stop).
  const cmpRR=(tpv)=>{ const stop=rstop?rstop.level:d.sl_tight; if(tpv==null||cmpE==null||stop==null||cmpE===stop) return null;
    const long=(d.side||'long')!=='short'; if(long? tpv<=cmpE : tpv>=cmpE) return null;
    return Math.min(Math.abs(tpv-cmpE)/Math.abs(cmpE-stop),8); };
  // A trade-plan TP cell: R:R to the RECOMMENDED stop (what we advise), tight-stop
  // R:R in the hover. ◎ flags the 200-EMA reclaim target.
  const aztp=(dd,n)=>{
    const v=dd['tp'+n], basis=dd['tp'+n+'_basis'], ema=dd['tp'+n+'_ema'];
    const side=(d.side||'long')==='short';
    const rrR=recRR(v), rrT=tightRR(v);
    const fb=side?"The next structural support level below.":"The next structural resistance level above.";
    const tip=esc((basis?('Based on: '+basis):fb)+` R:R ${rrR!=null?rrR.toFixed(2):'—'} to the recommended stop`+(rrT!=null?`, ${rrT.toFixed(2)} to the tight stop`:'')+'.');
    const lbl='TP'+n+(ema?' <span class="emastar">◎</span>':'');
    const val=v==null?'—':`${fmtNum(v)}${rrR!=null?` <span class="rr">R:R ${rrR.toFixed(2)}</span>`:''}`;
    return `<div class="azcell${ema?' ematp':''}" data-tip="${tip}"><div class="k">${lbl}</div><div class="v">${val}</div></div>`;
  };
  return `<div class="azcard">
    <div class="azhead">
      <span class="sym">${watchStar(d.symbol)}${dispSym(d.symbol)}</span>
      <span style="color:var(--dim);font-size:12px;border:1px solid var(--line);border-radius:6px;padding:1px 7px">${(d.interval||'4h')} chart</span>
      <span class="dirpill dir-${(d.direction||'neutral').toLowerCase()}" data-tip="${d.dir_reason||''}">${(d.direction||'—').toUpperCase()}${d.direction==='Long'?' ▲':d.direction==='Short'?' ▼':''}</span>
      <span class="biaspill bias-${d.bias}" data-tip="${d.bias_reason||''}">${d.bias.toUpperCase()}</span>
      <span>${fmtNum(d.price)}</span>
      <span style="color:var(--dim)">EMA200 ${fmtNum(d.ema)} · ${d.pct_vs_ema>=0?'+':''}${d.pct_vs_ema}% · trend ${d.trend}</span>
      <a href="${tvLink(d.symbol)}" target="_blank" rel="noopener">open chart ↗</a>
      ${d0.plans?`<span class="sidetog" data-tip="Switch the trade plan below between a LONG and a SHORT perspective. You can plan a reversal/counter-trend entry even when the coin's own lean is the other way. Click the active side again to snap back to the auto lean.">
        <button class="${activeSide==='long'?'on long':''}" onclick="setAzSide('long')">LONG</button>
        <button class="${activeSide==='short'?'on short':''}" onclick="setAzSide('short')">SHORT</button>
      </span>`:''}
    </div>
    ${(d0.plans&&activeSide!==auto)?`<div class="sidenote">Showing the <b>${activeSide.toUpperCase()}</b> perspective — a ${activeSide==='long'?'reversal/counter-trend long':'counter-trend short'}. The coin's own lean is <b>${auto.toUpperCase()}</b>, so treat this as the plan <i>if</i> it turns; the entries, stops, targets and R:R below are all for a ${activeSide}.</div>`:''}
    <div class="azverdict ${rec.tp!=null?((d.side||'long')==='short'?'short':'long'):'none'}" data-tip="The bottom line for this coin, at a glance. Everything below is the detail behind it.">
      ${rec.tp!=null?`
        <span class="vbadge v-${(d.side||'long')==='short'?'short':'long'}">${(d.side||'long')==='short'?'SHORT ▼':'LONG ▲'}</span>
        <span class="vgrade">Grade ${planGrade}</span><span class="vsep"></span>
        <span class="vitem"><i>Entry</i>${fmtNum(recE)}${crossTfTag}</span>
        <span class="vitem"><i>Stop</i>${rstop?fmtNum(rstop.level):'—'}${rstop&&rstop.atrx?` <span style="color:var(--dim2)">${rstop.atrx.toFixed(1)}×</span>`:''}</span>
        <span class="vitem"><i>Target</i>${fmtNum(rec.tp)}</span>
        <span class="vitem"><i>R:R</i><b>${rec.rr.toFixed(2)}</b></span>
        <span class="vitem"><i>Reach</i>${Math.round(rec.p*100)}%</span>`
      :`<span class="vbadge v-none">⛔ NO TRADE</span>
        <span class="vitem" style="font-family:'Inter'">Best realistic R:R is only <b>${rtg?rtg.bestRR.toFixed(2):'—'}</b> — under the 1.5 floor. Wait for a better entry or setup.</span>`}
    </div>
    <div class="aztags">
      <span class="aztag ${d.ema_reclaim?'on':''}">200-EMA reclaim${d.ema_reclaim?' · '+d.ema_reclaim_score:''}</span>
      <span class="aztag ${d.bull_flag?'on':''}">Bull flag${d.bull_flag?' · '+d.bull_flag_score:''}</span>
      <span class="aztag ${d.support_bounce?'on':''}" ${d.support_bounce?`data-tip="Flagged by clustering ${d.support_bounce_tf} ${d.support_bounce_method||'swing-low pivot'} levels (tested ${d.support_bounce_touches||'?'}× ). The support is the ${d.support_bounce_method||'swing-low pivot zone'} at ${fmtNum(d.support_bounce_support)}."`:''}>Support bounce${d.support_bounce?` · off ${d.support_bounce_tf} ${d.support_bounce_method||'swing-low'} ${fmtNum(d.support_bounce_support)} (${d.support_bounce_touches||'?'}×) · score `+d.support_bounce_score:''}</span>
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
      ${cell("Supports (distance)", (d.supports||[]).slice(0,3).map(v=>fmtNum(v)+pct(v,'-')).join(' · ')||'—', "Based on: swing-low pivots — prior candle lows the market previously bounced from — on this timeframe, nearest first, with the % below current price.")}
      ${cell("Resistances (distance)", (d.resistances||[]).slice(0,3).map(v=>fmtNum(v)+pct(v,'+')).join(' · ')||'—', "Based on: swing-high pivots — prior candle peaks that previously capped price — on this timeframe, nearest first, with the % above current price.")}
      ${cell("Next support 4h·1D·1W (drawdown)", [d.sup_4h,d.sup_1d,d.sup_1w].map(v=>v==null?'—':fmtNum(v)+pct(v,'-')).join(' · '), "Based on: the nearest swing-low pivot on the 4h, Daily and Weekly charts (each timeframe aggregated separately) — your multi-timeframe safety-net levels — with the % drawdown to each.")}
      ${cell("Next resistance 4h·1D·1W (upside)", [d.res_4h,d.res_1d,d.res_1w].map(v=>v==null?'—':fmtNum(v)+pct(v,'+')).join(' · '), "Based on: the nearest swing-high pivot on the 4h, Daily and Weekly charts — likely ceilings — with the % upside to each.")}
      ${cell("Dist. from 200 EMA (4h·1D·1W)", `4h ${d.pct_vs_ema>=0?'+':''}${d.pct_vs_ema}% · 1D ${d.dist_ema_1d==null?'—':(d.dist_ema_1d>=0?'+':'')+d.dist_ema_1d+'%'} · 1W ${d.dist_ema_1w==null?'—':(d.dist_ema_1w>=0?'+':'')+d.dist_ema_1w+'%'}`, "How far price sits above/below the 200 EMA on each timeframe. Above on all three = a strong multi-timeframe uptrend regime. '—' = not enough history for that EMA.")}
    </div>
    ${tfBiasSection(d.tf_bias)}
    ${patternsSection(d.patterns)}
    <div class="azsec">Trade plan <span class="azsub">${(d.side||'long')==='short'?'SHORT — sell into strength, stops ABOVE, targets BELOW':'LONG — buy the dip, stops BELOW, targets ABOVE'} · entry may be a patient pullback · only setups clearing 1.5:1 R:R are recommended</span></div>
    <div class="azgrid">
      ${cell("Entry (now · CMP)", fmtNum(cmpE)+rrTag(cmpE), "Entering NOW at the current market price. The R:R shown is to the recommended (base) target over the recommended stop from here — usually a bit worse than waiting for the pullback, because you're paying up. Good when you don't want to risk missing the move; otherwise wait for the 🎯 recommended entry.")}
      ${cell("Retest entry", fmtNum(d.retest_entry!=null?d.retest_entry:d.optimal_entry)+rrTag(d.retest_entry!=null?d.retest_entry:d.optimal_entry), (d.side||'long')==='short'?"A proper pullback fill — wait for a rally back UP to the nearest swing-high / EMA / Supertrend ABOVE price and short the retest, rather than chasing the drop. R:R shown is from here.":"A proper pullback fill — wait for a dip DOWN to the nearest swing-low support / reclaimed EMA / Supertrend BELOW price and buy the retest, rather than chasing the candle. R:R shown is from here.")}
      ${cell("Optimal entry", fmtNum(d.optimal_entry)+rrTag(d.optimal_entry), (d.side||'long')==='short'?"Based on: the nearest swing-high resistance — a better short fill is to sell into it. R:R shown is from here.":"Based on: the nearest swing-low support / reclaimed 200 EMA — a lower-risk long fill. R:R shown is from here.")}
      ${cell("SL near"+(atrxOf(d.sl_tight)!=null?` <span class="rr">${slInfo(d.sl_tight)}</span>`:''), fmtNum(d.sl_tight), esc("The nearer of the two structural stops. On a high-timeframe / high-volatility chart even the 'near' stop can be a big % — what matters is the ×ATR distance shown (≈1× ATR is genuinely tight, 3×+ is wide). "+(d.sl_tight_basis||"")))}
      ${cell("SL deep"+(atrxOf(d.sl_wide)!=null?` <span class="rr">${slInfo(d.sl_wide)}</span>`:''), fmtNum(d.sl_wide), esc("The deeper structural stop — more room, larger risk per unit. "+(d.sl_wide_basis||"")))}
      ${aztp(d,1)}
      ${aztp(d,2)}
      ${aztp(d,3)}
      ${aztp(d,4)}
      ${aztp(d,5)}
    </div>
    ${be?`<div class="azrec" data-tip="The smartest place to get IN — not necessarily near the current price. If price is extended or correcting, a deeper pullback (a support / EMA / Supertrend retest) makes a better, more realistic trade; a short can wait to sell into a higher rally. Chosen to maximise the resulting reward:risk while staying a fill that's likely to actually print. Based on: ${esc(be.basis)}.">🎯 Recommended entry: <b>${fmtNum(recE)}</b> <span class="rr">${sgn}${be.distPct.toFixed(1)}%${be.distATR?` · ${be.distATR.toFixed(1)}×ATR`:''} ${(d.side||'long')==='short'?'above':'below'} price</span> <span style="color:var(--dim)">— ${esc(be.basis)}${crossTfNote}${(be.distATR&&be.distATR>2)?' · patient fill — wait for it, don\\'t chase':''}</span></div>`:''}
    ${rstop?`<div class="azrec azstop" data-tip="The recommended stop, judged for THIS chart — not a fixed % or a blanket 'go wide'. It sits just beyond the nearest real level that would invalidate the setup (swing low, Supertrend, EMA, HTF support), once clear of noise (≥ max(1.5%, 1.1× ATR)). Distance is shown in ×ATR because that's the honest measure of 'tight' — a big % on a volatile coin can still be only ~1.5× ATR. ${rstop.note?esc(rstop.note.charAt(0).toUpperCase()+rstop.note.slice(1))+'. ':''}It's the stop the recommended R:R is measured against. Based on: ${esc(rstop.basis)}">🛑 Recommended stop-loss: <b>${fmtNum(rstop.level)}</b> <span class="rr">${sgn}${rstop.pct.toFixed(1)}%${rstop.atrx?` · ${rstop.atrx.toFixed(1)}×ATR`:''} from entry</span> <span style="color:var(--dim)">— ${esc(rstop.basis)}${rstop.note?' · '+esc(rstop.note):''}</span></div>`:''}
    ${rec.tp!=null?`<div class="azrec" data-tip="Recommended by EXPECTED VALUE, not raw ratio: reward:risk × how reachable the target is. Reachability decays with distance (in ATR units) but stretches out when the trend, momentum and volume back the move — so a far target isn't dismissed if the setup is strong, and a nearby one isn't over-rated if it's weak. Measured from the recommended entry (${fmtNum(recE)}) over the recommended stop (${rstop?fmtNum(rstop.level):'—'}), capped 8:1, and only shown because it clears the 1.5:1 floor. Grade blends R:R and reachability.">⭐ Recommended take-profit: <b>${fmtNum(rec.tp)}</b> <span class="rr">${(d.side||'long')==='short'?'−':'+'}${(rec.move*100).toFixed(1)}% · R:R <b>${rec.rr.toFixed(2)}</b> · ~${Math.round(rec.p*100)}% reach · grade <b>${planGrade}</b></span></div>
    <div class="sidenote" data-tip="A sensible way to take the trade off: bank part at the nearest solid target to de-risk, hold the core to the base target, leave a runner for the stretch if momentum carries.">Scale-out: 🔒 Secure <b>${fmtNum(rtg.secure.lvl)}</b> (R${rtg.secure.rr.toFixed(1)}) · 🎯 Base <b>${fmtNum(rtg.primary.lvl)}</b> (R${rtg.primary.rr.toFixed(1)})${rtg.stretch?` · 🚀 Stretch <b>${fmtNum(rtg.stretch.lvl)}</b> (R${rtg.stretch.rr.toFixed(1)})`:''}</div>
    <div class="sidenote" data-tip="How the same trade looks if you enter NOW at the current market price instead of waiting for the 🎯 recommended pullback — same stop and target, worse fill, so a lower R:R. Use it to decide: take it now, or wait for the better entry.">⚡ Enter now at market (CMP ${fmtNum(cmpE)}): ${rrAt(cmpE)!=null?`R:R <b>${rrAt(cmpE).toFixed(2)}</b> to the base target`:'stop is already in the way — no clean entry here'} <span style="color:var(--dim)">vs ${rec.rr.toFixed(2)} waiting for ${fmtNum(recE)}${(rrAt(cmpE)!=null&&rrAt(cmpE)<1.5)?' — under 1.5:1 now, better to wait for the pullback':(cmpE!=null&&recE!=null&&Math.abs(cmpE-recE)/recE<0.005?' — basically at the entry already':'')}</span></div>`
    :`<div class="azrec" style="color:#f0b429" data-tip="No target on the correct side clears a 1.5:1 reward:risk from a sensible stop. In crypto a sub-1.5 R:R trade isn't worth the risk — this is a 'no trade / wait' call, not a setup. Wait for a deeper entry (better R:R), a tighter valid stop level, or a different coin.">⛔ No trade here — best realistic R:R is only <b>${rtg?rtg.bestRR.toFixed(2):'—'}</b>, under the 1.5 minimum. Wait for a better entry or setup.</div>`}
    ${stopsSection(d.stop_levels, d.side||'long', rstop?rstop.level:null)}
    <div class="azsec" data-tip="A fuller ladder of profit targets: overhead resistance levels blended with Fibonacci extensions of the recent range, in order. Each shows % move and three reward:risk numbers — R = from the recommended (pullback) entry, Rc = if you enter NOW at market, both over the recommended stop; Rt = to the tighter stop. The ⭐ chip is the recommended target.">Target ladder <span class="azsub">R = recommended entry · Rc = enter now (CMP) · Rt = tight stop · ⭐ = recommended</span></div>
    <div class="azladder">
      ${(d.target_ladder||[]).map((t,i)=>{const rR=recRR(t.level),rC=cmpRR(t.level),rT=tightRR(t.level);return `<span class="ladchip${isRec(t.level)?' ladrec':''}" data-tip="Target ${i+1}: ${t.kind} at ${fmtNum(t.level)} — ${t.pct>=0?'+':''}${t.pct}% move. R:R ${rR!=null?rR.toFixed(2):'—'} from the recommended entry, ${rC!=null?rC.toFixed(2):'—'} if you enter now at market, ${rT!=null?rT.toFixed(2):'—'} to the tight stop.${isRec(t.level)?' ⭐ Recommended — best realistic reward:risk.':''}">${isRec(t.level)?'⭐ ':''}T${i+1} ${fmtNum(t.level)} <span class="rr">${t.pct>=0?'+':''}${t.pct}%${rR!=null?` · R${rR.toFixed(1)}`:''}${rC!=null?` · Rc${rC.toFixed(1)}`:''}${rT!=null?` · Rt${rT.toFixed(1)}`:''}</span></span>`;}).join('') || '<span style="color:var(--dim)">No further targets that side.</span>'}
    </div>
    <div class="azsec">In plain English</div>
    <ul class="aznotes">${notes}</ul>
  </div>`;
}
function renderBanner(){
  const b=document.getElementById("banner");
  // Highest-priority alert: a coin just entered ⭐ Top setups (show on any tab).
  if(topNew&&topNew.length){
    const chips=topNew.map(s=>`<span class="chip"><a href="${tvLink(s)}" target="_blank" rel="noopener" onclick="event.stopPropagation()">${dispSym(s)}</a></span>`).join("");
    b.innerHTML=`<b>🌟 ${topNew.length} coin${topNew.length>1?'s':''} just entered ⭐ Top setups</b>: ${chips} <span style="color:var(--dim)">— click Top setups to see the plan</span>`;
    b.classList.add("show"); return;
  }
  let newSyms=[], what="";
  if(activeTab==="setups"){ newSyms=(lastData&&lastData.new_symbols)||[]; what="200-EMA reclaim"; }
  else if(activeTab==="early"){ newSyms=(lastData&&lastData.early_new_symbols)||[]; what="early/accumulation"; }
  else if(activeTab==="flags"){ newSyms=(lastData&&lastData.flag_new_symbols)||[]; what="bull flag"; }
  else if(activeTab==="cpr"){ newSyms=(lastData&&lastData.cpr_new_symbols)||[]; what="narrow CPR"; }
  else if(activeTab==="bounce"){ newSyms=(lastData&&lastData.bounce_new_symbols)||[]; what="support bounce"; }
  else if(activeTab==="stb"){ newSyms=(lastData&&lastData.stb_new_symbols)||[]; what="supertrend bounce"; }
  else if(activeTab==="shorts"){ newSyms=(lastData&&lastData.short_new_symbols)||[]; what="short setup"; }
  if(!newSyms.length){ b.classList.remove("show"); b.innerHTML=""; return; }
  const chips=newSyms.map(s=>`<span class="chip"><a href="${tvLink(s)}" target="_blank" rel="noopener">${dispSym(s)}</a></span>`).join("");
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
      `<td class="sym">${watchStar(h.symbol)}<a href="${tvLink(h.symbol)}" target="_blank" rel="noopener">${dispSym(h.symbol)}</a>${analyzeBtn(h.symbol)}${badges(h)}</td>`+
      `<td data-tip="Live last-traded price — refreshes ~every 20s and is independent of the chart timeframe.">${fmtNum(h.live!=null?h.live:h.price)}</td>`+
      `<td>${(+h.pole_gain_pct).toFixed(1)}</td>`+
      `<td>${h.flag_bars}</td>`+
      `<td>${(+h.pullback_pct).toFixed(1)}</td>`+
      `<td>${(+h.vol_contraction).toFixed(2)}</td>`+
      biasPill(h)+
      `<td data-tip="Optimal entry — the lower-risk fill: a pullback to the setup's support / EMA / breakout level rather than chasing the current price.">${fmtNum(h.optimal_entry)}</td>`+
      slTightCell(h)+
      slWideCell(h)+
      tpsCells(h,5)+
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
      `<td class="sym">${watchStar(h.symbol)}<a href="${tvLink(h.symbol)}" target="_blank" rel="noopener">${dispSym(h.symbol)}</a>${analyzeBtn(h.symbol)}${badges(h)}</td>`+
      `<td data-tip="Live last-traded price — refreshes ~every 20s and is independent of the chart timeframe.">${fmtNum(h.live!=null?h.live:h.price)}</td>`+
      `<td>${(+h.pct_above_ema).toFixed(2)}</td>`+
      `<td>${h.bars_since_cross}</td>`+
      biasPill(h)+
      `<td data-tip="Optimal entry — the lower-risk fill: a pullback to the setup's support / EMA / breakout level rather than chasing the current price.">${fmtNum(h.optimal_entry)}</td>`+
      slTightCell(h)+
      slWideCell(h)+
      tpsCells(h,5)+
      rvCell(h.rvol)+
      `<td class="score">${(+h.score).toFixed(1)}</td>`;
    tb.appendChild(tr);
  }
}
// Clicking a TP column header ranks by that target's REWARD:RISK (not its price)
// — so you can surface the best-R:R setups. Maps tpN -> rrN for sorting.
function remapTp(k){ return /^tp\d$/.test(k) ? 'rr'+k.slice(2) : k; }
// Show a ▲/▼ arrow on the active-sort column header.
function setSortArrow(th, dir){
  const head=th.closest('thead'); if(head) head.querySelectorAll('th[data-sort]').forEach(x=>x.removeAttribute('data-sort'));
  th.setAttribute('data-sort', dir>0?'asc':'desc');
}
document.querySelectorAll("th[data-k]").forEach(th=>th.addEventListener("click",()=>{
  const k=remapTp(th.dataset.k); if(k===sortKey) sortDir*=-1; else {sortKey=k; sortDir=(k==="symbol")?1:-1;}
  setSortArrow(th,sortDir); render();
}));
document.querySelectorAll("th[data-fk]").forEach(th=>th.addEventListener("click",()=>{
  const k=remapTp(th.dataset.fk); if(k===fSortKey) fSortDir*=-1; else {fSortKey=k; fSortDir=(k==="symbol")?1:-1;}
  setSortArrow(th,fSortDir); renderFlags();
}));
document.querySelectorAll("th[data-ck]").forEach(th=>th.addEventListener("click",()=>{
  const k=remapTp(th.dataset.ck); if(k===cSortKey) cSortDir*=-1; else {cSortKey=k; cSortDir=(k==="symbol"||k==="position")?1:-1;}
  setSortArrow(th,cSortDir); renderCPR();
}));
document.querySelectorAll("th[data-bk]").forEach(th=>th.addEventListener("click",()=>{
  const k=remapTp(th.dataset.bk); if(k===bSortKey) bSortDir*=-1; else {bSortKey=k; bSortDir=(k==="symbol")?1:-1;}
  setSortArrow(th,bSortDir); renderBounce();
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
      `<td class="sym">${watchStar(h.symbol)}<a href="${tvLink(h.symbol)}" target="_blank" rel="noopener">${dispSym(h.symbol)}</a>${analyzeBtn(h.symbol)}${badges(h)}</td>`+
      `<td data-tip="Live last-traded price — refreshes ~every 20s and is independent of the chart timeframe.">${fmtNum(h.live!=null?h.live:h.price)}</td>`+
      `<td>${fmtNum(h.support)}</td>`+
      `<td><span class="tfpill tf-${(h.tf||'').toLowerCase()}">${h.tf||'—'}</span></td>`+
      `<td>${h.touches}</td>`+
      `<td>${(+h.dist_to_support_pct).toFixed(2)}</td>`+
      `<td>${h.rsi==null?'—':(+h.rsi).toFixed(0)}</td>`+
      `<td><span class="biaspill2 b-${(h.bias||'').toLowerCase().replace(/[^a-z]/g,'')}">${h.bias||'—'}</span></td>`+
      `<td data-tip="Optimal entry — the lower-risk fill: a pullback to the setup's support / EMA / breakout level rather than chasing the current price.">${fmtNum(h.optimal_entry)}</td>`+
      slTightCell(h)+
      slWideCell(h)+
      tpsCells(h,5)+
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
      `<td class="sym">${watchStar(h.symbol)}<a href="${tvLink(h.symbol)}" target="_blank" rel="noopener">${dispSym(h.symbol)}</a>${analyzeBtn(h.symbol)}${badges(h)}</td>`+
      `<td data-tip="Live last-traded price — refreshes ~every 20s and is independent of the chart timeframe.">${fmtNum(h.live!=null?h.live:h.price)}</td>`+
      `<td>${(+h.cpr_width_pct).toFixed(3)}</td>`+
      `<td>${h.position}</td>`+
      biasPill(h)+
      `<td>${fmtNum(h.tc)}</td>`+
      `<td>${fmtNum(h.bc)}</td>`+
      `<td data-tip="Optimal entry — the lower-risk fill: a pullback to the setup's support / EMA / breakout level rather than chasing the current price.">${fmtNum(h.optimal_entry)}</td>`+
      slTightCell(h)+
      slWideCell(h)+
      tpsCells(h,5)+
      rvCell(h.rvol)+
      `<td class="score">${(+h.score).toFixed(1)}</td>`;
    tb.appendChild(tr);
  }
}
document.querySelectorAll("th[data-sk]").forEach(th=>th.addEventListener("click",()=>{
  const k=remapTp(th.dataset.sk); if(k===sSortKey) sSortDir*=-1; else {sSortKey=k; sSortDir=(k==="symbol")?1:-1;}
  setSortArrow(th,sSortDir); renderShorts();
}));
document.querySelectorAll("th[data-xk]").forEach(th=>th.addEventListener("click",()=>{
  const k=remapTp(th.dataset.xk); if(k===xSortKey) xSortDir*=-1; else {xSortKey=k; xSortDir=(k==="symbol"||k==="tf")?1:-1;}
  setSortArrow(th,xSortDir); renderStb();
}));
document.querySelectorAll("th[data-ek]").forEach(th=>th.addEventListener("click",()=>{
  const k=remapTp(th.dataset.ek); if(k===eeSortKey) eeSortDir*=-1; else {eeSortKey=k; eeSortDir=(k==="symbol")?1:-1;}
  setSortArrow(th,eeSortDir); renderEarly();
}));
function renderEarly(){
  const rows=[...eelatest].filter(h=>biasOk(h,"early")).sort((a,b)=>{
    const x=a[eeSortKey],y=b[eeSortKey];
    if(typeof x==="string") return eeSortDir*x.localeCompare(y);
    return eeSortDir*((x??0)-(y??0));
  });
  const tb=document.getElementById("erows"); tb.innerHTML="";
  document.getElementById("eempty").style.display = rows.length? "none":"block";
  for(const h of rows){
    const tr=document.createElement("tr");
    tr.className=rowClass(h);
    tr.innerHTML =
      `<td class="sym">${watchStar(h.symbol)}<a href="${tvLink(h.symbol)}" target="_blank" rel="noopener">${dispSym(h.symbol)}</a>${analyzeBtn(h.symbol)}${badges(h)}</td>`+
      `<td data-tip="Live last-traded price — refreshes ~every 20s.">${fmtNum(h.live!=null?h.live:h.price)}</td>`+
      `<td data-tip="The strong support the coin is coiling on (daily/weekly swing low or base low).">${fmtNum(h.support)}</td>`+
      `<td data-tip="How far below its recent 120-bar high the coin is trading — how beaten down it is.">${h.drawdown_pct==null?'—':(+h.drawdown_pct).toFixed(0)}%</td>`+
      `<td data-tip="Volatility contraction — recent ATR ÷ its prior average. Lower = a tighter coil, closer to expanding.">${h.contraction==null?'—':(+h.contraction).toFixed(2)}×</td>`+
      `<td>${h.pct_below_ema==null?'—':(+h.pct_below_ema).toFixed(1)}</td>`+
      `<td>${h.rsi==null?'—':(+h.rsi).toFixed(0)}</td>`+
      `<td><span class="biaspill2 b-${(h.bias||'').toLowerCase().replace(/[^a-z]/g,'')}">${h.bias||'—'}</span></td>`+
      corrTd(h.btc_corr)+
      `<td data-tip="Optimal entry — a fill near the support rather than chasing.">${fmtNum(h.optimal_entry)}</td>`+
      slTightCell(h)+
      slWideCell(h)+
      tpsCells(h,3)+
      emaTargetCell(h)+
      revTpCell(h)+
      rvCell(h.rvol)+
      `<td class="score">${(+h.score).toFixed(1)}</td>`;
    tb.appendChild(tr);
  }
}
function renderStb(){
  const rows=[...xlatest].filter(h=>biasOk(h,"stb")).sort((a,b)=>{
    const x=a[xSortKey],y=b[xSortKey];
    if(typeof x==="string") return xSortDir*x.localeCompare(y);
    return xSortDir*((x??0)-(y??0));
  });
  const tb=document.getElementById("sbrows"); tb.innerHTML="";
  document.getElementById("sbempty").style.display = rows.length? "none":"block";
  for(const h of rows){
    const tr=document.createElement("tr");
    tr.className=rowClass(h);
    tr.innerHTML =
      `<td class="sym">${watchStar(h.symbol)}<a href="${tvLink(h.symbol)}" target="_blank" rel="noopener">${dispSym(h.symbol)}</a>${analyzeBtn(h.symbol)}${badges(h)}</td>`+
      `<td data-tip="Live last-traded price — refreshes ~every 20s and is independent of the chart timeframe.">${fmtNum(h.live!=null?h.live:h.price)}</td>`+
      `<td data-tip="The Supertrend line value on the strongest confirming timeframe — acting as support below price.">${fmtNum(h.supertrend)}</td>`+
      `<td><span class="tfpill tf-${(h.tf||'').toLowerCase()}">${h.tf||'4h'}</span></td>`+
      `<td data-tip="How many of 4h / Daily / Weekly currently have Supertrend in up-mode. More = stronger multi-timeframe trend support.">${h.tf_up==null?'—':h.tf_up+'/3'}</td>`+
      `<td>${h.dist_to_st_pct==null?'—':(+h.dist_to_st_pct).toFixed(2)}</td>`+
      `<td>${h.rsi==null?'—':(+h.rsi).toFixed(0)}</td>`+
      `<td><span class="biaspill2 b-${(h.bias||'').toLowerCase().replace(/[^a-z]/g,'')}">${h.bias||'—'}</span></td>`+
      corrTd(h.btc_corr)+
      `<td data-tip="Optimal entry — a fill near the Supertrend line rather than chasing.">${fmtNum(h.optimal_entry)}</td>`+
      slTightCell(h)+
      slWideCell(h)+
      tpsCells(h,5)+
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
// The best *realistic* target and its reward:risk — recomputed from the entry we
// actually display (the optimal fill) and the tight stop, so R:R = |target−entry| ÷
// |entry−stop| ALWAYS reconciles with the row. Side-aware (longs target up, shorts
// down); only counts targets a meaningful (>=2%) move away; capped at 8:1.
function realisticRR(h, entryArg){
  const E = (entryArg!=null)? entryArg
          : (h.optimal_entry!=null? h.optimal_entry : (h.live!=null?h.live:h.price));
  const sl = h.sl_tight;
  if(E==null || sl==null || E===sl) return {rr:0,tp:null,move:0,entry:E};
  const long = sl < E;                       // stop below entry = long
  const risk = Math.abs(E - sl);
  let best={rr:0,tp:null,move:0,entry:E};
  const consider=(tp)=>{
    if(tp==null) return; if(long? tp<=E : tp>=E) return;
    const move=Math.abs((tp-E)/E);
    if(move>=0.02){ const rr=Math.min(Math.abs(tp-E)/risk,8);
      if(rr>best.rr) best={rr:rr,tp:tp,move:move,entry:E}; }
  };
  for(let i=1;i<=5;i++) consider(h['tp'+i]);
  if(best.rr===0){ for(let i=1;i<=5;i++){ const tp=h['tp'+i];
    if(tp!=null && (long?tp>E:tp<E)){ best={rr:Math.min(Math.abs(tp-E)/risk,8),tp:tp,
      move:Math.abs((tp-E)/E),entry:E}; break; } } }
  return best;
}
// Has the LIVE price already breached the setup's tight stop? (For a long that
// means price fell to/below the stop; for a short it rallied to/above it.) If so
// the setup is invalidated — the ~10-min-old levels are stale and it shouldn't
// still be presented as a clean, high-conviction play.
function stopBreached(h){
  const P=(h.live!=null?h.live:h.price);
  const sl=h.sl_tight, e=(h.optimal_entry!=null?h.optimal_entry:h.entry);
  if(P==null||sl==null||e==null||e===sl) return false;
  return (sl < e) ? (P <= sl) : (P >= sl);
}
// Per-setup quality used to pick the single best setup for a coin (blends the
// detector's own score with its realistic reward:risk).
function planEntry(h){ return h.retest_entry!=null? h.retest_entry
                       : (h.optimal_entry!=null? h.optimal_entry : (h.live!=null?h.live:h.price)); }
function setupQuality(h){ return 0.55*clamp01((h.score||0)/100)+0.45*clamp01(realisticRR(h,planEntry(h)).rr/3); }
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
      `<td class="sym">${watchStar(h.symbol)}<a href="${tvLink(h.symbol)}" target="_blank" rel="noopener">${dispSym(h.symbol)}</a>${analyzeBtn(h.symbol)}${badges(h)}</td>`+
      ratingCell(h._rating,h._why)+
      `<td data-tip="Live last-traded price — refreshes ~every 20s and is independent of the chart timeframe.">${fmtNum(h.live!=null?h.live:h.price)}</td>`+
      `<td>${(+h.pct_below_ema).toFixed(2)}</td>`+
      `<td>${h.bars_since_cross}</td>`+
      `<td><span class="biaspill2 b-${(h.bias||'').toLowerCase().replace(/[^a-z]/g,'')}">${h.bias||'—'}</span></td>`+
      corrTd(h.btc_corr)+
      `<td class="whycell" data-tip="${esc(h._why.join(' · '))}">${h._why[0]}</td>`+
      `<td data-tip="Optimal short entry — a rally back up to the 200 EMA (resistance) rather than shorting into support.">${fmtNum(h.optimal_entry)}</td>`+
      slTightCell(h)+
      slWideCell(h)+
      tpsCells(h,5)+
      rvCell(h.rvol)+
      `<td class="score">${(+h.score).toFixed(1)}</td>`;
    tb.appendChild(tr);
  }
}
function renderTop(){
  if(!lastData) return;
  const srcB=[["200-EMA reclaim",lastData.hits],["Bull flag",lastData.flag_hits],
              ["Narrow CPR",lastData.cpr_hits],["Support bounce",lastData.bounce_hits],
              ["Supertrend bounce",lastData.stb_hits]];
  const m={};
  for(const [lbl,list] of srcB) for(const h of (list||[])){
    const k=h.symbol, q=setupQuality(h);
    if(!m[k]) m[k]={row:null,bestQ:-1,best:0,setups:new Set(),rvol:0,rr:{rr:0,tp:null,move:0},
                    fresh:false,bias:h.bias,choch:h.choch};
    const o=m[k]; o.setups.add(lbl);
    o.best=Math.max(o.best,h.score||0);
    if(h.rvol&&h.rvol>o.rvol) o.rvol=h.rvol;
    if(h.is_new||h.fresh) o.fresh=true;
    if(q>o.bestQ){ o.bestQ=q; o.row=h; o.rr=realisticRR(h,planEntry(h)); o.bias=h.bias; o.choch=h.choch; }
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
      !stopBreached(o.row) &&               // live price already broke the stop → drop it
      o.rating>=52 &&
      (o.rr.rr>=1.5 || (o.f.nScans>=3 && o.rr.rr>=1.1)) &&
      (o.f.nScans>=2 || o.best>=68)
  );
  if(FILT.top.indep) items=items.filter(o=> o.corr!=null && o.corr<0.6);
  items=items.sort((a,b)=> b.rating-a.rating || b.rr.rr-a.rr.rr).slice(0,30);
  // Detect coins newly entering Top setups (for the banner + tab badge).
  const curTop=new Set(items.map(o=>o.row.symbol));
  topNew = topSyms ? [...curTop].filter(s=>!topSyms.has(s)) : [];
  topSyms = curTop;
  const tabTop=document.getElementById('tabTop');
  if(tabTop) tabTop.innerHTML = '⭐ Top setups' + (topNew.length?` <span class="newbadge">${topNew.length} NEW</span>`:'');
  if(topNew.length && activeTab!=='top') renderBanner();   // surface even off-tab
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
      `<td class="sym">${watchStar(h.symbol)}<a href="${tvLink(h.symbol)}" target="_blank" rel="noopener">${dispSym(h.symbol)}</a>${analyzeBtn(h.symbol)}${badges(h)}</td>`+
      ratingCell(o.rating,o.why)+
      `<td data-tip="${esc(o.f.setups)}">${o.f.nScans>1?'★ ':''}${o.f.nScans}</td>`+
      `<td>${fmtNum(P)}</td>`+
      `<td><span class="biaspill2 b-${(h.bias||'').toLowerCase().replace(/[^a-z]/g,'')}">${h.bias||'—'}</span></td>`+
      corrTd(o.corr)+
      `<td data-tip="Retest entry — a pullback level to wait for (nearest swing-low support / reclaimed EMA / Supertrend below price), not chasing the current candle. The R:R is measured from here.">${fmtNum(planEntry(h))}</td>`+
      slTightCell(h)+
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
    xlatest=d.stb_hits||[]; renderStb();
    eelatest=d.early_hits||[]; renderEarly();
    slatest=d.short_hits||[]; renderShorts();
    renderTop();
    renderWatch();
    { const wc=document.getElementById("tabWatch"); if(wc) wc.textContent=`📌 Watchlist${WATCH.size?' ('+WATCH.size+')':''}`; }
    const evs=(d.breakout_events||[]).filter(e=>e.time>seenBreak);
    if(evs.length){ seenBreak=Math.max(seenBreak, ...evs.map(e=>e.time)); if(alertsOn) fireBreakout(evs); }
    renderBanner();
    const nboth=(d.both_symbols||[]).length;
    const bothTxt=nboth?` · ★ ${nboth} confluence`:"";
    document.getElementById("flagCount").textContent = `${flatest.length} flag(s) · ${d.universe} pairs${bothTxt}`;
    document.getElementById("cprCount").textContent = `${clatest.length} narrow-CPR · ${d.universe} pairs${bothTxt}`;
    document.getElementById("bounceCount").textContent = `${blatest.length} bounce(s) · ${d.universe} pairs${bothTxt}`;
    document.getElementById("stbCount").textContent = `${xlatest.length} supertrend bounce(s) · ${d.universe} pairs${bothTxt}`;
    document.getElementById("shortCount").textContent = `${slatest.length} short setup(s) · ${d.universe} pairs`;
    document.getElementById("earlyCount").textContent = `${eelatest.length} early setup(s) · ${d.universe} pairs — unconfirmed`;
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
  support:"The support level price is bouncing from — identified as a swing-low pivot zone (prior candle lows clustered across 4h/Daily/Weekly and tested repeatedly). The TF column shows the strongest timeframe it sits on.",
  tf:"Strongest timeframe the support sits on — Weekly (gold) > Daily (green) > 4h. Higher = more significant.",
  touches:"How many times that support level has been tested. More = stronger.",
  dist_to_support_pct:"How far price currently sits above the support (%). Smaller = fresher bounce.",
  rsi:"RSI(14) momentum, 0–100. Below 30 = oversold (bounce potential), above 70 = overbought.",
  conv_pct:"How far the two wedge lines have converged toward the apex. Higher = tighter coil, nearer resolution.",
  phase:"Coiling = still forming inside the wedge. Broke out = price has pushed above the upper (descending) line.",
  pct_below_ema:"How far price sits BELOW the falling 200 EMA (%). This is a short: closer to the line = tighter entry.",
  supertrend:"The Supertrend (ATR 10×3) line on the strongest confirming timeframe — sitting below price and acting as a trailing support.",
  tf_up:"How many of 4h / Daily / Weekly currently have Supertrend in up-mode. More = stronger multi-timeframe trend backing.",
  dist_to_st_pct:"How far price sits above the Supertrend line (%). Smaller = a fresher bounce, tighter entry.",
  drawdown_pct:"How far below its recent 120-bar high the coin trades — how beaten down / mean-reversion room it has.",
  contraction:"Volatility contraction: recent ATR ÷ its prior average. Well under 1 = a tight coil, close to expanding.",
  support:"The support the coin is coiling on (daily/weekly swing low or base low) — identified from swing-low pivots."
};
function applyHeaderTips(){
  document.querySelectorAll("th[data-k],th[data-fk],th[data-ck],th[data-bk],th[data-sk],th[data-xk],th[data-ek]").forEach(th=>{
    const k=th.dataset.k||th.dataset.fk||th.dataset.ck||th.dataset.bk||th.dataset.wk||th.dataset.sk||th.dataset.xk||th.dataset.ek;
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
