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
        list_symbols, scan_symbol, get_session, Hit, EMA_PERIOD,
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
                "error": self.error,
                "cfg": {
                    "interval": self.cfg["interval"],
                    "quote": self.cfg["quote"],
                    "scan_every": self.cfg["scan_every"],
                    "ema_period": EMA_PERIOD,
                },
            }


def run_one_scan(state: State) -> None:
    cfg = state.cfg
    sess = get_session()
    with state.lock:
        state.scanning = True
        state.error = ""
    try:
        symbols = list_symbols(sess, cfg["quote"])
        with state.lock:
            state.universe = len(symbols)
            state.progress = (0, len(symbols))
    except requests.RequestException as e:
        with state.lock:
            state.error = f"symbol fetch failed: {e}"
            state.scanning = False
        return

    hits: list[Hit] = []
    done = 0
    scan_cfg = {k: cfg[k] for k in
                ("kline_limit", "lookback", "retest_tol", "break_tol",
                 "max_above_now", "min_slope")}
    with ThreadPoolExecutor(max_workers=cfg["workers"]) as ex:
        futs = {ex.submit(scan_symbol, sess, s, cfg["interval"], scan_cfg): s
                for s in symbols}
        for fut in as_completed(futs):
            done += 1
            if done % 25 == 0:
                with state.lock:
                    state.progress = (done, len(symbols))
            try:
                h = fut.result()
            except Exception:
                h = None
            if h:
                hits.append(h)

    hits.sort(key=lambda h: h.score, reverse=True)
    cur_symbols = {h.symbol for h in hits}
    with state.lock:
        # "New" = setups that appear now but weren't in the previous scan.
        # On the very first scan there's no baseline, so nothing is marked new.
        if state.prev_symbols is None:
            new = set()
        else:
            new = cur_symbols - state.prev_symbols
        rows = []
        for h in hits:
            d = asdict(h)
            d["is_new"] = h.symbol in new
            rows.append(d)
        state.hits = rows
        state.new_symbols = [h.symbol for h in hits if h.symbol in new]
        state.prev_symbols = cur_symbols
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
</style></head>
<body>
<header>
  <h1>MEXC · 200-EMA cross &amp; retest</h1>
  <span class="sub" id="meta"></span>
</header>
<div class="banner" id="banner"></div>
<div class="tabs">
  <div class="tab active" id="tabSetups" onclick="showTab('setups')">Setups</div>
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
      <th data-k="score">Score</th>
    </tr></thead>
    <tbody id="rows"></tbody>
  </table>
  <div class="empty" id="empty" style="display:none">No cross-and-retest setups right now. The loop keeps scanning…</div>
</div>
</div>

<div class="view" id="viewInfo">
<div class="wrap"><div class="info">
  <h2>What this scanner looks for</h2>
  <p>It watches every crypto pair on MEXC spot quoted in USDT (leveraged tokens
  and stablecoin pairs are excluded) and flags one specific bullish pattern on
  the <b>4-hour</b> chart: a <b>200-EMA cross &amp; retest</b>.</p>
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

  <h2>The "just triggered" banner</h2>
  <p>Each scan is compared with the one before it. Any pair that shows up now but
  wasn't in the previous scan is a <b>brand-new setup</b> — it gets called out in
  the green banner at the top and tagged <span class="newbadge">NEW</span> in the
  table, so you can spot fresh signals at a glance.</p>

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
function fmtNum(n){ if(n===null||n===undefined) return "—";
  const a=Math.abs(n); if(a!==0&&a<0.001) return n.toExponential(2);
  return (+n).toLocaleString(undefined,{maximumSignificantDigits:8}); }
function ago(ts){ if(!ts) return "—"; const s=Math.max(0,Math.floor(Date.now()/1000-ts));
  if(s<60) return s+"s ago"; const m=Math.floor(s/60); if(m<60) return m+"m "+(s%60)+"s ago";
  return Math.floor(m/60)+"h "+(m%60)+"m ago"; }
function until(ts){ if(!ts) return "—"; const s=Math.floor(ts-Date.now()/1000);
  if(s<=0) return "due"; const m=Math.floor(s/60); return m>0? m+"m "+(s%60)+"s" : s+"s"; }
function tvLink(sym){ return "https://www.tradingview.com/chart/?symbol=MEXC:"+sym; }
function showTab(which){
  const s=which==="setups";
  document.getElementById("tabSetups").classList.toggle("active",s);
  document.getElementById("tabInfo").classList.toggle("active",!s);
  document.getElementById("viewSetups").classList.toggle("active",s);
  document.getElementById("viewInfo").classList.toggle("active",!s);
}
function renderBanner(newSyms){
  const b=document.getElementById("banner");
  if(!newSyms || !newSyms.length){ b.classList.remove("show"); b.innerHTML=""; return; }
  const chips=newSyms.map(s=>`<span class="chip"><a href="${tvLink(s)}" target="_blank" rel="noopener">${s}</a></span>`).join("");
  const label=newSyms.length===1?"new setup just triggered":"new setups just triggered";
  b.innerHTML=`<b>🆕 ${newSyms.length} ${label}</b> since the last scan: ${chips}`;
  b.classList.add("show");
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
    if(h.is_new) tr.className="isnew";
    const badge = h.is_new ? '<span class="newbadge">NEW</span>' : '';
    tr.innerHTML =
      `<td class="sym"><a href="${tvLink(h.symbol)}" target="_blank" rel="noopener">${h.symbol}</a>${badge}</td>`+
      `<td>${fmtNum(h.price)}</td>`+
      `<td>${fmtNum(h.ema)}</td>`+
      `<td>${(+h.pct_above_ema).toFixed(2)}</td>`+
      `<td>${h.bars_since_cross}</td>`+
      `<td>${(+h.retest_gap_pct).toFixed(2)}</td>`+
      `<td class="score">${(+h.score).toFixed(1)}</td>`;
    tb.appendChild(tr);
  }
}
document.querySelectorAll("th").forEach(th=>th.addEventListener("click",()=>{
  const k=th.dataset.k; if(k===sortKey) sortDir*=-1; else {sortKey=k; sortDir=(k==="symbol")?1:-1;}
  render();
}));
async function poll(){
  try{
    const r=await fetch("/data",{cache:"no-store"}); const d=await r.json();
    latest=d.hits||[]; render();
    renderBanner(d.new_symbols);
    document.getElementById("meta").textContent =
      `${d.cfg.interval} chart · ${d.cfg.quote} spot · EMA${d.cfg.ema_period} · rescans every ${d.cfg.scan_every}m`;
    const dot=document.getElementById("dot");
    dot.className = "dot" + (d.scanning? " live":"");
    document.getElementById("scanState").textContent =
      d.scanning ? `scanning… ${d.progress[0]}/${d.progress[1]}` : "idle";
    document.getElementById("lastScan").textContent = "last scan: "+ago(d.last_scan);
    document.getElementById("nextScan").textContent =
      d.scanning ? "next scan: —" : "next scan: "+until(d.next_scan);
    document.getElementById("count").textContent =
      `${latest.length} setup(s) · ${d.universe} pairs`;
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
            elif self.path in ("/", "/index.html"):
                self._send(200, PAGE.encode(), "text/html; charset=utf-8")
            else:
                self._send(404, b"not found", "text/plain")

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
    args = p.parse_args()

    cfg = {
        "port": args.port, "scan_every": args.scan_every, "quote": args.quote,
        "interval": args.interval, "workers": args.workers,
        "kline_limit": args.kline_limit, "lookback": args.lookback,
        "retest_tol": args.retest_tol, "break_tol": args.break_tol,
        "max_above_now": args.max_above, "min_slope": args.min_slope,
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
