"""
Pump radar ("urgent pump" watch)
================================
Modelled on the Crime Scanner channel's 🚨 URGENT PUMP alerts: a coin up 60%+ in 24 hours on real futures volume,
usually sitting at its 24h high. Every 2 minutes this reads Binance's 24h futures tickers (one request for all coins).

FLAG ("🚨 Urgent pump") when:
    24h change >= URGENT_24H %   AND   24h futures turnover >= MIN_TURNOVER
A coin that is already flagged is flagged AGAIN (a new log entry) when its price is REPEAT_STEP % above its last flag,
which mirrors the channel's repeat alerts on the same pump. Coins up PUMPING_24H–URGENT_24H % show as 📈 Pumping (watch only).

Every flag is logged with its price and followed on Binance 5-minute candles: best / worst move, change after 1h / 4h / 24h,
and the result of the exit rule the MEXC pump bot uses (stop 20% below, stop to entry at +10%, 7.5% trailing stop from +15%).
The log lives in Redis (when configured) so it survives restarts.
"""
import json
import threading
import time

import requests

import volume_spikes as vs

URGENT_24H = 60.0          # % in 24h for a 🚨 flag (the channel's first urgent alerts came at +60% and +62%)
PUMPING_24H = 30.0         # shown as 📈 Pumping, not flagged
MIN_TURNOVER = 10_000_000  # USDT futures volume over 24h (the channel's smallest urgent alert had $11M)
REPEAT_STEP = 10.0         # % above the last flag price for a repeat flag
SL, BE, TRAIL_START, TRAIL = 20.0, 10.0, 15.0, 7.5
EVERY = 120
LOG_KEY = "prad_log_v1"
LOG_MAX = 500
FOLLOW_DAYS = 7

HIST_KEY = "prad_hist_v1"
HIST_EVERY = 6 * 3600
HIST_MIN_RISE, HIST_MIN_TURN = 25.0, 5_000_000      # the history keeps every pump of 25%+ in 24h on $5M+, the page filters further

STATE = {"rows": [], "updated": None, "status": "starting", "n": 0}
HIST = {"rows": [], "built": None, "status": "not built yet", "coins": 0}
LOG = []
MEXC = {"syms": set(), "t": 0}
_LOCK = threading.Lock()


def _pct(a, b):
    return round((a / b - 1) * 100, 2) if a and b else None


def _load_log():
    global LOG
    try:
        if vs._redis():
            raw = vs._redis_cmd("GET", LOG_KEY)
            if raw:
                LOG = json.loads(raw)
    except Exception as e:
        print(f"[prad] log load failed: {e}", flush=True)


def _save_log():
    try:
        if vs._redis():
            vs._redis_cmd("SET", LOG_KEY, json.dumps(LOG[-LOG_MAX:], separators=(",", ":")))
    except Exception as e:
        print(f"[prad] log save failed: {e}", flush=True)


def _mexc_syms(now):
    """Coins with a MEXC USDT perpetual (the pump bot trades MEXC), refreshed hourly."""
    if now - MEXC["t"] > 3600:
        try:
            j = requests.get("https://contract.mexc.com/api/v1/contract/detail", timeout=20).json()
            s = {x["baseCoin"].upper() for x in j.get("data", []) if x.get("quoteCoin") == "USDT" and x.get("state") == 0}
            if s:
                MEXC["syms"] = s
            MEXC["t"] = now
        except Exception as e:
            MEXC["t"] = now - 3300
            print(f"[prad] MEXC list failed: {e}", flush=True)
    return MEXC["syms"]


def _base(sym):
    b = sym[:-4] if sym.endswith("USDT") else sym
    for pre in ("1000000", "1000", "1M"):
        if b.startswith(pre) and len(b) > len(pre) + 1:
            return b[len(pre):]
    return b


def simulate(entry, bars):
    """The pump bot's exit rule on 5m candles [t, o, h, l, c, v]. Returns (result %, how, minutes) or (latest %, 'open', minutes).
    Cautious: within one candle the stop is checked before any new high."""
    stop, trailing, peak, be = entry * (1 - SL / 100), False, entry, False
    last = None
    for b in bars:
        hi, lo, c = b[2], b[3], b[4]
        if lo <= stop:
            how = "trailing stop" if trailing else ("stop at entry" if be else "stop loss")
            return round((stop / entry - 1) * 100, 2), how, b[0]
        peak = max(peak, hi)
        if not be and peak >= entry * (1 + BE / 100):
            be, stop = True, max(stop, entry)
        if not trailing and peak >= entry * (1 + TRAIL_START / 100):
            trailing = True
        if trailing:
            stop = max(stop, peak * (1 - TRAIL / 100))
            if c <= stop:
                return round((stop / entry - 1) * 100, 2), "trailing stop", b[0]
        last = b
    return (round((last[4] / entry - 1) * 100, 2) if last else 0.0), "open", (last[0] if last else None)


def _follow(e, now):
    """Update one logged flag from 5m candles since it fired."""
    if e.get("done"):
        return
    start_ms = int(e["t"] // 300 * 300) * 1000
    kl = vs._parse_klines(vs._get("fut", "/fapi/v1/klines", {"symbol": e["sym"], "interval": "5m", "startTime": start_ms, "limit": 1000}))
    if not kl:
        return
    bars = [b for b in kl if b[0] >= e["t"] - 300]
    if not bars:
        return
    p = e["price"]
    e["best"] = round(max(0.0, (max(b[2] for b in bars) / p - 1) * 100), 2)
    e["worst"] = round(min(0.0, (min(b[3] for b in bars) / p - 1) * 100), 2)
    e["now"] = round((bars[-1][4] / p - 1) * 100, 2)
    for k, h in (("h1", 1), ("h4", 4), ("h24", 24), ("h72", 72)):
        if k not in e:
            at = [b for b in bars if b[0] <= e["t"] + h * 3600 - 300]
            if at and at[-1][0] >= e["t"] + h * 3600 - 600:
                e[k] = round((at[-1][4] / p - 1) * 100, 2)
    if e.get("how") in (None, "open"):
        r, how, t = simulate(p, bars)
        e["res"], e["how"] = r, how
        if how != "open":
            e["closed"] = t
    if now - e["t"] >= FOLLOW_DAYS * 86400 or (e.get("how") not in (None, "open") and "h72" in e):
        e["done"] = True


def scan_once():
    now = time.time()
    j = vs._get("fut", "/fapi/v1/ticker/24hr") or []
    fund = vs.fetch_funding() or {}
    uni = vs.fetch_universe() or set()
    mexc = _mexc_syms(now)
    rows = []
    for d in j:
        try:
            s = d["symbol"]
            if uni and s not in uni:
                continue
            chg, turn = float(d["priceChangePercent"]), float(d["quoteVolume"])
            if chg < PUMPING_24H or turn < MIN_TURNOVER:
                continue
            px, hi = float(d["lastPrice"]), float(d["highPrice"])
            rows.append({"symbol": s, "price": px, "chg24": round(chg, 1), "from_high": _pct(px, hi), "turnover24": turn,
                         "funding": fund.get(s), "mexc": _base(s) in mexc if mexc else None,
                         "tier": "urgent" if chg >= URGENT_24H else "pumping"})
        except (KeyError, TypeError, ValueError):
            continue
    try:
        vs.VSPIKE._mcaps(now)
    except Exception:
        pass
    for r in rows:                      # open interest only for the few coins that qualify
        try:
            oi = vs.fetch_oi_hist(r["symbol"])
            if oi and len(oi) >= 5:
                r["oi_usd"] = round(oi[-1][1])
                r["oi4"] = _pct(oi[-1][1], oi[-5][1])
            mc, _ = vs.VSPIKE.mcap_for(r["symbol"], now)
            if mc and r.get("oi_usd"):
                r["oi_mcap"] = round(r["oi_usd"] / mc * 100, 1)
        except Exception:
            pass
        time.sleep(0.2)
    rows.sort(key=lambda x: (x["tier"] != "urgent", -x["chg24"]))
    with _LOCK:
        for r in rows:
            if r["tier"] != "urgent":
                continue
            prev = [e for e in LOG if e["sym"] == r["symbol"] and now - e["t"] < 3 * 86400]
            last = prev[-1] if prev else None
            if last is None or now - last["t"] >= 86400 or r["price"] >= last["price"] * (1 + REPEAT_STEP / 100):
                n = (last.get("n", 1) + 1) if last and now - last["t"] < 86400 else 1
                LOG.append({"t": int(now), "sym": r["symbol"], "price": r["price"], "chg24": r["chg24"], "from_high": r["from_high"],
                            "turnover24": r["turnover24"], "funding": r["funding"], "oi_mcap": r.get("oi_mcap"), "oi4": r.get("oi4"),
                            "mexc": r["mexc"], "n": n})
                r["new"] = True
        del LOG[:-LOG_MAX]
        follow = [e for e in LOG if not e.get("done")]
    for e in follow:
        try:
            _follow(e, now)
        except Exception as ex:
            print(f"[prad] follow {e['sym']}: {ex}", flush=True)
        time.sleep(0.2)
    with _LOCK:
        STATE.update(rows=rows, updated=now, status="ok", n=len(j))
    _save_log()


def _hist_events(sym, kl, mexc):
    """Every pump of HIST_MIN_RISE%+ in 24h on HIST_MIN_TURN+ volume in this coin's hourly candles, with what it did next."""
    out, last = [], None
    for i in range(24 * 8, len(kl) - 1):
        c = kl[i][4]
        chg = (c / kl[i - 24][4] - 1) * 100 if kl[i - 24][4] else 0
        if chg < HIST_MIN_RISE:
            continue
        turn = sum(b[5] for b in kl[i - 23:i + 1])
        if turn < HIST_MIN_TURN:
            continue
        t = kl[i][0] + 3600                                   # the hourly candle has just closed
        if last and t - last["t"] < 86400 and c < last["price"] * (1 + REPEAT_STEP / 100):
            continue
        n = last["n"] + 1 if last and t - last["t"] < 86400 else 1
        prior = sum(b[5] for b in kl[i - 24 * 8 + 1:i - 23]) / 7
        hi24 = max(b[2] for b in kl[i - 23:i + 1])
        fut = kl[i + 1:]
        wk = fut[:168]
        e = {"t": t, "sym": sym, "price": c, "chg24": round(chg, 1), "turnover24": round(turn), "volx": round(turn / prior, 1) if prior else None,
             "from_high": _pct(c, hi24), "n": n, "mexc": _base(sym) in mexc if mexc else None,
             "best": round(max(0.0, (max(b[2] for b in wk) / c - 1) * 100), 1) if wk else None,
             "worst": round(min(0.0, (min(b[3] for b in wk) / c - 1) * 100), 1) if wk else None}
        for k, h in (("h1", 1), ("h4", 4), ("h24", 24), ("h72", 72)):
            if len(fut) >= h:
                e[k] = round((fut[h - 1][4] / c - 1) * 100, 1)
        r, how, tc = simulate(c, fut)
        e["res"], e["how"], e["hours"] = r, how, round((tc - kl[i][0]) / 3600) if tc else None
        # 7-day hourly path for the page's own exit settings: [open, high, low, close] as % from the flag price x10
        e["path"] = [x for b in wk for x in (round((b[1] / c - 1) * 1000), round((b[2] / c - 1) * 1000), round((b[3] / c - 1) * 1000), round((b[4] / c - 1) * 1000))]
        out.append(e)
        last = e
    return out


def build_history():
    """Last ~62 days of Binance hourly candles for every USDT perp; rebuilt every HIST_EVERY seconds."""
    HIST["status"] = "building"
    now = time.time()
    uni = sorted(vs.fetch_universe() or [])
    mexc = _mexc_syms(now)
    rows, coins = [], 0
    for sym in uni:
        try:
            kl = vs._parse_klines(vs._get("fut", "/fapi/v1/klines", {"symbol": sym, "interval": "1h", "limit": 1500}))
            if kl and len(kl) > 24 * 9:
                ev = _hist_events(sym, kl, mexc)
                if ev:
                    coins += 1
                    rows += ev
        except Exception as ex:
            print(f"[prad] history {sym}: {ex}", flush=True)
        time.sleep(0.3)
    rows.sort(key=lambda e: -e["t"])
    HIST.update(rows=rows, built=int(now), status="ok", coins=coins, scanned=len(uni))
    try:
        if vs._redis():
            slim = dict(HIST, rows=[{f: v for f, v in e.items() if f != "path"} for e in rows])   # paths stay in memory only (size)
            vs._redis_cmd("SET", HIST_KEY, json.dumps(slim, separators=(",", ":")))
    except Exception as ex:
        print(f"[prad] history save failed: {ex}", flush=True)
    print(f"[prad] history: {len(rows)} pumps on {coins} coins", flush=True)


def _hist_loop():
    time.sleep(120)
    try:
        if vs._redis():
            raw = vs._redis_cmd("GET", HIST_KEY)
            if raw:
                HIST.update(json.loads(raw))
    except Exception as e:
        print(f"[prad] history load failed: {e}", flush=True)
    while True:
        no_paths = bool(HIST.get("rows")) and "path" not in HIST["rows"][0]   # loaded from Redis: rebuild so the page's exit simulator has paths
        if no_paths or not HIST.get("built") or time.time() - HIST["built"] >= HIST_EVERY:
            try:
                build_history()
            except Exception as e:
                HIST["status"] = f"error: {e}"
                print(f"[prad] history failed: {e}", flush=True)
        time.sleep(600)


def _loop():
    time.sleep(60)
    _load_log()
    while True:
        t0 = time.time()
        try:
            scan_once()
        except Exception as e:
            STATE["status"] = f"error: {e}"
            print(f"[prad] scan failed: {e}", flush=True)
        time.sleep(max(30, EVERY - (time.time() - t0)))


def payload():
    with _LOCK:
        return {"rows": STATE["rows"], "updated": STATE["updated"], "status": STATE["status"], "n": STATE["n"],
                "log": list(reversed(LOG[-300:])),
                "rules": {"urgent_24h": URGENT_24H, "pumping_24h": PUMPING_24H, "min_turnover": MIN_TURNOVER, "repeat_step": REPEAT_STEP,
                          "sl": SL, "be": BE, "trail_start": TRAIL_START, "trail": TRAIL},
                "hist": {k: ([{f: v for f, v in e.items() if f != "path"} for e in v] if k == "rows" else v) for k, v in HIST.items()}}


def paths_payload():
    """Hourly paths of the 2-month history, keyed sym|t; loaded once by the page for its exit-settings simulator."""
    return {"built": HIST.get("built"), "paths": {f'{e["sym"]}|{e["t"]}': e.get("path") or [] for e in HIST.get("rows") or []}}


def start():
    threading.Thread(target=_loop, daemon=True).start()
    threading.Thread(target=_hist_loop, daemon=True).start()
