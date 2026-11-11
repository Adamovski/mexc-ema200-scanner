"""
OI build-up scanner ("pre-pump" watch)
=====================================
Open interest rising hard while price barely moves = traders piling into positions before a move
(AGT, 11 Nov 2026: OI +323% in 24h with price still flat, then the move). Every 15 minutes this
scans every Binance USDT perp: 24h / 4h change in OI (USD) vs 24h / 4h change in price, the 24h
price range, OI as % of market cap and funding.

FLAG ("🧲 OI build") when, over the last 24h:
    OI (USD) up >= OI_MIN_24H %   AND   |price change| <= PX_MAX_24H %   AND   OI >= OI_MIN_USD
Strength = OI change ÷ max(2, |price change|): lots of new positions per % of price movement.

Each new flag (first in 24h for that coin) is logged with its price, and the log tracks what price
did afterwards (best move up, worst move down, change after 4h / 24h / 72h), so the tab shows whether
the signal actually leads pumps. Funding tells which side is crowded: negative funding with OI
building = shorts piling in (squeeze fuel); high positive = longs piling in (can dump instead).
"""
import json
import threading
import time

import volume_spikes as vs

OI_MIN_24H = 40.0        # % OI growth over 24h
PX_MAX_24H = 8.0         # max |price change| over 24h to count as "price hasn't moved yet"
OI_MIN_USD = 1_000_000   # ignore tiny contracts
MIN_TURNOVER = 1_000_000
EVERY = 15 * 60
LOG_KEY = "oib_log_v1"
LOG_MAX = 400

STATE = {"rows": [], "updated": None, "status": "starting", "n": 0}
LOG = []
_LOCK = threading.Lock()


def _oi_hist(sym):
    j = vs._get("fut", "/futures/data/openInterestHist", {"symbol": sym, "period": "1h", "limit": 49})
    try:
        return [(int(x["timestamp"]) // 1000, float(x["sumOpenInterestValue"])) for x in j]
    except Exception:
        return None


def _kl(sym):
    return vs._parse_klines(vs._get("fut", "/fapi/v1/klines", {"symbol": sym, "interval": "1h", "limit": 49}))


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
        print(f"[oib] log load failed: {e}", flush=True)


def _save_log():
    try:
        if vs._redis():
            vs._redis_cmd("SET", LOG_KEY, json.dumps(LOG[-LOG_MAX:], separators=(",", ":")))
    except Exception as e:
        print(f"[oib] log save failed: {e}", flush=True)


def _update_outcomes(prices, now):
    """Follow each logged flag: best/worst move since, and the change at 4h / 24h / 72h."""
    for e in LOG:
        p = prices.get(e["sym"])
        if not p or e.get("done"):
            continue
        age = now - e["t"]
        chg = (p / e["price"] - 1) * 100
        e["now"] = round(chg, 2)
        e["best"] = round(max(e.get("best", 0), chg), 2)
        e["worst"] = round(min(e.get("worst", 0), chg), 2)
        for k, h in (("h4", 4), ("h24", 24), ("h72", 72)):
            if k not in e and age >= h * 3600:
                e[k] = round(chg, 2)
        if age >= 7 * 86400:
            e["done"] = True


def scan_once():
    now = time.time()
    tick = vs.fetch_tickers() or {}
    fund = vs.fetch_funding() or {}
    uni = vs.fetch_universe() or set()
    syms = [s for s, t in tick.items() if t["turnover24"] >= MIN_TURNOVER and (not uni or s in uni)]
    try:
        vs.VSPIKE._mcaps(now)
    except Exception:
        pass
    rows = []
    for s in syms:
        try:
            oi, kl = _oi_hist(s), _kl(s)
            if not oi or len(oi) < 25 or not kl or len(kl) < 25:
                continue
            o_now, o4, o24 = oi[-1][1], oi[-5][1], oi[-25][1]
            px = tick[s]["price"]
            p4, p24 = kl[-5][4], kl[-25][1]
            last24 = kl[-24:]
            hi, lo = max(k[2] for k in last24), min(k[3] for k in last24)
            r = {"symbol": s, "price": px, "oi_usd": round(o_now), "oi24": _pct(o_now, o24), "oi4": _pct(o_now, o4),
                 "px24": _pct(px, p24), "px4": _pct(px, p4), "range24": _pct(hi, lo),
                 "funding": fund.get(s), "turnover24": tick[s]["turnover24"]}
            try:
                mc, src = vs.VSPIKE.mcap_for(s, now)
                if mc:
                    r["mcap"] = mc
                    r["oi_mcap"] = round(o_now / mc * 100, 1)
            except Exception:
                pass
            if r["oi24"] is None or r["px24"] is None:
                continue
            r["strength"] = round(r["oi24"] / max(2.0, abs(r["px24"])), 1)
            r["flag"] = bool(r["oi24"] >= OI_MIN_24H and abs(r["px24"]) <= PX_MAX_24H and o_now >= OI_MIN_USD)
            if r["oi24"] >= 15 or r["flag"]:
                rows.append(r)
        except Exception as e:
            print(f"[oib] {s}: {e}", flush=True)
        time.sleep(0.25)
    rows.sort(key=lambda x: (-x["flag"], -(x["strength"] or 0)))
    prices = {s: t["price"] for s, t in tick.items()}
    with _LOCK:
        recent = {e["sym"] for e in LOG if now - e["t"] < 86400}
        for r in rows:
            if r["flag"] and r["symbol"] not in recent:
                LOG.append({"t": int(now), "sym": r["symbol"], "price": r["price"], "oi24": r["oi24"], "px24": r["px24"],
                            "oi_usd": r["oi_usd"], "funding": r["funding"], "oi_mcap": r.get("oi_mcap")})
                r["new"] = True
        _update_outcomes(prices, now)
        del LOG[:-LOG_MAX]
        STATE.update(rows=rows, updated=now, status="ok", n=len(syms))
    _save_log()


def _loop():
    time.sleep(90)
    _load_log()
    while True:
        t0 = time.time()
        try:
            scan_once()
        except Exception as e:
            STATE["status"] = f"error: {e}"
            print(f"[oib] scan failed: {e}", flush=True)
        time.sleep(max(60, EVERY - (time.time() - t0)))


def payload():
    with _LOCK:
        return {"rows": STATE["rows"], "updated": STATE["updated"], "status": STATE["status"], "n": STATE["n"],
                "log": list(reversed(LOG[-200:])),
                "rules": {"oi_min_24h": OI_MIN_24H, "px_max_24h": PX_MAX_24H, "oi_min_usd": OI_MIN_USD}}


def start():
    threading.Thread(target=_loop, daemon=True).start()
