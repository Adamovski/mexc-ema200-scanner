"""
+60% in a day — history (🚀 tab)
================================
Every MEXC USDT perpetual, scanned back as far as MEXC keeps candles. An alert is the first moment a coin trades
RISE% (60%) above its lowest price of the previous 24 hours; the "buy" is at that +60% level. A coin can alert again
only COOL_H hours after its last alert. Each alert is then followed for 7 days.

1. Hourly MEXC candles (~12 months) find the candidate hours.
2. Each candidate is pinned to the exact candle with 1-minute candles where MEXC still keeps them (~30 days),
   5-minute candles before that.
3. The 7 days after are stored as bars [minutes after, high %, low %, close %] (15-minute bars for the first day,
   hourly after) for the page's charts and strategy tester.

Known gaps: delisted coins are missing (survivorship bias); within a candle the page assumes the stop came first.
The result lives in Upstash Redis (chunked), with a checkpoint so a restart resumes the scan.
"""
import gzip
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import requests

import system_backtest as sbt
import volume_spikes as vs

BASE = "https://contract.mexc.com/api/v1/contract"
RISE = float(os.environ.get("P60_RISE", 60))
COOL_H = 24
FOLLOW_D = 7
DAYS = int(os.environ.get("P60_DAYS", 360))
ONE_MIN_DAYS = 29
REFRESH_EVERY = 24 * 3600
KEY = "p60_v1"
WORKERS = 4
STATE = {"status": "idle", "done": 0, "total": 0, "events": 0, "started": None, "error": None}
RES = {"blob": None, "made": None}
_LOCK = threading.Lock()
_RL = threading.Lock()
_last = [0.0]
_tls = threading.local()


def _sess():
    s = getattr(_tls, "s", None)
    if s is None:
        s = _tls.s = requests.Session()
        s.headers.update({"User-Agent": "Mozilla/5.0"})
    return s


def _get(path, params=None):
    for attempt in range(5):
        with _RL:                                    # ~12 requests a second over all threads
            gap = 0.085 - (time.time() - _last[0])
            if gap > 0:
                time.sleep(gap)
            _last[0] = time.time()
        try:
            r = _sess().get(BASE + path, params=params, timeout=20)
            if r.status_code == 429:
                raise RuntimeError("rate limited")
            j = r.json()
            if j.get("success") is False:
                raise RuntimeError(str(j)[:120])
            return j.get("data")
        except Exception:
            if attempt == 4:
                raise
            time.sleep(2 * (attempt + 1))


def _klines(sym, iv, start, end):
    step = {"Min1": 60, "Min5": 300, "Min60": 3600}[iv]
    out, t = {}, int(start)
    while t < end:
        e = min(int(end), t + step * 2000)
        d = _get(f"/kline/{sym}", {"interval": iv, "start": t, "end": e})
        if d and d.get("time"):
            for k in zip(d["time"], d["open"], d["high"], d["low"], d["close"], d.get("amount") or [0] * len(d["time"])):
                out[k[0]] = tuple(float(x) for x in k)
        t = e
    return [out[k] for k in sorted(out)]


def _candidates(hours):
    res, last = [], -1e18
    for i in range(len(hours)):
        lo = min(h[3] for h in hours[max(0, i - 23): i + 1])
        if lo > 0 and hours[i][2] >= lo * (1 + RISE / 100) and hours[i][0] - last >= COOL_H * 3600:
            res.append(i)
            last = hours[i][0]
    return res


def _event(sym, hours, i, now):
    t_h = int(hours[i][0])
    iv = "Min1" if now - t_h < ONE_MIN_DAYS * 86400 else "Min5"
    step = 60 if iv == "Min1" else 300
    pre = _klines(sym, iv, t_h - 86400, t_h + 3600)
    if len(pre) < 10:
        return None
    trig, lows = None, []
    for k in pre:                                   # first fine candle reaching RISE% above the rolling 24 h low
        while lows and lows[0][0] < k[0] - 86400:
            lows.pop(0)
        while lows and lows[-1][1] >= k[3]:
            lows.pop()
        lows.append((k[0], k[3]))
        lo = lows[0][1]
        if k[0] >= t_h and lo > 0 and k[2] >= lo * (1 + RISE / 100):
            level = lo * (1 + RISE / 100)
            trig = (k[0], max(k[1], level), lo, lows[0][0])
            break
    if not trig:
        return None
    t0, entry, low24, lo_t = trig
    end = min(now, t0 + FOLLOW_D * 86400)
    post = [k for k in _klines(sym, iv, t0, end + step) if k[0] >= t0]
    if not post:
        return None
    pct = lambda p: (p / entry - 1) * 100
    bars, cur = [], None
    for k in post:
        dt = k[0] - t0
        b = int(dt // 900) if dt < 86400 else 96 + int((dt - 86400) // 3600) * 4
        if cur is None or cur[0] != b:
            if cur:
                bars.append(cur)
            cur = [b, k[2], k[3], k[4]]
        else:
            cur[1] = max(cur[1], k[2]); cur[2] = min(cur[2], k[3]); cur[3] = k[4]
    bars.append(cur)
    bars = [[b * 15, round(pct(h), 2), round(pct(l), 2), round(pct(c), 2)] for b, h, l, c in bars]
    hi_b = max(bars, key=lambda b: b[1]); lo_b = min(bars, key=lambda b: b[2])

    def at(h):
        m = h * 60
        xs = [b for b in bars if b[0] < m]
        return xs[-1][3] if xs and bars[-1][0] >= m - 60 else None
    return {"coin": sym.replace("_USDT", ""), "sym": sym, "ts": int(t0), "entry": float(f"{entry:.6g}"), "low24": float(f"{low24:.6g}"),
            "rise_h": round((t0 - lo_t) / 3600, 1), "res": iv, "vol24": round(sum(h[5] for h in hours[max(0, i - 23): i + 1])),
            "done": end >= t0 + FOLLOW_D * 86400 - 60,
            "hi": hi_b[1], "hi_t": hi_b[0], "lo": lo_b[2], "lo_t": lo_b[0],
            "lbp": min([b[2] for b in bars if b[0] <= hi_b[0]] or [0]),
            "marks": {k: at(v) for k, v in (("h1", 1), ("h4", 4), ("h24", 24), ("d3", 72), ("d7", 168))}, "bars": bars}


def _coin(sym, now):
    hours = _klines(sym, "Min60", now - DAYS * 86400, now)
    out = []
    for i in _candidates(hours):
        try:
            ev = _event(sym, hours, i, now)
            if ev:
                out.append(ev)
        except Exception as e:
            print(f"[p60] {sym} event failed: {e}", flush=True)
    return out


def _publish(res):
    RES["blob"] = gzip.compress(json.dumps(res, separators=(",", ":")).encode(), 6)
    RES["made"] = res.get("made")


def run():
    with _LOCK:
        if STATE["status"] == "running":
            return
        STATE.update(status="running", done=0, events=0, started=time.time(), error=None)
    try:
        det = _get("/detail") or []
        syms = sorted(d["symbol"] for d in det if d.get("quoteCoin") == "USDT" and d.get("state", 0) == 0 and d["symbol"].endswith("_USDT"))
        ck = None
        try:
            raw = sbt._rget(KEY + ":ckpt") if vs._redis() else None
            ck = json.loads(raw) if raw else None
        except Exception:
            ck = None
        if ck and time.time() - ck.get("now", 0) < 2 * 86400:
            now, events, finished = ck["now"], ck["events"], set(ck["done"])
            print(f"[p60] resuming: {len(finished)} coins already done", flush=True)
        else:
            now, events, finished = int(time.time()), [], set()
        todo = [s for s in syms if s not in finished]
        STATE.update(total=len(syms), done=len(finished), events=len(events))
        with ThreadPoolExecutor(WORKERS) as ex:
            for n, (s, ev) in enumerate(zip(todo, ex.map(lambda s: _safe_coin(s, now), todo))):
                events += ev
                finished.add(s)
                STATE.update(done=len(finished), events=len(events))
                if (n + 1) % 100 == 0 and vs._redis():
                    try:
                        sbt._rset(KEY + ":ckpt", json.dumps({"now": now, "events": events, "done": sorted(finished)}, separators=(",", ":")))
                    except Exception as e:
                        print(f"[p60] checkpoint failed: {e}", flush=True)
        events.sort(key=lambda e: e["ts"])
        res = {"made": now, "rise": RISE, "days": DAYS, "coins": len(syms), "events": events}
        _publish(res)
        if vs._redis():
            try:
                sbt._rset(KEY, json.dumps(res, separators=(",", ":")))
                vs._redis_cmd("DEL", KEY + ":ckpt:n")
            except Exception as e:
                print(f"[p60] save failed: {e}", flush=True)
        STATE.update(status="done")
        print(f"[p60] done: {len(events)} alerts from {len(syms)} coins", flush=True)
    except Exception as e:
        STATE.update(status="error", error=str(e)[:200])
        print(f"[p60] failed: {e}", flush=True)


def _safe_coin(s, now):
    try:
        return _coin(s, now)
    except Exception as e:
        print(f"[p60] {s} failed: {e}", flush=True)
        return []


def status():
    return dict(STATE, made=RES["made"])


def blob():
    return RES["blob"]


def _loop():
    time.sleep(300)                                  # let the live scanners warm up first
    try:
        raw = sbt._rget(KEY) if vs._redis() else None
        if raw:
            _publish(json.loads(raw))
            STATE["status"] = "done"
    except Exception as e:
        print(f"[p60] load failed: {e}", flush=True)
    while True:
        ck = False
        try:
            ck = bool(vs._redis() and vs._redis_cmd("GET", KEY + ":ckpt:n"))
        except Exception:
            pass
        if not RES["made"] or time.time() - RES["made"] > REFRESH_EVERY or ck:
            run()
        time.sleep(1800)


def start():
    # Off by default: the full scan needs more than the free plan's 512 MB. Set P60_ON=1 to run it.
    if os.environ.get("P60_ON") == "1":
        threading.Thread(target=_loop, daemon=True).start()
