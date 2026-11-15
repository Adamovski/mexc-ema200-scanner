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
import gc
import gzip
import io
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
KEY = "p60_v2"
OLD_KEYS = ["p60_v1", "p60_v1:ckpt"]
WORKERS = 1
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
        with _RL:                                    # ~4 requests a second
            gap = 0.25 - (time.time() - _last[0])        # gentle: ~4 requests a second, leaves CPU for the dashboard
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
    time.sleep(0.02)
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
    time.sleep(0.02)
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


def _publish_blob(gz, made):
    RES["blob"] = gz
    RES["made"] = made


TMP = os.path.join(os.environ.get("TMPDIR", "/tmp"), "p60_events.jsonl")


def _drop_old():
    """Remove the first version's big checkpoint from Redis (loading it was what ran out of memory)."""
    for k in OLD_KEYS:
        try:
            n = vs._redis_cmd("GET", k + ":n")
            for i in range(int(n or 0)):
                vs._redis_cmd("DEL", f"{k}:{i}")
            vs._redis_cmd("DEL", k + ":n")
        except Exception:
            pass


def _rget_str(key):
    try:
        return vs._redis_cmd("GET", key)
    except Exception:
        return None


def run():
    """Memory- and CPU-light. Every 50 coins the new alerts are saved to Redis as a small compressed part, so a
    restart resumes where it stopped instead of starting over."""
    import base64
    with _LOCK:
        if STATE["status"] == "running":
            return
        STATE.update(status="running", done=0, events=0, started=time.time(), error=None)
    try:
        det = _get("/detail") or []
        syms = sorted(d["symbol"] for d in det if d.get("quoteCoin") == "USDT" and d.get("state", 0) == 0 and d["symbol"].endswith("_USDT"))
        det = None
        prog = None
        if vs._redis():
            try:
                prog = json.loads(_rget_str(KEY + ":prog") or "null")
            except Exception:
                prog = None
        if prog and time.time() - prog.get("now", 0) < 2 * 86400:
            now, start, parts, n_ev = prog["now"], prog["done"], prog["parts"], prog["events"]
            print(f"[p60] resuming at coin {start} of {len(syms)}", flush=True)
        else:
            now, start, parts, n_ev = int(time.time()), 0, 0, 0
        STATE.update(total=len(syms), done=start, events=n_ev)
        pend = []

        def flush(done):
            nonlocal parts, pend
            if not vs._redis():
                return
            z = base64.b64encode(gzip.compress("\n".join(pend).encode(), 6)).decode() if pend else ""
            vs._redis_cmd("SET", f"{KEY}:part:{parts}", z)
            parts += 1
            vs._redis_cmd("SET", KEY + ":prog", json.dumps({"now": now, "done": done, "parts": parts, "events": n_ev}))
            pend = []

        for k in range(start, len(syms)):
            ev = _safe_coin(syms[k], now)
            pend += [json.dumps(e, separators=(",", ":")) for e in ev]
            n_ev += len(ev)
            ev = None
            STATE.update(done=k + 1, events=n_ev)
            if (k + 1) % 50 == 0 or k + 1 == len(syms):
                flush(k + 1)
                gc.collect()
            time.sleep(0.05)
        # assemble: read parts back one at a time, sort by time keeping only (ts, part, line no.)
        idx = []
        for pi in range(parts):
            z = _rget_str(f"{KEY}:part:{pi}") if vs._redis() else None
            for li, line in enumerate(gzip.decompress(base64.b64decode(z)).decode().split("\n") if z else (pend if not vs._redis() else [])):
                if line:
                    idx.append((int(line[line.find('"ts":') + 5:].split(",", 1)[0]), pi, li))
        idx.sort()
        buf = io.BytesIO()
        cache = {}
        with gzip.GzipFile(fileobj=buf, mode="wb", compresslevel=6) as gz:
            gz.write(json.dumps({"made": now, "rise": RISE, "days": DAYS, "coins": len(syms)})[:-1].encode() + b',"events":[')
            for n, (_, pi, li) in enumerate(idx):
                if pi not in cache:
                    cache.clear()
                    z = _rget_str(f"{KEY}:part:{pi}")
                    cache[pi] = gzip.decompress(base64.b64decode(z)).decode().split("\n") if z else []
                gz.write((b"," if n else b"") + cache[pi][li].encode())
            gz.write(b"]}")
        cache = idx = None
        blob = buf.getvalue()
        buf = None
        _publish_blob(blob, now)
        if vs._redis():
            try:
                z = base64.b64encode(blob).decode()
                ch = [z[i:i + sbt.CHUNK] for i in range(0, len(z), sbt.CHUNK)] or [""]
                for i, part in enumerate(ch):
                    vs._redis_cmd("SET", f"{KEY}:gz:{i}", part)
                vs._redis_cmd("SET", KEY + ":gz:n", str(len(ch)))
                vs._redis_cmd("SET", KEY + ":made", str(now))
                for pi in range(parts):
                    vs._redis_cmd("DEL", f"{KEY}:part:{pi}")
                vs._redis_cmd("DEL", KEY + ":prog")
                z = ch = None
            except Exception as e:
                print(f"[p60] save failed: {e}", flush=True)
        gc.collect()
        STATE.update(status="done")
        print(f"[p60] done: {n_ev} alerts from {len(syms)} coins, {len(blob) // 1024} KB", flush=True)
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


def _load():
    try:
        import base64
        n = vs._redis_cmd("GET", KEY + ":gz:n")
        if not n:
            return
        z = "".join(vs._redis_cmd("GET", f"{KEY}:gz:{i}") or "" for i in range(int(n)))
        _publish_blob(base64.b64decode(z), int(vs._redis_cmd("GET", KEY + ":made") or 0))
        STATE["status"] = "done"
    except Exception as e:
        print(f"[p60] load failed: {e}", flush=True)


def _loop():
    time.sleep(300)                                  # let the live scanners warm up first
    if vs._redis():
        _drop_old()
        _load()
    while True:
        if not RES["made"] or time.time() - RES["made"] > REFRESH_EVERY:
            run()
        time.sleep(1800)


def start():
    # Off on Render: the free plan has no memory to spare. The scan runs on GitHub Actions instead (see .github/workflows/p60-scan.yml).
    if os.environ.get("P60_ON", "0") == "1":
        threading.Thread(target=_loop, daemon=True).start()
