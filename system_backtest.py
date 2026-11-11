"""
System backtest: replays the LIVE volume-spike system over Binance history.
=========================================================================
For every USDT perp the live scanner watches (24h turnover >= MIN_TURNOVER) it walks
forward hour by hour over the last ~50 days and, at each hourly close, rebuilds what the
scanner would have seen, using the scanner's OWN functions from volume_spikes.py:

  tf_metrics (1h, 4h, 1d; 4h/1d with the forming candle projected like the live scan)
  spot ratio (Binance spot), OI change + oi_divergence (Binance OI history),
  funding (Binance funding history), BTC regime, detect_flush / score_flush / score_row,
  recommend, plan_from_candles + finish_plan (wide stop, level targets, front-run),
  the low-upside skip, and step_plan for the exits.

Each alert is graded twice: with the old exit model (⅓ / ⅓ / ⅓, break-even after TP1)
and the current one (½ / ¼ / ¼, lock half of TP1, stop ≥ TP1 after TP2).

Known gaps vs live (shown on the page):
  * 15m spikes are not replayed (would need 4× the requests); 1h/4h/1d are.
  * The live plan starts from the Analyze card; here it starts from plan_from_candles.
    finish_plan then applies the same wide-stop rules and real-level targets to both.
  * Exits are walked on 1h candles (live uses 15m); stop is checked first in each candle.
  * Binance only keeps ~20 days of hourly OI history: older alerts have no OI signal.
  * Universe = coins listed and liquid today (survivorship bias: delisted coins missing).
"""
import base64
import bisect
import copy
import gzip
import json
import os
import threading
import time

import volume_spikes as vs

BT_KEY = "vs_sysbt_v2"
BT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "system_backtest.json")
DAYS = int(os.environ.get("SBT_DAYS", 180))
REFRESH_EVERY = 3 * 86400

STATE = {"status": "idle", "progress": None, "result": None, "started": None, "error": None}
_LOCK = threading.Lock()

COLS = ["t", "sym", "tf", "rec", "setup", "score", "ratio", "spot", "oi_div", "btc", "vs_ema", "has_spot",
        "entry", "sl_pct", "tp1_pct", "tp2_pct", "tp3_pct", "rr1",
        "st", "r", "closed_t", "hits", "mfe", "mae",          # current exit model (½/¼/¼)
        "st_s", "r_s", "closed_s",                            # FADE: the same alert traded SHORT
        "run48",                                              # % above the 48h low at the alert (early vs late)
        "btc7"]                                               # BTC % change over the 7 days before (market regime)


def _kl(kind, sym, tf, n):
    """Last n candles, paging back through history (futures max 1500 per call, spot 1000)."""
    path = "/fapi/v1/klines" if kind == "fut" else "/api/v3/klines"
    page = 1500 if kind == "fut" else 1000
    out, end = [], None
    while len(out) < n:
        p = {"symbol": sym, "interval": tf, "limit": min(page, n - len(out))}
        if end:
            p["endTime"] = end
        k = vs._parse_klines(vs._get(kind, path, p))
        if not k:
            break
        out = k + out
        if len(k) < p["limit"]:
            break
        end = k[0][0] * 1000 - 1
    return out or None


def _oi(sym):
    j = vs._get("fut", "/futures/data/openInterestHist", {"symbol": sym, "period": "1h", "limit": 500})
    try:
        return sorted((int(x["timestamp"]) // 1000, float(x["sumOpenInterestValue"])) for x in j)
    except Exception:
        return None


def _funding(sym):
    j = vs._get("fut", "/fapi/v1/fundingRate", {"symbol": sym, "limit": 1000})
    try:
        return sorted((int(x["fundingTime"]) // 1000, float(x["fundingRate"])) for x in j)
    except Exception:
        return None


def _bucket(c1h, secs):
    """Aggregate 1h candles into `secs` candles (spot 4h/1d)."""
    out = []
    for k in c1h:
        b = (k[0] // secs) * secs
        if out and out[-1][0] == b:
            o = out[-1]; o[2] = max(o[2], k[2]); o[3] = min(o[3], k[3]); o[4] = k[4]; o[5] += k[5]
        else:
            out.append([b, k[1], k[2], k[3], k[4], k[5]])
    return out


class _Series:
    """Candles of one timeframe + fast 'closed before T' / 'forming at T' lookups."""

    def __init__(self, c, secs):
        self.c, self.secs = c or [], secs
        self.opens = [k[0] for k in self.c]

    def closed_idx(self, T):              # number of candles fully closed at T
        return bisect.bisect_right(self.opens, T - self.secs)

    def closed(self, T, n=vs.HIST_CANDLES):
        i = self.closed_idx(T)
        return self.c[max(0, i - n):i]


def _forming(c1h, i_end, T, secs):
    """The forming `secs` candle at T built from closed 1h candles (like live's 15m aggregate)."""
    cur = (T // secs) * secs
    if cur >= T:
        return None
    part = [k for k in c1h[max(0, i_end - 30):i_end] if k[0] >= cur]
    return vs._aggregate(part, cur) if part else None


def _btc_at(bd, b1, T):
    d = bd.closed(T, 40)
    i = b1.closed_idx(T)
    if len(d) < 31 or i == 0:
        return None
    price = b1.c[i - 1][4]
    today = (price / d[-1][4] - 1) * 100
    usual = sum(abs(d[j][4] / d[j - 1][4] - 1) * 100 for j in range(len(d) - 30, len(d))) / 30
    return {"regime": "big_down" if today <= -1.5 * usual else "big_up" if today >= 1.5 * usual else "normal"}


def _btc7(b1, T):
    i = b1.closed_idx(T)
    if i < 170:
        return None
    return round((b1.c[i - 1][4] / b1.c[i - 169][4] - 1) * 100, 1)


def _cheap_ratio(c, i, forming, T, secs):
    """Volume ratio without the full metrics (most hours have no spike)."""
    if i < vs.BASE_N + 1:
        return 0
    avg = sum(k[5] for k in c[i - vs.BASE_N - 1:i - 1]) / vs.BASE_N
    best = c[i - 1][5] / avg if avg > 0 else 0
    if forming:
        frac = (T - forming[0]) / secs
        avl = sum(k[5] for k in c[i - vs.BASE_N:i]) / vs.BASE_N
        if frac >= vs.LIVE_MIN_FRAC and avl > 0:
            best = max(best, forming[5] / frac / avl)
    return best


def replay_symbol(sym, F, btcser, t_from):
    f1, f4, fd = F["1h"], F["4h"], F["1d"]
    s1 = F.get("spot")
    spot = {"1h": _Series(s1, 3600), "4h": _Series(_bucket(s1, 14400), 14400),
            "1d": _Series(_bucket(s1, 86400), 86400)} if s1 else None
    oi = F.get("oi") or []
    oi_t = [x[0] for x in oi]
    fu = F.get("fund") or []
    fu_t = [x[0] for x in fu]
    ser = {"1h": f1, "4h": f4, "1d": fd}
    out, seen = [], set()
    c1 = f1.c
    for i in range(vs.BASE_N + 2, len(c1)):
        T = c1[i - 1][0] + 3600                    # hourly close
        if T < t_from:
            continue
        price = c1[i - 1][4]
        forming = {"1h": None, "4h": _forming(c1, i, T, 14400), "1d": _forming(c1, i, T, 86400)}
        cheap = {}
        for tf, s in ser.items():
            j = s.closed_idx(T)
            cheap[tf] = _cheap_ratio(s.c, j, forming[tf], T, s.secs) if j else 0
        if max(cheap.values()) < vs.SPIKE_RATIO:
            continue
        r = {"symbol": sym, "price": price, "tfs": {}, "spot": {}, "has_spot": None if s1 else False}
        for tf, s in ser.items():
            m = vs.tf_metrics(s.closed(T), forming[tf], s.secs, T, price)
            if m:
                r["tfs"][tf] = m
        if not r["tfs"]:
            continue
        # spot (Binance spot, same ratio maths), OI, funding, BTC
        for tf, m in r["tfs"].items():
            if m["ratio"] >= vs.SPIKE_RATIO and spot:
                sp = spot[tf]
                sform = _forming(s1, bisect.bisect_right(spot["1h"].opens, T - 3600), T, sp.secs) if tf != "1h" else None
                sm = vs.tf_metrics(sp.closed(T, vs.BASE_N + 2), sform, sp.secs, T, None)
                r["spot"][tf] = sm["ratio"] if sm else None
        if i > 4 and c1[i - 5][4]:
            r["px_chg4"] = round((price / c1[i - 5][4] - 1) * 100, 2)
        j = bisect.bisect_right(oi_t, T) - 1
        if j >= 1 and oi[j][0] > T - 7200:
            if oi[j - 1][1]:
                r["oi_chg1"] = round((oi[j][1] / oi[j - 1][1] - 1) * 100, 2)
            if j >= 4 and oi[j - 4][1]:
                r["oi_chg4"] = round((oi[j][1] / oi[j - 4][1] - 1) * 100, 2)
        r["oi_div"] = vs.oi_divergence(r.get("px_chg4"), r.get("oi_chg4"))
        k = bisect.bisect_right(fu_t, T) - 1
        r["funding"] = fu[k][1] if k >= 0 else None
        btc = _btc_at(btcser["1d"], btcser["1h"], T)

        for tf, m in r["tfs"].items():
            if m["ratio"] < vs.SPIKE_RATIO:
                continue
            fl = vs.detect_flush(r, tf) if m["dir"] != "long" else None
            if fl:
                m["setup"], m["flush"] = "flush", fl
                sc, _ = vs.score_flush(r, tf, btc, fl)
            else:
                sc, _ = vs.score_row(r, tf, btc)
            m["score"] = sc
        cand, cand_s = [], []
        for tf, m in r["tfs"].items():
            if m["ratio"] < vs.SPIKE_RATIO or m.get("score") is None or (tf, m["candle"]) in seen:
                continue
            if m["score"] < vs.ALERT_MIN_SCORE:
                continue
            if m["dir"] == "long" or m.get("setup") == "flush":
                cand.append(tf)
            elif m["dir"] == "short":
                cand_s.append(tf)                           # red spike: the SHORT system
        order = {t: n for n, t in enumerate(vs.TF_ORDER)}
        for tf in cand + cand_s:
            seen.add((tf, r["tfs"][tf]["candle"]))
        if cand_s:
            tf = max(cand_s, key=lambda x: (r["tfs"][x]["score"], order.get(x, 0)))
            row = _short_alert(r, tf, btc, ser, f1, T, price, c1, i, sym, btcser)
            if row:
                out.append(row)
        if not cand:
            continue
        tf = max(cand, key=lambda x: (r["tfs"][x]["score"], order.get(x, 0)))   # several TFs = one alert (live v82)
        m = r["tfs"][tf]
        if True:
            rc = vs.recommend(r, tf, btc)
            a = {"t": T - 1, "symbol": sym, "tf": tf, "price": price, "rec": rc["call"] if rc else None,
                 "setup": m.get("setup") or "momentum", "score": m["score"], "ratio": m["ratio"],
                 "spot": r["spot"].get(tf), "oi_div": r.get("oi_div"), "btc": (btc or {}).get("regime"),
                 "vs_ema": m.get("vs_ema"), "flush_low": m.get("lo") if m.get("setup") == "flush" else None,
                 "run48": round((price / min(k[3] for k in c1[max(0, i - 48):i]) - 1) * 100, 1),
                 "btc7": _btc7(btcser["1h"], T)}
            row = _plan_and_grade(a, ser, f1, T)
            if row:
                out.append(row)
    return out


FADE_RR = (0.7, 1.4, 2.5)      # short targets in R (stop = same width as the long plan's stop)


def short_rec(r, tf, btc):
    """The scanner's own short-side checklist (recommend() with long-only switched off)."""
    m = r["tfs"][tf]
    sc = m.get("score") or 0
    flags = []
    od, f, ve = r.get("oi_div"), r.get("funding") or 0, m.get("vs_ema")
    if od == "exhausting": flags.append("exhausting")
    if (btc or {}).get("regime") == "big_up": flags.append("btc up")
    if f < -0.0005: flags.append("shorts crowded")
    if ve is not None and ve > 0: flags.append("above ema200")
    sp = r["spot"].get(tf)
    if sp is not None and sp < 1.5: flags.append("futures-only")
    if sc >= 65 and not flags:
        return "short"
    if sc >= 50 and len(flags) <= 1:
        return "watch_short"
    return "skip"


def _short_alert(r, tf, btc, ser, f1, T, price, c1, i, sym, btcser):
    """Red high-volume spike traded SHORT: stop = the same wide-stop rules above entry, targets FADE_RR."""
    m = r["tfs"][tf]
    ptf = vs.PLAN_TF.get(tf, tf)
    c = ser[ptf].closed(T)
    atr = vs._atr(c) if c else None
    sl, _ = vs.wide_stop(price, ptf, atr)                 # long-side distance, mirrored above entry
    sd = (price - sl) / price
    i0 = f1.closed_idx(T)
    fut = f1.c[i0:i0 + 7 * 24 + 1]
    if not fut or sd <= 0:
        return None
    a4 = vs._atr(ser["4h"].closed(T))
    st, rr, ct = sim_short(price, sd, fut, a4, vs.EXIT_CFG)
    mfe = round((1 - min(k[3] for k in fut) / price) * 100, 2)      # best = deepest drop
    mae = round(-(max(k[2] for k in fut) / price - 1) * 100, 2)      # worst = highest rise (negative)
    tp = [round(-k * sd * 100, 2) for k in FADE_RR]
    hits = sum(1 for k in FADE_RR if min(x[3] for x in fut) <= price * (1 - k * sd))
    hi48 = max(k[2] for k in c1[max(0, i - 48):i])
    return [int(T), sym, tf, short_rec(r, tf, btc), "short", m["score"], m["ratio"], r["spot"].get(tf), r.get("oi_div"),
            (btc or {}).get("regime"), m.get("vs_ema"), r["spot"].get(tf) is not None,
            float(f"{price:.6g}"), round(sd * 100, 2), tp[0], tp[1], tp[2], FADE_RR[0],
            st, rr, ct, hits, mfe, mae,
            None, None, None,
            round((price / hi48 - 1) * 100, 1), _btc7(btcser["1h"], T)]


def sim_short(entry, sd, fut, atr4, cfg):
    """Fade the spike: SHORT at the alert price, stop sd above, targets FADE_RR × risk below, same
    management as the long plan (½/¼/¼, break-even after TP1, lock ½ of TP1, stop ≤ TP1 after TP2,
    3×ATR(4h) trail over the lowest low). 1h candles, stop checked first. Returns (status, r, closed_t)."""
    risk = entry * sd
    stop = entry + risk
    tps = [entry - k * risk for k in FADE_RR]
    sc = cfg["scale"]
    hits, ll = 0, entry
    for k in fut:
        hi, lo = k[2], k[3]
        if hi >= stop:
            r = sum(FADE_RR[i] * sc[i] for i in range(min(hits, 2))) + (1 - sum(sc[:min(hits, 2)])) * (entry - stop) / risk
            st = "sl" if hits == 0 else ("be" if abs(stop - entry) <= entry * 1e-9 else "trail")
            return st, round(r, 2), k[0]
        while hits < 3 and lo <= tps[hits]:
            hits += 1
        ll = min(ll, lo)
        if hits >= 1:
            stop = min(stop, entry)
            if cfg.get("lock") and ll <= entry - 1.5 * (entry - tps[0]):
                stop = min(stop, entry - 0.5 * (entry - tps[0]))
        if hits >= 2:
            if cfg.get("tp1_after_tp2"):
                stop = min(stop, tps[0])
            if atr4:
                stop = min(stop, ll + vs.TRAIL_ATR * atr4)
    last = fut[-1][4]
    r = sum(FADE_RR[i] * sc[i] for i in range(min(hits, 2))) + (1 - sum(sc[:min(hits, 2)])) * (entry - last) / risk
    done = len(fut) > 7 * 24
    return ("expired" if done else "open"), round(r, 2), (fut[-1][0] + 3600 if done else None)


def _plan_and_grade(a, ser, f1, T):
    entry = a["price"]
    ptf = vs.PLAN_TF.get(a["tf"], a["tf"])
    c = ser[ptf].closed(T)
    pl = vs.plan_from_candles(c, entry)
    if not pl:
        return None
    fl_low = a.get("flush_low")
    if fl_low and pl["sl"] > fl_low * 0.997:
        pl["sl"] = fl_low * 0.995
    pl = vs.finish_plan(pl, entry, vs._atr(c), min_tp1_pct=vs.MIN_TP1_PCT.get(ptf, 0.0) or 10.0,
                        c1h=ser["1h"].closed(T), c4h=ser["4h"].closed(T), c1d=ser["1d"].closed(T), tf=ptf)
    if pl["tp_pct"][0] < vs.MIN_TP1_PCT.get(ptf, 0.0):
        a["rec"] = "skip"                                   # live low-upside rule
    a["plan"] = pl
    i0 = f1.closed_idx(T)
    fut = f1.c[i0:i0 + 7 * 24 + 1]
    if not fut:
        return None
    end_t = fut[-1][0] + 3600
    now = end_t if len(fut) > 7 * 24 else end_t       # expiry fires only when 7 days have passed
    b = copy.deepcopy(a)
    b["pstate"] = {"status": "open", "hits": 0, "mfe": 0, "mae": 0, "checked": 0}
    vs.step_plan(b, fut, fut[-1][4], now, cfg=vs.EXIT_CFG, bar=3600)
    n = b["pstate"]
    closed = n["status"] in ("sl", "be", "trail", "expired")
    s_st, s_r, s_ct = sim_short(entry, abs(pl["sl_pct"]) / 100, fut, pl.get("trail_atr"), vs.EXIT_CFG)
    tp = pl["tp_pct"] + [None] * 3
    entry = float(f"{entry:.6g}")
    return [int(T), a["symbol"], a["tf"], a["rec"], a["setup"], a["score"], a["ratio"], a["spot"], a["oi_div"],
            a["btc"], a["vs_ema"], a["spot"] is not None,
            entry, pl["sl_pct"], tp[0], tp[1], tp[2], pl["rr"][0],
            n["status"] if closed else "open", n.get("r") if closed else n.get("open_r"), n.get("closed_t"),
            n["hits"], n.get("mfe"), n.get("mae"),
            s_st, s_r, s_ct,
            a.get("run48"), a.get("btc7")]


def _save(res):
    blob = json.dumps(res, separators=(",", ":"))
    try:
        with open(BT_PATH, "w") as fh:
            fh.write(blob)
    except Exception:
        pass
    try:
        if vs._redis():
            vs._redis_cmd("SET", BT_KEY, base64.b64encode(gzip.compress(blob.encode())).decode())
    except Exception as e:
        print(f"[sbt] redis save failed: {e}", flush=True)


def _ckpt_save(obj):
    try:
        if obj is None:
            os.path.exists(BT_PATH + ".ckpt") and os.remove(BT_PATH + ".ckpt")
            if vs._redis():
                vs._redis_cmd("DEL", BT_KEY + ":ckpt")
            return
        blob = json.dumps(obj, separators=(",", ":"))
        with open(BT_PATH + ".ckpt", "w") as fh:
            fh.write(blob)
        if vs._redis():
            vs._redis_cmd("SET", BT_KEY + ":ckpt", base64.b64encode(gzip.compress(blob.encode())).decode())
    except Exception as e:
        print(f"[sbt] checkpoint save failed: {e}", flush=True)


def _ckpt_load():
    try:
        if vs._redis():
            z = vs._redis_cmd("GET", BT_KEY + ":ckpt")
            if z:
                return json.loads(gzip.decompress(base64.b64decode(z)).decode())
        with open(BT_PATH + ".ckpt") as fh:
            return json.loads(fh.read())
    except Exception:
        return None


def _load():
    raw = None
    try:
        if vs._redis():
            z = vs._redis_cmd("GET", BT_KEY)
            if z:
                raw = gzip.decompress(base64.b64decode(z)).decode()
    except Exception as e:
        print(f"[sbt] redis load failed: {e}", flush=True)
    if not raw:
        try:
            with open(BT_PATH) as fh:
                raw = fh.read()
        except Exception:
            raw = None
    try:
        return json.loads(raw) if raw else None
    except Exception:
        return None


def run():
    with _LOCK:
        if STATE["status"] == "running":
            return False
        STATE.update(status="running", progress=0, started=time.time(), error=None)
    try:
        t0 = time.time()
        tick = vs.fetch_tickers() or {}
        uni = vs.fetch_universe() or set()
        syms = sorted(s for s, t in tick.items() if t["turnover24"] >= vs.MIN_TURNOVER and (not uni or s in uni))
        n1h = DAYS * 24 + 320
        btc = {"1d": _Series(_kl("fut", "BTCUSDT", "1d", DAYS + 60), 86400),
               "1h": _Series(_kl("fut", "BTCUSDT", "1h", n1h), 3600)}
        ck = _ckpt_load()                                   # resume after a restart / sleep
        if ck and ck.get("days") == DAYS and time.time() - ck.get("t0", 0) < 2 * 86400:
            t0, rows, finished, skipped = ck["t0"], ck["rows"], set(ck["done"]), ck.get("skipped", 0)
            print(f"[sbt] resuming: {len(finished)} coins already done", flush=True)
        else:
            rows, finished, skipped = [], set(), 0
        t_from = int(t0) - DAYS * 86400
        done = len(finished)
        for s in syms:
            if s in finished:
                continue
            try:
                F = {"1h": _Series(_kl("fut", s, "1h", n1h), 3600),
                     "4h": _Series(_kl("fut", s, "4h", DAYS * 6 + 320), 14400),
                     "1d": _Series(_kl("fut", s, "1d", DAYS + 320), 86400)}
                if len(F["1h"].c) < 100:
                    skipped += 1
                else:
                    F["spot"] = _kl("spot", s, "1h", n1h)
                    F["oi"] = _oi(s)
                    F["fund"] = _funding(s)
                    rows += replay_symbol(s, F, btc, t_from)
                F = None
            except Exception as e:
                print(f"[sbt] {s} failed: {e}", flush=True)
                skipped += 1
            done += 1
            finished.add(s)
            STATE["progress"] = round(100 * done / max(1, len(syms)))
            STATE["partial"] = len(rows)
            if done % 20 == 0:
                _ckpt_save({"days": DAYS, "t0": t0, "rows": rows, "done": sorted(finished), "skipped": skipped})
            time.sleep(0.1)
        rows.sort(key=lambda x: x[0])
        res = {"cols": COLS, "rows": rows,
               "meta": {"built": time.time(), "secs": round(time.time() - t0), "days": DAYS, "symbols": len(syms),
                        "skipped": skipped, "from": t_from, "to": int(t0),
                        "spike_ratio": vs.SPIKE_RATIO, "alert_min_score": vs.ALERT_MIN_SCORE,
                        "exit_new": "½ at TP1, ¼ at TP2, ¼ runner · BE after TP1 · lock ½ of TP1 once price runs 1.5× TP1 · stop ≥ TP1 after TP2 · 3×ATR(4h) trail",
                        "short_system": f"RED volume spikes (score ≥ alert minimum) traded SHORT: stop = the same wide-stop rules above entry, targets {FADE_RR} R, ½/¼/¼ management; setup = 'short'",
                        "fade": f"SHORT the same alert: stop = the long plan's stop distance above entry, targets {FADE_RR} R below, same ½/¼/¼ management",
                        "stops": {k: list(v) for k, v in vs.STOP_RULES.items()}}}
        _save(res)
        _ckpt_save(None)
        STATE.update(status="done", result=res, progress=100)
        print(f"[sbt] done: {len(rows)} alerts from {len(syms)} perps in {res['meta']['secs']}s", flush=True)
    except Exception as e:
        STATE.update(status="error", error=str(e))
        print(f"[sbt] failed: {e}", flush=True)
    return True


def payload(path=""):
    if "run" in path:
        if STATE["status"] != "running":
            threading.Thread(target=run, daemon=True).start()
            time.sleep(0.2)
    out = {k: STATE.get(k) for k in ("status", "progress", "started", "error", "partial")}
    if "status" not in path:
        out["result"] = STATE.get("result")
    return out


def _loop():
    time.sleep(240)                                  # let the live scanner warm up first
    res = _load()
    if res:
        STATE["result"] = res
        if STATE["status"] == "idle":
            STATE["status"] = "done"
    while True:
        r = STATE.get("result")
        if not r or time.time() - r["meta"]["built"] > REFRESH_EVERY or _ckpt_load():
            run()
        time.sleep(3600)


def start():
    threading.Thread(target=_loop, daemon=True).start()
