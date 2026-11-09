"""
Volume-spike scanner — backend for the "🔊 Volume spikes" tab.
==============================================================

ALL market data comes from BINANCE USD-M perpetuals (+ Binance spot for confirmation).
MEXC is only used to tag whether a coin can be traded there (one ticker call).

Per pair and per timeframe (15m · 1h · 4h · 1D):
  * Volume ratio = candle quote-volume (USDT) / average of the previous 20 candles.
      closed = last fully closed candle
      live   = forming candle projected to a full candle from its pace
               (only once >=20% of the candle has elapsed)
  * Direction of the spike candle, adaptive to the coin's typical candle body:
      long / short, or "absorb" (big volume, price barely moved)
  * Spot confirmation: same ratio on Binance SPOT. Spot leading futures = real flow;
    futures-only = leverage-driven.
  * Price vs EMA200 on that timeframe
  * Open interest (USD) + 4h / 24h change (Binance openInterestHist)
  * Funding, 24h quote volume, BTC regime (today vs BTC's usual daily move, 7d)

Every alert is logged with its entry price and graded after 1h / 4h / 24h so the
tab shows the scanner's REAL hit-rate rather than a claimed "odds" number.

Env vars (optional):
  VS_MIN_TURNOVER   min 24h futures quote volume to scan (default 5,000,000)
  VS_SPIKE_RATIO    ratio that counts as a spike         (default 2.5)
  VS_ALERT_SCORE    min score to log an alert            (default 45)
  VS_SCAN_EVERY     seconds between cycles               (default 300)
  VS_REQ_PER_SEC    Binance request rate cap             (default 6)
  VS_TELEGRAM=1     also push alerts through the dashboard's Telegram bot
"""
from __future__ import annotations

import json
import os
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor

import requests

MIN_TURNOVER = float(os.environ.get("VS_MIN_TURNOVER", 5_000_000))
SPIKE_RATIO = float(os.environ.get("VS_SPIKE_RATIO", 2.5))
ALERT_MIN_SCORE = int(os.environ.get("VS_ALERT_SCORE", 45))
SCAN_EVERY = int(os.environ.get("VS_SCAN_EVERY", 300))
REQ_PER_SEC = float(os.environ.get("VS_REQ_PER_SEC", 6))
TELEGRAM_ON = os.environ.get("VS_TELEGRAM", "").strip() in ("1", "true", "yes", "on")
LONG_ONLY = os.environ.get("VS_LONG_ONLY", "1").strip() not in ("0", "false", "no", "off")   # only log/recommend longs

# Binance geo-blocks fapi./api. from US cloud IPs; www.binance.com proxies the same
# public endpoints (the Alpha tab already relies on this). Hosts are tried in order and
# the first one that answers is remembered.
FUT_HOSTS = ["https://www.binance.com", "https://fapi.binance.com"]
SPOT_HOSTS = ["https://www.binance.com", "https://data-api.binance.vision", "https://api.binance.com"]
UA = {"User-Agent": "Mozilla/5.0"}

TFS = {"15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}
TF_ORDER = ["15m", "1h", "4h", "1d"]
BASE_N = 20
EMA_N = 200
HIST_CANDLES = 260          # 1h/4h/1d: EMA200 + baseline (weight 2 per call)
M15_CANDLES = 400           # 15m: EMA200 + covers the forming 1D candle (weight 2)
LIVE_MIN_FRAC = 0.20
HORIZONS = [("1h", 3600), ("4h", 14400), ("24h", 86400)]
DATA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "volume_spikes_alerts.json")


# ─────────────────────────────────────────────────────────────────────────────
# HTTP
# ─────────────────────────────────────────────────────────────────────────────
class _Limiter:
    def __init__(self, per_sec):
        self.gap = 1.0 / max(per_sec, 0.1)
        self.lock = threading.Lock()
        self.next_t = 0.0
        self.pause_until = 0.0

    def wait(self):
        with self.lock:
            now = time.monotonic()
            t = max(now, self.next_t, self.pause_until)
            self.next_t = t + self.gap
        d = t - time.monotonic()
        if d > 0:
            time.sleep(d)

    def pause(self, secs):
        with self.lock:
            self.pause_until = max(self.pause_until, time.monotonic() + secs)


LIMIT = _Limiter(REQ_PER_SEC)
_tls = threading.local()
_host_pref = {"fut": None, "spot": None}


def _sess():
    s = getattr(_tls, "s", None)
    if s is None:
        s = requests.Session()
        s.headers.update(UA)
        _tls.s = s
    return s


def _get(kind, path, params=None):
    hosts = FUT_HOSTS if kind == "fut" else SPOT_HOSTS
    pref = _host_pref[kind]
    order = ([pref] if pref else []) + [h for h in hosts if h != pref]
    for h in order:
        for attempt in range(2):
            LIMIT.wait()
            try:
                r = _sess().get(h + path, params=params, timeout=20)
            except Exception:
                time.sleep(1)
                continue
            if r.status_code in (418, 429):
                LIMIT.pause(int(r.headers.get("Retry-After") or 30))
                continue
            try:
                used = int(r.headers.get("X-MBX-USED-WEIGHT-1M") or r.headers.get("x-mbx-used-weight-1m") or 0)
                if used > 1800:
                    LIMIT.pause(20)
            except ValueError:
                pass
            if r.status_code == 400:          # bad symbol etc. — not a host problem
                _host_pref[kind] = h
                return None
            if r.status_code != 200:
                break                          # try next host
            try:
                j = r.json()
            except ValueError:
                break
            _host_pref[kind] = h
            return j
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Binance fetchers
# ─────────────────────────────────────────────────────────────────────────────
def fetch_universe():
    j = _get("fut", "/fapi/v1/exchangeInfo") or {}
    return {s["symbol"] for s in j.get("symbols", [])
            if s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT"
            and s.get("status") == "TRADING"}


def fetch_tickers():
    j = _get("fut", "/fapi/v1/ticker/24hr")
    out = {}
    for d in j or []:
        try:
            out[d["symbol"]] = {"price": float(d["lastPrice"]), "chg24": float(d["priceChangePercent"]),
                                "turnover24": float(d["quoteVolume"])}
        except (KeyError, TypeError, ValueError):
            pass
    return out


def fetch_funding():
    j = _get("fut", "/fapi/v1/premiumIndex")
    out = {}
    for d in j or []:
        try:
            out[d["symbol"]] = float(d.get("lastFundingRate") or 0)
        except (TypeError, ValueError):
            pass
    return out


def _parse_klines(j):
    if not isinstance(j, list) or not j:
        return None
    try:   # [open_ts_s, open, high, low, close, quote_volume]
        return [[int(k[0]) // 1000, float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[7])] for k in j]
    except (IndexError, TypeError, ValueError):
        return None


def fetch_fut_klines(sym, tf, n):
    return _parse_klines(_get("fut", "/fapi/v1/klines", {"symbol": sym, "interval": tf, "limit": n}))


def fetch_spot_klines(sym, tf, n):
    return _parse_klines(_get("spot", "/api/v3/klines", {"symbol": sym, "interval": tf, "limit": n}))


def fetch_oi_hist(sym):
    """Hourly OI value history (USD) → list of (ts_s, oi_usd)."""
    j = _get("fut", "/futures/data/openInterestHist", {"symbol": sym, "period": "1h", "limit": 25})
    if not isinstance(j, list):
        return None
    try:
        return [(int(x["timestamp"]) // 1000, float(x["sumOpenInterestValue"])) for x in j]
    except (KeyError, TypeError, ValueError):
        return None


def fetch_mexc_perps():
    try:
        r = requests.get("https://contract.mexc.com/api/v1/contract/ticker", timeout=15, headers=UA)
        return {d["symbol"].replace("_", "") for d in r.json().get("data", [])}
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Maths
# ─────────────────────────────────────────────────────────────────────────────
def _ema_last(closes):
    if len(closes) < EMA_N:
        return None
    k = 2 / (EMA_N + 1)
    e = sum(closes[:EMA_N]) / EMA_N
    for c in closes[EMA_N:]:
        e = c * k + e * (1 - k)
    return e


def _aggregate(m15, since):
    part = [k for k in m15 if k[0] >= since]
    if not part:
        return None
    return [since, part[0][1], max(k[2] for k in part), min(k[3] for k in part),
            part[-1][4], sum(k[5] for k in part)]


def tf_metrics(closed, forming, secs, now, price):
    if not closed or len(closed) < BASE_N + 1:
        return None
    last = closed[-1]
    base = closed[-(BASE_N + 1):-1]
    avg = sum(k[5] for k in base) / BASE_N
    if avg <= 0:
        return None
    bodies = [abs(k[4] - k[1]) / k[1] for k in base if k[1] > 0]
    avg_body = sum(bodies) / len(bodies) if bodies else 0
    e200 = _ema_last([k[4] for k in closed] + ([forming[4]] if forming else []))

    closed_ratio = last[5] / avg
    closed_chg = (last[4] / last[1] - 1) if last[1] else 0
    live_ratio = live_chg = frac = None
    if forming:
        avg_live = sum(k[5] for k in closed[-BASE_N:]) / BASE_N
        frac = min(1.0, max(0.0, (now - forming[0]) / secs))
        if frac >= LIVE_MIN_FRAC and avg_live > 0:
            live_ratio = forming[5] / frac / avg_live
            live_chg = (forming[4] / forming[1] - 1) if forming[1] else 0

    if live_ratio is not None and live_ratio >= closed_ratio:
        ratio, chg, mode, c_open = live_ratio, live_chg, "live", forming[0]
    else:
        ratio, chg, mode, c_open = closed_ratio, closed_chg, "closed", last[0]
    thr = max(avg_body * 0.5, 0.0015)
    direction = "long" if chg >= thr else "short" if chg <= -thr else "absorb"
    return {"ratio": round(ratio, 2), "mode": mode, "chg": round(chg * 100, 2), "dir": direction,
            "candle": c_open, "closed_ratio": round(closed_ratio, 2),
            "live_ratio": round(live_ratio, 2) if live_ratio is not None else None,
            "frac": round(frac, 2) if frac is not None else None,
            "vs_ema": round((price / e200 - 1) * 100, 2) if e200 and price else None}


def score_row(r, tf, btc):
    """0-100 quality score + reasons. Deliberately no 'odds' claim — see the tracker."""
    m = r["tfs"][tf]
    d = m["dir"]
    why = [f"vol {m['ratio']:.1f}× on {tf}{' (live)' if m['mode'] == 'live' else ''}"]
    s = min(40.0, (m["ratio"] - 1) * 10)
    others = [t for t in TF_ORDER if t != tf and r["tfs"].get(t) and r["tfs"][t]["ratio"] >= 2]
    if others:
        s += min(20, 10 * len(others)); why.append("+ also spiking on " + "/".join(others))
    sp = r["spot"].get(tf)
    if sp is not None:
        if sp >= m["ratio"]:
            s += 15; why.append(f"+ spot leads ({sp:.1f}×)")
        elif sp >= 2:
            s += 8; why.append(f"+ spot confirms ({sp:.1f}×)")
        elif sp < 1.5:
            s -= 10; why.append(f"− futures-only (spot {sp:.1f}×)")
    elif r.get("has_spot") is False:
        why.append("· no Binance spot market")
    if d == "absorb":
        why.append("· price flat → absorption, no direction")
        return max(0, min(100, round(s))), why
    long = d == "long"
    ve = m.get("vs_ema")
    if ve is not None:
        if (long and ve > 0) or (not long and ve < 0):
            s += 10; why.append(f"+ {'above' if long else 'below'} EMA200 {tf}")
        else:
            s -= 5; why.append(f"− against EMA200 {tf}")
    od = r.get("oi_div")
    if od:
        if long and od in ("real up", "accumulating"):
            s += 8; why.append(f"+ OI div: {od}")
        elif long and od == "fake pump":
            s -= 12; why.append("− OI div: fake pump (price up, OI down = short covering)")
        elif not long and od == "shorts piling":
            s += 8; why.append("+ OI div: shorts piling")
        elif not long and od == "exhausting":
            s -= 8; why.append("− OI div: exhausting (late short, bounce risk)")
    f = r.get("funding") or 0
    if long and f > 0.0005:
        s -= 10; why.append("− crowded longs (funding)")
    if not long and f < -0.0005:
        s -= 10; why.append("− crowded shorts (funding)")
    if btc and btc.get("regime"):
        rg = btc["regime"]
        if long and rg == "big_down":
            s -= 15; why.append("− BTC big down day")
        elif not long and rg == "big_up":
            s -= 15; why.append("− BTC big up day")
        elif (long and rg == "big_up") or (not long and rg == "big_down"):
            s += 5; why.append("+ with BTC")
    return max(0, min(100, round(s))), why


def oi_divergence(px4, oi4):
    """Price vs open-interest over the last 4h."""
    if px4 is None or oi4 is None:
        return None
    if abs(px4) <= 1 and oi4 >= 3:
        return "accumulating"      # price flat, leverage quietly stacking
    if px4 > 1 and oi4 >= 2:
        return "real up"           # new longs backing the move
    if px4 > 1 and oi4 <= -2:
        return "fake pump"         # short covering, tends to fade
    if px4 < -1 and oi4 >= 2:
        return "shorts piling"     # new shorts backing the drop
    if px4 < -1 and oi4 <= -2:
        return "exhausting"        # longs unwinding, bounce risk
    return "neutral"


def recommend(r, tf, btc):
    """Rule-based call for the best spike. Not advice — a checklist summary."""
    m = r["tfs"][tf]
    d, sc = m["dir"], m.get("score") or 0
    if d == "absorb":
        return {"call": "wait", "why": ["Big volume but price flat: wait for it to break one way"]}
    long = d == "long"
    if LONG_ONLY and not long:
        return {"call": "skip", "why": ["Short-side spike: long-only mode"]}
    flags = []
    od, f, ve = r.get("oi_div"), r.get("funding") or 0, m.get("vs_ema")
    rg = (btc or {}).get("regime")
    if long:
        if od == "fake pump": flags.append("OI falling while price rises (short covering)")
        if rg == "big_down": flags.append("BTC having a big down day")
        if f > 0.0005: flags.append("funding high: longs crowded")
        if ve is not None and ve < 0: flags.append(f"below EMA200 on {tf}")
    else:
        if od == "exhausting": flags.append("OI and price both falling: move may be exhausted")
        if rg == "big_up": flags.append("BTC having a big up day")
        if f < -0.0005: flags.append("funding negative: shorts crowded")
        if ve is not None and ve > 0: flags.append(f"above EMA200 on {tf}")
    sp = r["spot"].get(tf)
    if sp is not None and sp < 1.5: flags.append("futures-only volume (spot not confirming)")
    side = "long" if long else "short"
    if sc >= 65 and not flags:
        return {"call": side, "why": [f"Score {sc} with no red flags"]}
    if sc >= 50 and len(flags) <= 1:
        return {"call": "watch_" + side, "why": [f"Score {sc}"] + (["Red flag: " + flags[0]] if flags else ["Score not high enough yet"])}
    return {"call": "skip", "why": [f"Score {sc}"] + ["Red flag: " + x for x in flags]}


# ─────────────────────────────────────────────────────────────────────────────
# Alert log + forward test
# ─────────────────────────────────────────────────────────────────────────────
REDIS_KEY = "vspike_alerts"


def _redis():
    u = os.environ.get("UPSTASH_REDIS_REST_URL", "").strip().rstrip("/")
    t = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "").strip()
    return (u, t) if (u and t) else None


def _redis_cmd(*args):
    up = _redis()
    if not up:
        return None
    r = requests.post(up[0], json=list(args), headers={"Authorization": f"Bearer {up[1]}"}, timeout=15)
    r.raise_for_status()
    return r.json().get("result")


class _Tracker:
    """Alert log. Durable in Upstash Redis (survives Render restarts/redeploys),
    with a local JSON file as fallback. Writes are batched: once per scan cycle."""

    def __init__(self):
        self.lock = threading.Lock()
        self.alerts, self.seen = [], set()
        self.dirty = False
        self.source = "empty"
        raw = None
        try:
            raw = _redis_cmd("GET", REDIS_KEY)
            if raw:
                self.source = "redis"
        except Exception as e:
            print(f"[vspike] redis load failed: {e}", flush=True)
        if not raw:
            try:
                with open(DATA_PATH) as fh:
                    raw = fh.read()
                self.source = "file"
            except Exception:
                raw = None
        if raw:
            try:
                self.alerts = json.loads(raw)
                self.seen = {a["key"] for a in self.alerts}
            except Exception:
                self.alerts, self.source = [], "empty"
        print(f"[vspike] alert log: {len(self.alerts)} alerts loaded from {self.source}"
              f" ({'durable: Upstash' if _redis() else 'NOT durable: no Upstash env'})", flush=True)

    def _save(self):
        self.dirty = True                      # actual write happens in flush()

    def flush(self):
        with self.lock:
            if not self.dirty:
                return
            data = [{k: v for k, v in a.items() if k != "why"} | {"why": (a.get("why") or [])[:4]}
                    for a in self.alerts[-3000:]]
            self.dirty = False
        blob = json.dumps(data, separators=(",", ":"))
        try:
            if _redis():
                _redis_cmd("SET", REDIS_KEY, blob)
        except Exception as e:
            self.dirty = True
            print(f"[vspike] redis save failed: {e}", flush=True)
        try:
            tmp = DATA_PATH + ".tmp"
            with open(tmp, "w") as fh:
                fh.write(blob)
            os.replace(tmp, DATA_PATH)
        except Exception:
            pass

    def add(self, a):
        with self.lock:
            if a["key"] in self.seen:
                return False
            self.seen.add(a["key"]); self.alerts.append(a)
            self.alerts = self.alerts[-3000:]
            self._save()
            return True

    def grade(self, prices, now):
        changed = False
        with self.lock:
            for a in self.alerts:
                px = prices.get(a["symbol"])
                if not px:
                    continue
                for name, secs in HORIZONS:
                    if a["out"].get(name) is None and now >= a["t"] + secs:
                        if now > a["t"] + secs + 1800:
                            a["out"][name] = "na"          # app was asleep — don't fake it
                        else:
                            raw = (px / a["price"] - 1) * 100
                            a["out"][name] = round(-raw if a["dir"] == "short" else raw, 2)
                        changed = True
            if changed:
                self._save()

    def stats(self):
        with self.lock:
            al = [a for a in self.alerts if a["dir"] != "absorb"]
        groups = [("All directional", al)] + [(tf, [a for a in al if a["tf"] == tf]) for tf in TF_ORDER] + [
            ("Rec ✅ (long/short)", [a for a in al if a.get("rec") in ("long", "short")]),
            ("Rec 👀 watch", [a for a in al if str(a.get("rec", "")).startswith("watch")]),
            ("Rec ✗ skip", [a for a in al if a.get("rec") == "skip"]),
            ("Score ≥ 70", [a for a in al if a["score"] >= 70]),
            ("Spot leads", [a for a in al if a.get("spot_leads")]),
            ("Futures-only", [a for a in al if a.get("spot") is not None and a["spot"] < 1.5]),
            ("Longs", [a for a in al if a["dir"] == "long"]),
            ("Shorts", [a for a in al if a["dir"] == "short"]),
            ("Long on BTC big-down day", [a for a in al if a["dir"] == "long" and a.get("btc") == "big_down"]),
        ]
        out = []
        for g, rows in groups:
            row = {"group": g, "n": len(rows)}
            for name, _ in HORIZONS:
                v = [a["out"][name] for a in rows if isinstance(a["out"].get(name), (int, float))]
                row[name] = {"n": len(v), "win": round(100 * sum(1 for x in v if x > 0) / len(v)) if v else None,
                             "avg": round(sum(v) / len(v), 2) if v else None}
            out.append(row)
        return out

    def compact(self):
        keep = ("t", "symbol", "tf", "dir", "ratio", "mode", "score", "price", "chg", "spot", "spot_leads",
                "btc", "out", "mexc", "vs_ema", "n_tf", "rec", "oi_div", "plan", "pstate")
        with self.lock:
            return json.loads(json.dumps([{k: a.get(k) for k in keep} for a in self.alerts]))

    def recent(self, n=80):
        with self.lock:
            return json.loads(json.dumps(list(reversed(self.alerts[-n:]))))


# ─────────────────────────────────────────────────────────────────────────────
# Trade plans for alerts: 1 stop + 3-target ladder, tracked candle by candle
# ─────────────────────────────────────────────────────────────────────────────
PLAN_EXPIRY = 7 * 86400          # close whatever is left at market after 7 days
PLAN_MAX_AGE = 1800              # only plan alerts younger than 30 min (levels must fit the entry)


def _f(x):
    try:
        x = float(x)
        return x if x > 0 else None
    except (TypeError, ValueError):
        return None


def _atr(c, n=14):
    if len(c) < n + 1:
        return None
    trs = [max(c[i][2] - c[i][3], abs(c[i][2] - c[i - 1][4]), abs(c[i][3] - c[i - 1][4])) for i in range(len(c) - n, len(c))]
    return sum(trs) / n


def plan_from_analyze(az, entry):
    """Take the Analyze card's LONG plan (stop + targets) and re-measure RR from the alert entry."""
    if not isinstance(az, dict) or az.get("error"):
        return None
    lp = (az.get("plans") or {}).get("long") or (az if az.get("side") == "long" else None)
    if not lp:
        return None
    sl = _f(lp.get("sl_tight"))
    if not sl or sl >= entry:
        return None
    tps, basis = [], []
    for i in range(1, 6):
        tp = _f(lp.get(f"tp{i}"))
        if tp and tp > entry * 1.002 and (not tps or tp > tps[-1] * 1.001):
            tps.append(tp); basis.append((lp.get(f"tp{i}_basis") or "")[:120])
        if len(tps) == 3:
            break
    return {"sl": sl, "tps": tps, "basis": basis, "sl_basis": (lp.get("sl_tight_basis") or "")[:160], "src": "analyze"}


def plan_from_candles(c, entry):
    """Fallback when the coin can't be analyzed: stop under the recent swing low
    (ATR-buffered, at least 1.5 ATR away), targets at 1R / 2R / 3R."""
    if not c or len(c) < 20:
        return None
    atr = _atr(c)
    if not atr:
        return None
    swing = min(k[3] for k in c[-12:])
    sl = min(swing - 0.3 * atr, entry - 1.5 * atr)
    if sl <= 0 or sl >= entry:
        return None
    return {"sl": sl, "tps": [], "basis": [], "sl_basis": "Below the recent swing low (last 12 candles), ATR-buffered, at least 1.5×ATR from entry.",
            "src": "fallback"}


def finish_plan(plan, entry):
    """Fill the ladder to 3 targets with R-multiples if needed, then compute RR from entry."""
    risk = entry - plan["sl"]
    tps, basis = list(plan["tps"]), list(plan["basis"])
    k = 1.0
    while len(tps) < 3:
        nxt = entry + risk * max(k, ((tps[-1] - entry) / risk + 1) if tps else k)
        tps.append(nxt); basis.append(f"{round((nxt - entry) / risk, 1)}R extension (no resistance level found)")
        k += 1
    plan["tps"], plan["basis"] = tps[:3], basis[:3]
    plan["rr"] = [round((t - entry) / risk, 2) for t in plan["tps"]]
    plan["sl_pct"] = round((plan["sl"] / entry - 1) * 100, 2)
    plan["tp_pct"] = [round((t / entry - 1) * 100, 2) for t in plan["tps"]]
    return plan


def step_plan(a, candles, px, now):
    """Advance one alert's plan through new 15m candles. Thirds off at TP1/2/3,
    stop to break-even after TP1. Stop is checked first inside a candle (conservative)."""
    pl, st = a.get("plan"), a.get("pstate")
    if not pl or not st or st["status"] in ("tp3", "sl", "be", "expired", "invalid"):
        return False
    entry, risk = a["price"], a["price"] - pl["sl"]
    changed = False
    start = (int(a["t"]) // 900) * 900 + 900          # first candle fully after the alert
    for k in candles:
        if k[0] < start or k[0] < st.get("checked", 0):
            continue
        hi, lo = k[2], k[3]
        st["mfe"] = round(max(st.get("mfe", 0), (hi / entry - 1) * 100), 2)
        st["mae"] = round(min(st.get("mae", 0), (lo / entry - 1) * 100), 2)
        stop = entry if st["hits"] >= 1 else pl["sl"]
        if lo <= stop:
            left = 1 - st["hits"] / 3
            st["r"] = round(sum(pl["rr"][:st["hits"]]) / 3 + left * ((stop - entry) / risk), 2)
            st["status"] = "be" if st["hits"] >= 1 else "sl"
            st["closed_t"] = k[0]; changed = True
            break
        while st["hits"] < 3 and hi >= pl["tps"][st["hits"]]:
            st["hits"] += 1; st["status"] = f"tp{st['hits']}"; changed = True
            st.setdefault("hit_t", []).append(k[0])
        if st["hits"] == 3:
            st["r"] = round(sum(pl["rr"]) / 3, 2); st["closed_t"] = k[0]
            break
        if k[0] + 900 <= now:
            st["checked"] = k[0] + 900                    # only skip fully closed candles next time
    if px:
        st["last_px"] = px
        st["now_pct"] = round((px / entry - 1) * 100, 2)
        if st["status"] not in ("tp3", "sl", "be"):
            left = 1 - st["hits"] / 3
            st["open_r"] = round(sum(pl["rr"][:st["hits"]]) / 3 + left * (px - entry) / risk, 2)
            if now - a["t"] > PLAN_EXPIRY:
                st["r"] = st["open_r"]; st["status"] = "expired"; st["closed_t"] = now; changed = True
    return changed


# ─────────────────────────────────────────────────────────────────────────────
# Scanner
# ─────────────────────────────────────────────────────────────────────────────
class VolumeSpikeScanner:
    def __init__(self):
        self.lock = threading.Lock()
        self.tracker = _Tracker()
        self.hist = {}
        self.m15 = {}
        self.spot_cache = {}
        self.no_spot = set()
        self.oi_cache = {}            # sym → (fetched_at, series)
        self.universe_all, self.universe_t = set(), 0
        self.mexc, self.mexc_t = None, 0
        self.notify = None
        self.planner = None           # fn(symbol, tf) -> Analyze-card dict (injected by the dashboard)
        self.plan_q = deque()
        self.plan_busy = set()
        self.snap = {"rows": [], "btc": None, "status": "warming up — first pass takes ~3 min after boot",
                     "updated": None, "progress": None, "universe": 0}

    def _status(self, msg, progress=None):
        with self.lock:
            self.snap["status"] = msg; self.snap["progress"] = progress

    def _stale(self, sym, tf, now):
        secs = TFS[tf]
        h = self.hist.get((sym, tf))
        return not h or h[-1][0] < (int(now) // secs) * secs - secs

    def _refresh(self, sym, tf, now):
        k = fetch_fut_klines(sym, tf, HIST_CANDLES)
        if k:
            cur = (int(now) // TFS[tf]) * TFS[tf]
            self.hist[(sym, tf)] = [c for c in k if c[0] < cur][-HIST_CANDLES:]

    def _spot_ratio(self, sym, tf, m):
        if sym in self.no_spot:
            return None
        key = (sym, tf, m["candle"], m["mode"])
        if key in self.spot_cache:
            return self.spot_cache[key]
        k = fetch_spot_klines(sym, tf, BASE_N + 3)
        val = None
        if not k:
            self.no_spot.add(sym)
        elif len(k) >= BASE_N + 2:
            secs, now = TFS[tf], time.time()
            if m["mode"] == "live":
                cur, base = k[-1], k[-(BASE_N + 1):-1]
                avg = sum(x[5] for x in base) / BASE_N
                frac = max(LIVE_MIN_FRAC, min(1.0, (now - cur[0]) / secs))
                val = cur[5] / frac / avg if avg > 0 else None
            else:
                cur_open = (int(now) // secs) * secs
                closed = [x for x in k if x[0] < cur_open]
                if len(closed) >= BASE_N + 1:
                    avg = sum(x[5] for x in closed[-(BASE_N + 1):-1]) / BASE_N
                    val = closed[-1][5] / avg if avg > 0 else None
        val = round(val, 2) if val is not None else None
        if len(self.spot_cache) > 20000:
            self.spot_cache.clear()
        self.spot_cache[key] = val
        return val

    def _oi(self, sym, now):
        c = self.oi_cache.get(sym)
        if c and now - c[0] < 900:
            return c[1]
        ser = fetch_oi_hist(sym)
        self.oi_cache[sym] = (now, ser)
        return ser

    def _btc(self, price):
        d = self.hist.get(("BTCUSDT", "1d"))
        if not d or len(d) < 31 or not price:
            return None
        today = (price / d[-1][4] - 1) * 100
        usual = sum(abs(d[i][4] / d[i - 1][4] - 1) * 100 for i in range(len(d) - 30, len(d))) / 30
        week = (price / d[-7][4] - 1) * 100
        rg = "big_down" if today <= -1.5 * usual else "big_up" if today >= 1.5 * usual else "normal"
        return {"price": price, "today": round(today, 2), "usual": round(usual, 2),
                "week": round(week, 2), "regime": rg}

    def _plan_one(self, a):
        try:
            entry = a["price"]
            pl = None
            if self.planner:
                try:
                    pl = plan_from_analyze(self.planner(a["symbol"], a["tf"]), entry)
                except Exception as e:
                    print(f"[vspike] analyze failed for {a['symbol']}: {e}", flush=True)
            if not pl:
                c = self.hist.get((a["symbol"], a["tf"])) if a["tf"] != "15m" else self.m15.get(a["symbol"])
                pl = plan_from_candles(c, entry)
            with self.tracker.lock:
                if pl:
                    a["plan"] = finish_plan(pl, entry)
                    a["pstate"] = {"status": "open", "hits": 0, "mfe": 0, "mae": 0, "checked": 0}
                else:
                    a["plan"] = None
                    a["pstate"] = {"status": "invalid", "hits": 0}
                self.tracker.dirty = True
        finally:
            self.plan_busy.discard(a["key"])

    def plan_worker(self):
        while True:
            try:
                a = self.plan_q.popleft()
            except IndexError:
                time.sleep(2)
                continue
            self._plan_one(a)

    def track_plans(self, prices, now):
        with self.tracker.lock:
            todo = [a for a in self.tracker.alerts if a.get("dir") == "long"]
        for a in todo:
            if "pstate" not in a:
                if now - a["t"] <= PLAN_MAX_AGE and a["key"] not in self.plan_busy:
                    self.plan_busy.add(a["key"]); self.plan_q.append(a)
                elif now - a["t"] > PLAN_MAX_AGE and a["key"] not in self.plan_busy:
                    a["pstate"] = {"status": "invalid", "hits": 0, "why": "logged before plans existed"}
                    self.tracker.dirty = True
                continue
            if step_plan(a, self.m15.get(a["symbol"]) or [], prices.get(a["symbol"]), now):
                self.tracker.dirty = True

    def cycle(self):
        now = time.time()
        if now - self.universe_t > 6 * 3600 or not self.universe_all:
            u = fetch_universe()
            if u:
                self.universe_all, self.universe_t = u, now
        if now - self.mexc_t > 1800:
            m = fetch_mexc_perps()
            if m:
                self.mexc, self.mexc_t = m, now
        tick = fetch_tickers()
        if not tick:
            self._status("Binance ticker request failed — retrying next cycle")
            self.tracker.flush()
            return
        funding = fetch_funding()
        prices = {s: t["price"] for s, t in tick.items()}
        self.tracker.grade(prices, now)

        uni = [s for s, t in tick.items() if t["turnover24"] >= MIN_TURNOVER
               and (not self.universe_all or s in self.universe_all)]
        if "BTCUSDT" not in uni and "BTCUSDT" in tick:
            uni.append("BTCUSDT")

        jobs = [(s, tf) for s in uni for tf in ("1h", "4h", "1d") if self._stale(s, tf, now)]
        jobs += [(s, "15m") for s in uni]
        total = len(jobs)
        done = [0]
        self._status(f"fetching Binance candles — {total} requests, {len(uni)} pairs", 0)

        def work(job):
            s, tf = job
            if tf == "15m":
                k = fetch_fut_klines(s, "15m", M15_CANDLES)
                if k:
                    self.m15[s] = k
            else:
                self._refresh(s, tf, now)
            done[0] += 1
            if done[0] % 40 == 0:
                with self.lock:
                    self.snap["progress"] = round(100 * done[0] / max(total, 1))

        with ThreadPoolExecutor(max_workers=6) as ex:
            list(ex.map(work, jobs))

        now = time.time()
        btc = self._btc(prices.get("BTCUSDT"))
        rows = []
        for s in uni:
            m15 = self.m15.get(s)
            if not m15:
                continue
            t = tick[s]
            price = t["price"] or m15[-1][4]
            r = {"symbol": s, "base": s[:-4], "price": price, "chg24": round(t["chg24"], 2),
                 "turnover24": t["turnover24"], "funding": funding.get(s), "tfs": {}, "spot": {},
                 "mexc": (s in self.mexc) if self.mexc is not None else None,
                 "has_spot": None if s not in self.no_spot else False}
            cur15 = (int(now) // 900) * 900
            r["tfs"]["15m"] = tf_metrics([k for k in m15 if k[0] < cur15],
                                         m15[-1] if m15[-1][0] >= cur15 else None, 900, now, price)
            for tf in ("1h", "4h", "1d"):
                secs = TFS[tf]
                r["tfs"][tf] = tf_metrics(self.hist.get((s, tf)), _aggregate(m15, (int(now) // secs) * secs),
                                          secs, now, price)
            r["tfs"] = {k: v for k, v in r["tfs"].items() if v}
            if r["tfs"]:
                rows.append(r)

        # Spot + OI only for pairs that actually spiked (keeps request weight low)
        hot = [r for r in rows if any(m["ratio"] >= SPIKE_RATIO for m in r["tfs"].values())]
        self._status(f"spot + OI check for {len(hot)} spiking pairs")

        def enrich(r):
            for tf, m in r["tfs"].items():
                if m["ratio"] >= SPIKE_RATIO:
                    r["spot"][tf] = self._spot_ratio(r["symbol"], tf, m)
            if r["symbol"] in self.no_spot:
                r["has_spot"] = False
            m15 = self.m15.get(r["symbol"]) or []
            if len(m15) > 17 and m15[-17][4]:
                r["px_chg4"] = round((r["price"] / m15[-17][4] - 1) * 100, 2)
            ser = self._oi(r["symbol"], now)
            if ser:
                r["oi_usd"] = ser[-1][1]
                r["oi_pct_vol"] = round(100 * ser[-1][1] / r["turnover24"], 1) if r["turnover24"] else None
                if len(ser) >= 5:
                    r["oi_chg4"] = round((ser[-1][1] / ser[-5][1] - 1) * 100, 2) if ser[-5][1] else None
                if len(ser) >= 25:
                    r["oi_chg24"] = round((ser[-1][1] / ser[-25][1] - 1) * 100, 2) if ser[-25][1] else None
            r["oi_div"] = oi_divergence(r.get("px_chg4"), r.get("oi_chg4"))

        with ThreadPoolExecutor(max_workers=4) as ex:
            list(ex.map(enrich, hot))

        new = []
        for r in rows:
            best = None
            for tf, m in r["tfs"].items():
                if m["ratio"] < SPIKE_RATIO:
                    continue
                sc, why = score_row(r, tf, btc)
                m["score"], m["why"] = sc, why
                rank = sc + (1000 if (not LONG_ONLY or m["dir"] == "long") else 0)
                if best is None or rank > best[2]:
                    best = (tf, sc, rank)
                if sc >= ALERT_MIN_SCORE and (m["dir"] == "long" or not LONG_ONLY):
                    sp = r["spot"].get(tf)
                    a = {"key": f"{r['symbol']}|{tf}|{m['candle']}", "t": now, "symbol": r["symbol"],
                         "tf": tf, "dir": m["dir"], "ratio": m["ratio"], "mode": m["mode"], "score": sc,
                         "price": r["price"], "chg": m["chg"], "spot": sp,
                         "spot_leads": bool(sp is not None and sp >= m["ratio"]),
                         "btc": (btc or {}).get("regime"), "why": why, "out": {},
                         "mexc": r.get("mexc"), "vs_ema": m.get("vs_ema"), "n_tf": sum(1 for x in r["tfs"].values() if x["ratio"] >= 2)}
                    if self.tracker.add(a):
                        new.append(a)
            r["best_tf"], r["best_score"] = (best[0], best[1]) if best else (None, None)
            r["rec"] = recommend(r, best[0], btc) if best else None
            for a in new:
                if a["symbol"] == r["symbol"]:
                    a["oi_div"] = r.get("oi_div")
                    if r["rec"] and a["tf"] == best[0]:
                        a["rec"] = r["rec"]["call"]
            r["max_ratio"] = max(m["ratio"] for m in r["tfs"].values())
            r["n_spiking"] = sum(1 for m in r["tfs"].values() if m["ratio"] >= SPIKE_RATIO)

        if new:
            with self.tracker.lock:
                self.tracker._save()
        if self.notify and TELEGRAM_ON:
            for a in sorted(new, key=lambda x: -x["score"])[:10]:
                try:
                    self.notify(_tg_text(a, btc))
                except Exception:
                    pass

        try:
            self.track_plans(prices, time.time())
        except Exception as e:
            print(f"[vspike] plan tracking error: {e}", flush=True)
        self.tracker.flush()
        rows.sort(key=lambda r: (r["best_score"] if r["best_score"] is not None else -1, r["max_ratio"]), reverse=True)
        with self.lock:
            self.snap.update({"rows": rows, "btc": btc, "updated": now, "progress": None, "universe": len(uni),
                              "status": f"{len(uni)} Binance perps · {len(hot)} spiking ≥{SPIKE_RATIO}×"})
        print(f"[vspike] {len(uni)} pairs, {len(hot)} spiking, {len(new)} new alerts", flush=True)

    def loop(self):
        time.sleep(15)
        while True:
            t0 = time.time()
            try:
                self.cycle()
            except Exception as e:
                import traceback; traceback.print_exc()
                self._status(f"scan error: {e}")
            time.sleep(max(20, SCAN_EVERY - (time.time() - t0)))

    def payload(self, path=""):
        if path and "hist" in path:          # full alert history for the forward-test analysis
            return {"hist": self.tracker.compact(), "now": time.time(), "long_only": LONG_ONLY}
        with self.lock:
            snap = dict(self.snap)
        snap["alerts"] = self.tracker.recent()
        snap["stats"] = self.tracker.stats()
        snap["store"] = "upstash" if _redis() else "local file (resets on restart)"
        snap["cfg"] = {"spike_ratio": SPIKE_RATIO, "alert_min_score": ALERT_MIN_SCORE,
                       "min_turnover": MIN_TURNOVER, "scan_every": SCAN_EVERY, "telegram": TELEGRAM_ON,
                       "long_only": LONG_ONLY}
        return snap


def _tg_text(a, btc):
    arrow = {"long": "🟢 LONG", "short": "🔴 SHORT", "absorb": "⚪ ABSORPTION"}[a["dir"]]
    sp = f" · spot {a['spot']:.1f}×" if a.get("spot") is not None else ""
    b = f"\nBTC {btc['today']:+.1f}% today (usual ±{btc['usual']:.1f}%)" if btc else ""
    return (f"🔊 <b>VOLUME SPIKE — {a['symbol'][:-4]}</b> [{a['tf']}]\n"
            f"vol {a['ratio']:.1f}×{' (live)' if a['mode'] == 'live' else ''}{sp} · price {a['chg']:+.2f}%\n"
            f"{arrow} · score {a['score']}/100 · {a['price']:.6g}{b}\n" + "\n".join(a["why"][1:6]))


VSPIKE = VolumeSpikeScanner()


def start(notify=None, planner=None):
    """Call once from main(). `notify(text)` = optional Telegram sender.
    `planner(symbol, tf)` = the dashboard's analyze_symbol, used for alert stop/targets."""
    VSPIKE.notify = notify
    VSPIKE.planner = planner
    for _ in range(2):
        threading.Thread(target=VSPIKE.plan_worker, daemon=True, name="vspike-plan").start()
    threading.Thread(target=VSPIKE.loop, daemon=True, name="vspike").start()
