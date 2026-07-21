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
        bias_on_tf, enrich_1h, best_pattern, coil_tfs_finer, scalp_setup,
        bounce_scalp_setup, backtest_board, backtest_all,
        compute_market_context, fetch_deriv_series, backfill_market_history, ema, BACKTEST_BASKET,
        STOCK_BASKET,
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
# Performance tracker — the app's own forward test. Every setup the boards produce
# is recorded; each is then resolved as target-hit (win) or stop-hit (loss) against
# the live price feed, from the moment it was flagged. Gives real win-rate + average
# R per board. Persists to a JSON file, and to Upstash Redis if configured (durable
# across redeploys): set UPSTASH_REDIS_REST_URL + UPSTASH_REDIS_REST_TOKEN.
# The "epoch" the tracker is recording under. Bump this whenever the recommendation
# logic changes meaningfully — the headline win-rate resets to the new version (a clean
# slate for the new logic), while every past version's results are kept and shown in the
# "site version" breakdown so you can compare how each iteration actually performed.
APP_MODE = os.environ.get("APP_MODE", "crypto").strip().lower()   # "crypto" (default) or "stocks" — lets the SAME app run as two independent Render services
APP_VERSION = ("v25 · STOCKS site" if APP_MODE == "stocks" else "v25 · 5-Tool scenario matrix") + " — 10 variants, no RR gating, scale-out tested"
# One-time reset marker for the user's own "My calls" tracker. Bump this string to wipe
# every call (open + resolved) on the next boot and start the calls scorecard fresh —
# auto-board trades and their version history are untouched.
CALLS_RESET = "2026-07-17-v8b"
# ----------------------------------------------------------------------------
class Tracker:
    def __init__(self):
        self.lock = threading.Lock()
        self.open: dict[str, dict] = {}
        self.closed: list[dict] = []
        self.calls_reset = ""
        # After a setup resolves, don't re-track the SAME idea (same board:sym:side at
        # ~the same entry) for a cooldown window — otherwise a setup that keeps appearing
        # on the board gets re-taken and re-stopped every scan (compounding one loss).
        self.cooldowns: dict[str, dict] = {}
        # Per-board last-time-a-setup-appeared, for the "drought" auto-loosen: if a board
        # produces nothing for a couple of hours the gate is probably too tight, so relax it.
        self.board_seen: dict[str, float] = {}
        try:
            self.path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                     "apex_perf.json")
        except Exception:
            self.path = "apex_perf.json"
        self._load()

    @staticmethod
    def _cooldown_secs(tf):
        """After a setup fails, rest it for a MULTIPLE of its own timeframe — bigger
        multiple on faster timeframes, smaller on slower ones. So a 5m scalp rests ~40min,
        a 4h setup ~16h, a daily ~2 days. (base_seconds, multiple) per timeframe."""
        table = {"5m": (300, 8), "15m": (900, 6), "1h": (3600, 5), "4h": (14400, 4),
                 "1d": (86400, 2), "daily": (86400, 2), "1w": (604800, 1), "weekly": (604800, 1)}
        base, mult = table.get(str(tf or "").strip().lower(), (14400, 4))   # default 4h
        return base * mult

    def _up(self):
        u = os.environ.get("UPSTASH_REDIS_REST_URL", "").strip()
        t = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "").strip()
        return (u, t) if (u and t) else None

    def _load(self):
        raw = None
        up = self._up()
        if up:
            try:
                import requests
                r = requests.post(up[0], json=["GET", "apex_perf"],
                                  headers={"Authorization": f"Bearer {up[1]}"}, timeout=8)
                if r.status_code == 200:
                    raw = r.json().get("result")
            except Exception:
                raw = None
        if raw is None:
            try:
                with open(self.path) as f:
                    raw = f.read()
            except Exception:
                raw = None
        if raw:
            try:
                d = json.loads(raw)
                self.open = d.get("open", {})
                self.closed = d.get("closed", [])
                self.cooldowns = d.get("cooldowns", {})
                self.board_seen = d.get("board_seen", {})
                self.calls_reset = d.get("calls_reset", "")
                # Anything recorded before versioning belongs to the previous epoch —
                # tag it v1 so the current version's headline starts fresh but the old
                # results are kept and shown in the site-version breakdown.
                for t in list(self.open.values()) + self.closed:
                    if not t.get("ver"):
                        t["ver"] = "v1 · pre-gate"
                # One-time wipe on a version/reset bump: drop every manual "call" AND the
                # retired scalp/coil auto-board trades (their tabs are gone in v8), so the
                # scorecard starts clean on just Long + Short.
                _drop = {"call", "scalp", "coil"}
                if self.calls_reset != CALLS_RESET:
                    self.open = {k: t for k, t in self.open.items()
                                 if t.get("board") not in _drop}
                    self.closed = [t for t in self.closed if t.get("board") not in _drop]
                    self.calls_reset = CALLS_RESET
                    self._save()
            except Exception:
                pass

    def _save(self):
        blob = json.dumps({"open": self.open, "closed": self.closed[-1000:],
                           "cooldowns": self.cooldowns, "calls_reset": self.calls_reset,
                           "board_seen": self.board_seen})
        try:
            with open(self.path, "w") as f:
                f.write(blob)
        except Exception:
            pass
        up = self._up()
        if up:
            try:
                import requests
                requests.post(up[0], json=["SET", "apex_perf", blob],
                              headers={"Authorization": f"Bearer {up[1]}"}, timeout=8)
            except Exception:
                pass

    @staticmethod
    def _mk(board, sym, side, entry, stop, tps, px, now, tf="", note="", state="pending",
            regime="", ver=APP_VERSION):
        """Build a two-phase, scale-out-aware trade. Phase 1 = wait for the recommended
        ENTRY to fill (measured fairly from the fill, not from when it was flagged).
        Phase 2 = a full TP ladder with a stop that RATCHETS UP as targets bank (→ break-
        even after TP1), so once TP1 is reached it can't become a full loss."""
        entry, stop, px = float(entry), float(stop), float(px)
        risk = abs(entry - stop) or 1e-9
        clean = []
        for t in (tps or []):
            lvl = t.get("lvl")
            if lvl is None:
                continue
            rr = t.get("rr")
            clean.append({"lvl": float(lvl),
                          "rr": (float(rr) if rr is not None else round(abs(float(lvl) - entry) / risk, 2)),
                          "basis": t.get("basis") or ""})
        clean = clean[:6]                                  # track up to 6 TPs, each individually
        n = len(clean)
        # Adaptive scale-out: take MORE off the more-reachable (lower-R) near targets and
        # leave a runner on the far, high-R ones. Weight ∝ 1/(1+0.5·R), normalised — so
        # the split adapts to each setup's actual target spacing (never a fixed 40/35/25).
        raw = [1.0 / (1.0 + 0.5 * max(t.get("rr") or 1.0, 0.1)) for t in clean]
        tot = sum(raw) or 1.0
        w = [x / tot for x in raw] if n else [1.0]
        return {"board": board, "symbol": sym, "side": side, "entry": entry, "stop": stop,
                "tps": clean, "weights": w, "phase": 0, "realized": 0.0,
                "cur_stop": stop, "cur_stop_r": -1.0, "px0": px, "ts": now,
                "tf": tf, "note": note, "tps_hit": [], "rr": (clean[0]["rr"] if clean else None),
                "regime": regime, "ver": ver,
                "state": state, "fill_up": (entry > px), "filled_ts": (now if state == "active" else None)}

    def register(self, board, setups):
        """Record newly-appeared setups (full TP ladder), as PENDING — they'll be
        measured only once/if the recommended entry actually fills. Geometry must be
        valid (stop | entry | first target on the correct sides)."""
        now = time.time()
        changed = False
        with self.lock:
            for s in (setups or []):
                sym = s.get("symbol")
                side = s.get("side", "long")
                entry = s.get("entry")
                stop = s.get("stop")
                tps = s.get("tps") or ([{"lvl": s.get("target")}] if s.get("target") else [])
                px = s.get("live") or s.get("price") or entry
                tp1 = (tps[0].get("lvl") if tps else None)
                if not (sym and entry and stop and tp1 and px):
                    continue
                long = side != "short"
                if long and not (stop < entry < tp1):
                    continue
                if (not long) and not (tp1 < entry < stop):
                    continue
                k = f"{board}:{sym}:{side}"
                if k in self.open:
                    continue
                # Cooldown: skip if this SAME setup (≈same entry) resolved recently, so a
                # setup that keeps re-appearing isn't re-taken and re-stopped every scan.
                cd = self.cooldowns.get(k)
                if cd and now < cd.get("until", 0) \
                        and cd.get("entry") and abs(float(entry) - cd["entry"]) / float(entry) < 0.004:
                    continue
                self.open[k] = self._mk(board, sym, side, entry, stop, tps, px, now,
                                        tf=(s.get("tf") or s.get("entry_tf") or ""),
                                        regime=s.get("regime", ""))
                self.open[k]["entry_tf"] = s.get("entry_tf") or ""
                self.open[k]["stop_tf"] = s.get("stop_tf") or ""
                changed = True
        if changed:
            with self.lock:
                self._save()

    def add_call(self, symbol, side, targets, entry, stop, tf="", note=""):
        """Manually track a curated 'call' (from the Analyze tab), with its full TP
        ladder. `targets` is a comma-separated string of TP levels. Recorded with a
        unique key so the same coin can be called more than once."""
        try:
            entry, stop = float(entry), float(stop)
        except (TypeError, ValueError):
            return False
        tps = []
        for x in str(targets).split(","):
            x = x.strip()
            if x:
                try:
                    tps.append({"lvl": float(x)})
                except ValueError:
                    pass
        if not (symbol and entry and stop and tps):
            return False
        long = side != "short"
        t1 = tps[0]["lvl"]
        if long and not (stop < entry < t1):
            return False
        if (not long) and not (t1 < entry < stop):
            return False
        now = time.time()
        with self.lock:
            # A manual call = you're taking the trade now, so it starts ACTIVE.
            self.open[f"call:{symbol}:{int(now)}"] = self._mk(
                "call", symbol, side, entry, stop, tps, entry, now, tf, (note or "")[:120],
                state="active")
            self._save()
        return True

    def cooling(self, board, sym, side):
        """True if this exact setup was recently STOPPED OUT (in its loss cooldown) — used
        to hide a just-failed idea from the recommendation boards, not only the tracker."""
        cd = self.cooldowns.get(f"{board}:{sym}:{side}")
        return bool(cd and time.time() < cd.get("until", 0))

    def resolve(self, prices):
        """Scale-out-aware resolution. Bank each TP by its weight as price reaches it,
        ratcheting the stop up (break-even after TP1, TP1 after TP2 …). A stop-out
        BEFORE any TP = full −1R loss; after TP1 the trade closes at the banked R (≥0)
        — never a full loss. R = weighted realised R across the scaled-out position."""
        if not prices:
            return
        now = time.time()
        done = []
        with self.lock:
            for k, t in list(self.open.items()):
                p = prices.get(t["symbol"])
                if p is None:
                    if now - t["ts"] > 8 * 86400:
                        done.append((k, ("win" if t["realized"] > 0 else "expired"),
                                     round(t["realized"], 3)))
                    continue
                long = t["side"] != "short"
                tps = t["tps"]
                # PHASE 1 — wait for the recommended ENTRY to actually fill. Only then is
                # the trade measured (fairly, from the fill). If price runs to the first
                # target without ever filling, it's a MISS (not a loss). Pending expires.
                if t.get("state") == "pending":
                    filled = (p >= t["entry"]) if t.get("fill_up") else (p <= t["entry"])
                    tp1 = tps[0]["lvl"]
                    hit_tp1 = (p >= tp1) if long else (p <= tp1)
                    if filled:
                        t["state"] = "active"
                        t["filled_ts"] = now
                    elif hit_tp1:
                        done.append((k, "missed", 0.0)); continue
                    elif now - t["ts"] > 3 * 86400:
                        done.append((k, "expired", 0.0)); continue
                    else:
                        continue
                closed = False
                while True:
                    hit_stop = (p <= t["cur_stop"]) if long else (p >= t["cur_stop"])
                    ph = t["phase"]
                    nxt = tps[ph] if ph < len(tps) else None
                    hit_tp = nxt is not None and ((p >= nxt["lvl"]) if long else (p <= nxt["lvl"]))
                    if hit_stop and ph == 0:
                        done.append((k, "loss", -1.0)); closed = True; break
                    if hit_stop and ph >= 1:
                        rem = sum(t["weights"][ph:])
                        r = t["realized"] + rem * t["cur_stop_r"]
                        done.append((k, "win" if r > 1e-4 else "be" if abs(r) <= 1e-4 else "loss",
                                     round(r, 3))); closed = True; break
                    if hit_tp:
                        t["realized"] += t["weights"][ph] * (nxt["rr"] or 0)
                        t["tps_hit"].append(ph + 1)
                        t["phase"] = ph + 1
                        if t["phase"] >= len(tps):
                            done.append((k, "win", round(t["realized"], 3))); closed = True; break
                        if t["phase"] == 1:
                            t["cur_stop"], t["cur_stop_r"] = t["entry"], 0.0
                        else:
                            t["cur_stop"] = tps[t["phase"] - 2]["lvl"]
                            t["cur_stop_r"] = tps[t["phase"] - 2]["rr"]
                        continue
                    break
                if not closed and now - (t.get("filled_ts") or t["ts"]) > 7 * 86400:
                    if t["phase"] >= 1:
                        done.append((k, "win" if t["realized"] > 0 else "be", round(t["realized"], 3)))
                    else:
                        done.append((k, "expired", 0.0))
            for k, outcome, r in done:
                t = self.open.pop(k, None)
                if not t:
                    continue
                t["status"] = outcome
                t["r"] = round(r, 2)
                t["closed_ts"] = now
                self.closed.append(t)
                # Cooldown ONLY when the idea PLAYED OUT and failed/broke even (stopped) —
                # don't re-recommend a just-stopped setup. A MISSED/EXPIRED setup never
                # filled, so it's still a valid pending pullback: no cooldown, keep watching.
                if outcome in ("loss", "be"):
                    self.cooldowns[f"{t['board']}:{t['symbol']}:{t['side']}"] = {
                        "entry": t.get("entry"), "stop": t.get("stop"), "ts": now,
                        "until": now + self._cooldown_secs(t.get("tf")), "status": outcome}
            # Forget expired cooldowns so genuinely new setups can register again.
            self.cooldowns = {kk: v for kk, v in self.cooldowns.items()
                              if now < v.get("until", v.get("ts", 0) + 12 * 3600)}
            self.closed = self.closed[-1000:]
            if done:
                self._save()

    def learn_adjust(self, window=40, min_n=12):
        """LEARN FROM RESULTS. For each board, look at its most recent `window` resolved
        trades FROM THE CURRENT VERSION and nudge that board's quality bar by how the
        CURRENT logic is actually performing: a board that's bleeding gets a HIGHER bar
        (tighten); a board that's printing gets a slightly LOWER bar (let a working edge
        through). Crucially it is scoped to APP_VERSION — a board with no results yet in
        this version is NOT judged by an older version's (different) logic, so the new
        logic gets a clean run before the gate reacts. Returns per-board
        {n, exp, winrate, conv_delta, rr_delta, note} — deltas ADD to the floors."""
        with self.lock:
            res = [t for t in self.closed if t.get("status") in ("win", "loss", "be")
                   and t.get("ver", "v1 · pre-gate") == APP_VERSION]
        out = {}
        for b in ("long", "short"):
            rows = [t for t in res if t.get("board") == b][-window:]
            n = len(rows)
            if n < min_n:
                out[b] = {"n": n, "exp": None, "winrate": None, "conv_delta": 0.0,
                          "rr_delta": 0.0, "note": f"learning — {n}/{min_n} resolved"}
                continue
            exp = sum((t.get("r") or 0) for t in rows) / n
            wr = round(sum(1 for t in rows if (t.get("r") or 0) > 1e-4) / n * 100, 1)
            if exp <= -0.20:                        # losing → tighten (up to +8 conv / +0.3 R:R)
                f = min(1.0, (-exp) / 0.6)
                cd, rd = round(8.0 * f, 1), round(0.30 * f, 2)
                note = f"tightened — last {n} avg {exp:+.2f}R"
            elif exp >= 0.30:                       # winning → relax a touch
                f = min(1.0, (exp - 0.30) / 0.6)
                cd, rd = round(-5.0 * f, 1), round(-0.20 * f, 2)
                note = f"relaxed — last {n} avg {exp:+.2f}R"
            else:
                cd, rd = 0.0, 0.0
                note = f"steady — last {n} avg {exp:+.2f}R"
            out[b] = {"n": n, "exp": round(exp, 2), "winrate": wr,
                      "conv_delta": cd, "rr_delta": rd, "note": note}
        return out

    def note_board(self, board, n):
        """Record whether a board produced any setups this scan. Starts a clock the first
        time we ever see the board, and resets it whenever the board is non-empty — so the
        gap since the last non-empty scan is the board's 'drought'."""
        now = time.time()
        with self.lock:
            if board not in self.board_seen:
                self.board_seen[board] = now
            if n > 0:
                self.board_seen[board] = now

    def drought_relax(self, board, grace_h=2.0):
        """If a board has flagged NOTHING for a while, the gate is probably too tight —
        so progressively loosen it. Returns (conv_delta, rr_delta, hours) where the deltas
        are ≤0 (they lower the floors) and grow with the drought past `grace_h`, capped."""
        last = self.board_seen.get(board)
        if not last:
            return (0.0, 0.0, 0.0)
        h = (time.time() - last) / 3600.0
        over = h - grace_h
        if over <= 0:
            return (0.0, 0.0, round(h, 1))
        cd = -min(15.0, round(4.0 * over, 1))      # up to −15 conviction
        rd = -min(0.5, round(0.12 * over, 2))      # up to −0.5 R:R
        return (cd, rd, round(h, 1))

    def stats(self, prices=None):
        prices = prices or {}

        def _cur_r(t):
            """Live mark-to-market R for an OPEN, filled trade: banked R plus the open
            remainder marked at the current price (up or down). None if not filled/priced."""
            if t.get("state") != "active":
                return None
            p = prices.get(t.get("symbol"))
            if not p:
                return None
            long = t.get("side") != "short"
            risk = abs((t.get("entry") or 0) - (t.get("stop") or 0)) or 1e-9
            openr = (p - t["entry"]) / risk if long else (t["entry"] - p) / risk
            rem = sum(t.get("weights", [1.0])[t.get("phase", 0):]) or 0.0
            return round((t.get("realized") or 0) + rem * openr, 2)

        with self.lock:
            res_all = [t for t in self.closed if t.get("status") in ("win", "loss", "be")]
            # Headline + boards show only the CURRENT version (a clean slate for the new
            # logic). Past versions are kept and surfaced separately in by_version.
            res = [t for t in res_all if t.get("ver", "v1 · pre-gate") == APP_VERSION]

            def agg(rows):
                n = len(rows)
                if not n:
                    return {"n": 0, "wins": 0, "winrate": None, "exp": None, "sumR": 0, "tp_rates": []}
                w = sum(1 for t in rows if (t.get("r") or 0) > 1e-4)
                # Per-TP hit rate: % of these trades that reached at least TP1, TP2, TP3 …
                tp_hits, maxtp = {}, 0
                for t in rows:
                    hits = t.get("tps_hit") or []
                    hi = max(hits) if hits else 0
                    maxtp = max(maxtp, len(t.get("tps") or []))
                    for i in range(1, hi + 1):
                        tp_hits[i] = tp_hits.get(i, 0) + 1
                tp_rates = [{"tp": i, "n": tp_hits.get(i, 0), "rate": round(tp_hits.get(i, 0) / n * 100, 1)}
                            for i in range(1, maxtp + 1)]
                # "R if you took 100% at TPk" — reached TPk → +its R:R, else the −1R stop.
                allout = {}
                for t in rows:
                    mx = max(t.get("tps_hit") or [0])
                    for i, tp in enumerate(t.get("tps") or []):
                        allout.setdefault(i + 1, []).append((tp.get("rr") or 0) if mx >= i + 1 else -1.0)
                allout_exp = [{"tp": kk, "n": len(v), "exp": round(sum(v) / len(v), 2)}
                              for kk, v in sorted(allout.items())]
                return {"n": n, "wins": w, "winrate": round(w / n * 100, 1),
                        "exp": round(sum((t.get("r") or 0) for t in rows) / n, 2),
                        "sumR": round(sum((t.get("r") or 0) for t in rows), 2),
                        "tp_rates": tp_rates, "allout": allout_exp}
            _cf = ("board", "symbol", "side", "entry", "stop", "tps", "rr", "status",
                   "r", "ts", "closed_ts", "tf", "note", "px0", "tps_hit", "state",
                   "phase", "filled_ts", "regime", "ver", "realized", "cur_stop_r",
                   "entry_tf", "stop_tf")
            _pk = lambda t: {**{c: t.get(c) for c in _cf}, "cur_r": _cur_r(t)}
            _open_all = list(self.open.values())
            # Current-version open only. Old-version open trades keep resolving quietly in
            # the background (so they still score under their own version in by_version),
            # but they are HIDDEN from every live display — boards, counts, winners, MTM.
            _open_cur = [t for t in _open_all if t.get("ver", "v1 · pre-gate") == APP_VERSION]
            # User calls are the trader's own picks — never version-scoped, always live.
            _calls_all = [t for t in _open_all if t.get("board") == "call"]
            _active = sum(1 for t in _open_cur if t.get("state") == "active")
            _pending = sum(1 for t in _open_cur if t.get("state") == "pending")
            _missed = sum(1 for t in self.closed if t.get("status") == "missed"
                          and t.get("ver", "v1 · pre-gate") == APP_VERSION)
            _closed_rev = list(reversed(self.closed))
            _closed_cur = [t for t in _closed_rev if t.get("ver", "v1 · pre-gate") == APP_VERSION]
            # Per-board trade lists so each board row on the Performance tab can EXPAND
            # to show its own open (live + waiting) and resolved setups. Capped per board.
            # Current version only: this version's live/waiting trades plus its resolved
            # ones. Old-version open trades are hidden (they score in by_version instead).
            board_rows = {}
            for b in ("long", "short"):
                board_rows[b] = {
                    "open": [_pk(t) for t in _open_cur if t.get("board") == b][:80],
                    "closed": [_pk(t) for t in _closed_cur if t.get("board") == b][:60],
                }
            # Learning signal: win-rate WITH the market regime vs AGAINST it. If
            # against-regime setups bleed (as they should), this proves it in numbers.
            auto = [t for t in res if t.get("board") in ("long", "short")]
            by_regime = {r: agg([t for t in auto if (t.get("regime") or "neutral") == r])
                         for r in ("with", "against", "neutral")}
            # Per-site-version scoreboard (auto boards only) — compare how each iteration
            # of the recommendation logic actually performed. Newest version first.
            _auto_all = [t for t in res_all if t.get("board") in ("long", "short")]
            _vers, _seen = [], set()
            for t in reversed(self.closed):
                v = t.get("ver", "v1 · pre-gate")
                if v not in _seen:
                    _seen.add(v); _vers.append(v)
            by_version = [{"ver": v, "current": v == APP_VERSION,
                           **agg([t for t in _auto_all if t.get("ver", "v1 · pre-gate") == v])}
                          for v in _vers]
            # LIVE winners: current-version open trades already past TP1 — risk-free and
            # running. Version-scoped like the rest of the live view; old-version open
            # trades are hidden here even if they're still in profit.
            _livewin = [t for t in _open_cur if t.get("tps_hit")]
            _livewin.sort(key=lambda t: max(t.get("tps_hit") or [0]), reverse=True)
            live_realized = round(sum((t.get("realized") or 0) for t in _open_cur), 2)
            _res_sumR = agg(res).get("sumR") or 0
            # Mark-to-market: current unrealized R across current-version filled open trades.
            _active_cur = [t for t in _open_cur if t.get("state") == "active"]
            open_unreal = round(sum((_cur_r(t) or 0) for t in _active_cur), 2)
            _mtm_n = sum(1 for t in _active_cur if _cur_r(t) is not None)
            return {"overall": agg(res), "version": APP_VERSION,
                    "live_winners": [_pk(t) for t in _livewin][:80],
                    "live_realized": live_realized,
                    "live_count": len(_open_cur),
                    "open_unreal_R": open_unreal, "mtm_n": _mtm_n,
                    "combined_R": round(_res_sumR + open_unreal, 2),
                    # Auto boards are version-scoped (fresh scorecard per version); the
                    # user's own CALLS are never version-scoped — they span every version.
                    "by_board": {**{b: agg([t for t in res if t["board"] == b])
                                    for b in ("long", "short")},
                                 "call": agg([t for t in res_all if t.get("board") == "call"])},
                    "by_regime": by_regime,
                    "by_version": by_version,
                    "board_rows": board_rows,
                    "open": len(_open_cur), "active": _active, "pending": _pending,
                    "missed": _missed,
                    "upstash": bool(self._up()),
                    "recent": [_pk(t) for t in _closed_cur[:80]],
                    "calls_open": [_pk(t) for t in _calls_all],
                    "calls_closed": [_pk(t) for t in _closed_rev
                                     if t.get("board") == "call"][:60]}


TRACKER = Tracker()
_BT_CACHE: dict = {}          # (tf, fees) -> (ts, result) for the on-demand backtester


# Coins whose derivatives (OI / funding / price) we keep a rolling history for.
# Small, liquid set so the per-scan Coinalyze cost stays bounded (~4 calls each).
HIST_COINS = ["BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "AVAX", "LINK"]


class History:
    """Rolling historical context for Apex so it can reason about how things CHANGE
    over time, not just the current snapshot. Four series, all persisted to Upstash
    (durable across deploys) with a local-file fallback:

      • market   — the Market-tab read each scan (BTC score/verdict, day/week scores,
                    alt breadth %). Lets us see regime flips ('BTC turned bullish',
                    'breadth improving vs a week ago').
      • boards   — how many setups each board produced each scan (activity over time).
      • signals  — the top long/short leaders each scan (which coins keep ranking).
      • coins    — per-coin OI / funding / price time-series (from Coinalyze), backloaded
                   with ~7 days of history on first run, appended every scan after.
    """

    MAXP = 3000      # market/boards points (~20 days at 10-min scans)
    MAXSIG = 600     # signal snapshots
    MAXCP = 500      # per-coin series points

    def __init__(self):
        self.lock = threading.Lock()
        self.market: list[dict] = []
        self.boards: list[dict] = []
        self.signals: list[dict] = []
        self.coins: dict[str, dict] = {}   # sym -> {"oi":[[t,c]],"funding":[[t,c]],"price":[[t,c]]}
        self._coins_backloaded = False
        self._market_backfilled = False
        try:
            self.path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                     "apex_hist.json")
        except Exception:
            self.path = "apex_hist.json"
        self._load()

    def _up(self):
        u = os.environ.get("UPSTASH_REDIS_REST_URL", "").strip()
        t = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "").strip()
        return (u, t) if (u and t) else None

    def _load(self):
        raw = None
        up = self._up()
        if up:
            try:
                import requests
                r = requests.post(up[0], json=["GET", "apex_hist"],
                                  headers={"Authorization": f"Bearer {up[1]}"}, timeout=8)
                if r.status_code == 200:
                    raw = r.json().get("result")
            except Exception:
                raw = None
        if raw is None:
            try:
                with open(self.path) as f:
                    raw = f.read()
            except Exception:
                raw = None
        if raw:
            try:
                d = json.loads(raw)
                self.market = d.get("market", [])
                self.boards = d.get("boards", [])
                self.signals = d.get("signals", [])
                self.coins = d.get("coins", {})
                self._coins_backloaded = bool(self.coins)
                self._market_backfilled = bool(d.get("market_backfilled"))
            except Exception:
                pass

    def _save(self):
        blob = json.dumps({"market": self.market[-self.MAXP:],
                           "boards": self.boards[-self.MAXP:],
                           "signals": self.signals[-self.MAXSIG:],
                           "coins": self.coins,
                           "market_backfilled": self._market_backfilled})
        try:
            with open(self.path, "w") as f:
                f.write(blob)
        except Exception:
            pass
        up = self._up()
        if up:
            try:
                import requests
                requests.post(up[0], json=["SET", "apex_hist", blob],
                              headers={"Authorization": f"Bearer {up[1]}"}, timeout=8)
            except Exception:
                pass

    def record_scan(self, market_context, boards_summary, signals):
        """Append one point to the market/boards/signals series from a finished scan."""
        now = int(time.time())
        with self.lock:
            mc = market_context or {}
            btc = mc.get("btc") or {}
            alts = mc.get("alts") or {}
            day = mc.get("day") or {}
            week = mc.get("week") or {}
            self.market.append({
                "t": now,
                "btc": btc.get("score"), "btc_v": btc.get("verdict"),
                "btc_px": btc.get("price"),
                "day": (day.get("score")), "day_longs": day.get("longs"),
                "week": (week.get("score")), "week_longs": week.get("longs"),
                "alt_above": alts.get("pct_above_200ema"), "alt_v": alts.get("verdict"),
                "alt_chg": alts.get("avg_chg"),
            })
            self.boards.append({"t": now, **(boards_summary or {})})
            if signals:
                self.signals.append({"t": now, **signals})
            self.market = self.market[-self.MAXP:]
            self.boards = self.boards[-self.MAXP:]
            self.signals = self.signals[-self.MAXSIG:]
            self._save()

    def backfill_market(self, fetch_backfill):
        """One-time: seed the market series with MONTHS of reconstructed history from
        daily candles, so the regime chart is meaningful immediately. `fetch_backfill`
        is scanner.backfill_market_history. Historical (daily) points are merged ahead
        of the live (10-min) points; runs once, then persists the flag."""
        if self._market_backfilled:
            return
        try:
            pts = fetch_backfill()
        except Exception:
            pts = None
        if not pts:
            return
        with self.lock:
            have = {int(p["t"]) for p in self.market}
            hist = [p for p in pts if int(p["t"]) not in have]
            self.market = sorted(hist + self.market, key=lambda p: p["t"])[-self.MAXP:]
            self._market_backfilled = True
            self._save()

    def update_coins(self, fetch_series):
        """Refresh the per-coin OI/funding/price series. On first run backloads ~7 days
        of hourly history from Coinalyze; afterwards merges only the newest points.
        `fetch_series(display_symbol, bars)` is injected (scanner.fetch_deriv_series)."""
        bars = 168 if not self._coins_backloaded else 6
        got = {}
        for sym in HIST_COINS:
            try:
                s = fetch_series(sym, bars)
            except Exception:
                s = None
            if s:
                got[sym] = s
        if not got:
            return
        with self.lock:
            for sym, s in got.items():
                cur = self.coins.setdefault(sym, {"oi": [], "funding": [], "price": []})
                for key in ("oi", "funding", "price"):
                    merged = {int(t): c for t, c in cur.get(key, [])}
                    for t, c in s.get(key, []):
                        merged[int(t)] = c
                    cur[key] = [[t, merged[t]] for t in sorted(merged)][-self.MAXCP:]
            self._coins_backloaded = True
            self._save()

    def payload(self):
        with self.lock:
            # Compact per-coin summary + full series (series capped already).
            coins = {}
            for sym, s in self.coins.items():
                oi, fr, px = s.get("oi", []), s.get("funding", []), s.get("price", [])
                summ = {}
                if len(oi) >= 2 and oi[0][1]:
                    summ["oi_now"] = oi[-1][1]
                    summ["oi_chg_7d"] = round((oi[-1][1] / oi[0][1] - 1) * 100, 1)
                if fr:
                    summ["funding"] = fr[-1][1]
                if len(px) >= 2 and px[0][1]:
                    summ["price"] = px[-1][1]
                    summ["price_chg_7d"] = round((px[-1][1] / px[0][1] - 1) * 100, 1)
                coins[sym] = {"summary": summ, "oi": oi, "funding": fr, "price": px}
            return {"market": self.market[-800:], "boards": self.boards[-800:],
                    "signals": self.signals[-120:], "coins": coins,
                    "upstash": bool(self._up()),
                    "span": [self.market[0]["t"], self.market[-1]["t"]] if self.market else None}


HISTORY = History()


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
        self.dtb_hits: list[dict] = []             # RUNNER: descending triple bottom (daily)
        self.accum_hits: list[dict] = []           # RUNNER: test-pump -> accumulation -> breakout
        self.capit_hits: list[dict] = []           # RUNNER: capitulation flush -> trendline break
        self.long_filtered: bool = False
        self.top_combos: list = []                 # combos that survived out-of-sample
        self.micro_stats: dict | None = None       # sub-$10m market-cap cohort
        self.runner_progress: tuple = (0, 0)
        self.runner_diag: dict | None = None      # why the runner boards are empty
        self.signal_rank: dict | None = None       # SIGNAL LAB: which indicators actually pay
        self.prev_early_symbols: set[str] | None = None
        self.new_early_symbols: list[str] = []
        self.long_board: list[dict] = []           # top-25 best longs (whole universe)
        self.short_board: list[dict] = []          # top-25 best shorts (whole universe)
        self.coil_board: list[dict] = []           # top-25 most coiled (imminent big move)
        self.scalp_board: list[dict] = []          # top-25 LTF scalps (tight SL, HTF-aligned)
        self.spot_board: list[dict] = []           # top-15 spot buys (long-only, 1x, no funding)
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
        self.symbol_list: list = []
        self.market_context: dict | None = None    # BTC + alts big-picture read
        self.gate_learn: dict | None = None         # per-board gate adjustment from results
        self.backtests: dict | None = None          # auto TF × side backtest matrix
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

            def edge_map():
                """A COMPACT read of the latest backtest edge — expectancy / win-rate / trade-count
                per timeframe & side (plus the spot sweep) — so the LIVE boards can badge every
                proposal with the measured edge of its timeframe/side. 'Only propose winners' made
                visible: a setup on a timeframe the sweep proves NEGATIVE gets a ❌; a proven one ✅."""
                bt = self.backtests
                if not bt or not isinstance(bt, dict):
                    return None
                def cell(a):
                    a = a or {}
                    return {"exp": a.get("exp"), "n": a.get("n"), "wr": a.get("winrate")} if a.get("n") else None
                fut = {}
                for tf, sides in (bt.get("data") or {}).items():
                    c = {sd: cell((sides or {}).get(sd)) for sd in ("long", "short")}
                    c = {k: v for k, v in c.items() if v}
                    if c:
                        fut[tf] = c
                spot = {}
                for tf, sides in (bt.get("spot") or {}).items():
                    v = cell((sides or {}).get("long"))
                    if v:
                        spot[tf] = v
                return {"fut": fut, "spot": spot} if (fut or spot) else None

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
                "dtb_hits": withlive(self.dtb_hits),
                "accum_hits": withlive(self.accum_hits),
                "signal_rank": self.signal_rank,
                "runner_diag": self.runner_diag,
                "capit_hits": withlive(self.capit_hits),
                "micro_stats": self.micro_stats,
                "top_combos": self.top_combos,
                "long_filtered": self.long_filtered,
                "runner_progress": list(self.runner_progress),
                "early_new_symbols": list(self.new_early_symbols),
                "long_board": withlive(self.long_board),
                "short_board": withlive(self.short_board),
                "coil_board": withlive(self.coil_board),
                "scalp_board": withlive(self.scalp_board),
                "spot_board": withlive(self.spot_board),
                "market_context": self.market_context,
                "gate_learn": self.gate_learn,
                "bt_edge": edge_map(),
                "perf": TRACKER.stats(lp),
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
            state.symbol_list = list(symbols)
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
    long_board: list[dict] = []
    short_board: list[dict] = []
    coil_board: list[dict] = []
    dtb_board: list[dict] = []
    accum_board: list[dict] = []
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
            # BTC trend on the scan timeframe — keeps live reversion setups from fighting BTC
            # (mirrors the backtest's don't-fight-BTC filter). up / down / range.
            try:
                _be = ema(_bcl, 200)
                if _be and _be[-1] is not None:
                    _bp = _be[-21] if (len(_be) > 21 and _be[-21] is not None) else None
                    if _bcl[-1] > _be[-1] and (_bp is None or _be[-1] > _bp):
                        scan_cfg["btc_trend"] = "up"
                    elif _bcl[-1] < _be[-1] and (_bp is None or _be[-1] < _bp):
                        scan_cfg["btc_trend"] = "down"
                    else:
                        scan_cfg["btc_trend"] = "range"
            except Exception:
                pass
    except Exception:
        pass
    # Big-picture market read (BTC multi-TF + alt breadth) — good day/week for longs?
    try:
        _mc = compute_market_context(sess, scan_cfg["market"])
        with state.lock:
            state.market_context = _mc
    except Exception:
        pass
    with ThreadPoolExecutor(max_workers=cfg["workers"]) as ex:
        diag_tally = {"dtb": {}, "accum": {}}
        futs = {ex.submit(scan_symbol_multi, sess, s, cfg["interval"], scan_cfg): s
                for s in symbols}
        for fut in as_completed(futs):
            done += 1
            if done % 25 == 0:
                with state.lock:
                    state.progress = (done, len(symbols))
            try:
                rdiag, h, f, c, b, w, sh, sb, el, lng, sht, coil, dtb, accum = fut.result()
            except Exception:
                rdiag, h, f, c, b, w, sh, sb, el, lng, sht, coil, dtb, accum = (None,) * 14
            if rdiag:
                for _p in ("dtb", "accum"):
                    _r = rdiag.get(_p)
                    diag_tally[_p][_r if _r else "PASSED"] = diag_tally[_p].get(_r if _r else "PASSED", 0) + 1
            if dtb:
                dtb_board.append(dtb)
            if accum:
                accum_board.append(accum)
            if lng:
                _sig = lng.pop("sig", None)
                lng["lab_hits"] = []
                if _sig:
                    with state.lock:
                        _tc = list(state.top_combos)
                    _fire = [c["name"] for c in _tc
                             if c["keys"] and all(_sig.get(k) for k in c["keys"])]
                    lng["lab_hits"] = _fire
                    lng["lab_best"] = min([c["rank"] for c in _tc
                                           if c["name"] in _fire], default=None)
                long_board.append(lng)
            if sht:
                short_board.append(sht)
            if coil:
                coil_board.append(coil)
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

    # Universe-wide leaderboards: the 25 best longs and 25 best shorts across every
    # scanned pair (skip frozen/delisted feeds). These power the Top-setups tabs.
    # A top long/short must have a tradeable reward:risk (≥1) — a strong trend with no
    # room to run isn't a setup. Filter junk R:R, relaxing only if too few qualify.
    def _tradeable(board, floor):
        keep = [d for d in board if not d.get("data_stale")
                and d.get("rr") is not None and d.get("rr") >= floor]
        return keep
    _lb = [d for d in long_board if not d.get("data_stale")]
    _sb = [d for d in short_board if not d.get("data_stale")]
    long_board = _tradeable(_lb, 1.2) or _tradeable(_lb, 1.0) or _lb
    short_board = _tradeable(_sb, 1.2) or _tradeable(_sb, 1.0) or _sb
    coil_board = [d for d in coil_board if not d.get("data_stale")]
    # ---- REGIME-AWARE RANKING: fight the tape less. Blend the live market read
    # (intraday + swing verdict + alt breadth) into one score (−1 fully favours
    # shorts → +1 fully favours longs). Down-rank setups that fight it and tag every
    # row's alignment so we (a) surface with-regime setups first and (b) can later
    # measure with- vs against-regime win-rate (learning from our own losses).
    _mc = state.market_context or {}
    _day = (_mc.get("day") or {}).get("score")
    _week = (_mc.get("week") or {}).get("score")
    _alts = _mc.get("alts") or {}
    _breadth = (((_alts.get("pct_above_200ema") or 50) - 50) / 50.0)
    _reg = 0.45 * (_day or 0.0) + 0.35 * (_week or 0.0) + 0.20 * _breadth
    # Volatility regime tilt: in COMPRESSION (chop) mean-reversion bounces work and
    # breakouts fake out; in EXPANSION momentum/breakouts follow through. Small nudges.
    _volstate = ((_mc.get("vol_regime") or {}).get("state"))

    def _align_for(side):
        favor = _reg if side == "long" else -_reg
        return "with" if favor >= 0.20 else ("against" if favor <= -0.20 else "neutral")

    def _apply_regime(board, side):
        for d in board:
            bc = d.get("btc_corr")
            # A coin decorrelated from BTC trades on its own — BTC's regime doesn't apply,
            # so we DON'T penalise it (a great setup isn't dismissed just because BTC is soft).
            decor = bc is not None and abs(bc) < 0.4
            favor = _reg if side == "long" else -_reg   # >0 = regime favours this side
            align = "with" if favor >= 0.20 else ("against" if favor <= -0.20 else "neutral")
            if decor:
                align = "neutral"
                d["decor"] = True
                fac = 1.0
            else:
                # Down-rank (not exclude) against-regime; nudge with-regime up.
                fac = 1.15 if align == "with" else (0.85 if align == "against" else 1.0)
            d["regime"] = align
            d["regime_score"] = round(_reg, 3)
            d["score"] = d.get("score", 0) * fac
    _apply_regime(long_board, "long")
    _apply_regime(short_board, "short")
    long_board.sort(key=lambda d: d["score"], reverse=True)
    short_board.sort(key=lambda d: d["score"], reverse=True)
    coil_board.sort(key=lambda d: d["score"], reverse=True)

    # ---- QUALITY GATE: only RECOMMEND setups that clear a real bar. Fewer, cleaner
    # trades beat 25 forced ones. A setup must have genuine conviction AND R:R, must not
    # fight the regime (unless it's elite), and must not be chasing a stretched extreme.
    # If nothing qualifies, the board is intentionally empty ("no trade").
    #
    # The bar is ASYMMETRIC by regime: when the market read explicitly favours a side
    # (e.g. a shorts-favourable week), that side gets a little more benefit of the doubt
    # — a slightly lower conviction/R:R floor — because the tape is already on its side.
    # The side the regime is fighting keeps the full bar. Base floors are a touch lower
    # than before so a decent setup isn't held back in a merely-mixed tape.
    CONV_FLOOR, RR_FLOOR = 57.0, 1.7
    _learn = TRACKER.learn_adjust()                    # results → live gate adjustment
    # DROUGHT auto-loosen: if long/short has flagged nothing for >2h, the bar is too tight,
    # so relax it progressively (folded into the same per-board deltas the gate applies).
    for _sd in ("long", "short"):
        _dcd, _drd, _dh = TRACKER.drought_relax(_sd)
        if _dcd < 0:
            _l = _learn.setdefault(_sd, {"n": 0, "exp": None, "conv_delta": 0.0, "rr_delta": 0.0, "note": ""})
            _l["conv_delta"] = (_l.get("conv_delta") or 0.0) + _dcd
            _l["rr_delta"] = (_l.get("rr_delta") or 0.0) + _drd
            _l["drought_h"] = _dh
            _l["note"] = ((_l.get("note") or "").rstrip() + f" · auto-loosened (no setup {_dh:.1f}h)").lstrip(" ·")

    def _quality_keep(board, side):
        favor = _reg if side == "long" else -_reg      # >0 = regime favours this side
        favored = favor >= 0.20
        la = _learn.get(side, {})
        conv_floor = CONV_FLOOR - (5.0 if favored else 0.0) + (la.get("conv_delta") or 0.0)
        rr_floor = RR_FLOOR - (0.15 if favored else 0.0) + (la.get("rr_delta") or 0.0)
        kept = []
        for d in board:
            conv = d.get("conviction", 0) or 0
            rr = d.get("rr")
            reg = d.get("regime", "neutral")
            pv = d.get("pct_vs_ema")
            # REGIME-CONDITION THE SIDE. The backtest showed the mechanics have no edge
            # independent of drift — they flip sign with the tape. So take the side the
            # tape is actually on: don't short a clearly-bullish market or long a clearly-
            # bearish one (unless the coin is decorrelated from BTC and trades on its own).
            if not d.get("decor"):
                if side == "short" and _reg >= 0.15:
                    d["gate"] = "tape is bullish — not shorting into an up-trend"
                    continue
                if side == "long" and _reg <= -0.30:
                    d["gate"] = "tape is bearish — not longing into a down-trend"
                    continue
            # Vol-regime tilt: a BREAKOUT entry gets more room in expansion (breakouts
            # follow through) and a tougher bar in compression (they fake out in chop).
            if d.get("breakout") and _volstate == "expansion":
                conv += 5
            elif d.get("breakout") and _volstate == "compression":
                conv -= 5
            # Backtest finding: extended market/CMP entries (no clean pullback level) lose;
            # only limit-at-a-level entries have positive expectancy. Drop CMP (breakouts,
            # which are intentional stop-entries, are exempt).
            if d.get("entry_cmp"):
                d["gate"] = "no clean pullback (CMP entry — backtest shows these lose)"
                continue
            if rr is None or rr < rr_floor:
                d["gate"] = "low R:R"
                continue
            if conv < conv_floor:
                d["gate"] = "low conviction"
                continue
            # Don't auto-exclude against-regime setups — a strong one can still be worth it,
            # and decorrelated coins aren't judged by BTC at all. Just ask for MORE
            # conviction when a correlated coin fights the tape.
            if reg == "against" and not d.get("decor") and conv < conv_floor + 12:
                d["gate"] = "against regime (needs more conviction)"
                continue
            if side == "long" and pv is not None and pv > 30:
                d["gate"] = "over-extended"
                continue
            if side == "short" and pv is not None and pv < -30:
                d["gate"] = "over-extended"
                continue
            d["gate"] = "pass"
            kept.append(d)
        return kept

    long_board = _quality_keep(long_board, "long")[:25]
    short_board = _quality_keep(short_board, "short")[:25]

    # Coiled: don't force a squeeze that has no clear lean or no room to run. Require a
    # recommended side AND a tradeable R:R on that side's plan, plus a real squeeze score.
    def _coil_keep(board):
        _cl = _learn.get("coil", {})                # results → coil gate adjustment
        _c_rr = _cl.get("rr_delta") or 0.0
        _c_sc = _cl.get("conv_delta") or 0.0
        kept = []
        for d in board:
            rs = d.get("rec_side")
            pl = d.get("plan_long") if rs == "long" else d.get("plan_short") if rs == "short" else None
            rr = (pl or {}).get("rr")
            bc = d.get("btc_corr")
            decor = bc is not None and abs(bc) < 0.4
            reg = "neutral" if decor else (_align_for(rs) if rs else "neutral")
            floor = (55 if not (reg == "against") else 68) + _c_sc   # ask more of counter-regime coils
            if not rs or not pl or rr is None or rr < 1.8 + _c_rr or (d.get("score", 0) or 0) < floor:
                d["gate"] = "no clear coil"
                continue
            d["gate"] = "pass"
            kept.append(d)
        return kept
    coil_board = _coil_keep(coil_board)[:25]
    _mkt = cfg.get("market", "futures")
    # Complete the coiled-timeframes picture for the top coils only: 15m & 1h squeeze
    # (finer than the 4h scan) — a cheap second pass (~50 calls) just for these 25.
    if coil_board:
        with ThreadPoolExecutor(max_workers=8) as _ex:
            _futs = {_ex.submit(coil_tfs_finer, sess, d["symbol"], _mkt): d
                     for d in coil_board}
            for _f in as_completed(_futs):
                _d = _futs[_f]
                try:
                    _fine = _f.result()
                except Exception:
                    _fine = {}
                if _fine:
                    _d.setdefault("coiled_tfs", {}).update(_fine)

    # Best scalps: a 15m/5m tight setup taken WITH the higher-timeframe direction, for
    # the top HTF-graded candidates (long_board → long side, short_board → short side).
    # Second pass (2 fetches each) just for these — HTF gives direction, LTF gives the
    # tight entry/stop.
    scalp_src = {}
    for d in long_board:
        scalp_src[d["symbol"]] = ("long", d.get("conviction", 50), d.get("tf_bias"), d["score"])
    for d in short_board:
        s = d["symbol"]
        if s not in scalp_src or d["score"] > scalp_src[s][3]:
            scalp_src[s] = ("short", d.get("conviction", 50), d.get("tf_bias"), d["score"])
    scalp_board = []
    if scalp_src:
        with ThreadPoolExecutor(max_workers=8) as _ex:
            _sf = {_ex.submit(scalp_setup, sess, sym, _mkt, v[0], v[1], v[2]): sym
                   for sym, v in scalp_src.items()}
            for _f in as_completed(_sf):
                try:
                    _sc = _f.result()
                except Exception:
                    _sc = None
                if _sc:
                    scalp_board.append(_sc)
        scalp_board.sort(key=lambda d: d["score"], reverse=True)

    # Counter-trend / bounce scalps — a quick trade off a STRONG, tested lower-timeframe
    # level even when no clean HTF trend scalp exists. Long bounces come from the
    # support-bounce + supertrend-bounce hits (already-strong support); short fades from
    # bearish hits that may be popping into LTF resistance. bounce_scalp_setup self-
    # qualifies (must be at the level, with an oversold/overbought snap), so weak ones
    # drop out. This keeps the scalp tab from going empty: there's always a bounce.
    _have = {d["symbol"] for d in scalp_board}
    _bounce_src = {}
    for _r in (bounces + st_bounces):
        _s = _r["symbol"]
        if _s not in _have:
            _bounce_src[_s] = "long"
    for _r in shorts:
        _s = _r["symbol"]
        if _s not in _have and _s not in _bounce_src:
            _bounce_src[_s] = "short"
    if _bounce_src:
        with ThreadPoolExecutor(max_workers=8) as _ex:
            _bf = {_ex.submit(bounce_scalp_setup, sess, sym, _mkt, sd): sym
                   for sym, sd in _bounce_src.items()}
            for _f in as_completed(_bf):
                try:
                    _bc = _f.result()
                except Exception:
                    _bc = None
                if _bc:
                    scalp_board.append(_bc)
        scalp_board.sort(key=lambda d: d["score"], reverse=True)

    for _d in scalp_board:
        _d["regime"] = _align_for(_d.get("side", "long"))
        _d["regime_score"] = round(_reg, 3)

    # Scalps: don't force. Require a genuine grade, tradeable R:R, alignment with the
    # regime, and no chasing an already-exhausted RSI extreme on the entry timeframe.
    def _scalp_keep(board):
        _sl = _learn.get("scalp", {})               # results → scalp gate adjustment
        _s_rr = _sl.get("rr_delta") or 0.0
        _s_sc = _sl.get("conv_delta") or 0.0        # applied to the score floor
        # Vol-regime tilt: compression rewards mean-reversion BOUNCES (lower their bar,
        # raise the trend bar); expansion rewards TREND scalps (and vice-versa).
        _vr_bounce = -5.0 if _volstate == "compression" else (5.0 if _volstate == "expansion" else 0.0)
        _vr_trend = -5.0 if _volstate == "expansion" else (5.0 if _volstate == "compression" else 0.0)
        kept = []
        for d in board:
            rr = d.get("rr")
            rsi = d.get("rsi")
            long = d.get("side") != "short"
            against = d.get("regime") == "against" and not d.get("decor")
            # Counter-trend BOUNCE scalps are judged on the level's strength + the snap-back,
            # NOT on HTF/regime alignment (they're deliberately against the trend). Lower the
            # R:R floor and skip the against-regime penalty for them.
            if d.get("kind") == "bounce":
                if rr is None or rr < 1.2 + _s_rr or (d.get("score", 0) or 0) < 50 + _s_sc + _vr_bounce:
                    d["gate"] = "weak bounce"
                    continue
                d["gate"] = "pass"
                kept.append(d)
                continue
            if rr is None or rr < 1.4 + _s_rr or (d.get("score", 0) or 0) < (55 if not against else 66) + _s_sc + _vr_trend:
                d["gate"] = "weak scalp"
                continue
            if rsi is not None and ((long and rsi > 75) or ((not long) and rsi < 25)):
                d["gate"] = "RSI exhausted"
                continue
            d["gate"] = "pass"
            kept.append(d)
        return kept
    scalp_board = _scalp_keep(scalp_board)[:25]

    # Hide setups that JUST got stopped out (in their loss cooldown) from the boards —
    # a failed idea shouldn't keep being recommended. Missed/never-filled ones stay.
    long_board = [d for d in long_board if not TRACKER.cooling("long", d["symbol"], "long")]
    short_board = [d for d in short_board if not TRACKER.cooling("short", d["symbol"], "short")]
    scalp_board = [d for d in scalp_board if not TRACKER.cooling("scalp", d["symbol"], d.get("side", "long"))]
    coil_board = [d for d in coil_board if not TRACKER.cooling("coil", d["symbol"], d.get("rec_side") or "long")]

    # --- SPOT board: the best longs, reframed for a cash / spot buyer -----------------------
    # Spot = long-only, 1× (no leverage, so no liquidation) and no funding to bleed while you
    # hold. The ideal spot buy is therefore a graded uptrend long you can LIMIT-BUY on a dip to
    # support and calmly hold to the mean/target. Start from the already-vetted long_board and
    # keep only the spot-appropriate ones: a real pullback level to buy at (not a market chase),
    # tradeable R:R, and NOT already stretched far above the 200-EMA (don't buy the top). Score
    # rewards being close to support (buy the dip) and de-rates extension above the mean.
    spot_board = []
    for _d in long_board:
        if _d.get("entry_cmp"):                         # spot buyers place a limit at support, not a chase
            continue
        _rr = _d.get("rr") or 0
        if _rr < 1.2:
            continue
        _ext = _d.get("pct_vs_ema")
        if _ext is not None and _ext > 30:              # >30% above the 200-EMA = chasing; skip for spot
            continue
        _s = dict(_d)
        _s["spot"] = True
        _q = _d.get("score", 0) or 0
        _px = _d.get("price"); _nl = _d.get("near_level")
        _prox = 0.0
        if _nl and _px:
            _prox = max(0.0, 12.0 - abs(_px - _nl) / max(_px, 1e-9) * 100 * 2.0)   # nearer support = better
        _extpen = min(15.0, max(0.0, (_ext or 0) - 8) * 0.5) if _ext is not None else 0.0
        _liq = min(6.0, (_d.get("rvol") or 1.0) * 2.0)
        _s["spot_score"] = round(max(0.0, min(100.0, _q + _prox + _liq - _extpen)), 1)
        _s["hold"] = ("swing (days)" if _d.get("entry_tf") in ("4h", "1d", "Daily", "Weekly")
                      else "short swing (hours–days)")
        _s["note_spot"] = ("1× spot — you own the coin, so no funding and no liquidation. "
                           "Limit-buy the dip into support; the stop is a mental invalidation "
                           "(a place to trim), not a forced liquidation. Free to hold to target.")
        spot_board.append(_s)
    spot_board.sort(key=lambda d: d.get("spot_score", 0), reverse=True)
    spot_board = spot_board[:15]

    # --- CROSS-BOARD CONFLUENCE: how many independent signals line up on a recommended
    # coin. A top long that's ALSO a bull flag, a support bounce and a supertrend reclaim
    # is far higher-probability than one signal alone. Attach the count + the list, and
    # give confluent setups a modest rank boost so they surface to the top. ---
    _bull_sets = {
        "200-EMA reclaim": {h["symbol"] for h in hits},
        "Bull flag": {h["symbol"] for h in flags},
        "Narrow CPR": {h["symbol"] for h in cprs},
        "Support bounce": {h["symbol"] for h in bounces},
        "Supertrend bounce": {h["symbol"] for h in st_bounces},
        "Falling wedge": {h["symbol"] for h in wedges},
        "Early mover": {h["symbol"] for h in earlies},
    }
    _bear_sets = {"Breakdown / short setup": {s["symbol"] for s in shorts}}

    def _attach_conf(board, base_label, sets):
        for d in board:
            sym = d["symbol"]
            sigs = [base_label] + [lab for lab, members in sets.items() if sym in members]
            tb = d.get("tf_bias") or {}
            want = "bullish" if base_label == "Top long" else "bearish"
            if isinstance(tb, dict) and sum(1 for v in tb.values() if v == want) >= 3:
                sigs.append(f"Multi-TF {want}")
            n = len(sigs)
            d["confluence"] = {"n": n, "signals": sigs}
            if n >= 2:                                   # boost rank by up to ~+12% for 4 signals
                d["score"] = round((d.get("score", 0) or 0) * (1 + 0.04 * min(3, n - 1)), 1)
        board.sort(key=lambda x: x.get("score", 0), reverse=True)
    _attach_conf(long_board, "Top long", _bull_sets)
    _attach_conf(short_board, "Top short", _bear_sets)

    # Drought clock: record whether each board produced anything this scan (drives the
    # auto-loosen next scan if a board stays empty for hours).
    TRACKER.note_board("long", len(long_board))
    TRACKER.note_board("short", len(short_board))

    # Performance tracking — v8 is a lean two-sided book, so ONLY the Long and Short boards
    # are tracked. Scalps/Coiled are no longer registered (their tabs are hidden).
    TRACKER.register("long", long_board)
    TRACKER.register("short", short_board)

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
        # LONG TAB = STRATEGIES ONLY. Once the lab has produced surviving combos, the board shows
        # only coins one of them is firing on. Before the first lab cycle finishes there is nothing
        # to filter by, so the graded board is shown unfiltered rather than showing an empty tab.
        try:
            with state.lock:
                _have_combos = bool(state.top_combos)
            if _have_combos:
                _f = [b for b in long_board if b.get("lab_hits")]
                long_board = _f
                state.long_filtered = True
            else:
                state.long_filtered = False
        except Exception:
            pass
        state.long_board = long_board
        state.short_board = short_board
        state.coil_board = coil_board
        _unused_dtb = sorted(dtb_board, key=lambda d: -(d.get("rr") or 0))[:25]
        _unused_accum = sorted(accum_board, key=lambda d: -(d.get("rr") or 0))[:25]
        state.scalp_board = scalp_board
        state.spot_board = spot_board
        state.gate_learn = _learn
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

    # ---- Historical context: snapshot this scan into the durable time-series ----
    try:
        boards_summary = {
            "n_long": len(long_board), "n_short": len(short_board),
            "n_coil": len(coil_board), "n_scalp": len(scalp_board),
            "n_hits": len(hits), "n_flags": len(flags), "n_early": len(earlies),
            "universe": len(symbols), "n_conf": len(both),
        }
        def _top(board, k=8):
            return [{"s": d["symbol"], "sc": round(d.get("score", 0), 1)}
                    for d in board[:k]]
        signals = {"longs": _top(long_board), "shorts": _top(short_board),
                   "coils": _top(coil_board)}
        HISTORY.record_scan(state.market_context, boards_summary, signals)
    except Exception:
        pass
    # One-time: seed the regime chart with ~4 months of reconstructed daily history.
    try:
        HISTORY.backfill_market(lambda: backfill_market_history(sess, cfg.get("market", "futures"), 120))
    except Exception:
        pass
    # Per-coin OI/funding/price history (Coinalyze) — backloads on first run.
    try:
        HISTORY.update_coins(fetch_deriv_series)
    except Exception:
        pass


def scan_loop(state: State) -> None:
    every = state.cfg["scan_every"] * 60
    while True:
        run_one_scan(state)
        with state.lock:
            state.next_scan = time.time() + every
        time.sleep(every)


def backtest_loop(state: State) -> None:
    """Run the full TF × side backtest matrix in the background over the WHOLE universe and
    refresh it every few hours, so the Backtest tab always shows current results without the
    user triggering it. Heavy, so it runs on its own slow cadence and after a startup delay."""
    time.sleep(120)                                  # let the first scan grab the CPU first
    cfg = state.cfg
    market = cfg.get("market", "futures")
    # STRATEGY LAB — sweep several DIFFERENT strategies head-to-head so we can compare edges in
    # their own tabs instead of overwriting one strategy forever. Run on a liquid basket (~60
    # coins) so it actually completes on the free tier; the meaningful TFs only.
    # STRATEGY LAB jobs: (tabKey, displayName, strategyFn, market, indexSym, basket, timeframes).
    # Runs the premium + high-win-rate strategies on BOTH crypto (MEXC futures, BTC index) and
    # US stocks (Stooq daily, SPY index) so the two markets sit side-by-side in their own tabs.
    # SCENARIO MATRIX — 10 variants of the 5-tool confluence system, all tested on the SAME
    # candles (one download, many tests). 4H = the style's stated sweet spot.
    # PARKED: the 10-variant 5-tool matrix and the two runner backtests. They were the CPU hogs on
    # this free instance and the tab was not earning its keep. The code is untouched -- re-add the
    # keys here to switch it back on once the indicator work has settled.
    PARKED_SCENARIOS = [
        ("emaconf:base", "1 Base"), ("emaconf:scaled", "2 Scale-out"),
        ("emaconf:scaled_near", "3 Near target"), ("emaconf:scaled_tp05", "4 TP 0.5R"),
        ("emaconf:strongup", "5 Market up only"), ("emaconf:conf4", "6 Confluence 4"),
        ("emaconf:conf5", "7 Confluence 5"), ("emaconf:fibonly", "8 Fib only"),
        ("emaconf:emaonly", "9 EMA only"), ("emaconf:trail", "10 Trailing"),
        ("dtb", "11 Triple bottom"), ("accum", "12 Quiet accumulation"),
    ]
    EMACONF_SCENARIOS = [
        ("signals", "Signal lab (63 indicators)"),
    ]
    CRYPTO_JOBS = []
    STOCK_JOBS = [
        ("pro",    "Premium ★",     "pro",    "stocks", "SPY", list(STOCK_BASKET), ("1d",)),
        ("highwr", "High win-rate", "highwr", "stocks", "SPY", list(STOCK_BASKET), ("1d",)),
    ]
    lab_jobs = STOCK_JOBS if APP_MODE == "stocks" else CRYPTO_JOBS
    lab_limit = {"1h": 12000, "4h": 8760, "1d": 1460}
    strat_meta = ([{"key": k, "name": nm} for k, nm in EMACONF_SCENARIOS] if APP_MODE != "stocks" else []) \
        + [{"key": j[0], "name": j[1]} for j in lab_jobs]
    while True:
        try:
            sess = get_session()
            t0 = time.time()
            lab = {}
            total = len(lab_jobs)
            if APP_MODE != "stocks":
                # FULL UNIVERSE, streamed. Previously this ran on a 60-coin basket because the old
                # sweep held every coin's candles in memory at once; storing signals as bitmasks and
                # discarding candles per coin makes all ~600 tradeable pairs feasible here.
                from mexc_ema200_scanner import (signal_lab_stream, _bt_signal_ranking_mask,
                                                 list_symbols)
                try:
                    universe = list_symbols(sess, cfg.get("quote", "USDT"),
                                            futures_only=True, market=market)
                except Exception:
                    universe = list(BACKTEST_BASKET)

                def _prog(done, total, ntr):
                    with state.lock:
                        state.backtests = {"ts": time.time(), "running": True,
                                           "progress": {"done": done, "total": total,
                                                        "last": f"{ntr} trades so far"}}
                trades = signal_lab_stream(sess, market, universe, tf="4h", limit=8760,
                                           fees_bps=5.0, index_sym="BTCUSDT", progress=_prog)
                _rank = _bt_signal_ranking_mask(trades)
                del trades
                with state.lock:
                    state.signal_rank = _rank
                    _sv = (_rank or {}).get("survivors") or []
                    # only combos that cleared BOTH holdouts may drive the live board
                    state.top_combos = [{"name": e["name"], "keys": e.get("keys") or [],
                                         "oos_exp": e.get("hold_exp"), "rank": i + 1}
                                        for i, e in enumerate(_sv) if e.get("keys")]
                    state.backtests = {"ts": time.time(), "took": round(time.time() - t0, 1),
                                       "fees_bps": 5.0, "coins": (_rank or {}).get("coins", 0),
                                       "lab": {}, "strategies": strat_meta,
                                       "progress": {"done": 1, "total": 1, "last": "done"}}
            for ji, (tabkey, jname, strat, jmarket, jindex, jbasket, jtfs) in enumerate(lab_jobs):
                sdata = {}
                for tf in jtfs:
                    part = backtest_all(sess, jmarket, tfs=(tf,), limit=lab_limit.get(tf, 1500),
                                        fees_bps=5.0, symbols=jbasket, strategy=strat, index_sym=jindex)
                    sdata.update(part)
                    lab[tabkey] = dict(sdata)
                    with state.lock:
                        state.backtests = {"ts": time.time(), "took": round(time.time() - t0, 1),
                                           "fees_bps": 5.0, "coins": len(jbasket), "lab": dict(lab),
                                           "strategies": strat_meta,
                                           "progress": {"done": ji, "total": total, "last": f"{jname} · {tf}"}}
            with state.lock:
                if state.backtests and "lab" in state.backtests:
                    state.backtests["progress"] = {"done": total, "total": total, "last": "done"}
        except Exception as e:
            with state.lock:
                if not state.backtests:
                    state.backtests = {"error": str(e)}
        time.sleep(6 * 3600)                          # lighter now the matrix is parked


def _micro_summary(micro, universe, mapped, ambiguous):
    """What the sub-$10m coins are actually doing, versus the universe as a whole."""
    n = len(micro)
    if not n:
        return {"n": 0, "universe": universe, "mapped": mapped, "ambiguous": ambiguous}
    return {"n": n, "universe": universe, "mapped": mapped, "ambiguous": ambiguous,
            "dtb": sum(1 for m in micro if m["dtb"]),
            "accum": sum(1 for m in micro if m["accum"]),
            "capit": sum(1 for m in micro if m["capit"]),
            "any": sum(1 for m in micro if m["dtb"] or m["accum"] or m["capit"]),
            "median_mcap": round(sorted(m["mcap"] for m in micro)[n // 2]),
            "coins": sorted(micro, key=lambda m: m["mcap"])[:40]}


def runner_loop(state: State) -> None:
    """RUNNER-PATTERN loop on REAL DAILY candles, run slowly and politely.

    Why this exists: the 10-minute scan aggregates its 4h candles into only ~166 daily bars. Quiet-
    accumulation looks for a pump that can be 6-12 months old, so that loop was structurally blind
    to the setups it was supposed to find - which is very likely why both boards read zero. These
    are daily patterns; they do not change minute to minute, so this runs every few hours and sleeps
    between coins to leave CPU for serving pages."""
    from mexc_ema200_scanner import scan_runner_daily, fetch_market_caps, _cap_bucket
    market = state.cfg.get("market", "futures")
    while True:
        try:
            sess = get_session()
            with state.lock:
                syms = list(state.symbol_list or [])
            if not syms:
                time.sleep(120); continue
            dtb_b = []; acc_b = []; cap_b = []; tally = {"dtb": {}, "accum": {}}
            try:
                caps, ambiguous = fetch_market_caps(sess)
            except Exception:
                caps, ambiguous = {}, {}
            micro = []
            for i, sym in enumerate(syms):
                try:
                    d, a, cp, dg = scan_runner_daily(sess, sym, market)
                    _mc = caps.get(sym.replace("USDT", "").upper())
                    _bk = _cap_bucket(_mc)
                    for _h in (d, a, cp):
                        if _h:
                            _h["mcap"] = _mc; _h["cap_bucket"] = _bk
                    if _mc is not None and _mc < 10_000_000:
                        micro.append({"symbol": sym, "mcap": _mc,
                                      "dtb": bool(d), "accum": bool(a), "capit": bool(cp)})
                    if d:
                        dtb_b.append(d)
                    if a:
                        acc_b.append(a)
                    if cp:
                        cap_b.append(cp)
                    if dg:
                        for p in ("dtb", "accum"):
                            k = dg.get(p) or "PASSED"
                            tally[p][k] = tally[p].get(k, 0) + 1
                except Exception:
                    pass
                if i % 10 == 0:
                    with state.lock:
                        state.runner_progress = (i, len(syms))
                time.sleep(0.25)                  # breathe: keep the page responsive
            with state.lock:
                state.dtb_hits = sorted(dtb_b, key=lambda d: -(d.get("rr") or 0))[:25]
                state.accum_hits = sorted(acc_b, key=lambda d: -(d.get("rr") or 0))[:25]
                state.capit_hits = sorted(cap_b, key=lambda d: -((d.get("confidence") or 0) * 10
                                                                 + (d.get("rr") or 0)))[:25]
                state.micro_stats = _micro_summary(micro, len(syms), len(caps), len(ambiguous))
                state.runner_diag = {p: sorted(v.items(), key=lambda kv: -kv[1])[:6]
                                     for p, v in tally.items()}
                state.runner_progress = (len(syms), len(syms))
        except Exception as e:
            with state.lock:
                state.runner_diag = {"error": str(e)}
        time.sleep(4 * 3600)


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
        TRACKER.resolve(prices)          # close any tracked setups that hit target/stop
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
  .azresult{max-width:1600px}
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
  .ladrunner{border-color:#a371f7!important;background:rgba(163,113,247,.12)!important;box-shadow:0 0 0 1px rgba(163,113,247,.3)}
  .ladsecure{border-color:rgba(63,185,80,.4)!important}
  .ladstretch{border-color:rgba(227,179,65,.45)!important;background:rgba(227,179,65,.07)!important}
  .soplan{margin:8px 0 4px;padding:11px 15px;border:1px solid var(--line2);border-radius:12px;background:var(--bg2)}
  .sohead{font-size:10.5px;font-weight:800;letter-spacing:.05em;color:var(--dim2);text-transform:uppercase;margin-bottom:6px}
  .solist{margin:0;padding-left:0;list-style:none}
  .solist li{font-size:13px;margin:4px 0;color:var(--txt);padding-left:16px;position:relative}
  .solist li:before{content:"›";position:absolute;left:2px;color:var(--accent);font-weight:700}
  .solist li b{color:var(--txt)}
  .soBE{color:var(--accent);font-weight:600}
  .socompact{background:transparent;border:0;padding:8px 0 0;margin:6px 0 0}
  .socompact .sohead{margin-bottom:4px}
  .dcaplan{border-color:rgba(88,166,255,.35);background:rgba(88,166,255,.06)}
  .dcaplan .solist li:before{color:#58a6ff}
  .dcaavg{margin-top:7px;padding-top:7px;border-top:1px dashed var(--line2);font-size:12.5px;color:var(--txt)}
  .dcaplan.socompact{background:transparent;border:0}
  .xtfcmp{font-size:12.5px;color:var(--txt);margin-top:8px;padding-top:7px;border-top:1px solid var(--line)}
  .xtfcmp b{color:var(--accent)}
  .xtfsub{font-size:11px;color:var(--dim2);font-weight:600;margin-left:auto}
  .xtftrade{padding:9px 0 7px}
  .xtftrade+.xtftrade{border-top:1px dashed var(--line)}
  .xtftrade:not(.xtftrade-top){opacity:.9}
  .xtftrade:not(.xtftrade-top) .xtfrow{font-size:12.5px}
  .xtfhead2{display:flex;flex-wrap:wrap;align-items:center;gap:6px 10px;margin-bottom:6px}
  .xtfrank{font-weight:800;font-size:14px;color:var(--dim)}
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
  .rnk{color:var(--dim);font-weight:700;font-variant-numeric:tabular-nums;text-align:center;width:30px}
  .tfstripcell{white-space:nowrap}
  .scorepill{display:inline-block;border-radius:6px;padding:1px 9px;font-size:12.5px;font-weight:800;
             border:1px solid var(--line);font-variant-numeric:tabular-nums;min-width:34px;text-align:center}
  .scorepill.sc-a{background:rgba(63,185,80,.18);color:var(--accent);border-color:rgba(63,185,80,.55)}
  .scorepill.sc-b{background:rgba(240,180,41,.16);color:#f0b429;border-color:rgba(240,180,41,.5)}
  .scorepill.sc-c{background:rgba(139,152,173,.14);color:#c3ccd8}
  .scorepill.sc-d{background:rgba(139,152,173,.08);color:var(--dim)}
  td.rrg b{color:var(--accent)} td.rry b{color:#f0b429} td.rrd{color:var(--dim)}
  .derivbox{margin:9px 0 2px;padding:9px 13px;border-radius:10px;font-size:13px;border:1px solid var(--line2);background:var(--bg2)}
  .derivbox.derivgood{border-color:rgba(63,185,80,.5);background:rgba(63,185,80,.08)}
  .derivbox.derivwarn{border-color:rgba(240,180,41,.55);background:rgba(240,180,41,.09)}
  .derivbox.derivneu{border-color:var(--line2)}
  .coilset{white-space:nowrap;font-variant-numeric:tabular-nums}
  .coilrec-l{background:rgba(63,185,80,.13);box-shadow:inset 3px 0 0 var(--accent);font-weight:700}
  .coilrec-s{background:rgba(248,81,73,.13);box-shadow:inset 3px 0 0 #f85149;font-weight:700}
  .ctf{display:inline-block;border-radius:5px;padding:0 5px;font-size:10px;font-weight:700;margin-left:4px;cursor:help;font-variant-numeric:tabular-nums;border:1px solid var(--line)}
  .ctf-hot{background:rgba(63,185,80,.2);color:var(--accent);border-color:rgba(63,185,80,.55)}
  .ctf-warm{background:rgba(240,180,41,.16);color:#f0b429;border-color:rgba(240,180,41,.5)}
  .ctf-cool{background:rgba(139,152,173,.1);color:var(--dim)}
  .expander{display:inline-block;color:var(--accent);font-weight:800;width:12px;cursor:pointer;transition:transform .1s}
  tr.rowsel{background:rgba(63,185,80,.06)}
  .rmax{color:var(--dim2);font-size:11px;font-weight:600}
  .confb{display:inline-block;margin-left:6px;padding:1px 6px;border-radius:5px;font-size:10.5px;font-weight:800;cursor:help;vertical-align:middle;border:1px solid rgba(210,153,34,.45);background:rgba(210,153,34,.14);color:#e3b341}
  .confb.cf3{border-color:rgba(63,185,80,.5);background:rgba(63,185,80,.16);color:#8ddf9c}
  .confb.cf4{border-color:rgba(63,185,80,.7);background:rgba(63,185,80,.24);color:#adf7bd}
  .cbadge{display:inline-block;margin-left:6px;padding:1px 6px;border-radius:5px;font-size:10px;font-weight:700;letter-spacing:.02em;background:rgba(88,166,255,.14);color:#79b8ff;border:1px solid rgba(88,166,255,.35);cursor:help;vertical-align:middle}
  .cbadge.cbounce{background:rgba(210,153,34,.16);color:#e3b341;border-color:rgba(210,153,34,.4)}
  .btbar{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin:10px 0 14px;font-size:13px;color:var(--dim)}
  .btseg{display:inline-flex;border:1px solid var(--line);border-radius:8px;overflow:hidden}
  .btopt{padding:5px 12px;cursor:pointer;font-size:12.5px;font-weight:600;color:var(--dim)}
  .btopt.btsel{background:var(--accent);color:#04110a}
  .btfees{width:64px;background:var(--bg2);border:1px solid var(--line);border-radius:7px;color:var(--fg);padding:5px 8px;font-size:12.5px}
  .btrun{background:var(--accent);color:#04110a;border:0;border-radius:8px;padding:7px 16px;font-weight:800;cursor:pointer;font-size:13px}
  .btrun:disabled{opacity:.6;cursor:default}
  .btmeta{color:var(--dim2);font-size:12px}
  .btgrid{display:flex;gap:16px;flex-wrap:wrap}
  .btcard{flex:1;min-width:420px;border:1px solid var(--line);border-radius:12px;padding:12px 14px;background:rgba(139,152,173,.03)}
  .btcard-h{font-size:14px;font-weight:800;margin-bottom:10px;display:flex;align-items:center;gap:10px}
  .btverdict{font-size:11px;font-weight:700;padding:2px 8px;border-radius:6px;border:1px solid var(--line)}
  .btnote,.btcard .histnote{color:var(--dim)}
  .btmatrix td,.btmatrix th{font-size:13.5px}
  .btmatrix tbody td:first-child{font-family:var(--mono)}
  .bttf>summary{cursor:pointer;padding:9px 12px;border:1px solid var(--line);border-radius:9px;background:var(--bg2);font-weight:700;font-size:13px;margin-top:8px;list-style:none}
  .bttf>summary::-webkit-details-marker{display:none}
  .bttf[open]>summary{border-bottom-left-radius:0;border-bottom-right-radius:0}
  .btanalysis{margin:10px 0 4px;display:flex;flex-direction:column;gap:5px}
  .btidea{font-size:12.5px;color:#c3ccd8;padding:6px 10px;border-radius:7px;background:rgba(139,152,173,.06);border:1px solid var(--line)}
  .btidea.btbad{border-color:rgba(248,81,73,.4);background:rgba(248,81,73,.07)}
  .btideah{font-size:12px;font-weight:800;letter-spacing:.03em;color:var(--dim);margin-top:6px}
  .learnbox{margin:10px 0 4px;padding:10px 12px;border:1px solid var(--line);border-radius:10px;background:var(--bg2)}
  .learnhd{font-size:12px;color:var(--dim);margin-bottom:8px}
  .lchips{display:flex;flex-wrap:wrap;gap:6px}
  .lchip{font-size:11px;padding:3px 9px;border-radius:6px;border:1px solid var(--line);background:var(--bg);cursor:help;font-variant-numeric:tabular-nums}
  .lchip b{font-weight:700}
  .lchip.lt{border-color:rgba(248,81,73,.4);color:#f0a5a0}
  .lchip.lr{border-color:rgba(63,185,80,.4);color:#8ddf9c}
  .lchip.ls{color:var(--dim)}
  .planrow>td{background:var(--bg2);padding:0 14px 12px 40px;border-top:0}
  .planpair{display:flex;gap:16px;flex-wrap:wrap}
  .planpanel{flex:1;min-width:320px;margin-top:10px;padding:12px 15px;border:1px solid var(--line2);border-radius:12px;background:var(--panel);font-family:"Inter",sans-serif}
  .planpanel.pp-long{border-left:3px solid var(--accent)}
  .planpanel.pp-short{border-left:3px solid #f85149}
  .pphead{font-size:12px;font-weight:800;letter-spacing:.03em;margin-bottom:8px}
  .pline{display:flex;align-items:baseline;gap:8px;flex-wrap:wrap;font-size:13px;margin:3px 0;font-variant-numeric:tabular-nums}
  .pline .plab{display:inline-block;min-width:44px;color:var(--dim);font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.04em}
  .pline b{color:var(--txt)}
  .pbasis{color:var(--dim);font-size:12px}
  .pmv{color:var(--dim2);font-size:12px;font-variant-numeric:tabular-nums}
  .pmv.risk{color:#f0b429}
  .prr{color:var(--accent);font-weight:700;font-size:12px;min-width:44px}
  .ptps{margin:8px 0 4px;padding-top:8px;border-top:1px dashed var(--line2)}
  .ptpsh{font-size:10.5px;font-weight:800;letter-spacing:.05em;color:var(--dim2);text-transform:uppercase;margin-bottom:4px}
  .ptp .plab{min-width:36px;color:var(--accent)}
  .pscale{margin-top:8px;font-size:12.5px;color:#c3ccd8}
  .psize{margin-top:6px;font-size:12.5px;color:#c3ccd8;cursor:help;border-top:1px dashed var(--line);padding-top:6px}
  .psize b{color:var(--fg);font-weight:700}
  .coilnote{margin:10px 0 0;font-size:12.5px;color:var(--dim)}
  .perfcards{display:flex;gap:12px;flex-wrap:wrap;margin:6px 0 14px}
  .perfcard{flex:1;min-width:150px;padding:16px 18px;border:1px solid var(--line2);border-radius:14px;background:var(--panel)}
  .perfcard.pcg{border-color:rgba(63,185,80,.5);background:rgba(63,185,80,.07)}
  .perfcard.pcb{border-color:rgba(248,81,73,.45);background:rgba(248,81,73,.06)}
  .pcval{font-size:26px;font-weight:800;font-variant-numeric:tabular-nums;line-height:1}
  .pclab{font-size:11px;font-weight:700;letter-spacing:.05em;text-transform:uppercase;color:var(--dim);margin-top:6px}
  .perfsub{font-size:11px;font-weight:800;letter-spacing:.05em;text-transform:uppercase;color:var(--dim2);margin:16px 0 6px}
  .pf-good{color:var(--accent)} .pf-bad{color:#f85149}
  .pf-be{color:#f0b429} .pf-miss{color:var(--dim);font-weight:700;cursor:help;border-bottom:1px dotted var(--line)}
  .perfhelp{margin:4px 0 14px;border:1px solid var(--line);border-radius:9px;background:var(--panel2,rgba(139,152,173,.05))}
  .perfhelp>summary{cursor:pointer;padding:9px 12px;font-weight:800;font-size:12px;color:var(--accent);letter-spacing:.02em}
  .perfhelp[open]>summary{border-bottom:1px solid var(--line)}
  .perfhelpbody{padding:4px 14px 10px}
  .perfhelpbody p{margin:9px 0;font-size:12.5px;line-height:1.55;color:var(--dim)}
  .perfhelpbody b{color:var(--fg,#e6edf3)}
  tr.perfexp{cursor:pointer} tr.perfexp:hover{background:rgba(139,152,173,.06)}
  tr.perfexp .cae{display:inline-block;width:14px;color:var(--dim);font-size:10px}
  tr.perfexp.on{background:rgba(139,152,173,.05)}
  tr.perfdetail>td{padding:0 0 10px!important;background:rgba(139,152,173,.03)}
  .bt-wrap{padding:6px 10px 2px}
  .bt-h{font-size:10.5px;font-weight:800;letter-spacing:.05em;text-transform:uppercase;color:var(--dim2);margin:8px 0 4px}
  table.bt{width:100%;border-collapse:collapse;font-size:12px}
  table.bt th{text-align:left;color:var(--dim2);font-weight:700;font-size:10px;text-transform:uppercase;letter-spacing:.04em;padding:3px 8px;border-bottom:1px solid var(--line)}
  table.bt td{padding:4px 8px;border-bottom:1px solid rgba(139,152,173,.08);font-variant-numeric:tabular-nums}
  table.bt td.dim{color:var(--dim);white-space:nowrap} table.bt td.sym a{color:var(--accent);text-decoration:none}
  .bt-empty{font-size:12px;color:var(--dim);padding:4px 8px 8px}
  .bt-wait{color:#f0b429;font-weight:700;cursor:help} .bt-live{color:var(--accent);font-weight:700}
  .mc-bull{color:var(--accent)} .mc-bear{color:#f85149} .mc-mid{color:#f0b429}
  .mc-heads{display:flex;gap:14px;flex-wrap:wrap;margin:6px 0 4px}
  .mc-now{margin:8px 0 2px;padding:8px 12px;border-radius:9px;border:1px solid var(--line);background:rgba(139,152,173,.05);font-size:13px;cursor:help}
  .mc-now.mc-bull{border-color:rgba(63,185,80,.45);background:rgba(63,185,80,.07)}
  .mc-now.mc-bear{border-color:rgba(248,81,73,.45);background:rgba(248,81,73,.07)}
  .mc-now b{font-weight:800}
  .histtypes{display:flex;flex-direction:column;gap:6px;margin:8px 0}
  .histtype>summary{cursor:pointer;padding:8px 12px;border:1px solid var(--line);border-radius:8px;background:var(--bg2);font-size:12.5px;font-weight:600;list-style:none}
  .histtype>summary::-webkit-details-marker{display:none}
  .histtype[open]>summary{border-bottom-left-radius:0;border-bottom-right-radius:0}
  .histtype .rnk{color:var(--dim2);width:26px}
  .mc-head{flex:1;min-width:280px;border:1px solid var(--line);border-radius:12px;padding:14px 16px;background:rgba(139,152,173,.04)}
  .mc-head.mc-bull{border-color:rgba(63,185,80,.5);background:rgba(63,185,80,.08)}
  .mc-head.mc-bear{border-color:rgba(248,81,73,.45);background:rgba(248,81,73,.08)}
  .mc-head.mc-mid{border-color:rgba(240,180,41,.4);background:rgba(240,180,41,.07)}
  .mc-head-lab{font-size:11px;font-weight:800;letter-spacing:.06em;text-transform:uppercase;color:var(--dim2)}
  .mc-head-line{font-size:14px;font-weight:650;margin:7px 0 10px;line-height:1.45}
  .mc-head-row{display:flex;gap:18px;font-size:12.5px;color:var(--dim)}
  .mc-btcbar{display:flex;align-items:center;justify-content:space-between;gap:18px;flex-wrap:wrap;font-size:13px;margin:2px 0 8px}
  .mcgauge{position:relative;flex:1;min-width:220px;height:10px;border-radius:6px;background:rgba(139,152,173,.15);overflow:hidden}
  .mcgauge-fill{position:absolute;left:0;top:0;bottom:0;border-radius:6px;opacity:.85}
  .mcgauge-mid{position:absolute;left:50%;top:-2px;bottom:-2px;width:1px;background:var(--dim2)}
  .mcpill{display:inline-block;border-radius:5px;padding:1px 8px;font-size:11px;font-weight:700;border:1px solid var(--line)}
  .mcpill.mc-bull{background:rgba(63,185,80,.16);border-color:rgba(63,185,80,.5)}
  .mcpill.mc-bear{background:rgba(248,81,73,.14);border-color:rgba(248,81,73,.45)}
  .mcpill.mc-mid{background:rgba(240,180,41,.13)}
  .mc-cards{display:flex;gap:12px;flex-wrap:wrap;margin:2px 0 6px}
  .mc-stat{flex:1;min-width:120px;border:1px solid var(--line);border-radius:10px;padding:11px 13px;background:rgba(139,152,173,.04)}
  .mc-stat-v{font-size:20px;font-weight:800;font-variant-numeric:tabular-nums}
  .mc-stat-l{font-size:11px;color:var(--dim);margin-top:3px}
  #mcbtctbl td,#mcalttbl td{padding:5px 10px;border-bottom:1px solid rgba(139,152,173,.08);font-variant-numeric:tabular-nums}
  #mcbtctbl th,#mcalttbl th{text-align:left;color:var(--dim2);font-size:10px;text-transform:uppercase;letter-spacing:.04em;padding:4px 10px;border-bottom:1px solid var(--line);cursor:help}
  #mcalttbl td.sym a{color:var(--accent);text-decoration:none}
  .histsvg{width:100%;height:190px;display:block;margin:2px 0 10px;background:rgba(139,152,173,.04);border:1px solid var(--line);border-radius:10px}
  .histsvg .hz{stroke:var(--dim2);stroke-width:1;stroke-dasharray:3 3;opacity:.6}
  .histsvg .hg{stroke:var(--line);stroke-width:1}
  .histsvg .hax{fill:var(--dim);font-size:9px}
  .histleg{display:flex;gap:16px;margin:4px 0 2px;font-size:11px;color:var(--dim)}
  .hleg i{display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:5px;vertical-align:middle}
  .spark{width:90px;height:22px;vertical-align:middle}
  .histnote{font-size:12.5px;color:var(--dim);line-height:1.5;margin:2px 0 10px;padding:8px 11px;border-left:2px solid var(--line);background:rgba(139,152,173,.04);border-radius:0 6px 6px 0}
  .histnote b{color:var(--fg,#e6edf3)}
  .histcard{min-width:150px} .histcard-desc{font-size:11px;color:var(--dim);margin-top:6px;line-height:1.4}
  .rgb{display:inline-block;font-size:10px;font-weight:700;padding:1px 6px;border-radius:4px;margin-left:4px;cursor:help;white-space:nowrap}
  .rgb-w{color:var(--accent);background:rgba(63,185,80,.14);border:1px solid rgba(63,185,80,.4)}
  .rgb-a{color:#f85149;background:rgba(248,81,73,.12);border:1px solid rgba(248,81,73,.4)}
  .tprcell{white-space:normal}
  .tpr{display:inline-block;border-radius:5px;padding:1px 7px;font-size:11px;font-weight:700;margin:2px 4px 2px 0;border:1px solid var(--line);font-variant-numeric:tabular-nums;cursor:help}
  .tpr-hi{background:rgba(63,185,80,.18);color:var(--accent);border-color:rgba(63,185,80,.5)}
  .tpr-mid{background:rgba(240,180,41,.14);color:#f0b429}
  .tpr-lo{background:rgba(139,152,173,.1);color:var(--dim)}
  .trackrow{display:flex;align-items:center;gap:10px;margin:10px 0 2px}
  .trackbtn{padding:7px 14px;border-radius:9px;border:1px solid rgba(63,185,80,.5);background:rgba(63,185,80,.12);color:var(--accent);font-weight:700;font-size:13px;cursor:pointer}
  .trackbtn:hover{background:rgba(63,185,80,.2)}
  .trackmsg{font-size:12.5px;color:var(--dim)}
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
  /* freeze the Symbol column so you never lose track of which coin a row is.
     Cap its width and let the badge cluster (patterns / TF bias / support / confluence)
     WRAP inside the cell — otherwise the pinned column grows very wide and overlaps
     the columns that scroll underneath it. */
  td.sym{position:sticky;left:0;z-index:30;background:var(--bg2);
       box-shadow:8px 0 12px -8px rgba(0,0,0,.55);
       white-space:normal;vertical-align:top;overflow:hidden;
       width:232px;min-width:232px;max-width:232px}
  /* browsers ignore max-width on table cells in auto layout, so an inner fixed-width
     div is what actually contains the badge cluster and stops it spilling over the
     scrolling columns */
  td.sym>.symbox{width:204px;overflow:hidden;white-space:normal}
  td.sym .wstar,td.sym>a{vertical-align:middle}
  /* badges wrap onto their own lines under the ticker and never spill into the
     scrolling columns */
  td.sym>.patbadge,td.sym>.tfbias,td.sym>.supbadge,td.sym>.corrpill,
  td.sym>.bothbadge,td.sym>.freshbadge,td.sym>.newbadge,td.sym>.brokebadge{margin-top:3px}
  tbody tr:hover td.sym{background:#121a28}
  thead th:first-child{position:sticky;left:0;z-index:46;background:rgba(21,28,41,.98);
       width:232px;min-width:232px;max-width:232px}
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
  /* cross-timeframe summary block (sits above the per-timeframe Analyze card) */
  /* two-column Analyze layout: coin analysis left, recommendation summary right */
  .azcols{display:flex;gap:16px;align-items:flex-start}
  .azmain{flex:1 1 auto;min-width:0}
  .azside{flex:0 0 400px;width:400px;position:sticky;top:12px;max-height:calc(100vh - 24px);overflow-y:auto}
  .azside .azxtf{margin:0}
  .xtfexp{font-size:11px}
  .azside{flex:0 0 360px;width:360px}
  .azpe{border:1px solid var(--line2);border-radius:13px;padding:12px 16px;background:rgba(20,25,36,.35)}
  .azpe .azsec{margin-top:0}
  .azsubh{font-size:10.5px;font-weight:800;letter-spacing:.05em;text-transform:uppercase;color:var(--accent);margin:11px 0 1px}
  .azpe .aznotes{margin:2px 0 0}
  @media(max-width:1150px){ .azcols{flex-direction:column} .azside{order:2;width:100%;flex:1 1 auto;position:static;max-height:none;overflow:visible} }
  .azxtf{margin:0 0 14px;padding:12px 16px;border-radius:13px;border:1px solid var(--line2);
       background:linear-gradient(90deg,rgba(63,185,80,.10),rgba(20,25,36,.30))}
  .azxtf-short{background:linear-gradient(90deg,rgba(248,81,73,.10),rgba(20,25,36,.30))}
  .azxtf-load{color:var(--dim);font-size:13px;background:rgba(20,25,36,.4)}
  .xtfhead{display:flex;flex-wrap:wrap;align-items:center;gap:8px 12px;margin-bottom:8px}
  .xtftitle{font-weight:800;font-size:14px;color:#fff;letter-spacing:.01em}
  .xtftf{font-weight:800;font-size:12px;color:var(--accent);background:rgba(63,185,80,.14);
       border:1px solid rgba(63,185,80,.4);border-radius:7px;padding:2px 9px}
  .xtfside{font-weight:800;font-size:12px;border-radius:7px;padding:2px 9px}
  .xtfcur{color:var(--dim);font-size:12px}
  .xtftog{display:inline-flex;border:1px solid var(--line2);border-radius:8px;overflow:hidden}
  .xtftog button{background:transparent;border:0;color:var(--dim);font-size:11px;font-weight:700;
       padding:3px 11px;cursor:pointer;font-family:"Inter",sans-serif;letter-spacing:.02em}
  .xtftog button:hover{color:var(--txt)}
  .xtftog button.on{background:var(--accent);color:#04140a}
  .xtftog button.on.short{background:#f85149;color:#fff}
  .xtfrow{display:flex;flex-wrap:wrap;gap:6px 20px}
  .xtfi{font-family:var(--mono);font-size:13.5px;font-weight:600;color:var(--txt)}
  .xtfi i{font-style:normal;color:var(--dim2);font-size:9.5px;text-transform:uppercase;
       letter-spacing:.06em;margin-right:6px;font-family:"Inter",sans-serif;font-weight:600}
  .xtfi b{color:var(--accent)}
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
</header>
<div id="tip"></div>
<div class="bkbanner" id="bkbanner"></div>
<div class="banner" id="banner"></div>
<div class="tabs">
  <!-- LEAN BUILD: only the reversion core is shown. The rest are hidden (not removed) while
       we build up from the ground up around the backtest-proven reversion logic. -->
  <div class="tab" id="tabAnalyze" onclick="showTab('analyze')">🔎 Analyze a coin</div>
  <div class="tab active" id="tabBestLong" onclick="showTab('bestlong')">🟢 Long</div>
  <div class="tab" id="tabBestShort" onclick="showTab('bestshort')">🔴 Short</div>
  <div class="tab" id="tabPerf" onclick="showTab('perf')">📊 Performance</div>
  <div class="tab" id="tabBacktest" style="display:none" onclick="showTab('backtest')">🧪 Backtest</div>
  <div class="tab" id="tabMarket" onclick="showTab('market')" style="display:none">🧭 Market</div>
  <div class="tab" id="tabHistory" onclick="showTab('history')" style="display:none">🕘 History</div>
  <div class="tab" id="tabWatch" onclick="showTab('watch')" style="display:none">📌 Watchlist</div>
  <div class="tab" id="tabEarly" onclick="showTab('early')" style="display:none">⏳ Early</div>
  <div class="tab" id="tabDtb" onclick="showTab('dtb')">🔻 Triple bottom</div>
  <div class="tab" id="tabAccum" onclick="showTab('accum')">🤫 Quiet accumulation</div>
  <div class="tab" id="tabCapit" onclick="showTab('capit')">⚡ Capitulation reversal</div>
  <div class="tab" id="tabMicro" onclick="showTab('micro')">🐜 Microcaps &lt;$10m</div>
  <div class="tab" id="tabSignals" onclick="showTab('signals')">🔬 Signal lab</div>
  <div class="tab" id="tabCoil" onclick="showTab('coil')" style="display:none">🚀 Coiled</div>
  <div class="tab" id="tabScalp" onclick="showTab('scalp')" style="display:none">⚡ Best scalps</div>
  <div class="tab" id="tabSpot" onclick="showTab('spot')" style="display:none">💰 Spot buys</div>
  <div class="tab" id="tabSetups" onclick="showTab('setups')" style="display:none">200-EMA reclaim</div>
  <div class="tab" id="tabFlags" onclick="showTab('flags')" style="display:none">Bull flags</div>
  <div class="tab" id="tabCpr" onclick="showTab('cpr')" style="display:none">Narrow CPR</div>
  <div class="tab" id="tabBounce" onclick="showTab('bounce')" style="display:none">Support bounce</div>
  <div class="tab" id="tabStb" onclick="showTab('stb')" style="display:none">Supertrend support bounce</div>
  <div class="tab" id="tabShorts" onclick="showTab('shorts')" style="display:none">Shorts</div>
  <div class="tab" id="tabCalls" onclick="showTab('calls')" style="display:none">📌 My calls</div>
  <div class="tab" id="tabInfo" onclick="showTab('info')" style="display:none">Info</div>
</div>
<div class="filterbar" id="filterbar"></div>

<div class="view" id="viewSetups">
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

<div class="view active" id="viewDtb">
<div class="status">
  <span>🔻 <b>Descending triple bottom</b> — three swing lows, each <b>lower than the last</b>, with price now basing at/just above the third. The classic capitulation base: sellers exhausting into a final flush. Detected on the <b>daily</b> chart (built from the 4h candles). Entry near the base, stop under the 3rd low, target the first-leg high — a full measured retrace. Ranked by reward:risk. <b>⚠ Lottery-ticket setups</b> — plenty keep making lower lows, so size small and respect the stop.</span>
</div>
<div class="wrap">
  <table id="dtbtbl">
    <thead><tr>
      <th>Symbol</th><th>Price</th>
      <th data-tip="First (highest) bottom.">Low 1</th>
      <th data-tip="Second bottom — lower than the first.">Low 2</th>
      <th data-tip="Third and lowest bottom — price is basing here now.">Low 3</th>
      <th>Entry</th><th data-tip="Below the 3rd low — the pattern is invalid under here.">Stop</th>
      <th data-tip="Measured-move objective: the first-leg high.">Target</th>
      <th>R:R</th>
      <th data-tip="Price has held above the 3rd low since making it — a higher low forming.">Higher low?</th>
      <th data-tip="Recent volume below its own average — nobody is watching it yet.">Quiet?</th>
    </tr></thead>
    <tbody id="dtbrows"></tbody>
  </table>
  <div id="dtbdiag" style="margin-top:8px"></div>
  <div class="empty" id="dtbempty" style="display:none">No descending triple bottoms right now. The loop keeps scanning…</div>
</div>
</div>

<div class="view active" id="viewCapit">
<div class="status">
  <span>⚡ <b>Capitulation reversal</b> &mdash; the BKR/HEMI setup. A grinding downtrend of <b>lower highs and lower lows</b>, then a final flush that <b>squeezes the longs out into a fresh low</b>, then price <b>breaks the descending trendline</b> of those lower highs. Entry on the break, stop under the flush low, target the <b>overhead supply zone</b> the decline started from.<br>
  <b>Confidence</b> counts confirmations on top of the required structure: <b>RSI divergence</b> (RSI refused to make a new low with price), a <b>volume surge</b>, and <b>reclaimed</b> structure. The trendline break alone is confidence 1; all three extras is 4. <span class="warn">Catching a falling knife is the highest-variance trade there is &mdash; a downtrend breaking is exactly where you find out it was not done falling. Size accordingly.</span></span>
</div>
<div class="wrap">
  <table id="capittbl">
    <thead><tr><th>Symbol</th><th>Price</th><th>Mkt cap</th><th>Flush low</th><th>Bars since</th><th>Trendline</th><th>Supply zone</th><th>Entry</th><th>Stop</th><th>Target</th><th>R:R</th><th>RSI div?</th><th>Vol surge?</th><th>Confidence</th></tr></thead>
    <tbody id="capitrows"></tbody>
  </table>
  <div class="empty" id="capitempty" style="display:none">No capitulation reversals right now. The daily loop keeps scanning&hellip;</div>
</div>
</div>
<div class="view active" id="viewMicro">
<div class="status">
  <span>🐜 <b>Microcaps under $10m</b> &mdash; market caps come from CoinGecko, matched by ticker.<br>
  <span class="warn">Read the caveat before trusting this: tickers collide constantly in crypto (several tokens trade as BANK), so where a symbol is ambiguous the tool keeps the <b>largest</b> matching market cap. That deliberately errs away from calling something a microcap &mdash; the list below is conservative and will miss some real microcaps rather than mislabel a large coin as tiny. Coins CoinGecko does not list at all show as unknown and are excluded entirely.</span></span>
</div>
<div class="wrap" id="microsum"></div>
<div class="wrap" style="margin-top:10px">
  <table id="microtbl">
    <thead><tr><th>Symbol</th><th>Market cap</th><th>Triple bottom?</th><th>Quiet accumulation?</th><th>Capitulation?</th></tr></thead>
    <tbody id="microrows"></tbody>
  </table>
  <div class="empty" id="microempty" style="display:none">No sub-$10m coins mapped yet &mdash; the daily loop refreshes market caps every few hours.</div>
</div>
</div>
<div class="view active" id="viewSignals">
<div class="status">
  <span>🔬 <b>Signal lab</b> — instead of assuming which indicators matter, this tests <b>~20 of them independently</b>. Every trade in a deliberately neutral base strategy (buy any green close, fixed 1.5&times;ATR stop / 3&times;ATR target) gets tagged with all 20 reads. Then each signal is scored by how much it <b>shifts expectancy versus the base</b>.<br>
  <b>Lift</b> is the number that matters: expectancy with the signal on, minus the base expectancy. Positive = the signal adds something. <b>H1 / H2</b> split the history in half &mdash; a signal that only worked in one half is probably noise, not edge. <b>Robust</b> means it beat the base in <i>both</i> halves.<br>
  <span class="warn">Read this honestly: with 20 signals and ~190 pairs, some will look good by chance alone. Treat only the <b>robust</b> ones with a decent sample as candidates, and expect the lift to shrink live.</span></span>
</div>
<div class="wrap" id="sigbase" style="margin-bottom:10px"></div>
<div class="wrap" id="wfbox" style="margin-bottom:14px"></div>
<div class="wrap">
  <h3 style="margin:6px 0 4px">Individual signals <span class="rr" id="sigcount"></span></h3>
  <table id="sigtbl">
    <thead><tr><th>Signal</th><th>Trades</th><th>Win rate</th><th>Expectancy (R)</th><th>Exp. w/ DCA</th><th>DCA helps?</th><th>Lift vs base</th><th>Total R</th><th>Max DD (R)</th><th>End equity</th><th>Return</th><th>Max DD $</th><th>Worst trade</th><th>1st half</th><th>2nd half</th><th>Decay</th><th>Profitable both halves?</th></tr></thead>
    <tbody id="sigrows"></tbody>
  </table>
  <div id="sigempty" class="empty">No signal results yet &mdash; the lab runs on a ~6-hour cycle.</div>
</div>
<div class="wrap" style="margin-top:14px">
  <h3 style="margin:6px 0 4px">Best pairs (confluence)</h3>
  <table id="sigptbl">
    <thead><tr><th>Combination</th><th>Trades</th><th>Win rate</th><th>Expectancy (R)</th><th>Exp. w/ DCA</th><th>DCA helps?</th><th>Lift vs base</th><th>Total R</th><th>Max DD (R)</th><th>End equity</th><th>Return</th><th>Max DD $</th><th>Worst trade</th><th>1st half</th><th>2nd half</th><th>Decay</th><th>Profitable both halves?</th></tr></thead>
    <tbody id="sigprows"></tbody>
  </table>
</div>
<div class="wrap" style="margin-top:14px">
  <h3 style="margin:6px 0 4px">Best triples (3-signal confluence)</h3>
  <table id="sigttbl">
    <thead><tr><th>Combination</th><th>Trades</th><th>Win rate</th><th>Expectancy (R)</th><th>Exp. w/ DCA</th><th>DCA helps?</th><th>Lift vs base</th><th>Total R</th><th>Max DD (R)</th><th>End equity</th><th>Return</th><th>Max DD $</th><th>Worst trade</th><th>1st half</th><th>2nd half</th><th>Decay</th><th>Profitable both halves?</th></tr></thead>
    <tbody id="sigtrows"></tbody>
  </table>
</div>
</div>
<div class="view active" id="viewAccum">
<div class="status">
  <span>🤫 <b>Quiet accumulation → breakout</b> — the $LAB pattern: an earlier <b>test pump</b> proves the coin can run, then it drops into a <b>tight range</b> where <b>volume dries up</b> (nobody watching), then presses/breaks out of the top. Detected on the <b>daily</b> chart. Entry on the range-high break, stop under the range, target a measured move capped at the prior pump high. <b>Vol ratio</b> = recent volume ÷ its earlier average — lower is quieter and better.</span>
</div>
<div class="wrap">
  <table id="accumtbl">
    <thead><tr>
      <th>Symbol</th><th>Price</th>
      <th>Range low</th><th>Range high</th>
      <th data-tip="How tight the accumulation range is. Tighter = more coiled.">Range %</th>
      <th data-tip="Recent volume vs its earlier average. Under 0.5x = properly quiet.">Vol ratio</th>
      <th data-tip="The earlier test-pump high — proof this coin can move.">Prior pump</th>
      <th>Entry</th><th>Stop</th><th>Target</th><th>R:R</th>
      <th>State</th>
    </tr></thead>
    <tbody id="accumrows"></tbody>
  </table>
  <div id="accumdiag" style="margin-top:8px"></div>
  <div class="empty" id="accumempty" style="display:none">No quiet-accumulation setups right now. The loop keeps scanning…</div>
</div>
</div>

<div class="view active" id="viewBestLong">
<div class="status">
  <span>🏆 Longs — <b>only coins where a lab strategy is currently firing.</b> These are the indicator combos that stayed profitable when tested on data they were never fitted to, so the board answers "what does the evidence say to buy right now" rather than "what looks nice". Triple-bottom, accumulation and the other pattern boards have their own tabs and no longer feed this one. Setups are then ranked across <b>every</b> scanned pair Graded on trend structure, multi-timeframe agreement, momentum, volume, pattern confluence and proximity to support — <b>then weighted by the trade's reward:risk</b>, so a strong trend with no room to run doesn't top the list. Only tradeable R:R (≥1) shown. Click any row (⚲) for the full cross-timeframe plan.</span>
  <span id="blCount"></span>
</div>
<div class="wrap">
  <table id="bltbl">
    <thead><tr>
      <th>#</th>
      <th>Symbol</th>
      <th data-tip="Setup score (0–100) = trend conviction (structure, timeframe agreement, momentum, volume, pattern confluence, proximity to support) MULTIPLIED by an R:R factor. A high-conviction coin with a poor reward:risk is dragged down, so the top rows are strong AND tradeable.">Score</th>
      <th data-tip="Live last-traded price (updates ~every 20s).">Price</th>
      <th data-tip="Market-structure bias from swing highs/lows.">Bias</th>
      <th data-tip="Per-timeframe market-structure bias (1h/4h/1D/1W): ▲ bullish, ▼ bearish, – neutral.">Timeframes</th>
      <th data-tip="Recommended entry — a pullback to the nearest support (a value fill), or current price if support is far. Quick preview; click ⚲ for the full cross-timeframe entry.">Entry</th>
      <th data-tip="Recommended stop — just beyond the next support below the entry (or an ATR buffer if none).">Stop</th>
      <th data-tip="Recommended target — the nearest resistance above (or a 2R projection if none overhead).">Target</th>
      <th data-tip="Reward:risk of the previewed trade (target vs stop from the entry).">R:R</th>
      <th data-tip="Which of the lab's surviving strategies are firing on this coin RIGHT NOW. These are the combos that stayed profitable OUT-OF-SAMPLE (selected on the first half of history, measured on the second half they never saw). #1 is the highest-ranked. Blank means no surviving combo currently fires — the coin may still be a good setup on the other columns, it just isn't one the lab has evidence for.">🔬 Lab</th>
      <th data-tip="Plain-English reasons this coin ranks where it does.">Why</th>
    </tr></thead>
    <tbody id="blrows"></tbody>
  </table>
  <div class="empty" id="blempty" style="display:none">🚫 No long setups clear the quality bar right now — Apex won't force a trade into a tape that isn't offering clean longs. Check the 🧭 Market tab for the regime, or come back next scan.</div>
</div>
</div>

<div class="view" id="viewBestShort">
<div class="status">
  <span>🩸 Best shorts — the <b>25 strongest SHORT</b> setups ranked across <b>every</b> scanned pair. Graded on downtrend structure, multi-timeframe agreement, downside momentum, volume and proximity to resistance — <b>then weighted by the trade's reward:risk</b>, so poor-R:R shorts don't top the list. Only tradeable R:R (≥1) shown. Click any row (⚲) for the full cross-timeframe plan (opens forced to the short side).</span>
  <span id="bsCount"></span>
</div>
<div class="wrap">
  <table id="bstbl">
    <thead><tr>
      <th>#</th>
      <th>Symbol</th>
      <th data-tip="Setup score (0–100) = trend conviction (downtrend structure, timeframe agreement, downside momentum, volume, breakdown confluence, proximity to resistance) MULTIPLIED by an R:R factor. A high-conviction coin with a poor reward:risk is dragged down, so the top rows are strong AND tradeable.">Score</th>
      <th data-tip="Live last-traded price (updates ~every 20s).">Price</th>
      <th data-tip="Market-structure bias from swing highs/lows.">Bias</th>
      <th data-tip="Per-timeframe market-structure bias (1h/4h/1D/1W): ▲ bullish, ▼ bearish, – neutral.">Timeframes</th>
      <th data-tip="Recommended entry — a pullback to the nearest resistance (sell into strength), or current price if resistance is far. Quick preview; click ⚲ for the full cross-timeframe entry.">Entry</th>
      <th data-tip="Recommended stop — just beyond the next resistance above the entry (or an ATR buffer if none).">Stop</th>
      <th data-tip="Recommended target — the nearest support below (or a 2R projection if none beneath).">Target</th>
      <th data-tip="Reward:risk of the previewed trade (target vs stop from the entry).">R:R</th>
      <th data-tip="Plain-English reasons this coin ranks where it does.">Why</th>
    </tr></thead>
    <tbody id="bsrows"></tbody>
  </table>
  <div class="empty" id="bsempty" style="display:none">🚫 No short setups clear the quality bar right now — Apex won't force a short when the tape isn't offering clean ones. Check the 🧭 Market tab, or come back next scan.</div>
</div>
</div>

<div class="view" id="viewCoil">
<div class="status">
  <span>🚀 Coiled — coins most likely to make a <b>big move soon</b>. Ranks every pair by how <b>compressed</b> it is: a Bollinger-band squeeze, contracting ATR, and a tight coil — with a bonus when it's coiled on <b>multiple timeframes</b> (15m→1W, shown per row). Quiet markets expand. Each side has a full breakout plan — two limit entries, a tight stop, and a <b>3-target ladder</b> (measured move → 1.618× Fib → 2×/next HTF level) with a scale-out — on hover. The recommended side (the lean) is highlighted.</span>
  <span id="coilCount"></span>
</div>
<div class="wrap">
  <table id="coiltbl">
    <thead><tr>
      <th>#</th>
      <th>Symbol</th>
      <th data-tip="Coil score (0–100) — how ready this coin is to expand. Blends Bollinger-band-width squeeze (60%), ATR contraction vs its prior baseline (25%), and how tight the recent price coil is (15%). Higher = wound tighter.">Coil</th>
      <th data-tip="Squeeze depth — the current Bollinger-band width is tighter than this % of its recent range. 90%+ = an unusually tight squeeze.">Squeeze</th>
      <th data-tip="Live last-traded price (updates ~every 20s).">Price</th>
      <th data-tip="Directional lean from multi-timeframe structure — the more likely break direction. 'Neutral' = trade whichever way it breaks.">Lean</th>
      <th data-tip="WHICH timeframes are coiled right now (15m → 1W). Each shows its squeeze %; brighter = tighter. A coil confirmed on MULTIPLE timeframes is a bigger, more explosive setup (and gets a score bonus). 15m/1h are computed live for these top coils.">Coiled on</th>
      <th data-tip="ATR as % of price — current volatility. It's low here by design (that's the squeeze); expect it to expand.">ATR%</th>
      <th data-tip="LONG breakout play — two limit entries (a retest limit at the range top + a break-confirm just above), a tight stop back inside the range, and a 3-target ladder (1× measured move, 1.618× Fib, 2×/next HTF resistance). Cell shows the entry and the R:R to the FINAL target; hover for the full plan with all TPs and the scale-out. Highlighted green when it's the recommended side.">▲ Long break</th>
      <th data-tip="SHORT breakdown setup — enter on a break below the range, stop back inside the range, target a 1× measured move down. Shows entry & R:R; hover for the full plan. Highlighted red when it's the recommended side (coin leans bearish).">▼ Short break</th>
      <th data-tip="Plain-English reasons this coin is coiled.">Why</th>
    </tr></thead>
    <tbody id="coilrows"></tbody>
  </table>
  <div class="empty" id="coilempty" style="display:none">🚫 No coiled setups with a clear lean and tradeable R:R right now — Apex won't force a squeeze that has no direction. Come back next scan.</div>
</div>
</div>

<div class="view" id="viewScalp">
<div class="status">
  <span>⚡ Best scalps — quick <b>15-minute</b> trades with a <b>tight-but-breathable</b> stop and high R:R (5m was too noisy for crypto, so the base timeframe is now 15m). Two kinds: <b>trend scalps</b> (15m entry taken <b>with</b> the 4h/Daily/Weekly direction) and <b>↩ counter-trend bounces</b> — a snap-back off a <b>strong, multi-tested support/resistance</b> even when there's no clean trend setup (oversold wash into support = long bounce; overbought pop into resistance = short fade). So there's almost always a good scalp on the board. Counter-trend rows are badged; the edge is the level + the snap, not the trend. Click a row for the full plan.</span>
  <span id="scalpCount"></span>
</div>
<div class="wrap">
  <table id="scalptbl">
    <thead><tr>
      <th>#</th>
      <th>Symbol</th>
      <th data-tip="Scalp score (0–100) = higher-timeframe conviction × R:R × how tight the stop is × 15m alignment. High = strong HTF trend + a clean, tight 15m entry.">Score</th>
      <th data-tip="Direction — taken WITH the higher-timeframe trend. Long = HTF bullish, Short = HTF bearish.">Side</th>
      <th data-tip="Higher- and lower-timeframe bias (5m/15m/4h/1D/1W): ▲ bullish, ▼ bearish, – neutral. The scalp direction follows the HIGHER frames; the 15m gives the entry.">Timeframes</th>
      <th data-tip="Live last-traded price (updates ~every 20s).">Price</th>
      <th data-tip="Scalp entry — a 15m swing level in the HTF direction (or current price on 15m momentum).">Entry</th>
      <th data-tip="Tight stop — just beyond the next 15m swing, shown with its % risk. Scalps keep this small.">Stop</th>
      <th data-tip="First 15m target. The row expands to the full ladder.">Target</th>
      <th data-tip="Reward:risk to the first target; the arrow shows R:R to the furthest.">R:R</th>
      <th data-tip="Plain-English reasons — HTF alignment, 15m trigger, stop tightness.">Why</th>
    </tr></thead>
    <tbody id="scalprows"></tbody>
  </table>
  <div class="empty" id="scalpempty" style="display:none">🚫 Nothing clears the bar this scan — no clean trend scalp AND no coin sitting on a strong enough support/resistance with a real snap-back. Rare, but Apex won't invent one. Come back next scan.</div>
</div>
</div>

<div class="view" id="viewSpot">
<div class="status">
  <span>💰 Spot buys — the best <b>LONG</b> setups reframed for a <b>cash / spot</b> buyer. Spot means <b>1× (no leverage → no liquidation)</b> and <b>no funding</b> to bleed while you hold, so the ideal spot pick is an uptrend long you can <b>limit-buy on a dip into support</b> and calmly hold to the mean/target. These are drawn from the whole-universe Best-longs grade, then filtered for spot: a <b>real pullback level to buy at</b> (no chasing), tradeable R:R, and <b>not stretched far above the 200-EMA</b> (don't buy the top). The stop is a <b>mental invalidation</b> — a place to trim, not a forced liquidation. Click a row for the full plan. <i>(The 🧪 Backtest tab now also runs a spot-only sweep — long reversion, spot fees — so you can see how the approach actually performs.)</i></span>
  <span id="spotCount"></span>
</div>
<div class="wrap">
  <table id="spottbl">
    <thead><tr>
      <th>#</th>
      <th>Symbol</th>
      <th data-tip="Spot score (0–100) = the coin's Best-long grade, plus a bonus for sitting CLOSE to support (buy the dip) and liquidity, minus a penalty for being stretched above the 200-EMA. High = a strong uptrend long you can buy on a calm pullback.">Score</th>
      <th data-tip="How far above the 200-EMA price is. Spot buys avoid heavily-extended coins — buying near the mean, not the top.">vs 200-EMA</th>
      <th data-tip="Live last-traded price (updates ~every 20s).">Price</th>
      <th data-tip="Recommended spot BUY — a limit into the nearest support / pullback level. Place the order and wait; don't chase.">🎯 Buy (limit)</th>
      <th data-tip="Invalidation — a MENTAL stop / place to trim if this level breaks. On spot there's no liquidation; you decide. Shown with its % below the buy.">Invalidation</th>
      <th data-tip="First target — the nearest level price is pulled toward. The row expands to the full ladder.">Target</th>
      <th data-tip="Reward:risk to the first target; the arrow shows R:R to the furthest.">R:R</th>
      <th data-tip="Rough hold horizon based on the setup's timeframe. Spot has no funding, so holding costs nothing.">Hold</th>
      <th data-tip="Plain-English reasons — trend structure, multi-timeframe agreement, proximity to support.">Why</th>
    </tr></thead>
    <tbody id="spotrows"></tbody>
  </table>
  <div class="empty" id="spotempty" style="display:none">🚫 No clean spot buys this scan — every top long is either chasing (no pullback level to limit-buy) or stretched too far above the mean. Apex won't tell you to buy a top. Check back next scan.</div>
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
      <th data-wk="sym" onclick="sortWatch('sym')" style="cursor:pointer">Symbol</th>
      <th data-wk="price" onclick="sortWatch('price')" style="cursor:pointer" data-tip="Live last-traded price. Click to sort.">Price</th>
      <th data-wk="bias" onclick="sortWatch('bias')" style="cursor:pointer" data-tip="Market-structure bias (auto direction) on the 4h chart. Click to sort bullish→bearish.">Bias</th>
      <th data-wk="setups" onclick="sortWatch('setups')" style="cursor:pointer" data-tip="Active scanner setups + the strongest chart pattern found. Click to sort by how many setups are firing.">Setups</th>
      <th data-wk="entry" onclick="sortWatch('entry')" style="cursor:pointer" data-tip="Recommended LONG entry — a proper pullback fill, not chasing. Click to sort.">🎯 Entry</th>
      <th data-wk="stop" onclick="sortWatch('stop')" style="cursor:pointer" data-tip="Recommended stop-loss (the level that invalidates the long), with ×ATR distance. Click to sort.">🛑 Stop</th>
      <th data-wk="rating" onclick="sortWatch('rating')" style="cursor:pointer" data-tip="Best realistic take-profit by expected value, with R:R and a plan grade. The list is sorted by this rating (best first) by default — click to reverse, or click any other header to sort by it.">⭐ Best TP</th>
      <th data-wk="corr" onclick="sortWatch('corr')" style="cursor:pointer" data-tip="BTC correlation ρ over ~10 days. Low/negative = its own mover. Click to sort.">BTC ρ</th>
      <th>Actions</th>
    </tr></thead>
    <tbody id="wlrows"></tbody>
  </table>
  <div class="empty" id="wlempty" style="display:none">Your watchlist is empty. Click the ☆ next to any coin's symbol (on any tab or in Analyze) to add it here.</div>
</div>
</div>

<div class="view" id="viewMarket">
<div class="status">
  <span>🧭 Market context — is it a good <b>day</b> or <b>week</b> to be hunting longs or shorts? A multi-timeframe read on <b>BTC</b> (15m → 1W) plus <b>alt-market breadth</b> (how much of the majors are trading above their key moving averages). The whole market tends to follow BTC, so this frames every setup on the other tabs.</span>
  <span id="marketMeta"></span>
</div>
<div class="wrap">
  <div id="marketBody"></div>
  <div class="empty" id="marketempty" style="display:none">Market read is being computed on the next scan — check back in a moment.</div>
</div>
</div>

<div class="view" id="viewHistory">
<div class="status">
  <span>🕘 History — Apex's own <b>memory</b>. Every scan it records the market regime (BTC + alt breadth), how busy each board was, which coins kept topping the leaderboards, and the <b>open-interest / funding / price</b> path of the majors (from Coinalyze). This is the historical context it reasons from — 'breadth improving vs a week ago', 'OI building into this level', 'BTC flipped bullish two days ago'.</span>
  <span id="histMeta"></span>
</div>
<div class="wrap">
  <div id="histBody"></div>
  <div class="empty" id="histempty" style="display:none">No history yet — Apex starts recording on its first completed scan. Check back after a scan or two. (Add the Upstash keys in Render → Environment to keep history across restarts.)</div>
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
    <span class="tfbtn" data-tf="5m" onclick="setAzTf('5m')" title="5-minute — the lowest timeframe; scalps + precise entries and low-TF context for the higher frames">5m</span>
    <span class="tfbtn" data-tf="15m" onclick="setAzTf('15m')" title="15-minute — for scalps / fast intraday setups">15m</span>
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

<div class="view" id="viewPerf">
<div class="status">
  <span>📊 Performance — Apex's own <b>forward test</b>, run honestly like a real trader would.</span>
  <span id="perfMeta"></span>
</div>
<div class="wrap">
  <details class="perfhelp"><summary>What exactly is being tracked here?</summary>
    <div class="perfhelpbody">
      <p>Every setup the boards produce is recorded as a <b>two-phase trade</b>, so the numbers reflect what you'd actually get:</p>
      <p><b>1 · Wait for the entry to fill.</b> A setup isn't a live trade the moment it's flagged — it's a <i>plan</i> with a specific entry price (a pullback for longs, a bounce for shorts, a break for coils). Apex holds it as <b>waiting for entry</b> and only starts the trade when price actually trades to that entry. If price never fills but runs to the first target without you, it's logged as <b>MISSED (no fill)</b> — no fake loss. If it never fills within a few days, it <b>EXPIRES</b> — cancelled, no trade.</p>
      <p><b>2 · Manage it exactly like the plan.</b> Once filled, Apex banks each target by its scale-out weight and <b>moves the stop to break-even after TP1</b>. So once TP1 hits the trade can no longer be a full loss — worst case it's a <b>break-even</b>. A trade that stops out before any TP is the only <b>−1R loss</b>.</p>
      <p><b>Win rate</b> = share of filled trades that reached at least TP1. <b>Expectancy</b> = average R across the scaled-out position. <b>Per-TP hit rates</b> = how far price typically runs. The <b>all-out table</b> below answers "what if I'd just taken 100% off at TP1 / TP2 / TP3?" — the simple one-target R for each exit choice.</p>
      <p><b>The coiled trades</b> track the <b>recommended (lean) side only</b> — not both long and short. Apex picks the direction the squeeze is leaning and forward-tests that single plan, so a coil can't "win" on one side and "lose" on the other.</p>
    </div>
  </details>
  <div id="perfCards" class="perfcards"></div>
  <div id="gateLearnBox" class="learnbox" style="display:none"></div>
  <div class="perfsub" id="liveWinSub" style="display:none">🟢 Live winners — open trades already past TP1 (stop at break-even, running)</div>
  <table id="livewintbl" style="display:none"><thead><tr>
      <th>Added</th><th>Board</th><th>Symbol</th><th>Side</th><th>TF</th><th>Entry</th><th data-tip="Current mark-to-market R right now (banked TPs + open remainder at live price).">Now (R)</th><th>Progress</th>
    </tr></thead><tbody id="livewinrows"></tbody></table>
  <div class="perfsub">By board — win rate, expectancy & per-TP hit rates</div>
  <table id="perftbl"><thead><tr>
      <th>Board</th><th data-tip="Filled setups that have resolved.">Trades</th>
      <th data-tip="% that reached at least TP1 (after which the stop moves to break-even, so it can't be a full loss).">Win rate</th>
      <th data-tip="Average R per trade across the scaled-out position. Above 0 = a positive edge.">Expectancy</th>
      <th data-tip="Cumulative R booked across every resolved trade on this board — the running total.">Total R</th>
      <th data-tip="What % of these trades reached each target — TP1, TP2, TP3 … Shows how far price typically runs.">TP hit rates</th>
    </tr></thead><tbody id="perfrows"></tbody></table>
  <div class="perfsub">If you took 100% off at one target — R per exit choice</div>
  <table id="perfaotbl"><thead><tr>
      <th>Board</th><th data-tip="Average R if every trade exited its whole position at TP1.">All-out at TP1</th>
      <th data-tip="Average R if every trade exited its whole position at TP2 (missing TP2 = −1R).">All-out at TP2</th>
      <th data-tip="Average R if every trade exited its whole position at TP3 (missing TP3 = −1R).">All-out at TP3</th>
    </tr></thead><tbody id="perfaorows"></tbody></table>
  <div class="perfsub">Learning — with the market regime vs against it</div>
  <div class="histnote" id="perfRegimeNote">Apex now tilts the boards toward the side the market favours and tags each setup <b>with-regime</b> or <b>against-regime</b>. This is the payoff: does trading with the tape actually win more? (Fills in as regime-tagged trades resolve.)</div>
  <table id="perfregtbl"><thead><tr>
      <th>Alignment</th><th>Trades</th><th>Win rate</th><th>Expectancy</th>
    </tr></thead><tbody id="perfregrows"></tbody></table>
  <div class="perfsub">By site version — how each iteration of the logic performed</div>
  <div class="histnote">The headline above resets to the <b>current version</b> so each change to the recommendation logic gets a clean scorecard. Every past version's results are kept here so you can see whether each iteration actually improved things.</div>
  <table id="perfvertbl"><thead><tr>
      <th>Version</th><th>Trades</th><th>Win rate</th><th>Expectancy</th><th>Total R</th>
    </tr></thead><tbody id="perfverrows"></tbody></table>
  <div class="perfsub">Recent resolved trades</div>
  <table id="perftrtbl"><thead><tr>
      <th>Closed</th><th>Board</th><th>Symbol</th><th>Side</th>
      <th>Entry</th><th>Stop</th><th data-tip="The furthest level price actually reached before the trade closed — the highest TP hit, or the stop.">Reached</th><th>Result</th>
    </tr></thead><tbody id="perftrrows"></tbody></table>
  <div class="empty" id="perfempty" style="display:none">No resolved setups yet — the tracker records setups as they appear and closes them when price hits a target or stop. Check back after a few scans.</div>
</div>
</div>

<div class="view" id="viewCalls">
<div class="status">
  <span>📌 My calls — setups you chose to track (via <b>📌 Track this setup</b> on the Analyze tab). Apex watches the live price and grades each one exactly like the plan: banks each TP, moves the stop to <b>break-even after TP1</b>, and scores the result in R. Their win-rate and per-TP hit rates are kept <b>separate</b> from the auto boards.</span>
  <span id="callsMeta"></span>
</div>
<div class="wrap">
  <div id="callsCards" class="perfcards"></div>
  <div class="perfsub">Open calls</div>
  <table id="callsopen"><thead><tr>
      <th>Added</th><th>Symbol</th><th>Side</th><th>TF</th><th>Entry</th><th>Stop</th><th>Targets</th><th>Progress</th>
    </tr></thead><tbody id="callsopenrows"></tbody></table>
  <div class="perfsub">Resolved calls</div>
  <table id="callsclosed"><thead><tr>
      <th>Closed</th><th>Symbol</th><th>Side</th><th>TF</th><th>Entry</th><th>Stop</th><th data-tip="The furthest level price reached before close — highest TP hit, or the stop.">Reached</th><th>Result</th>
    </tr></thead><tbody id="callsclosedrows"></tbody></table>
  <div class="empty" id="callsempty" style="display:none">No tracked calls yet. Open <b>🔎 Analyze a coin</b>, and on any setup that clears the R:R bar you'll see a <b>📌 Track this setup</b> button — click it to start tracking.</div>
</div>
</div>

<div class="view" id="viewBacktest">
<div class="status">
  <span>🧪 Backtest — now testing the <b>trend-aligned mean-reversion</b> strategy: only WITH the higher-timeframe trend (sloping 200-EMA), enter an <b>oversold flush that's snapping back</b> (long) / overbought pop (short), target the nearby <b>20-EMA mean</b> for a quick high-probability bounce, stop beyond the flush extreme. Buy panic in an uptrend, sell euphoria in a downtrend. Apex <b>runs it itself</b> across <b>every timeframe</b> and both sides over the <b>whole universe</b> — no look-ahead, stop-first on ties, <b>net of fees</b>. <b>Now over ~2 years</b> (paginated history on the higher TFs), with a <b>looser trigger</b> (RSI 45/55 — so BTC, SOL and the majors actually qualify, not just high-beta alts) and a <b>smarter BTC gate</b>: on 15m/1h it only trades WITH the BTC trend (cuts the noise), on 4h/1d it trades through a calm BTC drift and only steps aside for a volatile opposing tape. Every trade is sliced by <b>time-of-day, BTC regime & BTC volatility</b>, and there's a <b>trade log</b> (date · coin · result · market environment · why) in each breakdown. The matrix is the honest read on whether this actually wins before it ever goes to the live boards. (Refreshes every ~6h.)</span>
</div>
<div class="wrap">
  <div class="btbar">
    <span id="btMeta" class="btmeta">Loading the latest backtest…</span>
    <button id="btRun" class="btrun" onclick="loadBacktest(true)" style="margin-left:auto">↻ Refresh</button>
  </div>
  <div id="btBody"></div>
  <div class="empty" id="btempty" style="display:none"></div>
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
let activeTab="bestlong", lastData=null;
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
    return n.toFixed(decimals).replace(/0+$/,'').replace(/\\.$/,'');
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
// Open Analyze for a symbol and force the cross-TF summary to a given side
// (used by the Best-shorts / Best-longs leaderboards so the plan opens on the
// side you clicked, not the coin's auto lean).
function goAnalyzeSide(sym, side, ev){ if(ev){ ev.stopPropagation(); ev.preventDefault(); }
  azXtfSide=(side==='long'||side==='short')?side:null; goAnalyze(sym); }
function analyzeSideBtn(sym, side){ return `<span class="azbtn" title="Analyze ${dispSym(sym)} — full ${side} plan" onclick="goAnalyzeSide('${sym}','${side}',event)">⚲</span>`; }
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
  const grade=rec?planGradeOf(rec.rr,rec.p):'—';
  return {recE, rstop, rec, grade};
}
function setupsCell(d, on){
  const tags=[];
  if(d){ if(d.ema_reclaim) tags.push('200-EMA reclaim'); if(d.bull_flag) tags.push('Bull flag');
    if(d.support_bounce) tags.push('Support bounce'); }
  for(const nm of on){ if(!tags.includes(nm)) tags.push(nm); }
  const tfs=[['5m','5m'],['15m','15m'],['1h','1h'],['4h','4h'],['1d','1D'],['1w','1W']];
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
// Watchlist sort — defaults to the setup RATING (best plan first); every column
// header is clickable to sort by it (click again to reverse).
let watchSort={key:'rating', dir:-1};
const WGRANK={'A+':5,'A':4,'B':3,'C':2,'—':0};
function watchVal(sym,key){
  const cache=watchData[sym]; const d=cache?cache.d:null;
  const {row,on}=watchLookup(sym);
  if(key==='sym') return dispSym(sym);
  const price=(d&&d.live!=null)?d.live:(d?d.price:(row?(row.live!=null?row.live:row.price):null));
  if(key==='price') return price!=null?price:-Infinity;
  if(key==='bias'){ const b=(d?(d.bias||''):(row?(row.bias||''):'')).toLowerCase();
    return ({bullish:2,neutral:1,bearish:0})[b]!=null?({bullish:2,neutral:1,bearish:0})[b]:-1; }
  if(key==='corr'){ const c=d?d.btc_corr:(row?row.btc_corr:null); return c!=null?c:-Infinity; }
  if(key==='setups'){ let n=(on||[]).length; if(d){ if(d.ema_reclaim)n++; if(d.bull_flag)n++; if(d.support_bounce)n++; } return n; }
  if(!d) return -Infinity;
  const R=watchRec(d);
  if(key==='entry') return R.recE!=null?R.recE:-Infinity;
  if(key==='stop') return R.rstop?R.rstop.level:-Infinity;
  if(key==='rating'||key==='tp'){ if(!R.rec) return -Infinity; return (WGRANK[R.grade]||0)*1000 + R.rec.rr; }
  return -Infinity;
}
function sortWatch(key){
  if(watchSort.key===key) watchSort.dir*=-1;
  else { watchSort.key=key; watchSort.dir=(key==='sym')?1:-1; }
  renderWatch();
}
function setWatchArrows(){
  document.querySelectorAll('#wltbl thead th[data-wk]').forEach(th=>{
    const base=th.getAttribute('data-label')||th.textContent.replace(/[▲▼]\\s*$/,'').trim();
    th.setAttribute('data-label', base);
    const arrow=(th.dataset.wk===watchSort.key)?(watchSort.dir<0?' ▼':' ▲'):'';
    th.childNodes[0].nodeValue = base + arrow;
  });
}
function renderWatch(){
  const tb=document.getElementById('wlrows'); if(!tb) return;
  const syms=[...WATCH].sort((a,b)=>{
    const va=watchVal(a,watchSort.key), vb=watchVal(b,watchSort.key);
    let c; if(typeof va==='string'||typeof vb==='string') c=(''+va).localeCompare(''+vb);
    else c=(va>vb?1:va<vb?-1:0);
    if(c===0) c=dispSym(a).localeCompare(dispSym(b));   // stable tiebreak by symbol
    return watchSort.dir*c;
  });
  setWatchArrows();
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
  for(const [k,lbl] of [['5m','5m'],['15m','15m'],['1h','1h'],['4h','4h'],['1d','1D'],['1w','1W']]){
    const b=tb[k]; if(!b) continue;
    out+=`<span class="tfbias ${cls[b]}" data-tip="Market-structure bias on the ${lbl} chart: ${b} (from swing highs/lows + CHoCH). Lower timeframes (5m/15m) give early context for where the higher frames are heading.">${lbl}${sym[b]}</span>`;
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
  for(const [t,v] of [["market","Market"],["history","History"],["setups","Setups"],["flags","Flags"],["cpr","Cpr"],["bounce","Bounce"],["stb","Stb"],["shorts","Shorts"],["early","Early"],["dtb","Dtb"],["accum","Accum"],["capit","Capit"],["micro","Micro"],["signals","Signals"],["coil","Coil"],["scalp","Scalp"],["spot","Spot"],["bestlong","BestLong"],["bestshort","BestShort"],["watch","Watch"],["perf","Perf"],["backtest","Backtest"],["calls","Calls"],["analyze","Analyze"],["info","Info"]]){
    document.getElementById("tab"+v).classList.toggle("active", t===which);
    document.getElementById("view"+v).classList.toggle("active", t===which);
  }
  if(which==="market") renderMarket();
  if(which==="history") loadHistory();
  if(which==="bestlong") renderBestLong();
  if(which==="bestshort") renderBestShort();
  if(which==="coil") renderCoil();
  if(which==="scalp") renderScalp();
  if(which==="spot") renderSpot();
  if(which==="perf") renderPerf();
  if(which==="dtb") renderDtb();
  if(which==="accum") renderAccum();
  if(which==="backtest") loadBacktest();
  if(which==="calls") renderCalls();
  if(which==="watch"){ renderWatch(); loadWatch(); }
  renderBanner();  // banner follows the active scan tab
  renderFilterBar();  // filters are per-tab
}
let azTf="4h";
function setAzTf(tf){ azTf=tf;
  document.querySelectorAll(".tfbtn").forEach(x=>x.classList.toggle("active",x.dataset.tf===tf));
  if(document.getElementById("azInput").value.trim()) analyze();
}
// Long/Short perspective toggle for the Analyze card. null = the coin's own lean.
let azLast=null, azSide=null, azXtfHtml='', azXtfSide=null;   // azXtfSide: null=auto, 'long', 'short'
let azRec=null;   // the currently-shown MAIN recommended trade
// Track ANY setup — the main card or any of the cross-timeframe plans. Each card
// passes its own entry/stop/targets so you choose exactly which setup to track.
async function trackTrade(sym, side, entry, stop, targetsCsv, tf, msgId){
  const m=msgId?document.getElementById(msgId):null;
  if(!sym||entry==null||stop==null||!targetsCsv){ if(m) m.textContent='✗ missing levels'; return; }
  const q=new URLSearchParams({symbol:sym, side:side||'long', entry:entry, stop:stop, targets:targetsCsv, tf:tf||''});
  if(m) m.textContent='…';
  try{ const r=await fetch('/track?'+q.toString(),{cache:'no-store'}); const d=await r.json();
    if(m) m.textContent = d.ok? '✓ Tracking — see 📌 My calls' : '✗ Could not track (entry must sit between stop & target)';
    if(d.ok){ try{ const rr=await fetch('/data',{cache:'no-store'}); lastData=await rr.json(); renderPerf(); renderCalls(); }catch(e){} }
  }catch(e){ if(m) m.textContent='✗ Failed'; }
}
function trackSetup(ev){ if(ev) ev.stopPropagation();
  if(!azRec) return;
  trackTrade(azRec.symbol, azRec.side, azRec.entry, azRec.stop, (azRec.targets||[]).join(','), azRec.tf, 'trackmsg');
}
// Track a specific cross-TF plan card (b = the xtf trade). Builds its own TP ladder.
function trackXtf(i, ev){ if(ev) ev.stopPropagation();
  const b=(azXtfTop||[])[i]; if(!b||!b.be||!b.rec){ return; }
  const tps=(scaleOutRows(b.be.rt)||[]).map(r=>r.t&&r.t.lvl).filter(x=>x!=null);
  const tgts=tps.length?tps:[b.rec.lvl];
  trackTrade((azLast&&azLast.symbol)||'', b.side, b.be.level, (b.be.rs&&b.be.rs.level),
             tgts.join(','), b.tf, 'xtftrackmsg'+i);
}
let azXtfTop=null, azXtfOpen={};   // cached top-3 candidates + which alternatives are expanded
function toggleXtfTrade(i){ azXtfOpen[i]=!azXtfOpen[i]; if(azXtfTop){ azXtfHtml=xtfRender(azXtfTop); renderAz(); } }
function setXtfSide(s){ azXtfSide=(azXtfSide===s)?null:s;
  if(azLast){ azXtfHtml='<div class="azxtf azxtf-load">⏳ Re-scanning timeframes for the best '+(azXtfSide||'auto')+' plan…</div>'; renderAz(); crossTfSummary(azLast.symbol); } }
function xtfToggle(){ return `<span class="xtftog" data-tip="Pick which side to find the single best plan for across every timeframe: Auto = the coin's own lean per chart, or force the best LONG or best SHORT.">`
  +`<button class="${!azXtfSide?'on':''}" onclick="setXtfSide(null)">Auto</button>`
  +`<button class="${azXtfSide==='long'?'on':''}" onclick="setXtfSide('long')">Long</button>`
  +`<button class="${azXtfSide==='short'?'on short':''}" onclick="setXtfSide('short')">Short</button></span>`; }
// Cache of /analyze results per symbol per timeframe (for the cross-TF summary).
const AZ_TFS=['5m','15m','1h','4h','1d','1w'];
let azTfCache={};
function azCachePut(sym,tf,d){ sym=(sym||'').toUpperCase(); (azTfCache[sym]=azTfCache[sym]||{})[tf]={d,ts:Date.now()}; }
function renderAz(){ const box=document.getElementById("azResult"); if(!box||!azLast) return;
  // Cross-timeframe summary on top (full width); below it the coin analysis on the
  // left with the plain-English read as a sticky rail on the right, using the page's
  // full width so nothing is compressed.
  const pe=plainEnglishHtml(azLast);
  box.innerHTML=(azXtfHtml||'') + (pe
    ? `<div class="azcols"><div class="azmain">${azCard(azLast)}</div><aside class="azside">${pe}</aside></div>`
    : azCard(azLast)); }
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
    if(d.error){ azLast=null; azXtfHtml=''; box.innerHTML='<div class="azerr">'+d.error+'</div>'; }
    else { azLast=d; azSide=null;             // reset perspective to the coin's own lean
      azXtfHtml='<div class="azxtf azxtf-load">⏳ Comparing 15m · 1h · 4h · Daily · Weekly for the single best plan…</div>';
      renderAz(); azCachePut(d.symbol||sym, azTf, d); crossTfSummary(d.symbol||sym); }
  }catch(e){ box.innerHTML='<div class="azerr">Analysis failed — try again.</div>'; }
  btn.disabled=false; btn.textContent=t;
}
function setAzSide(s){
  azSide = (azSide===s) ? null : s;   // click the active side again to snap back to auto
  renderAz();
}
// Analyze this coin on EVERY timeframe and surface the single best-graded plan,
// independent of the timeframe being viewed — each level still labelled with its
// own source chart. Reuses the same recommendation engine per timeframe.
async function crossTfSummary(sym){
  const S=(sym||'').toUpperCase();
  await Promise.all(AZ_TFS.map(async tf=>{
    const c=azTfCache[S]&&azTfCache[S][tf];
    if(c && (Date.now()-c.ts)<120000) return;                 // fresh enough
    try{ const r=await fetch("/analyze?symbol="+encodeURIComponent(sym)+"&interval="+tf,{cache:"no-store"});
      const dd=await r.json(); if(!dd.error) azCachePut(S,tf,dd); }catch(e){}
  }));
  if(!azLast || (azLast.symbol||'').toUpperCase()!==S) return;  // user moved on
  // AUTO mode: respect the coin's DOMINANT direction across timeframes (higher TFs
  // weigh more) so a clearly bullish coin doesn't surface a counter-trend short as
  // "best" just because its EV edged out a nearly-equal long. Forced Long/Short wins.
  let leanScore=0; const wTF={'15m':1,'1h':2,'4h':3,'1d':4,'1w':4};
  for(const tf of AZ_TFS){ const c=azTfCache[S]&&azTfCache[S][tf]; if(!c) continue;
    const s=c.d.auto_side||c.d.side; if(s==='long') leanScore+=(wTF[tf]||1); else if(s==='short') leanScore-=(wTF[tf]||1); }
  const lean = leanScore>0?'long':leanScore<0?'short':null;
  // Collect the best plan on EVERY timeframe and (in Auto) BOTH directions, then rank
  // by grade then expected value — and show the TOP 3 so you see the strongest setup
  // AND its alternatives, not one debatable pick. In Auto the coin's dominant lean gets
  // a small tie-break nudge (so a bullish coin doesn't flip to a barely-better counter-
  // trend short), but a clearly higher-graded opposite-side trade still wins. Forced
  // Long/Short restricts to that side.
  const sides = azXtfSide? [azXtfSide] : ['long','short'];
  let cands=[];
  for(const tf of AZ_TFS){ const c=azTfCache[S]&&azTfCache[S][tf]; if(!c) continue; const dd=c.d;
    for(const side of sides){
      const dm=Object.assign({}, dd, (dd.plans&&dd.plans[side])||{}); dm.side=side;
      const be=pickEntry(dm); if(!be||!be.rt||!be.rt.primary) continue;
      const rec=be.rt.primary, gr=planGradeOf(rec.rr,rec.p), grank={'A+':4,'A':3,'B':2,'C':1}[gr];
      const leanBonus=(!azXtfSide && lean && side===lean)? 0.4 : 0;
      cands.push({tf,side,gr,grank,be,rec,ev:rec.ev,score:rec.ev+leanBonus});
    }
  }
  cands.sort((a,b)=> (b.grank-a.grank) || (b.score-a.score));
  const top=[];
  for(const c of cands){ if(top.length>=3) break;
    // In forced Long/Short mode always show 3 distinct-timeframe options (no dedup),
    // so you can compare e.g. the Daily vs 4h plan yourself. In Auto, collapse
    // near-identical entries so the same trade doesn't appear twice.
    if(!azXtfSide && top.some(t=> t.side===c.side && Math.abs(t.be.level-c.be.level)/(c.be.level||1)<0.006)) continue;
    top.push(c); }
  azXtfTop=top; azXtfOpen={};
  azXtfHtml = top.length? xtfRender(top)
    : `<div class="azxtf"><div class="xtfhead"><span class="xtftitle">⭐ Best across timeframes</span>${xtfToggle()}</div>`
      +`<div style="color:var(--dim);font-size:13px;margin-top:2px">No clean ≥1.5 R:R ${azXtfSide?azXtfSide.toUpperCase()+' ':''}plan on any timeframe (15m→Weekly) right now — better to wait${azXtfSide?', or try Auto / the other side':''}.</div></div>`;
  renderAz();
}
function xtfRender(top){
  const side0=top[0].side!=='short'?'long':'short';
  return `<div class="azxtf azxtf-${side0}" data-tip="The strongest trades for this coin right now across every timeframe (15m → Weekly) and, in Auto, both directions — ranked by plan grade then expected value. #1 is the top pick with its full 'enter now' and scale-out plan; the rest are alternatives. Each level shows which chart it comes from; hover any for what it is and why.">
    <div class="xtfhead"><span class="xtftitle">⭐ Best across timeframes</span>${xtfToggle()}<span class="xtfsub">top ${top.length} · ranked by grade × reach</span></div>
    ${top.map((b,i)=> xtfTradeBody(b,i)).join('')}</div>`;
}
function xtfTradeBody(b,i){
  const long=b.side!=='short', be=b.be, rec=b.rec, stop=be.rs, tfN=TFNAME[b.tf]||b.tf, detailed=(i===0)||!!azXtfOpen[i];
  const eTf=be.tf?(TFNAME[be.tf]||be.tf):tfN, sTf=tfLabelOf(stop&&stop.basis)||tfN, tTf=tfLabelOf(rec.kind)||tfN;
  const chip=(tf)=> tf?` <span class="tfsrc">${tf}</span>`:'';
  const rank=['①','②','③','④','⑤'][i]||('#'+(i+1));
  const cmp=azLast?(azLast.live!=null?azLast.live:azLast.price):null;
  let cmpRR=null;
  if(cmp!=null && stop && rec){ const t=rec.lvl, s=stop.level;
    if(long?(t>cmp&&s<cmp):(t<cmp&&s>cmp)) cmpRR=Math.abs(t-cmp)/Math.abs(cmp-s); }
  const cmpLine = (!detailed||cmp==null)?'' : `<div class="xtfcmp" data-tip="What this trade looks like if you enter NOW at the current market price (${fmtNum(cmp)}) instead of waiting for the recommended ${fmtNum(be.level)} pullback — same stop and target, so usually a lower reward:risk.">⚡ Enter now (CMP ${fmtNum(cmp)}): ${cmpRR!=null?`R:R <b>${cmpRR.toFixed(2)}</b> to target`:'the stop is already in the way — no clean entry here yet'} <span style="color:var(--dim2)">· vs ${rec.rr.toFixed(2)} waiting for ${fmtNum(be.level)}</span></div>`;
  return `<div class="xtftrade${detailed?' xtftrade-top':''}">
    <div class="xtfhead2"><span class="xtfrank">${rank}</span>
      <span class="xtfside ${long?'v-long':'v-short'}">${long?'LONG ▲':'SHORT ▼'}</span>
      <span class="vgrade" data-tip="Plan quality (A+→C), blending reward:risk with how reachable the target is. A+ needs a strong R:R and a ≥55% chance of reaching target.">Grade ${b.gr}</span>
      <span class="xtftf" data-tip="The timeframe whose plan graded highest for this trade — the individual levels below can each come from a different chart.">${tfN} chart</span>
      ${i>0?`<button class="wlbtn xtfexp" onclick="toggleXtfTrade(${i})">${detailed?'− collapse':'+ expand'}</button>`:''}
      ${b.tf!==azTf?`<button class="wlbtn" onclick="setAzTf('${b.tf}')">View on ${tfN} ↗</button>`:`<span class="xtfcur">— viewing this timeframe</span>`}
      <button class="wlbtn xtftrackbtn" onclick="trackXtf(${i},event)" data-tip="Track THIS ${tfN} plan (its entry, stop & TP ladder) in 📌 My calls — scale-out aware, graded in R.">📌 Track</button><span class="trackmsg" id="xtftrackmsg${i}"></span></div>
    <div class="xtfrow">
      <span class="xtfi" data-tip="Entry ${fmtNum(be.level)} — the recommended fill: the best value-area pullback that maximises reward:risk while still being a level price is likely to reach. From the ${eTf} chart. Why: ${esc(be.basis||'best value-area level')}."><i>Entry</i>${fmtNum(be.level)}${chip(eTf)}</span>
      <span class="xtfi" data-tip="Stop ${fmtNum(stop.level)} — the level that invalidates the trade, just beyond real structure and clear of the wick/noise range. From the ${sTf} chart. Why: ${esc(stop.basis||'—')}."><i>Stop</i>${fmtNum(stop.level)}${chip(sTf)}</span>
      <span class="xtfi" data-tip="Target ${fmtNum(rec.lvl)} — the best take-profit by expected value that clears the 1.5:1 floor. From the ${tTf} chart. Why: ${esc(rec.kind||'overhead resistance')}."><i>Target</i>${fmtNum(rec.lvl)}${chip(tTf)}</span>
      <span class="xtfi" data-tip="Reward:risk from the recommended entry to the target over the stop. Only ≥1.5:1 shown."><i>R:R</i><b>${rec.rr.toFixed(2)}</b></span>
      <span class="xtfi" data-tip="Reach — estimated chance of hitting the target before the stop, from ATR distance adjusted for trend / momentum / volume."><i>Reach</i>${Math.round(rec.p*100)}%</span>
    </div>${detailed?xtfWhy(b):''}${cmpLine}${detailed?dcaPlanHtml(azLast, be.level, stop&&stop.level, rec.lvl, b.side, be.distATR, true):''}${detailed?scaleOutHtml(be.rt, be.level, true):''}</div>`;
}
// Plain-English rationale for why the top trade is the recommended one.
function xtfWhy(b){
  const long=b.side!=='short', rec=b.rec, d=azLast||{}, bias=(d.bias||'').toLowerCase(), r=[];
  r.push(`the highest-graded plan across all timeframes (Grade ${b.gr})`);
  r.push(`R:R ${rec.rr.toFixed(2)} at ~${Math.round(rec.p*100)}% reach — ${rec.p>=0.55?'more likely than not to hit target':rec.p>=0.4?'a solid expected-value edge':'a high payoff on a lower hit-rate'}`);
  if(long && (d.trend==='up'||bias==='bullish')) r.push(`in line with the coin's bullish structure`);
  else if(!long && (d.trend==='down'||bias==='bearish')) r.push(`in line with the coin's bearish structure`);
  else r.push(`a counter-trend / reversal setup — treat as unconfirmed until price turns`);
  if(b.be&&b.be.basis) r.push(`entering at ${esc(b.be.basis)}`);
  return `<div class="xtfwhy" data-tip="Why the engine ranks this the top trade right now — grade (R:R × reach), how it aligns with the coin's structure, and where the entry sits.">💡 <b>Why this one:</b> ${r.join(' · ')}.</div>`;
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
function surviveProb(riskATR, mid){
  if(riskATR==null) riskATR=1.5;
  if(mid==null) mid=1.5;
  return Math.max(0.05, Math.min(0.96, 1/(1+Math.exp(-(riskATR-mid)*1.7))));
}
// Liquidity / size tier from open interest + volatility. A big-OI, low-volatility
// coin (BTC/ETH/SOL-like) respects levels and wicks far less, so a TIGHTER stop is
// safe and suits higher leverage; a thin, high-ATR coin needs more wick clearance.
// The survival curve's midpoint shifts LEFT for big coins (tight stops survive) and
// RIGHT for thin ones (they need room).
function liqTier(d){ const oi=d&&d.open_interest&&d.open_interest.oi_usd, atr=d&&d.atr_pct; let s=0;
  if(oi!=null){ if(oi>=5e8) s+=2; else if(oi>=1e8) s+=1; else if(oi<1e7) s-=1; }
  if(atr!=null){ if(atr<=2) s+=1; else if(atr>=6) s-=1; }
  return s>=2?'mega':s===1?'large':s<=-1?'thin':'mid'; }
function survMid(tier){ return tier==='mega'?1.05:tier==='large'?1.3:tier==='thin'?1.85:1.5; }
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
  const mid=survMid(liqTier(d));                         // liquidity-aware survival midpoint
  let best=null;
  for(const s of pool){
    const rt=recTargets(d, E, s.level);
    const ev=(rt&&rt.primary)? rt.primary.ev : 0;
    if(ev<=0) continue;                                  // must yield a >=1.5 R:R trade
    let sc=ev*surviveProb(atrx(s.level), mid);
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
    const rr=Math.abs(lvl-entry)/risk;                 // true R:R, uncapped (for display)
    const dATR=atr? (move*100)/atr : null;
    const p=reachProb(dATR, pot);
    cand.push({lvl,kind,move,rr,p,dATR,ev:Math.min(rr,8)*p});   // cap only the internal EV math
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
const TFRANK={'15m':0,'1h':1,'4h':2,'1d':3,'1w':4};
const TFNAME={'5m':'5m','15m':'15m','1h':'1h','4h':'4h','1d':'Daily','1w':'Weekly'};
// Global basis→timeframe label (used by the cross-timeframe summary).
function tfLabelOf(b){ b=(''+(b||'')).toLowerCase();
  if(/weekly|1w/.test(b)) return 'Weekly'; if(/daily|1d/.test(b)) return 'Daily';
  if(/\b4h\b/.test(b)) return '4h'; if(/15\s*m/.test(b)) return '15m';
  if(/\b1h\b/.test(b)) return '1h'; return null; }
// Plan grade — blends reward:risk with REACH (the estimated chance of actually
// hitting the target). A high R:R alone isn't enough: a trade you only reach
// <45% of the time is a solid B, not an A, no matter how big the payoff. A needs
// reach ≥45%, A+ needs ≥55% AND a strong R:R.
function planGradeOf(rr,p){ if(rr==null||p==null) return '—';
  if(rr>=3   && p>=0.55) return 'A+';
  if(rr>=2.5 && p>=0.45) return 'A';
  if(rr>=2   && p>=0.35) return 'B';
  return 'C'; }
// A concrete scale-out plan: how much to sell at each target, and when to move the
// stop to break-even. Default management: bank a first tranche at the nearest solid
// target and de-risk to break-even (trade can no longer lose), hold the core to the
// base target, leave a runner for the stretch / breakout target.
// Adaptive scale-out weights — take MORE off the more-reachable (lower-R) near
// targets, leave a runner on the far high-R ones. Weight ∝ 1/(1+0.5·R), normalised,
// so the split reflects THIS setup's target spacing (never a fixed 40/35/25). Matches
// the backend performance tracker exactly.
function scaleWeights(rrs){
  const raw=(rrs||[]).map(r=>1/(1+0.5*Math.max(r||1,0.1)));
  const tot=raw.reduce((a,b)=>a+b,0)||1;
  return raw.map(x=>x/tot);
}
function scaleOutRows(rtg){
  if(!rtg||!rtg.primary) return [];
  const base=rtg.primary, sec=rtg.secure, str=rtg.stretch;
  const distinct=(a,b)=> a&&b&&Math.abs(a.lvl-b.lvl)/(b.lvl||1)>0.004;
  const hasSec=distinct(sec,base), hasStr=distinct(str,base)&&(!sec||distinct(str,sec));
  let tgs;
  if(hasSec&&hasStr) tgs=[{t:sec,icon:'🔒',be:true},{t:base,icon:'⭐'},{t:str,icon:'🚀',runner:true}];
  else if(hasSec)    tgs=[{t:sec,icon:'🔒',be:true},{t:base,icon:'⭐'}];
  else if(hasStr)    tgs=[{t:base,icon:'⭐',be:true},{t:str,icon:'🚀',runner:true}];
  else               tgs=[{t:base,icon:'⭐'}];
  const w=scaleWeights(tgs.map(x=>x.t&&x.t.rr));
  tgs.forEach((x,i)=>{ x.pct=Math.round(w[i]*100)+'%'; });
  return tgs;
}
function scaleOutHtml(rtg, entry, compact){
  const rows=scaleOutRows(rtg); if(!rows.length) return '';
  const items=rows.map(r=>{
    const rr=(r.t.rr!=null)?` <span class="rr">R${r.t.rr.toFixed(1)}</span>`:'';
    const be=r.be?` <span class="soBE">→ move stop to break-even (${fmtNum(entry)}) — now risk-free</span>`:'';
    const run=r.runner?` <span style="color:var(--dim)">— runner: trail your stop up under each new higher low</span>`:'';
    return `<li><b>Sell ${r.pct}</b> at ${r.icon} <b>${fmtNum(r.t.lvl)}</b>${rr}${be}${run}</li>`;
  }).join('');
  return `<div class="soplan${compact?' socompact':''}" data-tip="A concrete way to take the trade off and manage risk: bank a first tranche at the nearest solid target and move your stop to break-even (so the trade can't turn into a loss), hold the core to the base target, and leave a runner for the stretch / breakout target with a trailing stop.">
    <div class="sohead">📤 Scale-out &amp; risk management</div><ul class="solist">${items}</ul></div>`;
}
// A DCA / laddered ENTRY plan — the mirror of the scale-out. For BIGGER swing setups
// (a wide stop, or a deep patient pullback) you rarely nail the exact turn, so split
// the position across a few limit orders from the recommended entry toward — but not
// into — the stop. Better blended entry + bigger cushion; if the lower rungs never
// fill you're simply in a smaller position at an even better R:R. One stop for the
// whole position sits below the deepest rung. Returns '' for tight/scalp setups.
// Candidate structural levels between the entry and the stop that a DCA rung could
// sit on — real places price reacts (HTF swing supports/resistances, the 200-EMA,
// Supertrend, this-TF swing pivots), each with a label and a strength (higher-TF =
// stronger, gets hit first on a pullback).
function dcaLevels(d, side, recE){
  const long = side!=='short', tl = d.tf_levels||{}, cand=[];
  const push=(lvl,label,str)=>{ if(lvl!=null && isFinite(+lvl)) cand.push({lvl:+lvl,label,str}); };
  if(long){
    push(tl['1w']&&tl['1w'].sup,'Weekly support',4);
    push(tl['1d']&&tl['1d'].sup,'Daily support',3);
    push(tl['4h']&&tl['4h'].sup,'4h support',2);
    push(tl['1h']&&tl['1h'].sup,'1h support',1);
    if(d.ema!=null && d.ema<recE) push(d.ema,'200-EMA',3);
    if(d.supertrend!=null && d.supertrend_role==='support' && d.supertrend<recE) push(d.supertrend,'Supertrend',2);
    (d.supports||[]).forEach(s=> push(s,'swing low',1));
    return cand.filter(c=> c.lvl < recE);
  }
  push(tl['1w']&&tl['1w'].res,'Weekly resistance',4);
  push(tl['1d']&&tl['1d'].res,'Daily resistance',3);
  push(tl['4h']&&tl['4h'].res,'4h resistance',2);
  push(tl['1h']&&tl['1h'].res,'1h resistance',1);
  if(d.ema!=null && d.ema>recE) push(d.ema,'200-EMA',3);
  if(d.supertrend!=null && d.supertrend_role==='resistance' && d.supertrend>recE) push(d.supertrend,'Supertrend',2);
  (d.resistances||[]).forEach(s=> push(s,'swing high',1));
  return cand.filter(c=> c.lvl > recE);
}
// A DCA / laddered ENTRY plan for BIGGER swing setups (wide entry→stop zone). Rather
// than mechanically slicing the zone, each deeper rung is SNAPPED to the nearest real
// structural level (weekly/daily/4h support, 200-EMA, Supertrend) so limit orders sit
// where price is actually likely to bounce — falling back to a pullback-zone price
// only where no level is nearby. Better blended entry + bigger cushion; if the lower
// rungs never fill you're simply in a smaller position at an even better R:R, and one
// stop covers the whole position, just beyond the deepest rung.
function dcaPlanHtml(d, recE, stop, tp, side, distATR, compact){
  if(!d||recE==null||stop==null||tp==null) return '';
  const long = side!=='short';
  const risk = Math.abs(recE - stop);
  if(!risk) return '';
  const riskPct = risk/recE*100;
  if(riskPct < 3) return '';                    // tight setups just take the single fill
  const zoneLo = long ? stop + 0.15*risk : stop - 0.15*risk;   // deepest allowable rung — keep a buffer to the stop
  const span = recE - zoneLo;                   // signed (+ve long, −ve short)
  const cand = dcaLevels(d, side, recE).filter(c=> long ? c.lvl>=zoneLo : c.lvl<=zoneLo);
  const tol = 0.32*Math.abs(span);
  const used=[recE];
  const near=(v)=> used.some(u=> Math.abs(u-v)/(recE||1) < 0.004);
  function snap(target){
    let best=null,bd=1e18;
    for(const c of cand){
      if(near(c.lvl)) continue;
      const dd=Math.abs(c.lvl-target);
      if(dd<=tol && (best==null || c.str>best.str || (c.str===best.str && dd<bd))){ best=c; bd=dd; }
    }
    return best;
  }
  const rungs=[{p:recE,label:'recommended entry'}];
  [[0.45,'pullback zone'],[0.85,'deep pullback']].forEach(([f,fb])=>{
    const target = recE - span*f;
    const s = snap(target);
    if(s){ rungs.push({p:s.lvl,label:s.label}); used.push(s.lvl); }
    else if(!near(target)){ rungs.push({p:target,label:fb}); used.push(target); }
  });
  if(rungs.length<2) return '';                 // couldn't build a real ladder
  const W = rungs.length===3? [0.30,0.33,0.37] : [0.42,0.58];
  const avg = rungs.reduce((s,r,i)=> s + r.p*W[i], 0);
  const rrAvg = Math.abs(tp - avg)/Math.abs(avg - stop);
  const rrSingle = Math.abs(tp - recE)/Math.abs(recE - stop);
  const dir = long?'Buy':'Sell';
  const items = rungs.map((r,i)=>{
    const dp = Math.abs(r.p/recE - 1)*100;
    const where = i===0? 'at recommended entry' : `${dp.toFixed(1)}% ${long?'lower':'higher'} · ${r.label}`;
    return `<li><b>${dir} ${Math.round(W[i]*100)}%</b> at <b>${fmtNum(r.p)}</b> <span class="rr">${where}</span></li>`;
  }).join('');
  return `<div class="soplan dcaplan${compact?' socompact':''}" data-tip="For a bigger swing setup you rarely catch the exact turn — so ladder in. Each deeper rung is placed on a REAL level price tends to react at (weekly/daily/4h support, the 200-EMA or Supertrend), not an arbitrary fraction — so your limit orders sit where a bounce is actually plausible. You get a better blended entry and a bigger cushion; if price reverses before the lower rungs fill you're simply in a smaller position at an even better R:R. One stop covers the whole position, just beyond the deepest rung, and the R:R below is measured from the AVERAGE fill.">
    <div class="sohead">🧩 DCA / laddered entry <span class="azsub">bigger setup — scale in on real levels, don't chase one price</span></div>
    <ul class="solist">${items}</ul>
    <div class="dcaavg">Average entry ≈ <b>${fmtNum(avg)}</b> · R:R from avg <b>${rrAvg.toFixed(2)}</b> <span style="color:var(--dim)">(vs ${rrSingle.toFixed(2)} single-fill) · one stop for the whole position at ${fmtNum(stop)}</span></div></div>`;
}
// Derivatives confluence line (Coinalyze) — does real positioning back the
// price-based setup, or warn against it? Empty unless a COINALYZE_API_KEY is set.
function derivConfluenceHtml(d){
  const v=d&&d.derivatives; if(!v) return '';
  const parts=[];
  if(v.divergence_note){
    const good=v.divergence==='real_up'||v.divergence==='real_down';
    const warn=v.divergence==='fake_up'||v.divergence==='exhaust_down';
    parts.push(`${good?'✅':warn?'⚠':'•'} <b>OI:</b> ${esc(v.divergence_note)}`);
  }
  if(v.funding!=null){ const f=v.funding*100;
    if(Math.abs(f)>=0.03) parts.push(`<b>Funding ${f>=0?'+':''}${f.toFixed(3)}%</b> — ${f>=0?'crowded longs (squeeze-down risk)':'crowded shorts (squeeze-up fuel)'}`);
  }
  if(v.long_short!=null && (v.long_short>1.3||v.long_short<0.77))
    parts.push(`L/S ${v.long_short.toFixed(2)} (${v.long_short>1?'crowd long':'crowd short'})`);
  if(v.liq_side==='long') parts.push(`🩸 <b>longs flushed</b> (bounce watch)`);
  else if(v.liq_side==='short') parts.push(`🩸 <b>shorts squeezed</b> (fade watch)`);
  if(!parts.length) return '';
  const cls=(v.divergence==='fake_up'||v.divergence==='exhaust_down')?'derivwarn':(v.divergence==='real_up'||v.divergence==='real_down')?'derivgood':'derivneu';
  return `<div class="derivbox ${cls}" data-tip="Derivatives confluence from Coinalyze — open-interest divergence, funding and long/short positioning. Real moves have open interest backing them; short-covering 'fake pumps' don't. Use it to confirm or fade the price-based setup.">🔬 Derivatives confluence: ${parts.join(' · ')}</div>`;
}
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
    const score=ee.rtg.primary.ev*surviveProb(ee.stop.atrx, survMid(liqTier(d)))*(0.35+0.65*fill)*tfBoost*shieldPen;
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
  // Capture the recommended trade (scale-out TP ladder) for the "Track this setup" button.
  try{
    const _tps=(scaleOutRows(rtg)||[]).map(r=>r.t&&r.t.lvl).filter(x=>x!=null);
    azRec=(recE!=null && rstop && _tps.length)
      ? {symbol:d0.symbol, side:(d.side||activeSide||'long'), entry:recE, stop:rstop.level, targets:_tps, tf:azTf}
      : null;
  }catch(e){ azRec=null; }
  // When the winning entry comes from a HIGHER timeframe than the one being viewed,
  // say so — that's the whole "take the Daily support over the deeper 4h dip" idea.
  const viewRankAz=TFRANK[d.interval||'4h']||2;
  const beHigherTf=(be&&be.tf&&(TFRANK[be.tf]||2)>viewRankAz);
  const crossTfTag=beHigherTf?` <span class="tfsrc" data-tip="This entry level is the ${TFNAME[be.tf]} chart's support — a higher, stronger timeframe than the ${d.interval||'4h'} you're viewing. Price should turn there first, so it's the smarter fill.">${TFNAME[be.tf]}</span>`:'';
  const crossTfNote=beHigherTf?` · <b style="color:var(--accent)">from the ${TFNAME[be.tf]} chart</b> — a stronger level price should reach first, so it beats waiting for a deeper ${d.interval||'4h'} dip that may never fill`:'';
  // Small colored bull/bear/neutral marker for a timeframe, from tf_bias.
  const tfMark=(tf)=>{ const b=(d.tf_bias||{})[tf];
    const co=b==='bullish'?'var(--accent)':b==='bearish'?'#f85149':'var(--dim2)';
    const sy=b==='bullish'?'▲':b==='bearish'?'▼':'–';
    return `<span style="color:${co};font-weight:700" title="${tf.toUpperCase()} market-structure bias: ${b||'n/a'}">${sy}</span>`; };
  const tfSup=(tf,v,sign)=>`${tf==='1d'?'1D':tf==='1w'?'1W':tf} ${tfMark(tf)} ${v==null?'—':fmtNum(v)+pct(v,sign)}`;
  const rec=(rtg&&rtg.primary)?{tp:rtg.primary.lvl, rr:rtg.primary.rr, move:rtg.primary.move, p:rtg.primary.p, kind:rtg.primary.kind}:{tp:null};
  // Which timeframe a level's basis text refers to (for "based on the Daily chart" labels).
  const tfOfBasis=(b)=>{ b=(''+(b||'')).toLowerCase();
    if(/weekly|1w/.test(b)) return 'Weekly'; if(/daily|1d/.test(b)) return 'Daily';
    if(/\b4h\b/.test(b)) return '4h'; if(/\b1h\b/.test(b)) return '1h'; return null; };
  const stopTf=rstop?tfOfBasis(rstop.basis):null;
  const tgtTf=rec.tp!=null?tfOfBasis(rec.kind):null;
  const isRec=(lvl)=> rec.tp!=null && lvl!=null && Math.abs(lvl-rec.tp)/(rec.tp||1) < 0.004;
  const near=(lvl,ref)=> ref!=null && lvl!=null && Math.abs(lvl-ref)/(ref||1) < 0.004;
  const isSecure=(lvl)=> rtg&&rtg.secure&&near(lvl,rtg.secure.lvl);
  const isStretch=(lvl)=> rtg&&rtg.stretch&&near(lvl,rtg.stretch.lvl);
  const sgn=(d.side||'long')==='short'?'+':'−';         // stop/entry sign relative to price for this side
  // Distance of a level in ATR units (the honest "how tight/far" for THIS coin).
  const atrxOf=(lvl,ref)=>{ const r=(ref!=null)?ref:d.price; if(lvl==null||r==null||!d.atr_pct) return null;
    return Math.abs((lvl-r)/r*100)/d.atr_pct; };
  const slInfo=(lvl)=>{ const p=(d.price&&lvl!=null)?Math.abs((lvl-d.price)/d.price*100):null; const a=atrxOf(lvl,d.price);
    return (p!=null?`${p.toFixed(1)}%`:'')+(a!=null?` · ${a.toFixed(1)}×ATR`:''); };
  const planGrade=(rec.tp==null)?'—':planGradeOf(rec.rr,rec.p);
  // R:R to the recommended (base) target over the recommended stop, from ANY
  // chosen entry — lets each entry cell show its own R:R and lets us compare
  // entering now at market vs waiting for the recommended pullback.
  const rrAt=(en)=>{ if(en==null||rec.tp==null||!rstop) return null; const stop=rstop.level;
    const long=(d.side||'long')!=='short';
    if(long?(rec.tp<=en||stop>=en):(rec.tp>=en||stop<=en)) return null;
    return Math.abs(rec.tp-en)/Math.abs(en-stop); };
  const rrTag=(en)=>{ const r=rrAt(en); return r!=null?` <span class="rr">R:R ${r.toFixed(2)}</span>`:''; };
  const cmpE=(d.live!=null?d.live:d.price);   // current market price ("enter now")
  // Reward:risk of a target measured from the retest entry over a given stop
  // (side-aware, only counts targets on the right side, capped at 8:1).
  const rrOn=(tpv,stop)=>{ if(tpv==null||recE==null||stop==null||recE===stop) return null;
    const long=(d.side||'long')!=='short'; if(long? tpv<=recE : tpv>=recE) return null;
    return Math.abs(tpv-recE)/Math.abs(recE-stop); };
  const recRR=(tpv)=> rrOn(tpv, rstop?rstop.level:d.sl_tight);
  const tightRR=(tpv)=> rrOn(tpv, d.sl_tight);
  // R:R if you enter NOW at the current market price (over the recommended stop).
  const cmpRR=(tpv)=>{ const stop=rstop?rstop.level:d.sl_tight; if(tpv==null||cmpE==null||stop==null||cmpE===stop) return null;
    const long=(d.side||'long')!=='short'; if(long? tpv<=cmpE : tpv>=cmpE) return null;
    return Math.abs(tpv-cmpE)/Math.abs(cmpE-stop); };
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
        <span class="vbadge v-${(d.side||'long')==='short'?'short':'long'}" data-tip="The side to trade — LONG (buy the dip, profit as it rises) or SHORT (sell strength, profit as it falls) — based on this coin's own directional lean. Use the LONG/SHORT toggle to plan the other side.">${(d.side||'long')==='short'?'SHORT ▼':'LONG ▲'}</span>
        <span class="vgrade" data-tip="Overall quality of this setup: A+ / A / B / C. It blends the reward:risk with how reachable the target is — an A+ has both a high R:R and a high chance of getting there.">Grade ${planGrade}</span><span class="vsep"></span>
        <span class="vitem" data-tip="The recommended place to get IN — a value-area pullback (support / EMA / Supertrend retest), not necessarily the current price. Chosen across timeframes for the best reward:risk that's still likely to fill.${beHigherTf?' Taken from the '+TFNAME[be.tf]+' chart — a stronger level price should reach first.':' Based on: '+esc(be?be.basis:'')}"><i>Entry</i>${fmtNum(recE)}${crossTfTag}</span>
        <span class="vitem" data-tip="The recommended stop-loss — the level that invalidates the trade, just beyond real structure and clear of the wick/noise range. Shown with its distance in ×ATR (the honest measure of 'tight'). Based on: ${esc(rstop?rstop.basis:'—')}${stopTf?' ('+stopTf+' chart)':''}."><i>Stop</i>${rstop?fmtNum(rstop.level):'—'}${rstop&&rstop.atrx?` <span style="color:var(--dim2)">${rstop.atrx.toFixed(1)}×</span>`:''}${stopTf&&beHigherTf?`<span class="tfsrc">${stopTf}</span>`:''}</span>
        <span class="vitem" data-tip="The recommended take-profit — the best target by expected value (reward:risk × reachability) that clears the 1.5:1 floor. Based on: ${esc(rec.kind||'overhead resistance')}${tgtTf?' ('+tgtTf+' chart)':''}."><i>Target</i>${fmtNum(rec.tp)}${tgtTf&&/Daily|Weekly/.test(rec.kind||'')?`<span class="tfsrc">${tgtTf}</span>`:''}</span>
        <span class="vitem" data-tip="Reward-to-risk: how many units of profit to the target for each unit risked to the stop, from the recommended entry. We only recommend trades clearing 1.5:1."><i>R:R</i><b>${rec.rr.toFixed(2)}</b></span>
        <span class="vitem" data-tip="Reachability — the estimated chance price actually reaches the target before the stop, from the distance (in ATR) adjusted for how strongly the trend, momentum and volume back the move."><i>Reach</i>${Math.round(rec.p*100)}%</span>
        <span class="vitem" data-tip="Reward:risk if you enter NOW at the current market price (${fmtNum(cmpE)}) over the recommended stop, instead of waiting for the ${fmtNum(recE)} pullback — same stop and target, usually a bit lower than waiting."><i>Now · CMP</i>${rrAt(cmpE)!=null?'<b>'+rrAt(cmpE).toFixed(2)+'</b>':'—'}</span>`
      :`<span class="vbadge v-none">⛔ NO TRADE</span>
        <span class="vitem" style="font-family:'Inter'">Best realistic R:R is only <b>${rtg?rtg.bestRR.toFixed(2):'—'}</b> — under the 1.5 floor. Wait for a better entry or setup.</span>`}
    </div>
    <div class="aztags">
      <span class="aztag ${d.ema_reclaim?'on':''}">200-EMA reclaim${d.ema_reclaim?' · '+d.ema_reclaim_score:''}</span>
      <span class="aztag ${d.bull_flag?'on':''}">Bull flag${d.bull_flag?' · '+d.bull_flag_score:''}</span>
      <span class="aztag ${d.support_bounce?'on':''}" ${d.support_bounce?`data-tip="Flagged by clustering ${d.support_bounce_tf} ${d.support_bounce_method||'swing-low pivot'} levels (tested ${d.support_bounce_touches||'?'}× ). The support is the ${d.support_bounce_method||'swing-low pivot zone'} at ${fmtNum(d.support_bounce_support)}."`:''}>Support bounce${d.support_bounce?` · off ${d.support_bounce_tf} ${d.support_bounce_method||'swing-low'} ${fmtNum(d.support_bounce_support)} (${d.support_bounce_touches||'?'}×) · score `+d.support_bounce_score:''}</span>
    </div>
    ${derivConfluenceHtml(d)}
    <div class="azsec">Market read</div>
    <div class="azgrid">
      ${cell("Structure", (d.structure||'—')+(d.choch?` · ${d.choch} CHoCH`:''), d.struct_reason||"Market structure from swing highs/lows. CHoCH = the first break the other way — an early reversal cue that can appear inside a trend.")}
      ${cell("RSI (14)", d.rsi==null?'—':(+d.rsi).toFixed(0)+(d.rsi<30?' oversold':d.rsi>70?' overbought':''), "Relative Strength Index (0-100) — momentum. Below 30 = oversold (bounce potential), above 70 = overbought (pullback risk).")}
      ${cell("RSI divergence", (()=>{const v=d.rsi_div; if(!v) return '<span style="color:var(--dim2)">none on this TF</span>'; const ic=v.dir==='bullish'?'✅':'⚠'; return `${ic} ${v.label.replace(' RSI divergence','')}`; })(), d.rsi_div?esc(d.rsi_div.label+' — '+d.rsi_div.note+' Regular divergence = reversal signal; hidden divergence = trend-continuation signal. It feeds the directional lean.'):"RSI divergence between the last two price swings vs RSI — checked on this timeframe. Regular (reversal) and hidden (continuation) divergences are detected; 'none' means no clean divergence right now. When present, it feeds the directional lean.")}
      ${cell("Volume", (d.vol_trend||'—')+(d.vol_ratio?` ×${d.vol_ratio}`:''), `Recent volume is ${d.vol_trend||'—'} vs its average. Rising volume in an uptrend = buyers committed (bullish confirmation); rising volume in a downtrend = sellers in control (bearish confirmation). Falling volume during a move usually means momentum is fading — expect consolidation or a possible reversal. Here the trend is ${d.trend||'flat'}.`)}
      ${cell("Pressure", d.pressure||'—', `Buyers vs sellers over recent candles (volume on up-candles vs down-candles). '${d.pressure||'—'}' are in control. Buyers-in-control backs a long; sellers-in-control backs a short; balanced = indecision.`)}
      ${cell("Rel volume", d.rvol==null?'—':(+d.rvol).toFixed(2)+'× latest bar', "The latest candle's volume ÷ its 20-bar average. Above 1× = the current move is happening on above-average participation = stronger confirmation. Below 1× = quiet, less conviction.")}
      ${cell("Range position", d.range_pos==null?'—':d.range_pos+'%', `Where price sits in its recent 120-candle range on the ${d.interval||'4h'} timeframe (0% = range low, 100% = range high).`)}
      ${cell("ATR", d.atr_pct==null?'—':d.atr_pct+'%', "Average True Range as a % of price — the coin's volatility. Stops are buffered by a fraction of this.")}
      ${d.squeeze_pct!=null?cell("Volatility squeeze", (()=>{const s=d.squeeze_pct; const lbl=s>=85?'🚀 very tight':s>=70?'🚀 coiled':s<=25?'wide':'normal'; return `${s}% <span class="rr">${lbl}</span>`; })(), `Bollinger-band-width squeeze on the ${d.interval||'4h'}: the current band width is tighter than this % of its recent range. 70%+ = coiled/compressed → a bigger move (expansion) tends to follow, so trade the break. Low = bands already wide (mid-move). See the 🚀 Coiled tab for the whole universe ranked by this.`):''}
      ${cell("Open interest", (()=>{const o=d.open_interest; if(!o||o.oi_usd==null) return '—'; const m=o.oi_usd; const s=m>=1e9?'$'+(m/1e9).toFixed(2)+'B':m>=1e6?'$'+(m/1e6).toFixed(1)+'M':'$'+(m/1e3).toFixed(0)+'K'; return s+(o.chg24!=null?` <span class="rr">${o.chg24>=0?'+':''}${o.chg24.toFixed(1)}% 24h</span>`:''); })(), "Open interest — the total notional in open perpetual positions right now (holdVol × price). Rising OI as price rises = new money backing the move (conviction). Price rising while OI is flat or falling = short-covering or a thin move that can be a fake pump — confirm before chasing. Perps only.")}
      ${cell("Liquidity tier", (()=>{const t=liqTier(d); return ({mega:'Mega-cap',large:'Large-cap',mid:'Mid',thin:'Thin / illiquid'})[t]; })(), "Size/liquidity tier from open interest + volatility. Mega/large-cap coins (big OI, low ATR — BTC/ETH/SOL-like) respect levels and wick far less, so the recommended STOP is allowed to sit tighter (better R:R, suits higher leverage). Thin/high-volatility coins get more wick clearance so a random spike doesn't stop you out.")}
      ${(d.derivatives&&d.derivatives.oi_chg_pct!=null)?cell("OI trend (24h)", (()=>{const v=d.derivatives; const oc=v.oi_chg_pct; const dv=v.divergence; const dl=dv==='fake_up'?'⚠ fake-pump risk':dv==='real_up'?'✅ real up':dv==='real_down'?'✅ real down':dv==='exhaust_down'?'⚠ selloff exhausting':'flat'; return `${oc>=0?'+':''}${oc}% <span class="rr">${dl}</span>`; })(), "Open-interest change over 24h vs price, from Coinalyze. Price up + OI up = new money (real). Price up + OI down = short-covering / fake pump. Price down + OI up = fresh shorts (real). Price down + OI down = longs unwinding (selloff may be exhausting)."):''}
      ${(d.derivatives&&d.derivatives.oi_tf&&d.derivatives.oi_tf.length)?cell("OI by timeframe", (()=>{const arr=d.derivatives.oi_tf; const ic=dv=>(dv==='real_up'||dv==='real_down')?'✅':(dv==='fake_up'||dv==='exhaust_down')?'⚠':'•'; return arr.map(t=>`<b>${t.tf}</b> <span class="${(t.oi_chg||0)>=0?'pf-good':'pf-bad'}">${t.oi_chg>=0?'+':''}${t.oi_chg}%</span> ${ic(t.divergence)}`).join(' · ');})(), "Open-interest change over 1h / 4h / 12h / 24h, each read against price on that same window: ✅ = OI backs the move (real), ⚠ = OI diverges from price (fake pump / exhaustion), • = flat/no divergence. Check the OI story on the timeframe you're actually trading, not just 24h. From Coinalyze (hourly series, no extra calls)."):''}
      ${(d.derivatives&&d.derivatives.funding!=null)?cell("Funding rate", (()=>{const f=d.derivatives.funding*100; const lbl=f>=0.03?'crowded long':f<=-0.03?'crowded short':'balanced'; return `${f>=0?'+':''}${f.toFixed(4)}% <span class="rr">${lbl}</span>`; })(), "Latest perpetual funding rate (Coinalyze). Positive = longs pay shorts (crowded longs, squeeze-down risk); negative = shorts pay longs (crowded shorts, squeeze-up fuel). Near zero = balanced positioning."):''}
      ${(d.derivatives&&d.derivatives.long_short!=null)?cell("Long/short ratio", (()=>{const r=d.derivatives.long_short; const lbl=r>1.05?'more longs':r<0.95?'more shorts':'balanced'; return `${r.toFixed(2)} <span class="rr">${lbl}</span>`; })(), "Ratio of long to short accounts (Coinalyze). Above 1 = crowd leans long, below 1 = leans short. Extreme readings often precede a squeeze the other way."):''}
      ${(d.derivatives&&d.derivatives.liq_side)?cell("Liquidations (24h)", (()=>{const s=d.derivatives.liq_side; const lbl=s==='long'?'🩸 longs flushed':s==='short'?'🩸 shorts squeezed':'balanced'; const cls=s==='long'?'pf-bad':s==='short'?'pf-good':''; return `<span class="${cls}">${lbl}</span>`; })(), "Which side got liquidated over the last 24h (Coinalyze). Heavy LONG liquidations = forced selling that often flushes into a local bottom (bounce). Heavy SHORT liquidations = forced buying that often marks a local top (fade). A flow read, not a resting-liquidity heatmap."):''}
      ${(d.liq_zones)?cell("Liquidation magnets (est.)", (()=>{const z=d.liq_zones; const b=(z.below_long_liqs||[]).slice(0,2).map(x=>fmtNum(x.level)+' <span class=\"rr\">'+x.pct+'% ('+x.lev+'×)</span>').join(' · '); const a=(z.above_short_liqs||[]).slice(0,2).map(x=>fmtNum(x.level)+' <span class=\"rr\">+'+x.pct+'% ('+x.lev+'×)</span>').join(' · '); return `<span class="rr">below</span> ${b}<br><span class="rr">above</span> ${a}`; })(), "ESTIMATED leverage-liquidation clusters (10×–100×) — where over-leveraged longs (below price) and shorts (above price) get liquidated. Price often gets 'magneted' toward these to trigger stops. An estimate from price × leverage, NOT an exact order-book heatmap (that needs a paid source)."):''}
      ${cell("BTC correlation", d.btc_corr==null?'—':('ρ '+(+d.btc_corr).toFixed(2)+(d.btc_corr>=0.85?' · just follows BTC':d.btc_corr<0.5?' · independent':' · partly linked')), "How closely this coin's 4h returns tracked BTC over the last ~10 days (Pearson ρ, −1 to +1). ρ≥0.85 means the move is largely just BTC beta — a 'breakout' here may only be BTC pulling it up. Low or negative ρ means the coin is trading on its own story, which is usually what you want for an independent setup.")}
      ${cell("Supertrend ("+(d.interval||'4h')+")", d.supertrend==null?'—':(fmtNum(d.supertrend)+' · '+(d.supertrend_role==='support'?'SUPPORT':'RESISTANCE')+pct(d.supertrend, d.supertrend_role==='support'?'-':'+')), `Supertrend (ATR 10×3) on the ${d.interval||'4h'} chart. When price is ABOVE the line the trend is up and the line acts as a trailing SUPPORT; when price is BELOW it the trend is down and it acts as RESISTANCE. Here it's ${d.supertrend_role||'—'} at ${d.supertrend==null?'—':fmtNum(d.supertrend)} — a level to watch for the trend flipping.`)}
      ${cell("Supports (distance)", (d.supports||[]).slice(0,3).map(v=>fmtNum(v)+pct(v,'-')).join(' · ')||'—', "Based on: swing-low pivots — prior candle lows the market previously bounced from — on this timeframe, nearest first, with the % below current price.")}
      ${cell("Resistances (distance)", (d.resistances||[]).slice(0,3).map(v=>fmtNum(v)+pct(v,'+')).join(' · ')||'—', "Based on: swing-high pivots — prior candle peaks that previously capped price — on this timeframe, nearest first, with the % above current price.")}
      ${cell("Next support 4h·1D·1W (drawdown)", `${tfSup('4h',d.sup_4h,'-')} · ${tfSup('1d',d.sup_1d,'-')} · ${tfSup('1w',d.sup_1w,'-')}`, "Based on: the nearest swing-low pivot on the 4h, Daily and Weekly charts — your multi-timeframe safety-net levels — with the % drawdown to each. The ▲/▼/– before each is that timeframe's trend bias (green = bullish, red = bearish).")}
      ${cell("Next resistance 4h·1D·1W (upside)", `${tfSup('4h',d.res_4h,'+')} · ${tfSup('1d',d.res_1d,'+')} · ${tfSup('1w',d.res_1w,'+')}`, "Based on: the nearest swing-high pivot on the 4h, Daily and Weekly charts — likely ceilings — with the % upside to each. The ▲/▼/– before each is that timeframe's trend bias.")}
      ${cell("Dist. from 200 EMA (4h·1D·1W)", `4h ${tfMark('4h')} ${d.pct_vs_ema>=0?'+':''}${d.pct_vs_ema}% · 1D ${tfMark('1d')} ${d.dist_ema_1d==null?'—':(d.dist_ema_1d>=0?'+':'')+d.dist_ema_1d+'%'} · 1W ${tfMark('1w')} ${d.dist_ema_1w==null?'—':(d.dist_ema_1w>=0?'+':'')+d.dist_ema_1w+'%'}`, "How far price sits above/below the 200 EMA on each timeframe, with each timeframe's trend bias (▲ bull / ▼ bear / – neutral). Above the EMA on all three = a strong multi-timeframe uptrend. '—' = not enough history for that EMA.")}
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
    ${rec.tp!=null?`<div class="azrec" data-tip="Recommended by EXPECTED VALUE, not raw ratio: reward:risk × how reachable the target is. Reachability decays with distance (in ATR units) but stretches out when the trend, momentum and volume back the move — so a far target isn't dismissed if the setup is strong, and a nearby one isn't over-rated if it's weak. Measured from the recommended entry (${fmtNum(recE)}) over the recommended stop (${rstop?fmtNum(rstop.level):'—'}), capped 8:1, and only shown because it clears the 1.5:1 floor. Grade blends R:R and reachability.">⭐ Recommended take-profit: <b>${fmtNum(rec.tp)}</b> <span class="rr">${(d.side||'long')==='short'?'−':'+'}${(rec.move*100).toFixed(1)}% · R:R <b>${rec.rr.toFixed(2)}</b> · ~${Math.round(rec.p*100)}% reach · grade <b>${planGrade}</b></span>${rec.kind?` <span style="color:var(--dim)">— ${esc(rec.kind)}${tgtTf&&/Daily|Weekly/.test(rec.kind||'')?` <span class="tfsrc">${tgtTf} chart</span>`:''}</span>`:''}</div>
    ${rec.tp!=null?dcaPlanHtml(d, recE, rstop?rstop.level:null, rec.tp, d.side||'long', be?be.distATR:null):''}
    ${scaleOutHtml(rtg, recE)}
    ${(rec.tp!=null&&azRec)?`<div class="trackrow"><button class="trackbtn" onclick="trackSetup(event)" data-tip="Record this exact setup (entry, stop, full TP ladder) into 📌 My calls. Apex then watches the live price and grades it: banks each TP, moves the stop to break-even after TP1, and scores the result in R. Win-rate + per-TP hit rates are on the My calls tab.">📌 Track this setup</button><span id="trackmsg" class="trackmsg"></span></div>`:''}
    <div class="sidenote" data-tip="How the same trade looks if you enter NOW at the current market price instead of waiting for the 🎯 recommended pullback — same stop and target, worse fill, so a lower R:R. Use it to decide: take it now, or wait for the better entry.">⚡ Enter now at market (CMP ${fmtNum(cmpE)}): ${rrAt(cmpE)!=null?`R:R <b>${rrAt(cmpE).toFixed(2)}</b> to the base target`:'stop is already in the way — no clean entry here'} <span style="color:var(--dim)">vs ${rec.rr.toFixed(2)} waiting for ${fmtNum(recE)}${(rrAt(cmpE)!=null&&rrAt(cmpE)<1.5)?' — under 1.5:1 now, better to wait for the pullback':(cmpE!=null&&recE!=null&&Math.abs(cmpE-recE)/recE<0.005?' — basically at the entry already':'')}</span></div>`
    :`<div class="azrec" style="color:#f0b429" data-tip="No target on the correct side clears a 1.5:1 reward:risk from a sensible stop. In crypto a sub-1.5 R:R trade isn't worth the risk — this is a 'no trade / wait' call, not a setup. Wait for a deeper entry (better R:R), a tighter valid stop level, or a different coin.">⛔ No trade here — best realistic R:R is only <b>${rtg?rtg.bestRR.toFixed(2):'—'}</b>, under the 1.5 minimum. Wait for a better entry or setup.</div>`}
    ${stopsSection(d.stop_levels, d.side||'long', rstop?rstop.level:null)}
    <div class="azsec" data-tip="A fuller ladder of profit targets in order: overhead resistances, Fibonacci extensions, and — for a coin basing far below its prior highs — breakout 'runner' targets toward those highs. The scale-out plan is marked right on the ladder: 🔒 Secure (bank part first) · ⭐ Base (recommended target) · 🚀 Stretch/Runner (leave a small bag). Each shows % move and R = from the recommended entry, Rc = enter now (CMP), Rt = to the tight stop.">Target ladder <span class="azsub">🔒 secure · ⭐ base (recommended) · 🚀 stretch/runner · R = rec entry · Rc = CMP · Rt = tight stop</span></div>
    <div class="azladder">
      ${(d.target_ladder||[]).map((t,i)=>{
        const rR=recRR(t.level),rC=cmpRR(t.level),rT=tightRR(t.level);
        const base=isRec(t.level), sec=isSecure(t.level), str=isStretch(t.level), run=!!t.runner;
        const role= run?'🚀': base?'⭐': sec?'🔒': str?'🚀':'';
        const roleTxt= run?' 🚀 Breakout runner — if it breaks the base it can run here; hold a small bag into strength.'
                     : base?' ⭐ Base — the recommended target (best realistic reward:risk).'
                     : sec?' 🔒 Secure — bank part of the position here first to de-risk.'
                     : str?' 🚀 Stretch — leave a runner for this if momentum carries.':'';
        const cls='ladchip'+(base?' ladrec':'')+(run?' ladrunner':'')+(sec&&!base?' ladsecure':'')+(str&&!run&&!base?' ladstretch':'');
        return `<span class="${cls}" data-tip="Target ${i+1}: ${t.kind} at ${fmtNum(t.level)} — ${t.pct>=0?'+':''}${t.pct}% move. R:R ${rR!=null?rR.toFixed(2):'—'} from the recommended entry, ${rC!=null?rC.toFixed(2):'—'} if you enter now at market, ${rT!=null?rT.toFixed(2):'—'} to the tight stop.${roleTxt}">${role?role+' ':''}T${i+1} ${fmtNum(t.level)} <span class="rr">${t.pct>=0?'+':''}${t.pct}%${rR!=null?` · R${rR.toFixed(1)}`:''}${rC!=null?` · Rc${rC.toFixed(1)}`:''}${rT!=null?` · Rt${rT.toFixed(1)}`:''}</span></span>`;
      }).join('') || '<span style="color:var(--dim)">No further targets that side.</span>'}
    </div>
  </div>`;
}
// The "In plain English" bullet summary — rendered in the right-hand rail.
function plainEnglishHtml(d){
  const notes=((d&&d.notes)||[]);
  if(!notes.length) return '';
  // Organise the read into labelled sections instead of one flat list.
  const sec={trend:[],vol:[],levels:[],oi:[],bottom:[]};
  for(const n of notes){ const t=(''+n).toLowerCase();
    if(t.includes('open interest')) sec.oi.push(n);
    else if(t.includes('active setup')||t.includes('directional lean')) sec.bottom.push(n);
    else if(t.includes('volume')||t.includes('range')||t.includes('atr')) sec.vol.push(n);
    else if(t.includes('support')||t.includes('resistance')||t.includes('supertrend')||t.includes('drawdown')) sec.levels.push(n);
    else sec.trend.push(n); }
  const block=(title,arr)=> arr.length? `<div class="azsubh">${title}</div><ul class="aznotes">${arr.map(n=>`<li>${n}</li>`).join('')}</ul>`:'';
  return `<div class="azpe"><div class="azsec">Summary <span class="azsub">— the read behind this coin, at a glance</span></div>`
    + block('Trend &amp; momentum', sec.trend)
    + block('Volume &amp; volatility', sec.vol)
    + block('Key levels', sec.levels)
    + block('Open interest', sec.oi)
    + block('Bottom line', sec.bottom)
    + `</div>`;
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
      `<td class="sym"><div class="symbox">${watchStar(h.symbol)}<a href="${tvLink(h.symbol)}" target="_blank" rel="noopener">${dispSym(h.symbol)}</a>${analyzeBtn(h.symbol)}${badges(h)}</div></td>`+
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
      `<td class="sym"><div class="symbox">${watchStar(h.symbol)}<a href="${tvLink(h.symbol)}" target="_blank" rel="noopener">${dispSym(h.symbol)}</a>${analyzeBtn(h.symbol)}${badges(h)}</div></td>`+
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
      `<td class="sym"><div class="symbox">${watchStar(h.symbol)}<a href="${tvLink(h.symbol)}" target="_blank" rel="noopener">${dispSym(h.symbol)}</a>${analyzeBtn(h.symbol)}${badges(h)}</div></td>`+
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
      `<td class="sym"><div class="symbox">${watchStar(h.symbol)}<a href="${tvLink(h.symbol)}" target="_blank" rel="noopener">${dispSym(h.symbol)}</a>${analyzeBtn(h.symbol)}${badges(h)}</div></td>`+
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
let dtblatest=[], accumlatest=[];
function renderDiag(which){
  const el=document.getElementById(which+"diag"); if(!el) return;
  const d=(rdiaglatest||{})[which];
  if(!d||!d.length){ el.innerHTML=""; return; }
  const tot=d.reduce((a,b)=>a+b[1],0);
  const passed=(d.find(x=>x[0]==="PASSED")||[null,0])[1];
  let h='<div class="status"><span><b>Why the board looks like this</b> &mdash; of '+tot+' coins scanned, <b>'+passed+'</b> passed every gate. The rest fell out here:</span></div><table><thead><tr><th>First gate that failed</th><th>Coins</th><th>Share</th></tr></thead><tbody>';
  for(const [k,v] of d){ if(k==="PASSED") continue;
    h+='<tr><td>'+k+'</td><td>'+v+'</td><td>'+(v/tot*100).toFixed(1)+'%</td></tr>'; }
  h+='</tbody></table>';
  el.innerHTML=h;
}
function renderDtb(){
  const tb=document.getElementById("dtbrows"); if(!tb) return; tb.innerHTML="";
  const rows=[...dtblatest].sort((a,b)=>(b.rr||0)-(a.rr||0));
  const emp=document.getElementById("dtbempty"); if(emp) emp.style.display=rows.length?"none":"block";
  for(const h of rows){
    const tr=document.createElement("tr");
    tr.innerHTML=`<td class="sym"><div class="symbox">${watchStar(h.symbol)}<a href="${tvLink(h.symbol)}" target="_blank" rel="noopener">${dispSym(h.symbol)}</a>${analyzeBtn(h.symbol)}</div></td>`+
      `<td>${fmtNum(h.live!=null?h.live:h.price)}</td>`+
      `<td>${fmtNum((h.lows||[])[0])}</td><td>${fmtNum((h.lows||[])[1])}</td>`+
      `<td>${fmtNum((h.lows||[])[2])}</td>`+
      `<td>${fmtNum(h.entry)}</td><td>${fmtNum(h.stop)}</td><td>${fmtNum(h.target)}</td>`+
      `<td class="${(h.rr||0)>=2?'pf-good':''}">${h.rr==null?'—':(+h.rr).toFixed(2)}</td>`+
      `<td>${h.higher_low?'<span class="pf-good">yes</span>':'no'}</td>`+
      `<td>${h.quiet?'<span class="pf-good">quiet</span>':'—'}</td>`;
    tb.appendChild(tr);
  }
}
let siglatest=null;
let capitlatest=[];
let microlatest=null;
let rdiaglatest=null;
function renderCapit(){
  const tb=document.getElementById("capitrows"); if(!tb) return; tb.innerHTML="";
  const rows=[...capitlatest];
  const emp=document.getElementById("capitempty"); if(emp) emp.style.display=rows.length?"none":"block";
  for(const h of rows){
    const tr=document.createElement("tr");
    const yn=v=>v?'<span class="good">yes</span>':'<span class="muted">no</span>';
    tr.innerHTML=`<td class="sym"><div class="symbox">${watchStar(h.symbol)}<a href="${tvLink(h.symbol)}" target="_blank" rel="noopener">${dispSym(h.symbol)}</a>${analyzeBtn(h.symbol)}</div></td>`+
      `<td>${fmtNum(h.live!=null?h.live:h.price)}</td><td>${h.mcap?("$"+(h.mcap/1e6).toFixed(1)+"m"):"&mdash;"}</td>`+
      `<td>${fmtNum(h.flush_low)}</td><td>${h.bars_since_low}d</td><td>${fmtNum(h.trendline)}</td>`+
      `<td>${fmtNum(h.supply_zone)}</td><td>${fmtNum(h.entry)}</td><td>${fmtNum(h.stop)}</td>`+
      `<td>${fmtNum(h.target)}</td><td><b>${(h.rr||0).toFixed(2)}</b></td>`+
      `<td>${yn(h.rsi_div)}</td><td>${yn(h.vol_surge_flag)}</td><td><b>${h.confidence||1}</b>/4</td>`;
    tb.appendChild(tr);
  }
}
function renderMicro(){
  const tb=document.getElementById("microrows"), sm=document.getElementById("microsum");
  if(!tb) return; tb.innerHTML="";
  const m=microlatest;
  const emp=document.getElementById("microempty");
  if(!m||!m.n){ if(emp) emp.style.display="block"; if(sm) sm.innerHTML=""; return; }
  if(emp) emp.style.display="none";
  const pct=(a,b)=>b?((a/b*100).toFixed(1)+"%"):"0%";
  if(sm) sm.innerHTML='<div class="status"><span>'+
    '<b>'+m.n+'</b> of '+m.universe+' scanned coins are under $10m (CoinGecko matched '+m.mapped+' tickers; '+m.ambiguous+' were ambiguous and resolved to the larger cap).<br>'+
    'Median cap in this cohort: <b>$'+((m.median_mcap||0)/1e6).toFixed(2)+'m</b>. Pattern hit-rate within microcaps: '+
    '<b>'+m.dtb+'</b> triple bottoms ('+pct(m.dtb,m.n)+'), <b>'+m.accum+'</b> quiet accumulation ('+pct(m.accum,m.n)+'), '+
    '<b>'+m.capit+'</b> capitulation reversals ('+pct(m.capit,m.n)+') &mdash; <b>'+m.any+'</b> showing at least one setup ('+pct(m.any,m.n)+').<br>'+
    '<span class="warn">Compare each rate against the same pattern across the whole universe before concluding microcaps are special. A higher hit-rate here may just mean these charts are noisier, which makes patterns easier to find and less meaningful.</span>'+
    '</span></div>';
  for(const c of (m.coins||[])){
    const tr=document.createElement("tr");
    const yn=v=>v?'<span class="good">yes</span>':'<span class="muted">&mdash;</span>';
    tr.innerHTML=`<td class="sym"><div class="symbox">${watchStar(c.symbol)}<a href="${tvLink(c.symbol)}" target="_blank" rel="noopener">${dispSym(c.symbol)}</a>${analyzeBtn(c.symbol)}</div></td>`+
      `<td>$${(c.mcap/1e6).toFixed(2)}m</td><td>${yn(c.dtb)}</td><td>${yn(c.accum)}</td><td>${yn(c.capit)}</td>`;
    tb.appendChild(tr);
  }
}
function wfRow(x){
  const g=v=>v==null?"&mdash;":((v>0?"+":"")+v.toFixed(1)+"%");
  const cl=v=>v==null?"":(v>0?"good":"bad");
  return `<td>${x.name}</td><td>${x.oos_n||0}</td><td>${x.oos_winrate==null?"&mdash;":x.oos_winrate+"%"}</td>`+
    `<td>${x.oos_exp==null?"&mdash;":x.oos_exp.toFixed(3)}</td>`+
    `<td>${x.oos_avg_rr==null?"&mdash;":x.oos_avg_rr.toFixed(2)}</td>`+
    `<td>${x.oos_total_r==null?"&mdash;":x.oos_total_r.toFixed(1)+"R"}</td>`+
    `<td class="bad">${x.oos_max_dd_r==null?"&mdash;":"-"+x.oos_max_dd_r.toFixed(1)+"R"}</td>`+
    `<td class="${cl(x.oos_ret_pct)}">${g(x.oos_ret_pct)}</td>`+
    `<td class="bad">${x.oos_max_dd_pct==null?"&mdash;":"-"+x.oos_max_dd_pct.toFixed(0)+"%"}</td>`+
    `<td>${x.dca_total_r==null?"&mdash;":x.dca_total_r.toFixed(1)+"R"}</td>`+
    `<td class="bad">${x.dca_max_dd_r==null?"&mdash;":"-"+x.dca_max_dd_r.toFixed(1)+"R"}</td>`+
    `<td class="${cl(x.dca_ret_pct)}">${g(x.dca_ret_pct)}</td>`+
    `<td class="bad">${x.dca_max_dd_pct==null?"&mdash;":"-"+x.dca_max_dd_pct.toFixed(0)+"%"}</td>`;
}
function renderWF(){
  const el=document.getElementById("wfbox"); if(!el) return;
  const r=siglatest; if(!r){ el.innerHTML=""; return; }
  // NEW full-universe format: survivors of BOTH holdouts
  if(r.survivors!==undefined){
    const bd=r.backdrop||null;
    let h='';
    if(bd){
      const bear=bd.risk_off_pct>bd.risk_on_pct;
      h+='<div class="status"><span>🌍 <b>What market did we test in?</b> Measured from the bars, not assumed. '+
        'Breadth was <b>risk-off '+bd.risk_off_pct+'%</b> vs <b>risk-on '+bd.risk_on_pct+'%</b>; index falling '+bd.idx_down_pct+'% of the time.<br>'+
        (bear?'<b class="good">Net bearish for alts</b> — a long-only edge surviving this is a stronger claim than one earned in a rising tape.'
             :'<b class="warn">Net risk-on</b> — some of any long-only edge here is simply market direction.')+'</span></div>';
    }
    h+='<div class="status"><span>🧪 <b>Full universe, two independent holdouts</b><br>'+
      'Tested <b>'+(r.n||0).toLocaleString()+'</b> trades across <b>'+(r.coins||0)+'</b> coins. Strategies were ranked using only <b>'+(r.rank_coins||0)+'</b> of them; the other <b>'+(r.hold_coins||0)+'</b> coins were held back entirely and took no part in choosing anything.<br>'+
      '<b>Time holdout</b>: profitable in the first AND second half of history. <b>Coin holdout</b>: still profitable on coins it never saw. A combo must pass <b>both</b> to appear below.<br>'+
      '<b>'+(r.tested||0)+'</b> combinations tested, of which roughly <b>'+(r.expected_false||0)+'</b> would look good by chance alone. '+
      '<b>'+(r.robust_count||0)+'</b> passed the time split; <b class="good">'+(r.survivor_count||0)+'</b> passed both.'+
      ((r.survivor_count||0)===0?' <span class="bad">Nothing cleared both bars this run — that is a real result, not a bug.</span>':'')+
      '</span></div>';
    if((r.survivors||[]).length){
      h+='<h3 style="margin:12px 0 4px">Strategies that survived both holdouts</h3>'+
         '<table><thead><tr><th>Strategy</th><th>Trades</th><th>Win rate</th><th>Exp (R)</th><th>Avg R:R</th>'+
         '<th>1st half</th><th>2nd half</th><th>Decay</th>'+
         '<th>Unseen coins: n</th><th>Exp</th><th>Win rate</th><th>Return</th><th>Max DD</th></tr></thead><tbody>';
      for(const e of r.survivors){
        h+='<tr><td>'+e.name+'</td><td>'+e.n+'</td><td>'+e.winrate+'%</td><td>'+e.exp.toFixed(3)+'</td><td>'+(e.avg_rr||0).toFixed(2)+'</td>'+
           '<td class="good">'+(e.h1==null?"—":e.h1.toFixed(3))+'</td><td class="good">'+(e.h2==null?"—":e.h2.toFixed(3))+'</td>'+
           '<td class="'+((e.decay||0)<0?"bad":"good")+'">'+(e.decay==null?"—":((e.decay>0?"+":"")+e.decay.toFixed(3)))+'</td>'+
           '<td>'+(e.hold_n||0)+'</td><td class="'+(((e.hold_exp||0)>0)?"good":"bad")+'"><b>'+(e.hold_exp==null?"—":e.hold_exp.toFixed(3))+'</b></td>'+
           '<td>'+(e.hold_winrate==null?"—":e.hold_winrate+"%")+'</td>'+
           '<td class="'+(((e.hold_ret_pct||0)>0)?"good":"bad")+'">'+(e.hold_ret_pct==null?"—":((e.hold_ret_pct>0?"+":"")+e.hold_ret_pct.toFixed(1)+"%"))+'</td>'+
           '<td class="bad">'+(e.hold_max_dd_pct==null?"—":"-"+e.hold_max_dd_pct.toFixed(0)+"%")+'</td></tr>';
      }
      h+='</tbody></table>';
    }
    const c=r.combined_holdout;
    if(c){
      h+='<h3 style="margin:14px 0 4px">Trading ALL survivors — measured only on the unseen coins</h3>'+
         '<div class="status"><span>Every surviving strategy combined into one portfolio, scored purely on the '+(r.hold_coins||0)+' coins that played no part in selecting them. This is the closest thing here to an honest forward estimate.</span></div>'+
         '<table><thead><tr><th>Strategies</th><th>Trades</th><th>Win rate</th><th>Exp (R)</th><th>Avg R:R</th><th>Total R</th><th>Max DD (R)</th><th>End equity</th><th>Return</th><th>Max DD</th><th>Exp w/ DCA</th><th>Return w/ DCA</th></tr></thead><tbody><tr style="background:rgba(80,200,120,.07)">'+
         '<td><b>'+(c.n_strategies||0)+' combined</b></td><td>'+c.n+'</td><td>'+c.winrate+'%</td>'+
         '<td class="'+((c.exp>0)?"good":"bad")+'"><b>'+c.exp.toFixed(3)+'</b></td><td>'+(c.avg_rr||0).toFixed(2)+'</td>'+
         '<td>'+(c.total_r||0).toFixed(1)+'R</td><td class="bad">-'+(c.max_dd_r||0).toFixed(1)+'R</td>'+
         '<td><b>$'+(c.end_equity||0).toLocaleString()+'</b></td>'+
         '<td class="'+((c.ret_pct>0)?"good":"bad")+'">'+(c.ret_pct>0?"+":"")+(c.ret_pct||0).toFixed(1)+'%</td>'+
         '<td class="bad">-'+(c.max_dd_pct||0).toFixed(0)+'%</td>'+
         '<td>'+(c.exp_dca==null?"—":c.exp_dca.toFixed(3))+'</td>'+
         '<td>'+(c.dca_ret_pct==null?"—":((c.dca_ret_pct>0?"+":"")+c.dca_ret_pct.toFixed(1)+"%"))+'</td></tr></tbody></table>';
      if(c.by_regime&&c.by_regime.length){
        h+='<h3 style="margin:12px 0 4px">…and split by market regime</h3>'+
           '<div class="status"><span>If the whole edge sits in the risk-on row, it is a bet on market direction wearing a strategy costume.</span></div>'+
           '<table><thead><tr><th>Regime</th><th>Trades</th><th>Win rate</th><th>Exp (R)</th><th>Total R</th><th>Max DD (R)</th><th>Return</th></tr></thead><tbody>';
        for(const x of c.by_regime)
          h+='<tr><td><b>'+x.regime+'</b></td><td>'+x.n+'</td><td>'+x.winrate+'%</td><td class="'+((x.exp>0)?"good":"bad")+'">'+x.exp.toFixed(3)+'</td><td>'+x.total_r.toFixed(1)+'R</td><td class="bad">-'+x.max_dd_r.toFixed(1)+'R</td><td class="'+((x.ret_pct>0)?"good":"bad")+'">'+(x.ret_pct>0?"+":"")+x.ret_pct.toFixed(1)+'%</td></tr>';
        h+='</tbody></table>';
      }
    }
    el.innerHTML=h; return;
  }
  const w=(r&&r.walk_forward)||null;
  if(!w){ el.innerHTML=""; return; }
  const hdr='<tr><th>Strategy</th><th>Trades</th><th>Win rate</th><th>Exp (R)</th><th>Avg R:R</th>'+
    '<th>Total R</th><th>Max DD (R)</th><th>Return</th><th>Max DD</th>'+
    '<th>Total R <i>DCA</i></th><th>Max DD (R) <i>DCA</i></th><th>Return <i>DCA</i></th><th>Max DD <i>DCA</i></th></tr>';
  const bd=(r&&r.backdrop)||null;
  let h='';
  if(bd){
    const bear = bd.risk_off_pct > bd.risk_on_pct;
    h+='<div class="status"><span>🌍 <b>What market did we test in?</b> &mdash; measured from the same bars, not assumed.<br>'+
      'Breadth was <b>risk-off '+bd.risk_off_pct+'%</b> of the time vs <b>risk-on '+bd.risk_on_pct+'%</b>; '+
      'the index was falling '+bd.idx_down_pct+'% and rising '+bd.idx_up_pct+'% of the time, across <b>'+bd.n.toLocaleString()+'</b> trades.<br>'+
      (bear
        ? '<b class="good">The test period was net bearish for alts.</b> That raises the bar in a useful way: a long-only edge that survives a falling tape is a much stronger claim than one earned while everything went up.'
        : '<b class="warn">The test period was net risk-on.</b> A long-only edge earned mostly in a rising tape is partly just beta &mdash; check the regime split below before trusting it.')+
      '</span></div>';
  }
  h+='<div class="status"><span>🏆 <b>Top strategies, tested walk-forward</b><br>'+
    'Every combo was ranked using <b>only the first half</b> of history. The top 10 from that ranking were then measured across the <b>second half, which they never saw</b>. '+
    'That second-half number is the honest estimate &mdash; it is the backtest that shows whether an edge survives.<br>'+
    '<span class="warn">Why not just rank over all history and total it up? Because those combos were chosen <i>because</i> they scored well on that data, so the total is guaranteed to look good. That is the same reasoning as picking the best fund of last year and calling it a forecast. The in-sample figure is shown below only so you can see the gap &mdash; the gap is the size of the self-deception.</span></span></div>';
  h+='<table><thead>'+hdr+'</thead><tbody>';
  for(const x of (w.solo||[])) h+='<tr>'+wfRow(x)+'</tr>';
  const combo=(k,label)=>{ const b=w[k]; if(!b) return '';
    return '<tr style="background:rgba(80,200,120,.07)"><td><b>'+label+'</b></td>'+wfRow(b).replace(/^<td>[^<]*<\/td>/,'')+'</tr>'; };
  h+='</tbody></table>';
  h+='<h3 style="margin:14px 0 4px">Following ALL signals from the top 5 / top 10 combined</h3>';
  h+='<table><thead>'+hdr+'</thead><tbody>';
  for(const [k,l] of [["top5","Top 5 combined"],["top10","Top 10 combined"]]){
    const b=w[k]; if(!b) continue;
    const row=wfRow(Object.assign({},b,{name:l}));
    h+='<tr style="background:rgba(80,200,120,.07)">'+row+'</tr>';
  }
  h+='</tbody></table>';
  // regime split: does the edge survive when the market is NOT helping?
  for(const [k,l] of [["top5","Top 5"],["top10","Top 10"]]){
    const b=w[k]; const rg=b&&b.oos_by_regime; if(!rg||!rg.length) continue;
    h+='<h3 style="margin:14px 0 4px">'+l+' broken down by market regime</h3>'+
       '<div class="status"><span>The question that matters for a long-only system is not whether it made money, but whether it made money <b>when the market was not helping</b>. If the entire edge sits in the risk-on row, it is a bet on market direction wearing a strategy costume.</span></div>'+
       '<table><thead><tr><th>Regime</th><th>Trades</th><th>Win rate</th><th>Exp (R)</th><th>Total R</th><th>Max DD (R)</th><th>Return</th><th>Max DD</th></tr></thead><tbody>';
    for(const x of rg){
      const cl=(x.exp>0)?"good":"bad";
      h+='<tr><td><b>'+x.regime+'</b></td><td>'+x.n+'</td><td>'+x.winrate+'%</td>'+
         '<td class="'+cl+'">'+x.exp.toFixed(3)+'</td><td>'+x.total_r.toFixed(1)+'R</td>'+
         '<td class="bad">-'+x.max_dd_r.toFixed(1)+'R</td>'+
         '<td class="'+((x.ret_pct>0)?"good":"bad")+'">'+(x.ret_pct>0?"+":"")+x.ret_pct.toFixed(1)+'%</td>'+
         '<td class="bad">-'+x.max_dd_pct.toFixed(0)+'%</td></tr>';
    }
    h+='</tbody></table>';
  }
  const ra=w.robust_all;
  if(ra){
    h+='<h3 style="margin:14px 0 4px">Every strategy that was profitable in BOTH halves ('+ra.n_strategies+')</h3>'+
       '<div class="status"><span>A stricter screen than "top N": a combo only qualifies if it made money in the <b>first half and the second half independently</b>, so nothing can fluke in by being huge in one era.<br>'+
       '<span class="warn">Be clear on what this is not: because both halves were used to choose, there is no untouched data left to verify it against. It is a consistency screen, not an out-of-sample proof. Read it as "these behaved steadily", not "these will earn this".</span></span></div>'+
       '<table><thead>'+hdr+'</thead><tbody><tr style="background:rgba(80,200,120,.07)">'+
       wfRow(Object.assign({},ra,{name:"All both-half-positive strategies"}))+'</tr></tbody></table>'+
       '<div class="status"><span>Split check: first half <b>'+(ra.h1_exp==null?"—":ra.h1_exp.toFixed(3))+'R</b>/trade vs second half <b>'+(ra.h2_exp==null?"—":ra.h2_exp.toFixed(3))+'R</b>/trade.'+
       (ra.oos_busted?' <span class="bad">ACCOUNT BUSTED</span>':'')+'</span></div>';
    if(ra.oos_by_regime&&ra.oos_by_regime.length){
      h+='<table><thead><tr><th>Regime</th><th>Trades</th><th>Win rate</th><th>Exp (R)</th><th>Total R</th><th>Max DD (R)</th><th>Return</th></tr></thead><tbody>';
      for(const x of ra.oos_by_regime)
        h+='<tr><td><b>'+x.regime+'</b></td><td>'+x.n+'</td><td>'+x.winrate+'%</td><td class="'+((x.exp>0)?"good":"bad")+'">'+x.exp.toFixed(3)+'</td><td>'+x.total_r.toFixed(1)+'R</td><td class="bad">-'+x.max_dd_r.toFixed(1)+'R</td><td class="'+((x.ret_pct>0)?"good":"bad")+'">'+(x.ret_pct>0?"+":"")+x.ret_pct.toFixed(1)+'%</td></tr>';
      h+='</tbody></table>';
    }
  }
  const t5=w.top5, t10=w.top10;
  if(t5||t10){
    h+='<div class="status" style="margin-top:8px"><span>';
    for(const [b,l] of [[t5,"Top 5"],[t10,"Top 10"]]){ if(!b) continue;
      h+='<b>'+l+'</b>: in-sample '+(b.in_exp==null?"&mdash;":b.in_exp.toFixed(3))+'R/trade ('+(b.in_ret_pct==null?"&mdash;":b.in_ret_pct.toFixed(0)+'%')+
         ') &rarr; out-of-sample <b>'+(b.oos_exp==null?"&mdash;":b.oos_exp.toFixed(3))+'R</b>/trade ('+(b.oos_ret_pct==null?"&mdash;":b.oos_ret_pct.toFixed(0)+'%')+')'+
         (b.oos_busted?' &mdash; <span class="bad">ACCOUNT BUSTED</span>':'')+'<br>';
    }
    h+='<span class="warn">If out-of-sample is far below in-sample, the ranking was fitting noise. If it holds up, that is genuine evidence &mdash; but it is still one market era, and these are long-only signals, so the regime breakdown above is the part to read hardest &mdash; it shows whether the edge exists when the tape is against you or only when it is helping.</span></span></div>';
  }
  el.innerHTML=h;
}
function sigRow(x){
  const lift=(x.lift||0), col=lift>0?"good":(lift<0?"bad":"");
  const rb=x.robust?'<span class="good">yes</span>'
        :(x.beats_base_both?'<span class="warn">beat base only</span>':'<span class="bad">no</span>');
  const dc=(x.decay==null)?null:x.decay;
  const pc=(x.ret_pct||0), pcol=pc>0?"good":(pc<0?"bad":"");
  const wt=(x.worst_trade||0);
  const dx=(x.exp_dca==null?null:x.exp_dca);
  const dh=x.dca_helps?'<span class="good">yes</span>':'<span class="muted">no</span>';
  return `<td>${x.name}</td><td>${x.n}</td><td>${x.winrate}%</td><td>${(x.exp||0).toFixed(3)}</td>`+
         `<td>${dx==null?"&mdash;":dx.toFixed(3)}</td><td>${dx==null?"&mdash;":dh}</td>`+
         `<td class="${col}">${lift>0?"+":""}${lift.toFixed(3)}</td>`+
         `<td>${(x.total_r==null?0:x.total_r).toFixed(1)}R</td>`+
         `<td class="bad">-${(x.max_dd_r||0).toFixed(1)}R</td>`+
         `<td><b>$${(x.end_equity||0).toLocaleString()}</b></td>`+
         `<td class="${pcol}">${pc>0?"+":""}${pc.toFixed(1)}%</td>`+
         `<td class="bad">-$${(x.max_dd_usd||0).toLocaleString()} (${(x.max_dd_pct||0).toFixed(0)}%)</td>`+
         `<td class="bad">$${wt.toLocaleString()}</td>`+
         `<td class="${(x.h1||0)>0?"good":"bad"}">${x.h1==null?"&mdash;":x.h1.toFixed(3)}</td>`+
         `<td class="${(x.h2||0)>0?"good":"bad"}">${x.h2==null?"&mdash;":x.h2.toFixed(3)}</td>`+
         `<td class="${dc==null?"":(dc<0?"bad":"good")}">${dc==null?"&mdash;":(dc>0?"+":"")+dc.toFixed(3)}</td>`+
         `<td>${rb}</td>`;
}
function renderSignals(){
  const b=document.getElementById("sigbase"), tb=document.getElementById("sigrows"), pb=document.getElementById("sigprows");
  if(!tb||!pb) return;
  tb.innerHTML=""; pb.innerHTML="";
  const r=siglatest;
  const emp=document.getElementById("sigempty");
  if(!r){ if(emp) emp.style.display="block"; if(b) b.innerHTML=""; return; }
  if(emp) emp.style.display="none";
  if(b) b.innerHTML=`<div class="status"><span>Base strategy: <b>${r.n}</b> trades, expectancy <b>${(r.base_exp||0).toFixed(3)}R</b>. Every signal below is measured against that.<br>`+
    `<b>${r.tested||0}</b> combinations were tested. If every signal were pure noise, roughly <b>${r.expected_false||0}</b> would still look significant by chance &mdash; so treat any single top row with suspicion and weight the <b>Holds up?</b> column instead. <b>${r.robust_count||0}</b> were <b>profitable in both halves</b>.<br>`+
    `<b>Read the two half-columns before anything else.</b> "Profitable both halves" now means exactly that &mdash; positive in each half independently. It previously meant "beat the baseline in both", which was misleading, because the baseline itself loses money: a combo whose second half was <i>negative</i> could still collect a green tick simply by losing less. Those now read <span class="warn">beat base only</span>.<br>`+
    `<b>Decay</b> is second half minus first half. Large negative decay is the signature of a fitted edge fading once it meets data it was not chosen on.<br>`+
    `<b>Money columns</b> use your sizing on <b>cross</b> margin: <b>$10,000</b> account, <b>$250 margin at 10x</b> = a $2,500 position, taken in date order. Because margin is cross the whole balance backs the trade &mdash; a $2,500 position is <b>not</b> force-closed by a 10% adverse move, so <b>the stop is actually reached</b> instead of the position being liquidated early.<br>`+
    `Dollar risk is position size &times; distance to stop: a 5% stop risks $125 (1.25% of the account), a 20% stop risks $500 (<b>5%</b>). Under cross the danger is not any single trade &mdash; it is a losing streak draining shared collateral, which is exactly what <b>Max DD</b> measures.<br>`+
    `<b>Exp. w/ DCA</b> adds a second equal-size unit halfway to the stop. It cuts both ways: a full stop-out becomes about <b>-1.5R</b> rather than -1R, while winners that dipped first pay considerably more. DCA does not create edge &mdash; it widens both tails. If a signal only becomes profitable with DCA switched on, treat that as a warning rather than a discovery.<br>`+
    `<span class="warn">All figures assume every signalled trade was taken with no cap on concurrent positions, so read the equity curve as an upper bound rather than a forecast.</span></span></div>`;
  const sc=document.getElementById("sigcount");
  if(sc) sc.textContent="— "+(r.n_signals||0)+" indicators tested, "+(r.singles||[]).length+" with enough trades to score";
  for(const x of (r.singles||[])){ const tr=document.createElement("tr"); tr.innerHTML=sigRow(x); tb.appendChild(tr); }
  for(const x of (r.pairs||[])){ const tr=document.createElement("tr"); tr.innerHTML=sigRow(x); pb.appendChild(tr); }
  renderWF();
  const trb=document.getElementById("sigtrows");
  if(trb){ trb.innerHTML=""; for(const x of (r.triples||[])){ const tr=document.createElement("tr"); tr.innerHTML=sigRow(x); trb.appendChild(tr); } }
}
function renderAccum(){
  const tb=document.getElementById("accumrows"); if(!tb) return; tb.innerHTML="";
  const rows=[...accumlatest].sort((a,b)=>(b.rr||0)-(a.rr||0));
  const emp=document.getElementById("accumempty"); if(emp) emp.style.display=rows.length?"none":"block";
  for(const h of rows){
    const tr=document.createElement("tr");
    tr.innerHTML=`<td class="sym"><div class="symbox">${watchStar(h.symbol)}<a href="${tvLink(h.symbol)}" target="_blank" rel="noopener">${dispSym(h.symbol)}</a>${analyzeBtn(h.symbol)}</div></td>`+
      `<td>${fmtNum(h.live!=null?h.live:h.price)}</td>`+
      `<td>${fmtNum((h.range||[])[0])}</td><td>${fmtNum((h.range||[])[1])}</td>`+
      `<td>${h.range_pct==null?'—':(+h.range_pct).toFixed(1)+'%'}</td>`+
      `<td class="${(h.vol_ratio||1)<=0.5?'pf-good':''}">${h.vol_ratio==null?'—':(+h.vol_ratio).toFixed(2)}×</td>`+
      `<td>${fmtNum(h.prior_pump_high)}</td>`+
      `<td>${fmtNum(h.entry)}</td><td>${fmtNum(h.stop)}</td><td>${fmtNum(h.target)}</td>`+
      `<td class="${(h.rr||0)>=2?'pf-good':''}">${h.rr==null?'—':(+h.rr).toFixed(2)}</td>`+
      `<td>${h.broke_out?'<span class="pf-good">broke out</span>':'coiling'}</td>`;
    tb.appendChild(tr);
  }
}
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
      `<td class="sym"><div class="symbox">${watchStar(h.symbol)}<a href="${tvLink(h.symbol)}" target="_blank" rel="noopener">${dispSym(h.symbol)}</a>${analyzeBtn(h.symbol)}${badges(h)}</div></td>`+
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
      `<td class="sym"><div class="symbox">${watchStar(h.symbol)}<a href="${tvLink(h.symbol)}" target="_blank" rel="noopener">${dispSym(h.symbol)}</a>${analyzeBtn(h.symbol)}${badges(h)}</div></td>`+
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
    if(move>=0.02){ const rr=Math.abs(tp-E)/risk;
      if(rr>best.rr) best={rr:rr,tp:tp,move:move,entry:E}; }
  };
  for(let i=1;i<=5;i++) consider(h['tp'+i]);
  if(best.rr===0){ for(let i=1;i<=5;i++){ const tp=h['tp'+i];
    if(tp!=null && (long?tp>E:tp<E)){ best={rr:Math.abs(tp-E)/risk,tp:tp,
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
      `<td class="sym"><div class="symbox">${watchStar(h.symbol)}<a href="${tvLink(h.symbol)}" target="_blank" rel="noopener">${dispSym(h.symbol)}</a>${analyzeBtn(h.symbol)}${badges(h)}</div></td>`+
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
      `<td class="sym"><div class="symbox">${watchStar(h.symbol)}<a href="${tvLink(h.symbol)}" target="_blank" rel="noopener">${dispSym(h.symbol)}</a>${analyzeBtn(h.symbol)}${badges(h)}</div></td>`+
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
// Universe-wide leaderboards (25 best longs / 25 best shorts across every pair).
// Symmetric renderer — `side` picks long vs short, colours and click behaviour.
function scoreBadge(s){
  const v=+s||0;
  const cls = v>=75?'sc-a' : v>=60?'sc-b' : v>=45?'sc-c' : 'sc-d';
  return `<span class="scorepill ${cls}">${v.toFixed(0)}</span>`;
}
// Which leaderboard/coil rows are expanded to show their full plan.
let rowOpen={};
function toggleRowPlan(key, ev){ if(ev){ ev.stopPropagation(); }
  rowOpen[key]=!rowOpen[key]; renderBestLong(); renderBestShort(); renderCoil(); renderScalp(); renderSpot(); }
// A clean, readable trade-plan panel: entry (with the timeframe + why), stop (with
// its basis + % risk), a target ladder where every TP shows its %move, R:R and WHICH
// level/timeframe it's from, and a concrete scale-out plan.
function planPanelHtml(p, side, cmp, sym){
  if(!p || p.entry==null) return '<div class="planpanel">No tradeable plan on this side right now.</div>';
  const long = side!=='short';
  const riskPct = Math.abs((p.entry-p.stop)/p.entry*100);
  const tfChip = t => t?`<span class="tfsrc">${t}</span>`:'';
  // Enter-now-at-CMP option: R:R to the base target over the stop from the current price.
  let cmpLine='';
  if(cmp!=null && p.stop!=null && p.tps && p.tps.length){
    const t=p.tps[0].lvl, s=p.stop;
    const ok = long?(cmp>s && t>cmp):(cmp<s && t<cmp);
    if(ok){
      const cmpRR=Math.abs(t-cmp)/Math.abs(cmp-s);
      const near=Math.abs(cmp-p.entry)/p.entry < 0.003;
      cmpLine=`<div class="pline pcmp"><span class="plab">Now</span><b>${fmtNum(cmp)}</b> <span class="pbasis">enter at market (CMP)</span> <span class="prr">R ${cmpRR.toFixed(1)}</span>${near?' <span class="pbasis">≈ already at the entry</span>':' <span class="pbasis">vs waiting for the pullback</span>'}</div>`;
    } else {
      cmpLine=`<div class="pline pcmp"><span class="plab">Now</span><span class="pbasis">at market (${fmtNum(cmp)}) the stop/target isn\\'t cleanly placed — better to wait for the entry.</span></div>`;
    }
  }
  const tps=(p.tps||[]).map((t,i)=>{
    const mv=Math.abs((t.lvl-p.entry)/p.entry*100);
    return `<div class="pline ptp"><span class="plab">TP${i+1}</span><b>${fmtNum(t.lvl)}</b>`
         + `<span class="pmv">${long?'+':'−'}${mv.toFixed(1)}%</span>`
         + `<span class="prr">R ${t.rr!=null?(+t.rr).toFixed(1):'—'}</span>`
         + `<span class="pbasis">${esc(t.basis||'')}</span></div>`;
  }).join('');
  const _pw=scaleWeights((p.tps||[]).map(t=>t.rr)).map(x=>Math.round(x*100)+'%');
  const scale = _pw.length>=2
    ? `Sell ${_pw[0]} at TP1 → move stop to break-even (${fmtNum(p.entry)})`
      + _pw.slice(1).map((pc,i)=>`, ${pc} at TP${i+2}`).join('')
      + ` (trail the stop up on the runner).`
    : `Sell 100% at TP1.`;
  const entryLine = p.entry_break!=null
    ? `<b>${fmtNum(p.entry)}</b> <span class="pbasis">limit / retest</span> &nbsp;·&nbsp; <b>${fmtNum(p.entry_break)}</b> <span class="pbasis">break-confirm</span>`
    : `<b>${fmtNum(p.entry)}</b>`;
  return `<div class="planpanel ${long?'pp-long':'pp-short'}">
     <div class="pphead">${long?'🟢 LONG plan':'🔴 SHORT plan'}</div>
     <div class="pline"><span class="plab">Entry</span>${entryLine} ${tfChip(p.entry_tf)}<span class="pbasis">${esc(p.entry_basis||'')}</span></div>
     ${cmpLine}
     <div class="pline"><span class="plab">Stop</span><b>${fmtNum(p.stop)}</b> <span class="pmv risk">${riskPct.toFixed(1)}% risk</span> ${tfChip(p.stop_tf)}<span class="pbasis">${esc(p.stop_basis||'')}</span></div>
     <div class="ptps"><div class="ptpsh">Targets — take profit in stages</div>${tps}</div>
     <div class="pscale">📤 Scale-out: ${scale}</div>
     ${(()=>{const rf=riskPct/100; if(!(rf>0)) return ''; const sizePct=Math.round(1/rf); const lev=Math.max(1,Math.ceil(sizePct/100)); const safeLev=Math.max(1,Math.min(25,Math.floor(0.5/rf))); return `<div class="psize" data-tip="Risking 1% of your account on this trade. Stop is ${riskPct.toFixed(1)}% from entry, so position notional ≈ ${sizePct}% of account (loss if stopped ≈ 1%). It needs ≥${lev}× leverage just to hold that notional; keeping leverage at ≤${safeLev}× (isolated) leaves the liquidation price well beyond the stop. Adjust for your own risk-per-trade %.">💰 Sizing (1% risk): size ≈ <b>${sizePct}%</b> of account · needs ≥${lev}× · keep ≤<b>${safeLev}×</b> lev</div>`;})()}
     ${sym?`<div class="trackrow" style="margin-top:8px"><button class="trackbtn" onclick="trackTrade('${sym}','${side}',${p.entry},${p.stop},'${(p.tps||[]).map(t=>t.lvl).join(',')}','${p.entry_tf||''}','pptrk_${esc(sym)}_${side}',event)" data-tip="Track this exact ${long?'long':'short'} plan in 📌 My calls — scale-out aware, graded in R.">📌 Track this</button><span class="trackmsg" id="pptrk_${esc(sym)}_${side}"></span></div>`:''}
   </div>`;
}
// Map a setup's timeframe label to a backtest sweep timeframe, and pull its measured edge.
function btEdgeFor(tf, side, spot){
  const e=lastData&&lastData.bt_edge; if(!e) return null;
  const map={'4h':'4h','1d':'1d','Daily':'1d','1D':'1d','15m':'15m','1h':'1h','5m':'15m','Weekly':null,'1W':null};
  const k=map[tf!=null?tf:'']; if(k===undefined||k===null) return null;
  if(spot){ return (e.spot&&e.spot[k])||null; }
  const cell=e.fut&&e.fut[k]; if(!cell) return null;
  return cell[side]||null;
}
// A small badge showing the backtest-measured edge for this proposal's timeframe/side.
// ✅ proven-positive · 🟡 thin · ❌ negative — 'only propose winners' made visible.
function edgeBadge(tf, side, spot){
  const c=btEdgeFor(tf, side, spot); if(!c||c.exp==null) return '';
  const x=+c.exp; const cls=x>=0.15?'rrg':x>0?'rry':'rrd';
  const lab=x>=0.15?'✅':x>0?'🟡':'❌';
  const tfk={'Daily':'1d','1D':'1d','5m':'15m'}[tf]||tf||'?';
  return ` <span class="rr ${cls}" data-tip="Backtest edge for ${tfk} ${spot?'spot ':''}${side}s over the whole universe: expectancy ${x>0?'+':''}${x}R across ${c.n} trades (win ${c.wr!=null?c.wr+'%':'—'}). ${x>=0.15?'The sweep proves this timeframe/side actually wins.':x>0?'Only a thin edge here — size down.':'The sweep says this timeframe/side does NOT win — treat with caution.'}">${lab}${x>0?'+':''}${x}R</span>`;
}
// Badge for a setup driven by the backtest-proven trend-aligned reversion mechanic.
function revBadge(h){
  if(!h||!h.revert) return '';
  return ` <span class="cbadge cbounce" data-tip="Trend-aligned reversion — this setup is an oversold snap-back in an uptrend / overbought roll-over in a downtrend, BTC-aligned. It's the exact mechanic the whole-universe backtest proved wins (best on 1h+), so it leads the board.">↩ reversion</span>`;
}
function labCell(h){
  const hits=h.lab_hits||[];
  if(!hits.length) return '<span class="muted">—</span>';
  const best=h.lab_best;
  const lbl = hits.length>1 ? (hits.length+' combos') : hits[0].split(' + ').length+'-signal';
  return `<span class="good"><b>#${best||'?'}</b> ${lbl}</span>`;
}
function renderBoard(side){
  const isLong = side==='long';
  const rows = (lastData && (isLong?lastData.long_board:lastData.short_board)) || [];
  const tb=document.getElementById(isLong?'blrows':'bsrows'); if(!tb) return;
  tb.innerHTML="";
  const emp=document.getElementById(isLong?'blempty':'bsempty');
  if(emp) emp.style.display = rows.length? "none":"block";
  const cnt=document.getElementById(isLong?'blCount':'bsCount');
  if(cnt) cnt.textContent = `${rows.length} of the whole universe (${(lastData&&lastData.universe)||'…'} pairs)`;
  let rank=0;
  for(const h of rows){
    rank++;
    const P = h.live!=null? h.live : h.price;
    const why = (h.why||[]);
    const shortWhy = why.slice(0,2).join(' · ') || '—';
    const rr = (h.rr!=null && isFinite(h.rr));
    const rrCls = !rr? '' : (h.rr>=2?'rrg':h.rr>=1.5?'rry':'rrd');
    const key=side+':'+h.symbol, open=!!rowOpen[key];
    const rrTxt = rr? ('<b>'+h.rr.toFixed(2)+'</b>'+(h.rr_max!=null&&h.rr_max>h.rr+0.2?` <span class="rmax">→${(+h.rr_max).toFixed(1)}</span>`:'')) : '—';
    const cf=h.confluence||null;
    const confBadge=(cf&&cf.n>=2)?`<span class="confb cf${Math.min(4,cf.n)}" data-tip="${esc('Confluence '+cf.n+'× — this coin lines up on '+cf.n+' independent signals: '+(cf.signals||[]).join(', ')+'. More stacked signals = higher-probability; these are boosted up the ranking.')}">⚑${cf.n}</span>`:'';
    const tr=document.createElement('tr'); tr.className=rowClass(h)+(open?' rowsel':''); tr.style.cursor='pointer';
    tr.setAttribute('onclick',`toggleRowPlan('${key}')`);
    tr.innerHTML =
      `<td class="rnk"><span class="expander">${open?'▾':'▸'}</span> ${rank}</td>`+
      `<td class="sym"><div class="symbox">${watchStar(h.symbol)}<a href="${tvLink(h.symbol)}" target="_blank" rel="noopener" onclick="event.stopPropagation()">${dispSym(h.symbol)}</a>${confBadge}${analyzeSideBtn(h.symbol,side)}</div></td>`+
      `<td>${scoreBadge(h.score)}${revBadge(h)}${edgeBadge(h.entry_tf||'4h', side)}</td>`+
      `<td>${fmtNum(P)}</td>`+
      `<td><span class="biaspill2 b-${(h.bias||'').toLowerCase().replace(/[^a-z]/g,'')}">${h.bias||'—'}</span></td>`+
      `<td class="tfstripcell">${tfBiasStrip(h.tf_bias)}</td>`+
      `<td data-tip="Recommended entry — ${esc(h.entry_basis||'a value pullback to the nearest level')}. Click the row for the full plan.">${h.entry!=null?fmtNum(h.entry):fmtNum(P)}${h.entry_tf?` <span class="tfsrc">${h.entry_tf}</span>`:''}</td>`+
      `<td data-tip="Recommended stop — ${esc(h.stop_basis||'beyond the next level')}.">${h.stop!=null?fmtNum(h.stop):'—'}</td>`+
      `<td data-tip="Base target — ${esc((h.tps&&h.tps[0]&&h.tps[0].basis)||'the nearest level the other way')}. Row expands to the full ladder.">${h.target!=null?fmtNum(h.target):'—'}</td>`+
      `<td class="${rrCls}" data-tip="Reward:risk to the base target${h.rr_max!=null?`; the arrow shows R:R to the furthest target (${(+h.rr_max).toFixed(1)}R)`:''}. Click the row for the full ladder.">${rrTxt}</td>`+
      (side==='long'
        ? `<td data-tip="${esc((h.lab_hits||[]).join(' · ')||'No out-of-sample-surviving lab combo fires on this coin right now.')}">${labCell(h)}</td>`
        : '')+
      `<td class="whycell" data-tip="${esc(why.join(' · '))}">${esc(shortWhy)}</td>`;
    tb.appendChild(tr);
    if(open){
      const dr=document.createElement('tr'); dr.className='planrow';
      dr.innerHTML=`<td colspan="${side==='long'?12:11}">${planPanelHtml(h, side, P, h.symbol)}</td>`;
      tb.appendChild(dr);
    }
  }
}
function renderBestLong(){ renderBoard('long'); }
function renderBestShort(){ renderBoard('short'); }
function leanPill(lean){
  const l=(lean||'neutral');
  const cls = l==='bullish'?'b-bullish':l==='bearish'?'b-bearish':'b-range';
  const txt = l==='bullish'?'Bullish ▲':l==='bearish'?'Bearish ▼':'Neutral';
  return `<span class="biaspill2 ${cls}">${txt}</span>`;
}
function renderCoil(){
  const rows=(lastData&&lastData.coil_board)||[];
  const tb=document.getElementById('coilrows'); if(!tb) return; tb.innerHTML="";
  const emp=document.getElementById('coilempty'); if(emp) emp.style.display=rows.length?'none':'block';
  const cnt=document.getElementById('coilCount');
  if(cnt) cnt.textContent=`${rows.length} of the whole universe (${(lastData&&lastData.universe)||'…'} pairs)`;
  let rank=0;
  for(const h of rows){
    rank++;
    const P=h.live!=null?h.live:h.price;
    const why=(h.why||[]); const shortWhy=why.slice(0,2).join(' · ')||'—';
    const key='coil:'+h.symbol, open=!!rowOpen[key];
    const tr=document.createElement('tr'); tr.className=rowClass(h)+(open?' rowsel':''); tr.style.cursor='pointer';
    tr.setAttribute('onclick',`toggleRowPlan('${key}')`);
    tr.innerHTML=
      `<td class="rnk"><span class="expander">${open?'▾':'▸'}</span> ${rank}</td>`+
      `<td class="sym"><div class="symbox">${watchStar(h.symbol)}<a href="${tvLink(h.symbol)}" target="_blank" rel="noopener" onclick="event.stopPropagation()">${dispSym(h.symbol)}</a>${analyzeBtn(h.symbol)}</div></td>`+
      `<td>${scoreBadge(h.score)}</td>`+
      `<td data-tip="Band width tighter than ${h.squeeze_pct!=null?h.squeeze_pct:'—'}% of its recent range.">${h.squeeze_pct!=null?h.squeeze_pct+'%':'—'}</td>`+
      `<td>${fmtNum(P)}</td>`+
      `<td>${leanPill(h.side)}</td>`+
      `<td class="tfstripcell">${coilTfStrip(h.coiled_tfs)}</td>`+
      `<td>${h.atr_pct==null?'—':(+h.atr_pct).toFixed(1)}%</td>`+
      coilSetupCell(h,'long')+
      coilSetupCell(h,'short')+
      `<td class="whycell" data-tip="${esc(why.join(' · '))}">${esc(shortWhy)}</td>`;
    tb.appendChild(tr);
    if(open){
      const dr=document.createElement('tr'); dr.className='planrow';
      const recFirst = h.rec_side==='short';
      const both = recFirst
        ? planPanelHtml(h.plan_short,'short',P,h.symbol)+planPanelHtml(h.plan_long,'long',P,h.symbol)
        : planPanelHtml(h.plan_long,'long',P,h.symbol)+planPanelHtml(h.plan_short,'short',P,h.symbol);
      const recNote = h.rec_side==='either'
        ? 'Neutral lean — trade whichever way it breaks; both plans shown.'
        : `Recommended side: <b>${h.rec_side==='long'?'LONG ▲':'SHORT ▼'}</b> (matches the coil\\'s lean) — shown first.`;
      dr.innerHTML=`<td colspan="11"><div class="coilnote">${recNote}</div><div class="planpair">${both}</div></td>`;
      tb.appendChild(dr);
    }
  }
}
// Which timeframes are coiled — a 15m→1W strip, each labelled with its squeeze %.
// Brighter/green = tighter (≥85 hot, ≥70 warm); dim = not really coiled or no data.
function coilTfStrip(ct){
  if(!ct) return '<span style="color:var(--dim2)">—</span>';
  let out='';
  for(const [k,lbl] of [['15m','15m'],['1h','1h'],['4h','4h'],['1d','1D'],['1w','1W']]){
    const v=ct[k];
    if(v==null){ continue; }
    const cls = v>=85?'ctf-hot' : v>=70?'ctf-warm' : 'ctf-cool';
    out+=`<span class="ctf ${cls}" data-tip="${lbl} squeeze: band width tighter than ${v}% of its recent range.${v>=70?' Coiled on this timeframe.':''}">${lbl} ${v}</span>`;
  }
  return out || '<span style="color:var(--dim2)">—</span>';
}
function renderScalp(){
  const rows=(lastData&&lastData.scalp_board)||[];
  const tb=document.getElementById('scalprows'); if(!tb) return; tb.innerHTML="";
  const emp=document.getElementById('scalpempty'); if(emp) emp.style.display=rows.length?'none':'block';
  const cnt=document.getElementById('scalpCount');
  const nB=rows.filter(h=>h.kind==='bounce').length, nT=rows.length-nB;
  if(cnt) cnt.textContent=`${nT} trend scalp${nT===1?'':'s'} + ${nB} counter-trend bounce${nB===1?'':'s'} off strong levels`;
  let rank=0;
  for(const h of rows){
    rank++;
    const P=h.live!=null?h.live:h.price;
    const side=h.side||'long';
    const why=(h.why||[]); const shortWhy=why.slice(0,2).join(' · ')||'—';
    const bounceBadge=(h.kind==='bounce')?`<span class="cbadge${h.counter_trend?' cbounce':''}" data-tip="${esc((h.counter_trend?'Counter-trend ':'')+'bounce scalp — a quick trade off a strong, '+(h.touches||'')+'×-tested '+(side==='long'?'support':'resistance')+' level ('+(h.entry_tf||'LTF')+'). Not HTF-aligned by design; the edge is the level + the snap-back.')}">${h.counter_trend?'↩ counter':'⤴ bounce'}</span>`:'';
    const rr=(h.rr!=null&&isFinite(h.rr));
    const rrCls=!rr?'':(h.rr>=2?'rrg':h.rr>=1.5?'rry':'rrd');
    const rrTxt=rr?('<b>'+h.rr.toFixed(2)+'</b>'+(h.rr_max!=null&&h.rr_max>h.rr+0.2?` <span class="rmax">→${(+h.rr_max).toFixed(1)}</span>`:'')):'—';
    const key='scalp:'+h.symbol, open=!!rowOpen[key];
    const tr=document.createElement('tr'); tr.className=rowClass(h)+(open?' rowsel':''); tr.style.cursor='pointer';
    tr.setAttribute('onclick',`toggleRowPlan('${key}')`);
    tr.innerHTML=
      `<td class="rnk"><span class="expander">${open?'▾':'▸'}</span> ${rank}</td>`+
      `<td class="sym"><div class="symbox">${watchStar(h.symbol)}<a href="${tvLink(h.symbol)}" target="_blank" rel="noopener" onclick="event.stopPropagation()">${dispSym(h.symbol)}</a>${analyzeSideBtn(h.symbol,side)}</div></td>`+
      `<td>${scoreBadge(h.score)}${revBadge(h)}${edgeBadge(h.entry_tf||'15m', side)}</td>`+
      `<td>${leanPill(side==='long'?'bullish':'bearish')}${bounceBadge}</td>`+
      `<td class="tfstripcell">${tfBiasStrip(h.tf_bias)}</td>`+
      `<td>${fmtNum(P)}</td>`+
      `<td data-tip="${esc(h.entry_basis||'15m entry')}">${h.entry!=null?fmtNum(h.entry):'—'}</td>`+
      `<td data-tip="${esc(h.stop_basis||'tight 15m stop')}">${h.stop!=null?fmtNum(h.stop):'—'} <span class="rr">${h.stop_pct!=null?h.stop_pct+'%':''}</span></td>`+
      `<td data-tip="${esc((h.tps&&h.tps[0]&&h.tps[0].basis)||'first 15m target')}">${h.target!=null?fmtNum(h.target):'—'}</td>`+
      `<td class="${rrCls}">${rrTxt}</td>`+
      `<td class="whycell" data-tip="${esc(why.join(' · '))}">${esc(shortWhy)}</td>`;
    tb.appendChild(tr);
    if(open){
      const dr=document.createElement('tr'); dr.className='planrow';
      dr.innerHTML=`<td colspan="11">${planPanelHtml(h, side, P, h.symbol)}</td>`;
      tb.appendChild(dr);
    }
  }
}
// ---- Spot buys tab (best longs reframed for a cash buyer) ----
function renderSpot(){
  const rows=(lastData&&lastData.spot_board)||[];
  const tb=document.getElementById('spotrows'); if(!tb) return; tb.innerHTML="";
  const emp=document.getElementById('spotempty'); if(emp) emp.style.display=rows.length?'none':'block';
  const cnt=document.getElementById('spotCount');
  if(cnt) cnt.textContent=`${rows.length} spot buy${rows.length===1?'':'s'} — 1×, no funding, no liquidation`;
  let rank=0;
  for(const h of rows){
    rank++;
    const P=h.live!=null?h.live:h.price;
    const why=(h.why||[]); const shortWhy=why.slice(0,2).join(' · ')||'—';
    const ext=(h.pct_vs_ema!=null)?h.pct_vs_ema:null;
    const extCls=ext==null?'':(ext<=8?'rrg':ext<=18?'rry':'rrd');
    const invPct=(h.entry!=null&&h.stop!=null&&h.entry>0)?((h.entry-h.stop)/h.entry*100):null;
    const rr=(h.rr!=null&&isFinite(h.rr));
    const rrCls=!rr?'':(h.rr>=2?'rrg':h.rr>=1.5?'rry':'rrd');
    const rrTxt=rr?('<b>'+h.rr.toFixed(2)+'</b>'+(h.rr_max!=null&&h.rr_max>h.rr+0.2?` <span class="rmax">→${(+h.rr_max).toFixed(1)}</span>`:'')):'—';
    const key='spot:'+h.symbol, open=!!rowOpen[key];
    const tr=document.createElement('tr'); tr.className=rowClass(h)+(open?' rowsel':''); tr.style.cursor='pointer';
    tr.setAttribute('onclick',`toggleRowPlan('${key}')`);
    tr.innerHTML=
      `<td class="rnk"><span class="expander">${open?'▾':'▸'}</span> ${rank}</td>`+
      `<td class="sym"><div class="symbox">${watchStar(h.symbol)}<a href="${tvLink(h.symbol)}" target="_blank" rel="noopener" onclick="event.stopPropagation()">${dispSym(h.symbol)}</a>${analyzeSideBtn(h.symbol,'long')}</div></td>`+
      `<td>${scoreBadge(h.spot_score!=null?h.spot_score:h.score)}${revBadge(h)}${edgeBadge(h.entry_tf||'4h','long',true)}</td>`+
      `<td class="${extCls}" data-tip="${esc(h.note_spot||'')}">${ext!=null?(ext>0?'+':'')+ext.toFixed(1)+'%':'—'}</td>`+
      `<td>${fmtNum(P)}</td>`+
      `<td data-tip="${esc(h.entry_basis||'limit into support')}">${h.entry!=null?fmtNum(h.entry):'—'}</td>`+
      `<td data-tip="${esc((h.stop_basis||'mental invalidation')+' — no liquidation on spot; a place to trim.')}">${h.stop!=null?fmtNum(h.stop):'—'} <span class="rr">${invPct!=null?'-'+invPct.toFixed(1)+'%':''}</span></td>`+
      `<td data-tip="${esc((h.tps&&h.tps[0]&&h.tps[0].basis)||'first target')}">${h.target!=null?fmtNum(h.target):'—'}</td>`+
      `<td class="${rrCls}">${rrTxt}</td>`+
      `<td data-tip="Spot has no funding — holding costs nothing.">${esc(h.hold||'swing')}</td>`+
      `<td class="whycell" data-tip="${esc(why.join(' · '))}">${esc(shortWhy)}</td>`;
    tb.appendChild(tr);
    if(open){
      const dr=document.createElement('tr'); dr.className='planrow';
      dr.innerHTML=`<td colspan="11">${planPanelHtml(h, 'long', P, h.symbol)}</td>`;
      tb.appendChild(dr);
    }
  }
}
// ---- Backtest tab (auto TF × side matrix, run in the background) ----
let btData=null;
async function loadBacktest(force){
  const meta=document.getElementById('btMeta');
  if(force&&meta) meta.textContent='refreshing…';
  try{ const r=await fetch('/backtest',{cache:'no-store'}); btData=await r.json(); }
  catch(e){ btData={error:'Request failed.'}; }
  renderBacktest();
  clearTimeout(window._btPoll);
  const incomplete = btData && (btData.pending || (btData.progress && btData.progress.done < btData.progress.total));
  if(incomplete){ window._btPoll=setTimeout(()=>loadBacktest(),20000); }  // poll while the sweep fills in
}
function btVerdict(exp){
  if(exp==null) return ['—',''];
  if(exp>=0.15) return ['✅ positive edge','pcg'];
  if(exp>0) return ['🟡 thin edge',''];
  return ['❌ negative edge','pcb'];
}
function btCell(s){
  if(!s||!s.n) return '<span style="color:var(--dim2)">—</span>';
  const cls=s.exp>=0.15?'pf-good':s.exp>0?'':'pf-bad';
  return `<span class="${cls}"><b>${s.exp>0?'+':''}${s.exp}R</b></span> · ${s.winrate}% <span class="rr">(${s.n})</span>`;
}
// Filter the per-coin table by symbol (BTC, SOL, …) and recompute the visible win-rate / total R.
function btFilterCoins(inp, key){
  const v=(inp.value||'').toUpperCase().trim();
  const tb=document.getElementById('btcoins_'+key); if(!tb) return;
  let tn=0, tw=0, tsum=0, shown=0;
  tb.querySelectorAll('tr.btcoin').forEach(function(r){
    const sym=r.getAttribute('data-sym')||''; const m=!v||sym.indexOf(v)>=0;
    r.style.display=m?'':'none';
    const d=r.nextElementSibling; if(d&&d.classList.contains('btdetail')) d.style.display='none';
    if(m){ shown++; const nn=+r.getAttribute('data-n')||0, wr=+r.getAttribute('data-wr')||0, sr=+r.getAttribute('data-sr')||0; tn+=nn; tw+=nn*wr/100; tsum+=sr; }
  });
  const st=document.getElementById('btfstat_'+key);
  if(st){ st.innerHTML = tn? `${shown} coin${shown===1?'':'s'} · ${tn} trades · win <b>${(tw/tn*100).toFixed(1)}%</b> · total <b class="${tsum>0?'pf-good':'pf-bad'}">${tsum>0?'+':''}${tsum.toFixed(1)}R</b>` : (v?'no matches':''); }
}
function btSideCard(label,emoji,s){
  if(!s||!s.n){ return `<div class="btcard"><div class="btcard-h">${emoji} ${label}</div><div class="btnote">No qualifying trades in the sample.</div></div>`; }
  const [vlabel,vcls]=btVerdict(s.exp);
  const ins=s.insights||{}, finds=s.findings||[];
  // Analysis — what worked + where the edge leaks.
  let anl='';
  if(s.breakeven_wr!=null){ const ok=s.winrate>=s.breakeven_wr;
    anl+=`<div class="btidea">🎯 Breakeven win-rate ≈ <b>${s.breakeven_wr}%</b> (from +${s.avg_win}/${s.avg_loss}R). Board wins <b>${s.winrate}%</b> — <span class="${ok?'pf-good':'pf-bad'}">${ok?'above breakeven':'below breakeven — needs a higher hit-rate or bigger winners'}</span>.</div>`; }
  if(ins.stop_then_tp_pct!=null){ const hot=ins.stop_then_tp_pct>=35;
    anl+=`<div class="btidea${hot?' btbad':''}">🛑 <b>${ins.stop_then_tp_pct}%</b> of losers hit the target <b>after</b> being stopped${hot?' — stops are too tight; widening ~30% would flip many of these to wins':' — stops look reasonably placed'}. Losers ran +${ins.loser_mfe}R toward target first.</div>`; }
  if(s.avg_win_mfe!=null&&s.avg_win!=null){ const extra=+(s.avg_win_mfe-s.avg_win).toFixed(2); const worth=extra>=0.4;
    anl+=`<div class="btidea${worth?' btgood':''}">🏃 Single TP at the mean — winners banked <b>+${s.avg_win}R</b> but ran to <b>+${s.avg_win_mfe}R</b> at best${worth?` — leaving ~${extra}R on the table; a runner/ladder would likely add expectancy`:' — little extra run, so a single mean-reversion TP is right'}. (Backtest resolves on this one TP, not a ladder.)</div>`; }
  if(s.avg_win_mae!=null){ const clean=s.avg_win_mae<=0.4; const deep=s.avg_win_mae>=0.7;
    anl+=`<div class="btidea${clean?' btgood':(deep?' btbad':'')}">📉 <b>Winners' avg drawdown: -${s.avg_win_mae}R</b> (losers -${s.avg_loss_mae!=null?s.avg_loss_mae:'—'}R). ${clean?'Winners barely dip before working — entries are clean, and the stop could be tightened to lift R:R.':(deep?'Winners routinely sit deep underwater first — the entry is early/loose; a more patient trigger would raise R:R and cut heat.':'Winners take a moderate dip first — some room to tighten entries.')}</div>`; }
  else if(s.avg_mae!=null){ anl+=`<div class="btidea">📉 Avg <b>max drawdown</b> before resolving: <b>-${s.avg_mae}R</b> · avg <b>TP distance</b>: <b>${s.avg_tp_pct!=null?s.avg_tp_pct+'%':'—'}</b> from entry.</div>`; }
  if(s.monthly&&s.monthly.length){
    const mm=s.monthly; const pos=mm.filter(x=>x.exp>0).length;
    // CORRELATION: does this board's monthly P&L track how bearish/bullish the market was that month?
    const pairs=mm.filter(x=>x.idx!=null).map(x=>[x.idx,x.sumR]);
    let corrTxt='';
    if(pairs.length>=6){
      const n=pairs.length, mx=pairs.reduce((a,p)=>a+p[0],0)/n, my=pairs.reduce((a,p)=>a+p[1],0)/n;
      let sxy=0,sxx=0,syy=0; pairs.forEach(p=>{const dx=p[0]-mx,dy=p[1]-my;sxy+=dx*dy;sxx+=dx*dx;syy+=dy*dy;});
      const r=(sxx>0&&syy>0)?sxy/Math.sqrt(sxx*syy):0;
      const strong=Math.abs(r)>=0.5, dir=r<0;
      corrTxt=`<div class="btidea ${strong?(dir?'btgood':'btbad'):''}">🔬 <b>Monthly P&L vs market direction:</b> correlation <b>${r.toFixed(2)}</b> — ${dir?'this board makes more when the market is <b>more bearish</b> that month':'this board makes more when the market is <b>more bullish</b>'} ${strong?'(a <b>strong</b> link — the edge is regime-driven, so gate it on market direction)':'(a weak link — the edge here is NOT mainly about market direction)'}.</div>`;
    }
    anl+=`<div class="btideah">🗓️ Win-rate by month + market environment (${pos}/${mm.length} months positive)</div>`;
    anl+=corrTxt;
    anl+=`<div style="overflow-x:auto"><table class="bt" style="min-width:100%"><thead><tr><th>Month</th><th>Trades</th><th>Win rate</th><th>Exp</th><th>Total R</th><th data-tip="Average alt-breadth that month (% of the basket above its 200-EMA) and how hard the market index was moving. This is the ENVIRONMENT the month played out in.">Market that month</th></tr></thead><tbody>`
      +mm.map(x=>{const envc=(x.idx!=null)?(x.idx<=-0.5?'pf-good':(x.idx>=0.5?'pf-bad':'')):''; const env=(x.breadth!=null?x.breadth+'% breadth':'')+(x.idxlab?' · '+x.idxlab:'');
        return `<tr><td style="white-space:nowrap">${x.m}</td><td>${x.n}</td><td class="${x.winrate>=50?'pf-good':'pf-bad'}">${x.winrate}%</td><td class="${x.exp>0?'pf-good':'pf-bad'}">${x.exp>0?'+':''}${x.exp}R</td><td class="${x.sumR>0?'pf-good':'pf-bad'}">${x.sumR>0?'+':''}${x.sumR}R</td><td class="${envc}" style="white-space:nowrap">${env||'—'}</td></tr>`;}).join('')
      +`</tbody></table></div>`;
  }
  if(finds.length){
    anl+=`<div class="btideah">💡 What worked (segment expectancy)</div>`;
    anl+=finds.map(f=>`<div class="btidea">${esc(f.label)}: <b class="${f.a.exp>f.b.exp?'pf-good':'pf-bad'}">${esc(f.a.name)} ${f.a.exp>0?'+':''}${f.a.exp}R</b> <span class="rr">(${f.a.n})</span> vs <b class="${f.b.exp>f.a.exp?'pf-good':'pf-bad'}">${esc(f.b.name)} ${f.b.exp>0?'+':''}${f.b.exp}R</b> <span class="rr">(${f.b.n})</span> → favour <b>${esc(f.better)}</b></div>`).join('');
  }
  // MARKET REGIME panel — how this strategy/side does by broad breadth (risk-on / mixed / risk-off)
  // and by dominance (alts vs BTC leading). This is the "market environment for longs" read.
  const bb=s.by_breadth||{}; const isLong=(s.side==='long');
  if(bb.breadth && Object.keys(bb.breadth).length){
    const order=['risk_on','mixed','risk_off'];
    const lbl={risk_on:'Risk-on (breadth ≥55%)',mixed:'Mixed (40–55%)',risk_off:'Risk-off (≤40%)'};
    const tip={risk_on:'Most of the basket is above its 200-EMA — broad alt participation.',mixed:'Split market — no clear breadth.',risk_off:'Most alts below their 200-EMA — broad bleed.'};
    anl+=`<div class="btideah">🌐 By market regime — where this ${isLong?'LONG':'SHORT'} edge lives</div>`;
    anl+=`<div style="overflow-x:auto"><table class="bt" style="min-width:100%"><thead><tr><th>Breadth regime</th><th>Trades</th><th>Win rate</th><th>Expectancy</th><th>Total R</th></tr></thead><tbody>`
      +order.filter(k=>bb.breadth[k]).map(k=>{const r=bb.breadth[k];
        return `<tr><td style="white-space:nowrap" data-tip="${tip[k]}">${lbl[k]}</td><td>${r.n}</td><td class="${r.winrate>=50?'pf-good':'pf-bad'}">${r.winrate}%</td><td class="${r.exp>0?'pf-good':'pf-bad'}">${r.exp>0?'+':''}${r.exp}R</td><td class="${r.sumR>0?'pf-good':'pf-bad'}">${r.sumR>0?'+':''}${r.sumR}R</td></tr>`;}).join('')
      +`</tbody></table></div>`;
    // recommendation line
    const best=order.filter(k=>bb.breadth[k]&&bb.breadth[k].n>=20).sort((a,b)=>bb.breadth[b].exp-bb.breadth[a].exp)[0];
    if(best){ const r=bb.breadth[best]; const good=r.exp>0;
      anl+=`<div class="btidea${good?' btgood':' btbad'}">➡️ Best in <b>${lbl[best].split(' (')[0]}</b> (${r.exp>0?'+':''}${r.exp}R over ${r.n}). ${good?`This ${isLong?'long':'short'} side should be gated to that regime — take it when breadth agrees, stand aside otherwise.`:'Even its best regime is negative — this side/timeframe has no edge worth trading.'}</div>`; }
  }
  if(bb.dom && (bb.dom.alt||bb.dom.btc)){
    const a=bb.dom.alt,b=bb.dom.btc;
    const cell=(r)=> r?`<b class="${r.exp>0?'pf-good':'pf-bad'}">${r.exp>0?'+':''}${r.exp}R</b> <span class="rr">(${r.n})</span>`:'—';
    anl+=`<div class="btidea" data-tip="Dominance proxy: over the prior 20 bars, did the median alt outrun BTC (alts leading / BTC.D falling) or lag it (BTC leading / BTC.D rising)?">🧭 Dominance — alts leading: ${cell(a)} vs BTC leading: ${cell(b)}${(a&&b)?` → favour <b>${a.exp>b.exp?'alt-led tape':'BTC-led tape'}</b>`:''}</div>`;
  }
  const ckey=((s.tf||'')+'_'+(s.side||'')).replace(/[^a-z0-9]/gi,'');
  const fmtd=ts=>{try{const d=new Date(ts);const p=n=>String(n).padStart(2,'0');return d.getUTCFullYear()+'-'+p(d.getUTCMonth()+1)+'-'+p(d.getUTCDate())+' '+p(d.getUTCHours())+':'+p(d.getUTCMinutes());}catch(e){return '—';}};
  const coinRow=(p)=>{
    const did='btd_'+ckey+'_'+String(p.symbol).replace(/[^A-Za-z0-9]/g,'');
    const trs=(p.trades||[]).map(x=>{const win=x.outcome==='win';
      const env=`BTC ${x.btc_trend||'?'} · ${x.btc_vol==='hi'?'volatile':x.btc_vol==='lo'?'calm':'?'} · ${x.session||'?'}`;
      const why=x.kind==='momentum'?`Breakout / trend-continuation (RSI ${x.rsi})`:`Fade an overbought pop (RSI ${x.rsi})`;
      return `<tr><td style="white-space:nowrap">${fmtd(x.ts)}</td><td class="${win?'pf-good':'pf-bad'}" data-tip="Entry ${fmtNum(x.entry)} · stop ${fmtNum(x.stop)} (${x.stopw}%) · target ${fmtNum(x.target)} (${x.tppct}%) · R:R ${x.rr} · held ${x.bars} bars · worst drawdown -${x.mae}R">${win?'✓':'✗'} ${x.r>0?'+':''}${x.r}R</td><td>${esc(env)}</td><td class="whycell">${esc(why)}</td></tr>`;}).join('');
    const detail=`<tr class="btdetail" id="${did}" style="display:none"><td colspan="8"><div class="perfsub">${dispSym(p.symbol)} — recent trades (date · result · environment · why)</div><table class="bt"><thead><tr><th>Date (UTC)</th><th>Result</th><th>Environment</th><th>Setup</th></tr></thead><tbody>${trs||'<tr><td colspan=4>—</td></tr>'}</tbody></table></td></tr>`;
    return `<tr class="btcoin" data-sym="${String(p.symbol).toUpperCase()}" data-n="${p.n}" data-wr="${p.winrate}" data-sr="${p.sumR}" style="cursor:pointer" onclick="var d=document.getElementById('${did}');if(d)d.style.display=d.style.display==='none'?'':'none';">`
      +`<td class="sym">▸ ${dispSym(p.symbol)}</td>`
      +`<td>${p.n}</td><td class="${p.winrate>=50?'pf-good':'pf-bad'}">${p.winrate}%</td>`
      +`<td class="${p.exp>0?'pf-good':'pf-bad'}">${p.exp>0?'+':''}${p.exp}R</td>`
      +`<td class="${p.sumR>0?'pf-good':'pf-bad'}">${p.sumR>0?'+':''}${p.sumR}R</td>`
      +`<td data-tip="Average stop distance from entry, as a % of price.">${p.stopw!=null?p.stopw+'%':'—'}</td>`
      +`<td class="${(p.mae||0)>=0.85?'pf-bad':''}" data-tip="Average worst drawdown before the trade resolved, in R.">${p.mae!=null?'-'+p.mae+'R':'—'}</td>`
      +`<td class="${(p.stp||0)>=35?'pf-bad':''}" data-tip="Share of losers that hit target after being stopped.">${p.stp!=null?p.stp+'%':'—'}</td></tr>`
      +detail;
  };
  const rows=(s.per_symbol||[]).map(coinRow).join('');
  return `<div class="btcard">
    <div class="btcard-h">${emoji} ${label} <span class="btverdict ${vcls}">${vlabel}</span></div>
    <div class="perfcards">
      ${perfCard('Trades', s.n, '')}
      ${perfCard('Win rate', s.winrate+'%', s.winrate>=50?'pcg':s.winrate>=40?'':'pcb')}
      ${perfCard('Expectancy (net)', (s.exp>0?'+':'')+s.exp+'R', s.exp>0?'pcg':'pcb', 'Average R per trade AFTER fees. This is the number that matters.')}
      ${perfCard('Total R (net)', (s.sumR>0?'+':'')+s.sumR+'R', s.sumR>0?'pcg':'pcb')}
      ${perfCard('Avg win / loss', (s.avg_win!=null?('+'+s.avg_win):'—')+' / '+(s.avg_loss!=null?s.avg_loss:'—')+'R', '')}
      ${perfCard('Avg TP distance', s.avg_tp_pct!=null?s.avg_tp_pct+'%':'—', '', 'Average distance from entry to the take-profit (the 20-EMA mean target), as a % of price. This is how far price has to travel to bank a win.')}
      ${perfCard('Avg max drawdown', s.avg_mae!=null?'-'+s.avg_mae+'R':'—', s.avg_mae!=null&&s.avg_mae>=0.8?'pcb':'', 'Average worst drawdown per trade before it resolved, in R (max adverse excursion). ~1R means trades typically dipped close to the stop before working; low means they went green quickly.')}
      ${perfCard('Fee drag', '-'+s.fee_drag+'R', s.fee_drag>0?'pcb':'', 'Fees cost per trade on average (gross expectancy '+(s.gross_exp>0?'+':'')+s.gross_exp+'R minus net).')}
    </div>
    <div class="btanalysis">${anl}</div>
    ${(()=>{const pf=s.portfolio; if(!pf) return ''; const c=pf.compound,f=pf.fixed; const money=v=>'$'+Math.round(v).toLocaleString();
      return `${pf.risk_flat?`<div class="perfsub">💵 Flat risk — <b>${money(pf.risk_per_trade)} risked per trade</b> (1R = ${money(pf.risk_per_trade)}; position auto-sized so a stop-out loses exactly ${money(pf.risk_per_trade)}). $ P&L = expectancy × ${pf.n} trades × ${money(pf.risk_per_trade)}.</div>
      <div class="perfcards">
        ${perfCard('Flat-risk → end', money(pf.risk_flat.end), pf.risk_flat.end>=pf.start?'pcg':'pcb', 'Start '+money(pf.start)+', add net-R×'+money(pf.risk_per_trade)+' each trade. This is the "risking $100 a trade" model.')}
        ${perfCard('Flat-risk return', (pf.risk_flat.ret_pct>0?'+':'')+pf.risk_flat.ret_pct+'%', pf.risk_flat.ret_pct>0?'pcg':'pcb')}
        ${perfCard('Flat-risk profit', (pf.risk_flat.end-pf.start>=0?'+':'')+money(pf.risk_flat.end-pf.start), pf.risk_flat.end>=pf.start?'pcg':'pcb', 'Total $ profit at '+money(pf.risk_per_trade)+' risk/trade = net total-R × '+money(pf.risk_per_trade)+'.')}
        ${perfCard('Flat-risk max DD', '-'+pf.risk_flat.max_dd_pct+'% ('+money(pf.risk_flat.max_dd_abs)+')', pf.risk_flat.max_dd_pct>=25?'pcb':'', 'Deepest peak-to-trough dip of the flat-risk equity curve — taking EVERY signal.')}
      </div>
      ${pf.capped?`<div class="perfsub">🧯 Capped concurrency — same $${pf.risk_per_trade}/trade but <b>max ${pf.capped.cap} positions open at once</b> (skip a signal when full). This is the realistic, tradeable version — it stops you piling into 60 correlated trades at once, which is what causes the huge drawdown.</div>
      <div class="perfcards">
        ${perfCard('Capped → end', money(pf.capped.end), pf.capped.end>=pf.start?'pcg':'pcb', 'Max '+pf.capped.cap+' concurrent positions, $'+pf.risk_per_trade+' risk each.')}
        ${perfCard('Capped return', (pf.capped.ret_pct>0?'+':'')+pf.capped.ret_pct+'%', pf.capped.ret_pct>0?'pcg':'pcb')}
        ${perfCard('Capped max DD', '-'+pf.capped.max_dd_pct+'% ('+money(pf.capped.max_dd_abs)+')', pf.capped.max_dd_pct>=25?'pcb':'pcg', 'Deepest dip when you cap at '+pf.capped.cap+' open positions — compare to the flat-risk DD above; this is the number that matters for surviving it.')}
        ${perfCard('Signals taken', pf.capped.taken+' / '+(pf.capped.taken+pf.capped.skipped), '', 'How many signals you actually took vs skipped because all '+pf.capped.cap+' slots were full. A high skip rate means the edge clusters — you only need a few slots.')}
      </div>`:''}`:''}
      <div class="perfsub">💰 Portfolio sim — ${money(pf.start)} start · risk 1% of equity as margin but <b>min ${money(pf.min_margin)}</b> at ${pf.lev}× (<b>min ${money(pf.min_notional)}</b> trade) · ${pf.n} trades · net of fees</div>
      <div class="perfcards">
        ${perfCard('Compounding → end', money(c.end), c.end>=pf.start?'pcg':'pcb', 'Margin each trade = max(1% of the CURRENT growing equity, $'+pf.min_margin+'), at '+pf.lev+'×. Winners get re-invested → exponential.')}
        ${perfCard('Compounding return', (c.ret_pct>0?'+':'')+c.ret_pct+'%', c.ret_pct>0?'pcg':'pcb')}
        ${perfCard('Compounding max DD', '-'+c.max_dd_pct+'% ('+money(c.max_dd_abs)+')', c.max_dd_pct>=25?'pcb':'', 'The deepest the account fell from a prior high, shown as a % and in $. This is the worst losing streak you would have had to sit through — the pain, not the average.')}
        ${perfCard('Fixed → end', money(f.end), f.end>=pf.start?'pcg':'pcb', 'Every trade uses the minimum $'+pf.min_margin+' margin ($'+pf.min_notional+' notional), flat — linear, no re-investment.')}
        ${perfCard('Fixed return', (f.ret_pct>0?'+':'')+f.ret_pct+'%', f.ret_pct>0?'pcg':'pcb')}
        ${perfCard('Fixed max DD', '-'+f.max_dd_pct+'% ('+money(f.max_dd_abs)+')', f.max_dd_pct>=25?'pcb':'')}
        ${perfCard('Max open at once', s.max_concurrent!=null?s.max_concurrent:'—', (s.max_concurrent||0)> (Math.floor(pf.start/pf.min_margin))?'pcb':'', 'The most positions that would be open simultaneously if you took every signal. Your $'+pf.start+' holds ~'+Math.floor(pf.start/pf.min_margin)+' margin slots of $'+pf.min_margin+'. If this exceeds that, you could not actually take them all.')}
      </div>
      ${(()=>{const series=[];
        if(pf.risk_flat&&pf.risk_flat.curve) series.push({name:'Flat $100-risk',color:'#3fb950',pts:pf.risk_flat.curve});
        if(pf.capped&&pf.capped.curve) series.push({name:'Capped ×'+(pf.capped.cap||5),color:'#58a6ff',pts:pf.capped.curve});
        if(pf.compound&&pf.compound.curve) series.push({name:'Compounding',color:'#d29922',pts:pf.compound.curve});
        if(!series.length) return '';
        let tmin=Infinity,tmax=-Infinity,ymin=Infinity,ymax=-Infinity;
        series.forEach(se=>se.pts.forEach(p=>{tmin=Math.min(tmin,p[0]);tmax=Math.max(tmax,p[0]);ymin=Math.min(ymin,p[1]);ymax=Math.max(ymax,p[1]);}));
        ymin=Math.min(ymin,pf.start); if(!(ymax>ymin)) ymax=ymin+1;
        const W=680,H=210,PL=54,PR=12,PT=26,PB=22;
        const xs=t=>PL+(W-PL-PR)*((t-tmin)/((tmax-tmin)||1));
        const ys=v=>PT+(H-PT-PB)*(1-((v-ymin)/((ymax-ymin)||1)));
        const path=se=>se.pts.map((p,i)=>(i?'L':'M')+xs(p[0]).toFixed(1)+' '+ys(p[1]).toFixed(1)).join(' ');
        const fmtM=v=>'$'+Math.round(v).toLocaleString();
        const fmtD=t=>{const d=new Date(t);return d.getUTCFullYear()+'-'+String(d.getUTCMonth()+1).padStart(2,'0');};
        const startY=ys(pf.start);
        const lines=series.map(se=>`<path d="${path(se)}" fill="none" stroke="${se.color}" stroke-width="1.8" vector-effect="non-scaling-stroke"/>`).join('');
        const legend=series.map((se,i)=>`<g transform="translate(${PL+i*175},14)"><rect width="11" height="11" y="-9" rx="2" fill="${se.color}"/><text x="16" y="0" fill="var(--dim)" font-size="11">${se.name} → ${fmtM(se.pts[se.pts.length-1][1])}</text></g>`).join('');
        return `<div class="perfsub">📈 Portfolio size over time — same trades, three sizing rules</div>
        <svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="none" style="width:100%;height:210px;background:var(--panel2);border:1px solid var(--line);border-radius:10px">
          <line x1="${PL}" y1="${startY}" x2="${W-PR}" y2="${startY}" stroke="var(--line)" stroke-dasharray="3 3"/>
          <text x="${PL-6}" y="${startY+3}" text-anchor="end" fill="var(--dim)" font-size="10">${fmtM(pf.start)}</text>
          <text x="${PL-6}" y="${ys(ymax)+9}" text-anchor="end" fill="var(--dim)" font-size="10">${fmtM(ymax)}</text>
          <text x="${PL+2}" y="${H-6}" fill="var(--dim)" font-size="10">${fmtD(tmin)}</text>
          <text x="${W-PR}" y="${H-6}" text-anchor="end" fill="var(--dim)" font-size="10">${fmtD(tmax)}</text>
          ${lines}${legend}
        </svg>
        <div class="histnote">Flat $100-risk (green) risks a fixed $100/trade. Capped ×5 (blue) = same but max 5 open at once. Compounding (amber) sizes by margin (1% of equity at 10×) — it risks far less per trade, so it grows slower but far smoother.</div>`;})()}
      <div class="histnote">⚠ Idealised single-stream model — trades run one after another. "Max open at once" shows how many would really overlap. Drawdown = worst peak-to-trough dip of the equity curve, in % and $. Fees ARE included.</div>`;})()}
    <div class="perfsub">Per-coin (best first) · click a coin to expand its trades · Stop % = avg stop width · MaxDD = avg drawdown · last = stops-too-tight rate</div>
    <div style="display:flex;gap:8px;align-items:center;margin:4px 0 8px;flex-wrap:wrap">
      <input type="text" placeholder="filter coin — e.g. BTC, SOL…" oninput="btFilterCoins(this,'${ckey}')" style="background:var(--panel2);border:1px solid var(--line);border-radius:8px;padding:6px 10px;color:var(--txt);font-size:12px;min-width:180px">
      <span id="btfstat_${ckey}" style="color:var(--dim);font-size:12px"></span>
    </div>
    <table class="bt"><thead><tr><th>Coin</th><th>Trades</th><th>Win rate</th><th>Expectancy</th><th>Total R</th><th data-tip="Average stop distance from entry as a % of price.">Stop %</th><th data-tip="Average worst drawdown before a trade resolved, in R.">Max DD</th><th data-tip="Share of losers that hit target after being stopped.">Stop-tight</th></tr></thead><tbody id="btcoins_${ckey}">${rows}</tbody></table>
    ${(()=>{const smp=s.sample||[]; if(!smp.length) return ''; const long=s.side==='long';
      const fmtd=ts=>{try{const d=new Date(ts);const p=n=>String(n).padStart(2,'0');return d.getUTCFullYear()+'-'+p(d.getUTCMonth()+1)+'-'+p(d.getUTCDate())+' '+p(d.getUTCHours())+':'+p(d.getUTCMinutes());}catch(e){return '—';}};
      const rows2=smp.map(x=>{const win=x.outcome==='win';
        const why=long?`Oversold flush (RSI ${x.rsi}) snapping back in an uptrend`:`Overbought pop (RSI ${x.rsi}) rolling over in a downtrend`;
        const env=`BTC ${x.btc_trend||'?'} · ${x.btc_vol==='hi'?'volatile':x.btc_vol==='lo'?'calm':'?'} · ${x.session||'?'}`;
        return `<tr><td style="white-space:nowrap">${fmtd(x.ts)}</td>`
          +`<td class="sym"><a href="${tvLink(x.symbol)}" target="_blank" rel="noopener">${dispSym(x.symbol)}</a></td>`
          +`<td class="${win?'pf-good':'pf-bad'}" data-tip="Entry ${fmtNum(x.entry)} · stop ${fmtNum(x.stop)} (${x.stopw}%) · target ${fmtNum(x.target)} (${x.tppct}%) · R:R ${x.rr} · held ${x.bars} bars · worst drawdown -${x.mae}R">${win?'✓':'✗'} ${x.r>0?'+':''}${x.r}R</td>`
          +`<td data-tip="What the market leader was doing when this trade fired.">${esc(env)}</td>`
          +`<td class="whycell">${esc(why)}</td></tr>`;}).join('');
      return `<div class="perfsub">🧾 Trade log — most recent ${smp.length} trades · date · coin · result · market environment · why it fired</div>
        <table class="bt"><thead><tr><th>Date (UTC)</th><th>Coin</th><th>Result</th><th>Market environment</th><th>Why the trade was taken</th></tr></thead><tbody>${rows2}</tbody></table>`;})()}
  </div>`;
}
let labStrat=null;
function setLabStrat(k){ labStrat=k; renderBacktest(); }
function renderBacktest(){
  const body=document.getElementById('btBody'), meta=document.getElementById('btMeta'), emp=document.getElementById('btempty'); if(!body) return;
  const d=btData; if(!d) return;
  if(d.pending){ if(emp){emp.style.display='block'; emp.innerHTML='⏳ Apex is running the first <b>Strategy Lab</b> sweep — 200-EMA · Supertrend · CPR · Mix · BTC Monday, each over a liquid basket × timeframes × both sides. Fills in here automatically, then refreshes every ~6h.';} body.innerHTML=''; if(meta) meta.textContent='running the first sweep…'; return; }
  if(d.error){ if(emp) emp.style.display='none'; body.innerHTML=`<div class="bt-empty">${esc(d.error)}</div>`; if(meta) meta.textContent=''; return; }
  if(emp) emp.style.display='none';
  const lab=d.lab||{};
  const strats=(d.strategies||[{key:'revert',name:'200-EMA reversion'}]);
  if(!labStrat||!strats.some(s=>s.key===labStrat)) labStrat=strats[0].key;
  const prog=d.progress;
  const sweeping=(prog&&prog.done<prog.total)?` · ⏳ sweeping ${prog.done}/${prog.total} strategies (last: ${esc(prog.last||'')})`:'';
  if(meta) meta.textContent=`${d.coins} coins · ${d.fees_bps} bps fees · as of ${ago(d.ts)}${sweeping}`;
  // Strategy selector — one tab per idea.
  const desc={revert:'Buy oversold dips / fade overbought pops back to the mean (200-EMA + RSI). Calm-BTC only.',
    supertrend:'Trend-follow: buy the pullback to a rising Supertrend line / sell the pop to a falling one.',
    cpr:'Trade reactions at the rolling Central Pivot Range (pivot / BC / TC).',
    mix:'Confluence: only trade when the 200-EMA trend, the Supertrend, AND the pivot all agree.',
    ema200pb:'Simplest trend trade: buy the pullback that tags a rising 200-EMA / short the rip that tags a falling 200-EMA. Wide stop (~1.8×ATR beyond the line) — only wrong on a decisive close through the mean.',
    goldencross:'Golden Cross (50-EMA crosses ABOVE 200-EMA) = go long / Death Cross (50 below 200) = go short. Classic long-term trend flip, wide stop, ~3R target, run across every timeframe.',
    highwr:'⭐ Built for WIN-RATE: trend-aligned + regime-gated + calm-only oversold/overbought snap, NEAR take-profit (banked often) + WIDE stop (rarely hit), R:R ~0.65–0.8 so a 60%+ hit-rate turns a profit. Now the ONLY strategy in the lab, over a DEEP history (daily & 4h ≈ 4 years, 1h ≈ 1.4y). Shorts win in risk-off (bear breadth); longs need risk-on.',
    pullback20:'High win-rate trend-continuation: buy the shallow pullback that tags & reclaims the fast 20-EMA in an uptrend (mirror for shorts). Near target, moderately wide stop. The fast mean reclaims often, so it tends to win frequently.',
    bullmom:'↗ The LONG-in-a-BULL hunt: only fires when the WHOLE market is risk-on (breadth ≥60%, BTC up) — buy a FRESH breakout in strong momentum and RIDE it (~2.5R). Mirror shorts a broad risk-off breakdown. Lower win-rate, bigger winners — the opposite trade-off to High win-rate.',
    emaconf:'🧰 The 5-TOOL CONFLUENCE swing system (4H/Daily only — never LTF). Long when the HTF stack is bullish (price above the 50 & 200 EMA, 10>20 EMA, Supertrend GREEN), then buy the PULLBACK into value — a tag of the 10/20/50 EMA or the 0.618–0.786 Fib magic zone — on the close that turns back up. Stop rides the Supertrend line / swing; target the prior swing high. Shorts mirror it. Chop is deliberately NOT filtered out.',
    pro:'★ PREMIUM confluence — the cleanest-only setups, aiming for ~70% win-rate WITH R:R ≥ 1. Every filter must align: with-trend + favourable breadth + BTC not fighting + calm tape + price reacting at a REAL support/resistance (or 20-EMA) + a turned RSI extreme, and the trade is skipped unless reward:risk ≥ 1. Long = buy held support in a bull; Short = fade a rejected resistance in a bear. Fewer, higher-quality trades.',
    pro_stk:'★ PREMIUM confluence on US STOCKS (daily, ~6yr history via Stooq, SPY as the market index, breadth across ~48 large-caps). Same cleanest-only logic as the crypto Premium board — buy held support in an up-market, fade rejected resistance in a down-market, R:R ≥ 1 required. Stocks trend more cleanly than alts, so this is the honest test of whether the edge is stronger here.',
    highwr_stk:'High win-rate mean-reversion on US STOCKS (daily, SPY index, ~48 large-caps). Near take-profit / wide stop snap-back, regime-gated. The stock version of the high-win-rate board.',
    monday:'BTC/majors: fade the weekly Monday opening range — hold the Monday low (long) or reject the Monday high (short).'};
  let sel=`<div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:10px">`+
    strats.map(s=>{const on=s.key===labStrat; const has=lab[s.key]; return `<button onclick="setLabStrat('${s.key}')" style="cursor:pointer;border-radius:8px;padding:7px 12px;font-size:12.5px;font-weight:600;border:1px solid ${on?'var(--accent)':'var(--line)'};background:${on?'rgba(63,185,80,.12)':'var(--panel2)'};color:${on?'var(--txt)':'var(--dim)'}">${esc(s.name)}${has?'':' ⏳'}</button>`;}).join('')+`</div>`;
  const S=lab[labStrat]||{};
  // Data-driven: show exactly the timeframes this board actually has (crypto = 1h/4h/1d, stocks = 1d).
  const _tford=['5m','15m','1h','4h','1d','1wk'];
  let tfs=_tford.filter(tf=>S[tf]&&(S[tf].long||S[tf].short));
  if(!tfs.length) tfs=(labStrat==='monday')?['4h','1d']:['1h','4h','1d'];
  const sname=(strats.find(s=>s.key===labStrat)||{}).name||labStrat;
  let m=`<div class="perfsub">🧪 ${esc(sname)} — <span style="color:var(--dim)">${esc(desc[labStrat]||'')}</span></div>`;
  m+=`<table class="bt btmatrix"><thead><tr><th>Timeframe</th><th>🟢 Longs</th><th>🔴 Shorts</th></tr></thead><tbody>`;
  let any=false;
  for(const tf of tfs){ const row=S[tf]||{}; if((row.long&&row.long.n)||(row.short&&row.short.n)) any=true;
    m+=`<tr><td><b>${tf}</b></td><td>${btCell(row.long)}</td><td>${btCell(row.short)}</td></tr>`; }
  m+=`</tbody></table>`;
  let det='';
  for(const tf of tfs){ const row=S[tf]||{};
    if(!(row.long&&row.long.n)&&!(row.short&&row.short.n)) continue;
    det+=`<details class="bttf"><summary>${tf} — full breakdown (per-coin, trade log, portfolio)</summary><div class="btgrid">${btSideCard('Longs · '+tf,'🟢',row.long)}${btSideCard('Shorts · '+tf,'🔴',row.short)}</div></details>`;
  }
  if(!any){ det=`<div class="histnote">⏳ This strategy hasn't finished sweeping yet — it fills in as the lab works through each one.</div>`; }
  body.innerHTML=sel+m+(any?`<div class="perfsub">Per-timeframe detail (click to expand)</div>`:'')+det
    +`<div class="histnote">⚠ Each strategy is backtested the same honest way — no look-ahead, stop-first on ties, net of fees, on a ~60-coin liquid basket. Compare the tabs: a strategy that's clearly positive on a timeframe is the one worth wiring live.</div>`;
}
function perfCard(label,val,cls,tip){
  const t=tip?` data-tip="${esc(tip)}" style="cursor:help"`:'';
  return `<div class="perfcard ${cls}"${t}><div class="pcval">${val}</div><div class="pclab">${label}</div></div>`;
}
function tpRatesHtml(tp_rates){
  if(!tp_rates||!tp_rates.length) return '<span style="color:var(--dim2)">—</span>';
  return tp_rates.map(t=>{
    const cls=t.rate>=60?'tpr-hi':t.rate>=35?'tpr-mid':'tpr-lo';
    return `<span class="tpr ${cls}" data-tip="${t.n} of these trades reached TP${t.tp}.">TP${t.tp} ${t.rate}%</span>`;
  }).join('');
}
function renderPerf(){
  const perf=(lastData&&lastData.perf)||null;
  const cards=document.getElementById('perfCards'); if(!cards) return;
  const rows=document.getElementById('perfrows'), trrows=document.getElementById('perftrrows');
  const meta=document.getElementById('perfMeta'), emp=document.getElementById('perfempty');
  const o=(perf&&perf.overall)||{};
  const hasData=(o.n||0)>0;
  if(emp) emp.style.display=hasData?'none':'block';
  if(meta) meta.textContent=`${(perf&&perf.version)?('['+perf.version+'] · '):''}${(perf&&perf.active)||0} live · ${(perf&&perf.pending)||0} waiting for entry · ${(perf&&perf.missed)||0} missed · ${o.n||0} resolved this version${perf&&perf.upstash?' · durable storage on':' · in-memory (add Upstash to persist)'}`;
  const wr=o.winrate, exp=o.exp, tot=o.sumR;
  const unreal=(perf&&perf.open_unreal_R)||0, comb=(perf&&perf.combined_R), mtmN=(perf&&perf.mtm_n)||0;
  cards.innerHTML=
     perfCard('Win rate', wr==null?'—':wr+'%', wr==null?'':(wr>=50?'pcg':wr>=40?'':'pcb'))
    +perfCard('Expectancy', exp==null?'—':(exp>0?'+':'')+exp+'R', exp==null?'':(exp>0?'pcg':'pcb'))
    +perfCard('Total R (resolved)', tot==null?'—':(tot>0?'+':'')+tot+'R', tot==null?'':(tot>0?'pcg':tot<0?'pcb':''))
    +perfCard('Open — unrealized', (unreal>0?'+':'')+unreal+'R', unreal>0?'pcg':unreal<0?'pcb':'', `Live mark-to-market of all ${mtmN} filled open trades right now (banked TPs + the open remainder at the current price). Updates every few seconds.`)
    +perfCard('Net (all, live)', comb==null?'—':(comb>0?'+':'')+comb+'R', comb>0?'pcg':comb<0?'pcb':'', 'Resolved Total R + the live unrealized R of open trades — the whole book, marked to market right now.')
    +perfCard('Open now', (perf&&perf.open)||0, '');
  // Adaptive gate — how the quality bar has moved per board off live results.
  const gl=(lastData&&lastData.gate_learn)||null;
  const glBox=document.getElementById('gateLearnBox');
  if(glBox){
    if(!gl){ glBox.style.display='none'; }
    else{
      const names={long:'Long',short:'Short'};
      const chips=['long','short'].map(b=>{
        const s=gl[b]||{}; const cd=s.conv_delta||0, rd=s.rr_delta||0;
        const moved=(cd||rd);
        const dir=moved?(cd>0||rd>0?'lt':'lr'):'ls'; // tighter / looser / steady
        const arrow=moved?(cd>0||rd>0?'▲ tighter':'▼ looser'):'— steady';
        const adj=moved?` (conv ${cd>0?'+':''}${cd}, R:R ${rd>0?'+':''}${rd})`:'';
        return `<span class="lchip ${dir}" data-tip="${esc((s.note||'learning')+adj+' · window of recent resolved trades')}">`
              +`<b>${names[b]}</b> ${arrow}${s.exp!=null?` · ${s.exp>0?'+':''}${s.exp}R avg (n=${s.n})`:` · n=${s.n||0}`}</span>`;
      }).join('');
      glBox.innerHTML=`<div class="learnhd">🧠 Adaptive gate — the bar moves with <b>this version's</b> results: boards that lose get <b>tighter</b>, boards that win get a little <b>looser</b>. A board with no resolved trades yet in this version stays neutral (it's not judged by old logic).</div><div class="lchips">${chips}</div>`;
      glBox.style.display='block';
    }
  }
  // Live winners — open trades already past TP1 (risk-free, running), with current MTM R.
  const lw=(perf&&perf.live_winners)||[];
  const lwSub=document.getElementById('liveWinSub'), lwTbl=document.getElementById('livewintbl'), lwRows=document.getElementById('livewinrows');
  if(lwRows){ lwRows.innerHTML='';
    if(lw.length){ if(lwSub)lwSub.style.display=''; if(lwTbl)lwTbl.style.display='';
      for(const t of lw){
        const hi=(t.tps_hit&&t.tps_hit.length)?Math.max.apply(null,t.tps_hit):0;
        const when=t.ts?new Date(t.ts*1000).toLocaleString(undefined,{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}):'—';
        const cr=(t.cur_r!=null)?((t.cur_r>0?'+':'')+t.cur_r+'R'):'—';
        const tr=document.createElement('tr');
        tr.innerHTML=`<td class="dim" style="white-space:nowrap">${when}</td>`
          +`<td>${({long:'Long',short:'Short',coil:'Coil',scalp:'Scalp'})[t.board]||t.board}</td>`
          +`<td class="sym"><a href="${tvLink(t.symbol)}" target="_blank" rel="noopener">${dispSym(t.symbol)}</a></td>`
          +`<td>${sideLabel(t)}</td><td>${t.tf||'—'}</td><td>${fmtNum(t.entry)}</td>`
          +`<td class="${t.cur_r>0?'pf-good':t.cur_r<0?'pf-bad':''}"><b>${cr}</b></td>`
          +`<td class="pf-good">TP${hi} hit → stop at break-even+</td>`;
        lwRows.appendChild(tr);
      }
    } else { if(lwSub)lwSub.style.display='none'; if(lwTbl)lwTbl.style.display='none'; }
  }
  const names={long:'🟢 Long',short:'🔴 Short'};
  const brows=(perf&&perf.board_rows)||{};
  if(rows){ rows.innerHTML='';
    for(const b of ['long','short']){
      const a=((perf&&perf.by_board)||{})[b]||{};
      const wcls=a.winrate==null?'':(a.winrate>=50?'pf-good':a.winrate>=40?'':'pf-bad');
      const ecls=a.exp==null?'':(a.exp>0?'pf-good':'pf-bad');
      const bd=brows[b]||{open:[],closed:[]};
      const nOpen=(bd.open||[]).length, nClosed=(bd.closed||[]).length;
      const canExp=(nOpen+nClosed)>0;
      const tr=document.createElement('tr');
      if(canExp) tr.className='perfexp'+(perfOpenBoards.has(b)?' on':'');
      tr.innerHTML=`<td>${canExp?`<span class="cae">${perfOpenBoards.has(b)?'▾':'▸'}</span>`:''}${names[b]}`
          +(canExp?` <span class="rr">(${nOpen} open · ${nClosed} resolved)</span>`:'')+`</td><td>${a.n||0}</td>`
        +`<td class="${wcls}">${a.winrate==null?'—':a.winrate+'%'}${a.n?` <span class="rr">(${a.wins}W/${a.n-a.wins}L)</span>`:''}</td>`
        +`<td class="${ecls}">${a.exp==null?'—':(a.exp>0?'+':'')+a.exp+'R'}</td>`
        +`<td class="${a.sumR>0?'pf-good':a.sumR<0?'pf-bad':''}">${a.sumR==null?'—':(a.sumR>0?'+':'')+a.sumR+'R'}</td>`
        +`<td class="tprcell">${tpRatesHtml(a.tp_rates)}</td>`;
      if(canExp) tr.onclick=()=>{ if(perfOpenBoards.has(b))perfOpenBoards.delete(b); else perfOpenBoards.add(b); renderPerf(); };
      rows.appendChild(tr);
      if(canExp && perfOpenBoards.has(b)){
        const dtr=document.createElement('tr'); dtr.className='perfdetail';
        const td=document.createElement('td'); td.colSpan=5;
        td.innerHTML=boardTradesHtml(bd);
        dtr.appendChild(td); rows.appendChild(dtr);
      }
    }
  }
  const aorows=document.getElementById('perfaorows');
  if(aorows){ aorows.innerHTML='';
    const cell=x=>{ if(!x) return '<td class="rr">—</td>';
      const c=x.exp>0?'pf-good':'pf-bad';
      return `<td class="${c}">${(x.exp>0?'+':'')+x.exp}R <span class="rr">(${x.n})</span></td>`; };
    for(const b of ['long','short']){
      const a=((perf&&perf.by_board)||{})[b]||{};
      const ao=a.allout||[];
      const byTp=k=>ao.find(z=>z.tp===k);
      if(!(a.n||0)) continue;
      const tr=document.createElement('tr');
      tr.innerHTML=`<td>${names[b]}</td>`+cell(byTp(1))+cell(byTp(2))+cell(byTp(3));
      aorows.appendChild(tr);
    }
    if(!aorows.children.length) aorows.innerHTML='<tr><td colspan="4" class="rr">No resolved trades yet.</td></tr>';
  }
  const regrows=document.getElementById('perfregrows');
  if(regrows){ regrows.innerHTML='';
    const byreg=(perf&&perf.by_regime)||{};
    const labels={with:'✓ With regime',against:'✗ Against regime',neutral:'– Neutral'};
    let any=false;
    for(const k of ['with','against','neutral']){
      const a=byreg[k]||{}; if(!(a.n||0)) continue; any=true;
      const wcls=a.winrate==null?'':(a.winrate>=50?'pf-good':a.winrate>=40?'':'pf-bad');
      const ecls=a.exp==null?'':(a.exp>0?'pf-good':'pf-bad');
      const kc=k==='with'?'pf-good':k==='against'?'pf-bad':'';
      const tr=document.createElement('tr');
      tr.innerHTML=`<td class="${kc}"><b>${labels[k]}</b></td><td>${a.n||0}</td>`
        +`<td class="${wcls}">${a.winrate==null?'—':a.winrate+'%'}${a.n?` <span class="rr">(${a.wins}W/${a.n-a.wins}L)</span>`:''}</td>`
        +`<td class="${ecls}">${a.exp==null?'—':(a.exp>0?'+':'')+a.exp+'R'}</td>`;
      regrows.appendChild(tr);
    }
    if(!any) regrows.innerHTML='<tr><td colspan="4" class="rr">No regime-tagged trades resolved yet — this fills in over the next scans.</td></tr>';
  }
  const verrows=document.getElementById('perfverrows');
  if(verrows){ verrows.innerHTML='';
    const vers=(perf&&perf.by_version)||[];
    for(const a of vers){
      const wcls=a.winrate==null?'':(a.winrate>=50?'pf-good':a.winrate>=40?'':'pf-bad');
      const tr=document.createElement('tr');
      tr.innerHTML=`<td>${a.current?'★ ':''}${a.ver}${a.current?' <span class="rr">(current)</span>':''}</td>`
        +`<td>${a.n||0}</td>`
        +`<td class="${wcls}">${a.winrate==null?'—':a.winrate+'%'}</td>`
        +`<td class="${a.exp>0?'pf-good':a.exp<0?'pf-bad':''}">${a.exp==null?'—':(a.exp>0?'+':'')+a.exp+'R'}</td>`
        +`<td class="${a.sumR>0?'pf-good':a.sumR<0?'pf-bad':''}">${a.sumR==null?'—':(a.sumR>0?'+':'')+a.sumR+'R'}</td>`;
      verrows.appendChild(tr);
    }
    if(!vers.length) verrows.innerHTML='<tr><td colspan="5" class="rr">No resolved trades yet.</td></tr>';
  }
  if(trrows){ trrows.innerHTML='';
    for(const t of ((perf&&perf.recent)||[])){
      const when=t.closed_ts?new Date(t.closed_ts*1000).toLocaleString(undefined,{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}):'—';
      const tr=document.createElement('tr');
      tr.innerHTML=`<td style="color:var(--dim);white-space:nowrap">${when}</td>`
        +`<td>${({long:'Long',short:'Short',coil:'Coil',scalp:'Scalp'})[t.board]||t.board} ${regimeBadge(t)}</td>`
        +`<td class="sym"><div class="symbox"><a href="${tvLink(t.symbol)}" target="_blank" rel="noopener">${dispSym(t.symbol)}</a></div></td>`
        +`<td>${sideLabel(t)}</td>`
        +`<td>${fmtNum(t.entry)}</td><td>${fmtNum(t.stop)}</td><td>${reachedHtml(t)}</td>`
        +`<td>${perfStatusHtml(t)}</td>`;
      trrows.appendChild(tr);
    }
  }
}
// ---- Market context tab ----
function mcVerdictClass(v){ return v==='bullish'||v==='favorable'||v==='risk-on'?'mc-bull'
  : v==='bearish'||v==='avoid'||v==='risk-off'?'mc-bear':'mc-mid'; }
function mcBiasPill(b){ const c=b==='bull'?'mc-bull':b==='bear'?'mc-bear':'mc-mid';
  const t=b==='bull'?'Bullish':b==='bear'?'Bearish':'Neutral'; return `<span class="mcpill ${c}">${t}</span>`; }
function signed(x,suf){ if(x==null) return '—'; return (x>0?'+':'')+x+(suf||''); }
function renderMarket(){
  const body=document.getElementById('marketBody'); if(!body) return;
  const mc=(lastData&&lastData.market_context)||null;
  const emp=document.getElementById('marketempty');
  const meta=document.getElementById('marketMeta');
  if(!mc||(!mc.btc&&!mc.alts)){ if(emp) emp.style.display='block'; body.innerHTML=''; return; }
  if(emp) emp.style.display='none';
  if(meta) meta.textContent = mc.asof? ('as of '+ago(mc.asof)) : '';
  const day=mc.day||{}, week=mc.week||{}, btc=mc.btc||{}, alts=mc.alts||{};
  // Headline verdict cards (day + week)
  const headCard=(label,o)=>{
    const cls=mcVerdictClass(o.longs);
    return `<div class="mc-head ${cls}">
      <div class="mc-head-lab">${label}</div>
      <div class="mc-head-line">${o.headline||'—'}</div>
      <div class="mc-head-row">
        <span>Longs: <b class="${mcVerdictClass(o.longs)}">${(o.longs||'—').toUpperCase()}</b></span>
        <span>Shorts: <b class="${mcVerdictClass(o.shorts)}">${(o.shorts||'—').toUpperCase()}</b></span>
      </div></div>`;
  };
  let h=`<div class="mc-heads">${headCard('📅 Today',day)}${headCard('🗓️ This week',week)}</div>`;
  // Intraday "right now" lean — the fastest read (BTC 15m/1h + 4h alt breadth).
  const rnow=mc.now||{};
  if(rnow.lean){
    const nc=rnow.lean==='long'?'mc-bull':rnow.lean==='short'?'mc-bear':'mc-mid';
    const nt=rnow.lean==='long'?'lean LONG':rnow.lean==='short'?'lean SHORT':'no clear lean';
    h+=`<div class="mc-now ${nc}" data-tip="A fast 'right now' lean from the quickest reads — BTC 15m/1h trend blended with 4h alt breadth. Shorter-horizon than the Today card; use it to time intraday entries.">⏱️ Right now — <b>${nt}</b> · ${rnow.note||''}</div>`;
  }
  const vr=mc.vol_regime||null;
  if(vr&&vr.state){
    const vc=vr.state==='expansion'?'mc-bull':vr.state==='compression'?'mc-mid':'';
    const vi=vr.state==='expansion'?'📈':vr.state==='compression'?'🪤':'➖';
    h+=`<div class="mc-now ${vc}" data-tip="${esc(vr.note||'')} Apex tilts the boards accordingly — favouring bounces in compression and breakouts/trend in expansion.">${vi} Volatility — <b>${vr.state.toUpperCase()}</b> · BTC 4h ATR at the ${vr.atr_pctile}th percentile (${vr.atr_pct}%)</div>`;
  }
  // BTC section
  h+=`<div class="perfsub">₿ Bitcoin — multi-timeframe trend</div>`;
  h+=`<div class="mc-btcbar"><div>Overall BTC read: <b class="${mcVerdictClass(btc.verdict)}">${(btc.verdict||'—').toUpperCase()}</b>`
    +`${btc.price?` <span class="rr">· BTC ${fmtNum(btc.price)}</span>`:''}</div>${mcBar(btc.score)}</div>`;
  h+=`<table id="mcbtctbl"><thead><tr>
      <th data-tip="Chart timeframe.">TF</th>
      <th data-tip="Bull/bear/neutral read from EMA side, EMA stack and RSI on this timeframe.">Bias</th>
      <th data-tip="How far price sits above (+) or below (−) its long EMA on this timeframe.">Price vs EMA</th>
      <th data-tip="20 / long EMA arrangement — 'stacked up' = clean uptrend, 'stacked down' = clean downtrend.">EMA stack</th>
      <th data-tip="RSI(14) on this timeframe.">RSI</th>
      <th data-tip="Bollinger-band squeeze percentile — high = coiled, a big move may be near.">Squeeze</th>
      <th data-tip="Last closed candle % change.">Last</th>
    </tr></thead><tbody>`;
  for(const r of (btc.tfs||[])){
    h+=`<tr><td><b>${r.tf}</b></td><td>${mcBiasPill(r.bias)}</td>`
      +`<td class="${(r.px_vs_ema||0)>=0?'pf-good':'pf-bad'}">${signed(r.px_vs_ema,'%')}</td>`
      +`<td>${r.ema_stack||'—'}</td>`
      +`<td>${r.rsi!=null?r.rsi:'—'}</td>`
      +`<td>${r.squeeze!=null?(r.squeeze+(r.squeeze>=70?' 🔥':'')):'—'}</td>`
      +`<td class="${(r.chg||0)>=0?'pf-good':'pf-bad'}">${signed(r.chg,'%')}</td></tr>`;
  }
  h+=`</tbody></table>`;
  // Alts section
  if(alts&&alts.n){
    h+=`<div class="perfsub">🪙 Alt-market breadth (majors, daily)</div>`;
    h+=`<div class="mc-cards">
      ${mcStat('Above 200-EMA', alts.pct_above_200ema+'%', alts.pct_above_200ema>=60?'mc-bull':alts.pct_above_200ema<=40?'mc-bear':'mc-mid')}
      ${mcStat('In a clean uptrend', alts.pct_stacked_up+'%','')}
      ${mcStat('Up / Down today', alts.up+' / '+alts.down, alts.up>alts.down?'mc-bull':alts.up<alts.down?'mc-bear':'mc-mid')}
      ${mcStat('Avg 24h change', signed(alts.avg_chg,'%'), (alts.avg_chg||0)>=0?'mc-bull':'mc-bear')}
      ${mcStat('Breadth', (alts.verdict||'—').toUpperCase(), mcVerdictClass(alts.verdict))}
    </div>`;
    h+=`<table id="mcalttbl"><thead><tr><th>Coin</th><th>Bias</th><th data-tip="Price vs its 200-day EMA.">vs 200-EMA</th><th>RSI</th><th data-tip="Squeeze percentile — high = coiling.">Squeeze</th><th>24h</th></tr></thead><tbody>`;
    for(const r of (alts.members||[])){
      h+=`<tr><td class="sym"><a href="${tvLink(r.symbol)}" target="_blank" rel="noopener">${dispSym(r.symbol)}</a></td>`
        +`<td>${mcBiasPill(r.bias)}</td>`
        +`<td class="${(r.px_vs_ema||0)>=0?'pf-good':'pf-bad'}">${signed(r.px_vs_ema,'%')}</td>`
        +`<td>${r.rsi!=null?r.rsi:'—'}</td>`
        +`<td>${r.squeeze!=null?(r.squeeze+(r.squeeze>=70?' 🔥':'')):'—'}</td>`
        +`<td class="${(r.chg||0)>=0?'pf-good':'pf-bad'}">${signed(r.chg,'%')}</td></tr>`;
    }
    h+=`</tbody></table>`;
  }
  // Intraday alt breadth (4h) — how alts are participating in the last few hours.
  const a4=mc.alts_4h||null;
  if(a4&&a4.n){
    h+=`<div class="perfsub">🪙 Alt breadth — intraday (4h)</div>`;
    h+=`<div class="mc-cards">
      ${mcStat('Above 200-EMA (4h)', a4.pct_above_200ema+'%', a4.pct_above_200ema>=60?'mc-bull':a4.pct_above_200ema<=40?'mc-bear':'mc-mid')}
      ${mcStat('Green on the 4h', a4.pct_up+'%', a4.pct_up>=60?'mc-bull':a4.pct_up<=40?'mc-bear':'mc-mid')}
      ${mcStat('Up / Down (4h)', a4.up+' / '+a4.down, a4.up>a4.down?'mc-bull':a4.up<a4.down?'mc-bear':'mc-mid')}
      ${mcStat('Avg 4h change', signed(a4.avg_chg,'%'), (a4.avg_chg||0)>=0?'mc-bull':'mc-bear')}
      ${mcStat('Intraday breadth', (a4.verdict||'—').toUpperCase(), mcVerdictClass(a4.verdict))}
    </div>`;
  }
  body.innerHTML=h;
}
function mcBar(score){ // score -1..+1 -> a left/right gauge
  const s=Math.max(-1,Math.min(1,score||0)); const pct=Math.round((s+1)/2*100);
  const c=s>=0.3?'var(--accent)':s<=-0.3?'#f85149':'#f0b429';
  return `<div class="mcgauge" data-tip="Aggregate BTC trend score across timeframes (−1 fully bearish → +1 fully bullish)."><div class="mcgauge-fill" style="width:${pct}%;background:${c}"></div><div class="mcgauge-mid"></div></div>`;
}
function mcStat(lab,val,cls){ return `<div class="mc-stat"><div class="mc-stat-v ${cls||''}">${val}</div><div class="mc-stat-l">${lab}</div></div>`; }

// ---- History tab ----
let histData=null, histLastFetch=0;
async function loadHistory(){
  const now=Date.now();
  if(histData && now-histLastFetch<20000){ renderHistory(); return; }
  try{ const r=await fetch('/history',{cache:'no-store'}); histData=await r.json(); histLastFetch=now; }
  catch(e){ /* keep old */ }
  renderHistory();
}
// Minimal inline multi-line SVG chart. series=[{name,color,pts:[[t,v]],min,max}], shared x.
function svgChart(series, opt){
  opt=opt||{}; const W=opt.w||860, H=opt.h||190, padL=38, padR=12, padT=12, padB=22;
  const all=[].concat(...series.map(s=>s.pts));
  if(!all.length) return '<div class="bt-empty">No data yet.</div>';
  const xs=all.map(p=>p[0]); const xmin=Math.min(...xs), xmax=Math.max(...xs)||xmin+1;
  const ymin=opt.ymin!=null?opt.ymin:Math.min(...all.map(p=>p[1]));
  const ymax=opt.ymax!=null?opt.ymax:Math.max(...all.map(p=>p[1]));
  const yr=(ymax-ymin)||1;
  const X=t=>padL+(xmax===xmin?0:(t-xmin)/(xmax-xmin))*(W-padL-padR);
  const Y=v=>padT+(1-(v-ymin)/yr)*(H-padT-padB);
  let g=`<svg viewBox="0 0 ${W} ${H}" class="histsvg" preserveAspectRatio="none">`;
  // zero line if range crosses 0
  if(ymin<0&&ymax>0){ const zy=Y(0); g+=`<line x1="${padL}" y1="${zy}" x2="${W-padR}" y2="${zy}" class="hz"/>`; }
  // gridlines top/bottom
  g+=`<line x1="${padL}" y1="${Y(ymax)}" x2="${W-padR}" y2="${Y(ymax)}" class="hg"/>`;
  g+=`<line x1="${padL}" y1="${Y(ymin)}" x2="${W-padR}" y2="${Y(ymin)}" class="hg"/>`;
  g+=`<text x="2" y="${Y(ymax)+4}" class="hax">${opt.fmtY?opt.fmtY(ymax):ymax.toFixed(1)}</text>`;
  g+=`<text x="2" y="${Y(ymin)+4}" class="hax">${opt.fmtY?opt.fmtY(ymin):ymin.toFixed(1)}</text>`;
  for(const s of series){
    if(!s.pts.length) continue;
    const d=s.pts.map((p,i)=>`${i?'L':'M'}${X(p[0]).toFixed(1)} ${Y(p[1]).toFixed(1)}`).join(' ');
    g+=`<path d="${d}" fill="none" stroke="${s.color}" stroke-width="1.8"/>`;
  }
  // x labels: first + last date
  const df=t=>new Date(t*1000).toLocaleDateString(undefined,{month:'short',day:'numeric'});
  g+=`<text x="${padL}" y="${H-6}" class="hax">${df(xmin)}</text>`;
  g+=`<text x="${W-padR}" y="${H-6}" class="hax" text-anchor="end">${df(xmax)}</text>`;
  g+='</svg>';
  const leg=series.map(s=>`<span class="hleg"><i style="background:${s.color}"></i>${s.name}</span>`).join('');
  return `<div class="histleg">${leg}</div>${g}`;
}
function renderHistory(){
  const body=document.getElementById('histBody'); if(!body) return;
  const h=histData; const emp=document.getElementById('histempty'); const meta=document.getElementById('histMeta');
  if(!h||!((h.market||[]).length)){ if(emp) emp.style.display='block'; body.innerHTML=''; if(meta) meta.textContent=''; return; }
  if(emp) emp.style.display='none';
  const mkt=h.market||[];
  const now=mkt[mkt.length-1]||{};
  const span=h.span;
  const hrs=span?((span[1]-span[0])/3600):0;
  if(meta){ meta.textContent=(mkt.length+' snapshots'+(span?' · over '+(hrs<1?Math.round(hrs*60)+' min':hrs.toFixed(1)+'h'):'')+(h.upstash?' · durable ✓':' · in-memory')); }
  let out='';
  // ---- RIGHT NOW band: the current regime in plain language ----
  const capV=v=>(v||'—').charAt(0).toUpperCase()+(v||'—').slice(1);
  out+=`<div class="perfsub">Right now — the current market read</div>`;
  const btcDesc = now.btc_v==='bullish'?'Big-picture uptrend — the tide is with longs'
                : now.btc_v==='bearish'?'Big-picture downtrend — the tide is with shorts'
                : 'No strong direction across 15m→1W — chop';
  const brdDesc = now.alt_above>=60?'Most majors are healthy (above their key averages) — risk-on'
                : now.alt_above<=40?'Most majors are weak (below their key averages) — risk-off'
                : 'Majors are split — no clear risk-on/off';
  const dayDesc = now.day_longs==='favorable'?'Intraday: a good day to hunt longs'
                : now.day_longs==='avoid'?'Intraday: a better day to hunt shorts'
                : 'Intraday: no clear edge — be picky';
  const wkDesc  = now.week_longs==='favorable'?'Swing: conditions favor longs this week'
                : now.week_longs==='avoid'?'Swing: conditions favor shorts this week'
                : 'Swing: mixed — no clear weekly bias';
  out+=`<div class="mc-cards">
    ${histStat('BTC trend', capV(now.btc_v), btcDesc, mcVerdictClass(now.btc_v==='bullish'?'favorable':now.btc_v==='bearish'?'avoid':''))}
    ${histStat('Alt breadth', (now.alt_above!=null?now.alt_above+'%':'—'), brdDesc, now.alt_above>=60?'mc-bull':now.alt_above<=40?'mc-bear':'mc-mid')}
    ${histStat('Today → longs', (now.day_longs||'—').toUpperCase(), dayDesc, mcVerdictClass(now.day_longs))}
    ${histStat('This week → longs', (now.week_longs||'—').toUpperCase(), wkDesc, mcVerdictClass(now.week_longs))}
  </div>`;
  out+=`<div class="histnote">${histSentence(now)}</div>`;
  // Explainer
  out+=`<details class="perfhelp"><summary>What am I looking at on this tab?</summary><div class="perfhelpbody">
    <p>Apex saves a <b>snapshot every scan</b> (~every 10 min). This tab is its memory of how the market has moved, so you can see change over time instead of just this instant:</p>
    <p><b>Right now</b> = the latest reading — is BTC trending, how many alts are healthy, and whether it's a good day/week to look for longs.</p>
    <p><b>Regime chart</b> = those readings plotted over time. The green line is BTC's trend score (−1 fully bearish → +1 fully bullish); the blue line is alt breadth (how much of the majors are above their key averages). Watch for the lines crossing zero — that's the market flipping.</p>
    <p><b>Signal leaders</b> = which coins keep showing up at the top of the boards across scans — the names worth watching.</p>
    <p><b>Open interest / funding</b> = the derivatives path of the majors from Coinalyze (needs the API key).</p>
    <p>It looks sparse at first — it fills out as more scans record. A few hours in, the chart becomes readable.</p>
  </div></details>`;
  // 1) Regime chart: BTC trend score + alt breadth (mapped to -1..1)
  const btcPts=mkt.filter(p=>p.btc!=null).map(p=>[p.t,p.btc]);
  const brdPts=mkt.filter(p=>p.alt_above!=null).map(p=>[p.t,(p.alt_above-50)/50]);
  out+=`<div class="perfsub">Market regime over time — BTC trend & alt breadth</div>`;
  if(mkt.length<8) out+=`<div class="histnote">📈 Still collecting — ${mkt.length} snapshot${mkt.length===1?'':'s'} so far. The lines below fill in and become meaningful after a few hours of scans. Above 0 = bullish, below 0 = bearish.</div>`;
  out+=svgChart([{name:'BTC trend',color:'#3fb950',pts:btcPts},{name:'Alt breadth',color:'#58a6ff',pts:brdPts}],
                {ymin:-1,ymax:1,fmtY:v=>v>0?'+'+v.toFixed(1):v.toFixed(1)});
  // BTC price chart
  const pxPts=mkt.filter(p=>p.btc_px!=null).map(p=>[p.t,p.btc_px]);
  if(pxPts.length>1){ out+=`<div class="perfsub">BTC price</div>`+svgChart([{name:'BTC',color:'#f0b429',pts:pxPts}],{fmtY:v=>fmtNum(v)}); }
  // 2) Recent snapshots table (verdict flips)
  out+=`<div class="perfsub">Recent snapshots — each scan's read (newest first)</div>`;
  out+=`<table class="bt"><thead><tr><th>When</th><th>BTC</th><th>Day → longs</th><th>Week → longs</th><th>Alt breadth</th><th>Alts 24h</th></tr></thead><tbody>`;
  const vpill=v=>`<span class="${mcVerdictClass(v)}">${(v||'—')}</span>`;
  for(const p of mkt.slice().reverse().slice(0,24)){
    const when=new Date(p.t*1000).toLocaleString(undefined,{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'});
    out+=`<tr><td class="dim">${when}</td><td>${vpill(p.btc_v)} <span class="rr">${signed(p.btc!=null?+p.btc.toFixed(2):null,'')}</span></td>`
      +`<td class="${mcVerdictClass(p.day_longs)}">${(p.day_longs||'—').toUpperCase()}</td>`
      +`<td class="${mcVerdictClass(p.week_longs)}">${(p.week_longs||'—').toUpperCase()}</td>`
      +`<td>${p.alt_above!=null?p.alt_above+'%':'—'} ${vpill(p.alt_v)}</td>`
      +`<td class="${(p.alt_chg||0)>=0?'pf-good':'pf-bad'}">${signed(p.alt_chg,'%')}</td></tr>`;
  }
  out+=`</tbody></table>`;
  // 3) Signal leaders — how often each coin topped the boards
  const sigs=h.signals||[];
  if(sigs.length){
    const tally={};
    for(const s of sigs){ for(const side of ['longs','shorts','coils']){ for(const it of (s[side]||[])){
      const k=it.s; tally[k]=tally[k]||{s:k,long:0,short:0,coil:0}; tally[k][side.slice(0,-1)]++; } } }
    const all=Object.values(tally);
    const lead=all.map(x=>({...x,tot:x.long+x.short+x.coil})).sort((a,b)=>b.tot-a.tot).slice(0,14);
    out+=`<div class="perfsub">Signal leaders — coins that keep topping the boards (last ${sigs.length} scans)</div>`;
    out+=`<div class="histnote">The names that repeatedly rank highest — the ones worth watching. A high long count = persistently strong; high short count = persistently weak. Expand a trade type below to rank the leaders for just that board.</div>`;
    out+=`<table class="bt"><thead><tr><th>Coin</th><th># times as top long</th><th># as top short</th><th># as top coil</th></tr></thead><tbody>`;
    for(const x of lead){ out+=`<tr><td class="sym"><a href="${tvLink(x.s)}" target="_blank" rel="noopener">${dispSym(x.s)}</a></td>`
      +`<td class="pf-good">${x.long||'—'}</td><td class="pf-bad">${x.short||'—'}</td><td>${x.coil||'—'}</td></tr>`; }
    out+=`</tbody></table>`;
    // Expand by trade type — a ranked leaderboard for each board on its own.
    const typeBlock=(key,label,emoji,cls)=>{
      const rows=all.filter(x=>x[key]>0).sort((a,b)=>b[key]-a[key]).slice(0,12);
      if(!rows.length) return '';
      const body=rows.map((x,i)=>`<tr><td class="rnk">${i+1}</td>`
        +`<td class="sym"><a href="${tvLink(x.s)}" target="_blank" rel="noopener">${dispSym(x.s)}</a></td>`
        +`<td class="${cls}">${x[key]}× top ${label.toLowerCase()}</td></tr>`).join('');
      return `<details class="histtype"><summary>${emoji} ${label} — ${rows.length} coin${rows.length===1?'':'s'} ranked</summary>`
        +`<table class="bt"><thead><tr><th>#</th><th>Coin</th><th>Times topping this board</th></tr></thead><tbody>${body}</tbody></table></details>`;
    };
    out+=`<div class="histtypes">`
      +typeBlock('long','Longs','🏆','pf-good')
      +typeBlock('short','Shorts','🔻','pf-bad')
      +typeBlock('coil','Coiled','🚀','')
      +`</div>`;
  }
  // 4) Per-coin derivatives history (Coinalyze)
  const coins=h.coins||{};
  const cks=Object.keys(coins);
  out+=`<div class="perfsub">Open interest, funding & price — majors (from Coinalyze)</div>`;
  if(!cks.length){
    if(h.coinalyze){
      out+=`<div class="histnote">🔑 Coinalyze key detected — but no derivatives have come back yet. This backloads on the <b>next completed scan</b> (it pulls ~7 days of OI/funding for the majors), so check back after a scan or two. If it stays empty, the key may be invalid or Coinalyze may be rate-limiting.</div>`;
    } else {
      out+=`<div class="bt-empty">No <b>COINALYZE_API_KEY</b> detected. Add it in Render → Environment and Apex will backload ~7 days of OI/funding for the majors and keep appending.</div>`;
    }
  } else {
    out+=`<table class="bt"><thead><tr><th>Coin</th><th>OI now</th><th>OI 7d</th><th>Funding</th><th>Price</th><th>Price 7d</th><th>OI trend</th></tr></thead><tbody>`;
    for(const k of cks){
      const c=coins[k], s=c.summary||{};
      const spark=(c.oi||[]).length>2?svgSpark(c.oi.map(p=>p[1]),'#58a6ff'):'—';
      out+=`<tr><td class="sym"><a href="${tvLink(k+'USDT')}" target="_blank" rel="noopener">${k}</a></td>`
        +`<td>${s.oi_now!=null?fmtNum(s.oi_now):'—'}</td>`
        +`<td class="${(s.oi_chg_7d||0)>=0?'pf-good':'pf-bad'}">${signed(s.oi_chg_7d,'%')}</td>`
        +`<td class="${(s.funding||0)>=0?'pf-good':'pf-bad'}">${s.funding!=null?(s.funding*100).toFixed(3)+'%':'—'}</td>`
        +`<td>${s.price!=null?fmtNum(s.price):'—'}</td>`
        +`<td class="${(s.price_chg_7d||0)>=0?'pf-good':'pf-bad'}">${signed(s.price_chg_7d,'%')}</td>`
        +`<td>${spark}</td></tr>`;
    }
    out+=`</tbody></table>`;
  }
  body.innerHTML=out;
}
// A "Right now" card: big value, label, and a plain-language description line.
function histStat(label,val,desc,cls){
  return `<div class="mc-stat histcard"><div class="mc-stat-v ${cls||''}">${val}</div>`
    +`<div class="mc-stat-l">${label}</div><div class="histcard-desc">${desc||''}</div></div>`;
}
// Plain-language one-liner describing the current regime.
function histSentence(p){
  if(!p||!p.btc_v) return 'Waiting for the first reading…';
  const btc = p.btc_v==='bullish'?'trending up':p.btc_v==='bearish'?'trending down':'range-bound / mixed';
  const brd = p.alt_above>=60?'most alts are healthy (risk-on)':p.alt_above<=40?'most alts are weak (risk-off)':'alts are mixed';
  const day = (p.day_longs==='favorable')?'a good day to look for longs'
            : (p.day_longs==='avoid')?'a better day to look for shorts'
            : 'no clear intraday edge — be selective';
  return `Bitcoin is <b>${btc}</b> and ${brd} (${p.alt_above!=null?p.alt_above+'%':'—'} above key averages). Net: <b>${day}</b>.`;
}
// Tiny sparkline for a value array.
function svgSpark(vals,color){
  const W=90,H=22,pad=2; const mn=Math.min(...vals),mx=Math.max(...vals),r=(mx-mn)||1;
  const X=i=>pad+i/(vals.length-1)*(W-2*pad), Y=v=>pad+(1-(v-mn)/r)*(H-2*pad);
  const d=vals.map((v,i)=>`${i?'L':'M'}${X(i).toFixed(1)} ${Y(v).toFixed(1)}`).join(' ');
  return `<svg viewBox="0 0 ${W} ${H}" class="spark"><path d="${d}" fill="none" stroke="${color}" stroke-width="1.4"/></svg>`;
}
const perfOpenBoards=new Set();
// Expanded per-board trade lists: waiting-for-entry + live (open), then resolved.
function boardTradesHtml(bd){
  const openR=(bd.open||[]), closedR=(bd.closed||[]);
  const fmtWhen=ts=>ts?new Date(ts*1000).toLocaleString(undefined,{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}):'—';
  const tgtOf=t=>((t.tps||[]).length?fmtNum(t.tps[t.tps.length-1].lvl):'—');
  const statePill=t=>{
    if(t.state==='pending') return `<span class="bt-wait" data-tip="Waiting for price to reach the entry before the trade starts.">⏳ waiting for entry</span>`;
    const hi=(t.tps_hit&&t.tps_hit.length)?Math.max.apply(null,t.tps_hit):0;
    return hi?`<span class="pf-good">live · TP${hi} hit, stop at BE+</span>`:`<span class="bt-live">● live · watching</span>`;
  };
  let h='<div class="bt-wrap">';
  h+=`<div class="bt-h">Open (${openR.length})</div>`;
  if(openR.length){
    h+='<table class="bt"><thead><tr><th>Added</th><th>Symbol</th><th>Side</th><th>TF</th><th>Entry</th><th>Stop</th><th>Target</th><th>Status</th></tr></thead><tbody>';
    for(const t of openR){
      h+=`<tr><td class="dim">${fmtWhen(t.ts)}</td>`
        +`<td class="sym"><a href="${tvLink(t.symbol)}" target="_blank" rel="noopener">${dispSym(t.symbol)}</a></td>`
        +`<td>${leanPill(t.side==='short'?'bearish':'bullish')}</td><td>${t.tf||'—'}</td>`
        +`<td>${fmtNum(t.entry)}</td><td>${fmtNum(t.stop)}</td><td>${tgtLadderHtml(t)}</td><td>${statePill(t)}</td></tr>`;
    }
    h+='</tbody></table>';
  } else h+='<div class="bt-empty">No open setups on this board right now.</div>';
  h+=`<div class="bt-h">Resolved (${closedR.length})</div>`;
  if(closedR.length){
    h+='<table class="bt"><thead><tr><th>Closed</th><th>Symbol</th><th>Side</th><th>TF</th><th>Entry</th><th>Stop</th><th>Reached</th><th>Result</th></tr></thead><tbody>';
    for(const t of closedR){
      h+=`<tr><td class="dim">${fmtWhen(t.closed_ts)}</td>`
        +`<td class="sym"><a href="${tvLink(t.symbol)}" target="_blank" rel="noopener">${dispSym(t.symbol)}</a></td>`
        +`<td>${sideLabel(t)} ${regimeBadge(t)}</td><td>${t.tf||'—'}</td>`
        +`<td>${fmtNum(t.entry)}</td><td>${fmtNum(t.stop)}</td><td>${reachedHtml(t)}</td><td>${perfStatusHtml(t)}</td></tr>`;
    }
    h+='</tbody></table>';
  } else h+='<div class="bt-empty">Nothing resolved on this board yet.</div>';
  h+='</div>';
  return h;
}
// Explicit Long/Short label (not just a colour).
function sideLabel(t){ return (t.side==='short')
  ? '<span class="pf-bad" style="font-weight:700">SHORT ▼</span>'
  : '<span class="pf-good" style="font-weight:700">LONG ▲</span>'; }
// The final level price reached before the trade closed (max TP hit, else stop/entry),
// with a hover explaining WHY that level was chosen and what timeframe the trade was on.
function reachedHtml(t){
  const tfnote = t.tf ? ` · trade based on the ${t.tf} timeframe` : '';
  const hi=(t.tps_hit&&t.tps_hit.length)?Math.max.apply(null,t.tps_hit):0;
  if(hi>0){ const tp=(t.tps&&t.tps[hi-1])||{}; const lvl=tp.lvl!=null?fmtNum(tp.lvl):'';
    const tip=esc(`Reached TP${hi}${lvl?' at '+lvl:''}${tp.basis?' — '+tp.basis:''}${tfnote}`);
    return `<span class="pf-good" data-tip="${tip}">TP${hi}${lvl?' @ '+lvl:''}</span>`; }
  if(t.status==='loss') return `<span class="pf-bad" data-tip="${esc('Stopped out at '+fmtNum(t.stop)+' before any target'+tfnote)}">Stop @ ${fmtNum(t.stop)}</span>`;
  if(t.status==='be') return `<span class="pf-be" data-tip="${esc('TP1 hit, then price returned to the break-even stop (entry) — banked the TP1 partial'+tfnote)}">Break-even @ ${fmtNum(t.entry)}</span>`;
  if(t.status==='missed') return `<span class="pf-miss" data-tip="Price ran to the first target without ever filling the entry — no trade taken.">Never filled</span>`;
  if(t.status==='expired') return `<span class="pf-miss" data-tip="The entry never filled within the allotted window — cancelled, no trade.">Never filled</span>`;
  return '—';
}
// A target-ladder cell whose hover lists every TP with its price, R:R and 'why'.
function tgtLadderHtml(t){
  const tps=(t.tps||[]);
  if(!tps.length) return '—';
  const last=tps[tps.length-1];
  const rows=tps.map((x,i)=>`TP${i+1} ${fmtNum(x.lvl)}${x.rr!=null?' (R '+(+x.rr).toFixed(1)+')':''}${x.basis?' — '+x.basis:''}`).join('  ·  ');
  const tip=esc(`Target ladder${t.tf?' ('+t.tf+' timeframe)':''} — `+rows);
  return `<span data-tip="${tip}">${fmtNum(last.lvl)}${tps.length>1?' <span class="rr">(+'+(tps.length-1)+')</span>':''}</span>`;
}
// Small badge: was this setup WITH or AGAINST the market regime when it was taken?
function regimeBadge(t){
  const r=t.regime;
  if(r==='with') return `<span class="rgb rgb-w" data-tip="Taken WITH the market regime (the tape favoured this side).">✓ with regime</span>`;
  if(r==='against') return `<span class="rgb rgb-a" data-tip="Taken AGAINST the market regime (fighting the tape) — historically the weaker bucket.">✗ against regime</span>`;
  return '';
}
function perfStatusHtml(t){
  const s=t.status, r=(t.r!=null)?(+t.r):null;
  if(s==='win')   return `<b class="pf-good">WIN ${r!=null?(r>0?'+':'')+r.toFixed(1)+'R':''}</b>`;
  if(s==='be')    return `<b class="pf-be">BREAK-EVEN ${r!=null?(r>0?'+':'')+r.toFixed(2)+'R':''}</b>`;
  if(s==='loss')  return `<b class="pf-bad">LOSS −1R</b>`;
  if(s==='missed')return `<span class="pf-miss" data-tip="Price never filled the recommended entry, then ran to target without us — no trade taken.">MISSED (no fill)</span>`;
  if(s==='expired')return `<span class="pf-miss" data-tip="Entry never filled in the allotted window — cancelled, counts as no trade.">EXPIRED</span>`;
  return `<span class="rr">${s||'—'}</span>`;
}
function renderCalls(){
  const perf=(lastData&&lastData.perf)||null;
  const cards=document.getElementById('callsCards'); if(!cards) return;
  const openrows=document.getElementById('callsopenrows'), closedrows=document.getElementById('callsclosedrows');
  const meta=document.getElementById('callsMeta'), emp=document.getElementById('callsempty');
  const a=((perf&&perf.by_board)||{}).call||{};
  const opens=(perf&&perf.calls_open)||[], closed=(perf&&perf.calls_closed)||[];
  const has=opens.length||closed.length;
  if(emp) emp.style.display=has?'none':'block';
  if(meta) meta.textContent=`${opens.length} open · ${a.n||0} resolved`;
  cards.innerHTML=
     perfCard('Win rate', a.winrate==null?'—':a.winrate+'%', a.winrate==null?'':(a.winrate>=50?'pcg':a.winrate>=40?'':'pcb'))
    +perfCard('Expectancy', a.exp==null?'—':(a.exp>0?'+':'')+a.exp+'R', a.exp==null?'':(a.exp>0?'pcg':'pcb'))
    +perfCard('Total R', a.sumR==null?'—':(a.sumR>0?'+':'')+a.sumR+'R', a.sumR>0?'pcg':a.sumR<0?'pcb':'')
    +perfCard('Resolved', a.n||0, '')
    +perfCard('Open now', opens.length, '')
    +`<div class="perfcard" style="flex:2"><div class="pclab" style="margin:0 0 6px">TP hit rates</div><div class="tprcell">${tpRatesHtml(a.tp_rates)}</div></div>`;
  const tgts=t=>((t.tps||[]).map(x=>fmtNum(x.lvl)).join(' · ')||'—');
  if(openrows){ openrows.innerHTML='';
    for(const t of opens){
      const when=t.ts?new Date(t.ts*1000).toLocaleString(undefined,{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}):'—';
      const st=(t.state==='pending')?'⏳ waiting for entry':((t.tps_hit&&t.tps_hit.length)?`TP${Math.max.apply(null,t.tps_hit)} hit → stop at ${t.phase>=1?'break-even+':'—'}`:'● live · watching');
      const liveR=(t.cur_r!=null)?` · <b style="color:${t.cur_r>=0?'var(--up,#3fb950)':'var(--down,#f85149)'}">${t.cur_r>=0?'+':''}${t.cur_r}R</b>`:'';
      const prog=st+liveR;
      const tr=document.createElement('tr');
      tr.innerHTML=`<td style="color:var(--dim);white-space:nowrap">${when}</td>`
        +`<td class="sym"><div class="symbox"><a href="${tvLink(t.symbol)}" target="_blank" rel="noopener">${dispSym(t.symbol)}</a></div></td>`
        +`<td>${leanPill(t.side==='short'?'bearish':'bullish')}</td><td>${t.tf||'—'}</td>`
        +`<td>${fmtNum(t.entry)}</td><td>${fmtNum(t.stop)}</td><td class="whycell">${tgts(t)}</td>`
        +`<td>${prog}</td>`;
      openrows.appendChild(tr);
    }
  }
  if(closedrows){ closedrows.innerHTML='';
    for(const t of closed){
      const when=t.closed_ts?new Date(t.closed_ts*1000).toLocaleString(undefined,{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}):'—';
      const tr=document.createElement('tr');
      tr.innerHTML=`<td style="color:var(--dim);white-space:nowrap">${when}</td>`
        +`<td class="sym"><div class="symbox"><a href="${tvLink(t.symbol)}" target="_blank" rel="noopener">${dispSym(t.symbol)}</a></div></td>`
        +`<td>${sideLabel(t)}</td><td>${t.tf||'—'}</td>`
        +`<td>${fmtNum(t.entry)}</td><td>${fmtNum(t.stop)}</td><td>${reachedHtml(t)}</td>`
        +`<td>${perfStatusHtml(t)}</td>`;
      closedrows.appendChild(tr);
    }
  }
}
// One breakout-setup cell for the Coiled row (long or short). Shows the entry and the
// R:R to the FINAL target; the hover carries the whole plan — two limit entries, the
// tight stop, a 3-TP ladder with each R:R, and a scale-out plan. ★ + colour when it's
// the recommended (lean) side.
function coilSetupCell(h, side){
  const p = side==='long'? h.plan_long : h.plan_short;
  if(!p || p.entry==null || !p.tps || !p.tps.length) return `<td>—</td>`;
  const rec = h.rec_side===side;
  const arrow = side==='long'?'▲':'▼';
  const word = side==='long'?'break above':'break below';
  const tps = p.tps;
  const tpTxt = tps.map((t,i)=> `TP${i+1} ${fmtNum(t.lvl)}${t.rr!=null?` (R ${t.rr.toFixed(1)})`:''}`).join(' · ');
  // Scale-out: bank the first tranche + move stop to break-even, hold to TP2, runner to TP3.
  const _cw=scaleWeights(tps.map(t=>t.rr)).map(x=>Math.round(x*100)+'%');
  const scale = _cw.length>=2
    ? `Scale-out: sell ${_cw[0]} at TP1 → move stop to break-even (${fmtNum(p.entry)})`
      + _cw.slice(1).map((pc,i)=>`, ${pc} at TP${i+2}`).join('') + ` (trail the runner).`
    : `Scale-out: sell 100% at TP1.`;
  const maxR = p.rr!=null? p.rr : (tps[tps.length-1].rr);
  const tip = `${side==='long'?'LONG break-up':'SHORT break-down'} plan. `
            + `Entries (limit): ${fmtNum(p.entry)} on the ${word} retest`
            + (p.entry_break!=null?`, or ${fmtNum(p.entry_break)} on the break-confirm`:'')
            + `. Stop ${fmtNum(p.stop)} — back inside the range (break failed). `
            + `Targets: ${tpTxt}. ${scale}`
            + (rec?' ★ Recommended side — matches the coil\\'s lean.':'');
  const cls = 'coilset '+(rec?(side==='long'?'coilrec-l':'coilrec-s'):'');
  return `<td class="${cls}" data-tip="${esc(tip)}">${rec?'★ ':''}${arrow} ${fmtNum(p.entry)} `
       + `<span class="rr">${maxR!=null?'R '+(+maxR).toFixed(1):''}</span></td>`;
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
  dtblatest=d.dtb_hits||[]; renderDtb();
  accumlatest=d.accum_hits||[]; renderAccum();
  siglatest=d.signal_rank||null; renderSignals();
  capitlatest=d.capit_hits||[]; renderCapit();
  microlatest=d.micro_stats||null; renderMicro();
  rdiaglatest=d.runner_diag||null; renderDiag("dtb"); renderDiag("accum");
    slatest=d.short_hits||[]; renderShorts();
    renderBestLong(); renderBestShort(); renderCoil(); renderScalp(); renderSpot(); renderPerf(); renderCalls();
    renderMarket(); renderWatch();
    if(activeTab==="history") loadHistory();
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
    { const mb=(t,tip)=>`<span class="metabit" data-tip="${tip}">${t}</span>`;
      const mkt=d.cfg.market==='futures'?'perps ⚡':'spot';
      document.getElementById("meta").innerHTML =
        mb(`${d.cfg.interval} chart`, `The candle timeframe the scanners run on — every setup is detected on this chart's closes. Use the Analyze tab to look at any coin on 1h / 4h / Daily / Weekly.`)
        +' · '+ mb(`MEXC ${mkt}`, `Data source: MEXC ${d.cfg.market==='futures'?'perpetual futures (perps) — leveraged contracts':'spot market'}. All prices, volume, levels and R:R come from this market.`)
        +' · '+ mb(d.cfg.quote, `Quote currency — every pair is priced in ${d.cfg.quote} (e.g. BTC${d.cfg.quote}).`)
        +' · '+ mb(`EMA${d.cfg.ema_period}`, `The ${d.cfg.ema_period}-period exponential moving average — the core trend filter. Price above it = uptrend regime (longs favoured), below = downtrend (shorts favoured).`)
        +' · '+ mb(`rescans every ${d.cfg.scan_every}m`, `How often the scanner re-pulls MEXC in the background and refreshes every tab automatically. You never need to reload.`)
        + (d.cfg.telegram?(' · '+mb('📲 Telegram on','Breakout & confluence alerts are being pushed to your Telegram.')):''); }
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


# ===========================================================================
# MEMBERSHIP: accounts, access codes, sessions, per-user watchlists.
# Durable storage lives in Upstash Redis (free, no expiry) via its HTTPS REST
# API — set UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN. With those unset
# the app still boots on an in-memory store (fine for local dev; NOT durable).
# ===========================================================================
import hashlib
import hmac
import secrets
import urllib.request as _urlreq
from urllib.parse import urlparse, parse_qs

ADMIN_EMAIL = os.environ.get("ADMIN_EMAIL", "").strip().lower()
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "")
SIGNUP_OPEN = os.environ.get("SIGNUP_OPEN", "1").strip() not in ("0", "false", "no", "")
BRAND = "Apex"


class Store:
    """Tiny KV wrapper. Upstash Redis REST when configured, else in-memory."""

    def __init__(self):
        self.url = os.environ.get("UPSTASH_REDIS_REST_URL", "").strip().rstrip("/")
        self.token = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "").strip()
        self.mem: dict[str, str] = {}
        self.durable = bool(self.url and self.token)

    def _cmd(self, *args):
        body = json.dumps(list(args)).encode()
        req = _urlreq.Request(self.url, data=body, method="POST")
        req.add_header("Authorization", f"Bearer {self.token}")
        req.add_header("Content-Type", "application/json")
        with _urlreq.urlopen(req, timeout=12) as r:
            return json.loads(r.read().decode()).get("result")

    def get(self, key):
        if not self.durable:
            return self.mem.get(key)
        try:
            return self._cmd("GET", key)
        except Exception:
            return None

    def set(self, key, val):
        if not self.durable:
            self.mem[key] = val
            return True
        try:
            self._cmd("SET", key, val)
            return True
        except Exception:
            return False

    def setex(self, key, ttl, val):
        if not self.durable:
            self.mem[key] = val
            return True
        try:
            self._cmd("SET", key, val, "EX", str(int(ttl)))
            return True
        except Exception:
            return False

    def delete(self, key):
        if not self.durable:
            self.mem.pop(key, None)
            return True
        try:
            self._cmd("DEL", key)
            return True
        except Exception:
            return False

    def keys(self, pattern):
        if not self.durable:
            import fnmatch
            return [k for k in list(self.mem) if fnmatch.fnmatch(k, pattern)]
        try:
            return self._cmd("KEYS", pattern) or []
        except Exception:
            return []


STORE = Store()


def _now() -> int:
    return int(time.time())


def _hash_pw(pw: str, salt: str) -> str:
    return hashlib.pbkdf2_hmac("sha256", pw.encode(), salt.encode(), 200_000).hex()


def _norm_email(e: str) -> str:
    return (e or "").strip().lower()


def load_user(email: str) -> dict | None:
    email = _norm_email(email)
    if not email:
        return None
    raw = STORE.get(f"user:{email}")
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def save_user(u: dict) -> bool:
    return STORE.set(f"user:{_norm_email(u['email'])}", json.dumps(u))


def is_admin(u: dict | None) -> bool:
    return bool(u and (u.get("admin") or (ADMIN_EMAIL and _norm_email(u.get("email")) == ADMIN_EMAIL)))


def user_active(u: dict | None) -> bool:
    """Admins are always active; everyone else until their active_until epoch."""
    if not u:
        return False
    if is_admin(u):
        return True
    return int(u.get("active_until", 0)) > _now()


def days_left(u: dict | None) -> int | None:
    if not u or is_admin(u):
        return None
    secs = int(u.get("active_until", 0)) - _now()
    return max(0, secs // 86400)


def create_user(email: str, pw: str, admin: bool = False) -> tuple[dict | None, str]:
    email = _norm_email(email)
    if not email or "@" not in email:
        return None, "Enter a valid email."
    if len(pw) < 6:
        return None, "Password must be at least 6 characters."
    if load_user(email):
        return None, "An account with that email already exists."
    salt = secrets.token_hex(16)
    u = {"email": email, "salt": salt, "phash": _hash_pw(pw, salt),
         "active_until": 0, "created": _now(), "admin": bool(admin), "watch": []}
    save_user(u)
    return u, ""


def verify_login(email: str, pw: str) -> dict | None:
    u = load_user(email)
    if not u:
        return None
    if _hash_pw(pw, u.get("salt", "")) == u.get("phash"):
        return u
    return None


def new_session(email: str) -> str:
    tok = secrets.token_urlsafe(32)
    STORE.setex(f"sess:{tok}", 60 * 60 * 24 * 30, _norm_email(email))  # 30-day session
    return tok


def session_user(cookie_header: str | None) -> dict | None:
    if not cookie_header:
        return None
    tok = None
    for part in cookie_header.split(";"):
        p = part.strip()
        if p.startswith("sid="):
            tok = p[4:]
            break
    if not tok:
        return None
    email = STORE.get(f"sess:{tok}")
    if not email:
        return None
    return load_user(email)


def end_session(cookie_header: str | None):
    if not cookie_header:
        return
    for part in cookie_header.split(";"):
        p = part.strip()
        if p.startswith("sid="):
            STORE.delete(f"sess:{p[4:]}")


# ---- access codes -----------------------------------------------------------
def gen_code(days: int) -> str:
    code = "APX-" + "-".join(secrets.token_hex(2).upper() for _ in range(3))
    STORE.set(f"code:{code}", json.dumps({"days": int(days), "used_by": None, "created": _now()}))
    return code


def redeem_code(u: dict, code: str) -> tuple[bool, str]:
    code = (code or "").strip().upper()
    raw = STORE.get(f"code:{code}")
    if not raw:
        return False, "That code isn't valid."
    try:
        c = json.loads(raw)
    except Exception:
        return False, "That code isn't valid."
    if c.get("used_by"):
        return False, "That code has already been used."
    base = max(int(u.get("active_until", 0)), _now())   # extend, never shorten
    u["active_until"] = base + int(c["days"]) * 86400
    save_user(u)
    c["used_by"] = u["email"]
    c["used_at"] = _now()
    STORE.set(f"code:{code}", json.dumps(c))
    return True, f"Added {c['days']} days. Access now runs to {datetime.fromtimestamp(u['active_until'], timezone.utc):%Y-%m-%d}."


def ensure_admin_bootstrap():
    """If ADMIN_EMAIL/PASSWORD are set and no such account exists yet, create it."""
    if ADMIN_EMAIL and ADMIN_PASSWORD:
        if not load_user(ADMIN_EMAIL):
            create_user(ADMIN_EMAIL, ADMIN_PASSWORD, admin=True)
        else:
            u = load_user(ADMIN_EMAIL)
            if u and not u.get("admin"):
                u["admin"] = True
                save_user(u)


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
            elif self.path.startswith("/history"):
                _hp = HISTORY.payload()
                try:
                    import mexc_ema200_scanner as _sc
                    _hp["coinalyze"] = bool(_sc.COINALYZE_KEY)
                    if self.path.startswith("/history?diag") or "diag" in self.path:
                        _hp["cx_diag"] = _sc.coinalyze_status()
                except Exception:
                    _hp["coinalyze"] = False
                body = json.dumps(_hp).encode()
                self._send(200, body, "application/json")
            elif self.path.startswith("/backtest"):
                self._backtest()
            elif self.path.startswith("/track"):
                self._track()
            elif self.path.startswith("/analyze"):
                self._analyze()
            elif self.path in ("/", "/index.html"):
                self._send(200, PAGE.encode(), "text/html; charset=utf-8")
            else:
                self._send(404, b"not found", "text/plain")

        def _backtest(self):
            # Served from the background matrix (backtest_loop) — no on-demand compute.
            with state.lock:
                bt = state.backtests
            out = bt if bt else {"pending": True}
            self._send(200, json.dumps(out).encode(), "application/json")

        def _track(self):
            from urllib.parse import urlparse, parse_qs
            q = parse_qs(urlparse(self.path).query)
            g = lambda k: (q.get(k) or [""])[0]
            sym = normalize_symbol(g("symbol"), state.cfg.get("quote", "USDT"))
            ok = TRACKER.add_call(sym, g("side") or "long", g("targets") or g("target"),
                                  g("entry"), g("stop"), g("tf"), g("note"))
            self._send(200, json.dumps({"ok": bool(ok), "symbol": sym}).encode(),
                       "application/json")

        def _analyze(self):
            from urllib.parse import urlparse, parse_qs
            q = parse_qs(urlparse(self.path).query)
            raw = (q.get("symbol") or [""])[0]
            iv = (q.get("interval") or [state.cfg["interval"]])[0]
            if iv not in ("5m", "15m", "1h", "4h", "1d", "1w"):
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

    if APP_MODE != "stocks":
        threading.Thread(target=scan_loop, args=(state,), daemon=True).start()
        threading.Thread(target=breakout_watcher, args=(state,), daemon=True).start()
        threading.Thread(target=runner_loop, args=(state,), daemon=True).start()
    threading.Thread(target=backtest_loop, args=(state,), daemon=True).start()

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
