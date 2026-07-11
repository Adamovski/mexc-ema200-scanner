# MEXC 200-EMA scanner — run it on the web, 24/7

You have three files that make up the app:

- `mexc_ema200_scanner.py` — the scan engine (fetch, EMA, cross-and-retest logic)
- `mexc_ema200_dashboard.py` — the web server + live dashboard
- `Dockerfile`, `requirements.txt`, `render.yaml`, `fly.toml` — deployment

The dashboard reads `PORT` and `SCAN_EVERY` from the environment, so it drops
straight into any host that gives you a web URL.

> Note: the "in-app" dashboard inside Claude can't run this — that view is network-locked
> and can't reach MEXC. Hosting it (below) gives you a real URL you can open from any
> browser, including your phone, and it keeps scanning even when your laptop is off.

---

## Option 1 — Render (easiest, free)

Zero terminal work beyond a git push. The free tier **spins down after ~15 min
with no visitors** and wakes on the next visit (first load takes ~30s while it
restarts). Fine for checking a few times a day. For truly non-stop, use the
`starter` plan (a few $/mo) or Option 2.

1. Put the files in a GitHub repo (drag-and-drop works at github.com → New repo
   → "uploading an existing file").
2. Go to https://render.com → sign in with GitHub.
3. **New +  →  Blueprint  →** select your repo. Render reads `render.yaml` and
   sets everything up.
4. Click **Apply**. In ~2 min you get a URL like
   `https://mexc-ema200-dashboard.onrender.com`. Open it — that's your dashboard.

To keep it awake on the free tier, point a free uptime pinger
(e.g. cron-job.org or uptimerobot.com) at the URL every 10 minutes.

---

## Option 2 — Fly.io (always-on, ~$2/mo, recommended for non-stop)

Stays running 24/7, no spin-down.

1. Install the CLI: https://fly.io/docs/horse/install/  (`brew install flyctl`
   on Mac).
2. In the folder with these files:
   ```
   fly auth signup        # or: fly auth login
   fly launch --no-deploy # accept the included fly.toml; pick a unique app name
   fly deploy
   ```
3. `fly open` launches your dashboard URL. Done — it runs continuously.

Adjust the rescan cadence any time:
```
fly secrets set SCAN_EVERY=5
```

---

## Option 3 — Any VPS / your own always-on box (Docker)

On a $4–6/mo droplet (DigitalOcean, Hetzner, Lightsail) with Docker installed:

```
# copy the files up, then in that folder:
docker build -t mexc-scanner .
docker run -d --restart unless-stopped -p 80:8000 \
  -e SCAN_EVERY=15 --name mexc-scanner mexc-scanner
```

Visit `http://YOUR_SERVER_IP`. `--restart unless-stopped` brings it back after
reboots. Logs: `docker logs -f mexc-scanner`.

No Docker? Plain Python works too:
```
pip install -r requirements.txt
nohup python3 mexc_ema200_dashboard.py --port 80 --scan-every 15 &
```

---

## Config reference

Environment variables (or CLI flags of the same name):

| Env          | Flag            | Default | Meaning                                            |
|--------------|-----------------|---------|----------------------------------------------------|
| `PORT`       | `--port`        | 8000    | Port the web server listens on                     |
| `SCAN_EVERY` | `--scan-every`  | 15      | Minutes between full rescans                        |
|              | `--interval`    | 4h      | Candle timeframe                                    |
|              | `--quote`       | USDT    | Quote asset                                         |
|              | `--lookback`    | 30      | How recent (in bars) the reclaim must be           |
|              | `--retest-tol`  | 0.02    | How close the pullback must tag the EMA (2%)        |
|              | `--break-tol`   | 0.005   | A close this far below the EMA voids the reclaim    |
|              | `--max-above`   | 0.08    | Skip names already >8% above the EMA (extended)     |
|              | `--min-slope`   | 0.0     | Require EMA slope ≥ this over the lookback          |
|              | `--pole-min-gain`   | 0.15 | Bull flag: minimum flagpole rise (15%)          |
|              | `--flag-max-retrace`| 0.5  | Bull flag: max pullback of the pole (50%)       |
|              | `--include-spot-only`| off | Also scan coins NOT on MEXC futures (default: futures-listed only) |

The scan is **crypto-only** (leveraged tokens, stablecoins, and tokenized
stocks/ETFs are filtered out) and, by default, restricted to coins that are also
listed on **MEXC USDT-perp futures**. The dashboard runs two scans — 200-EMA
reclaim and bull flags — and highlights coins that appear on both.

Example, a tighter/faster screen:
```
python3 mexc_ema200_dashboard.py --scan-every 5 --lookback 20 --retest-tol 0.015 --max-above 0.05
```

---

## A couple of practical notes

- **Data source:** MEXC's public REST API (no API key needed — read-only market data).
- **Rate limits:** the scanner throttles with a worker pool and backs off on 429s.
  If a host's IP ever gets limited, raise `SCAN_EVERY` or lower `--workers`.
- **This is a screener, not trading advice.** It surfaces candidates fast; always
  confirm the setup on the chart before acting.
