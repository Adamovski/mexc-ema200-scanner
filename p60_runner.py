"""Runs the +60% history scan on GitHub Actions (plenty of memory there) and writes p60.json.gz.
Apex's Render instance only downloads the finished file, so the scan can't slow down or crash the dashboard."""
import sys
import types

_store = {}


def _cmd(op, key, val=None):
    if op == "SET":
        _store[key] = val
    elif op == "GET":
        return _store.get(key)
    elif op == "DEL":
        _store.pop(key, None)


# pump60 saves its progress through volume_spikes' Redis helpers: give it an in-memory stand-in here.
vs = types.ModuleType("volume_spikes")
vs._redis = lambda: True
vs._redis_cmd = _cmd
sys.modules["volume_spikes"] = vs
sbt = types.ModuleType("system_backtest")
sbt.CHUNK = 900_000
sys.modules["system_backtest"] = sbt

import pump60  # noqa: E402

pump60.run()
if pump60.STATE["status"] != "done" or not pump60.RES["blob"]:
    print("scan failed:", pump60.STATE)
    sys.exit(1)
with open("p60.json.gz", "wb") as fh:
    fh.write(pump60.RES["blob"])
print("wrote p60.json.gz", len(pump60.RES["blob"]), "bytes,", pump60.STATE["events"], "alerts")
