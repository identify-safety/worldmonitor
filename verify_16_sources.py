#!/usr/bin/env python3
"""16 源单独验证 v2 — 单源线程超时保护，429 源快速失败不空等。
用法: cd ~/.worldmonitor && ./venv/Scripts/python.exe verify_16_sources.py
"""
import sys, time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutTimeout
from functools import partial

sys.path.insert(0, r"C:\Users\ph\.worldmonitor")
import intel_engine as ie

# 验证模式：429/5xx 只试 1 次，不空等退避（IP 封禁中的源快速标记失败即可）
ie._http_get = partial(ie._http_get, retries=0)

SOURCES = [
    ("earthquakes",  ie.fetch_earthquakes),
    ("disasters",    ie.fetch_gdacs_disasters),
    ("nasa_events",  ie.fetch_nasa_events),
    ("crypto",       ie.fetch_crypto),
    ("fear_greed",   ie.fetch_fear_greed),
    ("polymarket",   ie.fetch_polymarket),
    ("nws_alerts",   ie.fetch_nws_alerts),
    ("ucdp",         ie.fetch_ucdp_conflicts),
    ("fed_funds",    ie.fetch_fed_funds_rate),
    ("usa_spending", ie.fetch_usa_spending),
    ("climate",      ie.fetch_open_meteo_climate),
    ("unhcr",        ie.fetch_unhcr_displacement),
    ("feodo",        ie.fetch_feodo_tracker),
    ("opensky",      ie.fetch_opensky_conflict_zones),
    ("oil",          ie.fetch_eia_oil_prices),
    ("worldbank",    ie.fetch_worldbank_gdp),
]

def summarize(name, val):
    if val is None:
        return "❌ None (请求失败)"
    if isinstance(val, list):
        return f"✅ {len(val)} 条"
    if isinstance(val, dict):
        if not val:
            return "⚠️ 空 dict"
        n = sum(1 for v in val.values() if v is not None)
        return f"✅ dict {n}/{len(val)} 字段有值"
    return f"✅ {val}"

print("=" * 72)
print("16 源单独验证结果 (单源超时 30s, 429 快速失败)")
print("=" * 72)
ok = 0
with ThreadPoolExecutor(max_workers=4) as ex:
    futures = {ex.submit(fn): name for name, fn in SOURCES}
    done = {}
    for fut, name in sorted(futures.items(), key=lambda x: x[1]):
        t0 = time.time()
        try:
            val = fut.result(timeout=30)
            elapsed = time.time() - t0
            done[name] = (summarize(name, val), f"{elapsed:.1f}s")
        except FutTimeout:
            elapsed = time.time() - t0
            done[name] = (f"⏱️ 超时 30s", f"{elapsed:.1f}s")
        except Exception as e:
            elapsed = time.time() - t0
            done[name] = (f"❌ EXC {type(e).__name__}: {str(e)[:60]}", f"{elapsed:.1f}s")

for name, (status, elapsed) in sorted(done.items()):
    print(f"  {name:<16} {status:<48} {elapsed}")
    if status.startswith("✅"):
        ok += 1
print("-" * 72)
print(f"成功 {ok}/16")
