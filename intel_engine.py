#!/usr/bin/env python3
"""
WorldMonitor v2 Advanced Intelligence Engine
Pulls from 18 real-time feeds (including GDELT TimelineVolRaw + TimelineTone),
computes Country Instability Index, detects anomalies, tracks history in
PostgreSQL, and outputs intelligence briefs.

Usage:
    python3 intel_engine.py                    # Full JSON output
    python3 intel_engine.py --section          # Formatted 🌍 GLOBAL SITUATION
    python3 intel_engine.py --brief            # Full intelligence brief
    python3 intel_engine.py --store            # Pull + store snapshot to NeonDB
    python3 intel_engine.py --cii              # Just CII scores
    python3 intel_engine.py --alerts           # Just anomalies and convergence
    python3 intel_engine.py --polymarket       # Just prediction market odds
"""

import json
import os
import sys
import math
import urllib.request
import urllib.error
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError
from datetime import datetime, timezone, timedelta
from typing import Optional, Any, Dict, List, Tuple

# ==================== CONFIGURATION ====================

TIMEOUT_PER_REQUEST = 12  # seconds
TIMEOUT_TOTAL = 120  # seconds
USER_AGENT = "WorldMonitor-v2/2.0"
NWS_ALERT_POINT = os.environ.get("WORLDMONITOR_NWS_POINT", "39.8283,-98.5795")

# Minimum interval between GDELT API calls (global, thread-safe).
# GDELT is aggressively rate-limited; 18-way parallel pulls trigger HTTP 429.
GDELT_MIN_INTERVAL = 1.5  # seconds


def _load_env_file(path: str) -> None:
    """Minimal .env loader (no external deps). Does NOT override existing env vars."""
    try:
        if not os.path.exists(path):
            return
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key:
                    os.environ.setdefault(key, value)
    except Exception:
        pass  # env loading is best-effort


# Auto-load .env from project dir and the legacy config path (so cron runs
# work without manually exporting DATABASE_URL).
_load_env_file(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
_load_env_file(os.path.expanduser("~/.config/worldmonitor/neon.env"))

MONITORED_COUNTRIES = {
    'US': 'United States', 'RU': 'Russia', 'CN': 'China', 'UA': 'Ukraine',
    'IR': 'Iran', 'IL': 'Israel', 'TW': 'Taiwan', 'KP': 'North Korea',
    'SA': 'Saudi Arabia', 'TR': 'Turkey', 'PL': 'Poland', 'DE': 'Germany',
    'FR': 'France', 'GB': 'United Kingdom', 'IN': 'India', 'PK': 'Pakistan',
    'SY': 'Syria', 'YE': 'Yemen', 'MM': 'Myanmar', 'VE': 'Venezuela',
    'BR': 'Brazil', 'AE': 'UAE'
}

BASELINE_RISK = {
    'US': 5, 'RU': 35, 'CN': 25, 'UA': 50, 'IR': 40, 'IL': 45,
    'TW': 30, 'KP': 45, 'SA': 20, 'TR': 25, 'PL': 10, 'DE': 5,
    'FR': 10, 'GB': 5, 'IN': 20, 'PK': 35, 'SY': 50, 'YE': 50,
    'MM': 45, 'VE': 40, 'BR': 15, 'AE': 10
}

COUNTRY_KEYWORDS = {
    'US': ['united states', 'usa', 'america', 'washington', 'biden', 'trump', 'pentagon'],
    'RU': ['russia', 'moscow', 'kremlin', 'putin'],
    'CN': ['china', 'beijing', 'xi jinping', 'prc'],
    'UA': ['ukraine', 'kyiv', 'zelensky', 'donbas'],
    'IR': ['iran', 'tehran', 'khamenei', 'irgc'],
    'IL': ['israel', 'tel aviv', 'netanyahu', 'idf', 'gaza'],
    'TW': ['taiwan', 'taipei'],
    'KP': ['north korea', 'pyongyang', 'kim jong'],
    'SA': ['saudi arabia', 'riyadh', 'mbs'],
    'TR': ['turkey', 'ankara', 'erdogan'],
    'PL': ['poland', 'warsaw'],
    'DE': ['germany', 'berlin'],
    'FR': ['france', 'paris', 'macron'],
    'GB': ['britain', 'uk', 'london', 'starmer'],
    'IN': ['india', 'delhi', 'modi'],
    'PK': ['pakistan', 'islamabad'],
    'SY': ['syria', 'damascus', 'assad'],
    'YE': ['yemen', 'sanaa', 'houthi'],
    'MM': ['myanmar', 'burma', 'rangoon'],
    'VE': ['venezuela', 'caracas', 'maduro'],
    'BR': ['brazil', 'brasilia', 'lula', 'bolsonaro'],
    'AE': ['uae', 'emirates', 'dubai', 'abu dhabi'],
}

# ==================== HTTP FETCHING ====================

import threading
import time as _time

# GDELT 直连：api.gdeltproject.org 国内一般可直连，绕开 Clash（代理出口是共享 IP，
# 被 GDELT 限流比家庭宽带狠得多）。用显式无代理 opener，不依赖 NO_PROXY 环境变量
# （urllib 对 NO_PROXY 的支持各版本行为不一致，显式构造最可靠）。
_GDELT_OPENER = None
_HTTP_OPENER = None
_OPENER_LOCK = threading.Lock()
# 全局 GDELT 串行闸门：18 线程池并发时 gkg(4) + timeline(10) 共 14 个请求
# 同时爆发必然触发 GDELT 429，必须强制串行化。
_GDELT_LOCK = threading.Lock()
_LAST_GDELT_TS = 0.0


def _get_opener(direct: bool = False) -> urllib.request.OpenerDirector:
    """Shared opener with connection reuse (avoid fresh TCP+TLS per request).

    direct=True → no-proxy opener (GDELT only); False → env-proxy-aware opener.
    """
    if direct:
        global _GDELT_OPENER
        if _GDELT_OPENER is None:
            with _OPENER_LOCK:
                if _GDELT_OPENER is None:
                    _GDELT_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        return _GDELT_OPENER
    global _HTTP_OPENER
    if _HTTP_OPENER is None:
        with _OPENER_LOCK:
            if _HTTP_OPENER is None:
                _HTTP_OPENER = urllib.request.build_opener(urllib.request.HTTPHandler())
    return _HTTP_OPENER


def _gdelt_throttle() -> None:
    """Serialize GDELT requests with a minimum interval between them."""
    global _LAST_GDELT_TS
    with _GDELT_LOCK:
        wait = GDELT_MIN_INTERVAL - (_time.monotonic() - _LAST_GDELT_TS)
        if wait > 0:
            _time.sleep(wait)
        _LAST_GDELT_TS = _time.monotonic()


def _http_get(url: str, timeout: int = TIMEOUT_PER_REQUEST, name: str = "",
              gdelt: bool = False, retries: int = 3) -> Tuple[Optional[bytes], int]:
    """Unified HTTP GET: connection reuse, 429 exponential backoff (respects
    Retry-After), 5xx retry. Returns (body_bytes, status); body None on failure."""
    for attempt in range(retries + 1):
        if gdelt:
            _gdelt_throttle()
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with _get_opener(direct=gdelt).open(req, timeout=timeout) as resp:
                return resp.read(), getattr(resp, "status", 200)
        except urllib.error.HTTPError as e:
            if e.code == 429:
                retry_after = e.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    # Respect Retry-After but cap at 60s — some APIs return
                    # absurd values (e.g. 31013s ≈ 8.6h) that would hang
                    # the thread pool shutdown.
                    backoff = min(float(retry_after), 60.0)
                else:
                    backoff = min((2 ** attempt) * 1.0 + 0.5, 60.0)
                print(f"  ⚠️ {name}: HTTP 429, backoff {backoff:.1f}s (attempt {attempt + 1}/{retries + 1})", file=sys.stderr)
                _time.sleep(backoff)
                continue
            if 500 <= e.code < 600 and attempt < retries:
                print(f"  ⚠️ {name}: HTTP {e.code}, retrying (attempt {attempt + 1}/{retries + 1})", file=sys.stderr)
                _time.sleep(1.0 * (attempt + 1))
                continue
            print(f"  ⚠️ {name}: HTTP {e.code}", file=sys.stderr)
            return None, e.code
        except Exception as e:
            print(f"  ⚠️ {name} failed: {e}", file=sys.stderr)
            return None, -1
    print(f"  ⚠️ {name}: giving up after {retries + 1} attempts (HTTP 429)", file=sys.stderr)
    return None, 429


def fetch_json(url: str, name: str, gdelt: bool = False, timeout: int = TIMEOUT_PER_REQUEST) -> Optional[Any]:
    """Fetch JSON with timeout, connection reuse, throttling and error handling."""
    body, _status = _http_get(url, timeout=timeout, name=name, gdelt=gdelt)
    if body is None:
        return None
    try:
        return json.loads(body)
    except Exception as e:
        print(f"  ⚠️ {name} invalid JSON: {e}", file=sys.stderr)
        return None


# ==================== DATA SOURCE FETCHERS ====================

def fetch_earthquakes() -> List[Dict[str, Any]]:
    """USGS M4.5+ earthquakes in last 24h."""
    data = fetch_json(
        "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_day.geojson",
        "USGS Earthquakes"
    )
    if not data:
        return []
    quakes = []
    for f in data.get("features", []):
        p = f.get("properties", {})
        coords = f.get("geometry", {}).get("coordinates", [0, 0, 0])
        quakes.append({
            "magnitude": p.get("mag"),
            "place": p.get("place"),
            "time": datetime.fromtimestamp(p.get("time", 0) / 1000, tz=timezone.utc).isoformat(),
            "depth_km": round(coords[2], 1) if len(coords) > 2 else None,
            "lat": coords[1] if len(coords) > 1 else 0,
            "lon": coords[0] if len(coords) > 0 else 0,
            "alert": p.get("alert"),
            "tsunami": p.get("tsunami", 0),
        })
    quakes.sort(key=lambda q: q.get("magnitude", 0) or 0, reverse=True)
    return quakes


def fetch_gdacs_disasters() -> List[Dict[str, Any]]:
    """GDACS orange/red disasters (last 7 days). Retries once on 503."""
    import time as _time
    url = ("https://www.gdacs.org/gdacsapi/api/events/geteventlist/SEARCH"
           "?alertlevel=orange;red&eventlist=EQ;TC;FL;VO;DR&limit=50")
    data = fetch_json(url, "GDACS Disasters")
    if data is None:
        _time.sleep(3)
        data = fetch_json(url, "GDACS Disasters (retry)")
    if not data:
        return []
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    events = []
    for feature in data.get("features", []):
        p = feature.get("properties", {})
        date_str = p.get("fromdate") or p.get("todate", "")
        if date_str:
            try:
                event_date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                if event_date < cutoff:
                    continue
            except (ValueError, TypeError):
                pass
        events.append({
            "type": p.get("eventtype"),
            "name": p.get("name") or p.get("eventname"),
            "country": p.get("country"),
            "alert_level": p.get("alertlevel"),
            "date": p.get("fromdate"),
        })
    # Dedup by type+country
    seen = {}
    for e in events:
        key = f"{e.get('type')}|{e.get('country','')}"
        if key not in seen or e.get("alert_level") == "Red":
            seen[key] = e
    events = list(seen.values())
    events.sort(key=lambda e: (0 if e.get("alert_level") == "Red" else 1, e.get("date", "") or ""))
    return events[:10]


def fetch_nasa_events() -> List[Dict[str, Any]]:
    """NASA EONET active natural events. Retries once on 503."""
    import time as _time
    url = "https://eonet.gsfc.nasa.gov/api/v3/events?status=open&limit=20"
    data = fetch_json(url, "NASA EONET")
    if data is None:
        _time.sleep(3)
        data = fetch_json(url, "NASA EONET (retry)")
    if not data:
        return []
    events = []
    for e in data.get("events", []):
        categories = [c.get("title") for c in e.get("categories", [])]
        events.append({
            "title": e.get("title"),
            "categories": categories,
            "date": e.get("geometry", [{}])[-1].get("date") if e.get("geometry") else None,
        })
    return events


def fetch_crypto() -> Dict[str, Any]:
    """CoinGecko crypto prices with 24h change."""
    data = fetch_json(
        "https://api.coingecko.com/api/v3/simple/price"
        "?ids=bitcoin,ethereum,solana&vs_currencies=usd&include_24hr_change=true&include_market_cap=true",
        "CoinGecko"
    )
    if not data:
        return {}
    result = {}
    name_map = {"bitcoin": "BTC", "ethereum": "ETH", "solana": "SOL"}
    for coin_id, symbol in name_map.items():
        if coin_id in data:
            d = data[coin_id]
            result[symbol] = {
                "price": d.get("usd"),
                "change_24h": round(d.get("usd_24h_change", 0), 2),
                "market_cap": d.get("usd_market_cap"),
            }
    return result


def fetch_fear_greed() -> Dict[str, Any]:
    """Crypto Fear & Greed Index."""
    data = fetch_json("https://api.alternative.me/fng/?limit=2", "Fear & Greed")
    if not data or not data.get("data"):
        return {}
    today = data["data"][0]
    yesterday = data["data"][1] if len(data["data"]) > 1 else today
    return {
        "value": int(today.get("value", 0)),
        "label": today.get("value_classification", "Unknown"),
        "timestamp": today.get("timestamp"),
        "prev_value": int(yesterday.get("value", 0)),
        "change": int(today.get("value", 0)) - int(yesterday.get("value", 0)),
    }


def fetch_polymarket() -> List[Dict[str, Any]]:
    """Polymarket prediction markets (top by volume, geopolitical only)."""
    data = fetch_json(
        "https://gamma-api.polymarket.com/events?closed=false&order=volume&ascending=false&limit=20",
        "Polymarket"
    )
    if not data:
        return []
    markets = []
    for event in data:
        vol = event.get("volume") or event.get("volumeNum") or 0
        try:
            vol = float(vol)
        except (ValueError, TypeError):
            vol = 0
        if vol < 50000:
            continue
        title = event.get("title", "")
        # Filter out sports/entertainment
        if any(x in title.lower() for x in ["nfl", "nba", "mlb", "nhl", "ufc", "boxing", "award", "grammy", "oscar",
                                              "premier league", "champions league", "la liga", "serie a", "bundesliga",
                                              "world cup", "copa", "europa league", "mls", "liga mx", "ligue 1",
                                              "cricket", "rugby", "tennis", "golf", "f1", "formula",
                                              "super bowl", "playoffs", "march madness", "college"]):
            continue
        # Pick first market from event
        event_markets = event.get("markets", [])
        if not event_markets:
            continue
        market = event_markets[0]
        question = market.get("question", title)
        prices_str = market.get("outcomePrices", "[]")
        try:
            prices = json.loads(prices_str)
            yes_prob = round(float(prices[0]) * 100, 1) if prices else 50.0
        except:
            yes_prob = 50.0
        markets.append({
            "question": question,
            "yes_prob": yes_prob,
            "volume": vol,  # Already in USD
            "slug": event.get("slug", ""),
            "url": f"https://polymarket.com/event/{event.get('slug', '')}",
        })
    markets.sort(key=lambda m: m["volume"], reverse=True)
    return markets[:10]


def fetch_nws_alerts() -> List[Dict[str, Any]]:
    """NWS active alerts for configured point (WORLDMONITOR_NWS_POINT)."""
    data = fetch_json(f"https://api.weather.gov/alerts/active?point={NWS_ALERT_POINT}", "NWS Alerts")
    if not data:
        return []
    alerts = []
    for f in data.get("features", []):
        p = f.get("properties", {})
        alerts.append({
            "event": p.get("event"),
            "severity": p.get("severity"),
            "headline": p.get("headline"),
            "description": p.get("description", "")[:200],
        })
    return alerts[:5]


def fetch_ucdp_conflicts() -> List[Dict[str, Any]]:
    """UCDP armed conflict events (latest year).
    NOTE: As of ~Feb 2026, UCDP requires x-ucdp-access-token header.
    Set UCDP_ACCESS_TOKEN env var or register at https://ucdp.uu.se/apidocs/
    """
    ucdp_token = os.environ.get("UCDP_ACCESS_TOKEN", "")
    headers = {"User-Agent": USER_AGENT}
    if ucdp_token:
        headers["x-ucdp-access-token"] = ucdp_token
    # Try multiple API versions
    for version in ["24.1", "25.1"]:
        try:
            req = urllib.request.Request(
                f"https://ucdpapi.pcr.uu.se/api/gedevents/{version}?pagesize=50&page=0",
                headers=headers,
            )
            with urllib.request.urlopen(req, timeout=20) as resp:
                data = json.loads(resp.read())
            if data and data.get("Result"):
                break
        except urllib.error.HTTPError as e:
            if e.code == 401 and not ucdp_token:
                print(f"  ⚠️ UCDP requires API token now (set UCDP_ACCESS_TOKEN env). Skipping.", file=sys.stderr)
                return []
            print(f"  ⚠️ UCDP v{version} failed: {e}", file=sys.stderr)
            data = None
        except Exception as e:
            print(f"  ⚠️ UCDP v{version} failed: {e}", file=sys.stderr)
            data = None
    if not data:
        return []
    events = []
    for e in data.get("Result", []):
        events.append({
            "country": e.get("country"),
            "event_date": e.get("date_start"),
            "type": e.get("type_of_violence"),
            "deaths_low": e.get("best_est", 0),
            "side_a": e.get("side_a"),
            "side_b": e.get("side_b"),
        })
    return events[:50]


def fetch_fed_funds_rate() -> Optional[float]:
    """Federal funds effective rate via FRED (free, no key needed via alternative)."""
    # Try Treasury.gov yield data as proxy (no API key)
    data = fetch_json(
        "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/avg_interest_rates?sort=-record_date&page[size]=1",
        "Fed Funds Rate"
    )
    if data and data.get("data"):
        try:
            return float(data["data"][0].get("avg_interest_rate_amt", 0))
        except (ValueError, IndexError):
            pass
    return None


def fetch_usa_spending() -> Dict[str, Any]:
    """USASpending agency spending totals (public, no auth)."""
    data = fetch_json(
        "https://api.usaspending.gov/api/v2/references/toptier_agencies/",
        "USASpending"
    )
    if not data or not data.get("results"):
        return {}
    # Find DOD
    for agency in data["results"]:
        name = agency.get("agency_name", "")
        if "Defense" in name:
            return {
                "agency": name,
                "budget_authority": agency.get("budget_authority_amount", 0),
                "obligated": agency.get("obligated_amount", 0),
                "fiscal_year": agency.get("current_total_budget_authority_amount", 0),
            }
    return {}


def fetch_open_meteo_climate() -> Dict[str, Any]:
    """Open-Meteo climate API (sample location)."""
    # Climate API requires specific coords; we'll use a sample
    data = fetch_json(
        "https://climate-api.open-meteo.com/v1/climate?latitude=35&longitude=-80&start_date=2024-01-01&end_date=2024-12-31&models=EC_Earth3P_HR",
        "Open-Meteo Climate"
    )
    if not data:
        return {}
    return {"model": "EC_Earth3P_HR", "status": "available"}


def fetch_gdelt_gkg() -> List[Dict[str, Any]]:
    """GDELT DOC 2.0 ArtList (geopolitical/cyber headlines).

    Docs reference: https://www.gdeltproject.org/data.html#documentation

    Uses DOC 2.0 parameters (query/mode/format/timespan/maxrecords/sort)
    and rotates multiple focused queries to improve topical coverage while
    keeping runtime bounded.
    """
    import time as _time

    query_specs = [
        # Core geopolitical escalation
        ("(military OR conflict OR strike OR ceasefire OR sanctions) sourcelang:english", "3h", 20),
        # Security + cyber blend (useful for Polymarket conviction context)
        ("(cyberattack OR ransomware OR malware OR infrastructure attack OR drone strike) sourcelang:english", "6h", 18),
        # Regional flashpoint terms
        ("(iran OR israel OR gaza OR ukraine OR taiwan OR \"north korea\") near20:\"military escalation\" sourcelang:english", "6h", 18),
        # Broad fallback if above are sparse
        ("(war OR crisis OR mobilization OR deployment) sourcelang:english", "12h", 15),
    ]

    all_articles: List[Dict[str, Any]] = []
    seen_titles = set()

    for query, timespan, timeout in query_specs:
        params = urllib.parse.urlencode({
            "query": query,
            "mode": "ArtList",
            "format": "json",
            "maxrecords": 40,   # DOC 2.0 supports up to 250
            "timespan": timespan,
            "sort": "DateDesc",
        })
        url = f"https://api.gdeltproject.org/api/v2/doc/doc?{params}"

        body, _status = _http_get(url, timeout=timeout, name=f"GDELT DOC ({query[:28]}...)", gdelt=True)
        if body is None:
            continue
        try:
            data = json.loads(body)
        except Exception as e:
            print(f"  ⚠️ GDELT DOC ({query[:28]}...) invalid JSON: {e}", file=sys.stderr)
            continue

        articles = data.get("articles") if isinstance(data, dict) else None
        if not articles:
            continue

        for article in articles:
            title = (article.get("title") or "").strip()
            if not title:
                continue
            title_key = title.lower()[:220]
            if title_key in seen_titles:
                continue
            seen_titles.add(title_key)

            all_articles.append({
                "title": title,
                "url": article.get("url"),
                "domain": article.get("domain"),
                "seendate": article.get("seendate") or article.get("seendateutc"),
                "sourcecountry": article.get("sourcecountry"),
                "source_lang": article.get("language") or article.get("sourcelang"),
                "tone": article.get("tone"),
            })

        # Keep bounded to avoid over-weighting GDELT relative to other sources
        if len(all_articles) >= 80:
            break

    return all_articles[:80]


def _gdelt_doc_request(query: str, mode: str, timespan: str = "7d", timeout: int = 14) -> Optional[Dict[str, Any]]:
    """Low-level GDELT DOC 2.0 request helper (throttled, direct, with retries)."""
    params = urllib.parse.urlencode({
        "query": query,
        "mode": mode,
        "format": "json",
        "timespan": timespan,
    })
    url = f"https://api.gdeltproject.org/api/v2/doc/doc?{params}"

    body, _status = _http_get(url, timeout=timeout, name=f"GDELT {mode}", gdelt=True)
    if body is None:
        return None
    try:
        return json.loads(body)
    except Exception as e:
        print(f"  ⚠️ GDELT {mode} invalid JSON: {e}", file=sys.stderr)
        return None


def _extract_timeline_points(payload: Optional[Dict[str, Any]], normalize_with_norm: bool = False) -> List[Dict[str, Any]]:
    """Extract timeline points from GDELT timeline responses.

    Supports both flat timeline items and nested series/data structures.
    """
    if not payload or not isinstance(payload, dict):
        return []

    points: List[Dict[str, Any]] = []

    def _append_point(node: Any):
        if not isinstance(node, dict):
            return
        raw_val = node.get("value")
        try:
            value = float(raw_val)
        except (TypeError, ValueError):
            return

        if normalize_with_norm:
            try:
                norm = float(node.get("norm", 0) or 0)
            except (TypeError, ValueError):
                norm = 0
            if norm > 0:
                value = value / norm

        if not math.isfinite(value):
            return

        points.append({
            "date": node.get("date") or node.get("datetime") or node.get("time") or "",
            "value": value,
        })

    timeline = payload.get("timeline", [])
    if not isinstance(timeline, list):
        return []

    for item in timeline:
        if isinstance(item, dict) and isinstance(item.get("data"), list):
            for row in item.get("data", []):
                _append_point(row)
        else:
            _append_point(item)

    return points


def _compute_series_stats(points: List[Dict[str, Any]], baseline_window: int = 24) -> Dict[str, float]:
    """Compute current value, baseline mean/stddev, and z-score."""
    values = [float(p.get("value", 0)) for p in points if p.get("value") is not None]
    values = [v for v in values if math.isfinite(v)]

    if len(values) < 3:
        return {}

    current = values[-1]
    history = values[:-1]
    if baseline_window > 0 and len(history) > baseline_window:
        history = history[-baseline_window:]

    if not history:
        return {
            "current": round(current, 6),
            "baseline": round(current, 6),
            "stddev": 0.0,
            "zscore": 0.0,
        }

    baseline = sum(history) / len(history)
    variance = sum((x - baseline) ** 2 for x in history) / max(len(history), 1)
    stddev = math.sqrt(max(variance, 0.0))
    if stddev <= 1e-12:
        zscore = 0.0
    else:
        zscore = (current - baseline) / stddev

    return {
        "current": round(current, 6),
        "baseline": round(baseline, 6),
        "stddev": round(stddev, 6),
        "zscore": round(zscore, 3),
    }


def fetch_gdelt_timeline_signals() -> Dict[str, Any]:
    """Pull GDELT TimelineVolRaw + TimelineTone and detect spike regimes.

    Returns per-region metrics plus extracted spike candidates suitable for
    downstream Polymarket signal scoring.
    """
    import time as _time

    query_specs = [
        ("Iran", "(iran OR tehran OR khamenei OR irgc)"),
        ("Russia/Ukraine", "(russia OR ukraine OR moscow OR kyiv OR kremlin)"),
        ("China/Taiwan", "(china OR taiwan OR beijing OR taipei OR pla)"),
        ("Middle East", "(israel OR gaza OR hezbollah OR hamas OR lebanon)"),
        ("Cyber", "(cyberattack OR ransomware OR malware OR critical infrastructure attack)"),
    ]

    regions: List[Dict[str, Any]] = []
    spikes: List[Dict[str, Any]] = []

    for region, query in query_specs:
        vol_payload = _gdelt_doc_request(query, "TimelineVolRaw", timespan="7d", timeout=14)
        tone_payload = _gdelt_doc_request(query, "TimelineTone", timespan="7d", timeout=14)

        vol_points = _extract_timeline_points(vol_payload, normalize_with_norm=True)
        tone_points = _extract_timeline_points(tone_payload, normalize_with_norm=False)

        vol_stats = _compute_series_stats(vol_points, baseline_window=24)
        tone_stats = _compute_series_stats(tone_points, baseline_window=24)

        metric: Dict[str, Any] = {
            "region": region,
            "query": query,
            "volume_points": len(vol_points),
            "tone_points": len(tone_points),
            "volume": vol_stats,
            "tone": tone_stats,
            "signals": [],
        }

        vol_z = float(vol_stats.get("zscore", 0.0) or 0.0)
        tone_z = float(tone_stats.get("zscore", 0.0) or 0.0)
        tone_current = float(tone_stats.get("current", 0.0) or 0.0)

        signals: List[str] = []
        if vol_z >= 2.0:
            signals.append("volume_spike")
        if tone_z <= -1.5 or (tone_current <= -4.0 and tone_z <= 0.25):
            signals.append("tone_negative_shift")
        elif tone_z >= 1.5 or tone_current >= 3.0:
            signals.append("tone_positive_shift")
        if vol_z >= 2.0 and (tone_z <= -1.0 or tone_current <= -4.0):
            signals.append("escalation_combo")

        metric["signals"] = signals
        regions.append(metric)

        if signals:
            severity = "high" if (vol_z >= 3.0 or tone_z <= -2.0 or "escalation_combo" in signals) else "moderate"
            spikes.append({
                "region": region,
                "query": query,
                "signals": signals,
                "severity": severity,
                "volume_z": round(vol_z, 3),
                "tone_z": round(tone_z, 3),
                "tone_current": round(tone_current, 3),
                "volume_current": vol_stats.get("current"),
                "volume_baseline": vol_stats.get("baseline"),
                "tone_baseline": tone_stats.get("baseline"),
            })

        # Gentle pacing to reduce 429 risk.
        _time.sleep(0.2)

    return {
        "regions": regions,
        "spikes": spikes,
        "spike_count": len(spikes),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def fetch_unhcr_displacement() -> Dict[str, Any]:
    """UNHCR global displacement data (refugees, asylum seekers, IDPs, stateless).

    NOTE: The UNHCR endpoint returns *global totals* as a single record when called normally.
    When called with `coo_all=true`, it returns per-origin breakdowns (paginated).
    We do BOTH so totals are accurate.
    """

    YEAR = 2023

    def _safe_int(v):
        try:
            # UNHCR sometimes returns numbers as strings
            return int(float(v)) if v not in (None, "") else 0
        except (ValueError, TypeError):
            return 0

    # 1) Global totals (single record)
    totals_resp = fetch_json(
        f"https://api.unhcr.org/population/v1/population/?year={YEAR}&limit=1&page=1",
        "UNHCR Displacement (totals)"
    )
    totals_item = (totals_resp or {}).get("items", [{}])[0] if totals_resp else {}
    totals = {
        "refugees": _safe_int(totals_item.get("refugees")),
        "asylum_seekers": _safe_int(totals_item.get("asylum_seekers")),
        "idps": _safe_int(totals_item.get("idps")),
        "stateless": _safe_int(totals_item.get("stateless")),
    }

    # 2) Per-origin breakdown (top origins)
    by_origin: Dict[str, int] = {}
    origin_pages = 0
    max_pages = 1

    # Keep runtime bounded: cap pages + total rows
    LIMIT = 500
    PAGE_CAP = 10
    ROW_CAP = 5000

    for page in range(1, PAGE_CAP + 1):
        url = f"https://api.unhcr.org/population/v1/population/?year={YEAR}&limit={LIMIT}&page={page}&coo_all=true"
        resp = fetch_json(url, f"UNHCR Displacement (origins p{page})")
        if not resp or not resp.get("items"):
            break
        origin_pages += 1
        max_pages = int(resp.get("maxPages") or max_pages)

        for item in resp.get("items", []):
            origin = item.get("coo_name") or "Unknown"
            displaced = (
                _safe_int(item.get("refugees")) +
                _safe_int(item.get("asylum_seekers")) +
                _safe_int(item.get("idps")) +
                _safe_int(item.get("stateless"))
            )
            by_origin[origin] = by_origin.get(origin, 0) + displaced

        if page >= max_pages:
            break
        if sum(by_origin.values()) > ROW_CAP:
            break

    top_origins = sorted(by_origin.items(), key=lambda x: x[1], reverse=True)[:5]
    grand_total = totals["refugees"] + totals["asylum_seekers"] + totals["idps"] + totals["stateless"]

    return {
        "year": YEAR,
        "totals": totals,
        "grand_total": grand_total,
        "top_origins": [{"country": c, "displaced": d} for c, d in top_origins],
        "origin_pages_fetched": origin_pages,
        "origin_max_pages": max_pages,
    }


def fetch_feodo_tracker() -> Dict[str, Any]:
    """Feodo Tracker — active C2/botnet servers."""
    data = fetch_json(
        "https://feodotracker.abuse.ch/downloads/ipblocklist.json",
        "Feodo Tracker"
    )
    if not data:
        return {}
    entries = data if isinstance(data, list) else data.get("data", data.get("entries", []))
    if not isinstance(entries, list):
        return {}
    online = [e for e in entries if (e.get("status") or "").lower() == "online"]
    # Count by country
    by_country = {}
    for e in online:
        c = e.get("country", "Unknown") or "Unknown"
        by_country[c] = by_country.get(c, 0) + 1
    top_countries = sorted(by_country.items(), key=lambda x: x[1], reverse=True)[:3]
    # Count by malware family
    by_malware = {}
    for e in online:
        m = e.get("malware", "Unknown") or "Unknown"
        by_malware[m] = by_malware.get(m, 0) + 1
    return {
        "active_c2": len(online),
        "total_listed": len(entries),
        "top_countries": [{"country": c, "count": n} for c, n in top_countries],
        "by_malware": by_malware,
    }


def fetch_opensky_conflict_zones() -> Dict[str, Any]:
    """OpenSky Network — aircraft counts in conflict zone bounding boxes."""
    zones = {
        "Ukraine": {"lamin": 44, "lomin": 22, "lamax": 53, "lomax": 40},
        "Taiwan Strait": {"lamin": 21, "lomin": 115, "lamax": 27, "lomax": 125},
        "Middle East": {"lamin": 25, "lomin": 34, "lamax": 40, "lomax": 55},
    }
    results = {}
    for zone_name, bbox in zones.items():
        url = (
            f"https://opensky-network.org/api/states/all"
            f"?lamin={bbox['lamin']}&lomin={bbox['lomin']}"
            f"&lamax={bbox['lamax']}&lomax={bbox['lomax']}"
        )
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=TIMEOUT_PER_REQUEST) as resp:
                data = json.loads(resp.read())
            states = data.get("states", []) or []
            results[zone_name] = len(states)
        except Exception as e:
            print(f"  ⚠️ OpenSky {zone_name} failed: {e}", file=sys.stderr)
            results[zone_name] = None  # None = failed, 0 = no aircraft
    return results


def fetch_eia_oil_prices() -> Dict[str, Any]:
    """EIA crude oil spot prices (WTI + Brent)."""
    url = (
        "https://api.eia.gov/v2/petroleum/pri/spt/data/"
        "?api_key=DEMO_KEY&frequency=daily&data[0]=value"
        "&facets[product][]=EPCBRENT&facets[product][]=EPCWTI"
        "&sort[0][column]=period&sort[0][direction]=desc&length=2"
    )
    data = fetch_json(url, "EIA Oil Prices", gdelt=True)  # 直连：Clash 代理出口 IP 被 EIA 429，直连正常
    result = {"wti": None, "brent": None}
    if data and data.get("response", {}).get("data"):
        for row in data["response"]["data"]:
            product = row.get("product", "")
            value = row.get("value")
            try:
                value = float(value)
            except (ValueError, TypeError):
                continue
            if product == "EPCWTI" and result["wti"] is None:
                result["wti"] = round(value, 2)
            elif product == "EPCBRENT" and result["brent"] is None:
                result["brent"] = round(value, 2)
    return result


def fetch_worldbank_gdp() -> Dict[str, Any]:
    """World Bank GDP growth rates for CII countries."""
    # Map our 2-letter codes to World Bank 3-letter codes
    wb_codes = {
        'US': 'USA', 'RU': 'RUS', 'CN': 'CHN', 'UA': 'UKR', 'IR': 'IRN', 'IL': 'ISR',
        'TW': 'TWN', 'KP': 'PRK', 'SA': 'SAU', 'TR': 'TUR', 'PL': 'POL', 'DE': 'DEU',
        'FR': 'FRA', 'GB': 'GBR', 'IN': 'IND', 'PK': 'PAK', 'SY': 'SYR', 'YE': 'YEM',
        'MM': 'MMR', 'VE': 'VEN', 'BR': 'BRA', 'AE': 'ARE'
    }
    wb_to_iso2 = {v: k for k, v in wb_codes.items()}
    # Request only our specific countries (semicolon-separated 3-letter codes)
    country_str = ";".join(wb_codes.values())
    data = fetch_json(
        f"https://api.worldbank.org/v2/country/{country_str}/indicator/NY.GDP.MKTP.KD.ZG"
        "?format=json&per_page=200&date=2020:2024",
        "World Bank GDP"
    )
    if not data or not isinstance(data, list) or len(data) < 2:
        return {}
    # data[0] is pagination info, data[1] is the actual data
    entries = data[1] if len(data) > 1 else []
    if not isinstance(entries, list):
        return {}
    gdp_by_country = {}  # iso2 -> {"growth": float, "year": int}
    for entry in entries:
        if not entry or entry.get("value") is None:
            continue
        country_code_3 = entry.get("countryiso3code", "") or entry.get("country", {}).get("id", "")
        iso2 = wb_to_iso2.get(country_code_3)
        if not iso2:
            continue
        year = int(entry.get("date", "0"))
        growth = float(entry.get("value"))
        # Keep the most recent year with data
        if iso2 not in gdp_by_country or year > gdp_by_country[iso2]["year"]:
            gdp_by_country[iso2] = {"growth": round(growth, 2), "year": year}
    return gdp_by_country


# ==================== COUNTRY INSTABILITY INDEX (CII) ====================

def compute_cii(
    earthquakes: List[Dict],
    disasters: List[Dict],
    ucdp: List[Dict],
    gdelt: List[Dict],
    prev_scores: Dict[str, int],
    gdp_data: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    """Compute Country Instability Index for all monitored countries."""
    country_data = {code: {"unrest": 0, "conflict": 0, "security": 0, "information": 0}
                    for code in MONITORED_COUNTRIES}

    # Unrest: GDELT article mentions as proxy for protests/news velocity
    for article in gdelt:
        title = article.get("title", "").lower()
        for code, keywords in COUNTRY_KEYWORDS.items():
            if any(kw in title for kw in keywords):
                country_data[code]["unrest"] += 5

    # Conflict: earthquakes near countries, UCDP events, GDACS disasters
    for quake in earthquakes:
        # Simplified geo-matching
        if quake["magnitude"] and quake["magnitude"] >= 5.0:
            place = quake.get("place", "").lower()
            for code, name in MONITORED_COUNTRIES.items():
                if name.lower() in place or any(kw in place for kw in COUNTRY_KEYWORDS.get(code, [])):
                    country_data[code]["conflict"] += 10

    for event in ucdp:
        country = event.get("country", "")
        for code, name in MONITORED_COUNTRIES.items():
            if name.lower() in country.lower():
                country_data[code]["conflict"] += 15
                break

    for disaster in disasters:
        country = disaster.get("country", "")
        for code, name in MONITORED_COUNTRIES.items():
            if name.lower() in country.lower():
                boost = 20 if disaster.get("alert_level") == "Red" else 10
                country_data[code]["conflict"] += boost
                break

    # Security: military activity mentions in GDELT
    for article in gdelt:
        title = article.get("title", "").lower()
        if any(x in title for x in ["military", "troops", "forces", "navy", "army", "strike"]):
            for code, keywords in COUNTRY_KEYWORDS.items():
                if any(kw in title for kw in keywords):
                    country_data[code]["security"] += 8

    # Information: news velocity from GDELT
    for article in gdelt:
        title = article.get("title", "").lower()
        for code, keywords in COUNTRY_KEYWORDS.items():
            if any(kw in title for kw in keywords):
                country_data[code]["information"] += 3

    # Economic: World Bank GDP growth (negative growth = instability boost)
    if gdp_data:
        for code in MONITORED_COUNTRIES:
            entry = gdp_data.get(code)
            if entry and entry.get("growth") is not None:
                if entry["growth"] < 0:
                    country_data[code]["unrest"] += 5  # negative GDP -> +5 instability

    # Compute scores
    scores = []
    for code, name in MONITORED_COUNTRIES.items():
        components = country_data[code]
        # Clamp components to 0-100
        for k in components:
            components[k] = min(100, components[k])

        baseline = BASELINE_RISK.get(code, 20)
        event_score = (
            components["unrest"] * 0.3 +
            components["conflict"] * 0.3 +
            components["security"] * 0.25 +
            components["information"] * 0.15
        )
        score = int(baseline + event_score * 0.6)
        score = max(0, min(100, score))

        # Level thresholds
        if score >= 80:
            level = "critical"
        elif score >= 60:
            level = "high"
        elif score >= 40:
            level = "elevated"
        elif score >= 20:
            level = "normal"
        else:
            level = "low"

        # Trend arrows
        prev = prev_scores.get(code, score)
        if score - prev >= 3:
            trend = "↑"
        elif prev - score >= 3:
            trend = "↓"
        else:
            trend = "→"

        scores.append({
            "code": code,
            "name": name,
            "score": score,
            "level": level,
            "trend": trend,
            "change": score - prev,
            "components": components,
        })

    scores.sort(key=lambda s: s["score"], reverse=True)
    return scores


# ==================== CONVERGENCE DETECTION ====================

def detect_convergence(
    earthquakes: List[Dict],
    disasters: List[Dict],
    ucdp: List[Dict],
    gdelt: List[Dict]
) -> List[str]:
    """Detect convergence: 3+ signal types active in same region."""
    convergence = []

    for code, name in MONITORED_COUNTRIES.items():
        signals = []

        # Check earthquake
        for quake in earthquakes:
            place = quake.get("place", "").lower()
            if name.lower() in place or any(kw in place for kw in COUNTRY_KEYWORDS.get(code, [])):
                signals.append("earthquake")
                break

        # Check disaster
        for disaster in disasters:
            country = disaster.get("country", "")
            if name.lower() in country.lower():
                signals.append("disaster")
                break

        # Check conflict
        for event in ucdp:
            country = event.get("country", "")
            if name.lower() in country.lower():
                signals.append("conflict")
                break

        # Check military news
        for article in gdelt:
            title = article.get("title", "").lower()
            if any(x in title for x in ["military", "strike", "troops"]) and any(kw in title for kw in COUNTRY_KEYWORDS.get(code, [])):
                signals.append("military")
                break

        if len(set(signals)) >= 3:
            convergence.append(f"{name}: {' + '.join(set(signals))}")

    return convergence


# ==================== ANOMALY DETECTION ====================

def detect_anomalies(
    current_data: Dict[str, Any],
    historical_avg: Dict[str, float]
) -> List[str]:
    """Compare current values against 7-day rolling average + stateless checks."""
    anomalies = []

    # Earthquake count anomaly (vs historical if available)
    eq_count = len(current_data.get("earthquakes", []))
    eq_avg = historical_avg.get("earthquake_count")
    if eq_avg and eq_avg > 0 and eq_count > eq_avg * 2:
        anomalies.append(f"Earthquake count {eq_count / eq_avg:.1f}x above weekly average ({eq_count} vs avg {eq_avg:.0f})")

    # Stateless: Major earthquake (M6.5+)
    for q in current_data.get("earthquakes", []):
        if (q.get("magnitude") or 0) >= 6.5:
            anomalies.append(f"Major earthquake M{q['magnitude']} — {q.get('place', 'Unknown')}")
            break  # Only flag the biggest

    # Stateless: Multiple red alerts
    red_count = sum(1 for d in current_data.get("disasters", []) if d.get("alert_level") == "Red")
    if red_count >= 3:
        anomalies.append(f"{red_count} simultaneous GDACS Red Alerts active")

    # Bitcoin price anomaly (stateless: >5% move)
    btc = current_data.get("crypto", {}).get("BTC", {})
    btc_change = btc.get("change_24h", 0)
    if abs(btc_change) > 5:
        direction = "crashed" if btc_change < 0 else "surged"
        anomalies.append(f"BTC {direction} {abs(btc_change):.1f}% in 24h")

    # Fear & Greed anomaly (stateless: extreme values or big shift)
    fng = current_data.get("fear_greed", {})
    fng_val = fng.get("value", 50)
    fng_change = fng.get("change", 0)
    if fng_val <= 10:
        anomalies.append(f"Fear & Greed at {fng_val} — Extreme Fear territory")
    elif fng_val >= 90:
        anomalies.append(f"Fear & Greed at {fng_val} — Extreme Greed territory")
    if abs(fng_change) > 15:
        anomalies.append(f"Fear & Greed shifted {abs(fng_change)} points in 24h")

    # Stateless: CII critical countries
    cii = current_data.get("cii_scores", [])
    critical = [s for s in cii if s.get("level") == "critical"]
    if critical:
        names = ", ".join(s["name"] for s in critical[:3])
        anomalies.append(f"CII Critical: {names}")

    # GDELT timeline spike anomalies (volume + tone)
    gdelt_timeline = current_data.get("gdelt_timeline", {})
    spikes = gdelt_timeline.get("spikes", []) if isinstance(gdelt_timeline, dict) else []
    for spike in spikes[:5]:
        if not isinstance(spike, dict):
            continue
        region = spike.get("region", "Unknown")
        vol_z = float(spike.get("volume_z", 0) or 0)
        tone_z = float(spike.get("tone_z", 0) or 0)
        severity = spike.get("severity", "moderate")

        if "escalation_combo" in (spike.get("signals") or []):
            anomalies.append(
                f"GDELT escalation combo in {region}: volume spike {vol_z:+.1f}σ with negative tone {tone_z:+.1f}σ ({severity})"
            )
        elif vol_z >= 2.5:
            anomalies.append(f"GDELT volume spike in {region}: {vol_z:+.1f}σ above baseline")
        elif tone_z <= -2.0:
            anomalies.append(f"GDELT tone shock in {region}: {tone_z:+.1f}σ (more negative)")

    return anomalies


# ==================== DATABASE OPERATIONS (OPTIONAL) ====================

def get_db_connection():
    """Get NeonDB connection from env or config file."""
    try:
        import asyncpg
    except ImportError:
        print("  ⚠️ asyncpg not installed, DB operations disabled", file=sys.stderr)
        return None

    db_url = os.environ.get("DATABASE_URL") or os.environ.get("NEON_DATABASE_URL")
    if not db_url:
        config_path = os.path.expanduser(os.environ.get("WORLDMONITOR_DB_ENV_FILE", "~/.config/worldmonitor/neon.env"))
        if os.path.exists(config_path):
            with open(config_path) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("NEON_DATABASE_URL=") or line.startswith("DATABASE_URL="):
                        db_url = line.split("=", 1)[1].strip()
                        break

    if not db_url:
        print("  ⚠️ DATABASE_URL not found, DB operations disabled", file=sys.stderr)
        return None

    return db_url


async def store_snapshot(intel: Dict[str, Any], cii_scores: List[Dict]):
    """Store snapshot to NeonDB."""
    import asyncpg

    db_url = get_db_connection()
    if not db_url:
        return

    conn = await asyncpg.connect(db_url)
    try:
        # Get yesterday's CII scores for comparison
        prev_scores = {}
        rows = await conn.fetch(
            "SELECT country_code, score FROM worldmonitor_cii_history WHERE snapshot_date = CURRENT_DATE - INTERVAL '1 day'"
        )
        for row in rows:
            prev_scores[row["country_code"]] = row["score"]

        # Insert snapshot
        earthquakes = intel.get("earthquakes", [])
        max_mag = max((e.get("magnitude", 0) or 0 for e in earthquakes), default=0)
        disasters = intel.get("disasters", [])
        red_alerts = sum(1 for d in disasters if d.get("alert_level") == "Red")
        crypto = intel.get("crypto", {})
        btc = crypto.get("BTC", {})
        fng = intel.get("fear_greed", {})
        polymarket = intel.get("polymarket", [])
        convergence = intel.get("convergence", [])
        anomalies = intel.get("anomalies", [])

        cii_critical = sum(1 for s in cii_scores if s["level"] == "critical")
        cii_high = sum(1 for s in cii_scores if s["level"] == "high")
        cii_top = cii_scores[0] if cii_scores else {"code": "XX", "score": 0}

        await conn.execute("""
            INSERT INTO worldmonitor_snapshots (
                earthquake_count, max_earthquake_mag, disaster_count, red_alert_count,
                nasa_event_count, btc_price, btc_change_24h, fear_greed_index, fear_greed_label,
                cii_top_country, cii_top_score, cii_critical_count, cii_high_count,
                polymarket_top_event, polymarket_top_probability,
                convergence_zones, anomaly_flags, total_signals
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
            ON CONFLICT (snapshot_date) DO UPDATE SET
                earthquake_count = EXCLUDED.earthquake_count,
                max_earthquake_mag = EXCLUDED.max_earthquake_mag,
                disaster_count = EXCLUDED.disaster_count,
                red_alert_count = EXCLUDED.red_alert_count,
                nasa_event_count = EXCLUDED.nasa_event_count,
                btc_price = EXCLUDED.btc_price,
                btc_change_24h = EXCLUDED.btc_change_24h,
                fear_greed_index = EXCLUDED.fear_greed_index,
                fear_greed_label = EXCLUDED.fear_greed_label,
                cii_top_country = EXCLUDED.cii_top_country,
                cii_top_score = EXCLUDED.cii_top_score,
                cii_critical_count = EXCLUDED.cii_critical_count,
                cii_high_count = EXCLUDED.cii_high_count,
                polymarket_top_event = EXCLUDED.polymarket_top_event,
                polymarket_top_probability = EXCLUDED.polymarket_top_probability,
                convergence_zones = EXCLUDED.convergence_zones,
                anomaly_flags = EXCLUDED.anomaly_flags,
                total_signals = EXCLUDED.total_signals
        """, len(earthquakes), max_mag, len(disasters), red_alerts,
             len(intel.get("nasa_events", [])), btc.get("price"), btc.get("change_24h"),
             fng.get("value"), fng.get("label"), cii_top["code"], cii_top["score"],
             cii_critical, cii_high,
             polymarket[0]["question"] if polymarket else None,
             polymarket[0]["yes_prob"] if polymarket else None,
             convergence, anomalies,
             len(earthquakes) + len(disasters) + len(intel.get("nasa_events", []))
        )

        # Insert CII history
        for score in cii_scores:
            await conn.execute("""
                INSERT INTO worldmonitor_cii_history (
                    country_code, country_name, score, level,
                    unrest_component, conflict_component, security_component, information_component
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (snapshot_date, country_code) DO UPDATE SET
                    score = EXCLUDED.score,
                    level = EXCLUDED.level,
                    unrest_component = EXCLUDED.unrest_component,
                    conflict_component = EXCLUDED.conflict_component,
                    security_component = EXCLUDED.security_component,
                    information_component = EXCLUDED.information_component
            """, score["code"], score["name"], score["score"], score["level"],
                 score["components"]["unrest"], score["components"]["conflict"],
                 score["components"]["security"], score["components"]["information"]
            )

        print("  ✓ Snapshot stored to NeonDB", file=sys.stderr)
    finally:
        await conn.close()


async def store_intel_stories(intel: Dict[str, Any]):
    """Extract headline-level stories from intel data and store to intel_stories.
    
    This bridges the gap between WorldMonitor's 30-min refresh cycle and
    the trading scripts (early_warning_scanner, bayesian_processor, conviction_tracker)
    which all read from intel_stories.
    
    Sources extracted:
    - GDELT GKG articles (geopolitical headlines)
    - UCDP conflict events (armed conflict data)
    - GDACS disasters (orange/red alerts)
    - Significant earthquakes (M5.5+)
    - Polymarket odds shifts (large moves)
    - Convergence zones & anomalies
    - Brave News headlines (fresh geopolitical news)
    """
    import asyncpg

    db_url = get_db_connection()
    if not db_url:
        return

    conn = await asyncpg.connect(db_url)
    stories_inserted = 0

    try:
        today = datetime.now(timezone.utc).date()

        # Helper to classify region from text
        def classify_region(text: str) -> str:
            text_lower = text.lower()
            for code, keywords in COUNTRY_KEYWORDS.items():
                for kw in keywords:
                    if kw in text_lower:
                        return MONITORED_COUNTRIES.get(code, code)
            return "Global"

        # Helper to insert with dedup (skip if headline already exists in last 4 hours)
        # Using time-window dedup instead of daily dedup so 30min refresh cycles
        # can add genuinely new stories throughout the day.
        async def insert_story(headline: str, summary: str, region: str,
                               significance: str, category: str, confidence: str,
                               source: str):
            nonlocal stories_inserted
            if not headline or len(headline.strip()) < 10:
                return
            headline = headline[:500]
            summary = (summary or "")[:2000]

            # Dedup: check if similar headline exists in the last 4 hours
            existing = await conn.fetchval(
                "SELECT COUNT(*) FROM intel_stories WHERE created_at > NOW() - INTERVAL '4 hours' AND headline ILIKE $1",
                f"%{headline[:80]}%"
            )
            if existing and existing > 0:
                return

            await conn.execute("""
                INSERT INTO intel_stories 
                (region, headline, summary, significance, category, confidence,
                 sources, is_new_development, briefing_date, reported_in_briefing)
                VALUES ($1, $2, $3, $4, $5, $6, $7, true, $8, false)
            """, region, headline, summary, significance, category, confidence,
                 [source], today)
            stories_inserted += 1

        # 1. GDELT GKG articles — richest source of geopolitical headlines
        for article in intel.get("gdelt_gkg", []):
            title = article.get("title", "")
            if not title:
                continue
            region = classify_region(title)
            # GDELT articles are pre-filtered for military/conflict
            await insert_story(
                headline=title,
                summary=f"Source: {article.get('domain', 'unknown')}",
                region=region,
                significance="moderate",
                category="geopolitical",
                confidence="medium",
                source="GDELT"
            )

        # 1b. GDELT timeline spikes (TimelineVolRaw + TimelineTone)
        gdelt_timeline = intel.get("gdelt_timeline", {})
        timeline_spikes = gdelt_timeline.get("spikes", []) if isinstance(gdelt_timeline, dict) else []
        for spike in timeline_spikes:
            if not isinstance(spike, dict):
                continue
            region = spike.get("region", "Global")
            signals = spike.get("signals", []) or []
            vol_z = float(spike.get("volume_z", 0) or 0)
            tone_z = float(spike.get("tone_z", 0) or 0)
            tone_current = float(spike.get("tone_current", 0) or 0)

            headline_parts = [f"GDELT timeline spike in {region}"]
            if "volume_spike" in signals:
                headline_parts.append(f"volume {vol_z:+.1f}σ")
            if "tone_negative_shift" in signals:
                headline_parts.append(f"tone {tone_z:+.1f}σ")
            elif "tone_positive_shift" in signals:
                headline_parts.append(f"tone improving {tone_z:+.1f}σ")

            summary = (
                f"Signals={','.join(signals) if signals else 'none'} | "
                f"volume_z={vol_z:+.2f}, tone_z={tone_z:+.2f}, tone_current={tone_current:+.2f}"
            )

            significance = "high" if spike.get("severity") == "high" else "moderate"
            confidence = "high" if abs(vol_z) >= 2.5 or abs(tone_z) >= 2.0 else "medium"
            category = "geopolitical" if region != "Cyber" else "cyber"

            await insert_story(
                headline=" — ".join(headline_parts),
                summary=summary,
                region=region,
                significance=significance,
                category=category,
                confidence=confidence,
                source="GDELT_TIMELINE"
            )

        # 2. UCDP conflict events
        for event in intel.get("ucdp_conflicts", []):
            country = event.get("country", "Unknown")
            deaths = event.get("deaths_low", 0) or 0
            side_a = event.get("side_a", "")
            side_b = event.get("side_b", "")
            headline = f"Armed conflict in {country}: {side_a} vs {side_b}"
            significance = "critical" if deaths >= 50 else "high" if deaths >= 10 else "moderate"
            await insert_story(
                headline=headline,
                summary=f"Deaths (est.): {deaths}. Date: {event.get('event_date', 'unknown')}",
                region=country,
                significance=significance,
                category="conflict",
                confidence="high",
                source="UCDP"
            )

        # 3. GDACS disasters (orange/red)
        for disaster in intel.get("disasters", []):
            name = disaster.get("name", "Unknown disaster")
            country = disaster.get("country", "Unknown")
            alert = disaster.get("alert_level", "orange")
            headline = f"GDACS {alert.upper()} alert: {name} in {country}"
            await insert_story(
                headline=headline,
                summary=f"Type: {disaster.get('type', '?')}. Date: {disaster.get('date', 'unknown')}",
                region=country,
                significance="critical" if alert == "Red" else "high",
                category="disaster",
                confidence="high",
                source="GDACS"
            )

        # 4. Significant earthquakes (M5.5+)
        for quake in intel.get("earthquakes", []):
            mag = quake.get("magnitude", 0) or 0
            if mag < 5.5:
                continue
            place = quake.get("place", "Unknown location")
            headline = f"M{mag:.1f} earthquake near {place}"
            await insert_story(
                headline=headline,
                summary=f"Depth: {quake.get('depth_km', '?')}km. Alert: {quake.get('alert', 'none')}. Tsunami: {'yes' if quake.get('tsunami') else 'no'}",
                region=classify_region(place),
                significance="critical" if mag >= 7.0 else "high" if mag >= 6.0 else "moderate",
                category="disaster",
                confidence="high",
                source="USGS"
            )

        # 5. OpenSky conflict zone anomalies (unusual aircraft counts)
        for zone, data in intel.get("opensky_air", {}).items():
            if isinstance(data, dict) and data.get("aircraft_count", 0) > 50:
                count = data["aircraft_count"]
                headline = f"High aircraft activity in {zone} conflict zone: {count} aircraft detected"
                await insert_story(
                    headline=headline,
                    summary=f"OpenSky Network live tracking in {zone} bounding box",
                    region=classify_region(zone),
                    significance="moderate",
                    category="military",
                    confidence="medium",
                    source="OpenSky"
                )

        # 6. Convergence zones (multiple sources pointing at same region)
        for conv in intel.get("convergence", []):
            if isinstance(conv, str):
                region = classify_region(conv)
                await insert_story(
                    headline=f"Convergence alert: {conv}",
                    summary="Multiple independent data sources showing correlated signals in this region",
                    region=region,
                    significance="high",
                    category="convergence",
                    confidence="high",
                    source="WorldMonitor"
                )

        # 7. Anomalies
        for anomaly in intel.get("anomalies", []):
            if isinstance(anomaly, str):
                region = classify_region(anomaly)
                await insert_story(
                    headline=f"Anomaly detected: {anomaly}",
                    summary="Statistical anomaly vs. 7-day rolling average",
                    region=region,
                    significance="moderate",
                    category="anomaly",
                    confidence="medium",
                    source="WorldMonitor"
                )

        # 8. Brave News — expanded rotating queries for continuous refresh
        # Uses hour-based rotation so each 30min cycle gets different queries
        brave_key = os.environ.get("BRAVE_SEARCH_API_KEY") or os.environ.get("BRAVE_API_KEY")
        if brave_key:
            # Full query pool — 20 diverse queries across regions and categories
            all_brave_queries = [
                # Core geopolitical (always high-value)
                ("Iran military strike nuclear sanctions", "Iran", "geopolitical"),
                ("Russia Ukraine war offensive ceasefire", "Russia/Ukraine", "geopolitical"),
                ("China Taiwan military tension", "China/Taiwan", "geopolitical"),
                ("North Korea missile nuclear ICBM", "North Korea", "geopolitical"),
                ("Israel Gaza Hezbollah Lebanon strike", "Middle East", "geopolitical"),
                # Expanded geopolitical
                ("Syria conflict humanitarian crisis", "Syria", "geopolitical"),
                ("Yemen Houthi Red Sea shipping attack", "Yemen", "geopolitical"),
                ("NATO military deployment Europe", "Global", "geopolitical"),
                ("India Pakistan Kashmir border tension", "South Asia", "geopolitical"),
                ("Venezuela political crisis Maduro", "Venezuela", "geopolitical"),
                # Economics & markets
                ("Federal Reserve interest rate inflation", "United States", "economic"),
                ("oil price OPEC production cut supply", "Global", "economic"),
                ("global recession GDP economic downturn", "Global", "economic"),
                ("sanctions trade war tariff economic", "Global", "economic"),
                # Cyber & security
                ("cyberattack infrastructure ransomware state", "Global", "cyber"),
                ("election interference disinformation campaign", "Global", "information"),
                # Energy & resources
                ("energy crisis power grid nuclear plant", "Global", "energy"),
                ("Strait of Hormuz shipping disruption", "Middle East", "geopolitical"),
                # Humanitarian
                ("refugee crisis displacement humanitarian emergency", "Global", "humanitarian"),
                # Domestic US (relevant for Polymarket)
                ("US military deployment troops Pentagon", "United States", "geopolitical"),
            ]

            # Rotate: pick 8 queries based on current hour (ensures variety across 30min runs)
            current_hour = datetime.now(timezone.utc).hour
            half_hour = 1 if datetime.now(timezone.utc).minute >= 30 else 0
            rotation_index = (current_hour * 2 + half_hour) % len(all_brave_queries)
            # Select 8 queries starting from rotation point, wrapping around
            selected_queries = []
            for i in range(8):
                idx = (rotation_index + i * 3) % len(all_brave_queries)
                selected_queries.append(all_brave_queries[idx])

            # Use freshness=ph (past hour) to get truly fresh content each cycle
            for query, region, category in selected_queries:
                try:
                    params = urllib.parse.urlencode({
                        "q": query,
                        "count": 5,
                        "freshness": "ph",  # Past hour for fresh results
                    })
                    url = f"https://api.search.brave.com/res/v1/news/search?{params}"
                    req = urllib.request.Request(url, headers={
                        "Accept": "application/json",
                        "X-Subscription-Token": brave_key,
                    })
                    with urllib.request.urlopen(req, timeout=10) as resp:
                        data = resp.read()
                        if resp.headers.get("Content-Encoding") == "gzip":
                            import gzip
                            data = gzip.decompress(data)
                        result = json.loads(data)

                    for article in result.get("results", [])[:3]:
                        headline = article.get("title", "")
                        description = article.get("description", "")
                        if headline:
                            await insert_story(
                                headline=headline,
                                summary=description[:500],
                                region=region,
                                significance="moderate",
                                category=category,
                                confidence="medium",
                                source="BraveNews"
                            )
                except Exception as e:
                    print(f"  ⚠️ Brave News scan failed for {region}: {e}", file=sys.stderr)

            # Fallback: if past-hour returned nothing, try past-day with DIFFERENT queries
            # Use offset rotation so fallback queries don't overlap with main queries
            if stories_inserted == 0:
                print("  ℹ️ No fresh stories from past hour, trying past-day fallback...", file=sys.stderr)
                fallback_offset = (rotation_index + 10) % len(all_brave_queries)
                fallback_queries = [all_brave_queries[(fallback_offset + i) % len(all_brave_queries)] for i in range(5)]
                for query, region, category in fallback_queries:
                    try:
                        params = urllib.parse.urlencode({
                            "q": query, "count": 5, "freshness": "pd",
                        })
                        url = f"https://api.search.brave.com/res/v1/news/search?{params}"
                        req = urllib.request.Request(url, headers={
                            "Accept": "application/json",
                            "X-Subscription-Token": brave_key,
                        })
                        with urllib.request.urlopen(req, timeout=10) as resp:
                            data = resp.read()
                            if resp.headers.get("Content-Encoding") == "gzip":
                                import gzip
                                data = gzip.decompress(data)
                            result = json.loads(data)
                        for article in result.get("results", [])[:3]:
                            headline = article.get("title", "")
                            description = article.get("description", "")
                            if headline:
                                await insert_story(
                                    headline=headline,
                                    summary=description[:500],
                                    region=region,
                                    significance="moderate",
                                    category=category,
                                    confidence="medium",
                                    source="BraveNews"
                                )
                    except Exception as e:
                        print(f"  ⚠️ Brave News fallback failed for {region}: {e}", file=sys.stderr)

        # Alert on low story count
        if stories_inserted == 0:
            print("  ⚠️ WARNING: 0 intel stories stored this cycle! Data may be stale.", file=sys.stderr)
        print(f"  ✓ Stored {stories_inserted} intel stories to NeonDB", file=sys.stderr)

    except Exception as e:
        print(f"  ⚠️ store_intel_stories failed: {e}", file=sys.stderr)
    finally:
        await conn.close()


async def get_historical_avg() -> Dict[str, float]:
    """Get 7-day rolling averages from NeonDB."""
    import asyncpg

    db_url = get_db_connection()
    if not db_url:
        return {}

    conn = await asyncpg.connect(db_url)
    try:
        rows = await conn.fetch("""
            SELECT AVG(earthquake_count) as eq_avg, AVG(fear_greed_index) as fng_avg
            FROM worldmonitor_snapshots
            WHERE snapshot_date >= CURRENT_DATE - INTERVAL '7 days'
        """)
        if rows and rows[0]:
            return {
                "earthquake_count": float(rows[0]["eq_avg"] or 0),
                "fear_greed_index": float(rows[0]["fng_avg"] or 0),
            }
        return {}
    finally:
        await conn.close()


async def get_prev_cii_scores() -> Dict[str, int]:
    """Get yesterday's CII scores for trend arrows."""
    import asyncpg

    db_url = get_db_connection()
    if not db_url:
        return {}

    conn = await asyncpg.connect(db_url)
    try:
        rows = await conn.fetch("""
            SELECT country_code, score
            FROM worldmonitor_cii_history
            WHERE snapshot_date = CURRENT_DATE - INTERVAL '1 day'
        """)
        return {row["country_code"]: row["score"] for row in rows}
    finally:
        await conn.close()


# ==================== FORMATTING ====================

def format_section(intel: Dict[str, Any]) -> str:
    """Format 🌍 GLOBAL SITUATION section."""
    lines = ["🌍 GLOBAL SITUATION (WorldMonitor v2)", ""]

    # Earthquakes
    quakes = intel.get("earthquakes", [])
    sig_quakes = [q for q in quakes if (q.get("magnitude") or 0) >= 5.0]
    if sig_quakes:
        lines.append("🌋 Seismic Activity")
        for q in sig_quakes[:5]:
            mag = q.get("magnitude", "?")
            place = q.get("place", "Unknown")
            depth = q.get("depth_km")
            tsunami = " 🌊" if q.get("tsunami") else ""
            depth_str = f" ({depth}km deep)" if depth else ""
            lines.append(f"  • M{mag} — {place}{depth_str}{tsunami}")
    elif quakes:
        lines.append("🌋 Seismic Activity")
        lines.append(f"  • {len(quakes)} earthquakes M4.5+ in 24h, largest M{quakes[0].get('magnitude', '?')}")
    else:
        lines.append("🌋 Seismic: No significant activity")
    lines.append("")

    # GDACS Disasters
    disasters = intel.get("disasters", [])
    if disasters:
        lines.append("⚠️ Active Disasters (GDACS)")
        type_emoji = {"EQ": "🔴", "TC": "🌀", "FL": "🌊", "VO": "🌋", "DR": "☀️"}
        for d in disasters[:6]:
            emoji = type_emoji.get(d.get("type"), "🔶")
            name = d.get("name", "Unknown")
            country = d.get("country", "")[:50]
            alert_emoji = "🔴" if d.get("alert_level") == "Red" else "🟠"
            country_str = f" — {country}" if country else ""
            lines.append(f"  {emoji} {name}{country_str} {alert_emoji}")
        if len(disasters) > 6:
            lines.append(f"  ... +{len(disasters) - 6} more")
        lines.append("")

    # NASA Events
    nasa = intel.get("nasa_events", [])
    if nasa:
        lines.append("🛰️ NASA Active Events")
        for e in nasa[:4]:
            cats = ", ".join(e.get("categories", []))
            lines.append(f"  • {e.get('title', 'Unknown')} [{cats}]")
        if len(nasa) > 4:
            lines.append(f"  ... +{len(nasa) - 4} more")
        lines.append("")

    # NWS Weather Alerts (Greenville SC)
    nws = intel.get("nws_alerts", [])
    if nws:
        lines.append("🌪️ Weather Alerts (Greenville SC)")
        for alert in nws[:3]:
            severity = alert.get("severity", "")
            sev_emoji = "🔴" if severity == "Extreme" else "🟠" if severity == "Severe" else "🟡"
            lines.append(f"  {sev_emoji} {alert.get('event', 'Unknown')}: {alert.get('headline', '')[:80]}")
        lines.append("")

    # UNHCR Displacement
    unhcr = intel.get("unhcr_displacement", {})
    if unhcr and unhcr.get("grand_total"):
        lines.append("🚨 Displacement (UNHCR)")
        gt = unhcr["grand_total"]
        if gt >= 1_000_000:
            gt_str = f"{gt / 1_000_000:.1f}M"
        elif gt >= 1000:
            gt_str = f"{gt / 1000:.0f}K"
        else:
            gt_str = str(gt)
        totals = unhcr.get("totals", {})
        ref = totals.get("refugees", 0)
        idps = totals.get("idps", 0)
        ref_str = f"{ref / 1_000_000:.1f}M" if ref >= 1_000_000 else f"{ref / 1000:.0f}K" if ref >= 1000 else str(ref)
        idp_str = f"{idps / 1_000_000:.1f}M" if idps >= 1_000_000 else f"{idps / 1000:.0f}K" if idps >= 1000 else str(idps)
        lines.append(f"  • Global: {gt_str} displaced ({ref_str} refugees, {idp_str} IDPs)")
        top_origins = unhcr.get("top_origins", [])
        if top_origins:
            origin_parts = [f"{o['country']} ({o['displaced']:,})" for o in top_origins[:5]]
            lines.append(f"  • Top origins: {', '.join(origin_parts)}")
        lines.append("")

    # Feodo Tracker — Cyber Threats
    feodo = intel.get("feodo_tracker", {})
    if feodo and feodo.get("active_c2"):
        lines.append("🛡️ Cyber Threats (Feodo)")
        lines.append(f"  • Active C2 servers: {feodo['active_c2']} (of {feodo.get('total_listed', '?')} tracked)")
        top_c = feodo.get("top_countries", [])
        if top_c:
            c_parts = [f"{c['country']} ({c['count']})" for c in top_c]
            lines.append(f"  • Top hosting: {', '.join(c_parts)}")
        by_malware = feodo.get("by_malware", {})
        if by_malware:
            mal_parts = [f"{m}: {c}" for m, c in sorted(by_malware.items(), key=lambda x: x[1], reverse=True)[:3]]
            lines.append(f"  • Malware: {', '.join(mal_parts)}")
        lines.append("")

    # OpenSky — Air Activity in Conflict Zones
    opensky = intel.get("opensky_air", {})
    if opensky:
        has_data = any(v is not None for v in opensky.values())
        if has_data:
            lines.append("✈️ Air Activity (Conflict Zones)")
            for zone, count in opensky.items():
                if count is not None:
                    lines.append(f"  • {zone}: {count} aircraft tracked")
                else:
                    lines.append(f"  • {zone}: unavailable")
            lines.append("")

    # GDELT Timeline Spikes (volume + tone)
    gdelt_timeline = intel.get("gdelt_timeline", {})
    spikes = gdelt_timeline.get("spikes", []) if isinstance(gdelt_timeline, dict) else []
    if spikes:
        lines.append("📡 GDELT Timeline Spikes")
        for spike in spikes[:4]:
            region = spike.get("region", "Global")
            vol_z = float(spike.get("volume_z", 0) or 0)
            tone_z = float(spike.get("tone_z", 0) or 0)
            tone_cur = float(spike.get("tone_current", 0) or 0)
            severity_emoji = "🔴" if spike.get("severity") == "high" else "🟠"
            lines.append(
                f"  {severity_emoji} {region}: vol {vol_z:+.1f}σ | tone {tone_z:+.1f}σ (current {tone_cur:+.1f})"
            )
        if len(spikes) > 4:
            lines.append(f"  ... +{len(spikes) - 4} more")
        lines.append("")

    # Prediction Markets
    polymarket = intel.get("polymarket", [])
    if polymarket:
        lines.append("🎰 Prediction Markets (Polymarket)")
        for m in polymarket[:3]:
            q = m["question"][:70]
            prob = m["yes_prob"]
            vol = m["volume"]
            if vol >= 1_000_000:
                vol_str = f"${vol / 1_000_000:.1f}M"
            elif vol >= 1000:
                vol_str = f"${vol / 1000:.0f}K"
            else:
                vol_str = f"${vol:.0f}"
            lines.append(f"  • \"{q}\" — {prob}% YES ({vol_str} volume)")
        lines.append("")

    # Country Instability Index
    cii = intel.get("cii_scores", [])
    if cii:
        lines.append("🏴 Country Instability Index")
        top5 = cii[:5]
        for s in top5:
            emoji = "🔴" if s["level"] == "critical" else "🟠" if s["level"] == "high" else "🟡"
            lines.append(f"  {emoji} {s['code']}: {s['score']} ({s['level'].title()} {s['trend']})")
        lines.append("")

    # Convergence
    convergence = intel.get("convergence", [])
    if convergence:
        lines.append("⚡ Convergence Alerts")
        for c in convergence:
            lines.append(f"  • {c}")
        lines.append("")

    # Anomalies
    anomalies = intel.get("anomalies", [])
    if anomalies:
        lines.append("📊 Anomalies")
        for a in anomalies:
            lines.append(f"  • {a}")
        lines.append("")

    # Crypto
    crypto = intel.get("crypto", {})
    fng = intel.get("fear_greed", {})
    if crypto:
        lines.append("🪙 Crypto Radar")
        parts = []
        for symbol in ["BTC", "ETH", "SOL"]:
            if symbol in crypto:
                c = crypto[symbol]
                price = c.get("price", 0)
                change = c.get("change_24h", 0)
                arrow = "↑" if change >= 0 else "↓"
                price_str = f"${price:,.0f}" if price >= 1000 else f"${price:,.2f}"
                parts.append(f"{symbol}: {price_str} ({arrow}{abs(change):.1f}%)")
        lines.append(f"  • {' | '.join(parts[:2])}")
        if len(parts) > 2:
            lines.append(f"  • {parts[2]}")
        if fng:
            lines.append(f"  • Fear & Greed: {fng.get('value', '?')} ({fng.get('label', '?')})")
        # Oil prices
        oil = intel.get("oil_prices", {})
        if oil and (oil.get("wti") or oil.get("brent")):
            oil_parts = []
            if oil.get("wti"):
                oil_parts.append(f"WTI ${oil['wti']:.2f}")
            if oil.get("brent"):
                oil_parts.append(f"Brent ${oil['brent']:.2f}")
            lines.append(f"  • 🛢️ Oil: {' | '.join(oil_parts)}")

    return "\n".join(lines)


def format_brief(intel: Dict[str, Any]) -> str:
    """Full intelligence brief."""
    now = datetime.now(timezone.utc)
    lines = [
        "🌐 WORLDMONITOR v2 INTELLIGENCE BRIEF",
        f"📅 {now.strftime('%A, %B %d, %Y')} | {now.strftime('%H:%M')} UTC",
        "━" * 50,
        "",
    ]
    lines.append(format_section(intel))
    lines.append("")
    lines.append("━" * 50)
    lines.append("Sources: USGS, GDACS, NASA EONET, CoinGecko, Alternative.me, Polymarket, UCDP, GDELT ArtList, GDELT TimelineVolRaw/TimelineTone, UNHCR, Feodo, OpenSky, EIA, World Bank")
    lines.append("Dashboard: worldmonitor.app")
    return "\n".join(lines)


# ==================== MAIN ====================

def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "--json"

    # Allow disabling GDELT via env var when IP is globally rate-limited
    _GDELT_DISABLED = os.environ.get("GDELT_DISABLED", "").strip() in ("1", "true", "yes")

    if _GDELT_DISABLED:
        print("WorldMonitor v2 — GDELT_DISABLED=1, skipping GDELT (16/18 sources)", file=sys.stderr)
    else:
        print("WorldMonitor v2 Intelligence Engine — pulling from 18 sources...", file=sys.stderr)

    # Parallel fetch all sources
    executor = ThreadPoolExecutor(max_workers=18)
    try:
        _ALL_FETCHERS = {
            "earthquakes": fetch_earthquakes,
            "disasters": fetch_gdacs_disasters,
            "nasa_events": fetch_nasa_events,
            "crypto": fetch_crypto,
            "fear_greed": fetch_fear_greed,
            "polymarket": fetch_polymarket,
            "nws_alerts": fetch_nws_alerts,
            "ucdp_conflicts": fetch_ucdp_conflicts,
            "fed_funds_rate": fetch_fed_funds_rate,
            "usa_spending": fetch_usa_spending,
            "climate": fetch_open_meteo_climate,
            "gdelt_gkg": fetch_gdelt_gkg,
            "gdelt_timeline": fetch_gdelt_timeline_signals,
            "unhcr_displacement": fetch_unhcr_displacement,
            "feodo_tracker": fetch_feodo_tracker,
            "opensky_air": fetch_opensky_conflict_zones,
            "oil_prices": fetch_eia_oil_prices,
            "worldbank_gdp": fetch_worldbank_gdp,
        }

        # Skip GDELT when disabled (IP rate-limited, takes 120s+ of 429 errors)
        _GDELT_KEYS = {"gdelt_gkg", "gdelt_timeline"}
        futures = {}
        for name, func in _ALL_FETCHERS.items():
            if _GDELT_DISABLED and name in _GDELT_KEYS:
                continue
            futures[executor.submit(func)] = name

        intel = {"timestamp": datetime.now(timezone.utc).isoformat()}

        # Pre-seed every key so partial failures / timeouts never leave a
        # missing key behind. LIST_KEYS are article/event lists; everything
        # else defaults to a dict.
        _LIST_KEYS = {
            "earthquakes", "disasters", "nasa_events", "polymarket",
            "ucdp_conflicts", "gdelt_gkg",
        }
        for _name in _ALL_FETCHERS:
            intel[_name] = [] if _name in _LIST_KEYS else {}

        try:
            for future in as_completed(futures, timeout=TIMEOUT_TOTAL):
                name = futures[future]
                try:
                    result = future.result()
                    if result is not None:
                        intel[name] = result
                except Exception as e:
                    print(f"  ⚠️ {name} exception: {e}", file=sys.stderr)
        except FuturesTimeoutError:
            print(f"  ⚠️ as_completed timed out after {TIMEOUT_TOTAL}s; using partial data", file=sys.stderr)
            # Collect remaining futures that already finished
            for future, name in list(futures.items()):
                if intel.get(name) not in ([], {}):
                    continue
                try:
                    result = future.result(timeout=3)
                    if result is not None:
                        intel[name] = result
                except Exception:
                    pass
            # Force-kill the thread pool so runaway retries don't block us
            executor.shutdown(wait=False, cancel_futures=True)
        else:
            executor.shutdown(wait=True)
    finally:
        # Belt-and-suspenders: always shut down the executor
        try:
            executor.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass

    # Get previous CII scores for trend (try DB always)
    prev_scores = {}
    try:
        import asyncio
        prev_scores = asyncio.run(get_prev_cii_scores())
    except Exception:
        pass  # No DB or no history yet — trends will show →

    # Compute CII
    cii_scores = compute_cii(
        intel.get("earthquakes", []),
        intel.get("disasters", []),
        intel.get("ucdp_conflicts", []),
        intel.get("gdelt_gkg", []),
        prev_scores,
        intel.get("worldbank_gdp", {})
    )
    intel["cii_scores"] = cii_scores

    # Detect convergence
    convergence = detect_convergence(
        intel.get("earthquakes", []),
        intel.get("disasters", []),
        intel.get("ucdp_conflicts", []),
        intel.get("gdelt_gkg", [])
    )
    intel["convergence"] = convergence

    # Detect anomalies (try DB first, fall back to stateless detection)
    historical_avg = {}
    try:
        import asyncio
        historical_avg = asyncio.run(get_historical_avg())
    except Exception:
        pass
    anomalies = detect_anomalies(intel, historical_avg)
    intel["anomalies"] = anomalies

    # Store to DB if requested
    if mode == "--store":
        import asyncio
        asyncio.run(store_snapshot(intel, cii_scores))
        asyncio.run(store_intel_stories(intel))

    # Output
    counts = {
        "earthquakes": len(intel.get("earthquakes", [])),
        "disasters": len(intel.get("disasters", [])),
        "nasa_events": len(intel.get("nasa_events", [])),
        "polymarket": len(intel.get("polymarket", [])),
        "ucdp_conflicts": len(intel.get("ucdp_conflicts", [])),
        "gdelt_articles": len(intel.get("gdelt_gkg", [])),
        "gdelt_timeline_spikes": len(intel.get("gdelt_timeline", {}).get("spikes", [])) if isinstance(intel.get("gdelt_timeline"), dict) else 0,
        "unhcr": bool(intel.get("unhcr_displacement")),
        "feodo_c2": intel.get("feodo_tracker", {}).get("active_c2", 0),
        "opensky_zones": sum(1 for v in intel.get("opensky_air", {}).values() if v is not None),
        "oil": bool(intel.get("oil_prices", {}).get("wti") or intel.get("oil_prices", {}).get("brent")),
        "gdp_countries": len(intel.get("worldbank_gdp", {})),
    }
    print(f"✓ Collected: {counts}", file=sys.stderr)

    if mode == "--brief":
        print(format_brief(intel))
    elif mode == "--section":
        print(format_section(intel))
    elif mode == "--cii":
        for s in cii_scores:
            print(f"{s['code']}: {s['score']} ({s['level']}) {s['trend']}")
    elif mode == "--alerts":
        if convergence:
            print("⚡ CONVERGENCE:")
            for c in convergence:
                print(f"  • {c}")
        if anomalies:
            print("📊 ANOMALIES:")
            for a in anomalies:
                print(f"  • {a}")
        if not convergence and not anomalies:
            print("No alerts detected.")
    elif mode == "--polymarket":
        for m in intel["polymarket"]:
            vol = m['volume']
            vol_str = f"${vol/1_000_000:.1f}M" if vol >= 1_000_000 else f"${vol/1000:.0f}K"
            print(f"{m['yes_prob']}% — {m['question']} ({vol_str})")
    else:
        print(json.dumps(intel, indent=2, default=str))


if __name__ == "__main__":
    main()
