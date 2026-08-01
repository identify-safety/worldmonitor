#!/usr/bin/env python3
"""
WorldMonitor Intelligence Pull
Fetches real-time data from the same public APIs that WorldMonitor aggregates.
No API keys required for core data.

Usage:
    python3 intel_pull.py              # Full JSON output
    python3 intel_pull.py --brief      # Formatted brief for Telegram
    python3 intel_pull.py --section    # Just the 🌍 GLOBAL SITUATION section
"""

import json
import sys
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta

TIMEOUT = 12  # seconds per request


def fetch_json(url: str) -> dict | list | None:
    """Fetch JSON from a URL with timeout and error handling."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "WorldMonitor-Skill/1.0"})
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            return json.loads(resp.read())
    except Exception as e:
        print(f"  ⚠️ Failed: {url[:60]}... → {e}", file=sys.stderr)
        return None


def get_earthquakes() -> list[dict]:
    """USGS earthquakes M4.5+ in the last 24 hours."""
    data = fetch_json("https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_day.geojson")
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
            "alert": p.get("alert"),
            "tsunami": p.get("tsunami", 0),
            "url": p.get("url"),
        })
    # Sort by magnitude descending
    quakes.sort(key=lambda q: q.get("magnitude", 0) or 0, reverse=True)
    return quakes


def get_gdacs_disasters() -> list[dict]:
    """GDACS orange/red alert disasters — recent only (last 7 days)."""
    data = fetch_json(
        "https://www.gdacs.org/gdacsapi/api/events/geteventlist/SEARCH"
        "?alertlevel=orange;red&eventlist=EQ;TC;FL;VO;DR&limit=50"
    )
    if not data:
        return []
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    events = []
    for feature in data.get("features", []):
        p = feature.get("properties", {})
        # Filter to recent events only
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
            "severity": p.get("severitydata", {}).get("severity") if isinstance(p.get("severitydata"), dict) else None,
            "date": p.get("fromdate"),
            "url": p.get("url"),
        })
    # Deduplicate by type+country (keep highest alert)
    seen = {}
    for e in events:
        key = f"{e.get('type')}|{e.get('country','')}"
        if key not in seen or e.get("alert_level") == "Red":
            seen[key] = e
    events = list(seen.values())
    # Prioritize: Red > Orange, then by date
    events.sort(key=lambda e: (0 if e.get("alert_level") == "Red" else 1, e.get("date", "") or ""), reverse=False)
    return events[:10]  # Cap at 10 most important


def get_nasa_events() -> list[dict]:
    """NASA EONET active natural events."""
    data = fetch_json("https://eonet.gsfc.nasa.gov/api/v3/events?status=open&limit=15")
    if not data:
        return []
    events = []
    for e in data.get("events", []):
        categories = [c.get("title") for c in e.get("categories", [])]
        sources = [s.get("url") for s in e.get("sources", [])]
        events.append({
            "title": e.get("title"),
            "categories": categories,
            "date": e.get("geometry", [{}])[-1].get("date") if e.get("geometry") else None,
            "source": sources[0] if sources else None,
        })
    return events


def get_crypto() -> dict:
    """CoinGecko crypto prices with 24h change."""
    data = fetch_json(
        "https://api.coingecko.com/api/v3/simple/price"
        "?ids=bitcoin,ethereum,solana,ripple"
        "&vs_currencies=usd&include_24hr_change=true&include_market_cap=true"
    )
    if not data:
        return {}
    result = {}
    name_map = {"bitcoin": "BTC", "ethereum": "ETH", "solana": "SOL", "ripple": "XRP"}
    for coin_id, symbol in name_map.items():
        if coin_id in data:
            d = data[coin_id]
            result[symbol] = {
                "price": d.get("usd"),
                "change_24h": round(d.get("usd_24h_change", 0), 2),
                "market_cap": d.get("usd_market_cap"),
            }
    return result


def get_fear_greed() -> dict:
    """Crypto Fear & Greed Index."""
    data = fetch_json("https://api.alternative.me/fng/?limit=1")
    if not data or not data.get("data"):
        return {}
    entry = data["data"][0]
    return {
        "value": int(entry.get("value", 0)),
        "label": entry.get("value_classification", "Unknown"),
        "timestamp": entry.get("timestamp"),
    }


def format_section(intel: dict) -> str:
    """Format the 🌍 GLOBAL SITUATION section for Telegram."""
    lines = []
    lines.append("🌍 GLOBAL SITUATION (WorldMonitor)")
    lines.append("")

    # Earthquakes
    quakes = intel.get("earthquakes", [])
    sig_quakes = [q for q in quakes if (q.get("magnitude") or 0) >= 5.0]
    if sig_quakes:
        lines.append("🌋 Seismic Activity")
        for q in sig_quakes[:5]:
            mag = q.get("magnitude", "?")
            place = q.get("place", "Unknown")
            depth = q.get("depth_km")
            alert = q.get("alert")
            tsunami = " 🌊TSUNAMI" if q.get("tsunami") else ""
            alert_str = f" [{alert.upper()}]" if alert else ""
            depth_str = f" ({depth}km deep)" if depth else ""
            lines.append(f"  • M{mag} — {place}{depth_str}{alert_str}{tsunami}")
    elif quakes:
        lines.append("🌋 Seismic Activity")
        lines.append(f"  • {len(quakes)} earthquakes M4.5+ in 24h, largest M{quakes[0].get('magnitude', '?')} — {quakes[0].get('place', 'Unknown')}")
    else:
        lines.append("🌋 Seismic: No significant activity (M4.5+)")
    lines.append("")

    # GDACS Disasters
    disasters = intel.get("disasters", [])
    if disasters:
        lines.append("⚠️ Active Disasters (GDACS)")
        type_emoji = {"EQ": "🔴", "TC": "🌀", "FL": "🌊", "VO": "🌋", "DR": "☀️"}
        type_label = {"EQ": "Earthquake", "TC": "Cyclone", "FL": "Flood", "VO": "Volcano", "DR": "Drought"}
        for d in disasters[:6]:
            emoji = type_emoji.get(d.get("type"), "🔶")
            name = d.get("name") or type_label.get(d.get("type"), d.get("type", "Unknown"))
            # Truncate long country lists
            country = d.get("country", "")
            if country and len(country) > 40:
                parts = [c.strip() for c in country.split(",")]
                country = ", ".join(parts[:2]) + f" +{len(parts)-2} more" if len(parts) > 2 else ", ".join(parts)
            alert = d.get("alert_level", "")
            alert_emoji = "🔴" if alert == "Red" else "🟠"
            country_str = f" — {country}" if country else ""
            lines.append(f"  {emoji} {name}{country_str} {alert_emoji}")
        if len(disasters) > 6:
            lines.append(f"  ... +{len(disasters) - 6} more active alerts")
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

    # Crypto
    crypto = intel.get("crypto", {})
    fng = intel.get("fear_greed", {})
    if crypto:
        lines.append("🪙 Crypto Radar")
        parts = []
        for symbol in ["BTC", "ETH", "SOL", "XRP"]:
            if symbol in crypto:
                c = crypto[symbol]
                price = c.get("price", 0)
                change = c.get("change_24h", 0)
                arrow = "↑" if change >= 0 else "↓"
                if price >= 1000:
                    price_str = f"${price:,.0f}"
                else:
                    price_str = f"${price:,.2f}"
                parts.append(f"{symbol}: {price_str} ({arrow}{abs(change):.1f}%)")
        lines.append(f"  • {' | '.join(parts[:2])}")
        if len(parts) > 2:
            lines.append(f"  • {' | '.join(parts[2:])}")
        if fng:
            lines.append(f"  • Fear & Greed: {fng.get('value', '?')} ({fng.get('label', '?')})")

    return "\n".join(lines)


def format_brief(intel: dict) -> str:
    """Full formatted brief."""
    now = datetime.now(timezone.utc)
    lines = [
        f"🌐 WORLDMONITOR INTELLIGENCE PULL",
        f"📅 {now.strftime('%A, %B %d, %Y')} | {now.strftime('%H:%M')} UTC",
        "━" * 34,
        "",
    ]
    lines.append(format_section(intel))
    lines.append("")
    lines.append("━" * 34)
    lines.append(f"Sources: USGS, GDACS, NASA EONET, CoinGecko, Alternative.me")
    lines.append(f"Dashboard: worldmonitor.app")
    return "\n".join(lines)


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "--json"

    print("Pulling global intelligence...", file=sys.stderr)

    intel = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "earthquakes": get_earthquakes(),
        "disasters": get_gdacs_disasters(),
        "nasa_events": get_nasa_events(),
        "crypto": get_crypto(),
        "fear_greed": get_fear_greed(),
    }

    counts = {
        "earthquakes": len(intel["earthquakes"]),
        "disasters": len(intel["disasters"]),
        "nasa_events": len(intel["nasa_events"]),
        "crypto_coins": len(intel["crypto"]),
    }
    print(f"Collected: {counts}", file=sys.stderr)

    if mode == "--brief":
        print(format_brief(intel))
    elif mode == "--section":
        print(format_section(intel))
    else:
        print(json.dumps(intel, indent=2, default=str))


if __name__ == "__main__":
    main()
