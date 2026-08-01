#!/usr/bin/env python3
"""
WorldMonitor — Keyword-based Threat Classifier
Port of WorldMonitor's threat-classifier.ts

Classifies news headlines/events into threat levels and categories
using keyword matching. Used to enrich CII and event tracking.
"""

from typing import Optional

# Threat levels with priority
THREAT_PRIORITY = {
    "critical": 5,
    "high": 4,
    "medium": 3,
    "low": 2,
    "info": 1,
}

# Critical keywords → category
CRITICAL_KEYWORDS = {
    "nuclear strike": "military", "nuclear attack": "military", "nuclear war": "military",
    "invasion": "conflict", "declaration of war": "conflict", "martial law": "military",
    "coup": "military", "coup attempt": "military", "genocide": "conflict",
    "ethnic cleansing": "conflict", "chemical attack": "terrorism",
    "biological attack": "terrorism", "dirty bomb": "terrorism",
    "mass casualty": "conflict", "pandemic declared": "health",
    "health emergency": "health", "nato article 5": "military",
    "evacuation order": "disaster", "nuclear meltdown": "disaster",
    "magnitude 8": "disaster", "magnitude 9": "disaster",
    "category 5": "disaster", "dam collapse": "disaster",
}

HIGH_KEYWORDS = {
    "airstrike": "military", "air strike": "military", "missile strike": "military",
    "drone strike": "military", "shelling": "conflict", "offensive": "conflict",
    "counteroffensive": "conflict", "ceasefire violation": "conflict",
    "arms deal": "military", "military buildup": "military",
    "troop movement": "military", "naval blockade": "military",
    "sanctions": "diplomatic", "embargo": "economic",
    "mass protest": "protest", "riot": "protest", "state of emergency": "disaster",
    "assassination": "conflict", "hostage": "terrorism",
    "cyberattack": "cyber", "ransomware": "cyber", "data breach": "cyber",
    "critical infrastructure": "infrastructure",
    "category 4": "disaster", "magnitude 7": "disaster",
    "tsunami warning": "disaster", "wildfire": "disaster",
    "famine": "health", "epidemic": "health",
    "refugee crisis": "conflict", "displacement": "conflict",
    "blackout": "infrastructure", "internet shutdown": "infrastructure",
}

MEDIUM_KEYWORDS = {
    "protest": "protest", "demonstration": "protest", "strike": "economic",
    "military exercise": "military", "naval exercise": "military",
    "diplomatic tension": "diplomatic", "trade war": "economic",
    "tariff": "economic", "recession": "economic", "inflation": "economic",
    "earthquake": "disaster", "hurricane": "disaster", "cyclone": "disaster",
    "typhoon": "disaster", "flood": "disaster", "drought": "disaster",
    "volcano": "disaster", "landslide": "disaster",
    "election": "diplomatic", "referendum": "diplomatic",
    "border clash": "conflict", "skirmish": "conflict",
    "hacking": "cyber", "phishing": "cyber", "malware": "cyber",
    "pipeline": "infrastructure", "cable cut": "infrastructure",
    "power outage": "infrastructure",
}

LOW_KEYWORDS = {
    "diplomatic meeting": "diplomatic", "summit": "diplomatic",
    "trade agreement": "economic", "bilateral talks": "diplomatic",
    "peacekeeping": "military", "humanitarian aid": "health",
    "ceasefire": "conflict", "peace talks": "diplomatic",
    "tropical storm": "disaster", "minor earthquake": "disaster",
}


def classify_text(text: str) -> dict:
    """
    Classify a text string (headline, event title, etc.) into threat level and category.
    Returns: {"level": str, "category": str, "confidence": float, "keywords_matched": list}
    """
    text_lower = text.lower()
    matched = []

    # Check critical first
    for keyword, category in CRITICAL_KEYWORDS.items():
        if keyword in text_lower:
            matched.append({"keyword": keyword, "level": "critical", "category": category})

    if matched:
        return {
            "level": "critical",
            "category": matched[0]["category"],
            "confidence": 0.9,
            "keywords_matched": [m["keyword"] for m in matched],
        }

    # Check high
    for keyword, category in HIGH_KEYWORDS.items():
        if keyword in text_lower:
            matched.append({"keyword": keyword, "level": "high", "category": category})

    if matched:
        return {
            "level": "high",
            "category": matched[0]["category"],
            "confidence": 0.8,
            "keywords_matched": [m["keyword"] for m in matched],
        }

    # Check medium
    for keyword, category in MEDIUM_KEYWORDS.items():
        if keyword in text_lower:
            matched.append({"keyword": keyword, "level": "medium", "category": category})

    if matched:
        return {
            "level": "medium",
            "category": matched[0]["category"],
            "confidence": 0.7,
            "keywords_matched": [m["keyword"] for m in matched],
        }

    # Check low
    for keyword, category in LOW_KEYWORDS.items():
        if keyword in text_lower:
            matched.append({"keyword": keyword, "level": "low", "category": category})

    if matched:
        return {
            "level": "low",
            "category": matched[0]["category"],
            "confidence": 0.6,
            "keywords_matched": [m["keyword"] for m in matched],
        }

    return {
        "level": "info",
        "category": "general",
        "confidence": 0.3,
        "keywords_matched": [],
    }


# Named global hotspots with static baseline risk (from WorldMonitor)
HOTSPOTS = {
    "kyiv": {"country": "UA", "baseline": 85, "lat": 50.45, "lon": 30.52},
    "tehran": {"country": "IR", "baseline": 65, "lat": 35.69, "lon": 51.39},
    "taipei": {"country": "TW", "baseline": 55, "lat": 25.03, "lon": 121.57},
    "telaviv": {"country": "IL", "baseline": 70, "lat": 32.09, "lon": 34.78},
    "pyongyang": {"country": "KP", "baseline": 60, "lat": 39.02, "lon": 125.75},
    "moscow": {"country": "RU", "baseline": 50, "lat": 55.76, "lon": 37.62},
    "beijing": {"country": "CN", "baseline": 40, "lat": 39.91, "lon": 116.40},
    "sanaa": {"country": "YE", "baseline": 75, "lat": 15.35, "lon": 44.21},
    "damascus": {"country": "SY", "baseline": 70, "lat": 33.51, "lon": 36.29},
    "caracas": {"country": "VE", "baseline": 55, "lat": 10.48, "lon": -66.90},
    "naypyidaw": {"country": "MM", "baseline": 60, "lat": 19.76, "lon": 96.07},
    "kabul": {"country": "AF", "baseline": 70, "lat": 34.53, "lon": 69.17},
    "sahel": {"country": "ML", "baseline": 65, "lat": 17.57, "lon": -3.99},
    "mogadishu": {"country": "SO", "baseline": 70, "lat": 2.05, "lon": 45.32},
    "khartoum": {"country": "SD", "baseline": 75, "lat": 15.50, "lon": 32.56},
}


def score_hotspot(hotspot_id: str, news_count: int = 0, cii_score: int = 0,
                  disaster_nearby: bool = False, military_activity: bool = False) -> dict:
    """
    Calculate dynamic escalation score for a named hotspot.
    Ported from WorldMonitor's hotspot-escalation.ts
    """
    hs = HOTSPOTS.get(hotspot_id)
    if not hs:
        return {"hotspot": hotspot_id, "score": 0, "trend": "unknown"}

    baseline = hs["baseline"]

    # Component weights (from WorldMonitor)
    news_score = min(news_count * 5, 30)  # Max 30 from news
    cii_contribution = min(cii_score * 0.3, 25)  # Max 25 from CII
    geo_score = 15 if disaster_nearby else 0  # 15 if disaster nearby
    military_score = 15 if military_activity else 0  # 15 if military activity

    dynamic = (
        news_score * 0.35 +
        cii_contribution * 0.25 +
        geo_score * 0.25 +
        military_score * 0.15
    )

    combined = min(baseline + dynamic, 100)

    if combined >= 80:
        trend = "critical"
    elif combined >= 60:
        trend = "high"
    elif combined >= 40:
        trend = "elevated"
    else:
        trend = "normal"

    return {
        "hotspot": hotspot_id,
        "country": hs["country"],
        "score": round(combined),
        "trend": trend,
        "components": {
            "baseline": baseline,
            "news": round(news_score, 1),
            "cii": round(cii_contribution, 1),
            "geo": geo_score,
            "military": military_score,
        },
    }


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        text = " ".join(sys.argv[1:])
        result = classify_text(text)
        import json
        print(json.dumps(result, indent=2))
    else:
        # Demo
        examples = [
            "Russia launches massive missile strike on Kyiv, killing 12",
            "Iran university protests spread to multiple campuses",
            "Bitcoin crashes 10% after Bybit hack anniversary",
            "Tropical Cyclone GARANCE makes landfall in La Reunion",
            "US and China hold diplomatic talks in Geneva",
            "Nuclear submarine detected near Taiwan Strait",
        ]
        for ex in examples:
            r = classify_text(ex)
            emoji = {"critical": "🔴", "high": "🟠", "medium": "🟡", "low": "🟢", "info": "⚪"}.get(r["level"], "?")
            print(f'{emoji} [{r["level"]:8s}] [{r["category"]:14s}] {ex[:60]}')
