#!/usr/bin/env python3
"""
WorldMonitor v2 — Event Tracker & Lifecycle Manager
Autonomous tracking of global events with escalation detection,
auto-close, follow-up generation, and report continuity.

Usage:
    python3 event_tracker.py ingest              # Ingest current intel_engine data into tracked events
    python3 event_tracker.py status              # Show all active/escalating events
    python3 event_tracker.py briefing-context    # Get context for morning briefing (what changed since last report)
    python3 event_tracker.py follow-ups          # List pending follow-up investigations
    python3 event_tracker.py complete-followup ID "findings"  # Mark a follow-up complete
    python3 event_tracker.py close EVENT_ID      # Manually close an event
    python3 event_tracker.py report TYPE         # Log that a report was delivered
    python3 event_tracker.py auto-close          # Auto-close stale events
    python3 event_tracker.py stats               # DB stats overview
"""

import asyncio
import json
import os
import sys
import subprocess
from datetime import datetime, timezone, timedelta
from typing import Optional, Any

# ==================== DATABASE ====================

_db_url = None

def get_db_url() -> str:
    global _db_url
    if _db_url:
        return _db_url
    _db_url = os.environ.get("DATABASE_URL") or os.environ.get("NEON_DATABASE_URL")
    if not _db_url:
        config_path = os.path.expanduser(os.environ.get("WORLDMONITOR_DB_ENV_FILE", "~/.config/worldmonitor/neon.env"))
        if os.path.exists(config_path):
            with open(config_path) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("NEON_DATABASE_URL=") or line.startswith("DATABASE_URL="):
                        _db_url = line.split("=", 1)[1].strip()
                        break
    if not _db_url:
        raise RuntimeError("DATABASE_URL not found")
    return _db_url


async def db_exec(sql: str, *args) -> None:
    import asyncpg
    conn = await asyncpg.connect(get_db_url())
    try:
        await conn.execute(sql, *args)
    finally:
        await conn.close()


async def db_fetch(sql: str, *args) -> list:
    import asyncpg
    conn = await asyncpg.connect(get_db_url())
    try:
        rows = await conn.fetch(sql, *args)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


async def db_fetchrow(sql: str, *args) -> Optional[dict]:
    import asyncpg
    conn = await asyncpg.connect(get_db_url())
    try:
        row = await conn.fetchrow(sql, *args)
        return dict(row) if row else None
    finally:
        await conn.close()


# ==================== EVENT TYPE MAPPING ====================

def classify_severity(event_type: str, data: dict) -> str:
    """Determine severity from event data."""
    if event_type == "earthquake":
        mag = data.get("magnitude", 0) or 0
        if mag >= 7.0: return "critical"
        if mag >= 6.0: return "high"
        if mag >= 5.0: return "medium"
        return "low"
    elif event_type == "disaster":
        return "critical" if data.get("alert_level") == "Red" else "high"
    elif event_type == "cyclone":
        return "critical" if data.get("alert_level") == "Red" else "high"
    elif event_type == "conflict":
        deaths = data.get("deaths_low", 0) or 0
        if deaths >= 100: return "critical"
        if deaths >= 10: return "high"
        return "medium"
    elif event_type == "market_event":
        change = abs(data.get("change_24h", 0) or 0)
        if change >= 10: return "critical"
        if change >= 5: return "high"
        return "medium"
    elif event_type == "prediction_market":
        return "medium"
    return "medium"


def generate_external_id(event_type: str, data: dict) -> str:
    """Generate a stable external ID for deduplication."""
    if event_type == "earthquake":
        # Use place + rounded magnitude as ID
        place = (data.get("place") or "unknown").lower().replace(" ", "_")[:40]
        mag = data.get("magnitude", 0)
        return f"eq_{place}_{mag}"
    elif event_type in ("disaster", "cyclone"):
        name = (data.get("name") or "unknown").lower().replace(" ", "_")[:40]
        return f"dis_{name}"
    elif event_type == "conflict":
        country = (data.get("country") or "unknown").lower().replace(" ", "_")[:20]
        side_a = (data.get("side_a") or "unknown").lower().replace(" ", "_")[:20]
        return f"conf_{country}_{side_a}"
    elif event_type == "market_event":
        return f"mkt_{data.get('symbol', 'unknown')}_{datetime.now(timezone.utc).strftime('%Y%m%d')}"
    elif event_type == "prediction_market":
        slug = (data.get("slug") or "unknown")[:50]
        return f"poly_{slug}"
    elif event_type == "nasa_event":
        title = (data.get("title") or "unknown").lower().replace(" ", "_")[:40]
        return f"nasa_{title}"
    return f"{event_type}_{hash(json.dumps(data, default=str)) % 100000}"


def extract_country_code(event_type: str, data: dict) -> Optional[str]:
    """Try to extract a 2-letter country code from event data."""
    COUNTRY_MAP = {
        "alaska": "US", "united states": "US", "hawaii": "US", "california": "US",
        "russia": "RU", "china": "CN", "myanmar": "MM", "burma": "MM",
        "ukraine": "UA", "iran": "IR", "israel": "IL", "gaza": "IL",
        "taiwan": "TW", "japan": "JP", "indonesia": "ID", "philippines": "PH",
        "mexico": "MX", "brazil": "BR", "india": "IN", "pakistan": "PK",
        "turkey": "TR", "syria": "SY", "yemen": "YE", "afghanistan": "AF",
        "la reunion": "FR", "madagascar": "MG", "mozambique": "MZ",
        "north korea": "KP", "south korea": "KR", "saudi": "SA",
        "venezuela": "VE", "colombia": "CO", "tonga": "TO", "fiji": "FJ",
        "papua new guinea": "PG", "viet nam": "VN", "laos": "LA",
        "tanzania": "TZ", "malaysia": "MY",
    }
    text = " ".join([
        str(data.get("place", "")), str(data.get("country", "")),
        str(data.get("title", "")), str(data.get("name", ""))
    ]).lower()
    for keyword, code in COUNTRY_MAP.items():
        if keyword in text:
            return code
    return data.get("country_code")


# ==================== INGEST ====================

async def ingest_events():
    """Run the intel engine and ingest all events into the tracking DB."""
    import asyncpg

    # Run the engine
    print("Running intel engine...", file=sys.stderr)
    engine_path = os.path.join(os.path.dirname(__file__), "intel_engine.py")
    result = subprocess.run(
        [sys.executable, engine_path, "--json"],
        capture_output=True, text=True, timeout=90
    )
    if result.returncode != 0:
        print(f"Engine failed: {result.stderr}", file=sys.stderr)
        return

    intel = json.loads(result.stdout)
    conn = await asyncpg.connect(get_db_url())

    stats = {"new": 0, "updated": 0, "escalated": 0, "closed": 0}

    try:
        # --- Earthquakes ---
        for q in intel.get("earthquakes", []):
            if (q.get("magnitude") or 0) < 5.0:
                continue
            ext_id = generate_external_id("earthquake", q)
            severity = classify_severity("earthquake", q)
            country = extract_country_code("earthquake", q)
            title = f"M{q['magnitude']} Earthquake — {q.get('place', 'Unknown')}"

            existing = await conn.fetchrow(
                "SELECT id, severity, status FROM worldmonitor_events WHERE event_type = 'earthquake' AND external_id = $1",
                ext_id
            )
            if existing:
                # Update — check for escalation
                if severity_rank(severity) > severity_rank(existing["severity"]):
                    await log_escalation(conn, existing["id"], "severity_increase",
                        {"severity": existing["severity"]}, {"severity": severity},
                        f"Earthquake severity increased from {existing['severity']} to {severity}")
                    stats["escalated"] += 1
                await conn.execute(
                    "UPDATE worldmonitor_events SET last_updated = NOW(), severity = $1, metadata = $2 WHERE id = $3",
                    severity, json.dumps(q, default=str), existing["id"]
                )
                stats["updated"] += 1
            else:
                await conn.execute("""
                    INSERT INTO worldmonitor_events (event_type, event_source, external_id, title, country_code, severity, status, metadata)
                    VALUES ('earthquake', 'USGS', $1, $2, $3, $4, 'active', $5)
                    ON CONFLICT (event_type, external_id) DO NOTHING
                """, ext_id, title, country, severity, json.dumps(q, default=str))
                stats["new"] += 1
                # Generate follow-up for significant quakes
                if severity in ("high", "critical"):
                    event = await conn.fetchrow(
                        "SELECT id FROM worldmonitor_events WHERE event_type = 'earthquake' AND external_id = $1", ext_id)
                    if event:
                        await generate_follow_up(conn, event["id"],
                            f"Investigate damage, casualties, and response for {title}",
                            [f"{q.get('place', '')} earthquake damage casualties",
                             f"{q.get('place', '')} earthquake response aid"],
                            priority=8 if severity == "critical" else 6)

        # --- Disasters / Cyclones ---
        for d in intel.get("disasters", []):
            etype = "cyclone" if d.get("type") == "TC" else "disaster"
            ext_id = generate_external_id(etype, d)
            severity = classify_severity(etype, d)
            country = extract_country_code(etype, d)
            title = d.get("name") or f"Disaster in {d.get('country', 'Unknown')}"

            existing = await conn.fetchrow(
                "SELECT id, severity, status FROM worldmonitor_events WHERE event_type = $1 AND external_id = $2",
                etype, ext_id
            )
            if existing:
                if severity_rank(severity) > severity_rank(existing["severity"]):
                    await log_escalation(conn, existing["id"], "severity_increase",
                        {"severity": existing["severity"]}, {"severity": severity},
                        f"{title} escalated to {severity}")
                    stats["escalated"] += 1
                await conn.execute(
                    "UPDATE worldmonitor_events SET last_updated = NOW(), severity = $1, metadata = $2 WHERE id = $3",
                    severity, json.dumps(d, default=str), existing["id"]
                )
                stats["updated"] += 1
            else:
                await conn.execute("""
                    INSERT INTO worldmonitor_events (event_type, event_source, external_id, title, country_code, severity, status, metadata)
                    VALUES ($1, 'GDACS', $2, $3, $4, $5, 'active', $6)
                    ON CONFLICT (event_type, external_id) DO NOTHING
                """, etype, ext_id, title, country, severity, json.dumps(d, default=str))
                stats["new"] += 1
                if severity == "critical":
                    event = await conn.fetchrow(
                        "SELECT id FROM worldmonitor_events WHERE event_type = $1 AND external_id = $2", etype, ext_id)
                    if event:
                        await generate_follow_up(conn, event["id"],
                            f"Track {title}: casualties, displacement, response status",
                            [f"{title} casualties damage", f"{d.get('country','')} disaster response"],
                            priority=8)

        # --- Polymarket ---
        for m in intel.get("polymarket", []):
            ext_id = generate_external_id("prediction_market", m)
            title = m.get("question", "")[:200]
            existing = await conn.fetchrow(
                "SELECT id, metadata FROM worldmonitor_events WHERE event_type = 'prediction_market' AND external_id = $1",
                ext_id
            )
            if existing:
                old_meta = json.loads(existing["metadata"]) if isinstance(existing["metadata"], str) else (existing["metadata"] or {})
                old_prob = old_meta.get("yes_prob", 50)
                new_prob = m.get("yes_prob", 50)
                # Detect significant probability shift (>15 points)
                if abs(new_prob - old_prob) >= 15:
                    await log_escalation(conn, existing["id"], "probability_shift",
                        {"yes_prob": old_prob}, {"yes_prob": new_prob},
                        f"Polymarket '{title[:60]}' shifted from {old_prob}% to {new_prob}%")
                    stats["escalated"] += 1
                await conn.execute(
                    "UPDATE worldmonitor_events SET last_updated = NOW(), metadata = $1 WHERE id = $2",
                    json.dumps(m, default=str), existing["id"]
                )
                stats["updated"] += 1
            else:
                await conn.execute("""
                    INSERT INTO worldmonitor_events (event_type, event_source, external_id, title, severity, status, metadata)
                    VALUES ('prediction_market', 'Polymarket', $1, $2, 'medium', 'active', $3)
                    ON CONFLICT (event_type, external_id) DO NOTHING
                """, ext_id, title, json.dumps(m, default=str))
                stats["new"] += 1

        # --- Crypto market events ---
        crypto = intel.get("crypto", {})
        fng = intel.get("fear_greed", {})
        for symbol, data in crypto.items():
            change = abs(data.get("change_24h", 0) or 0)
            if change >= 3 or (fng.get("value", 50) <= 15):
                data["symbol"] = symbol
                data["fear_greed"] = fng.get("value")
                data["fear_greed_label"] = fng.get("label")
                ext_id = generate_external_id("market_event", data)
                severity = classify_severity("market_event", data)
                direction = "crash" if data.get("change_24h", 0) < 0 else "surge"
                title = f"{symbol} {direction} {abs(data.get('change_24h',0)):.1f}% — F&G: {fng.get('value','?')}"

                await conn.execute("""
                    INSERT INTO worldmonitor_events (event_type, event_source, external_id, title, severity, status, metadata)
                    VALUES ('market_event', 'CoinGecko', $1, $2, $3, 'active', $4)
                    ON CONFLICT (event_type, external_id) DO UPDATE SET
                        last_updated = NOW(), metadata = EXCLUDED.metadata, severity = EXCLUDED.severity
                """, ext_id, title, severity, json.dumps(data, default=str))
                stats["new"] += 1

        # --- CII escalations ---
        cii = intel.get("cii_scores", [])
        for score in cii:
            if score.get("level") in ("high", "critical"):
                ext_id = f"cii_{score['code']}_{datetime.now(timezone.utc).strftime('%Y%m')}"
                title = f"CII {score['level'].upper()}: {score['name']} ({score['score']})"
                await conn.execute("""
                    INSERT INTO worldmonitor_events (event_type, event_source, external_id, title, country_code, severity, status, metadata)
                    VALUES ('cii_alert', 'WorldMonitor', $1, $2, $3, $4, 'active', $5)
                    ON CONFLICT (event_type, external_id) DO UPDATE SET
                        last_updated = NOW(), severity = EXCLUDED.severity, title = EXCLUDED.title, metadata = EXCLUDED.metadata
                """, ext_id, title, score["code"],
                     "critical" if score["level"] == "critical" else "high",
                     json.dumps(score, default=str))

        # --- NASA events ---
        for e in intel.get("nasa_events", [])[:10]:
            cats = e.get("categories", [])
            # Only track significant ones (storms, not prescribed fires)
            if any("Storm" in c or "Volcano" in c or "Flood" in c or "Drought" in c for c in cats):
                ext_id = generate_external_id("nasa_event", e)
                title = e.get("title", "Unknown NASA Event")
                await conn.execute("""
                    INSERT INTO worldmonitor_events (event_type, event_source, external_id, title, severity, status, metadata)
                    VALUES ('nasa_event', 'NASA_EONET', $1, $2, 'medium', 'active', $3)
                    ON CONFLICT (event_type, external_id) DO UPDATE SET last_updated = NOW()
                """, ext_id, title, json.dumps(e, default=str))
                stats["new"] += 1

    finally:
        await conn.close()

    print(json.dumps(stats))


# ==================== ESCALATION ====================

def severity_rank(s: str) -> int:
    return {"low": 0, "medium": 1, "high": 2, "critical": 3}.get(s, 0)


async def log_escalation(conn, event_id: int, esc_type: str, prev: dict, new: dict, desc: str):
    await conn.execute("""
        INSERT INTO worldmonitor_escalations (event_id, escalation_type, previous_state, new_state, description)
        VALUES ($1, $2, $3, $4, $5)
    """, event_id, esc_type, json.dumps(prev), json.dumps(new), desc)
    # Bump follow-up priority
    await conn.execute(
        "UPDATE worldmonitor_events SET follow_up_priority = GREATEST(follow_up_priority, 7), status = 'escalating' WHERE id = $1",
        event_id
    )


# ==================== FOLLOW-UPS ====================

async def generate_follow_up(conn, event_id: int, question: str, queries: list, priority: int = 5):
    # Don't duplicate
    existing = await conn.fetchrow(
        "SELECT id FROM worldmonitor_follow_ups WHERE event_id = $1 AND question = $2 AND status = 'pending'",
        event_id, question
    )
    if not existing:
        await conn.execute("""
            INSERT INTO worldmonitor_follow_ups (event_id, priority, question, search_queries, status)
            VALUES ($1, $2, $3, $4, 'pending')
        """, event_id, priority, question, queries)


# ==================== AUTO-CLOSE ====================

async def auto_close_stale():
    """Close events not updated in N days based on type."""
    STALE_DAYS = {
        "earthquake": 3,
        "cyclone": 7,
        "disaster": 14,
        "market_event": 2,
        "prediction_market": 30,
        "nasa_event": 7,
        "conflict": 30,
        "cii_alert": 14,
    }
    stats = {"closed": 0}
    for etype, days in STALE_DAYS.items():
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        rows = await db_fetch(
            "SELECT id, title FROM worldmonitor_events WHERE event_type = $1 AND status IN ('new','active','de-escalating') AND last_updated < $2",
            etype, cutoff
        )
        for row in rows:
            await db_exec(
                "UPDATE worldmonitor_events SET status = 'closed', resolved_at = NOW() WHERE id = $1",
                row["id"]
            )
            stats["closed"] += 1
            print(f"  Auto-closed: {row['title'][:60]}", file=sys.stderr)

    # Also close prediction markets that have resolved (prob >= 99 or <= 1)
    resolved = await db_fetch("""
        SELECT id, title FROM worldmonitor_events
        WHERE event_type = 'prediction_market' AND status NOT IN ('closed','resolved')
        AND (metadata->>'yes_prob')::float >= 99.0 OR (metadata->>'yes_prob')::float <= 1.0
    """)
    for row in resolved:
        await db_exec(
            "UPDATE worldmonitor_events SET status = 'resolved', resolved_at = NOW() WHERE id = $1",
            row["id"]
        )
        stats["closed"] += 1

    print(json.dumps(stats))


# ==================== BRIEFING CONTEXT ====================

async def get_briefing_context():
    """Generate context for the morning briefing: what changed since last report."""
    context = {}

    # Last report
    last_report = await db_fetchrow(
        "SELECT * FROM worldmonitor_reports ORDER BY delivered_at DESC LIMIT 1"
    )
    context["last_report"] = {
        "date": str(last_report["report_date"]) if last_report else "none",
        "type": last_report["report_type"] if last_report else "none",
    } if last_report else None

    # New events since last report
    since = last_report["delivered_at"] if last_report else datetime.now(timezone.utc) - timedelta(days=1)
    new_events = await db_fetch(
        "SELECT id, event_type, title, severity, country_code FROM worldmonitor_events WHERE first_seen > $1 ORDER BY severity DESC, first_seen DESC",
        since
    )
    context["new_events"] = new_events

    # Escalations since last report
    escalations = await db_fetch(
        "SELECT e.description, e.escalation_type, ev.title as event_title FROM worldmonitor_escalations e JOIN worldmonitor_events ev ON e.event_id = ev.id WHERE e.detected_at > $1 ORDER BY e.detected_at DESC",
        since
    )
    context["escalations"] = escalations

    # Active high-priority events
    active_critical = await db_fetch(
        "SELECT id, event_type, title, severity, status, country_code, follow_up_priority, first_seen, last_updated FROM worldmonitor_events WHERE status IN ('active','escalating','new') AND severity IN ('high','critical') ORDER BY follow_up_priority DESC, severity DESC"
    )
    context["active_critical"] = active_critical

    # Recently resolved
    recently_resolved = await db_fetch(
        "SELECT id, event_type, title, resolved_at FROM worldmonitor_events WHERE status IN ('resolved','closed') AND resolved_at > $1 ORDER BY resolved_at DESC",
        since
    )
    context["recently_resolved"] = recently_resolved

    # Pending follow-ups
    follow_ups = await db_fetch(
        "SELECT f.id, f.question, f.priority, f.search_queries, ev.title as event_title FROM worldmonitor_follow_ups f JOIN worldmonitor_events ev ON f.event_id = ev.id WHERE f.status = 'pending' ORDER BY f.priority DESC LIMIT 10"
    )
    context["pending_follow_ups"] = follow_ups

    # Continuing stories (reported before, still active)
    continuing = await db_fetch(
        "SELECT id, event_type, title, severity, status, reported_count, last_reported_at, first_seen FROM worldmonitor_events WHERE status IN ('active','escalating') AND reported_count > 0 ORDER BY follow_up_priority DESC, last_updated DESC LIMIT 15"
    )
    context["continuing_stories"] = continuing

    print(json.dumps(context, default=str, indent=2))


# ==================== REPORT LOGGING ====================

async def log_report(report_type: str):
    """Log that a report was delivered."""
    # Get active event IDs
    active = await db_fetch(
        "SELECT id FROM worldmonitor_events WHERE status IN ('new','active','escalating') AND severity IN ('high','critical')"
    )
    event_ids = [r["id"] for r in active]

    # Get pending follow-ups
    follow_ups = await db_fetch(
        "SELECT question FROM worldmonitor_follow_ups WHERE status = 'pending' ORDER BY priority DESC LIMIT 5"
    )

    # Count stats
    new_count = len(await db_fetch(
        "SELECT id FROM worldmonitor_events WHERE first_seen > NOW() - INTERVAL '24 hours'"
    ))
    esc_count = len(await db_fetch(
        "SELECT id FROM worldmonitor_escalations WHERE detected_at > NOW() - INTERVAL '24 hours'"
    ))
    resolved_count = len(await db_fetch(
        "SELECT id FROM worldmonitor_events WHERE resolved_at > NOW() - INTERVAL '24 hours'"
    ))

    await db_exec("""
        INSERT INTO worldmonitor_reports (report_type, event_ids, follow_up_items, new_events_count, escalations_count, resolved_count)
        VALUES ($1, $2, $3, $4, $5, $6)
    """, report_type, event_ids,
         [f["question"] for f in follow_ups],
         new_count, esc_count, resolved_count)

    # Mark events as reported
    if event_ids:
        await db_exec(
            "UPDATE worldmonitor_events SET reported_count = reported_count + 1, last_reported_at = NOW() WHERE id = ANY($1::int[])",
            event_ids
        )

    print(json.dumps({"logged": True, "events": len(event_ids), "new": new_count, "escalated": esc_count, "resolved": resolved_count}))


# ==================== STATUS ====================

async def show_status():
    """Show all active/escalating events."""
    events = await db_fetch("""
        SELECT id, event_type, title, severity, status, country_code, follow_up_priority,
               first_seen, last_updated, reported_count
        FROM worldmonitor_events
        WHERE status IN ('new','active','escalating')
        ORDER BY
            CASE severity WHEN 'critical' THEN 0 WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
            follow_up_priority DESC
    """)
    for e in events:
        sev_emoji = {"critical": "🔴", "high": "🟠", "medium": "🟡", "low": "⚪"}.get(e["severity"], "⚪")
        status_emoji = {"escalating": "⬆️", "new": "🆕", "active": "▶️"}.get(e["status"], "")
        age = (datetime.now(timezone.utc) - e["first_seen"].replace(tzinfo=timezone.utc)).days if e["first_seen"] else 0
        print(f"{sev_emoji} {status_emoji} [{e['event_type']:20s}] {e['title'][:70]}")
        print(f"   ID:{e['id']} | {e['country_code'] or '??'} | Age:{age}d | Reports:{e['reported_count']} | Priority:{e['follow_up_priority']}")
    print(f"\nTotal active: {len(events)}")


async def show_stats():
    """DB stats overview."""
    stats = {}
    stats["total_events"] = (await db_fetchrow("SELECT COUNT(*) as c FROM worldmonitor_events"))["c"]
    stats["active"] = (await db_fetchrow("SELECT COUNT(*) as c FROM worldmonitor_events WHERE status IN ('new','active','escalating')"))["c"]
    stats["closed"] = (await db_fetchrow("SELECT COUNT(*) as c FROM worldmonitor_events WHERE status IN ('closed','resolved')"))["c"]
    stats["escalations_24h"] = (await db_fetchrow("SELECT COUNT(*) as c FROM worldmonitor_escalations WHERE detected_at > NOW() - INTERVAL '24 hours'"))["c"]
    stats["pending_follow_ups"] = (await db_fetchrow("SELECT COUNT(*) as c FROM worldmonitor_follow_ups WHERE status = 'pending'"))["c"]
    stats["reports_total"] = (await db_fetchrow("SELECT COUNT(*) as c FROM worldmonitor_reports"))["c"]
    stats["snapshots"] = (await db_fetchrow("SELECT COUNT(*) as c FROM worldmonitor_snapshots"))["c"]

    by_type = await db_fetch(
        "SELECT event_type, COUNT(*) as c FROM worldmonitor_events WHERE status IN ('new','active','escalating') GROUP BY event_type ORDER BY c DESC"
    )
    stats["by_type"] = {r["event_type"]: r["c"] for r in by_type}

    by_severity = await db_fetch(
        "SELECT severity, COUNT(*) as c FROM worldmonitor_events WHERE status IN ('new','active','escalating') GROUP BY severity ORDER BY c DESC"
    )
    stats["by_severity"] = {r["severity"]: r["c"] for r in by_severity}

    print(json.dumps(stats, indent=2, default=str))


# ==================== MAIN ====================

def main():
    if len(sys.argv) < 2:
        print("Usage: event_tracker.py <command> [args]")
        print("Commands: ingest, status, briefing-context, follow-ups, complete-followup, close, report, auto-close, stats")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "ingest":
        asyncio.run(ingest_events())
    elif cmd == "status":
        asyncio.run(show_status())
    elif cmd == "briefing-context":
        asyncio.run(get_briefing_context())
    elif cmd == "follow-ups":
        async def show_followups():
            rows = await db_fetch(
                "SELECT f.id, f.question, f.priority, f.search_queries, ev.title FROM worldmonitor_follow_ups f JOIN worldmonitor_events ev ON f.event_id = ev.id WHERE f.status = 'pending' ORDER BY f.priority DESC"
            )
            for r in rows:
                print(f"[{r['id']}] P{r['priority']} — {r['title'][:50]}")
                print(f"   Q: {r['question']}")
                if r['search_queries']:
                    print(f"   Searches: {r['search_queries']}")
        asyncio.run(show_followups())
    elif cmd == "complete-followup":
        fid = int(sys.argv[2])
        findings = sys.argv[3] if len(sys.argv) > 3 else ""
        asyncio.run(db_exec(
            "UPDATE worldmonitor_follow_ups SET status = 'completed', findings = $1, completed_at = NOW() WHERE id = $2",
            findings, fid
        ))
        print(f"Follow-up {fid} completed.")
    elif cmd == "close":
        eid = int(sys.argv[2])
        asyncio.run(db_exec(
            "UPDATE worldmonitor_events SET status = 'closed', resolved_at = NOW() WHERE id = $1", eid
        ))
        print(f"Event {eid} closed.")
    elif cmd == "report":
        rtype = sys.argv[2] if len(sys.argv) > 2 else "morning_briefing"
        asyncio.run(log_report(rtype))
    elif cmd == "auto-close":
        asyncio.run(auto_close_stale())
    elif cmd == "stats":
        asyncio.run(show_stats())
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)


if __name__ == "__main__":
    main()
