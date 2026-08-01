"""Fetch full snapshot + CII data for briefing generation."""
import asyncio, asyncpg, json, sys, os
from datetime import date, datetime
from decimal import Decimal

DSN = os.environ.get("DATABASE_URL",
    "postgresql://neondb_owner:npg_ycsdS6V3qkEK@ep-proud-sun-ayh9ltmg-pooler.c-5.us-east-2.aws.neon.tech/neondb?sslmode=require")

def _serialize(obj):
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (list, tuple)):
        return [_serialize(x) for x in obj]
    return obj

async def main():
    conn = await asyncpg.connect(DSN)
    
    row = await conn.fetchrow("SELECT * FROM worldmonitor_snapshots ORDER BY id DESC LIMIT 1")
    if not row:
        print(json.dumps({"error": "no data"}))
        return
    raw = dict(row)
    snap_date = raw["snapshot_date"]  # keep as date object for queries
    snap = {k: _serialize(v) for k, v in raw.items()}
    
    cii = [dict(x) for x in await conn.fetch(
        "SELECT country_code, country_name, score, level "
        "FROM worldmonitor_cii_history WHERE snapshot_date = $1 "
        "ORDER BY score DESC LIMIT 15",
        snap_date
    )]
    
    # Count by level
    level_counts = {}
    for c in cii:
        lv = c["level"]
        level_counts[lv] = level_counts.get(lv, 0) + 1
    
    await conn.close()
    
    result = {
        "snapshot": snap,
        "cii_top15": cii,
        "cii_level_counts": level_counts,
    }
    print(json.dumps(result, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    asyncio.run(main())
