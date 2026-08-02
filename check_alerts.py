"""
Check latest WorldMonitor snapshot for critical alerts.
If critical alerts are found, write them to /tmp/alerts_to_send.txt
as raw multiline text for the GitHub Actions workflow to email.
"""
import asyncio
import asyncpg
import os
import json
from datetime import date, datetime, timezone

DSN = os.environ.get("DATABASE_URL", "")

CRITICAL_RULES = [
    ("M{s} 强震", lambda s: s.get("max_earthquake_mag") and float(s["max_earthquake_mag"]) >= 7.0,
     lambda s: f"🌋 M{s['max_earthquake_mag']} 强震警报（阈值 ≥ M7.0）"),
    ("灾害密集", lambda s: (s.get("red_alert_count") or 0) >= 5,
     lambda s: f"🛰️ {s['red_alert_count']} 起红色警报同时活跃"),
    ("BTC剧烈波动", lambda s: s.get("btc_change_24h") is not None and abs(float(s["btc_change_24h"])) >= 5,
     lambda s: f"📉 BTC 24h波动 {float(s['btc_change_24h']):+.1f}%"),
    ("极度恐惧", lambda s: s.get("fear_greed_index") is not None and int(s["fear_greed_index"]) <= 20,
     lambda s: f"😱 恐惧指数 {s['fear_greed_index']}，极度恐惧"),
    ("极度贪婪", lambda s: s.get("fear_greed_index") is not None and int(s["fear_greed_index"]) >= 80,
     lambda s: f"🤑 贪婪指数 {s['fear_greed_index']}，极度贪婪"),
    ("CII极高", lambda s: s.get("cii_top_score") is not None and int(s["cii_top_score"]) >= 80,
     lambda s: f"🏴 {s['cii_top_country']} CII={s['cii_top_score']}，极高不稳定"),
]

async def main():
    conn = await asyncpg.connect(DSN)
    try:
        row = await conn.fetchrow(
            "SELECT * FROM worldmonitor_snapshots ORDER BY id DESC LIMIT 1"
        )
        if not row:
            print("No snapshot found, skipping alert check")
            return

        s = dict(row)
        # Serialize for display
        for k in ("snapshot_date", "created_at"):
            if k in s and hasattr(s[k], "isoformat"):
                s[k] = s[k].isoformat()

        print(f"Checking snapshot {s.get('snapshot_date')} for critical alerts...")

        triggered = []
        for name, check, msg_fn in CRITICAL_RULES:
            try:
                if check(s):
                    triggered.append(msg_fn(s))
                    print(f"  TRIGGERED: {name}")
            except Exception as e:
                print(f"  Error checking {name}: {e}")

        # 手动测试模式：workflow_dispatch 传入 test_alert=true 时强制发一封测试告警
        if os.environ.get("FORCE_TEST_ALERT", "").lower() in ("1", "true", "yes"):
            triggered.append("🧪 [TEST] 这是一封测试告警邮件，WorldMonitor 邮件链路工作正常 ✅")

        if triggered:
            alert_text = "🔴 WorldMonitor 异常告警\n\n" + "\n".join(triggered)
            alert_text += f"\n\n快照时间: {s.get('snapshot_date')}"
            alert_text += f"\n总信号: {s.get('total_signals', 0)}"

            with open("/tmp/alerts_to_send.txt", "w", encoding="utf-8") as f:
                f.write(alert_text)

            print(f"\n{len(triggered)} critical alerts found, written to /tmp/alerts_to_send.txt")
        else:
            print("\nNo critical alerts. All clear.")

    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
