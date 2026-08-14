"""
Check latest WorldMonitor snapshot for critical alerts.
If critical alerts are found, write them to /tmp/alerts_to_send.txt
as raw multiline text for the GitHub Actions workflow to email.
"""
import asyncio
import asyncpg
import os
import sys
import json
from datetime import date, datetime, timezone

DSN = os.environ.get("DATABASE_URL", "")

# 在线仪表盘地址：告警邮件中会附上此链接，方便直接跳转到事件明细。
# 重新部署 CloudStudio 后若地址变化，更新此默认值或设置环境变量 DASHBOARD_URL 即可。
DASHBOARD_URL = os.environ.get(
    "DASHBOARD_URL",
    "https://5c8c1a2bec9140018fb9ebd59f826ee6.bj9.agentos-app.net",
)

# 复用 generate_static.py 的 AI 简报生成逻辑（纯函数，import 时不触发网络）
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
from dashboard.generate_static import build_briefing

CRITICAL_RULES = [
    ("M{s} 强震", lambda s: s.get("max_earthquake_mag") and float(s["max_earthquake_mag"]) >= 7.5,
     lambda s: f"🌋 M{s['max_earthquake_mag']} 强震警报（阈值 ≥ M7.5）"),
    ("灾害密集", lambda s: (s.get("red_alert_count") or 0) >= 8,
     lambda s: f"🛰️ {s['red_alert_count']} 起红色警报同时活跃"),
    ("BTC剧烈波动", lambda s: s.get("btc_change_24h") is not None and abs(float(s["btc_change_24h"])) >= 8,
     lambda s: f"📉 BTC 24h波动 {float(s['btc_change_24h']):+.1f}%"),
    ("极度恐惧", lambda s: s.get("fear_greed_index") is not None and int(s["fear_greed_index"]) <= 15,
     lambda s: f"😱 恐惧指数 {s['fear_greed_index']}，极度恐惧"),
    ("极度贪婪", lambda s: s.get("fear_greed_index") is not None and int(s["fear_greed_index"]) >= 85,
     lambda s: f"🤑 贪婪指数 {s['fear_greed_index']}，极度贪婪"),
    ("CII极高", lambda s: s.get("cii_top_score") is not None and int(s["cii_top_score"]) >= 85,
     lambda s: f"🏴 {s['cii_top_country']} CII={s['cii_top_score']}，极高不稳定"),
]

# 冷却窗口（小时）：同一规则在窗口内不重复发邮件，避免持续状态每小时轰炸
COOLDOWN_HOURS = 12

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

        # 拉取 CII 明细，供 AI 简报（来龙去脉）使用
        snap_date_str = s.get("snapshot_date", "")
        try:
            snap_date = date.fromisoformat(snap_date_str) if snap_date_str else date.today()
        except Exception:
            snap_date = date.today()
        cii_rows = [dict(r) for r in await conn.fetch(
            "SELECT country_code, country_name, score, level "
            "FROM worldmonitor_cii_history WHERE snapshot_date = $1 ORDER BY score DESC",
            snap_date)]

        print(f"Checking snapshot {s.get('snapshot_date')} for critical alerts...")

        # 确保冷却表存在（幂等，首次运行自动建表）
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS alert_cooldowns (
                rule_name TEXT PRIMARY KEY,
                last_sent TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """)

        triggered = []
        for name, check, msg_fn in CRITICAL_RULES:
            try:
                if check(s):
                    triggered.append((name, msg_fn(s)))
                    print(f"  TRIGGERED: {name}")
            except Exception as e:
                print(f"  Error checking {name}: {e}")

        # 手动测试模式：workflow_dispatch 传入 test_alert=true 时强制发一封测试告警
        if os.environ.get("FORCE_TEST_ALERT", "").lower() in ("1", "true", "yes"):
            triggered.append(("__test__", "🧪 [TEST] 这是一封测试告警邮件，WorldMonitor 邮件链路工作正常 ✅"))

        if not triggered:
            print("\nNo critical alerts. All clear.")
            return

        # 冷却去重：同一规则在 COOLDOWN_HOURS 窗口内不重复发（测试告警与首次触发不受限）
        now = datetime.now(timezone.utc)
        fresh = []
        for name, msg in triggered:
            if name == "__test__":
                fresh.append((name, msg))
                continue
            rec = await conn.fetchrow(
                "SELECT last_sent FROM alert_cooldowns WHERE rule_name = $1", name)
            if rec and (now - rec["last_sent"]).total_seconds() < COOLDOWN_HOURS * 3600:
                print(f"  COOLDOWN skip: {name}（{COOLDOWN_HOURS}h 内已发过）")
                continue
            fresh.append((name, msg))

        if not fresh:
            print(f"\n{len(triggered)} 条触发均处于冷却窗口内，本小时不发邮件。")
            return

        fresh_msgs = [m for _, m in fresh]
        alert_text = (
            "🔴 WorldMonitor 异常告警\n"
            f"🌐 在线仪表盘（点击查看全部事件明细）：{DASHBOARD_URL}\n\n"
            + "\n".join(fresh_msgs)
        )

        # AI 简报（来龙去脉）：针对当前全局态势给出中文解释
        briefing = build_briefing(s, cii_rows)
        if briefing.get("sections"):
            alert_text += "\n\n📋 来龙去脉 · AI 简报\n" + "─" * 26
            for sec in briefing["sections"]:
                alert_text += f"\n\n{sec.get('icon', '')} {sec.get('title', '')}\n{sec.get('text', '')}"
            overall = briefing.get("overall") or {}
            if overall.get("text"):
                alert_text += f"\n\n💡 总体研判：{overall['text']}"

        alert_text += f"\n\n快照时间: {s.get('snapshot_date')}"
        alert_text += f"\n总信号: {s.get('total_signals', 0)}"
        alert_text += f"\n🌐 在线仪表盘：{DASHBOARD_URL}"

        with open("/tmp/alerts_to_send.txt", "w", encoding="utf-8") as f:
            f.write(alert_text)

        # 更新已发送规则的冷却时间戳
        for name, _ in fresh:
            if name == "__test__":
                continue
            await conn.execute("""
                INSERT INTO alert_cooldowns (rule_name, last_sent) VALUES ($1, now())
                ON CONFLICT (rule_name) DO UPDATE SET last_sent = now()
            """, name)

        print(f"\n{len(fresh)}/{len(triggered)} 条告警发邮件（其余冷却跳过），已写入 /tmp/alerts_to_send.txt")

    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
