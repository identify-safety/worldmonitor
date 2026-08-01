"""
WorldMonitor v2 — Dashboard Server
Connects to Neon PostgreSQL, serves JSON API + embedded HTML dashboard.
"""
import asyncio
import asyncpg
from datetime import date, timedelta, datetime, timezone
from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
import os

app = Flask(__name__, static_folder=None)
CORS(app)

DSN = os.environ.get("DATABASE_URL",
    "postgresql://neondb_owner:npg_ycsdS6V3qkEK@ep-proud-sun-ayh9ltmg-pooler.c-5.us-east-2.aws.neon.tech/neondb?sslmode=require")

async def _query(sql, *args):
    conn = await asyncpg.connect(DSN)
    try:
        return [dict(r) for r in await conn.fetch(sql, *args)]
    finally:
        await conn.close()

def _run(q, *args):
    return asyncio.run(_query(q, *args))

# ──────────────────────────────────────────
#  API
# ──────────────────────────────────────────

@app.route("/api/overview")
def api_overview():
    rows = _run("SELECT * FROM v_latest_snapshot")
    if not rows:
        return jsonify({"error": "no data"}), 404
    r = rows[0]
    for k in list(r.keys()):
        if isinstance(r[k], (date, datetime)):
            r[k] = r[k].isoformat()
        if isinstance(r[k], (int, float, str, type(None))):
            continue
        r[k] = str(r[k])
    return jsonify(r)

@app.route("/api/cii")
def api_cii():
    rows = _run("SELECT * FROM v_latest_cii ORDER BY score DESC")
    for r in rows:
        if isinstance(r.get("snapshot_date"), date):
            r["snapshot_date"] = r["snapshot_date"].isoformat()
    return jsonify(rows)

@app.route("/api/history")
def api_history():
    days = int(app.request.args.get("days", 7))
    rows = _run(
        "SELECT snapshot_date, earthquake_count, max_earthquake_mag, "
        "disaster_count, btc_price, fear_greed_index, total_signals "
        "FROM worldmonitor_snapshots "
        "WHERE snapshot_date >= $1 ORDER BY snapshot_date ASC",
        date.today() - timedelta(days=days))
    for r in rows:
        r["snapshot_date"] = r["snapshot_date"].isoformat()
        for k in list(r.keys()):
            if isinstance(r[k], (int, float, str, type(None))):
                continue
            r[k] = str(r[k])
    return jsonify(rows)

@app.route("/api/anomalies")
def api_anomalies():
    days = int(app.request.args.get("days", 7))
    rows = _run(
        "SELECT snapshot_date, anomaly_flags, convergence_zones "
        "FROM worldmonitor_snapshots WHERE snapshot_date >= $1 "
        "ORDER BY snapshot_date DESC", date.today() - timedelta(days=days))
    for r in rows:
        r["snapshot_date"] = r["snapshot_date"].isoformat()
    return jsonify(rows)

@app.route("/api/trends")
def api_trends():
    rows = _run("SELECT * FROM v_country_trends_30d ORDER BY avg_score DESC")
    return jsonify(rows)

@app.route("/api/snapshot/<int:snap_id>")
def api_snapshot_full(snap_id):
    rows = _run("SELECT * FROM worldmonitor_snapshots WHERE id = $1", snap_id)
    if not rows:
        return jsonify({"error": "not found"}), 404
    r = rows[0]
    for k in list(r.keys()):
        if isinstance(r[k], (date, datetime)):
            r[k] = r[k].isoformat()
        if isinstance(r[k], (int, float, str, type(None))):
            continue
        r[k] = str(r[k])
    return jsonify(r)

# ──────────────────────────────────────────
#  AI Briefing
# ──────────────────────────────────────────

@app.route("/api/briefing")
def api_briefing():
    """Generate an AI-style briefing from the latest snapshot."""
    rows = _run("SELECT * FROM worldmonitor_snapshots ORDER BY id DESC LIMIT 1")
    if not rows:
        return jsonify({"error": "no data"}), 404
    s = rows[0]
    
    cii_rows = _run(
        "SELECT country_code, country_name, score, level "
        "FROM worldmonitor_cii_history WHERE snapshot_date = $1 "
        "ORDER BY score DESC", s["snapshot_date"]
    )
    
    # Build briefing sections
    sections = []
    
    # 1. Seismic
    eq = s["earthquake_count"]
    max_mag = s["max_earthquake_mag"]
    if eq > 0:
        if max_mag and max_mag >= 7:
            level = "critical"
            text = f"全球发生 {eq} 次地震，其中最大震级 M{max_mag}，属于强震，需关注可能的海啸和余震风险。"
        elif max_mag and max_mag >= 6:
            level = "warning"
            text = f"全球记录 {eq} 次地震，最大 M{max_mag}，中度活跃。"
        else:
            level = "normal"
            text = f"全球记录 {eq} 次地震，最大 M{max_mag}，地震活动处于正常水平。"
        sections.append({"icon": "🌋", "title": "地震活动", "level": level, "text": text})
    
    # 2. Disasters
    disasters = s["disaster_count"]
    reds = s.get("red_alert_count") or 0
    if reds >= 5:
        level = "critical"
        text = f"当前 {disasters} 起活跃灾害中，有 {reds} 起处于红色警报状态，包括台风、洪水或野火。灾害密度显著高于常态，建议关注东亚台风和北美野火发展。"
    elif disasters > 0:
        level = "warning"
        text = f"{disasters} 起活跃灾害，{reds} 起红色警报。需持续关注。"
    else:
        level = "normal"
        text = "无重大灾害警报。"
    sections.append({"icon": "🛰️", "title": "灾害监测", "level": level, "text": text})
    
    # 3. Crypto
    btc = s["btc_price"]
    btc_chg = s["btc_change_24h"]
    fgi = s["fear_greed_index"]
    fgi_label = s.get("fear_greed_label", "")
    if btc:
        if btc_chg is not None and abs(btc_chg) >= 5:
            level = "critical"
        elif fgi and fgi <= 30:
            level = "warning"
        else:
            level = "normal"
        chg_str = f"{btc_chg:+.2f}%" if btc_chg is not None else "—"
        text = f"BTC 报 ${btc:,.0f}，日涨跌 {chg_str}。恐惧与贪婪指数 {fgi}（{fgi_label}）。"
        if fgi and fgi <= 25:
            text += " 市场处于极度恐惧状态，历史上此类极端情绪往往出现在阶段性底部附近，但也可能预示进一步下跌。"
        elif fgi and fgi >= 75:
            text += " 市场处于极度贪婪状态，短期回调风险升高。"
        sections.append({"icon": "🪙", "title": "加密市场", "level": level, "text": text})
    
    # 4. CII
    top_cii = [(r["country_code"], r["country_name"], r["score"], r["level"]) for r in cii_rows[:5]]
    if top_cii:
        top_str = "、".join([f"{name}({score})" for code, name, score, lv in top_cii[:3]])
        max_score = top_cii[0][2]
        if max_score >= 80:
            level = "critical"
        elif max_score >= 60:
            level = "warning"
        else:
            level = "normal"
        text = f"国家不稳定指数前三：{top_str}。"
        elevated = sum(1 for r in cii_rows if r["level"] in ("elevated", "high", "critical"))
        if elevated > 5:
            text += f" 目前 {elevated} 个国家的风险等级处于 elevated 或以上，全球地缘政治紧张度偏高。"
        sections.append({"icon": "🏴", "title": "地缘风险", "level": level, "text": text})
    
    # 5. NASA / climate
    nasa = s.get("nasa_event_count") or 0
    if nasa > 0:
        sections.append({"icon": "🌡️", "title": "气候与空间", "level": "normal",
                          "text": f"NASA EONET 记录 {nasa} 起自然灾害和空间事件，涵盖野火、台风、火山活动等。"})
    
    # 6. Anomalies
    anomalies = s.get("anomaly_flags") or []
    if anomalies:
        sections.append({"icon": "🚨", "title": "异常信号", "level": "critical",
                          "text": "；".join(anomalies)})
    
    # 7. Overall assessment
    critical_count = sum(1 for sec in sections if sec["level"] == "critical")
    warning_count = sum(1 for sec in sections if sec["level"] == "warning")
    
    if critical_count >= 2:
        overall = {"level": "critical", "text": "全球态势高度紧张：多重危机信号同时出现，建议密切关注后续发展。"}
    elif critical_count >= 1 or warning_count >= 2:
        overall = {"level": "warning", "text": "全球态势值得关注：部分领域的风险指标出现异常，需持续跟踪。"}
    else:
        overall = {"level": "normal", "text": "全球态势总体平稳，无重大异常信号。"}
    
    return jsonify({
        "date": s["snapshot_date"].isoformat() if hasattr(s["snapshot_date"], "isoformat") else str(s["snapshot_date"]),
        "total_signals": s["total_signals"],
        "overall": overall,
        "sections": sections,
    })

# ──────────────────────────────────────────
#  Alert Engine
# ──────────────────────────────────────────

ALERT_RULES = [
    {"id": "eq_critical", "icon": "🌋", "title": "强震警报", "level": "critical",
     "check": lambda s: s["max_earthquake_mag"] and float(s["max_earthquake_mag"]) >= 7.0,
     "msg": lambda s: f"检测到 M{s['max_earthquake_mag']} 强震（阈值 ≥ M7.0）"},
    {"id": "eq_warning", "icon": "🌋", "title": "中型地震", "level": "warning",
     "check": lambda s: s["max_earthquake_mag"] and 6.0 <= float(s["max_earthquake_mag"]) < 7.0,
     "msg": lambda s: f"检测到 M{s['max_earthquake_mag']} 地震（阈值 ≥ M6.0）"},
    {"id": "disaster_surge", "icon": "🛰️", "title": "灾害密集", "level": "critical",
     "check": lambda s: (s.get("red_alert_count") or 0) >= 5,
     "msg": lambda s: f"{s['red_alert_count']} 起红色警报同时活跃（阈值 ≥ 5）"},
    {"id": "fear_extreme", "icon": "😱", "title": "极度恐惧", "level": "warning",
     "check": lambda s: s["fear_greed_index"] is not None and int(s["fear_greed_index"]) <= 20,
     "msg": lambda s: f"恐惧指数 {s['fear_greed_index']}，处于极度恐惧区间（阈值 ≤ 20）"},
    {"id": "fear_elevated", "icon": "😰", "title": "恐惧偏高", "level": "info",
     "check": lambda s: s["fear_greed_index"] is not None and 20 < int(s["fear_greed_index"]) <= 30,
     "msg": lambda s: f"恐惧指数 {s['fear_greed_index']}，市场情绪偏谨慎（阈值 ≤ 30）"},
    {"id": "greed_extreme", "icon": "🤑", "title": "极度贪婪", "level": "warning",
     "check": lambda s: s["fear_greed_index"] is not None and int(s["fear_greed_index"]) >= 80,
     "msg": lambda s: f"贪婪指数 {s['fear_greed_index']}，处于极度贪婪区间（阈值 ≥ 80）"},
    {"id": "btc_volatile", "icon": "📉", "title": "BTC剧烈波动", "level": "critical",
     "check": lambda s: s["btc_change_24h"] is not None and abs(float(s["btc_change_24h"])) >= 5,
     "msg": lambda s: f"BTC 24小时波动 {float(s['btc_change_24h']):+.1f}%（阈值 ≥ ±5%）"},
    {"id": "cii_critical", "icon": "🏴", "title": "极高不稳定", "level": "critical",
     "check": lambda s: s["cii_top_score"] is not None and int(s["cii_top_score"]) >= 80,
     "msg": lambda s: f"国家 {s['cii_top_country']} CII 达到 {s['cii_top_score']}（阈值 ≥ 80）"},
    {"id": "cii_high", "icon": "🏴", "title": "高度不稳定", "level": "warning",
     "check": lambda s: s["cii_top_score"] is not None and 60 <= int(s["cii_top_score"]) < 80,
     "msg": lambda s: f"国家 {s['cii_top_country']} CII 达到 {s['cii_top_score']}（阈值 ≥ 60）"},
]

@app.route("/api/alerts")
def api_alerts():
    rows = _run("SELECT * FROM worldmonitor_snapshots ORDER BY id DESC LIMIT 1")
    if not rows:
        return jsonify({"alerts": [], "summary": "no data"})
    s = rows[0]
    triggered = []
    for rule in ALERT_RULES:
        try:
            if rule["check"](s):
                triggered.append({
                    "id": rule["id"],
                    "icon": rule["icon"],
                    "title": rule["title"],
                    "level": rule["level"],
                    "message": rule["msg"](s),
                })
        except Exception:
            pass
    
    critical = sum(1 for a in triggered if a["level"] == "critical")
    warning = sum(1 for a in triggered if a["level"] == "warning")
    
    if not triggered:
        summary = "✅ 各项指标正常，无告警触发。"
    elif critical > 0:
        summary = f"🔴 {critical} 条严重告警、{warning} 条警告。建议立即查看。"
    elif warning > 0:
        summary = f"🟡 {warning} 条警告。部分指标值得关注。"
    else:
        summary = f"ℹ️ {len(triggered)} 条信息提示。"
    
    return jsonify({"alerts": triggered, "summary": summary, "count": len(triggered)})

# ──────────────────────────────────────────
#  Frontend (embedded)
# ──────────────────────────────────────────

HTML = r"""<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>WorldMonitor v2 — Global Intelligence Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
<style>
:root {
  --bg: #0f1117;
  --card: #1a1d28;
  --card-hover: #1f2335;
  --border: #2a2d3a;
  --text: #d1d5db;
  --text-dim: #6b7280;
  --accent: #3b82f6;
  --green: #22c55e;
  --red: #ef4444;
  --yellow: #eab308;
  --orange: #f97316;
  --critical: #ef4444;
  --high: #f97316;
  --elevated: #eab308;
  --normal: #22c55e;
  --low: #3b82f6;
  --radius: 12px;
}

* { margin: 0; padding: 0; box-sizing: border-box; }
body {
  background: var(--bg);
  color: var(--text);
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Noto Sans SC", sans-serif;
  min-height: 100vh;
}

.header {
  background: linear-gradient(135deg, #1a1d28 0%, #0f1117 100%);
  border-bottom: 1px solid var(--border);
  padding: 20px 32px;
  display: flex;
  justify-content: space-between;
  align-items: center;
  position: sticky;
  top: 0;
  z-index: 100;
}
.header h1 {
  font-size: 20px;
  font-weight: 700;
  background: linear-gradient(135deg, #3b82f6, #8b5cf6);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
}
.header .status {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 13px;
  color: var(--text-dim);
}
.status .dot {
  width: 8px; height: 8px;
  border-radius: 50%;
  background: var(--green);
  animation: pulse 2s infinite;
}
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.4} }
.status .refresh { color: var(--accent); cursor: pointer; }
.status .refresh:hover { text-decoration: underline; }

.container {
  max-width: 1400px;
  margin: 0 auto;
  padding: 24px;
}

.stat-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: 16px;
  margin-bottom: 24px;
}
.stat-card {
  background: var(--card);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 20px;
  transition: all .2s;
}
.stat-card:hover { border-color: var(--accent); background: var(--card-hover); }
.stat-card .label {
  font-size: 12px;
  text-transform: uppercase;
  letter-spacing: .5px;
  color: var(--text-dim);
  margin-bottom: 8px;
  display: flex;
  align-items: center;
  gap: 6px;
}
.stat-card .value {
  font-size: 32px;
  font-weight: 700;
}
.stat-card .sub {
  font-size: 13px;
  color: var(--text-dim);
  margin-top: 4px;
}

.section { margin-bottom: 24px; }
.section-title {
  font-size: 15px;
  font-weight: 600;
  margin-bottom: 16px;
  padding-left: 12px;
  border-left: 3px solid var(--accent);
}
.section-subtitle { font-size: 12px; color: var(--text-dim); font-weight: normal; margin-left: 8px; }

.two-col {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 16px;
}
@media (max-width: 900px) { .two-col { grid-template-columns: 1fr; } .stat-grid { grid-template-columns: repeat(2, 1fr); } }

.card {
  background: var(--card);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 20px;
}
.card canvas { max-height: 300px; }

.cii-table { width: 100%; border-collapse: collapse; }
.cii-table th, .cii-table td {
  padding: 10px 14px;
  text-align: left;
  font-size: 13px;
  border-bottom: 1px solid var(--border);
}
.cii-table th { color: var(--text-dim); font-weight: 500; font-size: 11px; text-transform: uppercase; }
.cii-table tr:hover { background: rgba(59,130,246,.05); }
.cii-table .score { font-weight: 700; }
.badge {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 10px;
  font-size: 11px;
  font-weight: 600;
}
.badge-critical { background: rgba(239,68,68,.15); color: var(--critical); }
.badge-high { background: rgba(249,115,22,.15); color: var(--high); }
.badge-elevated { background: rgba(234,179,8,.15); color: var(--elevated); }
.badge-normal { background: rgba(34,197,94,.15); color: var(--normal); }

.anomaly-tag {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  background: rgba(234,179,8,.1);
  border: 1px solid rgba(234,179,8,.2);
  padding: 6px 12px;
  border-radius: 6px;
  font-size: 13px;
  margin: 4px;
}
.anomaly-tag .icon { font-size: 16px; }

.change-up { color: var(--red); }  /* Chinese convention: 涨=红 */
.change-down { color: var(--green); } /* Chinese convention: 跌=绿 */
.change-none { color: var(--text-dim); }

.empty-state {
  text-align: center;
  padding: 48px 24px;
  color: var(--text-dim);
}
.empty-state .icon { font-size: 40px; margin-bottom: 12px; }

footer {
  text-align: center;
  padding: 32px;
  color: var(--text-dim);
  font-size: 12px;
  border-top: 1px solid var(--border);
  margin-top: 32px;
}
footer a { color: var(--accent); text-decoration: none; }

/* Briefing */
.briefing-card {
  background: linear-gradient(135deg, #1a1d28 0%, #1f2335 100%);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 24px;
  margin-bottom: 24px;
}
.briefing-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
}
.briefing-header h2 {
  font-size: 16px;
  font-weight: 600;
}
.briefing-date { font-size: 12px; color: var(--text-dim); }
.briefing-overall {
  padding: 10px 16px;
  border-radius: 8px;
  margin-bottom: 16px;
  font-size: 14px;
  font-weight: 500;
}
.briefing-overall.critical { background: rgba(239,68,68,.1); border: 1px solid rgba(239,68,68,.2); color: #fca5a5; }
.briefing-overall.warning { background: rgba(234,179,8,.1); border: 1px solid rgba(234,179,8,.2); color: #fde047; }
.briefing-overall.normal { background: rgba(34,197,94,.1); border: 1px solid rgba(34,197,94,.2); color: #86efac; }
.briefing-sections { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
@media (max-width: 900px) { .briefing-sections { grid-template-columns: 1fr; } }
.briefing-item {
  background: rgba(15,17,23,.5);
  border-radius: 8px;
  padding: 14px;
  border-left: 3px solid var(--border);
}
.briefing-item.critical { border-left-color: var(--critical); }
.briefing-item.warning { border-left-color: var(--yellow); }
.briefing-item.normal { border-left-color: var(--green); }
.briefing-item .bi-title { font-size: 13px; font-weight: 600; margin-bottom: 6px; display: flex; align-items: center; gap: 6px; }
.briefing-item .bi-text { font-size: 12px; color: var(--text-dim); line-height: 1.6; }

/* Alerts */
.alert-item {
  display: flex;
  align-items: flex-start;
  gap: 10px;
  padding: 12px;
  border-radius: 8px;
  margin-bottom: 8px;
}
.alert-item.critical { background: rgba(239,68,68,.08); border: 1px solid rgba(239,68,68,.15); }
.alert-item.warning { background: rgba(234,179,8,.08); border: 1px solid rgba(234,179,8,.15); }
.alert-item.info { background: rgba(59,130,246,.08); border: 1px solid rgba(59,130,246,.15); }
.alert-icon { font-size: 18px; flex-shrink: 0; margin-top: 1px; }
.alert-content { flex: 1; }
.alert-content .alert-title { font-size: 13px; font-weight: 600; margin-bottom: 2px; }
.alert-content .alert-msg { font-size: 11px; color: var(--text-dim); }
.alert-summary { font-size: 13px; padding: 8px 12px; border-radius: 6px; margin-bottom: 12px; }
.alert-summary.ok { background: rgba(34,197,94,.08); color: #86efac; }
.alert-summary.warn { background: rgba(234,179,8,.08); color: #fde047; }
.alert-summary.bad { background: rgba(239,68,68,.08); color: #fca5a5; }
</style>
</head>
<body>

<header class="header">
  <div>
    <h1>🌍 WorldMonitor v2</h1>
  </div>
  <div class="status">
    <div class="dot" id="status-dot"></div>
    <span id="last-update">加载中...</span>
    <span class="refresh" onclick="refresh()">↻ 刷新</span>
  </div>
</header>

<div class="container">

  <!-- Stat cards -->
  <div class="stat-grid" id="stats"></div>

  <!-- AI Briefing -->
  <div class="briefing-card" id="briefing">
    <div class="briefing-header">
      <h2>🧠 AI 简报</h2>
      <span class="briefing-date" id="briefing-date"></span>
    </div>
    <div id="briefing-content">加载中...</div>
  </div>

  <!-- Alerts -->
  <div class="card section" id="alerts-card">
    <div class="section-title">🔔 实时告警</div>
    <div id="alerts-content">加载中...</div>
  </div>

  <div class="two-col section">
    <!-- CII Ranking -->
    <div class="card">
      <div class="section-title">🏴 国家不稳定指数 (CII)<span class="section-subtitle">Top 20</span></div>
      <div id="cii-table-container"></div>
    </div>

    <!-- Signals chart -->
    <div class="card">
      <div class="section-title">📊 信号趋势<span class="section-subtitle">近7天</span></div>
      <div id="chart-container">
        <canvas id="signalChart"></canvas>
      </div>
    </div>
  </div>

  <div class="two-col section">
    <!-- Crypto -->
    <div class="card">
      <div class="section-title">🪙 加密市场<span class="section-subtitle">近7天</span></div>
      <canvas id="cryptoChart"></canvas>
    </div>

    <!-- Anomalies -->
    <div class="card">
      <div class="section-title">🚨 异常信号</div>
      <div id="anomalies"></div>
    </div>
  </div>

  <!-- Country Trends -->
  <div class="section">
    <div class="card">
      <div class="section-title">📈 国家风险趋势<span class="section-subtitle">30日</span></div>
      <div id="trends-table-container"></div>
    </div>
  </div>

</div>

<footer>
  WorldMonitor v2 — Data from USGS, GDACS, NASA, CoinGecko, Polymarket, UNHCR, OpenSky & more.
  Running on <a href="https://neon.tech" target="_blank">Neon</a> PostgreSQL.
  <br>Last snapshot: <span id="footer-date">—</span>
</footer>

<script>
let signalChart = null;
let cryptoChart = null;

const LEVEL_COLORS = {critical:'#ef4444', high:'#f97316', elevated:'#eab308', normal:'#22c55e', low:'#3b82f6'};
const LEVEL_BADGE = {critical:'badge-critical', high:'badge-high', elevated:'badge-elevated', normal:'badge-normal', low:''};

async function fetchJSON(url) {
  const r = await fetch(url);
  if (!r.ok) return null;
  return r.json();
}

function fmtUSD(n) { return '$' + (n||0).toLocaleString('en-US'); }
function fmtChange(n) {
  if (n == null) return '<span class="change-none">—</span>';
  const cls = n >= 0 ? 'change-up' : 'change-down';
  const sign = n >= 0 ? '+' : '';
  return `<span class="${cls}">${sign}${n.toFixed(2)}%</span>`;
}

function renderStats(snap) {
  if (!snap) return;
  document.getElementById('stats').innerHTML = `
    <div class="stat-card">
      <div class="label">🌋 地震</div>
      <div class="value">${snap.earthquake_count || 0}</div>
      <div class="sub">最大 ${snap.max_earthquake_mag || '—'} 级</div>
    </div>
    <div class="stat-card">
      <div class="label">🛰️ 灾难 / NASA</div>
      <div class="value">${snap.disaster_count || 0} / ${snap.nasa_event_count || 0}</div>
      <div class="sub">红色警报 ${snap.red_alert_count || 0}</div>
    </div>
    <div class="stat-card">
      <div class="label">🪙 BTC</div>
      <div class="value">${fmtUSD(snap.btc_price)}</div>
      <div class="sub">${fmtChange(snap.btc_change_24h)}</div>
    </div>
    <div class="stat-card">
      <div class="label">😱 恐惧贪婪</div>
      <div class="value">${snap.fear_greed_index || '—'}</div>
      <div class="sub">${snap.fear_greed_label || '—'}</div>
    </div>
    <div class="stat-card">
      <div class="label">📡 总信号</div>
      <div class="value">${snap.total_signals || 0}</div>
      <div class="sub">CII: ${snap.cii_top_country || '—'} ${snap.cii_top_score || '—'}</div>
    </div>
  `;
}

function renderCII(data) {
  if (!data || !data.length) {
    document.getElementById('cii-table-container').innerHTML = '<div class="empty-state">暂无数据</div>';
    return;
  }
  const rows = data.slice(0, 20).map(c => `
    <tr>
      <td>${c.country_code || '—'}</td>
      <td class="score" style="color:${LEVEL_COLORS[c.level] || '#fff'}">${c.score}</td>
      <td><span class="badge ${LEVEL_BADGE[c.level] || ''}">${c.level || '—'}</span></td>
      <td>${c.change_24h > 0 ? '<span class="change-up">↑' + c.change_24h + '</span>' : c.change_24h < 0 ? '<span class="change-down">↓' + Math.abs(c.change_24h) + '</span>' : '—'}</td>
    </tr>`).join('');
  document.getElementById('cii-table-container').innerHTML = `
    <table class="cii-table">
      <thead><tr><th>国家</th><th>分数</th><th>级别</th><th>24h变化</th></tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
}

function renderAnomalies(data) {
  const el = document.getElementById('anomalies');
  if (!data || !data.length) { el.innerHTML = '<div class="empty-state">✅ 无异常信号</div>'; return; }
  let html = '';
  data.forEach(d => {
    if (d.anomaly_flags && d.anomaly_flags.length) {
      d.anomaly_flags.forEach(a => {
        html += `<div class="anomaly-tag"><span class="icon">⚠️</span>${a}</div>`;
      });
    }
  });
  if (!html) html = '<div class="empty-state">✅ 无异常信号</div>';
  el.innerHTML = html;
}

function renderTrends(data) {
  const el = document.getElementById('trends-table-container');
  if (!data || !data.length) { el.innerHTML = '<div class="empty-state">暂无趋势数据（需多天数据累积）</div>'; return; }
  const rows = data.map(c => `
    <tr>
      <td>${c.country_code || '—'}</td>
      <td>${c.country_name || '—'}</td>
      <td>${c.avg_score?.toFixed(1) || '—'}</td>
      <td>${c.max_score || '—'}</td>
      <td>${c.high_risk_days || 0}</td>
      <td>${c.data_points || 0}</td>
    </tr>`).join('');
  el.innerHTML = `
    <table class="cii-table">
      <thead><tr><th>代码</th><th>国家</th><th>平均分</th><th>最高分</th><th>高风险天数</th><th>数据点</th></tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
}

function renderBriefing(data) {
  if (!data || !data.sections) {
    document.getElementById('briefing-content').innerHTML = '<div class="empty-state">暂无简报数据</div>';
    return;
  }
  document.getElementById('briefing-date').textContent = data.date || '';
  const overall = data.overall;
  let html = `<div class="briefing-overall ${overall.level}">${overall.text}</div>`;
  html += '<div class="briefing-sections">';
  data.sections.forEach(sec => {
    html += `<div class="briefing-item ${sec.level}">
      <div class="bi-title">${sec.icon} ${sec.title}</div>
      <div class="bi-text">${sec.text}</div>
    </div>`;
  });
  html += '</div>';
  document.getElementById('briefing-content').innerHTML = html;
}

function renderAlerts(data) {
  const el = document.getElementById('alerts-content');
  if (!data || !data.alerts) { el.innerHTML = '<div class="empty-state">⚠️ 告警数据加载失败</div>'; return; }
  let html = '';
  const cls = data.summary.includes('🔴') ? 'bad' : data.summary.includes('🟡') ? 'warn' : 'ok';
  html += `<div class="alert-summary ${cls}">${data.summary}</div>`;
  if (data.alerts.length === 0) {
    html += '<div class="empty-state">✅ 各项指标正常，无告警触发</div>';
  } else {
    data.alerts.forEach(a => {
      html += `<div class="alert-item ${a.level}">
        <div class="alert-icon">${a.icon}</div>
        <div class="alert-content">
          <div class="alert-title">${a.title}</div>
          <div class="alert-msg">${a.message}</div>
        </div>
      </div>`;
    });
  }
  el.innerHTML = html;
}

function renderSignalChart(data) {
  if (!data || data.length < 1) return;
  const ctx = document.getElementById('signalChart');
  if (signalChart) signalChart.destroy();
  signalChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels: data.map(d => d.snapshot_date),
      datasets: [
        { label: '地震次数', data: data.map(d=>d.earthquake_count), borderColor: '#f97316', backgroundColor: 'transparent', tension: .3, pointRadius: 4 },
        { label: '总信号', data: data.map(d=>d.total_signals), borderColor: '#3b82f6', backgroundColor: 'transparent', tension: .3, pointRadius: 4, borderWidth: 2 },
        { label: '恐惧指数', data: data.map(d=>d.fear_greed_index), borderColor: '#eab308', backgroundColor: 'transparent', tension: .3, pointRadius: 4, borderDash: [5,3] },
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { labels: { color: '#d1d5db', usePointStyle: true, padding: 20 } } },
      scales: {
        x: { ticks: { color: '#6b7280' }, grid: { color: '#1f2335' } },
        y: { ticks: { color: '#6b7280' }, grid: { color: '#1f2335' } }
      }
    }
  });
  ctx.parentElement.style.height = '280px';
}

function renderCryptoChart(data) {
  if (!data || data.length < 1) return;
  const ctx = document.getElementById('cryptoChart');
  if (cryptoChart) cryptoChart.destroy();
  cryptoChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels: data.map(d => d.snapshot_date),
      datasets: [
        { label: 'BTC ($)', data: data.map(d=>d.btc_price), borderColor: '#f7931a', backgroundColor: 'rgba(247,147,26,.1)', fill: true, tension: .3, pointRadius: 4 },
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { labels: { color: '#d1d5db', usePointStyle: true } } },
      scales: {
        x: { ticks: { color: '#6b7280' }, grid: { color: '#1f2335' } },
        y: { ticks: { color: '#6b7280', callback: v => '$' + v.toLocaleString() }, grid: { color: '#1f2335' } }
      }
    }
  });
  ctx.parentElement.style.height = '280px';
}

async function refresh() {
  document.getElementById('status-dot').style.background = '#eab308';

  const [overview, cii, history, anomalies, trends, briefing, alerts] = await Promise.all([
    fetchJSON('/api/overview'),
    fetchJSON('/api/cii'),
    fetchJSON('/api/history'),
    fetchJSON('/api/anomalies'),
    fetchJSON('/api/trends'),
    fetchJSON('/api/briefing'),
    fetchJSON('/api/alerts'),
  ]);

  renderStats(overview);
  renderBriefing(briefing);
  renderAlerts(alerts);
  renderCII(cii);
  renderSignalChart(history);
  renderCryptoChart(history);
  renderAnomalies(anomalies);
  renderTrends(trends);

  if (overview?.snapshot_date) {
    document.getElementById('footer-date').textContent = overview.snapshot_date;
    document.getElementById('last-update').textContent = '更新于 ' + new Date().toLocaleTimeString('zh-CN');
  }

  document.getElementById('status-dot').style.background = '#22c55e';
}

refresh();
setInterval(refresh, 5 * 60 * 1000); // auto-refresh every 5 min
</script>
</body>
</html>"""

@app.route("/")
def index():
    return HTML

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    print(f"\n  🌍 WorldMonitor Dashboard → http://localhost:{port}\n")
    app.run(host="0.0.0.0", port=port, debug=False)
