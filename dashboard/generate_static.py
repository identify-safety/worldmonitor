"""
Generate data.json for static dashboard deployment.
Queries Neon PostgreSQL and outputs all API data as a single JSON file.
"""
import asyncio
import asyncpg
import json
import os
import urllib.request
from datetime import date, timedelta, datetime, timezone

DSN = os.environ.get("DATABASE_URL", "")

from decimal import Decimal

async def query(conn, sql, *args):
    rows = await conn.fetch(sql, *args)
    result = []
    for r in rows:
        d = dict(r)
        for k, v in d.items():
            if isinstance(v, (date, datetime)):
                d[k] = v.isoformat()
            elif isinstance(v, Decimal):
                d[k] = float(v)
        result.append(d)
    return result

def _http_get_json(url, timeout=25):
    """Synchronous GET returning parsed JSON. Used for live event detail feeds."""
    req = urllib.request.Request(url, headers={"User-Agent": "WorldMonitor/2.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_usgs_quakes():
    """USGS M4.5+ earthquakes in last 24h, full detail for drill-down."""
    try:
        data = _http_get_json(
            "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_day.geojson"
        )
        out = []
        for f in data.get("features", []):
            p = f.get("properties", {})
            c = f.get("geometry", {}).get("coordinates", [None, None, None])
            t = p.get("time")
            out.append({
                "mag": p.get("mag"),
                "place": p.get("place") or "未知地点",
                "time": datetime.fromtimestamp(t / 1000, tz=timezone.utc).isoformat() if t else None,
                "depth_km": round(c[2], 1) if len(c) > 2 and c[2] is not None else None,
                "lat": c[1] if len(c) > 1 else None,
                "lon": c[0] if len(c) > 0 else None,
                "alert": p.get("alert"),
                "tsunami": bool(p.get("tsunami")),
                "url": p.get("url"),
            })
        out.sort(key=lambda q: (q.get("mag") or 0), reverse=True)
        return out
    except Exception as e:
        print(f"  [warn] fetch USGS failed: {e}")
        return []


def fetch_gdacs_disasters():
    """GDACS orange/red disasters (last 7 days), full detail for drill-down."""
    try:
        url = ("https://www.gdacs.org/gdacsapi/api/events/geteventlist/SEARCH"
               "?alertlevel=orange;red&eventlist=EQ;TC;FL;VO;DR&limit=50")
        data = _http_get_json(url)
        out = []
        for feat in data.get("features", []):
            p = feat.get("properties", {})
            out.append({
                "type": p.get("eventtype"),
                "name": p.get("name") or p.get("eventname"),
                "country": p.get("country"),
                "alert_level": p.get("alertlevel"),
                "date": p.get("fromdate"),
            })
        return out[:30]
    except Exception as e:
        print(f"  [warn] fetch GDACS failed: {e}")
        return []


async def main():
    conn = await asyncpg.connect(DSN)

    # Overview
    rows = await query(conn, "SELECT * FROM v_latest_snapshot")
    overview = rows[0] if rows else {}

    # CII
    cii = await query(conn, "SELECT * FROM v_latest_cii ORDER BY score DESC")

    # History (7 days)
    history = await query(conn,
        "SELECT snapshot_date, earthquake_count, max_earthquake_mag, "
        "disaster_count, btc_price, fear_greed_index, total_signals "
        "FROM worldmonitor_snapshots "
        "WHERE snapshot_date >= $1 ORDER BY snapshot_date ASC",
        date.today() - timedelta(days=7))

    # Anomalies
    anomalies = await query(conn,
        "SELECT snapshot_date, anomaly_flags, convergence_zones "
        "FROM worldmonitor_snapshots WHERE snapshot_date >= $1 "
        "ORDER BY snapshot_date DESC",
        date.today() - timedelta(days=7))

    # Trends
    trends = await query(conn, "SELECT * FROM v_country_trends_30d ORDER BY avg_score DESC")

    # Latest snapshot for briefing/alerts
    snap_rows = await query(conn, "SELECT * FROM worldmonitor_snapshots ORDER BY id DESC LIMIT 1")
    snap = snap_rows[0] if snap_rows else {}

    snap_date_str = snap.get("snapshot_date", "")
    snap_date = date.fromisoformat(snap_date_str) if snap_date_str else date.today()
    cii_rows = await query(conn,
        "SELECT country_code, country_name, score, level "
        "FROM worldmonitor_cii_history WHERE snapshot_date = $1 "
        "ORDER BY score DESC",
        snap_date)

    # Build briefing
    briefing = build_briefing(snap, cii_rows)

    # Build alerts
    alerts = build_alerts(snap)

    # Live event detail (drill-down): USGS earthquakes + GDACS disasters
    earthquakes = await asyncio.to_thread(fetch_usgs_quakes)
    disasters = await asyncio.to_thread(fetch_gdacs_disasters)

    data = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "overview": overview,
        "cii": cii,
        "history": history,
        "anomalies": anomalies,
        "trends": trends,
        "briefing": briefing,
        "alerts": alerts,
        "earthquakes": earthquakes,
        "disasters": disasters,
    }

    await conn.close()

    out_path = os.path.join(os.path.dirname(__file__), "static", "data.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Generated {out_path}")
    print(f"  Overview: {bool(overview)}")
    print(f"  CII entries: {len(cii)}")
    print(f"  History points: {len(history)}")
    print(f"  Briefing sections: {len(briefing.get('sections', []))}")
    print(f"  Alerts: {len(alerts.get('alerts', []))}")
    print(f"  Earthquakes (detail): {len(earthquakes)}")
    print(f"  Disasters (detail): {len(disasters)}")


def build_briefing(s, cii_rows):
    sections = []

    eq = s.get("earthquake_count", 0)
    max_mag = s.get("max_earthquake_mag")
    if eq > 0:
        if max_mag and float(max_mag) >= 7:
            level = "critical"
            text = f"全球发生 {eq} 次地震，其中最大震级 M{max_mag}，属于强震，需关注可能的海啸和余震风险。"
        elif max_mag and float(max_mag) >= 6:
            level = "warning"
            text = f"全球记录 {eq} 次地震，最大 M{max_mag}，中度活跃。"
        else:
            level = "normal"
            text = f"全球记录 {eq} 次地震，最大 M{max_mag}，地震活动处于正常水平。"
        sections.append({"icon": "🌋", "title": "地震活动", "level": level, "text": text})

    disasters = s.get("disaster_count", 0)
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

    btc = s.get("btc_price")
    btc_chg = s.get("btc_change_24h")
    fgi = s.get("fear_greed_index")
    fgi_label = s.get("fear_greed_label", "")
    if btc:
        if btc_chg is not None and abs(float(btc_chg)) >= 5:
            level = "critical"
        elif fgi and int(fgi) <= 30:
            level = "warning"
        else:
            level = "normal"
        chg_str = f"{float(btc_chg):+.2f}%" if btc_chg is not None else "—"
        text = f"BTC 报 ${float(btc):,.0f}，日涨跌 {chg_str}。恐惧与贪婪指数 {fgi}（{fgi_label}）。"
        if fgi and int(fgi) <= 25:
            text += " 市场处于极度恐惧状态，历史上此类极端情绪往往出现在阶段性底部附近，但也可能预示进一步下跌。"
        elif fgi and int(fgi) >= 75:
            text += " 市场处于极度贪婪状态，短期回调风险升高。"
        sections.append({"icon": "🪙", "title": "加密市场", "level": level, "text": text})

    top_cii = [(r["country_code"], r["country_name"], r["score"], r["level"]) for r in cii_rows[:5]]
    if top_cii:
        top_str = "、".join([f"{name}({score})" for code, name, score, lv in top_cii[:3]])
        max_score = top_cii[0][2]
        if int(max_score) >= 80:
            level = "critical"
        elif int(max_score) >= 60:
            level = "warning"
        else:
            level = "normal"
        text = f"国家不稳定指数前三：{top_str}。"
        elevated = sum(1 for r in cii_rows if r["level"] in ("elevated", "high", "critical"))
        if elevated > 5:
            text += f" 目前 {elevated} 个国家的风险等级处于 elevated 或以上，全球地缘政治紧张度偏高。"
        sections.append({"icon": "🏴", "title": "地缘风险", "level": level, "text": text})

    nasa = s.get("nasa_event_count") or 0
    if nasa > 0:
        sections.append({"icon": "🌡️", "title": "气候与空间", "level": "normal",
                          "text": f"NASA EONET 记录 {nasa} 起自然灾害和空间事件，涵盖野火、台风、火山活动等。"})

    anomaly_flags = s.get("anomaly_flags") or []
    if isinstance(anomaly_flags, str):
        import ast
        try:
            anomaly_flags = ast.literal_eval(anomaly_flags)
        except Exception:
            anomaly_flags = []
    if anomaly_flags:
        sections.append({"icon": "🚨", "title": "异常信号", "level": "critical",
                          "text": "；".join(anomaly_flags)})

    critical_count = sum(1 for sec in sections if sec["level"] == "critical")
    warning_count = sum(1 for sec in sections if sec["level"] == "warning")

    if critical_count >= 2:
        overall = {"level": "critical", "text": "全球态势高度紧张：多重危机信号同时出现，建议密切关注后续发展。"}
    elif critical_count >= 1 or warning_count >= 2:
        overall = {"level": "warning", "text": "全球态势值得关注：部分领域的风险指标出现异常，需持续跟踪。"}
    else:
        overall = {"level": "normal", "text": "全球态势总体平稳，无重大异常信号。"}

    return {
        "date": str(s.get("snapshot_date", "")),
        "total_signals": s.get("total_signals", 0),
        "overall": overall,
        "sections": sections,
    }


def build_alerts(s):
    ALERT_RULES = [
        ("eq_critical", "🌋", "强震警报", "critical",
         lambda x: x.get("max_earthquake_mag") and float(x["max_earthquake_mag"]) >= 7.0,
         lambda x: f"检测到 M{x['max_earthquake_mag']} 强震（阈值 ≥ M7.0）"),
        ("eq_warning", "🌋", "中型地震", "warning",
         lambda x: x.get("max_earthquake_mag") and 6.0 <= float(x["max_earthquake_mag"]) < 7.0,
         lambda x: f"检测到 M{x['max_earthquake_mag']} 地震（阈值 ≥ M6.0）"),
        ("disaster_surge", "🛰️", "灾害密集", "critical",
         lambda x: (x.get("red_alert_count") or 0) >= 5,
         lambda x: f"{x['red_alert_count']} 起红色警报同时活跃（阈值 ≥ 5）"),
        ("fear_extreme", "😱", "极度恐惧", "warning",
         lambda x: x.get("fear_greed_index") is not None and int(x["fear_greed_index"]) <= 20,
         lambda x: f"恐惧指数 {x['fear_greed_index']}，处于极度恐惧区间（阈值 ≤ 20）"),
        ("fear_elevated", "😰", "恐惧偏高", "info",
         lambda x: x.get("fear_greed_index") is not None and 20 < int(x["fear_greed_index"]) <= 30,
         lambda x: f"恐惧指数 {x['fear_greed_index']}，市场情绪偏谨慎（阈值 ≤ 30）"),
        ("greed_extreme", "🤑", "极度贪婪", "warning",
         lambda x: x.get("fear_greed_index") is not None and int(x["fear_greed_index"]) >= 80,
         lambda x: f"贪婪指数 {x['fear_greed_index']}，处于极度贪婪区间（阈值 ≥ 80）"),
        ("btc_volatile", "📉", "BTC剧烈波动", "critical",
         lambda x: x.get("btc_change_24h") is not None and abs(float(x["btc_change_24h"])) >= 5,
         lambda x: f"BTC 24小时波动 {float(x['btc_change_24h']):+.1f}%（阈值 ≥ ±5%）"),
        ("cii_critical", "🏴", "极高不稳定", "critical",
         lambda x: x.get("cii_top_score") is not None and int(x["cii_top_score"]) >= 80,
         lambda x: f"国家 {x['cii_top_country']} CII 达到 {x['cii_top_score']}（阈值 ≥ 80）"),
        ("cii_high", "🏴", "高度不稳定", "warning",
         lambda x: x.get("cii_top_score") is not None and 60 <= int(x["cii_top_score"]) < 80,
         lambda x: f"国家 {x['cii_top_country']} CII 达到 {x['cii_top_score']}（阈值 ≥ 60）"),
    ]

    triggered = []
    for rid, icon, title, level, check, msg in ALERT_RULES:
        try:
            if check(s):
                triggered.append({"id": rid, "icon": icon, "title": title, "level": level, "message": msg(s)})
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

    return {"alerts": triggered, "summary": summary, "count": len(triggered)}


if __name__ == "__main__":
    asyncio.run(main())
