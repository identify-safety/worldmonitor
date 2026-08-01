# WorldMonitor v2 — GitHub Actions 部署指南

## 需要推到 GitHub 仓库的文件

```
worldmonitor/
├── intel_engine.py          # 数据采集引擎
├── db_schema.sql             # 数据库建表脚本
├── requirements.txt          # Python 依赖
├── check_alerts.py           # 告警检查脚本
├── .github/
│   └── workflows/
│       └── worldmonitor.yml  # GitHub Actions 定时任务
└── dashboard/
    ├── generate_static.py    # 生成 data.json
    └── static/
        └── index.html        # 静态仪表盘
```

## 部署步骤

### 1. 创建 GitHub 仓库（Public）

```bash
cd ~/.worldmonitor
git init
git add intel_engine.py db_schema.sql requirements.txt check_alerts.py .github/ dashboard/
git commit -m "WorldMonitor v2"
# 在 GitHub 上创建一个 public 仓库，然后：
git remote add origin https://github.com/<你的用户名>/worldmonitor.git
git push -u origin main
```

### 2. 配置 Secrets

在仓库页面：Settings → Secrets and variables → Actions → New repository secret

| Secret 名称 | 值 |
|---|---|
| `DATABASE_URL` | `postgresql://neondb_owner:npg_ycsdS6V3qkEK@ep-proud-sun-ayh9ltmg-pooler.c-5.us-east-2.aws.neon.tech/neondb?sslmode=require` |
| `ALERT_WEBHOOK` | （可选）告警推送 webhook URL，见下方说明 |

### 3. 验证

推送代码后，去仓库的 Actions 页面，手动触发一次 `WorldMonitor Data Collection` workflow。
如果成功，检查 Neon 数据库是否新增了一条 snapshot。

## 告警推送配置（ALERT_WEBHOOK）

当检测到异常时，GitHub Actions 会向 `ALERT_WEBHOOK` 发送 POST 请求：

```json
{"text": "🔴 WorldMonitor 异常告警\n\n🌋 M7.2 强震警报\n📉 BTC 24h波动 -6.5%\n\n快照时间: 2026-08-01"}
```

### 推荐的 Webhook 方案

**方案 A: Telegram Bot（推荐，最简单）**

1. 在 Telegram 找 @BotFather，创建一个 bot，获取 token
2. 把 bot 加到一个群或你的私聊
3. 获取 chat_id
4. Webhook URL 设为：
   ```
   https://api.telegram.org/bot<TOKEN>/sendMessage?chat_id=<CHAT_ID>
   ```
   注意：workflow 里的 curl 已经用了 `{"text": "..."}` 格式，
   Telegram 需要改成 `{"chat_id": "<CHAT_ID>", "text": "..."}` 格式，
   需要微调 worldmonitor.yml 里的 curl 命令。

**方案 B: Hermes Gateway Webhook**

如果 Hermes 的 gateway 暴露了 HTTP 接口，可以直接推到 Hermes，由他转发到 QQ/微信。
需要确认 Hermes gateway 的 webhook URL。

**方案 C: 不配 Webhook**

不设 `ALERT_WEBHOOK` secret，告警检查照常运行，只是不推送。
你可以随时打开 CloudStudio dashboard 查看告警状态。

## cron 频率

当前配置：每小时整点触发（UTC 时间，可能有 5-15 分钟延迟）

如需修改，改 worldmonitor.yml 里的 cron 表达式：
- `'0 */3 * * *'` — 每 3 小时
- `'0 */6 * * *'` — 每 6 小时
- `'30 0 * * *'` — 每天北京时间 8:30

## 免费额度

Public 仓库 GitHub Actions **不限量**，随便跑。
