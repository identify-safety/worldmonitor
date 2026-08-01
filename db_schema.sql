-- WorldMonitor v2 NeonDB Schema
-- Historical tracking for intelligence snapshots and Country Instability Index

-- Drop existing tables if needed (use with caution in production)
-- DROP TABLE IF EXISTS worldmonitor_cii_history;
-- DROP TABLE IF EXISTS worldmonitor_snapshots;

-- ============================================================
-- Table: worldmonitor_snapshots
-- Stores daily snapshots of global intelligence metrics
-- ============================================================

CREATE TABLE IF NOT EXISTS worldmonitor_snapshots (
    id SERIAL PRIMARY KEY,
    snapshot_date DATE NOT NULL DEFAULT CURRENT_DATE,
    created_at TIMESTAMPTZ DEFAULT NOW(),

    -- Raw counts from data sources
    earthquake_count INT DEFAULT 0,
    max_earthquake_mag NUMERIC(3,1),
    disaster_count INT DEFAULT 0,
    red_alert_count INT DEFAULT 0,
    nasa_event_count INT DEFAULT 0,

    -- Crypto market data
    btc_price NUMERIC(12,2),
    btc_change_24h NUMERIC(6,2),
    eth_price NUMERIC(12,2),
    eth_change_24h NUMERIC(6,2),
    sol_price NUMERIC(12,2),
    sol_change_24h NUMERIC(6,2),

    -- Fear & Greed Index
    fear_greed_index INT,
    fear_greed_label VARCHAR(30),

    -- Country Instability Index aggregates
    cii_top_country VARCHAR(2),
    cii_top_score INT,
    cii_critical_count INT DEFAULT 0,
    cii_high_count INT DEFAULT 0,
    cii_elevated_count INT DEFAULT 0,

    -- Prediction markets
    polymarket_top_event TEXT,
    polymarket_top_probability NUMERIC(5,2),
    polymarket_volume_total NUMERIC(15,2),

    -- UCDP conflict data
    ucdp_event_count INT DEFAULT 0,
    ucdp_fatalities_total INT DEFAULT 0,

    -- GDELT article count
    gdelt_article_count INT DEFAULT 0,

    -- Fed funds rate
    fed_funds_rate NUMERIC(5,2),

    -- Summary arrays
    convergence_zones TEXT[],     -- countries with multi-signal convergence
    anomaly_flags TEXT[],          -- detected anomalies
    total_signals INT DEFAULT 0,

    -- Ensure one snapshot per day
    UNIQUE(snapshot_date)
);

-- Index for date-based queries
CREATE INDEX IF NOT EXISTS idx_snapshots_date ON worldmonitor_snapshots(snapshot_date DESC);

-- Index for CII queries
CREATE INDEX IF NOT EXISTS idx_snapshots_cii ON worldmonitor_snapshots(cii_top_score DESC);

-- ============================================================
-- Table: worldmonitor_cii_history
-- Stores daily Country Instability Index scores per country
-- ============================================================

CREATE TABLE IF NOT EXISTS worldmonitor_cii_history (
    id SERIAL PRIMARY KEY,
    snapshot_date DATE NOT NULL DEFAULT CURRENT_DATE,
    country_code VARCHAR(2) NOT NULL,
    country_name VARCHAR(50),

    -- Overall score and level
    score INT NOT NULL,
    level VARCHAR(10) NOT NULL, -- low, normal, elevated, high, critical

    -- Component scores (0-100 each)
    unrest_component INT DEFAULT 0,
    conflict_component INT DEFAULT 0,
    security_component INT DEFAULT 0,
    information_component INT DEFAULT 0,

    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),

    -- Ensure one entry per country per day
    UNIQUE(snapshot_date, country_code)
);

-- Index for date-based queries
CREATE INDEX IF NOT EXISTS idx_cii_date ON worldmonitor_cii_history(snapshot_date DESC);

-- Index for country-based queries
CREATE INDEX IF NOT EXISTS idx_cii_country ON worldmonitor_cii_history(country_code);

-- Index for score-based queries (find highest risk countries)
CREATE INDEX IF NOT EXISTS idx_cii_score ON worldmonitor_cii_history(score DESC);

-- Index for level-based queries (find critical/high countries)
CREATE INDEX IF NOT EXISTS idx_cii_level ON worldmonitor_cii_history(level);

-- Composite index for country trend queries
CREATE INDEX IF NOT EXISTS idx_cii_country_date ON worldmonitor_cii_history(country_code, snapshot_date DESC);

-- ============================================================
-- Helper Views
-- ============================================================

-- Latest snapshot summary
CREATE OR REPLACE VIEW v_latest_snapshot AS
SELECT
    snapshot_date,
    earthquake_count,
    max_earthquake_mag,
    disaster_count,
    red_alert_count,
    cii_top_country,
    cii_top_score,
    cii_critical_count,
    cii_high_count,
    btc_price,
    btc_change_24h,
    fear_greed_index,
    fear_greed_label,
    total_signals
FROM worldmonitor_snapshots
ORDER BY snapshot_date DESC
LIMIT 1;

-- Latest CII scores with trends
CREATE OR REPLACE VIEW v_latest_cii AS
WITH latest AS (
    SELECT country_code, score, level, snapshot_date
    FROM worldmonitor_cii_history
    WHERE snapshot_date = CURRENT_DATE
),
previous AS (
    SELECT country_code, score as prev_score
    FROM worldmonitor_cii_history
    WHERE snapshot_date = CURRENT_DATE - INTERVAL '1 day'
)
SELECT
    l.country_code,
    l.score,
    l.level,
    COALESCE(p.prev_score, l.score) as prev_score,
    (l.score - COALESCE(p.prev_score, l.score)) as change_24h,
    CASE
        WHEN l.score - COALESCE(p.prev_score, l.score) >= 3 THEN 'rising'
        WHEN COALESCE(p.prev_score, l.score) - l.score >= 3 THEN 'falling'
        ELSE 'stable'
    END as trend
FROM latest l
LEFT JOIN previous p ON l.country_code = p.country_code
ORDER BY l.score DESC;

-- 7-day rolling averages for anomaly detection
CREATE OR REPLACE VIEW v_rolling_avg_7d AS
SELECT
    AVG(earthquake_count) as avg_earthquake_count,
    AVG(disaster_count) as avg_disaster_count,
    AVG(red_alert_count) as avg_red_alert_count,
    AVG(fear_greed_index) as avg_fear_greed,
    AVG(btc_price) as avg_btc_price,
    AVG(total_signals) as avg_total_signals,
    STDDEV(earthquake_count) as std_earthquake_count,
    STDDEV(fear_greed_index) as std_fear_greed
FROM worldmonitor_snapshots
WHERE snapshot_date >= CURRENT_DATE - INTERVAL '7 days'
  AND snapshot_date < CURRENT_DATE;

-- Country risk trends (30-day)
CREATE OR REPLACE VIEW v_country_trends_30d AS
SELECT
    country_code,
    country_name,
    AVG(score) as avg_score,
    MAX(score) as max_score,
    MIN(score) as min_score,
    COUNT(*) as data_points,
    SUM(CASE WHEN level IN ('critical', 'high') THEN 1 ELSE 0 END) as high_risk_days
FROM worldmonitor_cii_history
WHERE snapshot_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY country_code, country_name
ORDER BY avg_score DESC;

-- ============================================================
-- Sample Queries
-- ============================================================

-- Get latest snapshot summary
-- SELECT * FROM v_latest_snapshot;

-- Get latest CII scores with trends
-- SELECT * FROM v_latest_cii WHERE level IN ('critical', 'high');

-- Get 7-day averages for anomaly detection
-- SELECT * FROM v_rolling_avg_7d;

-- Get countries with rising instability (7-day trend)
-- SELECT
--     country_code,
--     score,
--     LAG(score, 7) OVER (PARTITION BY country_code ORDER BY snapshot_date) as score_7d_ago,
--     score - LAG(score, 7) OVER (PARTITION BY country_code ORDER BY snapshot_date) as change_7d
-- FROM worldmonitor_cii_history
-- WHERE snapshot_date >= CURRENT_DATE - INTERVAL '7 days'
-- ORDER BY change_7d DESC NULLS LAST;

-- Get convergence zones over last 7 days
-- SELECT
--     snapshot_date,
--     UNNEST(convergence_zones) as convergence_zone
-- FROM worldmonitor_snapshots
-- WHERE snapshot_date >= CURRENT_DATE - INTERVAL '7 days'
--   AND convergence_zones IS NOT NULL
--   AND array_length(convergence_zones, 1) > 0
-- ORDER BY snapshot_date DESC;

-- Get anomaly history
-- SELECT
--     snapshot_date,
--     UNNEST(anomaly_flags) as anomaly
-- FROM worldmonitor_snapshots
-- WHERE snapshot_date >= CURRENT_DATE - INTERVAL '7 days'
--   AND anomaly_flags IS NOT NULL
--   AND array_length(anomaly_flags, 1) > 0
-- ORDER BY snapshot_date DESC;

-- Get BTC price correlation with global instability
-- SELECT
--     s.snapshot_date,
--     s.btc_price,
--     s.btc_change_24h,
--     s.cii_critical_count + s.cii_high_count as high_risk_countries,
--     s.total_signals
-- FROM worldmonitor_snapshots s
-- WHERE s.snapshot_date >= CURRENT_DATE - INTERVAL '30 days'
-- ORDER BY s.snapshot_date DESC;

-- ============================================================
-- Maintenance
-- ============================================================

-- Clean up old data (retain 90 days)
-- DELETE FROM worldmonitor_snapshots WHERE snapshot_date < CURRENT_DATE - INTERVAL '90 days';
-- DELETE FROM worldmonitor_cii_history WHERE snapshot_date < CURRENT_DATE - INTERVAL '90 days';

-- Vacuum to reclaim space
-- VACUUM ANALYZE worldmonitor_snapshots;
-- VACUUM ANALYZE worldmonitor_cii_history;

-- ============================================================
-- EVENT TRACKING TABLES (v2.1)
-- ============================================================

CREATE TABLE IF NOT EXISTS worldmonitor_events (
    id SERIAL PRIMARY KEY,
    event_type VARCHAR(30) NOT NULL,
    event_source VARCHAR(20) NOT NULL,
    external_id VARCHAR(100),
    title TEXT NOT NULL,
    description TEXT,
    country_code VARCHAR(5),
    region VARCHAR(50),
    severity VARCHAR(10) NOT NULL DEFAULT 'medium',
    status VARCHAR(15) NOT NULL DEFAULT 'new',
    first_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ,
    last_reported_at TIMESTAMPTZ,
    reported_count INT DEFAULT 0,
    follow_up_priority INT DEFAULT 0,
    follow_up_notes TEXT,
    metadata JSONB DEFAULT '{}',
    UNIQUE(event_type, external_id)
);

CREATE TABLE IF NOT EXISTS worldmonitor_escalations (
    id SERIAL PRIMARY KEY,
    event_id INT NOT NULL REFERENCES worldmonitor_events(id),
    escalation_type VARCHAR(30) NOT NULL,
    previous_state JSONB,
    new_state JSONB,
    description TEXT NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS worldmonitor_follow_ups (
    id SERIAL PRIMARY KEY,
    event_id INT REFERENCES worldmonitor_events(id),
    priority INT DEFAULT 5,
    question TEXT NOT NULL,
    search_queries TEXT[],
    status VARCHAR(15) NOT NULL DEFAULT 'pending',
    findings TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS worldmonitor_reports (
    id SERIAL PRIMARY KEY,
    report_date DATE NOT NULL DEFAULT CURRENT_DATE,
    report_type VARCHAR(20) NOT NULL DEFAULT 'morning_briefing',
    event_ids INT[],
    key_findings TEXT,
    follow_up_items TEXT[],
    new_events_count INT DEFAULT 0,
    escalations_count INT DEFAULT 0,
    resolved_count INT DEFAULT 0,
    delivered_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_events_status ON worldmonitor_events(status);
CREATE INDEX IF NOT EXISTS idx_events_type ON worldmonitor_events(event_type);
CREATE INDEX IF NOT EXISTS idx_events_country ON worldmonitor_events(country_code);
CREATE INDEX IF NOT EXISTS idx_events_severity ON worldmonitor_events(severity);
CREATE INDEX IF NOT EXISTS idx_events_follow_up ON worldmonitor_events(follow_up_priority DESC) WHERE status IN ('new','active','escalating');
CREATE INDEX IF NOT EXISTS idx_escalations_event ON worldmonitor_escalations(event_id);
CREATE INDEX IF NOT EXISTS idx_followups_status ON worldmonitor_follow_ups(status) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_reports_date ON worldmonitor_reports(report_date DESC);
