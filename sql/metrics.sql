-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS gold;

------------------------------------------------------------
-- 1. Actions per minute (APM) – first 10 minutes
------------------------------------------------------------
CREATE OR REPLACE TABLE gold.apm AS
SELECT
    match_id,
    player_id,
    COUNT(*) * 60.0 / 600 AS apm_10min
FROM events_clean
WHERE seconds_since_start <= 600
GROUP BY match_id, player_id;

------------------------------------------------------------
-- 2. Age timings – find dynamically all activities containing "age"
------------------------------------------------------------
CREATE OR REPLACE TABLE gold.age_timings AS
SELECT
    match_id,
    player_id,
    activity AS age_research,
    MIN(seconds_since_start) AS reached_at
FROM events_clean
WHERE LOWER(activity) LIKE '%age%'
GROUP BY match_id, player_id, activity;

------------------------------------------------------------
-- 3. Opening build orders – first 50 actions of high-elo players
------------------------------------------------------------
CREATE OR REPLACE TABLE gold.openings AS
WITH ranked AS (
    SELECT
        match_id,
        player_id,
        elo,
        activity,
        seconds_since_start,
        ROW_NUMBER() OVER (
            PARTITION BY match_id, player_id
            ORDER BY seconds_since_start ASC
        ) AS action_rank
    FROM events_clean
),
high_elo AS (
    SELECT percentile_disc(0.75) WITHIN GROUP (ORDER BY elo) AS q3
    FROM (
        SELECT DISTINCT player_id, elo FROM events_clean
    )
)
SELECT
    r.match_id,
    r.player_id,
    r.activity,
    r.action_rank
FROM ranked r, high_elo h
WHERE r.elo >= h.q3
  AND r.action_rank <= 50
ORDER BY r.match_id, r.player_id, r.action_rank;

------------------------------------------------------------
-- 4. Winrate by civilization – dynamic civilizations
------------------------------------------------------------
CREATE OR REPLACE TABLE gold.winrate_civ AS
SELECT
    civilization,
    COUNT(*) FILTER (WHERE win = 1) * 1.0 / COUNT(*) AS winrate,
    COUNT(*) AS total_games
FROM (
    SELECT DISTINCT match_id, player_id, civilization, win
    FROM events_clean
    WHERE civilization IS NOT NULL
)
GROUP BY civilization
ORDER BY winrate DESC;
