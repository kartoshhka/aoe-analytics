-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS gold;

------------------------------------------------------------
-- 1. Player summary (max Elo, total matches, wins, loses, winrate)
------------------------------------------------------------
-- One row per player-match
CREATE OR REPLACE TABLE gold.player_match_results AS
SELECT
    player_id,
    match_id,
    MAX(elo) AS elo,    -- get the player's elo in that match
    MAX(win) AS win     -- win flag per match
FROM events_clean
GROUP BY player_id, match_id;

CREATE OR REPLACE TABLE gold.player_summary AS
SELECT
    player_id,
    MAX(elo) AS max_elo,
    COUNT(DISTINCT match_id) AS total_matches,
    SUM(CASE WHEN win = 1 THEN 1 ELSE 0 END) AS total_wins,
    SUM(CASE WHEN win = 0 THEN 1 ELSE 0 END) AS total_loses,
    CAST(SUM(CASE WHEN win = 1 THEN 1 ELSE 0 END) AS DOUBLE)
        / COUNT(DISTINCT match_id) AS winrate
FROM gold.player_match_results
WHERE player_id IS NOT NULL
GROUP BY player_id
ORDER BY max_elo DESC;

------------------------------------------------------------
-- 1. Events per minute (EPM) – first 10 minutes
------------------------------------------------------------
CREATE OR REPLACE TABLE gold.apm AS
SELECT
    match_id,
    player_id,
    COUNT(*) * 60.0 / 600 AS apm
FROM events_clean
WHERE seconds_since_start <= 600
GROUP BY match_id, player_id;

------------------------------------------------------------
-- 2. Age timings – find dynamically all activities containing "age"
------------------------------------------------------------
CREATE OR REPLACE TABLE gold.age_timings AS
SELECT
    civilization,
    activity,
    ROUND(AVG(seconds_since_start) / 60.0, 2) AS avg_time_min
FROM events_clean
WHERE REGEXP_MATCHES(LOWER(activity), '(\b)age(\b)')
GROUP BY civilization, activity
ORDER BY civilization;

------------------------------------------------------------
-- 3. Opening build orders – first 50 actions of high-elo players
------------------------------------------------------------
CREATE OR REPLACE TABLE gold.openings AS
WITH ranked AS (
    SELECT
        civilization,
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
)
SELECT
    civilization,
    match_id,
    player_id,
    elo,
    activity,
    action_rank
FROM ranked
WHERE action_rank <= 50
ORDER BY match_id, player_id, action_rank;

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
