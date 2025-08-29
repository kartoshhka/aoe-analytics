-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS gold;

------------------------------------------------------------
-- 1. Player summary (max Elo, total matches, wins, loses, winrate)
------------------------------------------------------------
-- One row per player-match
CREATE OR REPLACE TABLE gold.player_summary AS
WITH strat AS (
    SELECT
        player_id,
        match_id,
        strategy
    FROM events_clean
    GROUP BY player_id, match_id, strategy
),
strat_count AS (
    SELECT
        player_id,
        strategy,
        COUNT(*) AS matches_with_strategy
    FROM strat
    WHERE strategy IS NOT NULL
    GROUP BY player_id, strategy
),
strategy_list AS (
    SELECT
        player_id,
        STRING_AGG(strategy || ' - ' || matches_with_strategy, ', ') AS used_strategies
    FROM strat_count
    GROUP BY player_id
)
SELECT
    pmr.player_id,
    MAX(pmr.elo) AS max_elo,
    COUNT(DISTINCT pmr.match_id) AS total_matches,
    SUM(pmr.win) AS total_wins,
    COUNT(*) - SUM(pmr.win) AS total_loses,
    SUM(pmr.win) * 1.0 / COUNT(DISTINCT pmr.match_id) AS winrate,
    sl.used_strategies
FROM gold.player_match_results pmr
LEFT JOIN strategy_list sl ON pmr.player_id = sl.player_id
WHERE pmr.player_id IS NOT NULL
GROUP BY pmr.player_id, sl.used_strategies
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
    ROUND(AVG(seconds_since_start) / 60.0, 2) AS avg_time_mins
FROM events_clean
WHERE REGEXP_MATCHES(LOWER(activity), '(\b)age(\b)')
GROUP BY civilization, activity
ORDER BY civilization, avg_time_mins;

------------------------------------------------------------
-- 3. Opening build orders – first 100 actions
------------------------------------------------------------
CREATE OR REPLACE TABLE gold.openings AS
WITH ranked AS (
    SELECT
        civilization,
        civilization_category,
        map_type,
        strategy,
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
    r.civilization,
    r.civilization_category,
    r.map_type,
    r.strategy,
    r.match_id,
    r.player_id,
    r.elo,
    r.activity,
    r.action_rank,
    pmr.win  -- Add win flag from player_match_results
FROM ranked r
LEFT JOIN gold.player_match_results pmr
    ON r.player_id = pmr.player_id AND r.match_id = pmr.match_id
WHERE r.action_rank <= 100
ORDER BY r.elo, r.match_id, r.player_id, r.action_rank;

------------------------------------------------------------
-- 4. Winrate & playrate by civilization – dynamic civilizations
------------------------------------------------------------
CREATE OR REPLACE TABLE gold.winrate_civ AS
WITH base AS (
    SELECT DISTINCT match_id, player_id, civilization, win
    FROM events_clean
    WHERE civilization IS NOT NULL
),
totals AS (
    SELECT COUNT(*) AS all_games
    FROM base
)
SELECT
    b.civilization,
    COUNT(*) FILTER (WHERE b.win = 1) * 1.0 / COUNT(*) AS winrate,
    COUNT(*) AS total_games,
    COUNT(*) * 1.0 / t.all_games AS playrate
FROM base b
CROSS JOIN totals t
GROUP BY b.civilization, t.all_games
ORDER BY winrate DESC;
------------------------------------------------------------
-- 4. Winrate by strategy
------------------------------------------------------------
CREATE OR REPLACE TABLE gold.winrate_strat AS
SELECT
    strategy,
    COUNT(*) FILTER (WHERE win = 1) * 1.0 / COUNT(*) AS winrate,
    COUNT(*) AS total_games,
    STRING_AGG(DISTINCT civilization, ', ') AS civilizations,
FROM (
    SELECT DISTINCT strategy, match_id, player_id, civilization, win
    FROM events_clean
    WHERE strategy IS NOT NULL
)
GROUP BY strategy
ORDER BY winrate DESC;
