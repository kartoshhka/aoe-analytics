import duckdb
import pytest
from pathlib import Path

@pytest.fixture(scope="module")
def con():
    DB = Path("warehouse/aoe.duckdb")
    assert DB.exists(), "DuckDB file missing; run transform_events pipeline first."
    con = duckdb.connect(str(DB))
    with open("sql/metrics.sql", "r") as f:
        sql_script = f.read()
    con.execute(f"CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute(f"CREATE SCHEMA IF NOT EXISTS events_clean;")
    con.execute(f"CREATE SCHEMA IF NOT EXISTS gold;")
    con.execute(sql_script)
    yield con
    con.close()

def test_apm_has_expected_columns(con):
    result = con.execute("SELECT * FROM gold.apm LIMIT 1").fetchdf()
    expected_cols = {"match_id", "player_id", "apm"}
    assert expected_cols.issubset(result.columns)


def test_age_timings(con):
    result = con.execute("SELECT * FROM gold.age_timings LIMIT 1").fetchdf()
    expected_cols = {"civilization", "activity", "avg_time_mins"}
    assert expected_cols.issubset(result.columns)


def test_openings(con):
    result = con.execute("SELECT * FROM gold.openings LIMIT 1").fetchdf()
    expected_cols = {"civilization", "civilization_category", "map_type",
        "strategy","match_id", "player_id", "elo", "activity", "action_rank"}
    assert expected_cols.issubset(result.columns)


def test_player_summary(con):
    result = con.execute("SELECT * FROM gold.player_summary LIMIT 1").fetchdf()
    expected_cols = {"player_id", "total_matches", "total_wins", "total_loses", "winrate", "max_elo, used_strategies"}
    assert expected_cols.issubset(result.columns)

def test_winrate_civ(con):
    result = con.execute("SELECT * FROM gold.winrate_civ LIMIT 1").fetchdf()
    expected_cols = {"civilization", "total_games", "winrate", "playrate"}
    assert expected_cols.issubset(result.columns)

def test_winrate_strat(con):
    result = con.execute("SELECT * FROM gold.winrate_strat LIMIT 1").fetchdf()
    expected_cols = {"strategy", "total_games", "winrate", "civilizations"}
    assert expected_cols.issubset(result.columns)

def test_unknown_strategies(con):
    result = con.execute("SELECT * FROM gold.clustered_unknown_strategies LIMIT 1").fetchdf()
    expected_cols = {"cluster_id", "num_matches", "winrate", "num_players",
        "avg_ngram", "avg_jaccard", "avg_levenshtein"}
    assert expected_cols.issubset(result.columns)
