import pytest

from tests.util import (
    COMBINED_DATA_GAME_DICT,
    update_scraped_bbref_games_for_date,
    update_scraped_boxscore,
    update_scraped_brooks_games_for_date,
    update_scraped_pitch_logs,
    update_scraped_pitchfx_logs,
)
from vigorish.data.all_game_data import AllGameData
from vigorish.enums import DefensePosition
from vigorish.tasks.add_pitchfx_to_database import AddPitchFxToDatabase
from vigorish.tasks.combine_scraped_data import CombineScrapedDataTask

TEST_ID = "NO_ERRORS"
GAME_DICT = COMBINED_DATA_GAME_DICT[TEST_ID]
GAME_DATE = COMBINED_DATA_GAME_DICT[TEST_ID]["game_date"]


@pytest.fixture(scope="module", autouse=True)
def create_test_data(vig_app):
    """Initialize DB with data to verify test functions in test_all_game_data module."""
    db_session = vig_app["db_session"]
    scraped_data = vig_app["scraped_data"]
    game_date = GAME_DICT["game_date"]
    bbref_game_id = GAME_DICT["bbref_game_id"]
    bb_game_id = GAME_DICT["bb_game_id"]
    apply_patch_list = GAME_DICT["apply_patch_list"]
    update_scraped_bbref_games_for_date(db_session, scraped_data, game_date)
    update_scraped_brooks_games_for_date(db_session, scraped_data, game_date)
    update_scraped_boxscore(db_session, scraped_data, bbref_game_id)
    update_scraped_pitch_logs(db_session, scraped_data, game_date, bbref_game_id)
    update_scraped_pitchfx_logs(db_session, scraped_data, bb_game_id)
    CombineScrapedDataTask(vig_app).execute(bbref_game_id, apply_patch_list)
    add_pfx_to_db = AddPitchFxToDatabase(vig_app)
    add_pfx_to_db.execute(vig_app["scraped_data"].get_audit_report(), 2019)
    db_session.commit()
    return True


def test_all_game_data(vig_app):
    all_game_data = AllGameData(vig_app, GAME_DICT["bbref_game_id"])
    away_team_id = all_game_data.away_team_id
    home_team_id = all_game_data.home_team_id
    assert away_team_id == "LAA"
    assert home_team_id == "TOR"
    assert all_game_data.away_team.team_id_br == away_team_id
    assert all_game_data.home_team.team_id_br == home_team_id

    pitch_app_player_ids = all_game_data.pitch_stats_player_ids
    bat_stats_player_ids = all_game_data.bat_stats_player_ids
    pitch_app_player_id = pitch_app_player_ids[0]
    result = all_game_data.view_valid_at_bats_for_pitcher(pitch_app_player_id)
    assert result.success
    bat_stats_player_id = bat_stats_player_ids[0]
    result = all_game_data.view_valid_at_bats_for_batter(bat_stats_player_id)
    assert result.success

    away_team_bat_boxscore = all_game_data.bat_boxscore[away_team_id]
    away_team_lineup_1 = away_team_bat_boxscore[1]
    assert away_team_lineup_1["team_id"] == "LAA"
    assert away_team_lineup_1["name"] == "Tommy La Stella"
    assert away_team_lineup_1["bbref_id"] == "lasteto01"
    assert away_team_lineup_1["mlb_id"] == 600303
    assert away_team_lineup_1["def_position"] == DefensePosition.SECOND_BASE
    assert away_team_lineup_1["at_bats"] == "0/4"
    assert away_team_lineup_1["bat_stats"] == "R, 2K, BB"
    assert away_team_lineup_1["stats_to_date"] == ".296/.354/.515/.869"

    away_team_bat_boxscore_again = all_game_data.bat_boxscore[away_team_id]
    assert away_team_bat_boxscore_again is away_team_bat_boxscore

    home_team_bat_boxscore = all_game_data.bat_boxscore[home_team_id]
    home_team_lineup_1 = home_team_bat_boxscore[1]
    assert home_team_lineup_1["team_id"] == "TOR"
    assert home_team_lineup_1["name"] == "Eric Sogard"
    assert home_team_lineup_1["bbref_id"] == "sogarer01"
    assert home_team_lineup_1["mlb_id"] == 519299
    assert home_team_lineup_1["def_position"] == DefensePosition.DH
    assert home_team_lineup_1["at_bats"] == "2/5"
    assert home_team_lineup_1["bat_stats"] == "R"
    assert home_team_lineup_1["stats_to_date"] == ".294/.361/.485/.845"

    home_team_bat_boxscore_again = all_game_data.bat_boxscore[home_team_id]
    assert home_team_bat_boxscore_again is home_team_bat_boxscore

    away_team_pitch_boxscore = all_game_data.pitch_boxscore[away_team_id]
    away_team_sp = away_team_pitch_boxscore["SP"]
    assert away_team_sp["team_id"] == "LAA"
    assert away_team_sp["name"] == "Luis Garcia"
    assert away_team_sp["mlb_id"] == 472610
    assert away_team_sp["bbref_id"] == "garcilu03"
    assert away_team_sp["pitch_app_type"] == "SP"
    assert away_team_sp["game_results"] == "1.0 IP, 1ER, H, K (GS: 48)"

    away_team_pitch_boxscore_again = all_game_data.pitch_boxscore[away_team_id]
    assert away_team_pitch_boxscore_again is away_team_pitch_boxscore

    home_team_pitch_boxscore = all_game_data.pitch_boxscore[home_team_id]
    home_team_sp = home_team_pitch_boxscore["SP"]
    assert home_team_sp["team_id"] == "TOR"
    assert home_team_sp["name"] == "Derek Law"
    assert home_team_sp["mlb_id"] == 571882
    assert home_team_sp["bbref_id"] == "lawde01"
    assert home_team_sp["pitch_app_type"] == "SP"
    assert home_team_sp["game_results"] == "1.0 IP, 0ER, 2K (GS: 55)"

    home_team_pitch_boxscore_again = all_game_data.pitch_boxscore[home_team_id]
    assert home_team_pitch_boxscore_again is home_team_pitch_boxscore

    game_meta_data_viewer = all_game_data.view_game_meta_info()
    assert game_meta_data_viewer

    result = all_game_data.view_at_bats_by_inning()
    assert result.success

    mlb_id_str = str(bat_stats_player_id)
    result = all_game_data.validate_mlb_id(mlb_id_str)
    assert result.success
    mlb_id = result.value
    assert mlb_id == bat_stats_player_id

    pitch_mix = all_game_data.get_pitch_mix(571882)
    assert "all" in pitch_mix and pitch_mix["all"] == {
        "ALL": {"count": 17, "percent": 1},
        "CH": {"count": 3, "percent": 0.17647058823529413},
        "CU": {"count": 6, "percent": 0.35294117647058826},
        "FF": {"count": 6, "percent": 0.35294117647058826},
        "SL": {"count": 2, "percent": 0.11764705882352941},
    }
    assert "bat_l" in pitch_mix and pitch_mix["bat_l"] == {
        "ALL": {"count": 12, "percent": 1},
        "CH": {"count": 3, "percent": 0.25},
        "CU": {"count": 4, "percent": 0.3333333333333333},
        "FF": {"count": 3, "percent": 0.25},
        "SL": {"count": 2, "percent": 0.16666666666666666},
    }
    assert "bat_r" in pitch_mix and pitch_mix["bat_r"] == {
        "ALL": {"count": 5, "percent": 1},
        "CU": {"count": 2, "percent": 0.4},
        "FF": {"count": 3, "percent": 0.6},
    }
    assert "by_year" in pitch_mix and 2019 in pitch_mix["by_year"]
    assert pitch_mix["by_year"][2019] == {
        "ALL": {"count": 17, "percent": 1},
        "CH": {"count": 3, "percent": 0.17647058823529413},
        "CU": {"count": 6, "percent": 0.35294117647058826},
        "FF": {"count": 6, "percent": 0.35294117647058826},
        "SL": {"count": 2, "percent": 0.11764705882352941},
    }
    assert "pitch_app" in pitch_mix and pitch_mix["pitch_app"] == {
        "ALL": {"count": 17, "percent": 1},
        "CH": {"count": 3, "percent": 0.17647058823529413},
        "CU": {"count": 6, "percent": 0.35294117647058826},
        "FF": {"count": 6, "percent": 0.35294117647058826},
        "SL": {"count": 2, "percent": 0.11764705882352941},
    }

    matchup = all_game_data.get_matchup_details()
    assert matchup == (
        "Monday, June 17 2019 07:07 PM (UTC-04:00)\n"
        "Los Angeles Angels of Anaheim (36-37) vs Toronto Blue Jays (26-46)\n"
    )

    linescore = all_game_data.get_linescore()
    assert "TOR   1   0   0   0   0   0   2   2   0   5   8   0" in linescore
    assert "TOR   1   0   0   0   0   0   2   2   0   5   8   0" in linescore
