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
from vigorish.tasks.add_to_database import AddToDatabaseTask
from vigorish.tasks.combine_scraped_data import CombineScrapedDataTask

TEST_ID = "NO_ERRORS"
GAME_DICT = COMBINED_DATA_GAME_DICT[TEST_ID]
GAME_DATE = GAME_DICT["game_date"]


@pytest.fixture(scope="module", autouse=True)
def create_test_data(vig_app):
    """Initialize DB with data to verify test functions in test_all_game_data module."""
    db_session = vig_app.db_session
    scraped_data = vig_app.scraped_data
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
    add_to_db = AddToDatabaseTask(vig_app)
    add_to_db.execute(vig_app.scraped_data.get_audit_report(), 2019)
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

    result = all_game_data.validate_mlb_id("derek jeter")
    assert result.failure
    assert 'derek jeter" could not be parsed as a valid int' in result.error

    result = all_game_data.validate_mlb_id(543257)
    assert result.failure
    assert "Error: Player MLB ID: 543257 did not appear in game TOR201906170" in result.error

    pitch_mix_viewer = all_game_data.view_pitch_mix_for_player(571882)
    assert pitch_mix_viewer

    pitch_mix_viewer = all_game_data.view_pd_stats_for_player(571882)
    assert pitch_mix_viewer

    pitch_mix_viewer = all_game_data.view_bb_stats_for_player(571882)
    assert pitch_mix_viewer

    result = all_game_data.get_pitch_mix_data_for_player(571882)
    assert result.success
    pitch_mix = result.value
    assert "all" in pitch_mix
    (pitch_mix_total_all, pitch_mix_detail_all) = pitch_mix["all"]
    assert pitch_mix_total_all == {
        "called_strike_rate": 0.294,
        "contact_rate": 0.235,
        "csw_rate": 0.412,
        "fly_ball_rate": 0.0,
        "ground_ball_rate": 0.0,
        "line_drive_rate": 1.0,
        "money_pitch": 0,
        "o_contact_rate": 0.5,
        "o_swing_rate": 0.25,
        "percent": 1.0,
        "pitch_types": ["CURVEBALL", "FOUR_SEAM_FASTBALL", "CHANGEUP", "SLIDER"],
        "pop_up_rate": 0.0,
        "swing_rate": 0.353,
        "swinging_strike_rate": 0.118,
        "total_batted_balls": 1,
        "total_called_strikes": 5,
        "total_contact_inside_zone": 3,
        "total_contact_outside_zone": 1,
        "total_fly_balls": 0,
        "total_ground_balls": 0,
        "total_inside_strike_zone": 9,
        "total_line_drives": 1,
        "total_outside_strike_zone": 8,
        "total_pitches": 17,
        "total_pop_ups": 0,
        "total_swinging_strikes": 2,
        "total_swings": 6,
        "total_swings_inside_zone": 4,
        "total_swings_made_contact": 4,
        "total_swings_outside_zone": 2,
        "whiff_rate": 0.333,
        "z_contact_rate": 0.75,
        "z_swing_rate": 0.444,
        "zone_rate": 0.529,
    }
    assert pitch_mix_detail_all["CURVEBALL"] == {
        "avg_pfx_x": 2.912,
        "avg_pfx_z": -4.573,
        "avg_px": 0.007,
        "avg_pz": 1.785,
        "avg_speed": 78.692,
        "called_strike_rate": 0.5,
        "contact_rate": 0.167,
        "csw_rate": 0.5,
        "custom_score": 0.833,
        "fly_ball_rate": 0.0,
        "ground_ball_rate": 0.0,
        "line_drive_rate": 0.0,
        "money_pitch": False,
        "o_contact_rate": 1.0,
        "o_swing_rate": 0.333,
        "percent": 0.353,
        "pitch_type": "CURVEBALL",
        "pop_up_rate": 0.0,
        "swing_rate": 0.167,
        "swinging_strike_rate": 0.0,
        "total_batted_balls": 0,
        "total_called_strikes": 3,
        "total_contact_inside_zone": 0,
        "total_contact_outside_zone": 1,
        "total_fly_balls": 0,
        "total_ground_balls": 0,
        "total_inside_strike_zone": 3,
        "total_line_drives": 0,
        "total_outside_strike_zone": 3,
        "total_pitches": 6,
        "total_pop_ups": 0,
        "total_swinging_strikes": 0,
        "total_swings": 1,
        "total_swings_inside_zone": 0,
        "total_swings_made_contact": 1,
        "total_swings_outside_zone": 1,
        "whiff_rate": 0.0,
        "z_contact_rate": 0.0,
        "z_swing_rate": 0.0,
        "zone_rate": 0.5,
    }
    assert pitch_mix_detail_all["FOUR_SEAM_FASTBALL"] == {
        "avg_pfx_x": -1.258,
        "avg_pfx_z": 8.69,
        "avg_px": -0.05,
        "avg_pz": 3.307,
        "avg_speed": 94.26,
        "called_strike_rate": 0.167,
        "contact_rate": 0.333,
        "csw_rate": 0.167,
        "custom_score": 0.5,
        "fly_ball_rate": 0.0,
        "ground_ball_rate": 0.0,
        "line_drive_rate": 1.0,
        "money_pitch": False,
        "o_contact_rate": 0.0,
        "o_swing_rate": 0.0,
        "percent": 0.353,
        "pitch_type": "FOUR_SEAM_FASTBALL",
        "pop_up_rate": 0.0,
        "swing_rate": 0.333,
        "swinging_strike_rate": 0.0,
        "total_batted_balls": 1,
        "total_called_strikes": 1,
        "total_contact_inside_zone": 2,
        "total_contact_outside_zone": 0,
        "total_fly_balls": 0,
        "total_ground_balls": 0,
        "total_inside_strike_zone": 3,
        "total_line_drives": 1,
        "total_outside_strike_zone": 3,
        "total_pitches": 6,
        "total_pop_ups": 0,
        "total_swinging_strikes": 0,
        "total_swings": 2,
        "total_swings_inside_zone": 2,
        "total_swings_made_contact": 2,
        "total_swings_outside_zone": 0,
        "whiff_rate": 0.0,
        "z_contact_rate": 1.0,
        "z_swing_rate": 0.667,
        "zone_rate": 0.5,
    }
    assert pitch_mix_detail_all["CHANGEUP"] == {
        "avg_pfx_x": -6.37,
        "avg_pfx_z": 7.137,
        "avg_px": -0.3,
        "avg_pz": 2.37,
        "avg_speed": 86.037,
        "called_strike_rate": 0.333,
        "contact_rate": 0.333,
        "csw_rate": 0.667,
        "custom_score": 0.0,
        "fly_ball_rate": 0.0,
        "ground_ball_rate": 0.0,
        "line_drive_rate": 0.0,
        "money_pitch": False,
        "o_contact_rate": 0.0,
        "o_swing_rate": 0.0,
        "percent": 0.176,
        "pitch_type": "CHANGEUP",
        "pop_up_rate": 0.0,
        "swing_rate": 0.667,
        "swinging_strike_rate": 0.333,
        "total_batted_balls": 0,
        "total_called_strikes": 1,
        "total_contact_inside_zone": 1,
        "total_contact_outside_zone": 0,
        "total_fly_balls": 0,
        "total_ground_balls": 0,
        "total_inside_strike_zone": 3,
        "total_line_drives": 0,
        "total_outside_strike_zone": 0,
        "total_pitches": 3,
        "total_pop_ups": 0,
        "total_swinging_strikes": 1,
        "total_swings": 2,
        "total_swings_inside_zone": 2,
        "total_swings_made_contact": 1,
        "total_swings_outside_zone": 0,
        "whiff_rate": 0.5,
        "z_contact_rate": 0.5,
        "z_swing_rate": 0.667,
        "zone_rate": 1.0,
    }
    assert pitch_mix_detail_all["SLIDER"] == {
        "avg_pfx_x": 1.83,
        "avg_pfx_z": -1.265,
        "avg_px": 0.845,
        "avg_pz": 0.54,
        "avg_speed": 84.225,
        "called_strike_rate": 0.0,
        "contact_rate": 0.0,
        "csw_rate": 0.5,
        "custom_score": 1.0,
        "fly_ball_rate": 0.0,
        "ground_ball_rate": 0.0,
        "line_drive_rate": 0.0,
        "money_pitch": False,
        "o_contact_rate": 0.0,
        "o_swing_rate": 0.5,
        "percent": 0.118,
        "pitch_type": "SLIDER",
        "pop_up_rate": 0.0,
        "swing_rate": 0.5,
        "swinging_strike_rate": 0.5,
        "total_batted_balls": 0,
        "total_called_strikes": 0,
        "total_contact_inside_zone": 0,
        "total_contact_outside_zone": 0,
        "total_fly_balls": 0,
        "total_ground_balls": 0,
        "total_inside_strike_zone": 0,
        "total_line_drives": 0,
        "total_outside_strike_zone": 2,
        "total_pitches": 2,
        "total_pop_ups": 0,
        "total_swinging_strikes": 1,
        "total_swings": 1,
        "total_swings_inside_zone": 0,
        "total_swings_made_contact": 0,
        "total_swings_outside_zone": 1,
        "whiff_rate": 1.0,
        "z_contact_rate": 0.0,
        "z_swing_rate": 0.0,
        "zone_rate": 0.0,
    }

    assert "bat_r" in pitch_mix
    (pitch_mix_total_bat_r, pitch_mix_detail_bat_r) = pitch_mix["bat_r"]
    assert pitch_mix_total_bat_r == {
        "called_strike_rate": 0.2,
        "contact_rate": 0.4,
        "csw_rate": 0.2,
        "fly_ball_rate": 0.0,
        "ground_ball_rate": 0.0,
        "line_drive_rate": 1.0,
        "money_pitch": 0,
        "o_contact_rate": 0.0,
        "o_swing_rate": 0.0,
        "percent": 1.0,
        "pitch_types": ["FOUR_SEAM_FASTBALL", "CURVEBALL"],
        "pop_up_rate": 0.0,
        "swing_rate": 0.4,
        "swinging_strike_rate": 0.0,
        "total_batted_balls": 1,
        "total_called_strikes": 1,
        "total_contact_inside_zone": 2,
        "total_contact_outside_zone": 0,
        "total_fly_balls": 0,
        "total_ground_balls": 0,
        "total_inside_strike_zone": 3,
        "total_line_drives": 1,
        "total_outside_strike_zone": 2,
        "total_pitches": 5,
        "total_pop_ups": 0,
        "total_swinging_strikes": 0,
        "total_swings": 2,
        "total_swings_inside_zone": 2,
        "total_swings_made_contact": 2,
        "total_swings_outside_zone": 0,
        "whiff_rate": 0.0,
        "z_contact_rate": 1.0,
        "z_swing_rate": 0.667,
        "zone_rate": 0.6,
    }

    assert pitch_mix_detail_bat_r["FOUR_SEAM_FASTBALL"] == {
        "avg_pfx_x": -1.923,
        "avg_pfx_z": 9.193,
        "avg_px": -0.047,
        "avg_pz": 3.453,
        "avg_speed": 94.85,
        "called_strike_rate": 0.0,
        "contact_rate": 0.667,
        "csw_rate": 0.0,
        "custom_score": 0.667,
        "fly_ball_rate": 0.0,
        "ground_ball_rate": 0.0,
        "line_drive_rate": 1.0,
        "o_contact_rate": 0.0,
        "o_swing_rate": 0.0,
        "percent": 0.6,
        "pitch_type": "FOUR_SEAM_FASTBALL",
        "pop_up_rate": 0.0,
        "swing_rate": 0.667,
        "swinging_strike_rate": 0.0,
        "total_batted_balls": 1,
        "total_called_strikes": 0,
        "total_contact_inside_zone": 2,
        "total_contact_outside_zone": 0,
        "total_fly_balls": 0,
        "total_ground_balls": 0,
        "total_inside_strike_zone": 2,
        "total_line_drives": 1,
        "total_outside_strike_zone": 1,
        "total_pitches": 3,
        "total_pop_ups": 0,
        "total_swinging_strikes": 0,
        "total_swings": 2,
        "total_swings_inside_zone": 2,
        "total_swings_made_contact": 2,
        "total_swings_outside_zone": 0,
        "whiff_rate": 0.0,
        "z_contact_rate": 1.0,
        "z_swing_rate": 1.0,
        "zone_rate": 0.667,
    }
    assert pitch_mix_detail_bat_r["CURVEBALL"] == {
        "avg_pfx_x": 2.215,
        "avg_pfx_z": -3.9,
        "avg_px": 0.53,
        "avg_pz": 2.09,
        "avg_speed": 78.88,
        "called_strike_rate": 0.5,
        "contact_rate": 0.0,
        "csw_rate": 0.5,
        "custom_score": 0.5,
        "fly_ball_rate": 0.0,
        "ground_ball_rate": 0.0,
        "line_drive_rate": 0.0,
        "o_contact_rate": 0.0,
        "o_swing_rate": 0.0,
        "percent": 0.4,
        "pitch_type": "CURVEBALL",
        "pop_up_rate": 0.0,
        "swing_rate": 0.0,
        "swinging_strike_rate": 0.0,
        "total_batted_balls": 0,
        "total_called_strikes": 1,
        "total_contact_inside_zone": 0,
        "total_contact_outside_zone": 0,
        "total_fly_balls": 0,
        "total_ground_balls": 0,
        "total_inside_strike_zone": 1,
        "total_line_drives": 0,
        "total_outside_strike_zone": 1,
        "total_pitches": 2,
        "total_pop_ups": 0,
        "total_swinging_strikes": 0,
        "total_swings": 0,
        "total_swings_inside_zone": 0,
        "total_swings_made_contact": 0,
        "total_swings_outside_zone": 0,
        "whiff_rate": 0.0,
        "z_contact_rate": 0.0,
        "z_swing_rate": 0.0,
        "zone_rate": 0.5,
    }

    assert "bat_l" in pitch_mix
    (pitch_mix_total_bat_l, pitch_mix_detail_bat_l) = pitch_mix["bat_l"]
    assert pitch_mix_total_bat_l == {
        "called_strike_rate": 0.333,
        "contact_rate": 0.167,
        "csw_rate": 0.5,
        "fly_ball_rate": 0.0,
        "ground_ball_rate": 0.0,
        "line_drive_rate": 0.0,
        "money_pitch": 0,
        "o_contact_rate": 0.5,
        "o_swing_rate": 0.333,
        "percent": 1.0,
        "pitch_types": ["CURVEBALL", "CHANGEUP", "FOUR_SEAM_FASTBALL", "SLIDER"],
        "pop_up_rate": 0.0,
        "swing_rate": 0.333,
        "swinging_strike_rate": 0.167,
        "total_batted_balls": 0,
        "total_called_strikes": 4,
        "total_contact_inside_zone": 1,
        "total_contact_outside_zone": 1,
        "total_fly_balls": 0,
        "total_ground_balls": 0,
        "total_inside_strike_zone": 6,
        "total_line_drives": 0,
        "total_outside_strike_zone": 6,
        "total_pitches": 12,
        "total_pop_ups": 0,
        "total_swinging_strikes": 2,
        "total_swings": 4,
        "total_swings_inside_zone": 2,
        "total_swings_made_contact": 2,
        "total_swings_outside_zone": 2,
        "whiff_rate": 0.5,
        "z_contact_rate": 0.5,
        "z_swing_rate": 0.333,
        "zone_rate": 0.5,
    }
    assert pitch_mix_detail_bat_l["CURVEBALL"] == {
        "avg_pfx_x": 3.26,
        "avg_pfx_z": -4.91,
        "avg_px": -0.255,
        "avg_pz": 1.633,
        "avg_speed": 78.597,
        "called_strike_rate": 0.5,
        "contact_rate": 0.25,
        "csw_rate": 0.5,
        "custom_score": 1.0,
        "fly_ball_rate": 0.0,
        "ground_ball_rate": 0.0,
        "line_drive_rate": 0.0,
        "o_contact_rate": 1.0,
        "o_swing_rate": 0.5,
        "percent": 0.333,
        "pitch_type": "CURVEBALL",
        "pop_up_rate": 0.0,
        "swing_rate": 0.25,
        "swinging_strike_rate": 0.0,
        "total_batted_balls": 0,
        "total_called_strikes": 2,
        "total_contact_inside_zone": 0,
        "total_contact_outside_zone": 1,
        "total_fly_balls": 0,
        "total_ground_balls": 0,
        "total_inside_strike_zone": 2,
        "total_line_drives": 0,
        "total_outside_strike_zone": 2,
        "total_pitches": 4,
        "total_pop_ups": 0,
        "total_swinging_strikes": 0,
        "total_swings": 1,
        "total_swings_inside_zone": 0,
        "total_swings_made_contact": 1,
        "total_swings_outside_zone": 1,
        "whiff_rate": 0.0,
        "z_contact_rate": 0.0,
        "z_swing_rate": 0.0,
        "zone_rate": 0.5,
    }
    assert pitch_mix_detail_bat_l["CHANGEUP"] == {
        "avg_pfx_x": -6.37,
        "avg_pfx_z": 7.137,
        "avg_px": -0.3,
        "avg_pz": 2.37,
        "avg_speed": 86.037,
        "called_strike_rate": 0.333,
        "contact_rate": 0.333,
        "csw_rate": 0.667,
        "custom_score": 0.0,
        "fly_ball_rate": 0.0,
        "ground_ball_rate": 0.0,
        "line_drive_rate": 0.0,
        "o_contact_rate": 0.0,
        "o_swing_rate": 0.0,
        "percent": 0.25,
        "pitch_type": "CHANGEUP",
        "pop_up_rate": 0.0,
        "swing_rate": 0.667,
        "swinging_strike_rate": 0.333,
        "total_batted_balls": 0,
        "total_called_strikes": 1,
        "total_contact_inside_zone": 1,
        "total_contact_outside_zone": 0,
        "total_fly_balls": 0,
        "total_ground_balls": 0,
        "total_inside_strike_zone": 3,
        "total_line_drives": 0,
        "total_outside_strike_zone": 0,
        "total_pitches": 3,
        "total_pop_ups": 0,
        "total_swinging_strikes": 1,
        "total_swings": 2,
        "total_swings_inside_zone": 2,
        "total_swings_made_contact": 1,
        "total_swings_outside_zone": 0,
        "whiff_rate": 0.5,
        "z_contact_rate": 0.5,
        "z_swing_rate": 0.667,
        "zone_rate": 1.0,
    }
    assert pitch_mix_detail_bat_l["FOUR_SEAM_FASTBALL"] == {
        "avg_pfx_x": -0.593,
        "avg_pfx_z": 8.187,
        "avg_px": -0.053,
        "avg_pz": 3.16,
        "avg_speed": 93.67,
        "called_strike_rate": 0.333,
        "contact_rate": 0.0,
        "csw_rate": 0.333,
        "custom_score": 0.333,
        "fly_ball_rate": 0.0,
        "ground_ball_rate": 0.0,
        "line_drive_rate": 0.0,
        "o_contact_rate": 0.0,
        "o_swing_rate": 0.0,
        "percent": 0.25,
        "pitch_type": "FOUR_SEAM_FASTBALL",
        "pop_up_rate": 0.0,
        "swing_rate": 0.0,
        "swinging_strike_rate": 0.0,
        "total_batted_balls": 0,
        "total_called_strikes": 1,
        "total_contact_inside_zone": 0,
        "total_contact_outside_zone": 0,
        "total_fly_balls": 0,
        "total_ground_balls": 0,
        "total_inside_strike_zone": 1,
        "total_line_drives": 0,
        "total_outside_strike_zone": 2,
        "total_pitches": 3,
        "total_pop_ups": 0,
        "total_swinging_strikes": 0,
        "total_swings": 0,
        "total_swings_inside_zone": 0,
        "total_swings_made_contact": 0,
        "total_swings_outside_zone": 0,
        "whiff_rate": 0.0,
        "z_contact_rate": 0.0,
        "z_swing_rate": 0.0,
        "zone_rate": 0.333,
    }
    assert pitch_mix_detail_bat_l["SLIDER"] == {
        "avg_pfx_x": 1.83,
        "avg_pfx_z": -1.265,
        "avg_px": 0.845,
        "avg_pz": 0.54,
        "avg_speed": 84.225,
        "called_strike_rate": 0.0,
        "contact_rate": 0.0,
        "csw_rate": 0.5,
        "custom_score": 1.0,
        "fly_ball_rate": 0.0,
        "ground_ball_rate": 0.0,
        "line_drive_rate": 0.0,
        "o_contact_rate": 0.0,
        "o_swing_rate": 0.5,
        "percent": 0.167,
        "pitch_type": "SLIDER",
        "pop_up_rate": 0.0,
        "swing_rate": 0.5,
        "swinging_strike_rate": 0.5,
        "total_batted_balls": 0,
        "total_called_strikes": 0,
        "total_contact_inside_zone": 0,
        "total_contact_outside_zone": 0,
        "total_fly_balls": 0,
        "total_ground_balls": 0,
        "total_inside_strike_zone": 0,
        "total_line_drives": 0,
        "total_outside_strike_zone": 2,
        "total_pitches": 2,
        "total_pop_ups": 0,
        "total_swinging_strikes": 1,
        "total_swings": 1,
        "total_swings_inside_zone": 0,
        "total_swings_made_contact": 0,
        "total_swings_outside_zone": 1,
        "whiff_rate": 1.0,
        "z_contact_rate": 0.0,
        "z_swing_rate": 0.0,
        "zone_rate": 0.0,
    }

    matchup = all_game_data.get_matchup_details()
    assert matchup == (
        "Monday, June 17 2019 07:07 PM (UTC-04:00)\n"
        "Los Angeles Angels of Anaheim (36-37) vs Toronto Blue Jays (26-46)\n"
        "WP: Felix Pena (LAA) LP: Edwin Jackson (TOR)\n"
    )

    linescore = all_game_data.get_linescore()
    assert "LAA   0   7   1   0   1   1   0   0   0  10  13   1" in linescore
    assert "TOR   1   0   0   0   0   0   2   2   0   5   8   0" in linescore
