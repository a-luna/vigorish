from vigorish.data.team_data import TeamData
from vigorish.enums import DefensePosition


def test_team_data(vig_app):
    away_pitch_stats_dict = {
        "bases_on_balls": 3,
        "batters_faced": 37,
        "bb_per_nine": 3.0,
        "bb_rate": 0.1,
        "bbref_id": None,
        "earned_runs": 5,
        "era": 5.0,
        "fly_balls": 13,
        "games_as_rp": 4,
        "games_as_sp": 1,
        "ground_balls": 13,
        "hits": 8,
        "homeruns": 3,
        "hr_per_fb": 0.2,
        "hr_per_nine": 3.0,
        "inherited_runners": 2,
        "inherited_scored": 0,
        "innings_pitched": 9.0,
        "k_minus_bb": 0.1,
        "k_per_bb": 72.0,
        "k_per_nine": 8.0,
        "k_rate": 0.2,
        "line_drives": 1,
        "losses": 0,
        "mlb_id": None,
        "opponent_team_id_bbref": None,
        "pitch_count": 129,
        "player_team_id_bbref": "LAA",
        "re24_pitch": -0.2,
        "role": None,
        "runs": 5,
        "saves": 0,
        "stint_number": None,
        "strikeouts": 8,
        "strikes": 81,
        "strikes_contact": 40,
        "strikes_looking": 23,
        "strikes_swinging": 18,
        "team_id_bbref": "LAA",
        "total_games": 5,
        "total_outs": 27,
        "unknown_type": 0,
        "whip": 1.22,
        "wins": 1,
        "wpa_pitch": 0.02,
        "year": 2019,
    }

    away_bat_stats_dict = {
        "all_player_stats_for_bat_order": False,
        "all_player_stats_for_def_pos": False,
        "all_stats_for_season": False,
        "all_stats_for_stint": False,
        "all_team_stats_for_bat_order": False,
        "all_team_stats_for_def_pos": False,
        "at_bats": 38,
        "avg": 0.342,
        "bases_on_balls": 6,
        "bat_order_list": [],
        "bb_rate": 0.1,
        "bbref_id": None,
        "career_stats_all_teams": False,
        "career_stats_for_team": False,
        "caught_stealing": 1,
        "changed_teams_midseason": False,
        "contact_rate": 0.8,
        "def_position_list": [],
        "doubles": 4,
        "gdp": 0,
        "hit_by_pitch": 0,
        "hits": 13,
        "homeruns": 4,
        "intentional_bb": 0,
        "is_starter": False,
        "iso": 0.421,
        "k_rate": 0.1,
        "mlb_id": None,
        "obp": 0.432,
        "opponent_team_id_bbref": None,
        "ops": 1.195,
        "plate_appearances": 45,
        "player_team_id_bbref": "LAA",
        "rbis": 10,
        "re24_bat": 5.2,
        "runs_scored": 10,
        "sac_fly": 1,
        "sac_hit": 0,
        "separate_player_stats_for_bat_order": False,
        "separate_player_stats_for_def_pos": False,
        "slg": 0.763,
        "stint_number": None,
        "stolen_bases": 0,
        "strikeouts": 6,
        "team_id_bbref": "LAA",
        "total_games": 9,
        "total_pitches": 169,
        "total_strikes": 102,
        "triples": 0,
        "wpa_bat": 0.48,
        "wpa_bat_neg": -0.13,
        "wpa_bat_pos": 0.61,
        "year": 2019,
        "bat_order": "",
        "def_position": "",
    }

    home_pitch_stats_dict = {
        "bases_on_balls": 6,
        "batters_faced": 45,
        "bb_per_nine": 6.0,
        "bb_rate": 0.1,
        "bbref_id": None,
        "earned_runs": 10,
        "era": 10.0,
        "fly_balls": 22,
        "games_as_rp": 6,
        "games_as_sp": 1,
        "ground_balls": 11,
        "hits": 13,
        "homeruns": 4,
        "hr_per_fb": 0.2,
        "hr_per_nine": 4.0,
        "inherited_runners": 1,
        "inherited_scored": 0,
        "innings_pitched": 9.0,
        "k_minus_bb": 0.0,
        "k_per_bb": 27.0,
        "k_per_nine": 6.0,
        "k_rate": 0.1,
        "line_drives": 9,
        "losses": 1,
        "mlb_id": None,
        "opponent_team_id_bbref": None,
        "pitch_count": 169,
        "player_team_id_bbref": "TOR",
        "re24_pitch": -5.3,
        "role": None,
        "runs": 10,
        "saves": 0,
        "stint_number": None,
        "strikeouts": 6,
        "strikes": 102,
        "strikes_contact": 60,
        "strikes_looking": 32,
        "strikes_swinging": 10,
        "team_id_bbref": "TOR",
        "total_games": 7,
        "total_outs": 27,
        "unknown_type": 0,
        "whip": 2.11,
        "wins": 0,
        "wpa_pitch": -0.48,
        "year": 2019,
    }

    home_bat_stats_dict = {
        "all_player_stats_for_bat_order": False,
        "all_player_stats_for_def_pos": False,
        "all_stats_for_season": False,
        "all_stats_for_stint": False,
        "all_team_stats_for_bat_order": False,
        "all_team_stats_for_def_pos": False,
        "at_bats": 34,
        "avg": 0.235,
        "bases_on_balls": 3,
        "bat_order_list": [],
        "bb_rate": 0.1,
        "bbref_id": None,
        "career_stats_all_teams": False,
        "career_stats_for_team": False,
        "caught_stealing": 0,
        "changed_teams_midseason": False,
        "contact_rate": 0.8,
        "def_position_list": [],
        "doubles": 0,
        "gdp": 2,
        "hit_by_pitch": 0,
        "hits": 8,
        "homeruns": 3,
        "intentional_bb": 0,
        "is_starter": False,
        "iso": 0.265,
        "k_rate": 0.2,
        "mlb_id": None,
        "obp": 0.297,
        "opponent_team_id_bbref": None,
        "ops": 0.797,
        "plate_appearances": 37,
        "player_team_id_bbref": "TOR",
        "rbis": 5,
        "re24_bat": 0.3,
        "runs_scored": 5,
        "sac_fly": 0,
        "sac_hit": 0,
        "separate_player_stats_for_bat_order": False,
        "separate_player_stats_for_def_pos": False,
        "slg": 0.5,
        "stint_number": None,
        "stolen_bases": 0,
        "strikeouts": 8,
        "team_id_bbref": "TOR",
        "total_games": 10,
        "total_pitches": 129,
        "total_strikes": 81,
        "triples": 0,
        "wpa_bat": -0.02,
        "wpa_bat_neg": -0.19,
        "wpa_bat_pos": 0.17,
        "year": 2019,
        "bat_order": "",
        "def_position": "",
    }

    away_team_data = TeamData(vig_app, "LAA", 2019)
    home_team_data = TeamData(vig_app, "TOR", 2019)
    assert away_team_data.pitch_stats.as_dict() == away_pitch_stats_dict
    assert away_team_data.bat_stats.as_dict() == away_bat_stats_dict
    assert home_team_data.pitch_stats.as_dict() == home_pitch_stats_dict
    assert home_team_data.bat_stats.as_dict() == home_bat_stats_dict
    assert away_team_data.pitch_stats_by_year[2019].as_dict() == away_pitch_stats_dict
    assert away_team_data.bat_stats_by_year[2019].as_dict() == away_bat_stats_dict
    assert home_team_data.pitch_stats_by_year[2019].as_dict() == home_pitch_stats_dict
    assert home_team_data.bat_stats_by_year[2019].as_dict() == home_bat_stats_dict


def test_team_data_by_player(vig_app):
    away_pitch_stats_dict = {
        "bases_on_balls": 3,
        "batters_faced": 25,
        "bb_per_nine": 4.5,
        "bb_rate": 0.1,
        "bbref_id": "penafe01",
        "earned_runs": 4,
        "era": 6.0,
        "fly_balls": 9,
        "games_as_rp": 1,
        "games_as_sp": 0,
        "ground_balls": 8,
        "hits": 6,
        "homeruns": 2,
        "hr_per_fb": 0.2,
        "hr_per_nine": 3.0,
        "inherited_runners": 0,
        "inherited_scored": 0,
        "innings_pitched": 6.0,
        "k_minus_bb": 0.1,
        "k_per_bb": 45.0,
        "k_per_nine": 7.5,
        "k_rate": 0.2,
        "line_drives": 1,
        "losses": 0,
        "mlb_id": 570240,
        "opponent_team_id_bbref": None,
        "pitch_count": 85,
        "player_team_id_bbref": "LAA",
        "re24_pitch": -0.8,
        "role": None,
        "runs": 4,
        "saves": 0,
        "stint_number": None,
        "strikeouts": 5,
        "strikes": 52,
        "strikes_contact": 24,
        "strikes_looking": 17,
        "strikes_swinging": 11,
        "team_id_bbref": "LAA",
        "total_games": 1,
        "total_outs": 18,
        "unknown_type": 0,
        "whip": 1.5,
        "wins": 1,
        "wpa_pitch": 0.05,
        "year": 2019,
    }

    away_bat_stats_dict = {
        "all_player_stats_for_bat_order": False,
        "all_player_stats_for_def_pos": False,
        "all_stats_for_season": False,
        "all_stats_for_stint": False,
        "all_team_stats_for_bat_order": False,
        "all_team_stats_for_def_pos": False,
        "at_bats": 5,
        "avg": 0.8,
        "bases_on_balls": 0,
        "bat_order_list": [],
        "bb_rate": 0.0,
        "bbref_id": "troutmi01",
        "career_stats_all_teams": False,
        "career_stats_for_team": False,
        "caught_stealing": 1,
        "changed_teams_midseason": False,
        "contact_rate": 1.0,
        "def_position_list": [],
        "doubles": 1,
        "gdp": 0,
        "hit_by_pitch": 0,
        "hits": 4,
        "homeruns": 1,
        "intentional_bb": 0,
        "is_starter": False,
        "iso": 0.8,
        "k_rate": 0.0,
        "mlb_id": 545361,
        "obp": 0.8,
        "opponent_team_id_bbref": None,
        "ops": 2.4,
        "plate_appearances": 5,
        "player_team_id_bbref": "LAA",
        "rbis": 3,
        "re24_bat": 2.9,
        "runs_scored": 2,
        "sac_fly": 0,
        "sac_hit": 0,
        "separate_player_stats_for_bat_order": False,
        "separate_player_stats_for_def_pos": False,
        "slg": 1.6,
        "stint_number": None,
        "stolen_bases": 0,
        "strikeouts": 0,
        "team_id_bbref": "LAA",
        "total_games": 1,
        "total_pitches": 19,
        "total_strikes": 11,
        "triples": 0,
        "wpa_bat": 0.14,
        "wpa_bat_neg": -0.02,
        "wpa_bat_pos": 0.17,
        "year": 2019,
        "bat_order": "",
        "def_position": "",
    }

    home_pitch_stats_dict = {
        "bases_on_balls": 0,
        "batters_faced": 13,
        "bb_per_nine": 0.0,
        "bb_rate": 0.0,
        "bbref_id": "gavigsa01",
        "earned_runs": 2,
        "era": 5.4,
        "fly_balls": 6,
        "games_as_rp": 1,
        "games_as_sp": 0,
        "ground_balls": 5,
        "hits": 4,
        "homeruns": 0,
        "hr_per_fb": 0.0,
        "hr_per_nine": 0.0,
        "inherited_runners": 1,
        "inherited_scored": 0,
        "innings_pitched": 3.1,
        "k_minus_bb": 0.2,
        "k_per_bb": 0.0,
        "k_per_nine": 5.4,
        "k_rate": 0.2,
        "line_drives": 2,
        "losses": 0,
        "mlb_id": 543208,
        "opponent_team_id_bbref": None,
        "pitch_count": 39,
        "player_team_id_bbref": "TOR",
        "re24_pitch": -0.1,
        "role": None,
        "runs": 2,
        "saves": 0,
        "stint_number": None,
        "strikeouts": 2,
        "strikes": 27,
        "strikes_contact": 14,
        "strikes_looking": 12,
        "strikes_swinging": 1,
        "team_id_bbref": "TOR",
        "total_games": 1,
        "total_outs": 10,
        "unknown_type": 0,
        "whip": 1.2,
        "wins": 0,
        "wpa_pitch": -0.01,
        "year": 2019,
    }

    home_bat_stats_dict = {
        "all_player_stats_for_bat_order": False,
        "all_player_stats_for_def_pos": False,
        "all_stats_for_season": False,
        "all_stats_for_stint": False,
        "all_team_stats_for_bat_order": False,
        "all_team_stats_for_def_pos": False,
        "at_bats": 2,
        "avg": 1.0,
        "bases_on_balls": 2,
        "bat_order_list": [],
        "bb_rate": 0.5,
        "bbref_id": "biggica01",
        "career_stats_all_teams": False,
        "career_stats_for_team": False,
        "caught_stealing": 0,
        "changed_teams_midseason": False,
        "contact_rate": 1.0,
        "def_position_list": [],
        "doubles": 0,
        "gdp": 0,
        "hit_by_pitch": 0,
        "hits": 2,
        "homeruns": 2,
        "intentional_bb": 0,
        "is_starter": False,
        "iso": 3.0,
        "k_rate": 0.0,
        "mlb_id": 624415,
        "obp": 1.0,
        "opponent_team_id_bbref": None,
        "ops": 5.0,
        "plate_appearances": 4,
        "player_team_id_bbref": "TOR",
        "rbis": 3,
        "re24_bat": 3.6,
        "runs_scored": 2,
        "sac_fly": 0,
        "sac_hit": 0,
        "separate_player_stats_for_bat_order": False,
        "separate_player_stats_for_def_pos": False,
        "slg": 4.0,
        "stint_number": None,
        "stolen_bases": 0,
        "strikeouts": 0,
        "team_id_bbref": "TOR",
        "total_games": 1,
        "total_pitches": 22,
        "total_strikes": 8,
        "triples": 0,
        "wpa_bat": 0.13,
        "wpa_bat_neg": 0.0,
        "wpa_bat_pos": 0.13,
        "year": 2019,
        "bat_order": "",
        "def_position": "",
    }

    away_team_data = TeamData(vig_app, "LAA", 2019)
    home_team_data = TeamData(vig_app, "TOR", 2019)

    away_team_player_pitch_stats = away_team_data.pitch_stats_by_player
    away_team_player_pitch_stats.sort(key=lambda x: x.innings_pitched, reverse=True)
    assert len(away_team_player_pitch_stats) == 5
    assert away_team_player_pitch_stats[0].as_dict() == away_pitch_stats_dict

    away_team_player_bat_stats = away_team_data.bat_stats_by_player
    away_team_player_bat_stats.sort(key=lambda x: x.re24_bat, reverse=True)
    assert len(away_team_player_bat_stats) == 9
    assert away_team_player_bat_stats[0].as_dict() == away_bat_stats_dict

    home_team_player_pitch_stats = home_team_data.pitch_stats_by_player
    home_team_player_pitch_stats.sort(key=lambda x: x.innings_pitched, reverse=True)
    assert len(home_team_player_pitch_stats) == 7
    assert home_team_player_pitch_stats[0].as_dict() == home_pitch_stats_dict

    home_team_player_bat_stats = home_team_data.bat_stats_by_player
    home_team_player_bat_stats.sort(key=lambda x: x.re24_bat, reverse=True)
    assert len(home_team_player_bat_stats) == 10
    assert home_team_player_bat_stats[0].as_dict() == home_bat_stats_dict


def test_team_temp(vig_app):
    away_team_data = TeamData(vig_app, "LAA", 2019)
    home_team_data = TeamData(vig_app, "TOR", 2019)

    temp = vig_app.scraped_data.get_bat_stats_for_season_for_all_teams(2019)

    temp = away_team_data.pitch_stats_for_sp
    temp = away_team_data.pitch_stats_for_sp_by_year
    temp = away_team_data.pitch_stats_for_sp_by_player
    temp = vig_app.scraped_data.get_pitch_stats_for_sp_for_season_for_all_teams(2019)

    temp = away_team_data.pitch_stats_for_rp
    temp = away_team_data.pitch_stats_for_rp_by_year
    temp = away_team_data.pitch_stats_for_rp_by_player
    temp = vig_app.scraped_data.get_pitch_stats_for_rp_for_season_for_all_teams(2019)

    temp = away_team_data.bat_stats_by_lineup_spot
    temp = away_team_data.get_bat_stats_for_lineup_spot_by_year([2])
    temp = away_team_data.get_bat_stats_for_lineup_spot_by_player([2])
    temp = vig_app.scraped_data.get_bat_stats_for_lineup_spot_for_season_for_all_teams([1], 2019)

    temp = away_team_data.bat_stats_by_defpos
    temp = away_team_data.get_bat_stats_for_defpos_by_year([DefensePosition.CATCHER])
    temp = away_team_data.get_bat_stats_for_defpos_by_player([DefensePosition.CATCHER])
    temp = vig_app.scraped_data.get_bat_stats_for_defpos_for_season_for_all_teams([DefensePosition.CATCHER], 2019)

    temp = away_team_data.bat_stats_for_starters
    temp = away_team_data.bat_stats_for_starters_by_year
    temp = away_team_data.bat_stats_for_starters_by_player
    temp = vig_app.scraped_data.get_bat_stats_for_starters_for_season_for_all_teams(2019)

    temp = home_team_data.bat_stats_for_subs
    temp = home_team_data.bat_stats_for_subs_by_year
    temp = home_team_data.bat_stats_for_subs_by_player
    temp = vig_app.scraped_data.get_bat_stats_for_subs_for_season_for_all_teams(2019)
    assert temp
