from dacite.core import from_dict

from vigorish.data.metrics import BatStatsMetrics, PitchStatsMetrics
from vigorish.data.team_data import TeamData
from vigorish.enums import DefensePosition


def test_team_data(vig_app):
    away_pitch_stats_dict = {
        "mlb_id": 0,
        "year": 2019,
        "team_id_bbref": "LAA",
        "opponent_team_id_bbref": "",
        "stint_number": 0,
        "total_games": 1,
        "games_as_sp": 1,
        "games_as_rp": 4,
        "wins": 1,
        "losses": 0,
        "saves": 0,
        "innings_pitched": 9.0,
        "total_outs": 27,
        "batters_faced": 37,
        "runs": 5,
        "earned_runs": 5,
        "hits": 8,
        "homeruns": 3,
        "strikeouts": 8,
        "bases_on_balls": 3,
        "era": 5.0,
        "whip": 1.222,
        "k_per_nine": 8.0,
        "bb_per_nine": 3.0,
        "hr_per_nine": 3.0,
        "k_per_bb": 2.667,
        "k_rate": 0.216,
        "bb_rate": 0.081,
        "k_minus_bb": 0.135,
        "hr_per_fb": 0.231,
        "pitch_count": 129,
        "strikes": 81,
        "strikes_contact": 40,
        "strikes_swinging": 18,
        "strikes_looking": 23,
        "ground_balls": 13,
        "fly_balls": 13,
        "line_drives": 1,
        "unknown_type": 0,
        "inherited_runners": 2,
        "inherited_scored": 0,
        "wpa_pitch": 0.021,
        "re24_pitch": -0.2,
    }

    away_bat_stats_dict = {
        "mlb_id": 0,
        "year": 2019,
        "team_id_bbref": "LAA",
        "opponent_team_id_bbref": "",
        "stint_number": 0,
        "total_games": 1,
        "avg": 0.342,
        "obp": 0.422,
        "slg": 0.763,
        "ops": 1.185,
        "iso": 0.421,
        "bb_rate": 0.133,
        "k_rate": 0.133,
        "contact_rate": 0.842,
        "plate_appearances": 45,
        "at_bats": 38,
        "hits": 13,
        "runs_scored": 10,
        "rbis": 10,
        "bases_on_balls": 6,
        "strikeouts": 6,
        "doubles": 4,
        "triples": 0,
        "homeruns": 4,
        "stolen_bases": 0,
        "caught_stealing": 1,
        "hit_by_pitch": 0,
        "intentional_bb": 0,
        "gdp": 0,
        "sac_fly": 1,
        "sac_hit": 0,
        "total_pitches": 169,
        "total_strikes": 102,
        "wpa_bat": 0.479,
        "wpa_bat_pos": 0.606,
        "wpa_bat_neg": -0.128,
        "re24_bat": 5.2,
        "bbref_id": "",
        "bat_order": 0,
        "def_position": DefensePosition.NONE,
        "is_starter": False,
    }

    home_pitch_stats_dict = {
        "mlb_id": 0,
        "year": 2019,
        "team_id_bbref": "TOR",
        "opponent_team_id_bbref": "",
        "stint_number": 0,
        "total_games": 1,
        "games_as_sp": 1,
        "games_as_rp": 6,
        "wins": 0,
        "losses": 1,
        "saves": 0,
        "innings_pitched": 9.0,
        "total_outs": 27,
        "batters_faced": 45,
        "runs": 10,
        "earned_runs": 10,
        "hits": 13,
        "homeruns": 4,
        "strikeouts": 6,
        "bases_on_balls": 6,
        "era": 10.0,
        "whip": 2.111,
        "k_per_nine": 6.0,
        "bb_per_nine": 6.0,
        "hr_per_nine": 4.0,
        "k_per_bb": 1.0,
        "k_rate": 0.133,
        "bb_rate": 0.133,
        "k_minus_bb": 0.0,
        "hr_per_fb": 0.182,
        "pitch_count": 169,
        "strikes": 102,
        "strikes_contact": 60,
        "strikes_swinging": 10,
        "strikes_looking": 32,
        "ground_balls": 11,
        "fly_balls": 22,
        "line_drives": 9,
        "unknown_type": 0,
        "inherited_runners": 1,
        "inherited_scored": 0,
        "wpa_pitch": -0.478,
        "re24_pitch": -5.3,
    }

    home_bat_stats_dict = {
        "mlb_id": 0,
        "year": 2019,
        "team_id_bbref": "TOR",
        "opponent_team_id_bbref": "",
        "stint_number": 0,
        "total_games": 1,
        "avg": 0.235,
        "obp": 0.297,
        "slg": 0.5,
        "ops": 0.797,
        "iso": 0.265,
        "bb_rate": 0.081,
        "k_rate": 0.216,
        "contact_rate": 0.765,
        "plate_appearances": 37,
        "at_bats": 34,
        "hits": 8,
        "runs_scored": 5,
        "rbis": 5,
        "bases_on_balls": 3,
        "strikeouts": 8,
        "doubles": 0,
        "triples": 0,
        "homeruns": 3,
        "stolen_bases": 0,
        "caught_stealing": 0,
        "hit_by_pitch": 0,
        "intentional_bb": 0,
        "gdp": 2,
        "sac_fly": 0,
        "sac_hit": 0,
        "total_pitches": 129,
        "total_strikes": 81,
        "wpa_bat": -0.022,
        "wpa_bat_pos": 0.17,
        "wpa_bat_neg": -0.192,
        "re24_bat": 0.3,
        "bbref_id": "",
        "bat_order": 0,
        "def_position": DefensePosition.NONE,
        "is_starter": False,
    }

    away_team_data = TeamData(vig_app, "LAA", 2019)
    home_team_data = TeamData(vig_app, "TOR", 2019)
    assert away_team_data.pitch_stats == from_dict(data_class=PitchStatsMetrics, data=away_pitch_stats_dict)
    assert away_team_data.bat_stats == from_dict(data_class=BatStatsMetrics, data=away_bat_stats_dict)
    assert home_team_data.pitch_stats == from_dict(data_class=PitchStatsMetrics, data=home_pitch_stats_dict)
    assert home_team_data.bat_stats == from_dict(data_class=BatStatsMetrics, data=home_bat_stats_dict)
    assert away_team_data.pitch_stats_by_year == {
        2019: from_dict(data_class=PitchStatsMetrics, data=away_pitch_stats_dict)
    }
    assert away_team_data.bat_stats_by_year == {2019: from_dict(data_class=BatStatsMetrics, data=away_bat_stats_dict)}
    assert home_team_data.pitch_stats_by_year == {
        2019: from_dict(data_class=PitchStatsMetrics, data=home_pitch_stats_dict)
    }
    assert home_team_data.bat_stats_by_year == {2019: from_dict(data_class=BatStatsMetrics, data=home_bat_stats_dict)}


def test_team_data_by_player(vig_app):

    away_pitch_stats_dict = {
        "bases_on_balls": 0,
        "batters_faced": 1,
        "bb_per_nine": 0.0,
        "bb_rate": 0.0,
        "bbref_id": "buttrty01",
        "earned_runs": 0,
        "era": 0.0,
        "fly_balls": 0,
        "games_as_rp": 1,
        "games_as_sp": 0,
        "ground_balls": 0,
        "hits": 0,
        "homeruns": 0,
        "hr_per_fb": 0.0,
        "hr_per_nine": 0.0,
        "inherited_runners": 2,
        "inherited_scored": 0,
        "innings_pitched": 0.1,
        "k_minus_bb": 1.0,
        "k_per_bb": 0.0,
        "k_per_nine": 27.0,
        "k_rate": 1.0,
        "line_drives": 0,
        "losses": 0,
        "mlb_id": 621142,
        "opponent_team_id_bbref": "",
        "pitch_count": 6,
        "team_id_bbref": "LAA",
        "re24_pitch": 0.5,
        "runs": 0,
        "saves": 0,
        "stint_number": 0,
        "strikeouts": 1,
        "strikes": 5,
        "strikes_contact": 3,
        "strikes_looking": 1,
        "strikes_swinging": 1,
        "total_games": 1,
        "total_outs": 1,
        "unknown_type": 0,
        "whip": 0.0,
        "wins": 0,
        "wpa_pitch": 0.013,
        "year": 2019,
    }

    away_bat_stats_dict = {
        "at_bats": 4,
        "avg": 0.25,
        "bases_on_balls": 0,
        "bat_order": 0,
        "bb_rate": 0.0,
        "bbref_id": "bourju01",
        "caught_stealing": 0,
        "def_position": DefensePosition.NONE,
        "doubles": 1,
        "gdp": 0,
        "hit_by_pitch": 0,
        "hits": 1,
        "homeruns": 0,
        "intentional_bb": 0,
        "iso": 0.25,
        "is_starter": False,
        "k_rate": 0.0,
        "contact_rate": 1.0,
        "mlb_id": 571506,
        "obp": 0.2,
        "opponent_team_id_bbref": "",
        "ops": 0.7,
        "plate_appearances": 5,
        "team_id_bbref": "LAA",
        "rbis": 1,
        "re24_bat": -0.2,
        "runs_scored": 1,
        "sac_fly": 1,
        "sac_hit": 0,
        "slg": 0.5,
        "stint_number": 0,
        "stolen_bases": 0,
        "strikeouts": 0,
        "total_games": 1,
        "total_pitches": 16,
        "total_strikes": 10,
        "triples": 0,
        "wpa_bat": -0.004,
        "wpa_bat_neg": -0.016,
        "wpa_bat_pos": 0.012,
        "year": 2019,
    }

    home_pitch_stats_dict = {
        "bases_on_balls": 0,
        "batters_faced": 3,
        "bb_per_nine": 0.0,
        "bb_rate": 0.0,
        "bbref_id": "lawde01",
        "earned_runs": 0,
        "era": 0.0,
        "fly_balls": 1,
        "games_as_rp": 0,
        "games_as_sp": 1,
        "ground_balls": 0,
        "hits": 0,
        "homeruns": 0,
        "hr_per_fb": 0.0,
        "hr_per_nine": 0.0,
        "inherited_runners": 0,
        "inherited_scored": 0,
        "innings_pitched": 1.0,
        "k_minus_bb": 0.667,
        "k_per_bb": 0.0,
        "k_per_nine": 18.0,
        "k_rate": 0.667,
        "line_drives": 1,
        "losses": 0,
        "mlb_id": 571882,
        "opponent_team_id_bbref": "",
        "pitch_count": 17,
        "team_id_bbref": "TOR",
        "re24_pitch": 0.5,
        "runs": 0,
        "saves": 0,
        "stint_number": 0,
        "strikeouts": 2,
        "strikes": 11,
        "strikes_contact": 4,
        "strikes_looking": 5,
        "strikes_swinging": 2,
        "total_games": 1,
        "total_outs": 3,
        "unknown_type": 0,
        "whip": 0.0,
        "wins": 0,
        "wpa_pitch": 0.049,
        "year": 2019,
    }

    home_bat_stats_dict = {
        "at_bats": 5,
        "avg": 0.4,
        "bases_on_balls": 0,
        "bat_order": 0,
        "bb_rate": 0.0,
        "bbref_id": "sogarer01",
        "caught_stealing": 0,
        "def_position": DefensePosition.NONE,
        "doubles": 0,
        "gdp": 0,
        "hit_by_pitch": 0,
        "hits": 2,
        "homeruns": 0,
        "intentional_bb": 0,
        "is_starter": False,
        "iso": 0.0,
        "k_rate": 0.0,
        "contact_rate": 1.0,
        "mlb_id": 519299,
        "obp": 0.4,
        "opponent_team_id_bbref": "",
        "ops": 0.8,
        "plate_appearances": 5,
        "rbis": 0,
        "re24_bat": 0.2,
        "runs_scored": 1,
        "sac_fly": 0,
        "sac_hit": 0,
        "slg": 0.4,
        "stint_number": 0,
        "stolen_bases": 0,
        "strikeouts": 0,
        "team_id_bbref": "TOR",
        "total_games": 1,
        "total_pitches": 14,
        "total_strikes": 8,
        "triples": 0,
        "wpa_bat": -0.02,
        "wpa_bat_neg": -0.026,
        "wpa_bat_pos": 0.006,
        "year": 2019,
    }

    away_team_data = TeamData(vig_app, "LAA", 2019)
    home_team_data = TeamData(vig_app, "TOR", 2019)

    away_team_player_pitch_stats = away_team_data.pitch_stats_by_player
    assert len(away_team_player_pitch_stats) == 5
    assert away_team_player_pitch_stats[0] == from_dict(data_class=PitchStatsMetrics, data=away_pitch_stats_dict)

    away_team_player_bat_stats = away_team_data.bat_stats_by_player
    assert len(away_team_player_bat_stats) == 9
    assert away_team_player_bat_stats[0] == from_dict(data_class=BatStatsMetrics, data=away_bat_stats_dict)

    home_team_player_pitch_stats = home_team_data.pitch_stats_by_player
    assert len(home_team_player_pitch_stats) == 7
    assert home_team_player_pitch_stats[0] == from_dict(data_class=PitchStatsMetrics, data=home_pitch_stats_dict)

    home_team_player_bat_stats = home_team_data.bat_stats_by_player
    assert len(home_team_player_bat_stats) == 10
    assert home_team_player_bat_stats[0] == from_dict(data_class=BatStatsMetrics, data=home_bat_stats_dict)


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
