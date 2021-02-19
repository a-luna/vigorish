from dacite.core import from_dict

from vigorish.data.player_data import PlayerData
from vigorish.data.metrics import BatStatsMetrics, PitchFxMetricsCollection, PitchStatsMetrics
from vigorish.enums import PitchType


def test_bat_stats(vig_app):
    bat_stats_for_career_dict = {
        "mlb_id": 545361,
        "year": 0,
        "player_team_id_bbref": "",
        "opponent_team_id_bbref": "",
        "stint_number": 0,
        "total_games": 1,
        "avg": 0.8,
        "obp": 0.8,
        "slg": 1.6,
        "ops": 2.4,
        "iso": 0.8,
        "bb_rate": 0.0,
        "k_rate": 0.0,
        "plate_appearances": 5,
        "at_bats": 5,
        "hits": 4,
        "runs_scored": 2,
        "rbis": 3,
        "bases_on_balls": 0,
        "strikeouts": 0,
        "doubles": 1,
        "triples": 0,
        "homeruns": 1,
        "stolen_bases": 0,
        "caught_stealing": 1,
        "hit_by_pitch": 0,
        "intentional_bb": 0,
        "gdp": 0,
        "sac_fly": 0,
        "sac_hit": 0,
        "total_pitches": 19,
        "total_strikes": 11,
        "wpa_bat": 0.145,
        "wpa_bat_pos": 0.166,
        "wpa_bat_neg": -0.021,
        "re24_bat": 2.9,
    }

    bat_stats_by_year_dict = {
        "mlb_id": 545361,
        "year": 2019,
        "player_team_id_bbref": "",
        "opponent_team_id_bbref": "",
        "stint_number": 0,
        "total_games": 1,
        "avg": 0.8,
        "obp": 0.8,
        "slg": 1.6,
        "ops": 2.4,
        "iso": 0.8,
        "bb_rate": 0.0,
        "k_rate": 0.0,
        "plate_appearances": 5,
        "at_bats": 5,
        "hits": 4,
        "runs_scored": 2,
        "rbis": 3,
        "bases_on_balls": 0,
        "strikeouts": 0,
        "doubles": 1,
        "triples": 0,
        "homeruns": 1,
        "stolen_bases": 0,
        "caught_stealing": 1,
        "hit_by_pitch": 0,
        "intentional_bb": 0,
        "gdp": 0,
        "sac_fly": 0,
        "sac_hit": 0,
        "total_pitches": 19,
        "total_strikes": 11,
        "wpa_bat": 0.145,
        "wpa_bat_pos": 0.166,
        "wpa_bat_neg": -0.021,
        "re24_bat": 2.9,
    }

    bat_stats_by_team_dict = {
        "mlb_id": 545361,
        "year": 0,
        "player_team_id_bbref": "LAA",
        "opponent_team_id_bbref": "",
        "stint_number": 0,
        "total_games": 1,
        "avg": 0.8,
        "obp": 0.8,
        "slg": 1.6,
        "ops": 2.4,
        "iso": 0.8,
        "bb_rate": 0.0,
        "k_rate": 0.0,
        "plate_appearances": 5,
        "at_bats": 5,
        "hits": 4,
        "runs_scored": 2,
        "rbis": 3,
        "bases_on_balls": 0,
        "strikeouts": 0,
        "doubles": 1,
        "triples": 0,
        "homeruns": 1,
        "stolen_bases": 0,
        "caught_stealing": 1,
        "hit_by_pitch": 0,
        "intentional_bb": 0,
        "gdp": 0,
        "sac_fly": 0,
        "sac_hit": 0,
        "total_pitches": 19,
        "total_strikes": 11,
        "wpa_bat": 0.145,
        "wpa_bat_pos": 0.166,
        "wpa_bat_neg": -0.021,
        "re24_bat": 2.9,
    }

    bat_stats_by_team_by_year_dict = {
        "mlb_id": 545361,
        "year": 2019,
        "player_team_id_bbref": "LAA",
        "opponent_team_id_bbref": "",
        "stint_number": 1,
        "total_games": 1,
        "avg": 0.8,
        "obp": 0.8,
        "slg": 1.6,
        "ops": 2.4,
        "iso": 0.8,
        "bb_rate": 0.0,
        "k_rate": 0.0,
        "plate_appearances": 5,
        "at_bats": 5,
        "hits": 4,
        "runs_scored": 2,
        "rbis": 3,
        "bases_on_balls": 0,
        "strikeouts": 0,
        "doubles": 1,
        "triples": 0,
        "homeruns": 1,
        "stolen_bases": 0,
        "caught_stealing": 1,
        "hit_by_pitch": 0,
        "intentional_bb": 0,
        "gdp": 0,
        "sac_fly": 0,
        "sac_hit": 0,
        "total_pitches": 19,
        "total_strikes": 11,
        "wpa_bat": 0.145,
        "wpa_bat_pos": 0.166,
        "wpa_bat_neg": -0.021,
        "re24_bat": 2.9,
    }

    bat_stats_by_opp_team_dict = {
        "mlb_id": 545361,
        "year": 0,
        "player_team_id_bbref": "",
        "opponent_team_id_bbref": "TOR",
        "stint_number": 0,
        "total_games": 1,
        "avg": 0.8,
        "obp": 0.8,
        "slg": 1.6,
        "ops": 2.4,
        "iso": 0.8,
        "bb_rate": 0.0,
        "k_rate": 0.0,
        "plate_appearances": 5,
        "at_bats": 5,
        "hits": 4,
        "runs_scored": 2,
        "rbis": 3,
        "bases_on_balls": 0,
        "strikeouts": 0,
        "doubles": 1,
        "triples": 0,
        "homeruns": 1,
        "stolen_bases": 0,
        "caught_stealing": 1,
        "hit_by_pitch": 0,
        "intentional_bb": 0,
        "gdp": 0,
        "sac_fly": 0,
        "sac_hit": 0,
        "total_pitches": 19,
        "total_strikes": 11,
        "wpa_bat": 0.145,
        "wpa_bat_pos": 0.166,
        "wpa_bat_neg": -0.021,
        "re24_bat": 2.9,
    }

    bat_stats_by_opp_team_by_year_dict = {
        "mlb_id": 545361,
        "year": 2019,
        "player_team_id_bbref": "",
        "opponent_team_id_bbref": "TOR",
        "stint_number": 0,
        "total_games": 1,
        "avg": 0.8,
        "obp": 0.8,
        "slg": 1.6,
        "ops": 2.4,
        "iso": 0.8,
        "bb_rate": 0.0,
        "k_rate": 0.0,
        "plate_appearances": 5,
        "at_bats": 5,
        "hits": 4,
        "runs_scored": 2,
        "rbis": 3,
        "bases_on_balls": 0,
        "strikeouts": 0,
        "doubles": 1,
        "triples": 0,
        "homeruns": 1,
        "stolen_bases": 0,
        "caught_stealing": 1,
        "hit_by_pitch": 0,
        "intentional_bb": 0,
        "gdp": 0,
        "sac_fly": 0,
        "sac_hit": 0,
        "total_pitches": 19,
        "total_strikes": 11,
        "wpa_bat": 0.145,
        "wpa_bat_pos": 0.166,
        "wpa_bat_neg": -0.021,
        "re24_bat": 2.9,
    }

    bat_stats_for_career = from_dict(data_class=BatStatsMetrics, data=bat_stats_for_career_dict)
    bat_stats_by_year = from_dict(data_class=BatStatsMetrics, data=bat_stats_by_year_dict)
    bat_stats_by_team = from_dict(data_class=BatStatsMetrics, data=bat_stats_by_team_dict)
    bat_stats_by_team_by_year = from_dict(data_class=BatStatsMetrics, data=bat_stats_by_team_by_year_dict)
    bat_stats_by_opp_team = from_dict(data_class=BatStatsMetrics, data=bat_stats_by_opp_team_dict)
    bat_stats_by_opp_team_by_year = from_dict(data_class=BatStatsMetrics, data=bat_stats_by_opp_team_by_year_dict)

    player_data = PlayerData(vig_app, 545361)
    assert player_data.bat_stats_for_career == bat_stats_for_career
    assert player_data.bat_stats_by_year == [bat_stats_by_year]
    assert player_data.bat_stats_by_team == [bat_stats_by_team]
    assert player_data.bat_stats_by_team_by_year == [bat_stats_by_team_by_year]
    assert player_data.bat_stats_by_opp_team == [bat_stats_by_opp_team]
    assert player_data.bat_stats_by_opp_team_by_year == [bat_stats_by_opp_team_by_year]


def test_pitch_stats(vig_app):
    pitch_stats_for_career_dict = {
        "mlb_id": 571882,
        "year": 0,
        "player_team_id_bbref": "",
        "opponent_team_id_bbref": "",
        "stint_number": 0,
        "total_games": 1,
        "games_as_sp": 1,
        "games_as_rp": 0,
        "wins": 0,
        "losses": 0,
        "saves": 0,
        "innings_pitched": 1.0,
        "total_outs": 3,
        "batters_faced": 3,
        "runs": 0,
        "earned_runs": 0,
        "hits": 0,
        "homeruns": 0,
        "strikeouts": 2,
        "bases_on_balls": 0,
        "era": 0.0,
        "whip": 0.0,
        "k_per_nine": 18.0,
        "bb_per_nine": 0.0,
        "hr_per_nine": 0.0,
        "k_per_bb": 0.0,
        "k_rate": 0.667,
        "bb_rate": 0.0,
        "k_minus_bb": 0.667,
        "hr_per_fb": 0.0,
        "pitch_count": 17,
        "strikes": 11,
        "strikes_contact": 4,
        "strikes_swinging": 2,
        "strikes_looking": 5,
        "ground_balls": 0,
        "fly_balls": 1,
        "line_drives": 1,
        "unknown_type": 0,
        "inherited_runners": 0,
        "inherited_scored": 0,
        "wpa_pitch": 0.049,
        "re24_pitch": 0.5,
    }

    pitch_stats_as_sp_dict = {
        "mlb_id": 571882,
        "year": 0,
        "player_team_id_bbref": "",
        "opponent_team_id_bbref": "",
        "stint_number": 0,
        "total_games": 1,
        "games_as_sp": 1,
        "games_as_rp": 0,
        "wins": 0,
        "losses": 0,
        "saves": 0,
        "innings_pitched": 1.0,
        "total_outs": 3,
        "batters_faced": 3,
        "runs": 0,
        "earned_runs": 0,
        "hits": 0,
        "homeruns": 0,
        "strikeouts": 2,
        "bases_on_balls": 0,
        "era": 0.0,
        "whip": 0.0,
        "k_per_nine": 18.0,
        "bb_per_nine": 0.0,
        "hr_per_nine": 0.0,
        "k_per_bb": 0.0,
        "k_rate": 0.667,
        "bb_rate": 0.0,
        "k_minus_bb": 0.667,
        "hr_per_fb": 0.0,
        "pitch_count": 17,
        "strikes": 11,
        "strikes_contact": 4,
        "strikes_swinging": 2,
        "strikes_looking": 5,
        "ground_balls": 0,
        "fly_balls": 1,
        "line_drives": 1,
        "unknown_type": 0,
        "inherited_runners": 0,
        "inherited_scored": 0,
        "wpa_pitch": 0.049,
        "re24_pitch": 0.5,
    }

    pitch_stats_as_rp = None

    pitch_stats_by_year_dict = {
        "mlb_id": 571882,
        "year": 2019,
        "player_team_id_bbref": "",
        "opponent_team_id_bbref": "",
        "stint_number": 0,
        "total_games": 1,
        "games_as_sp": 1,
        "games_as_rp": 0,
        "wins": 0,
        "losses": 0,
        "saves": 0,
        "innings_pitched": 1.0,
        "total_outs": 3,
        "batters_faced": 3,
        "runs": 0,
        "earned_runs": 0,
        "hits": 0,
        "homeruns": 0,
        "strikeouts": 2,
        "bases_on_balls": 0,
        "era": 0.0,
        "whip": 0.0,
        "k_per_nine": 18.0,
        "bb_per_nine": 0.0,
        "hr_per_nine": 0.0,
        "k_per_bb": 0.0,
        "k_rate": 0.667,
        "bb_rate": 0.0,
        "k_minus_bb": 0.667,
        "hr_per_fb": 0.0,
        "pitch_count": 17,
        "strikes": 11,
        "strikes_contact": 4,
        "strikes_swinging": 2,
        "strikes_looking": 5,
        "ground_balls": 0,
        "fly_balls": 1,
        "line_drives": 1,
        "unknown_type": 0,
        "inherited_runners": 0,
        "inherited_scored": 0,
        "wpa_pitch": 0.049,
        "re24_pitch": 0.5,
    }

    pitch_stats_by_team_dict = {
        "mlb_id": 571882,
        "year": 0,
        "player_team_id_bbref": "TOR",
        "opponent_team_id_bbref": "",
        "stint_number": 0,
        "total_games": 1,
        "games_as_sp": 1,
        "games_as_rp": 0,
        "wins": 0,
        "losses": 0,
        "saves": 0,
        "innings_pitched": 1.0,
        "total_outs": 3,
        "batters_faced": 3,
        "runs": 0,
        "earned_runs": 0,
        "hits": 0,
        "homeruns": 0,
        "strikeouts": 2,
        "bases_on_balls": 0,
        "era": 0.0,
        "whip": 0.0,
        "k_per_nine": 18.0,
        "bb_per_nine": 0.0,
        "hr_per_nine": 0.0,
        "k_per_bb": 0.0,
        "k_rate": 0.667,
        "bb_rate": 0.0,
        "k_minus_bb": 0.667,
        "hr_per_fb": 0.0,
        "pitch_count": 17,
        "strikes": 11,
        "strikes_contact": 4,
        "strikes_swinging": 2,
        "strikes_looking": 5,
        "ground_balls": 0,
        "fly_balls": 1,
        "line_drives": 1,
        "unknown_type": 0,
        "inherited_runners": 0,
        "inherited_scored": 0,
        "wpa_pitch": 0.049,
        "re24_pitch": 0.5,
    }

    pitch_stats_by_team_by_year_dict = {
        "mlb_id": 571882,
        "year": 2019,
        "player_team_id_bbref": "TOR",
        "opponent_team_id_bbref": "",
        "stint_number": 1,
        "total_games": 1,
        "games_as_sp": 1,
        "games_as_rp": 0,
        "wins": 0,
        "losses": 0,
        "saves": 0,
        "innings_pitched": 1.0,
        "total_outs": 3,
        "batters_faced": 3,
        "runs": 0,
        "earned_runs": 0,
        "hits": 0,
        "homeruns": 0,
        "strikeouts": 2,
        "bases_on_balls": 0,
        "era": 0.0,
        "whip": 0.0,
        "k_per_nine": 18.0,
        "bb_per_nine": 0.0,
        "hr_per_nine": 0.0,
        "k_per_bb": 0.0,
        "k_rate": 0.667,
        "bb_rate": 0.0,
        "k_minus_bb": 0.667,
        "hr_per_fb": 0.0,
        "pitch_count": 17,
        "strikes": 11,
        "strikes_contact": 4,
        "strikes_swinging": 2,
        "strikes_looking": 5,
        "ground_balls": 0,
        "fly_balls": 1,
        "line_drives": 1,
        "unknown_type": 0,
        "inherited_runners": 0,
        "inherited_scored": 0,
        "wpa_pitch": 0.049,
        "re24_pitch": 0.5,
    }

    pitch_stats_by_opp_team_dict = {
        "mlb_id": 571882,
        "year": 0,
        "player_team_id_bbref": "",
        "opponent_team_id_bbref": "LAA",
        "stint_number": 0,
        "total_games": 1,
        "games_as_sp": 1,
        "games_as_rp": 0,
        "wins": 0,
        "losses": 0,
        "saves": 0,
        "innings_pitched": 1.0,
        "total_outs": 3,
        "batters_faced": 3,
        "runs": 0,
        "earned_runs": 0,
        "hits": 0,
        "homeruns": 0,
        "strikeouts": 2,
        "bases_on_balls": 0,
        "era": 0.0,
        "whip": 0.0,
        "k_per_nine": 18.0,
        "bb_per_nine": 0.0,
        "hr_per_nine": 0.0,
        "k_per_bb": 0.0,
        "k_rate": 0.667,
        "bb_rate": 0.0,
        "k_minus_bb": 0.667,
        "hr_per_fb": 0.0,
        "pitch_count": 17,
        "strikes": 11,
        "strikes_contact": 4,
        "strikes_swinging": 2,
        "strikes_looking": 5,
        "ground_balls": 0,
        "fly_balls": 1,
        "line_drives": 1,
        "unknown_type": 0,
        "inherited_runners": 0,
        "inherited_scored": 0,
        "wpa_pitch": 0.049,
        "re24_pitch": 0.5,
    }

    pitch_stats_by_opp_team_by_year_dict = {
        "mlb_id": 571882,
        "year": 2019,
        "player_team_id_bbref": "",
        "opponent_team_id_bbref": "LAA",
        "stint_number": 0,
        "total_games": 1,
        "games_as_sp": 1,
        "games_as_rp": 0,
        "wins": 0,
        "losses": 0,
        "saves": 0,
        "innings_pitched": 1.0,
        "total_outs": 3,
        "batters_faced": 3,
        "runs": 0,
        "earned_runs": 0,
        "hits": 0,
        "homeruns": 0,
        "strikeouts": 2,
        "bases_on_balls": 0,
        "era": 0.0,
        "whip": 0.0,
        "k_per_nine": 18.0,
        "bb_per_nine": 0.0,
        "hr_per_nine": 0.0,
        "k_per_bb": 0.0,
        "k_rate": 0.667,
        "bb_rate": 0.0,
        "k_minus_bb": 0.667,
        "hr_per_fb": 0.0,
        "pitch_count": 17,
        "strikes": 11,
        "strikes_contact": 4,
        "strikes_swinging": 2,
        "strikes_looking": 5,
        "ground_balls": 0,
        "fly_balls": 1,
        "line_drives": 1,
        "unknown_type": 0,
        "inherited_runners": 0,
        "inherited_scored": 0,
        "wpa_pitch": 0.049,
        "re24_pitch": 0.5,
    }

    pitch_stats_for_career = from_dict(data_class=PitchStatsMetrics, data=pitch_stats_for_career_dict)
    pitch_stats_as_sp = from_dict(data_class=PitchStatsMetrics, data=pitch_stats_as_sp_dict)
    pitch_stats_as_rp = None
    pitch_stats_by_year = from_dict(data_class=PitchStatsMetrics, data=pitch_stats_by_year_dict)
    pitch_stats_by_team = from_dict(data_class=PitchStatsMetrics, data=pitch_stats_by_team_dict)
    pitch_stats_by_team_by_year = from_dict(data_class=PitchStatsMetrics, data=pitch_stats_by_team_by_year_dict)
    pitch_stats_by_opp_team = from_dict(data_class=PitchStatsMetrics, data=pitch_stats_by_opp_team_dict)
    pitch_stats_by_opp_team_by_year = from_dict(data_class=PitchStatsMetrics, data=pitch_stats_by_opp_team_by_year_dict)

    player_data = PlayerData(vig_app, 571882)
    assert player_data.pitch_stats_for_career == pitch_stats_for_career
    assert player_data.pitch_stats_as_sp == pitch_stats_as_sp
    assert player_data.pitch_stats_as_rp == pitch_stats_as_rp
    assert player_data.pitch_stats_by_year == [pitch_stats_by_year]
    assert player_data.pitch_stats_by_team == [pitch_stats_by_team]
    assert player_data.pitch_stats_by_team_by_year == [pitch_stats_by_team_by_year]
    assert player_data.pitch_stats_by_opp_team == [pitch_stats_by_opp_team]
    assert player_data.pitch_stats_by_opp_team_by_year == [pitch_stats_by_opp_team_by_year]


def test_pitchfx_metrics_career(vig_app):
    pfx_metrics_career_CU_dict = {
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
        "pitch_type": PitchType.CURVEBALL,
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

    pfx_metrics_career_FF_dict = {
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
        "pitch_type": PitchType.FOUR_SEAM_FASTBALL,
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

    pfx_metrics_career_CH_dict = {
        "avg_pfx_x": -6.37,
        "avg_pfx_z": 7.137,
        "avg_px": -0.3,
        "avg_pz": 2.37,
        "avg_speed": 86.037,
        "called_strike_rate": 0.333,
        "contact_rate": 0.333,
        "csw_rate": 0.667,
        "custom_score": 1.333,
        "fly_ball_rate": 0.0,
        "ground_ball_rate": 0.0,
        "line_drive_rate": 0.0,
        "money_pitch": False,
        "o_contact_rate": 0.0,
        "o_swing_rate": 0.0,
        "percent": 0.176,
        "pitch_type": PitchType.CHANGEUP,
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

    pfx_metrics_career_SL_dict = {
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
        "pitch_type": PitchType.SLIDER,
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

    pfx_metrics_career_dict = {
        "pitcher_id_mlb": 571882,
        "metrics_detail": {
            PitchType.CURVEBALL: pfx_metrics_career_CU_dict,
            PitchType.FOUR_SEAM_FASTBALL: pfx_metrics_career_FF_dict,
            PitchType.CHANGEUP: pfx_metrics_career_CH_dict,
            PitchType.SLIDER: pfx_metrics_career_SL_dict,
        },
    }
    pfx_metrics_career = from_dict(data_class=PitchFxMetricsCollection, data=pfx_metrics_career_dict)
    player_data = PlayerData(vig_app, 571882)
    assert player_data.pitchfx_metrics_career == pfx_metrics_career


#     (pitch_mix_total_bat_r, pitch_mix_detail_bat_r) = pitch_mix["vs_rhb"]
#     assert pitch_mix_total_bat_r == {
#         "called_strike_rate": 0.2,
#         "contact_rate": 0.4,
#         "csw_rate": 0.2,
#         "fly_ball_rate": 0.0,
#         "ground_ball_rate": 0.0,
#         "line_drive_rate": 1.0,
#         "money_pitch": 0,
#         "o_contact_rate": 0.0,
#         "o_swing_rate": 0.0,
#         "percent": 1.0,
#         "pitch_types": ["FOUR_SEAM_FASTBALL", "CURVEBALL"],
#         "pop_up_rate": 0.0,
#         "swing_rate": 0.4,
#         "swinging_strike_rate": 0.0,
#         "total_batted_balls": 1,
#         "total_called_strikes": 1,
#         "total_contact_inside_zone": 2,
#         "total_contact_outside_zone": 0,
#         "total_fly_balls": 0,
#         "total_ground_balls": 0,
#         "total_inside_strike_zone": 3,
#         "total_line_drives": 1,
#         "total_outside_strike_zone": 2,
#         "total_pitches": 5,
#         "total_pop_ups": 0,
#         "total_swinging_strikes": 0,
#         "total_swings": 2,
#         "total_swings_inside_zone": 2,
#         "total_swings_made_contact": 2,
#         "total_swings_outside_zone": 0,
#         "whiff_rate": 0.0,
#         "z_contact_rate": 1.0,
#         "z_swing_rate": 0.667,
#         "zone_rate": 0.6,
#     }

#     assert pitch_mix_detail_bat_r["FOUR_SEAM_FASTBALL"] == {
#         "avg_pfx_x": -1.923,
#         "avg_pfx_z": 9.193,
#         "avg_px": -0.047,
#         "avg_pz": 3.453,
#         "avg_speed": 94.85,
#         "called_strike_rate": 0.0,
#         "contact_rate": 0.667,
#         "csw_rate": 0.0,
#         "custom_score": 0.667,
#         "fly_ball_rate": 0.0,
#         "ground_ball_rate": 0.0,
#         "line_drive_rate": 1.0,
#         "o_contact_rate": 0.0,
#         "o_swing_rate": 0.0,
#         "percent": 0.6,
#         "pitch_type": "FOUR_SEAM_FASTBALL",
#         "pop_up_rate": 0.0,
#         "swing_rate": 0.667,
#         "swinging_strike_rate": 0.0,
#         "total_batted_balls": 1,
#         "total_called_strikes": 0,
#         "total_contact_inside_zone": 2,
#         "total_contact_outside_zone": 0,
#         "total_fly_balls": 0,
#         "total_ground_balls": 0,
#         "total_inside_strike_zone": 2,
#         "total_line_drives": 1,
#         "total_outside_strike_zone": 1,
#         "total_pitches": 3,
#         "total_pop_ups": 0,
#         "total_swinging_strikes": 0,
#         "total_swings": 2,
#         "total_swings_inside_zone": 2,
#         "total_swings_made_contact": 2,
#         "total_swings_outside_zone": 0,
#         "whiff_rate": 0.0,
#         "z_contact_rate": 1.0,
#         "z_swing_rate": 1.0,
#         "zone_rate": 0.667,
#     }
#     assert pitch_mix_detail_bat_r["CURVEBALL"] == {
#         "avg_pfx_x": 2.215,
#         "avg_pfx_z": -3.9,
#         "avg_px": 0.53,
#         "avg_pz": 2.09,
#         "avg_speed": 78.88,
#         "called_strike_rate": 0.5,
#         "contact_rate": 0.0,
#         "csw_rate": 0.5,
#         "custom_score": 0.5,
#         "fly_ball_rate": 0.0,
#         "ground_ball_rate": 0.0,
#         "line_drive_rate": 0.0,
#         "o_contact_rate": 0.0,
#         "o_swing_rate": 0.0,
#         "percent": 0.4,
#         "pitch_type": "CURVEBALL",
#         "pop_up_rate": 0.0,
#         "swing_rate": 0.0,
#         "swinging_strike_rate": 0.0,
#         "total_batted_balls": 0,
#         "total_called_strikes": 1,
#         "total_contact_inside_zone": 0,
#         "total_contact_outside_zone": 0,
#         "total_fly_balls": 0,
#         "total_ground_balls": 0,
#         "total_inside_strike_zone": 1,
#         "total_line_drives": 0,
#         "total_outside_strike_zone": 1,
#         "total_pitches": 2,
#         "total_pop_ups": 0,
#         "total_swinging_strikes": 0,
#         "total_swings": 0,
#         "total_swings_inside_zone": 0,
#         "total_swings_made_contact": 0,
#         "total_swings_outside_zone": 0,
#         "whiff_rate": 0.0,
#         "z_contact_rate": 0.0,
#         "z_swing_rate": 0.0,
#         "zone_rate": 0.5,
#     }

#     assert "vs_lhb" in pitch_mix
#     (pitch_mix_total_bat_l, pitch_mix_detail_bat_l) = pitch_mix["vs_lhb"]
#     assert pitch_mix_total_bat_l == {
#         "called_strike_rate": 0.333,
#         "contact_rate": 0.167,
#         "csw_rate": 0.5,
#         "fly_ball_rate": 0.0,
#         "ground_ball_rate": 0.0,
#         "line_drive_rate": 0.0,
#         "money_pitch": 0,
#         "o_contact_rate": 0.5,
#         "o_swing_rate": 0.333,
#         "percent": 1.0,
#         "pitch_types": ["CURVEBALL", "CHANGEUP", "FOUR_SEAM_FASTBALL", "SLIDER"],
#         "pop_up_rate": 0.0,
#         "swing_rate": 0.333,
#         "swinging_strike_rate": 0.167,
#         "total_batted_balls": 0,
#         "total_called_strikes": 4,
#         "total_contact_inside_zone": 1,
#         "total_contact_outside_zone": 1,
#         "total_fly_balls": 0,
#         "total_ground_balls": 0,
#         "total_inside_strike_zone": 6,
#         "total_line_drives": 0,
#         "total_outside_strike_zone": 6,
#         "total_pitches": 12,
#         "total_pop_ups": 0,
#         "total_swinging_strikes": 2,
#         "total_swings": 4,
#         "total_swings_inside_zone": 2,
#         "total_swings_made_contact": 2,
#         "total_swings_outside_zone": 2,
#         "whiff_rate": 0.5,
#         "z_contact_rate": 0.5,
#         "z_swing_rate": 0.333,
#         "zone_rate": 0.5,
#     }
#     assert pitch_mix_detail_bat_l["CURVEBALL"] == {
#         "avg_pfx_x": 3.26,
#         "avg_pfx_z": -4.91,
#         "avg_px": -0.255,
#         "avg_pz": 1.633,
#         "avg_speed": 78.597,
#         "called_strike_rate": 0.5,
#         "contact_rate": 0.25,
#         "csw_rate": 0.5,
#         "custom_score": 1.0,
#         "fly_ball_rate": 0.0,
#         "ground_ball_rate": 0.0,
#         "line_drive_rate": 0.0,
#         "o_contact_rate": 1.0,
#         "o_swing_rate": 0.5,
#         "percent": 0.333,
#         "pitch_type": "CURVEBALL",
#         "pop_up_rate": 0.0,
#         "swing_rate": 0.25,
#         "swinging_strike_rate": 0.0,
#         "total_batted_balls": 0,
#         "total_called_strikes": 2,
#         "total_contact_inside_zone": 0,
#         "total_contact_outside_zone": 1,
#         "total_fly_balls": 0,
#         "total_ground_balls": 0,
#         "total_inside_strike_zone": 2,
#         "total_line_drives": 0,
#         "total_outside_strike_zone": 2,
#         "total_pitches": 4,
#         "total_pop_ups": 0,
#         "total_swinging_strikes": 0,
#         "total_swings": 1,
#         "total_swings_inside_zone": 0,
#         "total_swings_made_contact": 1,
#         "total_swings_outside_zone": 1,
#         "whiff_rate": 0.0,
#         "z_contact_rate": 0.0,
#         "z_swing_rate": 0.0,
#         "zone_rate": 0.5,
#     }
#     assert pitch_mix_detail_bat_l["CHANGEUP"] == {
#         "avg_pfx_x": -6.37,
#         "avg_pfx_z": 7.137,
#         "avg_px": -0.3,
#         "avg_pz": 2.37,
#         "avg_speed": 86.037,
#         "called_strike_rate": 0.333,
#         "contact_rate": 0.333,
#         "csw_rate": 0.667,
#         "custom_score": 0.0,
#         "fly_ball_rate": 0.0,
#         "ground_ball_rate": 0.0,
#         "line_drive_rate": 0.0,
#         "o_contact_rate": 0.0,
#         "o_swing_rate": 0.0,
#         "percent": 0.25,
#         "pitch_type": "CHANGEUP",
#         "pop_up_rate": 0.0,
#         "swing_rate": 0.667,
#         "swinging_strike_rate": 0.333,
#         "total_batted_balls": 0,
#         "total_called_strikes": 1,
#         "total_contact_inside_zone": 1,
#         "total_contact_outside_zone": 0,
#         "total_fly_balls": 0,
#         "total_ground_balls": 0,
#         "total_inside_strike_zone": 3,
#         "total_line_drives": 0,
#         "total_outside_strike_zone": 0,
#         "total_pitches": 3,
#         "total_pop_ups": 0,
#         "total_swinging_strikes": 1,
#         "total_swings": 2,
#         "total_swings_inside_zone": 2,
#         "total_swings_made_contact": 1,
#         "total_swings_outside_zone": 0,
#         "whiff_rate": 0.5,
#         "z_contact_rate": 0.5,
#         "z_swing_rate": 0.667,
#         "zone_rate": 1.0,
#     }
#     assert pitch_mix_detail_bat_l["FOUR_SEAM_FASTBALL"] == {
#         "avg_pfx_x": -0.593,
#         "avg_pfx_z": 8.187,
#         "avg_px": -0.053,
#         "avg_pz": 3.16,
#         "avg_speed": 93.67,
#         "called_strike_rate": 0.333,
#         "contact_rate": 0.0,
#         "csw_rate": 0.333,
#         "custom_score": 0.333,
#         "fly_ball_rate": 0.0,
#         "ground_ball_rate": 0.0,
#         "line_drive_rate": 0.0,
#         "o_contact_rate": 0.0,
#         "o_swing_rate": 0.0,
#         "percent": 0.25,
#         "pitch_type": "FOUR_SEAM_FASTBALL",
#         "pop_up_rate": 0.0,
#         "swing_rate": 0.0,
#         "swinging_strike_rate": 0.0,
#         "total_batted_balls": 0,
#         "total_called_strikes": 1,
#         "total_contact_inside_zone": 0,
#         "total_contact_outside_zone": 0,
#         "total_fly_balls": 0,
#         "total_ground_balls": 0,
#         "total_inside_strike_zone": 1,
#         "total_line_drives": 0,
#         "total_outside_strike_zone": 2,
#         "total_pitches": 3,
#         "total_pop_ups": 0,
#         "total_swinging_strikes": 0,
#         "total_swings": 0,
#         "total_swings_inside_zone": 0,
#         "total_swings_made_contact": 0,
#         "total_swings_outside_zone": 0,
#         "whiff_rate": 0.0,
#         "z_contact_rate": 0.0,
#         "z_swing_rate": 0.0,
#         "zone_rate": 0.333,
#     }
#     assert pitch_mix_detail_bat_l["SLIDER"] == {
#         "avg_pfx_x": 1.83,
#         "avg_pfx_z": -1.265,
#         "avg_px": 0.845,
#         "avg_pz": 0.54,
#         "avg_speed": 84.225,
#         "called_strike_rate": 0.0,
#         "contact_rate": 0.0,
#         "csw_rate": 0.5,
#         "custom_score": 1.0,
#         "fly_ball_rate": 0.0,
#         "ground_ball_rate": 0.0,
#         "line_drive_rate": 0.0,
#         "o_contact_rate": 0.0,
#         "o_swing_rate": 0.5,
#         "percent": 0.167,
#         "pitch_type": "SLIDER",
#         "pop_up_rate": 0.0,
#         "swing_rate": 0.5,
#         "swinging_strike_rate": 0.5,
#         "total_batted_balls": 0,
#         "total_called_strikes": 0,
#         "total_contact_inside_zone": 0,
#         "total_contact_outside_zone": 0,
#         "total_fly_balls": 0,
#         "total_ground_balls": 0,
#         "total_inside_strike_zone": 0,
#         "total_line_drives": 0,
#         "total_outside_strike_zone": 2,
#         "total_pitches": 2,
#         "total_pop_ups": 0,
#         "total_swinging_strikes": 1,
#         "total_swings": 1,
#         "total_swings_inside_zone": 0,
#         "total_swings_made_contact": 0,
#         "total_swings_outside_zone": 1,
#         "whiff_rate": 1.0,
#         "z_contact_rate": 0.0,
#         "z_swing_rate": 0.0,
#         "zone_rate": 0.0,
#     }
