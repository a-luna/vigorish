PFX_FLOAT_METRIC_NAMES = [
    "zone_rate",  # 0
    "called_strike_rate",  # 1
    "swinging_strike_rate",  # 2
    "whiff_rate",  # 3
    "csw_rate",  # 4
    "o_swing_rate",  # 5
    "z_swing_rate",  # 6
    "swing_rate",  # 7
    "o_contact_rate",  # 8
    "z_contact_rate",  # 9
    "contact_rate",  # 10
    "custom_score",  # 11
    "ground_ball_rate",  # 12
    "line_drive_rate",  # 13
    "fly_ball_rate",  # 14
    "pop_up_rate",  # 15
    "avg_speed",  # 16
    "avg_pfx_x",  # 17
    "avg_pfx_z",  # 18
    "avg_px",  # 19
    "avg_pz",  # 20
]

PFX_INT_METRIC_NAMES = [
    "total_pitches",  # 0
    "total_swings",  # 1
    "total_swings_made_contact",  # 2
    "total_called_strikes",  # 3
    "total_swinging_strikes",  # 4
    "total_inside_strike_zone",  # 5
    "total_outside_strike_zone",  # 6
    "total_swings_inside_zone",  # 7
    "total_swings_outside_zone",  # 8
    "total_contact_inside_zone",  # 9
    "total_contact_outside_zone",  # 10
    "total_batted_balls",  # 11
    "total_ground_balls",  # 12
    "total_line_drives",  # 13
    "total_fly_balls",  # 14
    "total_pop_ups",  # 15
]

PFX_BOOL_METRIC_NAMES = ["money_pitch"]

PFX_PLATE_DISCIPLINE_METRICS = {
    "Z%": (PFX_FLOAT_METRIC_NAMES[0], PFX_INT_METRIC_NAMES[5]),
    "SS%": (PFX_FLOAT_METRIC_NAMES[2], PFX_INT_METRIC_NAMES[4]),
    "O-SW%": (PFX_FLOAT_METRIC_NAMES[5], PFX_INT_METRIC_NAMES[8]),
    "CON%": (PFX_FLOAT_METRIC_NAMES[10], PFX_INT_METRIC_NAMES[2]),
}

PFX_BATTED_BALL_METRICS = {
    "GB%": (PFX_FLOAT_METRIC_NAMES[12], PFX_INT_METRIC_NAMES[12]),
    "LD%": (PFX_FLOAT_METRIC_NAMES[13], PFX_INT_METRIC_NAMES[13]),
    "FB%": (PFX_FLOAT_METRIC_NAMES[14], PFX_INT_METRIC_NAMES[14]),
    "PU%": (PFX_FLOAT_METRIC_NAMES[15], PFX_INT_METRIC_NAMES[15]),
}

PITCH_STATS_FLOAT_METRIC_NAMES = [
    "innings_pitched",
    "era",
    "whip",
    "k_per_nine",
    "bb_per_nine",
    "hr_per_nine",
    "k_per_bb",
    "k_rate",
    "bb_rate",
    "k_minus_bb",
    "hr_per_fb",
    "wpa_pitch",
    "re24_pitch",
]

PITCH_STATS_INT_METRIC_NAMES = [
    "year",
    "stint_number",
    "total_games",
    "games_as_sp",
    "games_as_rp",
    "wins",
    "losses",
    "saves",
    "total_outs",
    "batters_faced",
    "runs",
    "earned_runs",
    "hits",
    "homeruns",
    "strikeouts",
    "bases_on_balls",
    "pitch_count",
    "strikes",
    "strikes_contact",
    "strikes_swinging",
    "strikes_looking",
    "ground_balls",
    "fly_balls",
    "line_drives",
    "unknown_type",
    "inherited_runners",
    "inherited_scored",
]

PITCH_STATS_STR_METRIC_NAMES = [
    "player_team_id_bbref",
    "opponent_team_id_bbref",
]
