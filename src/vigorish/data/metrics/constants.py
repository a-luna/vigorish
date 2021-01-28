FLOAT_PITCH_METRIC_NAMES = [
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

INT_PITCH_METRIC_NAMES = [
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

BOOL_PITCH_METRIC_NAMES = ["money_pitch"]

PLATE_DISCIPLINE_METRICS = {
    "Z%": (FLOAT_PITCH_METRIC_NAMES[0], INT_PITCH_METRIC_NAMES[5]),
    "SS%": (FLOAT_PITCH_METRIC_NAMES[2], INT_PITCH_METRIC_NAMES[4]),
    "O-SW%": (FLOAT_PITCH_METRIC_NAMES[5], INT_PITCH_METRIC_NAMES[8]),
    "CON%": (FLOAT_PITCH_METRIC_NAMES[10], INT_PITCH_METRIC_NAMES[2]),
}

BATTED_BALL_METRICS = {
    "GB%": (FLOAT_PITCH_METRIC_NAMES[12], INT_PITCH_METRIC_NAMES[12]),
    "LD%": (FLOAT_PITCH_METRIC_NAMES[13], INT_PITCH_METRIC_NAMES[13]),
    "FB%": (FLOAT_PITCH_METRIC_NAMES[14], INT_PITCH_METRIC_NAMES[14]),
    "PU%": (FLOAT_PITCH_METRIC_NAMES[15], INT_PITCH_METRIC_NAMES[15]),
}
