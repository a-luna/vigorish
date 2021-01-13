from vigorish.enums import PitchType

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
    "money_pitch",  # 16
]

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


def get_metrics_for_all_pitch_types(pitch_type_results, threshold=0.01):
    all_pitch_types = {}
    for d in [dict(row) for row in pitch_type_results]:
        (pitch_type, metrics) = get_metrics_for_single_pitch_type(d)
        all_pitch_types[pitch_type] = metrics
    total_pitches_all = sum(metrics["total_pitches"] for metrics in all_pitch_types.values())
    total_pitches_valid = 0
    pitch_mix_int = 0
    valid_pitch_types = []
    for pitch_type, metrics in all_pitch_types.items():
        if not _check_threshold(metrics["total_pitches"], total_pitches_all, threshold):
            continue
        total_pitches_valid += metrics["total_pitches"]
        pitch_mix_int += int(pitch_type)
        metrics["pitch_type"] = pitch_type.name
        valid_pitch_types.append(metrics)
    for metrics in valid_pitch_types:
        metrics["percent"] = round(metrics["total_pitches"] / total_pitches_valid, 3)
    valid_pitch_types.sort(key=lambda x: x["percent"], reverse=True)
    pitch_mix = get_pitch_types_from_int(pitch_mix_int)
    pitch_mix.sort(key=lambda x: all_pitch_types[x]["percent"], reverse=True)
    pitch_mix_total = calculate_pitch_metrics_for_all_pitch_types(pitch_mix, valid_pitch_types)
    pitch_mix_detail = {x["pitch_type"]: x for x in valid_pitch_types}
    return (pitch_mix_total, pitch_mix_detail)


def get_metrics_for_single_pitch_type(pitch_type_dict):
    pitch_metrics = {}
    pitch_type = PitchType.from_abbrev(pitch_type_dict["pitch_type"])
    for metric in FLOAT_PITCH_METRIC_NAMES:
        if metric in pitch_type_dict:
            pitch_metrics[metric] = round(pitch_type_dict[metric], 3)
    for metric in INT_PITCH_METRIC_NAMES:
        if metric in pitch_type_dict:
            pitch_metrics[metric] = pitch_type_dict[metric]
    return (pitch_type, pitch_metrics)


def _check_threshold(pitch_count, total_pitches, threshold):
    percent = pitch_count / float(total_pitches)
    return percent >= threshold


def get_pitch_types_from_int(pitch_mix_int):
    pitch_types = []
    for pt in PitchType:
        if pt == PitchType.ALL:
            continue
        if pitch_mix_int & pt == pt:
            pitch_types.append(pt)
    return pitch_types


def calculate_pitch_metrics_for_all_pitch_types(pitch_mix, pitch_type_metrics):
    all_pitch_types = {"pitch_types": [pt.name for pt in pitch_mix], "percent": 1.000}
    for metric in INT_PITCH_METRIC_NAMES:
        all_pitch_types[metric] = sum(
            pitch_type[metric]
            for pitch_type in pitch_type_metrics
            if metric in pitch_type and pitch_type[metric]
        )
    calculate_pitch_discipline_metrics(all_pitch_types)
    calculate_batted_ball_metrics(all_pitch_types)
    return all_pitch_types


def calculate_pitch_discipline_metrics(pitch_metrics):
    pitch_metrics["zone_rate"] = (
        round(pitch_metrics["total_inside_strike_zone"] / float(pitch_metrics["total_pitches"]), 3)
        if pitch_metrics["total_pitches"]
        else 0.0
    )
    pitch_metrics["called_strike_rate"] = (
        round(pitch_metrics["total_called_strikes"] / float(pitch_metrics["total_pitches"]), 3)
        if pitch_metrics["total_pitches"]
        else 0.0
    )
    pitch_metrics["swinging_strike_rate"] = (
        round(pitch_metrics["total_swinging_strikes"] / float(pitch_metrics["total_pitches"]), 3)
        if pitch_metrics["total_pitches"]
        else 0.0
    )
    pitch_metrics["whiff_rate"] = (
        round(pitch_metrics["total_swinging_strikes"] / float(pitch_metrics["total_swings"]), 3)
        if pitch_metrics["total_swings"]
        else 0.0
    )
    pitch_metrics["csw_rate"] = (
        round(
            (pitch_metrics["total_called_strikes"] + pitch_metrics["total_swinging_strikes"])
            / float(pitch_metrics["total_pitches"]),
            3,
        )
        if pitch_metrics["total_pitches"]
        else 0.0
    )
    pitch_metrics["o_swing_rate"] = (
        round(
            pitch_metrics["total_swings_outside_zone"]
            / float(pitch_metrics["total_outside_strike_zone"]),
            3,
        )
        if pitch_metrics["total_outside_strike_zone"]
        else 0.0
    )
    pitch_metrics["z_swing_rate"] = (
        round(
            pitch_metrics["total_swings_inside_zone"]
            / float(pitch_metrics["total_inside_strike_zone"]),
            3,
        )
        if pitch_metrics["total_inside_strike_zone"]
        else 0.0
    )
    pitch_metrics["swing_rate"] = (
        round(pitch_metrics["total_swings"] / float(pitch_metrics["total_pitches"]), 3)
        if pitch_metrics["total_pitches"]
        else 0.0
    )
    pitch_metrics["o_contact_rate"] = (
        round(
            pitch_metrics["total_contact_outside_zone"]
            / float(pitch_metrics["total_swings_outside_zone"]),
            3,
        )
        if pitch_metrics["total_swings_outside_zone"]
        else 0.0
    )
    pitch_metrics["z_contact_rate"] = (
        round(
            pitch_metrics["total_contact_inside_zone"]
            / float(pitch_metrics["total_swings_inside_zone"]),
            3,
        )
        if pitch_metrics["total_swings_inside_zone"]
        else 0.0
    )
    pitch_metrics["contact_rate"] = (
        round(pitch_metrics["total_swings_made_contact"] / float(pitch_metrics["total_pitches"]), 3)
        if pitch_metrics["total_pitches"]
        else 0.0
    )


def calculate_batted_ball_metrics(pitch_metrics):
    pitch_metrics["ground_ball_rate"] = (
        round(pitch_metrics["total_ground_balls"] / float(pitch_metrics["total_batted_balls"]), 3)
        if pitch_metrics["total_batted_balls"]
        else 0.0
    )
    pitch_metrics["fly_ball_rate"] = (
        round(pitch_metrics["total_fly_balls"] / float(pitch_metrics["total_batted_balls"]), 3)
        if pitch_metrics["total_batted_balls"]
        else 0.0
    )
    pitch_metrics["line_drive_rate"] = (
        round(pitch_metrics["total_line_drives"] / float(pitch_metrics["total_batted_balls"]), 3)
        if pitch_metrics["total_batted_balls"]
        else 0.0
    )
    pitch_metrics["pop_up_rate"] = (
        round(pitch_metrics["total_pop_ups"] / float(pitch_metrics["total_batted_balls"]), 3)
        if pitch_metrics["total_batted_balls"]
        else 0.0
    )
