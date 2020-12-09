from vigorish.enums import PitchType

PITCH_METRIC_NAMES = ["total_pitches", "avg_speed", "avg_pfx_x", "avg_pfx_z", "avg_px", "avg_pz"]


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
        metrics["pitch_type"] = pitch_type
        metrics["count"] = metrics.pop("total_pitches")
        valid_pitch_types.append(metrics)
    for metrics in valid_pitch_types:
        metrics["percent"] = round(metrics["count"] / total_pitches_valid, 3)
    valid_pitch_types.sort(key=lambda x: x["percent"], reverse=True)
    pitch_mix = get_pitch_types_from_int(pitch_mix_int)
    pitch_mix.sort(key=lambda x: all_pitch_types[x]["percent"], reverse=True)
    pitch_mix_total = {"pitch_types": pitch_mix, "percent": 1.000, "count": total_pitches_valid}
    pitch_mix_detail = {x["pitch_type"]: x for x in valid_pitch_types}
    return (pitch_mix_total, pitch_mix_detail)


def get_metrics_for_single_pitch_type(pitch_type_dict):
    pitch_metrics = {}
    pitch_type = PitchType.from_abbrev(pitch_type_dict["pitch_type"])
    for metric in PITCH_METRIC_NAMES:
        pitch_metrics[metric] = round(pitch_type_dict[metric], 3)
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
