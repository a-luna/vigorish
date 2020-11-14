from copy import deepcopy

from vigorish.enums import PitchType


def calc_pitch_mix(pitch_count_dict, threshold=0.01):
    pitch_types = []
    mix_total = 0
    for col_name, val in pitch_count_dict.items():
        if not ("_count" in col_name):
            continue
        if val == 0:
            continue
        if not _check_threshold(val, pitch_count_dict["total_pitches"], threshold):
            continue
        mix_total += val
        pitch_type_abbrev = col_name.split("_")[0].upper()
        pitch_type = PitchType.from_abbrev(pitch_type_abbrev)
        pitch_types.append({"type": pitch_type, "count": val})
    pitch_mix_int = 0
    for pt in pitch_types:
        pt["percent"] = round(pt["count"] / float(mix_total), 3)
        pitch_mix_int += int(pt["type"])
    pitch_types.append({"type": PitchType.ALL, "percent": 1.000, "count": mix_total})
    pitch_types.sort(key=lambda x: x["percent"], reverse=True)
    return {x["type"]: {"percent": x["percent"], "count": x["count"]} for x in pitch_types}


def _check_threshold(pitch_count, total_pitches, threshold):
    percent = pitch_count / float(total_pitches)
    return percent >= threshold


def get_pitch_mix_from_int(pitch_mix_int):
    pitch_types = []
    for ptype in PitchType:
        if ptype == PitchType.ALL:
            continue
        if pitch_mix_int & ptype == ptype:
            pitch_types.append(ptype)
    return pitch_types


def get_pitch_type_metrics(pitch_type_dict):
    d = deepcopy(pitch_type_dict)
    d.pop("id")
    d.pop("season_id")
    d.pop("mlb_id")
    d.pop("name")
    pitch_type = PitchType.from_abbrev(d.pop("pitch_type"))
    for metric, val in d.items():
        d[metric] = round(val, 3)
    return (pitch_type, d)
