def calc_pitch_mix(pitch_count_dict, total_pitches):
    pitch_types = []
    mix_total = 0
    for col_name, val in pitch_count_dict.items():
        if not ("_count" in col_name):
            continue
        if val == 0:
            continue
        pitch_type = col_name.split("_")[0].upper()
        pitch_percent = val / float(total_pitches)
        if pitch_percent < 0.01:
            continue
        mix_total += val
        pitch_types.append({"type": pitch_type, "percent": pitch_percent, "count": val})
    pitch_types.append({"type": "ALL", "percent": 1, "count": mix_total})
    pitch_types.sort(key=lambda x: x["percent"], reverse=True)
    return {x["type"]: {"percent": x["percent"], "count": x["count"]} for x in pitch_types}
