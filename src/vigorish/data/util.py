from datetime import datetime, timezone

from vigorish.util.datetime_util import TIME_ZONE_NEW_YORK
from vigorish.util.regex import PFX_TIMESTAMP_REGEX
from vigorish.util.string_helpers import validate_bbref_game_id


def calc_pitch_metrics(combined_data):
    pitch_samples = []
    inning_samples = []
    prev_inning_last_pfx = None
    for half_inning in combined_data["play_by_play_data"]:
        for ab_num, at_bat in enumerate(half_inning["inning_events"], start=1):
            if at_bat["pitchfx_data_error"] or at_bat["missing_pitchfx_count"]:
                continue
            for count, pfx in enumerate(at_bat["pitchfx"], start=1):
                if count == 1 and prev_inning_last_pfx and ab_num == 1:
                    this_pitch = get_time_pitch_thrown(pfx)
                    last_pitch = get_time_pitch_thrown(prev_inning_last_pfx)
                    time_between_innings = this_pitch - last_pitch
                    if time_between_innings.total_seconds() > 0:
                        inning_samples.append(time_between_innings.total_seconds())
                if count == len(at_bat["pitchfx"]) and ab_num == len(half_inning["inning_events"]):
                    prev_inning_last_pfx = pfx
                    continue
                next_pitch = get_time_pitch_thrown(at_bat["pitchfx"][count])
                this_pitch = get_time_pitch_thrown(pfx)
                time_between_pitches = next_pitch - this_pitch
                if time_between_pitches.total_seconds() > 0:
                    pitch_samples.append(time_between_pitches.total_seconds())

    pitch_metrics = process_data_set(pitch_samples)
    inning_metrics = process_data_set(inning_samples)
    return (pitch_samples, pitch_metrics, inning_samples, inning_metrics)


def get_time_pitch_thrown(pfx):
    match = PFX_TIMESTAMP_REGEX.match(pfx["park_sv_id"])
    if not match:
        return None
    group_dict = match.groupdict()
    game_dict = validate_bbref_game_id(pfx["bbref_game_id"]).value
    timestamp = datetime(
        game_dict["game_date"].year,
        int(group_dict["month"]),
        int(group_dict["day"]),
        int(group_dict["hour"]),
        int(group_dict["minute"]),
        int(group_dict["second"]),
    )
    return timestamp.replace(tzinfo=timezone.utc).astimezone(TIME_ZONE_NEW_YORK)


def process_data_set(data_set, trim=False, st_dev=1):
    if not data_set or not isinstance(data_set, list):
        return {}
    return {
        "total": sum(data_set),
        "count": len(data_set),
        "avg": sum(data_set) / len(data_set),
        "max": max(data_set),
        "min": min(data_set),
        "range": max(data_set) - min(data_set),
    }
