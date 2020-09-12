from datetime import datetime, timezone

from vigorish.util.datetime_util import TIME_ZONE_NEW_YORK
from vigorish.util.regex import PFX_TIMESTAMP_REGEX
from vigorish.util.numeric_helpers import trim_data_set
from vigorish.util.string_helpers import validate_bbref_game_id


def calc_pitch_metrics(combined_data):
    pitch_samples = []
    at_bat_samples = []
    inning_samples = []
    prev_at_bat_last_pfx = None
    prev_inning_last_pfx = None
    for half_inning in combined_data["play_by_play_data"]:
        for ab_num, at_bat in enumerate(half_inning["inning_events"], start=1):
            if (
                at_bat["at_bat_pitchfx_audit"]["pitchfx_error"]
                or at_bat["at_bat_pitchfx_audit"]["missing_pitchfx_count"]
            ):
                continue
            for count, pfx in enumerate(at_bat["pitchfx"], start=1):
                if count == 1 and prev_inning_last_pfx and ab_num == 1:
                    seconds_between_innings = get_seconds_between_pitches(
                        prev_inning_last_pfx, pfx
                    )
                    if seconds_between_innings > 0:
                        inning_samples.append(seconds_between_innings)
                    prev_inning_last_pfx = None
                if count == 1 and prev_at_bat_last_pfx and ab_num != 1:
                    seconds_between_at_bats = get_seconds_between_pitches(
                        prev_at_bat_last_pfx, pfx
                    )
                    if seconds_between_at_bats > 0:
                        at_bat_samples.append(seconds_between_at_bats)
                    prev_at_bat_last_pfx = None
                if count == len(at_bat["pitchfx"]):
                    if ab_num == len(half_inning["inning_events"]):
                        prev_inning_last_pfx = pfx
                    else:
                        prev_at_bat_last_pfx = pfx
                    continue
                seconds_between_pitches = get_seconds_between_pitches(
                    pfx, at_bat["pitchfx"][count]
                )
                if seconds_between_pitches > 0:
                    pitch_samples.append(seconds_between_pitches)
    return (
        pitch_samples,
        process_data_set(pitch_samples),
        at_bat_samples,
        process_data_set(at_bat_samples),
        inning_samples,
        process_data_set(inning_samples),
    )


def get_seconds_between_pitches(pitch1, pitch2):
    pitch1_thrown = get_time_pitch_thrown(pitch1)
    pitch2_thrown = get_time_pitch_thrown(pitch2)
    if not pitch1_thrown or not pitch2_thrown:
        return 0
    return (pitch2_thrown - pitch1_thrown).total_seconds()


def get_time_pitch_thrown(pfx):
    match = PFX_TIMESTAMP_REGEX.match(pfx["park_sv_id"])
    if not match:
        return None
    group_dict = match.groupdict()
    game_dict = validate_bbref_game_id(pfx["bbref_game_id"]).value
    try:
        timestamp = datetime(
            game_dict["game_date"].year,
            int(group_dict["month"]),
            int(group_dict["day"]),
            int(group_dict["hour"]),
            int(group_dict["minute"]),
            int(group_dict["second"]),
        )
    except ValueError:
        return None
    return timestamp.replace(tzinfo=timezone.utc).astimezone(TIME_ZONE_NEW_YORK)


def process_data_set(data_set, trim=False, st_dev=1):
    if not data_set or not isinstance(data_set, list):
        return {}
    if trim:
        data_set = trim_data_set(data_set, st_dev_limit=st_dev)
    results = {
        "total": sum(data_set),
        "count": len(data_set),
        "avg": sum(data_set) / len(data_set),
        "max": max(data_set),
        "min": min(data_set),
        "range": max(data_set) - min(data_set),
        "trim": trim,
    }
    if trim:
        results["trim_stdev"] = st_dev
    return results
