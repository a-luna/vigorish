from datetime import datetime, timezone

from vigorish.util.datetime_util import TIME_ZONE_NEW_YORK
from vigorish.util.regex import PFX_TIMESTAMP_REGEX
from vigorish.util.string_helpers import validate_bbref_game_id


def calc_avg_time_between_pitches(combined_data):
    pitch_deltas = []
    inning_deltas = []
    prev_inning_last_pfx = None
    bbref_game_id = combined_data["bbref_game_id"]
    pbp_data = combined_data["play_by_play_data"]
    for half_inning in pbp_data:
        for ab_num, at_bat in enumerate(half_inning["inning_events"], start=1):
            if at_bat["missing_pitchfx_count"]:
                continue
            for pfx_count, pfx in enumerate(at_bat["pitchfx"], start=1):
                if pfx_count == 1 and prev_inning_last_pfx and ab_num == 1:
                    this_pitch = get_timestamp_pitch_thrown(pfx, bbref_game_id)
                    last_pitch = get_timestamp_pitch_thrown(prev_inning_last_pfx, bbref_game_id)
                    delta = this_pitch - last_pitch
                    if delta.total_seconds() > 0:
                        inning_deltas.append(delta.total_seconds())
                if pfx_count == len(at_bat["pitchfx"]):
                    if ab_num == len(half_inning["inning_events"]):
                        prev_inning_last_pfx = pfx
                    continue
                next_pfx = at_bat["pitchfx"][pfx_count]
                next_pitch = get_timestamp_pitch_thrown(next_pfx, bbref_game_id)
                this_pitch = get_timestamp_pitch_thrown(pfx, bbref_game_id)
                delta = next_pitch - this_pitch
                if delta.total_seconds() > 0:
                    pitch_deltas.append(delta.total_seconds())
    pitch_delta = {}
    if len(pitch_deltas):
        pitch_delta = {
            "total": sum(pitch_deltas),
            "count": len(pitch_deltas),
            "avg": sum(pitch_deltas) / len(pitch_deltas),
            "max": max(pitch_deltas),
            "min": min(pitch_deltas),
            "range": max(pitch_deltas) - min(pitch_deltas),
        }
    inning_delta = {}
    if len(inning_deltas):
        inning_delta = {
            "total": sum(inning_deltas),
            "count": len(inning_deltas),
            "avg": sum(inning_deltas) / len(inning_deltas),
            "max": max(inning_deltas),
            "min": min(inning_deltas),
            "range": max(inning_deltas) - min(inning_deltas),
        }
    return (pitch_deltas, pitch_delta, inning_deltas, inning_delta)


def get_timestamp_pitch_thrown(pfx_park_sv_id, bbref_game_id):
    match = PFX_TIMESTAMP_REGEX.match(pfx_park_sv_id)
    if not match:
        return None
    group_dict = match.groupdict()
    game_dict = validate_bbref_game_id(bbref_game_id).value
    timestamp = datetime(
        game_dict["game_date"].year,
        int(group_dict["month"]),
        int(group_dict["day"]),
        int(group_dict["hour"]),
        int(group_dict["minute"]),
        int(group_dict["second"]),
    )
    return timestamp.replace(tzinfo=timezone.utc).astimezone(TIME_ZONE_NEW_YORK)
