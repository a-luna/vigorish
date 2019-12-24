"""Aggregate pitchfx data and play-by-play data into a single ."""
import json
from collections import Counter, defaultdict
from copy import deepcopy

from app.main.constants import TEAM_ID_DICT, PPB_PITCH_LOG_DICT
from app.main.models.player import Player
from app.main.models.status_game import GameScrapeStatus
from app.main.util.file_util import write_json_dict_to_file
from app.main.util.list_functions import compare_lists
from app.main.util.result import Result
from app.main.util.s3_helper import get_bbref_boxscore_from_s3, get_all_pitchfx_logs_for_game_from_s3

def get_pitching_stats_for_game(session, bbref_game_id):
    result = get_all_pbp_events_for_game(session, bbref_game_id)
    (boxscore, all_pbp_events_for_game) = result.value
    result = get_all_pfx_data_for_game(session, bbref_game_id)
    (pitchfx_logs_for_game, all_pfx_data_for_game) = result.value
    result = reconcile_at_bat_ids(all_pbp_events_for_game, all_pfx_data_for_game)
    at_bat_ids_ordered = result.value
    combined_data = combine_boxscore_and_pitchfx_data(
        boxscore,
        all_pbp_events_for_game,
        pitchfx_logs_for_game,
        all_pfx_data_for_game,
        at_bat_ids_ordered
    )
    combined_data_json = json.dumps(combined_data, indent=2, sort_keys=False)
    result = write_json_dict_to_file(combined_data_json, f"{bbref_game_id}_COMBINED_DATA.json")
    if result.failure:
        return result
    return Result.Ok(f"Successfully combined pbp data and pfx data for game: {bbref_game_id}")


def get_all_pbp_events_for_game(session, bbref_game_id):
    result = get_bbref_boxscore_from_s3(bbref_game_id)
    if result.failure:
        return result
    boxscore = result.value
    all_pbp_events_for_game = []
    for inning in boxscore.innings_list:
        for game_event in inning.game_events:
            game_event.at_bat_id = get_at_bat_id_for_pbp_event(session, bbref_game_id, game_event)
        all_pbp_events_for_game.extend(inning.game_events)
    all_pbp_events_for_game.sort(key=lambda x: x.pbp_table_row_number)
    return Result.Ok((boxscore, all_pbp_events_for_game))


def get_all_pfx_data_for_game(session, bbref_game_id):
    result = get_all_pitchfx_logs_for_game_from_s3(session, bbref_game_id)
    if result.failure:
        return result
    pitchfx_logs_for_game = result.value
    game_status = GameScrapeStatus.find_by_bbref_game_id(session, bbref_game_id)
    game_start_time = game_status.game_start_time
    pitchfx_logs_for_game = [
        remove_duplicate_pitchfx_data(pitchfx_log, game_start_time)
        for pitchfx_log in pitchfx_logs_for_game
    ]
    all_pfx_data_for_game = []
    for pitchfx_log in pitchfx_logs_for_game:
        all_pfx_data_for_game.extend(pitchfx_log.pitchfx_log)
    return Result.Ok((pitchfx_logs_for_game, all_pfx_data_for_game))


def reconcile_at_bat_ids(all_pbp_events_for_game, all_pfx_data_for_game):
    at_bat_ids_from_boxscore = list(set([game_event.at_bat_id for game_event in all_pbp_events_for_game]))
    at_bat_ids_from_pfx = list(set([pfx.at_bat_id for pfx in all_pfx_data_for_game]))
    if compare_lists(at_bat_ids_from_boxscore, at_bat_ids_from_pfx):
        at_bat_ids_ordered = order_at_bat_ids_by_time(at_bat_ids_from_pfx, all_pbp_events_for_game)
        return Result.Ok(at_bat_ids_ordered)
    at_bat_ids_boxscore_only = list(set(at_bat_ids_from_boxscore) - set(at_bat_ids_from_pfx))
    at_bat_ids_pfx_only = list(set(at_bat_ids_from_pfx) - set(at_bat_ids_from_boxscore))
    message = "Play-by-play data and PitchFx data could not be reconciled:"
    if at_bat_ids_boxscore_only:
        message += f"\nat_bat_ids found ONLY in play-by-play data: {at_bat_ids_boxscore_only}"
    if at_bat_ids_pfx_only:
        message += f"\nat_bat_ids found ONLY in PitchFx data: {at_bat_ids_pfx_only}"
    return Result.Fail(message)


def order_at_bat_ids_by_time(at_bat_ids, all_pbp_events_for_game):
    game_event_pbp_map = [{
        "at_bat_id": ab_id,
        "pbp_table_row_number": min(
            game_event.pbp_table_row_number
            for game_event in all_pbp_events_for_game
            if game_event.at_bat_id == ab_id
        )}
        for ab_id in at_bat_ids
    ]
    game_event_pbp_map.sort(key=lambda x: x["pbp_table_row_number"])
    return [event_pbp_map["at_bat_id"] for event_pbp_map in game_event_pbp_map]


def remove_duplicate_pitchfx_data(pitchfx_log, game_start_time):
    pfx_log_copy = deepcopy(pitchfx_log.pitchfx_log)
    pitch_guids = [pfx.play_guid for pfx in pfx_log_copy]
    histogram = Counter(pitch_guids)
    unique_guids = Counter(list(set(pitch_guids)))
    duplicate_guids = histogram - unique_guids
    if not duplicate_guids:
        pitchfx_log.removed_count = 0
        return pitchfx_log
    dupe_rank_dict = defaultdict(list)
    dupe_id_map = {}
    for pfx in pfx_log_copy:
        if pfx.play_guid in duplicate_guids:
            pfx_criteria = {}
            time_since_pitch_thrown = pfx.timestamp_pitch_thrown - game_start_time
            pfx_criteria["time_since_pitch_thrown"] = time_since_pitch_thrown.total_seconds()
            pfx_criteria["has_zone_location"] = pfx.has_zone_location
            pfx_criteria["park_sv_id"] = pfx.park_sv_id
            dupe_rank_dict[pfx.play_guid].append(pfx_criteria)
            dupe_rank_dict[pfx.play_guid].sort(key=lambda x: (-x["has_zone_location"], x["time_since_pitch_thrown"]))
            dupe_id_map[pfx.park_sv_id] = pfx
    pfx_log_no_dupes = []
    dupe_tracker = {guid:False for guid in unique_guids.keys()}
    for pfx in pfx_log_copy:
        if dupe_tracker[pfx.play_guid]:
            continue
        if pfx.play_guid in duplicate_guids:
            best_pfx_id = dupe_rank_dict[pfx.play_guid][0]["park_sv_id"]
            pfx_log_no_dupes.append(dupe_id_map[best_pfx_id])
        else:
            pfx_log_no_dupes.append(pfx)
        dupe_tracker[pfx.play_guid] = True
    removed_count = len(pfx_log_copy) - len(pfx_log_no_dupes)
    pitchfx_log.duplicate_pitches_removed_count = removed_count
    pitchfx_log.pitchfx_log = pfx_log_no_dupes
    pitchfx_log.pitch_count_by_inning = get_pitch_count_by_inning(pfx_log_no_dupes)
    pitchfx_log.total_pitch_count = len(pfx_log_copy)
    return pitchfx_log


def get_pitch_count_by_inning(pitchfx_log):
    pitch_count_by_inning = defaultdict(int)
    for pfx in pitchfx_log:
        pitch_count_by_inning[pfx.inning] += 1
    return pitch_count_by_inning


def combine_boxscore_and_pitchfx_data(
    boxscore,
    all_pbp_events_for_game,
    pitchfx_logs_for_game,
    all_pfx_data_for_game,
    at_bat_ids
):
    boxscore_dict = boxscore.as_dict()
    for half_inning in boxscore.innings_list:
        inning_events = []
        at_bat_ids_this_inning = set([game_event.at_bat_id for game_event in half_inning.game_events])
        ordered_at_bat_ids = order_at_bat_ids_by_time(at_bat_ids_this_inning, all_pbp_events_for_game)
        for at_bat_id in ordered_at_bat_ids:
            pbp_events_for_at_bat = [
                game_event.as_dict()
                for game_event in all_pbp_events_for_game
                if game_event.at_bat_id == at_bat_id
            ]
            for game_event in pbp_events_for_at_bat:
                game_event["at_bat_id"] = at_bat_id
            pbp_events_for_at_bat.sort(key=lambda x: x["pbp_table_row_number"])
            pfx_data_for_at_bat = [
                pfx.as_dict() for pfx
                in all_pfx_data_for_game
                if pfx.at_bat_id == at_bat_id
            ]
            for pfx in pfx_data_for_at_bat:
                pfx["at_bat_id"] = at_bat_id
            pfx_data_for_at_bat.sort(key=lambda x: x["ab_count"])
            pitch_count_bbref_pitch_seq = get_pitch_count(pbp_events_for_at_bat[-1]["pitch_sequence"])
            pitch_count_pitchfx = len(pfx_data_for_at_bat)
            pitch_sequence_description = get_pitch_sequence_description(pbp_events_for_at_bat[-1])
            combined_at_bat_data = {
                "event_type": "at_bat",
                "at_bat_id": at_bat_id,
                "pbp_table_row_number": pbp_events_for_at_bat[0]["pbp_table_row_number"],
                "pitchfx_data_for_at_bat_is_complete": pitch_count_bbref_pitch_seq == pitch_count_pitchfx,
                "pitch_count_bbref_pitch_seq": pitch_count_bbref_pitch_seq,
                "pitch_count_pitchfx": pitch_count_pitchfx,
                "total_missing_pitchfx": pitch_count_bbref_pitch_seq - pitch_count_pitchfx,
                "pitch_sequence_description": pitch_sequence_description,
                "pbp_events": pbp_events_for_at_bat,
                "pitchfx": pfx_data_for_at_bat
            }
            inning_events.append(combined_at_bat_data)
        if half_inning.substitutions:
            substitutions_this_inning = [sub.as_dict() for sub in half_inning.substitutions]
            for sub in substitutions_this_inning:
                sub["event_type"] = "player_substitution"
                sub["sub_team_id"] = boxscore.player_team_dict.get(sub["incoming_player_id_br"], None)
            inning_events.extend(substitutions_this_inning)

        inning_events.sort(key=lambda x: x["pbp_table_row_number"])
        at_bats_this_inning = [event for event in inning_events if event["event_type"] == "at_bat"]
        at_bats_with_complete_pitchfx_data_count = len([
            at_bat for at_bat in at_bats_this_inning
            if at_bat["pitchfx_data_for_at_bat_is_complete"] == True
        ])
        at_bats_missing_pitchfx_data_count = len([
            at_bat for at_bat in at_bats_this_inning
            if at_bat["total_missing_pitchfx"] > 0
        ])
        at_bats_extra_pitchfx_data_count = len([
            at_bat for at_bat in at_bats_this_inning
            if at_bat["total_missing_pitchfx"] < 0
        ])
        total_pitch_count_bbref_pitch_seq = sum(
            at_bat["pitch_count_bbref_pitch_seq"] for at_bat in at_bats_this_inning
        )
        total_pitch_count_pitchfx = sum(at_bat["pitch_count_pitchfx"] for at_bat in at_bats_this_inning)
        total_missing_pitchfx = sum(at_bat["total_missing_pitchfx"] for at_bat in at_bats_this_inning)

        inning_dict = [
            inning for inning in boxscore_dict["innings_list"]
            if inning["inning_id"] == half_inning.inning_id
        ][0]
        inning_dict["at_bats_with_complete_pitchfx_data_count"] = at_bats_with_complete_pitchfx_data_count
        inning_dict["at_bats_missing_pitchfx_data_count"] = at_bats_missing_pitchfx_data_count
        inning_dict["at_bats_extra_pitchfx_data_count"] = at_bats_extra_pitchfx_data_count
        inning_dict["total_pitch_count_bbref_pitch_seq"] = total_pitch_count_bbref_pitch_seq
        inning_dict["total_pitch_count_pitchfx"] = total_pitch_count_pitchfx
        inning_dict["total_missing_pitchfx"] = total_missing_pitchfx
        inning_dict["inning_events"] = inning_events
        inning_dict.pop("game_events", None)
        inning_dict.pop("substitutions", None)
    pitchfx_log_dicts = [pfx_log.as_dict() for pfx_log in pitchfx_logs_for_game]
    for pfx_log_dict in pitchfx_log_dicts:
        pfx_log_dict.pop("pitchfx_log", None)
    pitch_count_by_inning = {}
    all_at_bats = []
    for inning in boxscore_dict["innings_list"]:
        pitch_count_by_pitcher_id = defaultdict(int)
        at_bats_this_inning = [
            event for event in inning["inning_events"]
            if event["event_type"] == "at_bat"
        ]
        for event in at_bat_ids_this_inning:
            for pfx in event["pitchfx"]:
                pitch_count_by_pitcher_id[pfx.pitcher_id] += 1
        pitch_count_by_inning[inning.inning_label] = pitch_count_by_pitcher_id
        all_at_bats.extend(at_bats_this_inning)
    at_bats_with_complete_pitchfx_data_count = len([
        at_bat for at_bat in all_at_bats
        if at_bat["pitchfx_data_for_at_bat_is_complete"] == True
    ])
    at_bats_missing_pitchfx_data_count = len([
        at_bat for at_bat in all_at_bats
        if at_bat["total_missing_pitchfx"] > 0
    ])
    at_bats_extra_pitchfx_data_count = len([
        at_bat for at_bat in all_at_bats
        if at_bat["total_missing_pitchfx"] < 0
    ])
    total_pitch_count_bbref_pitch_seq = sum(
        at_bat["pitch_count_bbref_pitch_seq"] for at_bat in all_at_bats
    )
    total_pitch_count_pitchfx = sum(at_bat["pitch_count_pitchfx"] for at_bat in all_at_bats)
    total_missing_pitchfx = sum(at_bat["total_missing_pitchfx"] for at_bat in all_at_bats)
    boxscore_dict["pitchfx_logs"] = pitchfx_log_dicts
    boxscore_dict["pitch_count_by_inning"] = pitch_count_by_inning
    boxscore_dict["at_bats_with_complete_pitchfx_data_count"] = at_bats_with_complete_pitchfx_data_count
    boxscore_dict["at_bats_missing_pitchfx_data_count"] = at_bats_missing_pitchfx_data_count
    boxscore_dict["at_bats_extra_pitchfx_data_count"] = at_bats_extra_pitchfx_data_count
    boxscore_dict["total_pitch_count_bbref_pitch_seq"] = total_pitch_count_bbref_pitch_seq
    boxscore_dict["total_pitch_count_pitchfx"] = total_pitch_count_pitchfx
    boxscore_dict["total_missing_pitchfx"] = total_missing_pitchfx
    return boxscore_dict


def get_at_bat_id_for_pbp_event(session, bbref_game_id, game_event):
    inning_num = game_event.inning_label[1:]
    team_pitching_id_bb = get_brooks_team_id(game_event.team_pitching_id_br)
    pitcher_id_mlb = Player.find_by_bbref_id(session, game_event.pitcher_id_br).mlb_id
    team_batting_id_bb = get_brooks_team_id(game_event.team_batting_id_br)
    batter_id_mlb = Player.find_by_bbref_id(session, game_event.batter_id_br).mlb_id
    return f"{bbref_game_id}_{inning_num}_{team_pitching_id_bb}_{pitcher_id_mlb}_{team_batting_id_bb}_{batter_id_mlb}"


def get_brooks_team_id(br_team_id):
        if br_team_id in TEAM_ID_DICT:
            return TEAM_ID_DICT[br_team_id]
        return br_team_id


def get_pitch_sequence_description(game_event):
    pitch_sequence = game_event["pitch_sequence"]
    total_pitches_in_sequence = get_pitch_count(pitch_sequence)
    current_pitch_count = 0
    sequence_description = []
    for abbrev in pitch_sequence:
        if PPB_PITCH_LOG_DICT[abbrev]["pitch_counts"]:
            current_pitch_count += 1
            pitch_number = f"Pitch {current_pitch_count}/{total_pitches_in_sequence}"
            pitch_description = f"{pitch_number}..: {PPB_PITCH_LOG_DICT[abbrev]['description']}"
            sequence_description.append(pitch_description)
        else:
            sequence_description.append(PPB_PITCH_LOG_DICT[abbrev]['description'])
    sequence_description.append(f'Result.....: {game_event["play_description"]}')
    return sequence_description


def get_pitch_count(pitch_sequence):
    return sum(PPB_PITCH_LOG_DICT[abbrev]["pitch_counts"] for abbrev in pitch_sequence)
