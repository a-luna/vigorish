"""Aggregate pitchfx data and play-by-play data into a single ."""
import json
from collections import Counter, defaultdict
from copy import deepcopy
from pprint import pformat

from app.main.constants import TEAM_ID_DICT, PPB_PITCH_LOG_DICT
from app.main.models.player import Player
from app.main.models.status_game import GameScrapeStatus
from app.main.util.file_util import write_json_dict_to_file
from app.main.util.list_functions import compare_lists, flatten_list2d
from app.main.util.result import Result
from app.main.util.s3_helper import get_bbref_boxscore_from_s3, get_all_pitchfx_logs_for_game_from_s3

def update_boxscore_with_pitchfx_data(session, bbref_game_id):
    result = get_all_pbp_events_for_game(session, bbref_game_id)
    if result.failure:
        return result
    (boxscore, player_id_dict, all_pbp_events_for_game) = result.value
    result = get_all_pfx_data_for_game(session, bbref_game_id)
    if result.failure:
        return result
    (pitchfx_logs_for_game, all_pfx_data_for_game) = result.value
    result = reconcile_at_bat_ids(all_pbp_events_for_game, all_pfx_data_for_game)
    if result.failure:
        return result
    at_bat_ids = result.value
    game_events_combined_data = combine_boxscore_and_pitchfx_data(
        all_pbp_events_for_game, all_pfx_data_for_game, at_bat_ids
    )
    result = update_boxscore_with_combined_data(
        boxscore, player_id_dict, game_events_combined_data, pitchfx_logs_for_game
    )
    if result.failure:
        return result
    boxscore_dict = result.value
    boxscore_json = json.dumps(boxscore_dict, indent=2, sort_keys=False)
    result = write_json_dict_to_file(boxscore_json, f"{bbref_game_id}_COMBINED_DATA.json")
    if result.failure:
        return result
    return Result.Ok(f"Successfully combined pbp data and pfx data for game: {bbref_game_id}")


def get_all_pbp_events_for_game(session, bbref_game_id):
    result = get_bbref_boxscore_from_s3(bbref_game_id)
    if result.failure:
        return result
    boxscore = result.value
    result = get_player_id_dict_for_game(session, boxscore)
    if result.failure:
        return result
    player_id_dict = result.value
    all_pbp_events_for_game = []
    for inning in boxscore.innings_list:
        for game_event in inning.game_events:
            game_event.inning_id = inning.inning_id
            game_event.at_bat_id = get_at_bat_id_for_pbp_event(
                session, bbref_game_id, game_event, player_id_dict
            )
        all_pbp_events_for_game.extend(inning.game_events)
    all_pbp_events_for_game.sort(key=lambda x: x.pbp_table_row_number)
    pbp_ids = [event.at_bat_id for event in all_pbp_events_for_game]
    histogram = Counter(pbp_ids)
    unique_pbp_ids = Counter(list(set(pbp_ids)))
    duplicate_pbp_ids = histogram - unique_pbp_ids
    if not duplicate_pbp_ids:
        return Result.Ok((boxscore, player_id_dict, all_pbp_events_for_game))
    duplicate_map = {}
    for at_bat_id in duplicate_pbp_ids.keys():
        pbp_events = [
            event for event in all_pbp_events_for_game if event.at_bat_id == at_bat_id
        ]
        pbp_events.sort(key=lambda x: x.pbp_table_row_number)
        total_events = len(pbp_events)
        rows_apart = []
        for index, pbp_event in enumerate(pbp_events):
            if index != total_events - 1:
                diff = pbp_events[index + 1].pbp_table_row_number - pbp_event.pbp_table_row_number
                rows_apart.append(diff)
        group_number = 1
        group_size = 1
        group_dict = {}
        for diff in rows_apart:
            if diff == 1:
                group_size += 1
                continue
            group_dict[group_number] = group_size
            group_number += 1
            group_size = 1
        group_dict[group_number] = group_size
        event_index = 0
        for group_number, total_events_this_at_bat in group_dict.items():
            instance_number = group_number - 1
            for _ in range(total_events_this_at_bat):
                game_event = pbp_events[event_index]
                game_event.at_bat_id = get_at_bat_id_for_pbp_event(
                    session, bbref_game_id, game_event, player_id_dict, instance_number
                )
                event_index += 1
    return Result.Ok((boxscore, player_id_dict, all_pbp_events_for_game))


def get_player_id_dict_for_game(session, boxscore):
    player_name_dict = boxscore.player_name_dict
    player_team_dict = boxscore.player_team_dict
    player_id_dict = {}
    for name, bbref_id in player_name_dict.items():
        player = Player.find_by_bbref_id(session, bbref_id)
        if not player:
            return Result.Fail(f"Unable to retrieve MLB ID for {name} ({bbref_id})")
        player_id_dict[bbref_id] = {
            "name": name,
            "mlb_id": player.mlb_id,
            "team_id_bbref": player_team_dict.get(bbref_id, "")
        }
    return Result.Ok(player_id_dict)


def get_at_bat_id_for_pbp_event(session, bbref_game_id, game_event, player_id_dict, instance_number=0):
    inning_num = game_event.inning_label[1:]
    team_pitching_id_bb = get_brooks_team_id(game_event.team_pitching_id_br)
    pitcher_id_mlb = player_id_dict[game_event.pitcher_id_br]["mlb_id"]
    team_batting_id_bb = get_brooks_team_id(game_event.team_batting_id_br)
    batter_id_mlb = player_id_dict[game_event.batter_id_br]["mlb_id"]
    return f"{bbref_game_id}_{inning_num}_{team_pitching_id_bb}_{pitcher_id_mlb}_{team_batting_id_bb}_{batter_id_mlb}_{instance_number}"


def get_brooks_team_id(br_team_id):
        if br_team_id in TEAM_ID_DICT:
            return TEAM_ID_DICT[br_team_id]
        return br_team_id


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
        for pfx in pitchfx_log.pitchfx_log:
            pfx.at_bat_id = get_at_bat_id_for_pfx_data(pfx)
        all_pfx_data_for_game.extend(pitchfx_log.pitchfx_log)
    all_at_bat_ids = list(set([pfx.at_bat_id for pfx in all_pfx_data_for_game]))
    for at_bat_id in all_at_bat_ids:
        pfx_for_at_bat = [pfx for pfx in all_pfx_data_for_game if pfx.at_bat_id == at_bat_id]
        pfx_ab_ids_for_at_bat = list(set([pfx.ab_id for pfx in pfx_for_at_bat]))
        if len(pfx_ab_ids_for_at_bat) <= 1:
            continue
        for index, pfx_ab_id in enumerate(sorted(pfx_ab_ids_for_at_bat)):
            pfx_for_separate_at_bat = [pfx for pfx in pfx_for_at_bat if pfx.ab_id == pfx_ab_id]
            for pfx in pfx_for_separate_at_bat:
                instance_number = index + 1
                pfx.at_bat_id = get_at_bat_id_for_pfx_data(pfx, instance_number)
    all_pfx_data_for_game = []
    for pitchfx_log in pitchfx_logs_for_game:
        pitchfx_log.at_bat_ids = list(set([pfx.at_bat_id for pfx in pitchfx_log.pitchfx_log]))
        all_pfx_data_for_game.extend(pitchfx_log.pitchfx_log)
    all_pfx_data_for_game.sort(key=lambda x: (x.ab_id, x.ab_count))
    return Result.Ok((pitchfx_logs_for_game, all_pfx_data_for_game))


def remove_duplicate_pitchfx_data(pitchfx_log, game_start_time):
    pfx_log_copy = deepcopy(pitchfx_log.pitchfx_log)
    pitch_guids = [pfx.play_guid for pfx in pfx_log_copy]
    histogram = Counter(pitch_guids)
    unique_guids = Counter(list(set(pitch_guids)))
    duplicate_guids = histogram - unique_guids
    if not duplicate_guids:
        pitchfx_log.duplicate_pitches_removed_count = 0
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
            dupe_rank_dict[pfx.play_guid].sort(
                key=lambda x: (-x["has_zone_location"], x["time_since_pitch_thrown"])
            )
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
    pfx_log_no_dupes.sort(key=lambda x: (x.ab_id, x.ab_count))
    removed_count = len(pfx_log_copy) - len(pfx_log_no_dupes)
    pitchfx_log.duplicate_pitches_removed_count = removed_count
    pitchfx_log.pitchfx_log = pfx_log_no_dupes
    pitchfx_log.pitch_count_by_inning = get_pitch_count_by_inning(pfx_log_no_dupes)
    pitchfx_log.total_pitch_count = len(pfx_log_no_dupes)
    pfx_log_copy = None
    return pitchfx_log


def get_pitch_count_by_inning(pitchfx_log):
    pitch_count_by_inning = defaultdict(int)
    for pfx in pitchfx_log:
        pitch_count_by_inning[pfx.inning] += 1
    return pitch_count_by_inning


def get_at_bat_id_for_pfx_data(pfx, instance_number=0):
    return f"{pfx.bbref_game_id}_{pfx.inning}_{pfx.pitcher_team_id_bb}_{pfx.pitcher_id}_{pfx.opponent_team_id_bb}_{pfx.batter_id}_{instance_number}"


def reconcile_at_bat_ids(all_pbp_events_for_game, all_pfx_data_for_game):
    at_bat_ids_from_boxscore = list(set([game_event.at_bat_id for game_event in all_pbp_events_for_game]))
    at_bat_ids_from_pfx = list(set([pfx.at_bat_id for pfx in all_pfx_data_for_game]))
    at_bat_ids_match_exactly = compare_lists(at_bat_ids_from_boxscore, at_bat_ids_from_pfx)
    at_bat_ids_boxscore_only = list(set(at_bat_ids_from_boxscore) - set(at_bat_ids_from_pfx))
    at_bat_ids_pfx_only = list(set(at_bat_ids_from_pfx) - set(at_bat_ids_from_boxscore))
    if at_bat_ids_match_exactly or (at_bat_ids_boxscore_only and not at_bat_ids_pfx_only):
        at_bat_ids_ordered = order_at_bat_ids_by_time(at_bat_ids_from_boxscore, all_pbp_events_for_game)
        return Result.Ok(at_bat_ids_ordered)
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


def combine_boxscore_and_pitchfx_data(all_pbp_events_for_game, all_pfx_data_for_game, at_bat_ids):
    game_events_combined_data = []
    for ab_id in at_bat_ids:
        pbp_events_for_at_bat = get_all_pbp_events_for_at_bat(all_pbp_events_for_game, ab_id)
        pfx_data_for_at_bat = get_all_pfx_data_for_at_bat(all_pfx_data_for_game, ab_id)
        first_event_this_at_bat = pbp_events_for_at_bat[0]
        final_event_this_at_bat = pbp_events_for_at_bat[-1]
        pitch_count_pitch_seq = get_total_pitches_in_sequence(final_event_this_at_bat["pitch_sequence"])
        pitch_count_pitchfx = len(pfx_data_for_at_bat)
        if pfx_data_for_at_bat and pitch_count_pitch_seq == pitch_count_pitchfx:
            final_pitch_this_at_bat = pfx_data_for_at_bat[-1]
        else:
            final_pitch_this_at_bat = None
        pitch_sequence_description = get_detailed_pitch_sequence_description(
            final_event_this_at_bat, final_pitch_this_at_bat
        )
        combined_at_bat_data = {
            "event_type": "at_bat",
            "at_bat_id": ab_id,
            "pbp_table_row_number": first_event_this_at_bat["pbp_table_row_number"],
            "pitchfx_data_complete": pitch_count_pitch_seq == pitch_count_pitchfx,
            "pitch_count_bbref_pitch_seq": pitch_count_pitch_seq,
            "pitch_count_pitchfx": pitch_count_pitchfx,
            "missing_pitchfx_count": pitch_count_pitch_seq - pitch_count_pitchfx,
            "pitch_sequence_description": pitch_sequence_description,
            "pbp_events": pbp_events_for_at_bat,
            "pitchfx": pfx_data_for_at_bat
        }
        game_events_combined_data.append(combined_at_bat_data)
    return game_events_combined_data


def get_all_pbp_events_for_at_bat(all_pbp_events_for_game, at_bat_id):
    pbp_events_for_at_bat = [
        game_event.as_dict()
        for game_event in all_pbp_events_for_game
        if game_event.at_bat_id == at_bat_id
    ]
    for game_event in pbp_events_for_at_bat:
        game_event.pop("__bbref_pbp_game_event__", None)
        game_event["at_bat_id"] = at_bat_id
    pbp_events_for_at_bat.sort(key=lambda x: x["pbp_table_row_number"])
    return pbp_events_for_at_bat


def get_all_pfx_data_for_at_bat(all_pfx_data_for_game, at_bat_id):
    pfx_data_for_at_bat = [
        pfx.as_dict()
        for pfx in all_pfx_data_for_game
        if pfx.at_bat_id == at_bat_id
    ]
    for pfx in pfx_data_for_at_bat:
        pfx.pop("__brooks_pitchfx_data__", None)
        pfx["at_bat_id"] = at_bat_id
    pfx_data_for_at_bat.sort(key=lambda x: x["ab_count"])
    return pfx_data_for_at_bat


def get_total_pitches_in_sequence(pitch_sequence):
    return sum(PPB_PITCH_LOG_DICT[abbrev]["pitch_counts"] for abbrev in pitch_sequence)


def get_detailed_pitch_sequence_description(game_event, pfx_data=None):
    total_pitches_in_sequence = get_total_pitches_in_sequence(game_event["pitch_sequence"])
    current_pitch_count = 0
    next_pitch_blocked_by_c = False
    sequence_description = []
    for abbrev in game_event["pitch_sequence"]:
        if abbrev == "*":
            next_pitch_blocked_by_c = True
            continue
        if PPB_PITCH_LOG_DICT[abbrev]["pitch_counts"]:
            current_pitch_count += 1
            space_count = 1
            if total_pitches_in_sequence >= 10 and current_pitch_count < 10:
                space_count = 2
            elif total_pitches_in_sequence >= 10 and current_pitch_count >= 10:
                space_count = 1
            pitch_number = f"Pitch{' '*space_count}{current_pitch_count}/{total_pitches_in_sequence}"
            pitch_description = f"{pitch_number}..: {PPB_PITCH_LOG_DICT[abbrev]['description']}"
            if pfx_data and abbrev == "X":
                pitch_description = f'{pitch_number}..: {pfx_data["pdes"]}'
        else:
            pitch_description = PPB_PITCH_LOG_DICT[abbrev]['description']
        if next_pitch_blocked_by_c:
            pitch_description += " (pitch was blocked by catcher)"
            next_pitch_blocked_by_c = False
        sequence_description.append(pitch_description)
    sequence_description.append(f'Result.....: {game_event["play_description"]}')
    return sequence_description


def update_boxscore_with_combined_data(boxscore, player_id_dict, game_events_combined_data, pitchfx_logs_for_game):
    updated_innings_list = []
    for inning in boxscore.innings_list:
        inning_dict = update_inning_with_combined_data(
            inning, game_events_combined_data, boxscore.player_team_dict
        )
        updated_innings_list.append(inning_dict)

    result = update_pitching_stats_for_both_teams(pitchfx_logs_for_game, boxscore, player_id_dict)
    if result.failure:
        return result
    (home_team_pitching_stats, away_team_pitching_stats) = result.value

    game_meta_info = boxscore.game_meta_info.as_dict()
    game_meta_info.pop("__bbref_boxscore_meta__", None)

    away_team_data = boxscore.away_team_data.as_dict()
    away_team_data.pop("__bbref_boxscore_team_data__", None)
    away_team_data.pop("pitching_stats", None)
    away_team_data["pitching_stats"] = away_team_pitching_stats

    home_team_data = boxscore.home_team_data.as_dict()
    home_team_data.pop("__bbref_boxscore_team_data__", None)
    home_team_data.pop("pitching_stats", None)
    home_team_data["pitching_stats"] = home_team_pitching_stats

    pitchfx_vs_bbref_audit = audit_pitchfx_vs_bbref_data(
        updated_innings_list, home_team_pitching_stats, away_team_pitching_stats
    )
    updated_boxscore = {
        "bbref_game_id": boxscore.bbref_game_id,
        "boxscore_url": boxscore.boxscore_url,
        "game_meta_info": game_meta_info,
        "away_team_data": away_team_data,
        "home_team_data": home_team_data,
        "play_by_play_data": updated_innings_list,
        "pitchfx_vs_bbref_audit": pitchfx_vs_bbref_audit,
        "umpires": boxscore.as_dict()["umpires"],
        "player_id_dict": player_id_dict
    }
    return Result.Ok(updated_boxscore)


def update_inning_with_combined_data(inning, game_events_combined_data, player_team_dict):
    at_bat_ids_this_inning = set([game_event.at_bat_id for game_event in inning.game_events])
    inning_events = [
        event for event in game_events_combined_data
        if event["at_bat_id"] in at_bat_ids_this_inning
    ]
    pitchfx_data_complete = all(event["pitchfx_data_complete"] for event in inning_events)
    total_at_bats_pitchfx_complete = len([event for event in inning_events if event["pitchfx_data_complete"]])
    total_at_bats_missing_pitchfx = len([event for event in inning_events if event["missing_pitchfx_count"] > 0])
    total_at_bats_extra_pitchfx = len([event for event in inning_events if event["missing_pitchfx_count"] < 0])
    pitch_count_bbref_pitch_seq = sum(event["pitch_count_bbref_pitch_seq"] for event in inning_events)
    pitch_count_pitchfx = sum(event["pitch_count_pitchfx"] for event in inning_events)
    pitch_count_missing_pitchfx = sum(event["missing_pitchfx_count"] for event in inning_events)
    at_bat_ids_missing_pitchfx = sorted(list(set(
        event["at_bat_id"] for event in inning_events if not event["pitchfx_data_complete"]
    )))
    if inning.substitutions:
        substitutions_this_inning = [sub.as_dict() for sub in inning.substitutions]
        for sub in substitutions_this_inning:
            sub.pop("__bbref_pbp_in_game_substitution__", None)
            sub["event_type"] = "player_substitution"
            sub["sub_team_id"] = player_team_dict.get(sub["incoming_player_id_br"], None)
        inning_events.extend(substitutions_this_inning)
    inning_events.sort(key=lambda x: x["pbp_table_row_number"])
    inning_totals = {
        "inning_total_runs": inning.inning_total_runs,
        "inning_total_hits": inning.inning_total_hits,
        "inning_total_errors": inning.inning_total_errors,
        "inning_total_left_on_base": inning.inning_total_left_on_base,
        "away_team_runs_after_inning": inning.away_team_runs_after_inning,
        "home_team_runs_after_inning": inning.home_team_runs_after_inning,
    }
    inning_pitchfx_audit = {
        "pitchfx_data_complete": pitchfx_data_complete,
        "total_at_bats_pitchfx_complete": total_at_bats_pitchfx_complete,
        "total_at_bats_missing_pitchfx": total_at_bats_missing_pitchfx,
        "total_at_bats_extra_pitchfx": total_at_bats_extra_pitchfx,
        "pitch_count_bbref_pitch_seq": pitch_count_bbref_pitch_seq,
        "pitch_count_pitchfx": pitch_count_pitchfx,
        "pitch_count_missing_pitchfx": pitch_count_missing_pitchfx,
        "at_bat_ids_missing_pitchfx": at_bat_ids_missing_pitchfx,
    }
    return {
        "inning_id": inning.inning_id,
        "inning_label": inning.inning_label,
        "begin_inning_summary": inning.begin_inning_summary,
        "end_inning_summary": inning.end_inning_summary,
        "inning_totals": inning_totals,
        "inning_pitchfx_audit": inning_pitchfx_audit,
        "inning_events": inning_events
    }


def update_pitching_stats_for_both_teams(pitchfx_logs_for_game, boxscore, player_id_dict):
    updated_pitching_stats = []
    for pfx_log in pitchfx_logs_for_game:
        combined_pitching_stats = update_pitching_stats_with_combined_data(
            pfx_log, boxscore, player_id_dict
        )
        updated_pitching_stats.append(combined_pitching_stats)
    return separate_pitching_stats_by_team(
        updated_pitching_stats,
        boxscore.home_team_data.team_id_br,
        boxscore.away_team_data.team_id_br
    )


def update_pitching_stats_with_combined_data(pfx_log, boxscore, player_id_dict):
    all_bbref_pitch_stats = deepcopy(boxscore.away_team_data.pitching_stats)
    all_bbref_pitch_stats.extend(deepcopy(boxscore.home_team_data.pitching_stats))
    bbref_id = None
    player_pitch_stats = None
    for pitch_stats in all_bbref_pitch_stats:
        check_bbref_id = pitch_stats.player_id_br
        if pfx_log.pitcher_id_mlb != player_id_dict[check_bbref_id]["mlb_id"]:
            continue
        bbref_id = check_bbref_id
        player_pitch_stats = pitch_stats
        break
    bbref_data = player_pitch_stats.as_dict()
    bbref_data.pop("player_id_br", None)
    bbref_data.pop("player_team_id_br", None)
    bbref_data.pop("opponent_team_id_br", None)
    all_bbref_pitch_stats = None
    at_bat_ids = sorted(list(set([pfx.at_bat_id for pfx in pfx_log.pitchfx_log])))
    return {
        "pitcher_name": pfx_log.pitcher_name,
        "pitcher_id_mlb": pfx_log.pitcher_id_mlb,
        "pitcher_id_bbref": bbref_id,
        "pitch_app_id": pfx_log.pitch_app_id,
        "pitcher_team_id_bb": pfx_log.pitcher_team_id_bb,
        "pitcher_team_id_bbref": player_pitch_stats.player_team_id_br,
        "opponent_team_id_bb": pfx_log.opponent_team_id_bb,
        "opponent_team_id_bbref": player_pitch_stats.opponent_team_id_br,
        "bb_game_id": pfx_log.bb_game_id,
        "bbref_game_id": pfx_log.bbref_game_id,
        "batters_faced_bbref": player_pitch_stats.batters_faced,
        "batters_faced_pitchfx": len(at_bat_ids),
        "total_pitch_count_bbref": player_pitch_stats.pitch_count,
        "total_pitch_count_pitchfx": pfx_log.total_pitch_count,
        "at_bat_ids": at_bat_ids,
        "bbref_data": bbref_data,
        "pitchfx_data": {
            "total_pitch_count": pfx_log.total_pitch_count,
            "duplicate_pitches_removed_count": pfx_log.duplicate_pitches_removed_count,
            "pitch_count_by_inning": pfx_log.pitch_count_by_inning,
        }
    }


def separate_pitching_stats_by_team(updated_pitching_stats, home_team_id, away_team_id):
    home_team_pitching_stats = []
    away_team_pitching_stats = []
    for pitching_stats in updated_pitching_stats:
        if pitching_stats["pitcher_team_id_bbref"] == home_team_id:
            home_team_pitching_stats.append(pitching_stats)
        elif pitching_stats["pitcher_team_id_bbref"] == away_team_id:
            away_team_pitching_stats.append(pitching_stats)
        else:
            error = (
                f'Error occurred trying to assign pitching_stats for pitch_app_id {pitching_stats["pitch_app_id"]} '
                f'to either home or away team (home_team_id: {home_team_id}, away_team_id: {away_team_id}, '
                f'pitcher_team_id_bbref: {pitching_stats["pitcher_team_id_bbref"]}'
            )
            return Result.Fail(error)
    return Result.Ok((home_team_pitching_stats, away_team_pitching_stats))


def audit_pitchfx_vs_bbref_data(updated_innings_list, home_team_pitching_stats, away_team_pitching_stats):
    pitchfx_data_complete = all(
        inning["inning_pitchfx_audit"]["pitchfx_data_complete"]
        for inning in updated_innings_list
    )
    total_batters_faced_bbref_home = sum(
        pitch_stats["batters_faced_bbref"] for pitch_stats in home_team_pitching_stats
    )
    total_batters_faced_bbref_away = sum(
        pitch_stats["batters_faced_bbref"] for pitch_stats in away_team_pitching_stats
    )
    total_batters_faced_bbref = total_batters_faced_bbref_home + total_batters_faced_bbref_away
    total_batters_faced_pitchfx_home = sum(
        pitch_stats["batters_faced_pitchfx"] for pitch_stats in home_team_pitching_stats
    )
    total_batters_faced_pitchfx_away = sum(
        pitch_stats["batters_faced_pitchfx"] for pitch_stats in away_team_pitching_stats
    )
    total_batters_faced_pitchfx = total_batters_faced_pitchfx_home + total_batters_faced_pitchfx_away
    total_at_bats_pitchfx_complete = sum(
        inning["inning_pitchfx_audit"]["total_at_bats_pitchfx_complete"]
        for inning in updated_innings_list
    )
    total_at_bats_missing_pitchfx = sum(
        inning["inning_pitchfx_audit"]["total_at_bats_missing_pitchfx"]
        for inning in updated_innings_list
    )
    total_at_bats_extra_pitchfx = sum(
        inning["inning_pitchfx_audit"]["total_at_bats_extra_pitchfx"]
        for inning in updated_innings_list
    )
    total_pitch_count_bbref_stats_table_home = sum(
        pitch_stats["bbref_data"]["pitch_count"] for pitch_stats in home_team_pitching_stats
    )
    total_pitch_count_bbref_stats_table_away = sum(
        pitch_stats["bbref_data"]["pitch_count"] for pitch_stats in away_team_pitching_stats
    )
    total_pitch_count_bbref_stats_table = (
        total_pitch_count_bbref_stats_table_home + total_pitch_count_bbref_stats_table_away
    )
    total_pitch_count_bbref_pitch_seq = sum(
        inning["inning_pitchfx_audit"]["pitch_count_bbref_pitch_seq"]
        for inning in updated_innings_list
    )
    total_pitch_count_pitchfx = sum(
        inning["inning_pitchfx_audit"]["pitch_count_pitchfx"]
        for inning in updated_innings_list
    )
    total_pitch_count_missing_pitchfx = sum(
        inning["inning_pitchfx_audit"]["pitch_count_missing_pitchfx"]
        for inning in updated_innings_list
    )
    at_bat_ids_missing_pitchfx = sorted(list(set(flatten_list2d([
        inning["inning_pitchfx_audit"]["at_bat_ids_missing_pitchfx"]
        for inning in updated_innings_list
    ]))))
    duplicate_pfx_removed_home = sum(
        pitch_stats["pitchfx_data"]["duplicate_pitches_removed_count"]
        for pitch_stats in home_team_pitching_stats
    )
    duplicate_pfx_removed_away = sum(
        pitch_stats["pitchfx_data"]["duplicate_pitches_removed_count"]
        for pitch_stats in away_team_pitching_stats
    )
    total_duplicate_pitchfx_removed = duplicate_pfx_removed_home + duplicate_pfx_removed_away

    return {
        "pitchfx_data_complete": pitchfx_data_complete,
        "total_batters_faced_bbref": total_batters_faced_bbref,
        "total_batters_faced_pitchfx": total_batters_faced_pitchfx,
        "total_at_bats_pitchfx_complete": total_at_bats_pitchfx_complete,
        "total_at_bats_missing_pitchfx": total_at_bats_missing_pitchfx,
        "total_at_bats_extra_pitchfx": total_at_bats_extra_pitchfx,
        "total_pitch_count_bbref_stats_table": total_pitch_count_bbref_stats_table,
        "total_pitch_count_bbref_pitch_seq": total_pitch_count_bbref_pitch_seq,
        "total_pitch_count_pitchfx": total_pitch_count_pitchfx,
        "total_pitch_count_missing_pitchfx": total_pitch_count_missing_pitchfx,
        "at_bat_ids_missing_pitchfx": at_bat_ids_missing_pitchfx,
        "total_duplicate_pitchfx_removed": total_duplicate_pitchfx_removed
    }
