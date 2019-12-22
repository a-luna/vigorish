"""Aggregate pitchfx data and play-by-play data into a single ."""
import json
from collections import Counter, defaultdict
from copy import deepcopy

from app.main.constants import TEAM_ID_DICT
from app.main.models.player import Player
from app.main.models.status_game import GameScrapeStatus
from app.main.util.file_util import write_json_dict_to_file
from app.main.util.list_functions import compare_lists
from app.main.util.result import Result
from app.main.util.s3_helper import get_bbref_boxscore_from_s3, get_all_pitchfx_logs_for_game_from_s3

def get_pitching_stats_for_game(session, bbref_game_id):
    result = get_all_pbp_events_for_game(session, bbref_game_id)
    if result.failure:
        return result
    all_pbp_events_for_game = result.value
    result = get_all_pfx_data_for_game(session, bbref_game_id)
    if result.failure:
        return result
    all_pfx_data_for_game = result.value
    result = reconcile_at_bat_ids(all_pbp_events_for_game, all_pfx_data_for_game)
    if result.failure:
        return result
    at_bat_ids = result.value
    combined_data = combine_at_bat_data(all_pbp_events_for_game, all_pfx_data_for_game, at_bat_ids)
    combined_data_json = json.dumps(combined_data, indent=2, sort_keys=False)
    result = write_json_dict_to_file(combined_data_json, f"{bbref_game_id}_COMBINED_AT_BATS.json")
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
    return Result.Ok(all_pbp_events_for_game)


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
    return Result.Ok(all_pfx_data_for_game)


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
    pitchfx_log.removed_count = removed_count
    pitchfx_log.pitchfx_log = pfx_log_no_dupes
    return pitchfx_log


def reconcile_at_bat_ids(all_pbp_events_for_game, all_pfx_data_for_game):
    at_bat_ids_from_boxscore = list(set([game_event.at_bat_id for game_event in all_pbp_events_for_game]))
    at_bat_ids_from_pfx = list(set([pfx.at_bat_id for pfx in all_pfx_data_for_game]))
    if compare_lists(at_bat_ids_from_boxscore, at_bat_ids_from_pfx):
        return Result.Ok(at_bat_ids_from_boxscore)
    at_bat_ids_boxscore_only = list(set(at_bat_ids_from_boxscore) - set(at_bat_ids_from_pfx))
    at_bat_ids_pfx_only = list(set(at_bat_ids_from_pfx) - set(at_bat_ids_from_boxscore))
    message = "Play-by-play data and PitchFx data could not be reconciled:"
    if at_bat_ids_boxscore_only:
        message += f"\nat_bat_ids found ONLY in play-by-play data: {at_bat_ids_boxscore_only}"
    if at_bat_ids_pfx_only:
        message += f"\nat_bat_ids found ONLY in PitchFx data: {at_bat_ids_pfx_only}"
    return Result.Fail(message)


def combine_at_bat_data(all_pbp_events_for_game, all_pfx_data_for_game, at_bat_ids):
    combined_at_bat_data = {}
    for at_bat_id in at_bat_ids:
        pbp_events_for_at_bat = [
            game_event for game_event
            in all_pbp_events_for_game
            if game_event.at_bat_id == at_bat_id
        ]
        pfx_data_for_at_bat = [
            pfx for pfx
            in all_pfx_data_for_game
            if pfx.at_bat_id == at_bat_id
        ]
        combined_at_bat_data[at_bat_id] = dict(
            pbp_events=pbp_events_for_at_bat, pitchfx=pfx_data_for_at_bat
        )
    return combined_at_bat_data


def get_at_bat_id_for_pbp_event(session, bbref_game_id, game_event):
    inning_num = game_event.inning_label[1:]
    team_pitching_id_bb = get_brooks_team_id(game_event.team_pitching_id_br).lower()
    pitcher_id_mlb = Player.find_by_bbref_id(session, game_event.pitcher_id_br).mlb_id
    team_batting_id_bb = get_brooks_team_id(game_event.team_batting_id_br).lower()
    batter_id_mlb = Player.find_by_bbref_id(session, game_event.batter_id_br).mlb_id
    return f"{bbref_game_id}_{inning_num}_{team_pitching_id_bb}_{pitcher_id_mlb}_{team_batting_id_bb}_{batter_id_mlb}"


def get_brooks_team_id(br_team_id):
        if br_team_id in TEAM_ID_DICT:
            return TEAM_ID_DICT[br_team_id]
        return br_team_id
