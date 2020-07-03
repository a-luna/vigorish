"""Aggregate pitchfx data and play-by-play data into a single object."""
from collections import Counter, defaultdict
from copy import deepcopy
from datetime import datetime, timezone
from typing import Dict, List

from vigorish.constants import (
    TEAM_ID_DICT,
    PPB_PITCH_LOG_DICT,
    PITCH_TYPE_DICT,
)
from vigorish.config.database import Player, GameScrapeStatus
from vigorish.scrape.bbref_boxscores.models.boxscore import BBRefBoxscore
from vigorish.scrape.brooks_pitchfx.models.pitchfx_log import BrooksPitchFxLog
from vigorish.scrape.brooks_pitchfx.models.pitchfx import BrooksPitchFxData
from vigorish.scrape.mlb_player_info.scrape_mlb_player_info import scrape_mlb_player_info
from vigorish.util.datetime_util import TIME_ZONE_NEW_YORK
from vigorish.util.list_helpers import compare_lists, flatten_list2d, report_dict
from vigorish.util.string_helpers import validate_at_bat_id
from vigorish.util.regex import PFX_TIMESTAMP_REGEX
from vigorish.util.result import Result


class CombineScrapedData:
    boxscore: BBRefBoxscore
    player_id_dict: Dict = {}
    at_bat_ids: List[str] = []
    at_bat_event_groups: Dict = {}
    pitchfx_logs_for_game: List[BrooksPitchFxLog] = []
    all_pfx_data_for_game: List[BrooksPitchFxData] = []
    game_events_combined_data: List[Dict] = []
    updated_boxscore_dict: Dict = {}

    def __init__(self, db_session):
        self.db_session = db_session

    def execute(self, bbref_boxscore, pitchfx_logs_for_game, avg_pitch_times):
        self.bbref_game_id = bbref_boxscore.bbref_game_id
        self.boxscore = bbref_boxscore
        self.pitchfx_logs_for_game = pitchfx_logs_for_game
        self.avg_pitch_times = avg_pitch_times
        return (
            self.get_all_pbp_events_for_game()
            .on_success(self.get_all_pfx_data_for_game)
            .on_success(self.combine_pbp_events_with_pfx_data)
            .on_success(self.update_boxscore_with_combined_data)
        )

    def generate_investigative_materials(self, bbref_boxscore, pitchfx_logs_for_game):
        self.bbref_game_id = bbref_boxscore.bbref_game_id
        self.boxscore = bbref_boxscore
        self.pitchfx_logs_for_game = pitchfx_logs_for_game
        result = self.get_all_pbp_events_for_game()
        if result.failure:
            return result
        result = self.get_all_pfx_data_for_game()
        if result.failure:
            return result
        result = self.reconcile_at_bat_ids()
        if result.success:
            return Result.Fail("")
        game_data = {
            "error_message": result.error,
            "boxscore": self.boxscore,
            "player_id_dict": self.player_id_dict,
            "at_bat_event_groups": self.at_bat_event_groups,
            "pitchfx_logs_for_game": self.pitchfx_logs_for_game,
            "all_pfx_data_for_game": self.all_pfx_data_for_game,
        }
        return Result.Ok(game_data)

    def get_all_pbp_events_for_game(self):
        result = self.get_player_id_dict_for_game()
        if result.failure:
            return result
        all_events = self.get_all_events()
        at_bat_events = []

        for game_event in all_events:
            if at_bat_events and game_event.inning_label != at_bat_events[-1].inning_label:
                prev_event = at_bat_events[-1]
                self.update_at_bat_event_groups(prev_event, at_bat_events)
                at_bat_events = []
            at_bat_events.append(game_event)
            if self.event_is_player_substitution_or_misc(game_event):
                continue
            if game_event.pitch_sequence:
                game_event.pitch_sequence = game_event.pitch_sequence.replace("^", "")
                result = self.pitch_sequence_is_complete_at_bat(
                    game_event.pitch_sequence, game_event.pbp_table_row_number
                )
                if result.failure:
                    return result
                game_event_is_complete_at_bat = result.value
            elif "balk" in game_event.play_description.lower():
                game_event_is_complete_at_bat = False
            else:
                row_num = game_event.pbp_table_row_number
                error = f"Error! No pitch sequence was found for row# {row_num}"
                return Result.Fail(error)
            if game_event_is_complete_at_bat or self.event_resulted_in_third_out(game_event):
                self.add_new_at_bat_event_group(game_event, at_bat_events)
                at_bat_events = []
        if at_bat_events:
            prev_event = at_bat_events[-1]
            self.update_at_bat_event_groups(prev_event, at_bat_events)
        return Result.Ok()

    def get_player_id_dict_for_game(self):
        player_name_dict = self.boxscore.player_name_dict
        player_team_dict = self.boxscore.player_team_dict
        for name, bbref_id in player_name_dict.items():
            player = Player.find_by_bbref_id(self.db_session, bbref_id)
            if not player:
                result = scrape_mlb_player_info(
                    self.db_session, name, bbref_id, self.boxscore.game_date
                )
                if result.failure:
                    return result
                player = result.value
            self.player_id_dict[bbref_id] = {
                "name": name,
                "mlb_id": player.mlb_id,
                "team_id_bbref": player_team_dict.get(bbref_id, ""),
            }
        return Result.Ok()

    def get_all_events(self):
        game_events = [
            event for event in [inning.game_events for inning in self.boxscore.innings_list]
        ]
        substitutions = [
            sub for sub in [inning.substitutions for inning in self.boxscore.innings_list]
        ]
        misc_events = [
            misc for misc in [inning.misc_events for inning in self.boxscore.innings_list]
        ]
        all_events = flatten_list2d(game_events + substitutions + misc_events)
        all_events.sort(key=lambda x: x.pbp_table_row_number)
        return all_events

    def update_at_bat_event_groups(self, game_event, at_bat_events):
        return (
            self.update_existing_at_bat_event_group(game_event, at_bat_events)
            if self.event_is_player_substitution_or_misc(game_event)
            else self.add_new_at_bat_event_group(game_event, at_bat_events)
        )

    def update_existing_at_bat_event_group(self, game_event, at_bat_events):
        prev_at_bat_id = self.at_bat_ids[-1]
        self.at_bat_event_groups[prev_at_bat_id].append(
            self.create_bbref_game_event_dict(game_event, prev_at_bat_id)
        )

    def add_new_at_bat_event_group(self, game_event, at_bat_events):
        at_bat_id = self.get_new_at_bat_id(game_event)
        self.at_bat_ids.append(at_bat_id)
        self.at_bat_event_groups[at_bat_id] = [
            self.create_bbref_game_event_dict(event, at_bat_id) for event in at_bat_events
        ]

    def event_is_player_substitution_or_misc(self, event):
        return "BBRefInGameSubstitution" in str(type(event)) or "BBRefPlayByPlayMiscEvent" in str(
            type(event)
        )

    def pitch_sequence_is_complete_at_bat(self, pitch_seq, row_num):
        last_pitch = pitch_seq[-1]
        if last_pitch in ["X", "H", "Y"]:
            return Result.Ok(True)
        balls = 0
        strikes = 0
        for pitch in pitch_seq:
            if pitch in ["C", "S", "T", "K", "L", "M", "O", "Q"]:
                strikes += 1
            if pitch in ["F", "R"] and strikes < 2:
                strikes += 1
            if pitch in ["B", "I", "P", "V"]:
                balls += 1
            if pitch in ["U"]:
                error = (
                    f"Error! Unknown pitch type occurred in sequence: {pitch_seq} "
                    f"(row# {row_num}, game_id: {self.bbref_game_id})"
                )
                return Result.Fail(error)
        return Result.Ok(True) if strikes == 3 or balls == 4 else Result.Ok(False)

    def event_resulted_in_third_out(self, game_event):
        return "O" in game_event.runs_outs_result and game_event.outs_before_play == 2

    def get_new_at_bat_id(self, game_event):
        instance_num = 0
        at_bat_id = self.get_at_bat_id_for_pbp_event(game_event, instance_num)
        id_exists = at_bat_id in self.at_bat_ids
        while id_exists:
            instance_num += 1
            at_bat_id = self.get_at_bat_id_for_pbp_event(game_event, instance_num)
            id_exists = at_bat_id in self.at_bat_ids
        return at_bat_id

    def create_bbref_game_event_dict(self, event, at_bat_id):
        event_dict = event.as_dict()
        event_dict.pop("__bbref_pbp_game_event__", None)
        event_dict.pop("__bbref_pbp_misc_event__", None)
        event_dict.pop("__bbref_pbp_in_game_substitution__", None)
        event_dict["at_bat_id"] = at_bat_id
        event_dict["event_type"] = event.event_type.name
        return event_dict

    def get_at_bat_id_for_pbp_event(self, game_event, instance_number=0):
        inn = game_event.inning_label[1:]
        pteam = self.get_brooks_team_id(game_event.team_pitching_id_br)
        pid = self.player_id_dict[game_event.pitcher_id_br]["mlb_id"]
        bteam = self.get_brooks_team_id(game_event.team_batting_id_br)
        bid = self.player_id_dict[game_event.batter_id_br]["mlb_id"]
        return f"{self.bbref_game_id}_{inn}_{pteam}_{pid}_{bteam}_{bid}_{instance_number}"

    def get_brooks_team_id(self, br_team_id):
        if br_team_id in TEAM_ID_DICT:
            return TEAM_ID_DICT[br_team_id]
        return br_team_id

    def get_all_pfx_data_for_game(self):
        game_status = GameScrapeStatus.find_by_bbref_game_id(self.db_session, self.bbref_game_id)
        game_start_time = game_status.game_start_time
        self.pitchfx_logs_for_game = [
            self.remove_duplicate_pitchfx_data(pitchfx_log, game_start_time)
            for pitchfx_log in self.pitchfx_logs_for_game
        ]
        for pitchfx_log in self.pitchfx_logs_for_game:
            for pfx in pitchfx_log.pitchfx_log:
                pfx.at_bat_id = self.get_at_bat_id_for_pfx_data(pfx)
            self.all_pfx_data_for_game.extend(pitchfx_log.pitchfx_log)
        all_at_bat_ids = list(set([pfx.at_bat_id for pfx in self.all_pfx_data_for_game]))
        for at_bat_id in all_at_bat_ids:
            pfx_for_at_bat = [
                pfx for pfx in self.all_pfx_data_for_game if pfx.at_bat_id == at_bat_id
            ]
            pfx_ab_ids_for_at_bat = list(set([pfx.ab_id for pfx in pfx_for_at_bat]))
            if len(pfx_ab_ids_for_at_bat) <= 1:
                continue
            for instance_number, pfx_ab_id in enumerate(sorted(pfx_ab_ids_for_at_bat)):
                pfx_for_separate_at_bat = [pfx for pfx in pfx_for_at_bat if pfx.ab_id == pfx_ab_id]
                for pfx in pfx_for_separate_at_bat:
                    pfx.at_bat_id = self.get_at_bat_id_for_pfx_data(pfx, instance_number)
        self.all_pfx_data_for_game = []
        for pitchfx_log in self.pitchfx_logs_for_game:
            pitchfx_log.at_bat_ids = list(set([pfx.at_bat_id for pfx in pitchfx_log.pitchfx_log]))
            self.all_pfx_data_for_game.extend(pitchfx_log.pitchfx_log)
        self.all_pfx_data_for_game.sort(key=lambda x: (x.ab_id, x.ab_count))
        return Result.Ok()

    def remove_duplicate_pitchfx_data(self, pitchfx_log, game_start):
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
                pfx_criteria["seconds_since_pitch_thrown"] = pfx.seconds_since_pitch_thrown(
                    game_start
                )
                pfx_criteria["has_zone_location"] = pfx.has_zone_location
                pfx_criteria["park_sv_id"] = pfx.park_sv_id
                dupe_rank_dict[pfx.play_guid].append(pfx_criteria)
                dupe_rank_dict[pfx.play_guid].sort(
                    key=lambda x: (-x["has_zone_location"], x["seconds_since_pitch_thrown"])
                )
                dupe_id_map[pfx.park_sv_id] = pfx
        pfx_log_no_dupes = []
        dupe_tracker = {guid: False for guid in unique_guids.keys()}
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
        pitchfx_log.duplicate_pitches_removed_count = len(pfx_log_copy) - len(pfx_log_no_dupes)
        pitchfx_log.pitchfx_log = pfx_log_no_dupes
        pitchfx_log.pitch_count_by_inning = self.get_pitch_count_by_inning(pfx_log_no_dupes)
        pitchfx_log.total_pitch_count = len(pfx_log_no_dupes)
        pfx_log_copy = None
        return pitchfx_log

    def get_pitch_count_by_inning(self, pitchfx_log):
        pitch_count_by_inning = defaultdict(int)
        for pfx in pitchfx_log:
            pitch_count_by_inning[pfx.inning] += 1
        return pitch_count_by_inning

    def get_at_bat_id_for_pfx_data(self, pfx, instance_number=0):
        game_id = pfx.bbref_game_id
        inn = pfx.inning
        pteam = pfx.pitcher_team_id_bb
        pid = pfx.pitcher_id
        bteam = pfx.opponent_team_id_bb
        bid = pfx.batter_id
        return f"{game_id}_{inn}_{pteam}_{pid}_{bteam}_{bid}_{instance_number}"

    def combine_pbp_events_with_pfx_data(self):
        result = self.reconcile_at_bat_ids()
        if result.failure:
            return result
        self.at_bat_ids = result.value
        for ab_id in self.at_bat_ids:
            pbp_events_for_at_bat = self.get_all_pbp_events_for_at_bat(ab_id)
            pfx_data_for_at_bat = self.get_all_pfx_data_for_at_bat(ab_id)
            first_event_this_at_bat = pbp_events_for_at_bat[0]
            final_event_this_at_bat = pbp_events_for_at_bat[-1]
            pitch_count_pitch_seq = self.get_total_pitches_in_sequence(
                final_event_this_at_bat["pitch_sequence"]
            )
            pitch_count_pitchfx = len(pfx_data_for_at_bat)
            missing_pitchfx_count = pitch_count_pitch_seq - pitch_count_pitchfx
            incorrect_extra_pitchfx_removed_count = 0
            missing_pitchfx_is_valid = True
            error_message = ""
            missing_pitch_numbers = []
            if missing_pitchfx_count > 0:
                result = self.check_pfx_data_for_at_bat(pfx_data_for_at_bat, pitch_count_pitch_seq)
                if result.failure:
                    missing_pitchfx_is_valid = False
                    error_message = result.error
                else:
                    missing_pitch_numbers = result.value
                    missing_pfx_matches = missing_pitchfx_count == len(missing_pitch_numbers)
                    missing_pitchfx_is_valid = missing_pfx_matches
            if missing_pitchfx_count < 0:
                result = self.find_pfx_out_of_sequence(
                    ab_id, pfx_data_for_at_bat, pitch_count_pitch_seq
                )
                if result.failure:
                    missing_pitchfx_is_valid = False
                    error_message = result.error
                else:
                    incorrect_pitch_count = len(pfx_data_for_at_bat)
                    pfx_data_for_at_bat = result.value
                    pitch_count_pitchfx = len(pfx_data_for_at_bat)
                    missing_pitchfx_count = pitch_count_pitch_seq - pitch_count_pitchfx
                    incorrect_extra_pitchfx_removed_count = (
                        incorrect_pitch_count - pitch_count_pitchfx
                    )
            if pfx_data_for_at_bat and not missing_pitchfx_count:
                pfx_data_copy = deepcopy(pfx_data_for_at_bat)
            else:
                pfx_data_copy = None
            pitch_sequence_description = self.construct_pitch_sequence_description(
                final_event_this_at_bat, pfx_data_copy
            )
            pitcher_name = self.player_id_dict[first_event_this_at_bat["pitcher_id_br"]].get(
                "name", ""
            )
            batter_name = self.player_id_dict[first_event_this_at_bat["batter_id_br"]].get(
                "name", ""
            )
            combined_at_bat_data = {
                "at_bat_id": ab_id,
                "inning_id": first_event_this_at_bat["inning_id"],
                "pbp_table_row_number": first_event_this_at_bat["pbp_table_row_number"],
                "pitcher_name": pitcher_name,
                "batter_name": batter_name,
                "pitch_sequence_description": pitch_sequence_description,
                "pitch_count_bbref_pitch_seq": pitch_count_pitch_seq,
                "pitch_count_pitchfx": pitch_count_pitchfx,
                "missing_pitchfx_count": missing_pitchfx_count,
                "missing_pitchfx_is_valid": missing_pitchfx_is_valid,
                "error_message": error_message,
                "missing_pitch_numbers": missing_pitch_numbers,
                "incorrect_extra_pitchfx_removed_count": incorrect_extra_pitchfx_removed_count,
                "pbp_events": self.at_bat_event_groups[ab_id],
                "pitchfx": pfx_data_for_at_bat,
            }
            self.game_events_combined_data.append(combined_at_bat_data)
        return Result.Ok()

    def reconcile_at_bat_ids(self):
        at_bat_ids_from_boxscore = list(
            set([at_bat_id for at_bat_id in self.at_bat_event_groups.keys()])
        )
        at_bat_ids_from_pfx = list(set([pfx.at_bat_id for pfx in self.all_pfx_data_for_game]))
        at_bat_ids_match_exactly = compare_lists(at_bat_ids_from_boxscore, at_bat_ids_from_pfx)
        at_bat_ids_boxscore_only = list(set(at_bat_ids_from_boxscore) - set(at_bat_ids_from_pfx))
        at_bat_ids_pfx_only = list(set(at_bat_ids_from_pfx) - set(at_bat_ids_from_boxscore))
        if at_bat_ids_match_exactly or (at_bat_ids_boxscore_only and not at_bat_ids_pfx_only):
            at_bat_ids = self.order_at_bat_ids_by_time(at_bat_ids_from_boxscore)
            return Result.Ok(at_bat_ids)
        error_report = self.create_error_report(at_bat_ids_pfx_only, True)
        if at_bat_ids_boxscore_only:
            boxscore_errors = self.create_error_report(at_bat_ids_boxscore_only, False)
            error_report += f"\n\n{'=' * 60}\n{boxscore_errors}"
        return Result.Fail(error_report)

    def create_error_report(self, missing_at_bat_ids, ids_are_pfx_only):
        at_bat_data = [validate_at_bat_id(ab_id).value for ab_id in missing_at_bat_ids]
        for at_bat in at_bat_data:
            pitcher_id_data = self.get_player_id_data(at_bat["pitcher_mlb_id"])
            at_bat["pitcher_bbref_id"] = pitcher_id_data[0]
            at_bat["pitcher_name"] = pitcher_id_data[1]
            batter_id_data = self.get_player_id_data(at_bat["batter_mlb_id"])
            at_bat["batter_bbref_id"] = batter_id_data[0]
            at_bat["batter_name"] = batter_id_data[1]
        missing_at_bat_pitch_app_ids = list(set(ab["pitch_app_id"] for ab in at_bat_data))
        summary = (
            f'\n{len(missing_at_bat_ids)} at bat{"s" if len(missing_at_bat_ids) > 1 else ""} from '
            f'{"PitchFX" if ids_are_pfx_only else "BBRef boxscore"}, NOT found in '
            f'{"BBRef boxscore" if ids_are_pfx_only else "PitchFX"}'
        )
        if len(missing_at_bat_ids) > 1:
            summary += (
                f'\n(at bat{"s are" if len(missing_at_bat_ids) > 1 else " is"} from '
                f"{len(missing_at_bat_pitch_app_ids)} pithing appearance"
                f'{"s" if len(missing_at_bat_ids) > 1 else ""})\n'
            )
        detail = "\n\n".join(report_dict(at_bat) for at_bat in at_bat_data)
        return f"{summary}\n{detail}"

    def get_player_id_data(self, mlb_id):
        player_id_data = [
            (bbref_id, id_dict["name"])
            for bbref_id, id_dict in self.player_id_dict.items()
            if id_dict["mlb_id"] == int(mlb_id)
        ]
        return player_id_data[0] if player_id_data else ("", "")

    def order_at_bat_ids_by_time(self, at_bat_ids):
        game_event_pbp_map = [
            {
                "at_bat_id": ab_id,
                "pbp_table_row_number": min(
                    game_event["pbp_table_row_number"]
                    for game_event in self.at_bat_event_groups[ab_id]
                ),
            }
            for ab_id in at_bat_ids
        ]
        game_event_pbp_map.sort(key=lambda x: x["pbp_table_row_number"])
        return [event_pbp_map["at_bat_id"] for event_pbp_map in game_event_pbp_map]

    def get_all_pbp_events_for_at_bat(self, at_bat_id):
        at_bat_events = [
            event
            for event in self.at_bat_event_groups[at_bat_id]
            if event["event_type"] == "AT_BAT"
        ]
        at_bat_events.sort(key=lambda x: x["pbp_table_row_number"])
        return at_bat_events

    def get_all_pfx_data_for_at_bat(self, at_bat_id):
        pfx_data_for_at_bat = [
            pfx.as_dict() for pfx in self.all_pfx_data_for_game if pfx.at_bat_id == at_bat_id
        ]
        for pfx in pfx_data_for_at_bat:
            pfx.pop("__brooks_pitchfx_data__", None)
            pfx["at_bat_id"] = at_bat_id
        pfx_data_for_at_bat.sort(key=lambda x: x["ab_count"])
        return pfx_data_for_at_bat

    def get_total_pitches_in_sequence(self, pitch_sequence):
        return sum(PPB_PITCH_LOG_DICT[abbrev]["pitch_counts"] for abbrev in pitch_sequence)

    def check_pfx_data_for_at_bat(self, pfx_data, pitch_count):
        pitch_count_error = any(pfx["ab_total"] != pitch_count for pfx in pfx_data)
        if pitch_count_error:
            return Result.Fail("Pitch count does not match")
        pfx_pitch_numbers = []
        for pfx in pfx_data:
            if pfx["ab_count"] in pfx_pitch_numbers:
                error = (
                    f'PitchFX data is invalid, pitch #{pfx["ab_count"]} ' "occurs more than once"
                )
                return Result.Fail(error)
            pfx_pitch_numbers.append(pfx["ab_count"])
        all_pitch_numbers = set(range(1, pitch_count + 1))
        missing_pitch_numbers = list(all_pitch_numbers.difference(pfx_pitch_numbers))
        return Result.Ok(missing_pitch_numbers)

    def find_pfx_out_of_sequence(
        self, at_bat_id, pfx_data_for_at_bat, pitch_count_pitch_seq,
    ):
        ab_index = self.at_bat_ids.index(at_bat_id)
        if ab_index == 0:
            return self.find_pfx_out_of_sequence_first_at_bat(
                at_bat_id, pfx_data_for_at_bat, pitch_count_pitch_seq
            )
        prev_ab_id = self.at_bat_ids[ab_index - 1]
        pfx_data_for_prev_at_bat = self.get_all_pfx_data_for_at_bat(prev_ab_id)
        matches = [pfx for pfx in pfx_data_for_prev_at_bat if pfx["ab_count"] == pfx["ab_total"]]
        if not matches:
            error = (
                f"Unable to determine correct pitch sequence for at bat {at_bat_id}. No PitchFX "
                "data was found for the last pitch of the previous at bat which is required to "
                "validate PitchFX data for the next at bat."
            )
            return Result.Fail(error)
        if len(matches) == 1:
            last_pfx_prev_at_bat = matches[0]
        else:
            # TODO: Implement function to choose best pfx for last pitch of ab
            last_pfx_prev_at_bat = matches[0]
        prev_pitch_thrown = self.get_timestamp_pitch_thrown(last_pfx_prev_at_bat)

        valid_pfx_sequence_for_at_bat = []
        for pitch_num in range(1, pitch_count_pitch_seq + 1):
            matches = [pfx for pfx in pfx_data_for_at_bat if pfx["ab_count"] == pitch_num]
            if not matches:
                error = (
                    f"Unable to determine correct pitch sequence for at bat {at_bat_id} "
                    f"(PitchFX contains 0 pitches that are identified as Pitch #{pitch_num})"
                )
                return Result.Fail(error)
            if len(matches) == 1:
                valid_pfx_sequence_for_at_bat.append(matches[0])
                prev_pitch_thrown = self.get_timestamp_pitch_thrown(matches[0])
            else:
                result = self.determine_best_pfx_from_prev_pitch(
                    at_bat_id, matches, pitch_num, prev_pitch_thrown
                )
                if result.failure:
                    return result
                best_pfx = result.value
                valid_pfx_sequence_for_at_bat.append(best_pfx)
                prev_pitch_thrown = self.get_timestamp_pitch_thrown(best_pfx)
        return Result.Ok(valid_pfx_sequence_for_at_bat)

    def find_pfx_out_of_sequence_first_at_bat(
        self, at_bat_id, pfx_data_for_at_bat, pitch_count_pitch_seq
    ):
        pfx_data_for_next_at_bat = self.get_all_pfx_data_for_at_bat(at_bat_id)
        matches = [pfx for pfx in pfx_data_for_next_at_bat if pfx["ab_count"] == 1]
        if not matches:
            error = (
                f"Unable to determine correct pitch sequence for at bat {at_bat_id}, which "
                "is the first at bat of the game. No PitchFX data was found for the first "
                "pitch of the second at bat which is required to validate PitchFX data for "
                "the previous at bat."
            )
            return Result.Fail(error)
        if len(matches) == 1:
            first_pitch_next_at_bat = matches[0]
        else:
            # TODO: Add function to choose best pfx for next ab where only one valid pfx exists
            first_pitch_next_at_bat = matches[0]
        next_pitch_thrown = self.get_timestamp_pitch_thrown(first_pitch_next_at_bat)

        valid_pfx_sequence_for_at_bat = []
        for pitch_num in reversed(range(1, pitch_count_pitch_seq + 1)):
            matches = [pfx for pfx in pfx_data_for_at_bat if pfx["ab_count"] == pitch_num]
            if not matches:
                error = (
                    f"Unable to determine correct pitch sequence for at bat {at_bat_id} "
                    f"(PitchFX contains 0 pitches that are identified as Pitch #{pitch_num})"
                )
                return Result.Fail(error)
            if len(matches) == 1:
                valid_pfx_sequence_for_at_bat.append(matches[0])
                next_pitch_thrown = self.get_timestamp_pitch_thrown(matches[0])
            else:
                result = self.determine_best_pfx_from_next_pitch(
                    at_bat_id, matches, pitch_count_pitch_seq, pitch_num, next_pitch_thrown,
                )
                if result.failure:
                    return result
                best_pfx = result.value
                valid_pfx_sequence_for_at_bat.append(best_pfx)
                next_pitch_thrown = self.get_timestamp_pitch_thrown(best_pfx)
        valid_pfx_sequence_for_at_bat.reverse()
        return Result.Ok(valid_pfx_sequence_for_at_bat)

    def get_timestamp_pitch_thrown(self, pfx):
        match = PFX_TIMESTAMP_REGEX.match(pfx["park_sv_id"])
        if not match:
            raise ValueError(f'Failed to parse PitchFX timestamp: {pfx["park_sv_id"]}')
        group_dict = match.groupdict()
        timestamp = datetime(
            int(f'20{group_dict["year"]}'),
            int(group_dict["month"]),
            int(group_dict["day"]),
            int(group_dict["hour"]),
            int(group_dict["minute"]),
            int(group_dict["second"]),
        )
        return timestamp.replace(tzinfo=timezone.utc).astimezone(TIME_ZONE_NEW_YORK)

    def determine_best_pfx_from_prev_pitch(
        self, at_bat_id, possible_pfx, this_pitch_num, prev_pitch_thrown
    ):
        time_since_last_pitch_min = (
            self.get_min_time_between_innings()
            if this_pitch_num == 1
            else self.get_min_time_between_pitches()
        )
        time_since_last_pitch_max = (
            self.get_max_time_between_innings()
            if this_pitch_num == 1
            else self.get_max_time_between_pitches()
        )
        for pfx in possible_pfx:
            pfx_thrown = self.get_timestamp_pitch_thrown(pfx)
            if prev_pitch_thrown > pfx_thrown:
                possible_pfx.remove(pfx)
                continue
            pitch_delta = (pfx_thrown - prev_pitch_thrown).total_seconds()
            if pitch_delta < time_since_last_pitch_min or pitch_delta > time_since_last_pitch_max:
                possible_pfx.remove(pfx)
                continue
        if not possible_pfx:
            error = (
                f"Unable to determine correct pitch sequence for at bat {at_bat_id} "
                f"(PitchFX contains {len(possible_pfx)} pitches that are both identified as "
                f"Pitch #{this_pitch_num}), but the timestamps on all of them cannot be "
                "be reconciled with the previous at bat."
            )
            return Result.Fail(error)
        if len(possible_pfx) == 1:
            return Result.Ok(possible_pfx[0])
        compare_deltas = []
        time_since_last_pitch_avg = (
            self.get_avg_time_between_innings()
            if this_pitch_num == 1
            else self.get_avg_time_between_pitches()
        )
        for pfx in possible_pfx:
            compare_deltas.append(
                {
                    "pfx": pfx,
                    "delta": abs(
                        time_since_last_pitch_avg
                        - (
                            self.get_timestamp_pitch_thrown(pfx) - prev_pitch_thrown
                        ).total_seconds()
                    ),
                }
            )
        compare_deltas.sort(key=lambda x: x["delta"])
        return Result.Ok(compare_deltas[0]["pfx"])

    def determine_best_pfx_from_next_pitch(
        self, at_bat_id, possible_pfx, pitch_count_pitch_seq, this_pitch_num, next_pitch_thrown,
    ):
        time_since_last_pitch_min = (
            self.get_min_time_between_innings()
            if this_pitch_num == pitch_count_pitch_seq
            else self.get_min_time_between_pitches()
        )
        time_since_last_pitch_max = (
            self.get_max_time_between_innings()
            if this_pitch_num == pitch_count_pitch_seq
            else self.get_max_time_between_pitches()
        )
        for pfx in possible_pfx:
            pfx_thrown = self.get_timestamp_pitch_thrown(pfx)
            if pfx_thrown > next_pitch_thrown:
                possible_pfx.remove(pfx)
                continue
            pitch_delta = (next_pitch_thrown - pfx_thrown).total_seconds()
            if pitch_delta < time_since_last_pitch_min or pitch_delta > time_since_last_pitch_max:
                possible_pfx.remove(pfx)
                continue
        if not possible_pfx:
            error = (
                f"Unable to determine correct pitch sequence for at bat {at_bat_id} "
                f"(PitchFX contains {len(possible_pfx)} pitches that are both identified as "
                f"Pitch #{this_pitch_num})"
            )
            return Result.Fail(error)
        if len(possible_pfx) == 1:
            return Result.Ok(possible_pfx[0])
        compare_deltas = []
        time_since_last_pitch_avg = (
            self.get_avg_time_between_innings()
            if this_pitch_num == pitch_count_pitch_seq
            else self.get_avg_time_between_pitches()
        )
        for pfx in possible_pfx:
            compare_deltas.append(
                {
                    "pfx": pfx,
                    "delta": abs(
                        time_since_last_pitch_avg
                        - (
                            next_pitch_thrown - self.get_timestamp_pitch_thrown(pfx)
                        ).total_seconds()
                    ),
                }
            )
        compare_deltas.sort(key=lambda x: x["delta"])
        return Result.Ok(compare_deltas[0]["pfx"])

    def get_min_time_between_innings(self):
        return self.avg_pitch_times["inning_delta"]["min"]

    def get_max_time_between_innings(self):
        return self.avg_pitch_times["inning_delta"]["max"]

    def get_avg_time_between_innings(self):
        return self.avg_pitch_times["inning_delta"]["avg"]

    def get_min_time_between_pitches(self):
        return self.avg_pitch_times["pitch_delta"]["min"]

    def get_max_time_between_pitches(self):
        return self.avg_pitch_times["pitch_delta"]["max"]

    def get_avg_time_between_pitches(self):
        return self.avg_pitch_times["pitch_delta"]["avg"]

    def construct_pitch_sequence_description(self, game_event, pfx_data=None):
        total_pitches = self.get_total_pitches_in_sequence(game_event["pitch_sequence"])
        current_pitch = 0
        next_pitch_blocked_by_c = False
        sequence_description = []
        for abbrev in game_event["pitch_sequence"]:
            if abbrev == "*":
                next_pitch_blocked_by_c = True
                continue
            if PPB_PITCH_LOG_DICT[abbrev]["pitch_counts"]:
                current_pitch += 1
                space_count = 1
                if total_pitches >= 10 and current_pitch < 10:
                    space_count = 2
                elif total_pitches >= 10 and current_pitch >= 10:
                    space_count = 1
                pitch_number = f"Pitch{' '*space_count}{current_pitch}/{total_pitches}"
                pitch_description = (
                    f"{pitch_number}..: {PPB_PITCH_LOG_DICT[abbrev]['description']}"
                )
                if pfx_data:
                    pfx = pfx_data[current_pitch - 1]
                    if abbrev == "X":
                        pfx_des = pfx["pdes"] if "missing_pdes" not in pfx["pdes"] else pfx["des"]
                        pitch_description = f"{pitch_number}..: {pfx_des}"
                    pitch_type = PITCH_TYPE_DICT[pfx["mlbam_pitch_name"]]
                    pitch_description += f' ({pfx["start_speed"]:02.0f}mph {pitch_type})'
            else:
                pitch_description = PPB_PITCH_LOG_DICT[abbrev]["description"]
            if next_pitch_blocked_by_c:
                pitch_description += " (pitch was blocked by catcher)"
                next_pitch_blocked_by_c = False
            sequence_description.append(pitch_description)
        extra_dots = 0
        if total_pitches >= 10:
            extra_dots = 2
        sequence_description.append(
            f'Result.....{"."*extra_dots}: {game_event["play_description"]}'
        )
        return sequence_description

    def update_boxscore_with_combined_data(self):
        updated_innings_list = []
        for inning in self.boxscore.innings_list:
            inning_dict = self.update_inning_with_combined_data(inning)
            updated_innings_list.append(inning_dict)

        result = self.update_pitching_stats_for_both_teams()
        if result.failure:
            return result
        (home_team_pitching_stats, away_team_pitching_stats) = result.value

        game_meta_info = self.boxscore.game_meta_info.as_dict()
        game_meta_info.pop("__bbref_boxscore_meta__", None)
        game_meta_info["umpires"] = self.boxscore.as_dict()["umpires"]

        away_team_data = self.boxscore.away_team_data.as_dict()
        away_team_data.pop("__bbref_boxscore_team_data__", None)
        away_team_data.pop("pitching_stats", None)
        away_team_data["pitching_stats"] = away_team_pitching_stats

        home_team_data = self.boxscore.home_team_data.as_dict()
        home_team_data.pop("__bbref_boxscore_team_data__", None)
        home_team_data.pop("pitching_stats", None)
        home_team_data["pitching_stats"] = home_team_pitching_stats

        pitchfx_vs_bbref_audit = self.audit_pitchfx_vs_bbref_data(
            updated_innings_list, home_team_pitching_stats, away_team_pitching_stats
        )
        updated_boxscore_dict = {
            "bbref_game_id": self.bbref_game_id,
            "boxscore_url": self.boxscore.boxscore_url,
            "pitchfx_vs_bbref_audit": pitchfx_vs_bbref_audit,
            "game_meta_info": game_meta_info,
            "away_team_data": away_team_data,
            "home_team_data": home_team_data,
            "play_by_play_data": updated_innings_list,
            "player_id_dict": self.player_id_dict,
        }
        return Result.Ok(updated_boxscore_dict)

    def update_inning_with_combined_data(self, inning):
        inning_events = [
            event
            for event in self.game_events_combined_data
            if event["inning_id"] == inning.inning_id
        ]
        total_at_bats_missing_pitchfx = len(
            [event for event in inning_events if event["missing_pitchfx_count"] > 0]
        )
        total_at_bats_extra_pitchfx = len(
            [event for event in inning_events if event["missing_pitchfx_count"] < 0]
        )
        pitch_count_bbref_pitch_seq = sum(
            event["pitch_count_bbref_pitch_seq"] for event in inning_events
        )
        pitch_count_pitchfx = sum(event["pitch_count_pitchfx"] for event in inning_events)
        pitch_count_diff = sum(event["missing_pitchfx_count"] for event in inning_events)
        if pitch_count_diff > 0:
            pitch_count_missing_pitchfx = pitch_count_diff
            pitch_count_extra_pitchfx = 0
        else:
            pitch_count_extra_pitchfx = abs(pitch_count_diff)
            pitch_count_missing_pitchfx = 0
        missing_pitchfx_is_valid = all(
            event["missing_pitchfx_is_valid"] for event in inning_events
        )
        incorrect_extra_pitchfx_removed_count = sum(
            event["incorrect_extra_pitchfx_removed_count"] for event in inning_events
        )
        at_bat_ids_missing_pitchfx = sorted(
            list(
                set(
                    event["at_bat_id"]
                    for event in inning_events
                    if event["missing_pitchfx_count"] > 0
                )
            )
        )
        at_bat_ids_extra_pitchfx = sorted(
            list(
                set(
                    event["at_bat_id"]
                    for event in inning_events
                    if event["missing_pitchfx_count"] < 0
                )
            )
        )
        at_bat_ids_pitchfx_data_error = sorted(
            list(
                set(
                    event["at_bat_id"]
                    for event in inning_events
                    if not event["missing_pitchfx_is_valid"]
                )
            )
        )
        inning_totals = {
            "inning_total_runs": inning.inning_total_runs,
            "inning_total_hits": inning.inning_total_hits,
            "inning_total_errors": inning.inning_total_errors,
            "inning_total_left_on_base": inning.inning_total_left_on_base,
            "away_team_runs_after_inning": inning.away_team_runs_after_inning,
            "home_team_runs_after_inning": inning.home_team_runs_after_inning,
        }
        inning_pitchfx_audit = {
            "total_at_bats_missing_pitchfx": total_at_bats_missing_pitchfx,
            "total_at_bats_extra_pitchfx": total_at_bats_extra_pitchfx,
            "pitch_count_bbref_pitch_seq": pitch_count_bbref_pitch_seq,
            "pitch_count_pitchfx": pitch_count_pitchfx,
            "pitch_count_missing_pitchfx": pitch_count_missing_pitchfx,
            "pitch_count_extra_pitchfx": pitch_count_extra_pitchfx,
            "missing_pitchfx_is_valid": missing_pitchfx_is_valid,
            "incorrect_extra_pitchfx_removed_count": incorrect_extra_pitchfx_removed_count,
            "at_bat_ids_missing_pitchfx": at_bat_ids_missing_pitchfx,
            "at_bat_ids_extra_pitchfx": at_bat_ids_extra_pitchfx,
            "at_bat_ids_pitchfx_data_error": at_bat_ids_pitchfx_data_error,
        }
        return {
            "inning_id": inning.inning_id,
            "inning_label": inning.inning_label,
            "begin_inning_summary": inning.begin_inning_summary,
            "end_inning_summary": inning.end_inning_summary,
            "inning_totals": inning_totals,
            "inning_pitchfx_audit": inning_pitchfx_audit,
            "inning_events": inning_events,
        }

    def update_pitching_stats_for_both_teams(self):
        pitch_stats_dict = {}
        all_bbref_pitch_stats = deepcopy(self.boxscore.away_team_data.pitching_stats)
        all_bbref_pitch_stats.extend(deepcopy(self.boxscore.home_team_data.pitching_stats))
        for pitch_stats in all_bbref_pitch_stats:
            bbref_id = pitch_stats.player_id_br
            mlb_id = self.player_id_dict[bbref_id]["mlb_id"]
            pitch_stats_dict[mlb_id] = pitch_stats
        updated_pitching_stats = []
        for pfx_log in self.pitchfx_logs_for_game:
            player_pitch_stats = pitch_stats_dict.pop(pfx_log.pitcher_id_mlb, None)
            if not player_pitch_stats:
                error = f"Error retrieving boxscore stats for pitch app: {pfx_log.pitch_app_id}"
                return Result.Fail(error)
            combined_pitching_stats = self.update_pitching_stats_with_combined_data(
                pfx_log, player_pitch_stats
            )
            updated_pitching_stats.append(combined_pitching_stats)
        for _, player_pitch_stats in pitch_stats_dict.items():
            pitch_stats = self.handle_pitch_stats_without_pitchfx_data(player_pitch_stats)
            updated_pitching_stats.append(pitch_stats)
        return self.separate_pitching_stats_by_team(
            updated_pitching_stats,
            self.boxscore.home_team_data.team_id_br,
            self.boxscore.away_team_data.team_id_br,
        )

    def update_pitching_stats_with_combined_data(self, pfx_log, player_pitch_stats):
        bbref_data = player_pitch_stats.as_dict()
        bbref_data.pop("player_id_br", None)
        bbref_data.pop("player_team_id_br", None)
        bbref_data.pop("opponent_team_id_br", None)
        self.at_bat_ids = sorted(list(set([pfx.at_bat_id for pfx in pfx_log.pitchfx_log])))
        return {
            "pitcher_name": pfx_log.pitcher_name,
            "pitcher_id_mlb": pfx_log.pitcher_id_mlb,
            "pitcher_id_bbref": player_pitch_stats.player_id_br,
            "pitch_app_id": pfx_log.pitch_app_id,
            "pitcher_team_id_bb": pfx_log.pitcher_team_id_bb,
            "pitcher_team_id_bbref": player_pitch_stats.player_team_id_br,
            "opponent_team_id_bb": pfx_log.opponent_team_id_bb,
            "opponent_team_id_bbref": player_pitch_stats.opponent_team_id_br,
            "bb_game_id": pfx_log.bb_game_id,
            "bbref_game_id": pfx_log.bbref_game_id,
            "batters_faced_bbref": player_pitch_stats.batters_faced,
            "batters_faced_pitchfx": len(self.at_bat_ids),
            "total_pitch_count_bbref": player_pitch_stats.pitch_count,
            "total_pitch_count_pitchfx": pfx_log.total_pitch_count,
            "at_bat_ids": self.at_bat_ids,
            "bbref_data": bbref_data,
            "pitchfx_data": {
                "total_pitch_count": pfx_log.total_pitch_count,
                "duplicate_pitches_removed_count": pfx_log.duplicate_pitches_removed_count,
                "pitch_count_by_inning": pfx_log.pitch_count_by_inning,
            },
        }

    def handle_pitch_stats_without_pitchfx_data(self, player_pitch_stats):
        bbref_id = player_pitch_stats.player_id_br
        pitcher_team_id_br = player_pitch_stats.player_team_id_br
        pitcher_team_id_bb = self.get_brooks_team_id(pitcher_team_id_br)
        opponent_team_id_br = player_pitch_stats.opponent_team_id_br
        opponent_team_id_bb = self.get_brooks_team_id(opponent_team_id_br)
        mlb_id = self.player_id_dict[bbref_id].get("mlb_id", "")
        pitch_app_id = f"{self.bbref_game_id}_{mlb_id}"
        pitcher_name = self.player_id_dict[bbref_id].get("name", "")
        self.at_bat_ids = sorted(
            list(
                set(
                    [
                        game_event["at_bat_id"]
                        for game_event in self.game_events_combined_data
                        if game_event["pitcher_name"] == pitcher_name
                    ]
                )
            )
        )
        bbref_data = player_pitch_stats.as_dict()
        bbref_data.pop("player_id_br", None)
        bbref_data.pop("player_team_id_br", None)
        bbref_data.pop("opponent_team_id_br", None)
        return {
            "pitcher_name": pitcher_name,
            "pitcher_id_mlb": mlb_id,
            "pitcher_id_bbref": bbref_id,
            "pitch_app_id": pitch_app_id,
            "pitcher_team_id_bb": pitcher_team_id_bb,
            "pitcher_team_id_bbref": pitcher_team_id_br,
            "opponent_team_id_bb": opponent_team_id_bb,
            "opponent_team_id_bbref": opponent_team_id_br,
            "bb_game_id": self.boxscore.bb_game_id,
            "bbref_game_id": self.bbref_game_id,
            "batters_faced_bbref": player_pitch_stats.batters_faced,
            "batters_faced_pitchfx": len(self.at_bat_ids),
            "total_pitch_count_bbref": player_pitch_stats.pitch_count,
            "total_pitch_count_pitchfx": 0,
            "at_bat_ids": self.at_bat_ids,
            "bbref_data": bbref_data,
            "pitchfx_data": {
                "total_pitch_count": 0,
                "duplicate_pitches_removed_count": 0,
                "pitch_count_by_inning": [],
            },
        }

    def separate_pitching_stats_by_team(self, updated_pitching_stats, home_team_id, away_team_id):
        home_team_pitching_stats = []
        away_team_pitching_stats = []
        for pitching_stats in updated_pitching_stats:
            if pitching_stats["pitcher_team_id_bbref"] == home_team_id:
                home_team_pitching_stats.append(pitching_stats)
            elif pitching_stats["pitcher_team_id_bbref"] == away_team_id:
                away_team_pitching_stats.append(pitching_stats)
            else:
                error = (
                    "Error occurred trying to assign pitching_stats for pitch_app_id "
                    f'{pitching_stats["pitch_app_id"]} to either home or away team '
                    f"(home_team_id: {home_team_id}, away_team_id: {away_team_id}, "
                    f'pitcher_team_id_bbref: {pitching_stats["pitcher_team_id_bbref"]}'
                )
                return Result.Fail(error)
        return Result.Ok((home_team_pitching_stats, away_team_pitching_stats))

    def audit_pitchfx_vs_bbref_data(
        self, updated_innings_list, home_team_pitching_stats, away_team_pitching_stats
    ):
        total_batters_faced_bbref_home = sum(
            pitch_stats["batters_faced_bbref"] for pitch_stats in home_team_pitching_stats
        )
        total_batters_faced_bbref_away = sum(
            pitch_stats["batters_faced_bbref"] for pitch_stats in away_team_pitching_stats
        )
        total_batters_faced_bbref = total_batters_faced_bbref_home + total_batters_faced_bbref_away

        batters_faced_pitchfx_home = sum(
            pitch_stats["batters_faced_pitchfx"] for pitch_stats in home_team_pitching_stats
        )
        batters_faced_pitchfx_away = sum(
            pitch_stats["batters_faced_pitchfx"] for pitch_stats in away_team_pitching_stats
        )
        total_batters_faced_pitchfx = batters_faced_pitchfx_home + batters_faced_pitchfx_away

        total_at_bats_missing_pitchfx = sum(
            inning["inning_pitchfx_audit"]["total_at_bats_missing_pitchfx"]
            for inning in updated_innings_list
        )
        total_at_bats_extra_pitchfx = sum(
            inning["inning_pitchfx_audit"]["total_at_bats_extra_pitchfx"]
            for inning in updated_innings_list
        )
        total_at_bats_pitchfx_complete = (
            total_batters_faced_pitchfx
            - total_at_bats_missing_pitchfx
            - total_at_bats_extra_pitchfx
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
        total_pitch_count_extra_pitchfx = sum(
            inning["inning_pitchfx_audit"]["pitch_count_extra_pitchfx"]
            for inning in updated_innings_list
        )
        missing_pitchfx_is_valid = all(
            inning["inning_pitchfx_audit"]["missing_pitchfx_is_valid"]
            for inning in updated_innings_list
        )
        total_incorrect_extra_pitchfx_removed_count = sum(
            inning["inning_pitchfx_audit"]["incorrect_extra_pitchfx_removed_count"]
            for inning in updated_innings_list
        )
        at_bat_ids_missing_pitchfx = sorted(
            list(
                set(
                    flatten_list2d(
                        [
                            inning["inning_pitchfx_audit"]["at_bat_ids_missing_pitchfx"]
                            for inning in updated_innings_list
                        ]
                    )
                )
            )
        )
        at_bat_ids_extra_pitchfx = sorted(
            list(
                set(
                    flatten_list2d(
                        [
                            inning["inning_pitchfx_audit"]["at_bat_ids_extra_pitchfx"]
                            for inning in updated_innings_list
                        ]
                    )
                )
            )
        )
        at_bat_ids_pitchfx_data_error = sorted(
            list(
                set(
                    flatten_list2d(
                        [
                            inning["inning_pitchfx_audit"]["at_bat_ids_pitchfx_data_error"]
                            for inning in updated_innings_list
                        ]
                    )
                )
            )
        )
        duplicate_pfx_removed_home = sum(
            pitch_stats["pitchfx_data"]["duplicate_pitches_removed_count"]
            for pitch_stats in home_team_pitching_stats
        )
        duplicate_pfx_removed_away = sum(
            pitch_stats["pitchfx_data"]["duplicate_pitches_removed_count"]
            for pitch_stats in away_team_pitching_stats
        )
        duplicate_pitchfx_removed_count = duplicate_pfx_removed_home + duplicate_pfx_removed_away

        return {
            "total_batters_faced_bbref": total_batters_faced_bbref,
            "total_at_bats_pitchfx_complete": total_at_bats_pitchfx_complete,
            "total_at_bats_missing_pitchfx": total_at_bats_missing_pitchfx,
            "total_at_bats_extra_pitchfx": total_at_bats_extra_pitchfx,
            "pitch_count_bbref_stats_table": total_pitch_count_bbref_stats_table,
            "pitch_count_bbref_pitch_seq": total_pitch_count_bbref_pitch_seq,
            "pitch_count_pitchfx_audited": total_pitch_count_pitchfx,
            "pitch_count_missing_pitchfx": total_pitch_count_missing_pitchfx,
            "pitch_count_extra_pitchfx": total_pitch_count_extra_pitchfx,
            "incorrect_extra_pitchfx_removed_count": total_incorrect_extra_pitchfx_removed_count,
            "missing_pitchfx_is_valid": missing_pitchfx_is_valid,
            "at_bat_ids_missing_pitchfx": at_bat_ids_missing_pitchfx,
            "at_bat_ids_extra_pitchfx": at_bat_ids_extra_pitchfx,
            "at_bat_ids_pitchfx_data_error": at_bat_ids_pitchfx_data_error,
            "duplicate_pitchfx_removed_count": duplicate_pitchfx_removed_count,
        }
