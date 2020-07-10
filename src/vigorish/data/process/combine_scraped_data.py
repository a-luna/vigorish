"""Aggregate pitchfx data and play-by-play data into a single object."""
from collections import Counter, defaultdict, OrderedDict
from copy import deepcopy
from datetime import datetime, timezone
from typing import Dict, List

from vigorish.constants import (
    TEAM_ID_DICT,
    PPB_PITCH_LOG_DICT,
    PITCH_TYPE_DICT,
)
from vigorish.config.database import Player, GameScrapeStatus
from vigorish.data.process.avg_time_between_pitches import get_timestamp_pitch_thrown
from vigorish.scrape.bbref_boxscores.models.boxscore import BBRefBoxscore
from vigorish.scrape.brooks_pitchfx.models.pitchfx_log import BrooksPitchFxLog
from vigorish.scrape.brooks_pitchfx.models.pitchfx import BrooksPitchFxData
from vigorish.scrape.mlb_player_info.scrape_mlb_player_info import scrape_mlb_player_info
from vigorish.util.datetime_util import TIME_ZONE_NEW_YORK
from vigorish.util.dt_format_strings import DT_AWARE
from vigorish.util.list_helpers import compare_lists, flatten_list2d, report_dict
from vigorish.util.string_helpers import validate_at_bat_id
from vigorish.util.regex import PFX_TIMESTAMP_REGEX
from vigorish.util.result import Result


class CombineScrapedData:
    _game_start_time: datetime
    bbref_game_id: str
    boxscore: BBRefBoxscore
    pitchfx_logs_for_game: BrooksPitchFxLog
    player_id_dict: Dict
    at_bat_ids: List
    at_bat_event_groups: Dict
    all_pfx_data_for_game: List
    game_events_combined_data: List

    def __init__(self, db_session):
        self.db_session = db_session
        self._game_start_time = None

    @property
    def game_start_time(self):
        return self._game_start_time if self._game_start_time else None

    @game_start_time.setter
    def game_start_time(self, start_time):
        if not self._game_start_time:
            self._game_start_time = start_time

    def execute(self, bbref_boxscore, pitchfx_logs_for_game):
        self.bbref_game_id = bbref_boxscore.bbref_game_id
        self.boxscore = bbref_boxscore
        self.pitchfx_logs_for_game = pitchfx_logs_for_game
        return (
            self.get_all_pbp_events_for_game()
            .on_success(self.get_all_pfx_data_for_game)
            .on_success(self.combine_pbp_events_with_pfx_data)
            .on_success(self.update_boxscore_with_combined_data)
        )

    def investigate(self, bbref_boxscore, pitchfx_logs_for_game):
        self.bbref_game_id = bbref_boxscore.bbref_game_id
        self.boxscore = bbref_boxscore
        self.pitchfx_logs_for_game = pitchfx_logs_for_game
        audit_results = {
            "bbref_game_id": self.bbref_game_id,
            "boxscore": self.boxscore,
            "pitchfx_logs_for_game": self.pitchfx_logs_for_game,
        }
        result = self.get_all_pbp_events_for_game()
        if result.failure:
            audit_results["get_all_pbp_events_success"] = False
            audit_results["error_message"] = result.error
            return audit_results
        audit_results["get_all_pbp_events_success"] = True
        audit_results["player_id_dict"] = self.player_id_dict
        audit_results["at_bat_ids"] = self.at_bat_ids
        audit_results["at_bat_event_groups"] = self.at_bat_event_groups
        result = self.get_all_pfx_data_for_game()
        if result.failure:
            audit_results["get_all_pfx_data_success"] = False
            audit_results["error_message"] = result.error
            return audit_results
        audit_results["get_all_pfx_data_success"] = True
        audit_results["all_pfx_data_for_game"] = self.all_pfx_data_for_game
        result = self.combine_pbp_events_with_pfx_data()
        if result.failure:
            audit_results["combine_data_success"] = False
            audit_results["error_message"] = result.error
            return audit_results
        audit_results["combine_data_success"] = True
        audit_results["game_events_combined_data"] = self.game_events_combined_data
        result = self.update_boxscore_with_combined_data()
        if result.failure:
            audit_results["update_boxscore_success"] = False
            audit_results["error_message"] = result.error
            return audit_results
        audit_results["update_boxscore_success"] = True
        audit_results["updated_boxscore_dict"] = result.value
        return audit_results

    def get_all_pbp_events_for_game(self):
        result = self.get_player_id_dict_for_game()
        if result.failure:
            return result
        self.player_id_dict = result.value
        self.at_bat_ids = []
        self.at_bat_event_groups = {}
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
                game_event.is_complete_at_bat = game_event_is_complete_at_bat
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
        player_id_dict = {}
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
            player_id_dict[bbref_id] = {
                "name": name,
                "mlb_id": player.mlb_id,
                "team_id_bbref": player_team_dict.get(bbref_id, ""),
            }
        return Result.Ok(player_id_dict)

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

    def event_is_pbp_at_bat(self, event):
        return "BBRefPlayByPlayEvent" in str(type(event))

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
                    f'Error! Unknown pitch type, "{pitch}", occurred in sequence: {pitch_seq} '
                    f"(pbp_table_row# {row_num}, game_id: {self.bbref_game_id})"
                )
                return Result.Fail(error)
        return Result.Ok(True) if strikes == 3 or balls == 4 else Result.Ok(False)

    def event_resulted_in_third_out(self, game_event):
        got_third_out = "O" in game_event.runs_outs_result and game_event.outs_before_play == 2
        got_third_out_dp = "OO" in game_event.runs_outs_result and game_event.outs_before_play == 1
        return got_third_out or got_third_out_dp

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
        if self.event_is_pbp_at_bat(event):
            event_dict["is_complete_at_bat"] = event.is_complete_at_bat
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
        self.pitchfx_logs_for_game = [
            self.remove_duplicate_guids_from_pfx(pitchfx_log)
            for pitchfx_log in self.pitchfx_logs_for_game
        ]
        self.check_pfx_game_start_time()
        self.all_pfx_data_for_game = []
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

    def remove_duplicate_guids_from_pfx(self, pitchfx_log):
        pfx_log_copy = deepcopy(pitchfx_log.pitchfx_log)
        pitch_guids = [pfx.play_guid for pfx in pfx_log_copy]
        histogram = Counter(pitch_guids)
        unique_guids = Counter(list(set(pitch_guids)))
        duplicate_guids = histogram - unique_guids
        if not duplicate_guids:
            pitchfx_log.duplicate_pitchfx_removed_count = 0
            return pitchfx_log
        dupe_rank_dict = defaultdict(list)
        for pfx in pfx_log_copy:
            if pfx.play_guid in duplicate_guids:
                dupe_rank_dict[pfx.play_guid].append(pfx)
                dupe_rank_dict[pfx.play_guid].sort(
                    key=lambda x: (-x.has_zone_location, x.seconds_since_game_start)
                )
        pfx_log_no_dupes = []
        dupe_tracker = {guid: False for guid in unique_guids.keys()}
        for pfx in pfx_log_copy:
            if dupe_tracker[pfx.play_guid]:
                continue
            if pfx.play_guid in duplicate_guids:
                best_pfx = dupe_rank_dict[pfx.play_guid][0]
                pfx_log_no_dupes.append(best_pfx)
            else:
                pfx_log_no_dupes.append(pfx)
            dupe_tracker[pfx.play_guid] = True
        pfx_log_no_dupes.sort(key=lambda x: (x.ab_id, x.ab_count))
        pitchfx_log.duplicate_pitchfx_removed_count = len(pfx_log_copy) - len(pfx_log_no_dupes)
        pitchfx_log.pitchfx_log = pfx_log_no_dupes
        pitchfx_log.pitch_count_by_inning = self.get_pitch_count_by_inning(pfx_log_no_dupes)
        pitchfx_log.total_pitch_count = len(pfx_log_no_dupes)
        pfx_log_copy = None
        return pitchfx_log

    def check_pfx_game_start_time(self):
        for pitchfx_log in self.pitchfx_logs_for_game:
            if pitchfx_log.game_start_time:
                self.game_start_time = pitchfx_log.game_start_time
                continue
            if not self.game_start_time:
                self.game_start_time = self.get_start_time_from_pfx()
            self.update_game_start_time()
            pitchfx_log.game_date_year = game_start_time.year
            pitchfx_log.game_date_month = game_start_time.month
            pitchfx_log.game_date_day = game_start_time.day
            pitchfx_log.game_time_hour = game_start_time.hour
            pitchfx_log.game_time_minute = game_start_time.minute
            pitchfx_log.time_zone_name = "America/New_York"
            for pfx in pitchfx_log.pitchfx_log:
                pfx.game_start_time_str = game_start_time.strftime(DT_AWARE)
                pitch_thrown = pfx.timestamp_pitch_thrown
                pfx.seconds_since_game_start = int(
                    (pitch_thrown - game_start_time).total_seconds()
                )

    def get_start_time_from_pfx(self):
        first_park_sv_id = min(
            pfx.park_sv_id
            for pitchfx_log in self.pitchfx_logs_for_game
            for pfx in pitchfx_log.pitchfx_log
        )
        if not first_park_sv_id:
            return None
        first_pitch_thrown = get_timestamp_pitch_thrown(first_park_sv_id, self.bbref_game_id)
        return first_pitch_thrown.replace(second=0)

    def update_game_start_time(self):
        game_status = GameScrapeStatus.find_by_bbref_game_id(self.db_session, self.bbref_game_id)
        setattr(game_status, "game_time_hour", self.game_start_time.hour)
        setattr(game_status, "game_time_minute", self.game_start_time.minute)
        setattr(game_status, "game_time_zone", "America/New_York")
        self.db_session.commit()

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
        self.game_events_combined_data = []
        for ab_id in self.at_bat_ids:
            error_messages = []
            pbp_events_for_at_bat = self.get_all_pbp_events_for_at_bat(ab_id)
            first_event_this_at_bat = pbp_events_for_at_bat[0]
            final_event_this_at_bat = pbp_events_for_at_bat[-1]
            pitch_seq = final_event_this_at_bat["pitch_sequence"]
            pitch_count = self.get_total_pitches_in_sequence(pitch_seq)
            pfx_data = self.get_all_pfx_data_for_at_bat(ab_id)
            (pfx_data, removed_count) = self.remove_duplicate_pitches_pfx(pfx_data, pitch_count)
            extra_pitchfx_removed_count = removed_count
            extra_pitchfx_count = len(pfx_data) - pitch_count
            if extra_pitchfx_count > 0:
                breakpoint()
                error_messages.append(
                    "At bat still contains extra PitchFX data: "
                    f"Total from Pitch Sequence: {pitch_count}, "
                    f"Total PitchFX Scraped: {len(pfx_data)}"
                )
            missing_pitchfx_count = pitch_count - len(pfx_data)
            missing_pitch_numbers = self.get_missing_pitch_numbers(pfx_data, pitch_count)
            if missing_pitchfx_count != len(missing_pitch_numbers):
                total_unidentified = pitch_count - len(pfx_data) - len(missing_pitch_numbers)
                missing_pitch_ids = ", ".join(str(num) for num in missing_pitch_numbers)
                error_messages.append(
                    f"Identified {len(missing_pitch_numbers)} pitches missing from "
                    f"PitchFX data ({missing_pitch_ids}), however this "
                    "does not account for all pitches in the at bat: "
                    f"Total from Pitch Sequence: {pitch_count}, "
                    f"Total PitchFX Scraped: {len(pfx_data)}, "
                    f"Total Identified Missing: {len(missing_pitch_numbers)}, "
                    f"Total Unidentified Missing: {total_unidentified}"
                )
            pfx_data_copy = deepcopy(pfx_data) if not missing_pitch_numbers else None
            pitch_sequence_description = self.construct_pitch_sequence_description(
                ab_id, final_event_this_at_bat, pfx_data_copy,
            )
            pitcher_id_bbref = first_event_this_at_bat["pitcher_id_br"]
            pitcher_name = self.player_id_dict[pitcher_id_bbref].get("name", "")
            batter_id_bbref = first_event_this_at_bat["batter_id_br"]
            batter_name = self.player_id_dict[batter_id_bbref].get("name", "")
            combined_at_bat_data = {
                "at_bat_id": ab_id,
                "inning_id": first_event_this_at_bat["inning_id"],
                "pbp_table_row_number": first_event_this_at_bat["pbp_table_row_number"],
                "pitcher_name": pitcher_name,
                "batter_name": batter_name,
                "pitch_count_bbref": pitch_count,
                "pitch_count_pitchfx": len(pfx_data),
                "missing_pitchfx_count": missing_pitchfx_count if missing_pitchfx_count > 0 else 0,
                "missing_pitch_numbers": missing_pitch_numbers,
                "extra_pitchfx_count": extra_pitchfx_count if extra_pitchfx_count > 0 else 0,
                "extra_pitchfx_removed_count": extra_pitchfx_removed_count,
                "pitchfx_data_error": len(error_messages) > 0,
                "error_messages": error_messages,
                "is_complete_at_bat": final_event_this_at_bat["is_complete_at_bat"],
                "pitch_sequence_description": pitch_sequence_description,
                "pbp_events": self.at_bat_event_groups[ab_id],
                "pitchfx": pfx_data,
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
        game_event_id_map = [
            {
                "at_bat_id": ab_id,
                "pbp_table_row_number": min(
                    game_event["pbp_table_row_number"]
                    for game_event in self.at_bat_event_groups[ab_id]
                ),
            }
            for ab_id in at_bat_ids
        ]
        game_event_id_map.sort(key=lambda x: x["pbp_table_row_number"])
        return [id_map["at_bat_id"] for id_map in game_event_id_map]

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

    def remove_duplicate_pitches_pfx(self, pfx_data, pitch_count):
        pitch_numbers_pitch_seq = set(range(1, pitch_count + 1))
        pitch_numbers_pfx = [pfx["ab_count"] for pfx in pfx_data]
        unique_pitch_numbers_pfx = Counter(list(set(pitch_numbers_pfx)))
        duplicate_pitch_numbers_pfx = Counter(pitch_numbers_pfx) - unique_pitch_numbers_pfx
        if not duplicate_pitch_numbers_pfx:
            return (pfx_data, 0)
        dupe_rank_dict = defaultdict(list)
        for pfx in pfx_data:
            if pfx["ab_count"] in duplicate_pitch_numbers_pfx:
                dupe_rank_dict[pfx["ab_count"]].append(pfx)
                dupe_rank_dict[pfx["ab_count"]].sort(
                    key=lambda x: (-x["has_zone_location"], x["seconds_since_game_start"])
                )
        pfx_data_no_dupes = []
        dupe_tracker = {pitch_num: False for pitch_num in unique_pitch_numbers_pfx.keys()}
        for pfx in pfx_data:
            if dupe_tracker[pfx["ab_count"]]:
                continue
            if pfx["ab_count"] in duplicate_pitch_numbers_pfx:
                best_pfx = dupe_rank_dict[pfx["ab_count"]][0]
                pfx_data_no_dupes.append(best_pfx)
            else:
                pfx_data_no_dupes.append(pfx)
            dupe_tracker[pfx["ab_count"]] = True
        pfx_data_no_dupes.sort(key=lambda x: x["ab_count"])
        removed_count = len(pfx_data) - len(pfx_data_no_dupes)
        return (pfx_data_no_dupes, removed_count)

    def get_missing_pitch_numbers(self, pfx_data, pitch_count):
        pitch_numbers = set(range(1, pitch_count + 1))
        pitch_numbers_pfx = set(pfx["ab_count"] for pfx in pfx_data)
        return list(pitch_numbers - pitch_numbers_pfx)

    def construct_pitch_sequence_description(
        self, at_bat_id, final_event_in_at_bat, pfx_data=None
    ):
        pitch_sequence = final_event_in_at_bat["pitch_sequence"]
        total_pitches = self.get_total_pitches_in_sequence(pitch_sequence)
        non_batter_events = self.get_all_other_events_for_at_bat(at_bat_id, final_event_in_at_bat)
        current_pitch = 0
        next_pitch_blocked_by_c = False
        sequence_description = []
        for abbrev in pitch_sequence:
            pitch_number = ""
            pfx_des = ""
            blocked_by_c = ""
            outcome = PPB_PITCH_LOG_DICT[abbrev]["description"]
            if abbrev == "*":
                next_pitch_blocked_by_c = True
                continue
            if PPB_PITCH_LOG_DICT[abbrev]["pitch_counts"]:
                current_pitch += 1
                space_count = 1 if total_pitches < 10 or current_pitch >= 10 else 2
                pitch_number = f"Pitch{' '*space_count}{current_pitch}/{total_pitches}"
                if pfx_data:
                    pfx = pfx_data[current_pitch - 1]
                    if abbrev == "X":
                        outcome = pfx["pdes"] if "missing_pdes" not in pfx["pdes"] else pfx["des"]
                    pitch_type = PITCH_TYPE_DICT[pfx["mlbam_pitch_name"]]
                    pfx_des = f' ({pfx["start_speed"]:02.0f}mph {pitch_type})'
                if next_pitch_blocked_by_c:
                    blocked_by_c = " (pitch was blocked by catcher)"
                    next_pitch_blocked_by_c = False
                sequence_description.append(f"{pitch_number}..: {outcome}{pfx_des}{blocked_by_c}")
                continue
            if abbrev != ".":
                sequence_description.append(outcome)
            else:
                outcome = self.get_next_event_description(non_batter_events, outcome)
                sequence_description.append(outcome)
        while any(not event_dict["processed"] for event_dict in non_batter_events.values()):
            outcome = self.get_next_event_description(non_batter_events)
            if outcome:
                sequence_description.append(outcome)
        extra_dots = 0 if total_pitches < 10 else 2
        sequence_description.append(
            f'Result.....{"."*extra_dots}: {final_event_in_at_bat["play_description"]}'
        )
        return sequence_description

    def get_all_other_events_for_at_bat(self, at_bat_id, final_event_this_at_bat):
        all_other_events = [event for event in self.at_bat_event_groups[at_bat_id]]
        all_other_events.sort(key=lambda x: x["pbp_table_row_number"])
        all_other_events.remove(final_event_this_at_bat)
        if not all_other_events:
            return {}
        non_batter_events = OrderedDict()
        for num, event in enumerate(all_other_events, start=1):
            non_batter_events[num] = {
                "processed": False,
                "event": event,
            }
        return non_batter_events

    def get_next_event_description(self, non_batter_events, default_outcome=""):
        outcome = default_outcome
        for event_dict in non_batter_events.values():
            if not event_dict["processed"]:
                event = event_dict["event"]
                outcome = (
                    f'({event["play_description"]})'
                    if event["event_type"] == "AT_BAT"
                    else f'({event["sub_description"]})'
                    if event["event_type"] == "SUBSTITUTION"
                    else f'({event["description"]})'
                )
                event_dict["processed"] = True
                break
        return outcome.strip(".")

    def update_boxscore_with_combined_data(self):
        updated_innings_list = []
        for inning in self.boxscore.innings_list:
            inning_dict = self.update_inning_with_combined_data(inning)
            updated_innings_list.append(inning_dict)

        (pitch_stats_away, pitch_stats_home) = self.update_all_pitch_stats()
        away_team_data = self.boxscore.away_team_data.as_dict()
        home_team_data = self.boxscore.home_team_data.as_dict()
        away_team_data.pop("__bbref_boxscore_team_data__", None)
        home_team_data.pop("__bbref_boxscore_team_data__", None)
        away_team_data.pop("pitching_stats", None)
        home_team_data.pop("pitching_stats", None)
        away_team_data["pitching_stats"] = pitch_stats_away
        home_team_data["pitching_stats"] = pitch_stats_home

        game_meta_info = self.boxscore.game_meta_info.as_dict()
        game_meta_info.pop("__bbref_boxscore_meta__", None)
        game_meta_info["game_time_hour"] = self.game_start_time.hour
        game_meta_info["game_time_minute"] = self.game_start_time.minute
        game_meta_info["game_date_time_str"] = self.game_start_time.strftime(DT_AWARE)
        game_meta_info["umpires"] = self.boxscore.as_dict()["umpires"]

        pitchfx_vs_bbref_audit = self.audit_pitchfx_vs_bbref_data(
            updated_innings_list, pitch_stats_away, pitch_stats_home
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
        inning_events.sort(key=lambda x: x["pbp_table_row_number"])
        pitch_count_bbref = sum(event["pitch_count_bbref"] for event in inning_events)
        pitch_count_pitchfx = sum(event["pitch_count_pitchfx"] for event in inning_events)
        missing_pitchfx_count = sum(event["missing_pitchfx_count"] for event in inning_events)
        extra_pitchfx_count = sum(event["extra_pitchfx_count"] for event in inning_events)
        pitchfx_data_error = any(event["pitchfx_data_error"] for event in inning_events)
        extra_pitchfx_removed_count = sum(
            event["extra_pitchfx_removed_count"] for event in inning_events
        )
        at_bat_ids_missing_pitchfx = list(
            set(
                event["at_bat_id"] for event in inning_events if event["missing_pitchfx_count"] > 0
            )
        )
        at_bat_ids_extra_pitchfx = list(
            set(event["at_bat_id"] for event in inning_events if event["extra_pitchfx_count"] > 0)
        )
        at_bat_ids_pitchfx_data_error = {
            event["at_bat_id"]: event["error_messages"]
            for event in inning_events
            if event["pitchfx_data_error"]
        }
        at_bat_ids_pitchfx_complete = list(
            set(
                event["at_bat_id"]
                for event in inning_events
                if event["missing_pitchfx_count"] == 0
                and event["extra_pitchfx_count"] == 0
                and not event["pitchfx_data_error"]
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
            "pitch_count_bbref": pitch_count_bbref,
            "pitch_count_pitchfx": pitch_count_pitchfx,
            "missing_pitchfx_count": missing_pitchfx_count,
            "extra_pitchfx_count": extra_pitchfx_count,
            "extra_pitchfx_removed_count": extra_pitchfx_removed_count,
            "pitchfx_data_error": pitchfx_data_error,
            "total_at_bats_pitchfx_complete": len(at_bat_ids_pitchfx_complete),
            "at_bat_ids_pitchfx_complete": at_bat_ids_pitchfx_complete,
            "total_at_bats_missing_pitchfx": len(at_bat_ids_missing_pitchfx),
            "at_bat_ids_missing_pitchfx": at_bat_ids_missing_pitchfx,
            "total_at_bats_extra_pitchfx": len(at_bat_ids_extra_pitchfx),
            "at_bat_ids_extra_pitchfx": at_bat_ids_extra_pitchfx,
            "total_at_bats_pitchfx_data_error": len(at_bat_ids_pitchfx_data_error),
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

    def update_all_pitch_stats(self):
        pitch_stats_dict = {}
        all_bbref_pitch_stats = deepcopy(self.boxscore.away_team_data.pitching_stats)
        all_bbref_pitch_stats.extend(deepcopy(self.boxscore.home_team_data.pitching_stats))
        for pitch_stats in all_bbref_pitch_stats:
            mlb_id = self.player_id_dict[pitch_stats.player_id_br]["mlb_id"]
            pitch_stats_dict[mlb_id] = pitch_stats
        updated_pitching_stats = defaultdict(list)
        for pfx_log in self.pitchfx_logs_for_game:
            pitch_stats = pitch_stats_dict.pop(pfx_log.pitcher_id_mlb, None)
            if not pitch_stats:
                error = f"Error retrieving boxscore stats for pitch app: {pfx_log.pitch_app_id}"
                return Result.Fail(error)
            (team_id, updated_stats) = self.update_player_pitch_stats(pfx_log, pitch_stats)
            updated_pitching_stats[team_id].append(updated_stats)
        for pitch_stats in pitch_stats_dict.values():
            (team_id, updated_stats) = self.update_player_pitch_stats_no_pfx(pitch_stats)
            updated_pitching_stats[team_id].append(updated_stats)
        away_team_id = self.boxscore.away_team_data.team_id_br
        home_team_id = self.boxscore.home_team_data.team_id_br
        return (updated_pitching_stats[away_team_id], updated_pitching_stats[home_team_id])

    def update_player_pitch_stats(self, pfx_log, pitch_stats):
        bbref_data = pitch_stats.as_dict()
        bbref_data.pop("player_id_br", None)
        bbref_data.pop("player_team_id_br", None)
        bbref_data.pop("opponent_team_id_br", None)
        pitcher_events = [
            game_event
            for game_event in self.game_events_combined_data
            if game_event["pitcher_name"] == pfx_log.pitcher_name
        ]
        pitcher_events.sort(key=lambda x: x["pbp_table_row_number"])
        pitcher_at_bat_ids = list(set([event["at_bat_id"] for event in pitcher_events]))
        pitch_count_bbref = sum(event["pitch_count_bbref"] for event in pitcher_events)
        pitch_count_pitchfx = sum(event["pitch_count_pitchfx"] for event in pitcher_events)
        missing_pitchfx_count = sum(event["missing_pitchfx_count"] for event in pitcher_events)
        extra_pitchfx_count = sum(event["extra_pitchfx_count"] for event in pitcher_events)
        extra_pitchfx_removed_count = sum(
            event["extra_pitchfx_removed_count"] for event in pitcher_events
        )
        pitchfx_data_error = any(event["pitchfx_data_error"] for event in pitcher_events)
        batters_faced_pfx = len([event for event in pitcher_events if event["is_complete_at_bat"]])
        ab_ids_missing_pfx = [
            event["at_bat_id"] for event in pitcher_events if event["missing_pitchfx_count"] > 0
        ]
        ab_ids_extra_pfx = [
            event["at_bat_id"] for event in pitcher_events if event["extra_pitchfx_count"] > 0
        ]
        ab_ids_pfx_error = {
            event["at_bat_id"]: event["error_messages"]
            for event in pitcher_events
            if event["pitchfx_data_error"]
        }
        ab_ids_pfx_complete = [
            event["at_bat_id"]
            for event in pitcher_events
            if event["missing_pitchfx_count"] == 0
            and event["extra_pitchfx_count"] == 0
            and not event["pitchfx_data_error"]
        ]
        updated_stats = {
            "pitcher_name": pfx_log.pitcher_name,
            "pitcher_id_mlb": pfx_log.pitcher_id_mlb,
            "pitcher_id_bbref": pitch_stats.player_id_br,
            "pitch_app_id": pfx_log.pitch_app_id,
            "pitcher_team_id_bb": pfx_log.pitcher_team_id_bb,
            "pitcher_team_id_bbref": pitch_stats.player_team_id_br,
            "opponent_team_id_bb": pfx_log.opponent_team_id_bb,
            "opponent_team_id_bbref": pitch_stats.opponent_team_id_br,
            "bb_game_id": pfx_log.bb_game_id,
            "bbref_game_id": pfx_log.bbref_game_id,
            "pitch_count_by_inning": pfx_log.pitch_count_by_inning,
            "pitch_count_bbref": pitch_count_bbref,
            "pitch_count_pitchfx": pitch_count_pitchfx,
            "missing_pitchfx_count": missing_pitchfx_count,
            "extra_pitchfx_count": extra_pitchfx_count,
            "duplicate_pitchfx_removed_count": pfx_log.duplicate_pitchfx_removed_count,
            "extra_pitchfx_removed_count": extra_pitchfx_removed_count,
            "pitchfx_data_error": pitchfx_data_error,
            "batters_faced_bbref": pitch_stats.batters_faced,
            "batters_faced_pitchfx": batters_faced_pfx,
            "total_at_bats_pitchfx_complete": len(ab_ids_pfx_complete),
            "at_bat_ids_pitchfx_complete": ab_ids_pfx_complete,
            "total_at_bats_missing_pitchfx": len(ab_ids_missing_pfx),
            "at_bat_ids_missing_pitchfx": ab_ids_missing_pfx,
            "total_at_bats_extra_pitchfx": len(ab_ids_extra_pfx),
            "at_bat_ids_extra_pitchfx": ab_ids_extra_pfx,
            "total_at_bats_pitchfx_data_error": len(ab_ids_pfx_error),
            "at_bat_ids_pitchfx_data_error": ab_ids_pfx_error,
            "bbref_data": bbref_data,
        }
        return (pitch_stats.player_team_id_br, updated_stats)

    def update_player_pitch_stats_no_pfx(self, pitch_stats):
        bbref_id = pitch_stats.player_id_br
        pitcher_team_id_br = pitch_stats.player_team_id_br
        pitcher_team_id_bb = self.get_brooks_team_id(pitcher_team_id_br)
        opponent_team_id_br = pitch_stats.opponent_team_id_br
        opponent_team_id_bb = self.get_brooks_team_id(opponent_team_id_br)
        mlb_id = self.player_id_dict[bbref_id].get("mlb_id", "")
        pitch_app_id = f"{self.bbref_game_id}_{mlb_id}"
        pitcher_name = self.player_id_dict[bbref_id].get("name", "")
        pitcher_events = [
            game_event
            for game_event in self.game_events_combined_data
            if game_event["pitcher_name"] == pitcher_name
        ]
        pitcher_events.sort(key=lambda x: x["pbp_table_row_number"])
        pitcher_at_bat_ids = list(set([event["at_bat_id"] for event in pitcher_events]))
        bbref_data = pitch_stats.as_dict()
        bbref_data.pop("player_id_br", None)
        bbref_data.pop("player_team_id_br", None)
        bbref_data.pop("opponent_team_id_br", None)
        updated_stats = {
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
            "pitch_count_by_inning": [],
            "batters_faced_bbref": pitch_stats.batters_faced,
            "batters_faced_pitchfx": 0,
            "pitch_count_bbref": pitch_stats.pitch_count,
            "pitch_count_pitchfx": 0,
            "missing_pitchfx_count": pitch_stats.pitch_count,
            "extra_pitchfx_count": 0,
            "duplicate_pitchfx_removed_count": 0,
            "extra_pitchfx_removed_count": 0,
            "pitchfx_data_error": False,
            "total_at_bats_pitchfx_complete": 0,
            "at_bat_ids_pitchfx_complete": [],
            "total_at_bats_missing_pitchfx": len(pitcher_at_bat_ids),
            "at_bat_ids_missing_pitchfx": pitcher_at_bat_ids,
            "total_at_bats_extra_pitchfx": 0,
            "at_bat_ids_extra_pitchfx": [],
            "total_at_bats_pitchfx_data_error": 0,
            "at_bat_ids_pitchfx_data_error": [],
            "bbref_data": bbref_data,
        }
        return (pitcher_team_id_br, updated_stats)

    def audit_pitchfx_vs_bbref_data(
        self, updated_innings_list, away_team_pitching_stats, home_team_pitching_stats
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
        total_at_bats_pitchfx_data_error = sum(
            inning["inning_pitchfx_audit"]["total_at_bats_pitchfx_data_error"]
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
            inning["inning_pitchfx_audit"]["pitch_count_bbref"] for inning in updated_innings_list
        )
        total_pitch_count_pitchfx = sum(
            inning["inning_pitchfx_audit"]["pitch_count_pitchfx"]
            for inning in updated_innings_list
        )
        total_missing_pitchfx_count = sum(
            inning["inning_pitchfx_audit"]["missing_pitchfx_count"]
            for inning in updated_innings_list
        )
        total_extra_pitchfx_count = sum(
            inning["inning_pitchfx_audit"]["extra_pitchfx_count"]
            for inning in updated_innings_list
        )
        pitchfx_data_error = any(
            inning["inning_pitchfx_audit"]["pitchfx_data_error"] for inning in updated_innings_list
        )
        total_extra_pitchfx_removed_count = sum(
            inning["inning_pitchfx_audit"]["extra_pitchfx_removed_count"]
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
        at_bat_ids_pitchfx_data_error = {}
        for inning in updated_innings_list:
            pitchfx_error_dict = inning["inning_pitchfx_audit"]["at_bat_ids_pitchfx_data_error"]
            if not pitchfx_error_dict:
                continue
            at_bat_ids_pitchfx_data_error.update(pitchfx_error_dict)

        duplicate_pfx_removed_home = sum(
            pitch_stats["duplicate_pitchfx_removed_count"]
            for pitch_stats in home_team_pitching_stats
        )
        duplicate_pfx_removed_away = sum(
            pitch_stats["duplicate_pitchfx_removed_count"]
            for pitch_stats in away_team_pitching_stats
        )
        duplicate_pitchfx_removed_count = duplicate_pfx_removed_home + duplicate_pfx_removed_away

        return {
            "total_batters_faced_bbref": total_batters_faced_bbref,
            "total_batters_faced_pitchfx": total_batters_faced_pitchfx,
            "total_at_bats_pitchfx_complete": total_at_bats_pitchfx_complete,
            "total_at_bats_missing_pitchfx": total_at_bats_missing_pitchfx,
            "total_at_bats_extra_pitchfx": total_at_bats_extra_pitchfx,
            "total_at_bats_pitchfx_data_error": total_at_bats_pitchfx_data_error,
            "pitch_count_bbref_stats_table": total_pitch_count_bbref_stats_table,
            "pitch_count_bbref_pitch_seq": total_pitch_count_bbref_pitch_seq,
            "pitch_count_pitchfx_audited": total_pitch_count_pitchfx,
            "missing_pitchfx_count": total_missing_pitchfx_count,
            "extra_pitchfx_count": total_extra_pitchfx_count,
            "extra_pitchfx_removed_count": total_extra_pitchfx_removed_count,
            "duplicate_pitchfx_removed_count": duplicate_pitchfx_removed_count,
            "pitchfx_data_error": pitchfx_data_error,
            "at_bat_ids_missing_pitchfx": at_bat_ids_missing_pitchfx,
            "at_bat_ids_extra_pitchfx": at_bat_ids_extra_pitchfx,
            "at_bat_ids_pitchfx_data_error": at_bat_ids_pitchfx_data_error,
        }
