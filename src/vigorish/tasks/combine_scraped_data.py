"""Aggregate pitchfx data and play-by-play data into a single object."""
import logging
from collections import Counter, defaultdict, OrderedDict
from copy import deepcopy
from datetime import datetime
from typing import Dict, List

from vigorish.config.database import GameScrapeStatus, PlayerId, TimeBetweenPitches
from vigorish.constants import PITCH_TYPE_DICT, PPB_PITCH_LOG_DICT
from vigorish.enums import DataSet
from vigorish.scrape.bbref_boxscores.models.boxscore import BBRefBoxscore
from vigorish.scrape.brooks_pitchfx.models.pitchfx_log import BrooksPitchFxLog
from vigorish.status.update_status_combined_data import update_pitch_apps_for_game_combined_data
from vigorish.tasks.base import Task
from vigorish.tasks.scrape_mlb_player_info import scrape_mlb_player_info
from vigorish.util.dt_format_strings import DT_AWARE
from vigorish.util.list_helpers import flatten_list2d
from vigorish.util.result import Result
from vigorish.util.string_helpers import (
    get_brooks_team_id,
    get_inning_id_from_at_bat_id,
    replace_char_with_newlines,
    validate_at_bat_id,
)

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


class CombineScrapedDataTask(Task):
    game_status: GameScrapeStatus
    bbref_game_id: str
    boxscore: BBRefBoxscore
    pitchfx_logs_for_game: BrooksPitchFxLog
    player_id_dict: Dict
    at_bat_ids: List
    at_bat_event_groups: Dict
    all_pfx_data_for_game: List
    removed_guid_dupes: List
    all_dupe_guids: defaultdict
    removed_pfx: List
    all_removed_pfx: defaultdict
    invalid_pitchfx: defaultdict
    game_events_combined_data: List

    def __init__(self, app):
        super().__init__(app)
        self.db_session = self.app["db_session"]

    @property
    def away_team_id_br(self):
        return self.boxscore.away_team_data.team_id_br

    @property
    def home_team_id_br(self):
        return self.boxscore.home_team_data.team_id_br

    @property
    def away_team_id_bb(self):
        return get_brooks_team_id(self.boxscore.away_team_data.team_id_br)

    @property
    def home_team_id_bb(self):
        return get_brooks_team_id(self.boxscore.home_team_data.team_id_br)

    @property
    def game_start_time(self):
        return self.game_status.game_start_time if self.game_status else None

    @property
    def game_start_time_str(self):
        return self.game_start_time.strftime(DT_AWARE) if self.game_status else None

    def execute(self, bbref_game_id, apply_patch_list=True, write_json=True, update_db=True):
        self.bbref_game_id = bbref_game_id
        self.update_db = update_db
        result = self.gather_scraped_data(apply_patch_list)
        if result.failure:
            return {"gather_scraped_data_success": False, "error": result.error}
        result = (
            self.check_pfx_game_start_time()
            .on_success(self.get_all_pbp_events_for_game)
            .on_success(self.get_all_pfx_data_for_game)
            .on_success(self.combine_pbp_events_with_pfx_data)
            .on_success(self.update_boxscore_with_combined_data)
        )
        if result.failure:
            self.update_game_status(success=False)
            return {
                "gather_scraped_data_success": True,
                "combined_data_success": False,
                "error": result.error,
            }
        self.update_game_status(success=True)
        combined_data = result.value
        if write_json:
            result = self.scraped_data.save_combined_game_data(combined_data)
        if result.failure:
            return result
        if not update_db:
            return {
                "gather_scraped_data_success": True,
                "combined_data_success": True,
                "results": combined_data,
            }
        result = update_pitch_apps_for_game_combined_data(self.db_session, combined_data)
        if result.failure:
            return {
                "gather_scraped_data_success": True,
                "combined_data_success": True,
                "update_pitch_apps_success": False,
                "error": result.error,
            }
        return {
            "gather_scraped_data_success": True,
            "combined_data_success": True,
            "update_pitch_apps_success": True,
            "results": result.value,
        }

    def gather_scraped_data(self, apply_patch_list):
        self.game_status = GameScrapeStatus.find_by_bbref_game_id(
            self.db_session, self.bbref_game_id
        )
        self.boxscore = self.scraped_data.get_bbref_boxscore(self.bbref_game_id)
        if not self.boxscore:
            error = f"Failed to retrieve {DataSet.BBREF_BOXSCORES} (URL ID: {self.bbref_game_id})"
            Result.Ok(error)
        if apply_patch_list:
            result = self.scraped_data.apply_patch_list(
                DataSet.BBREF_BOXSCORES, self.bbref_game_id, self.boxscore
            )
            if result.failure:
                return result
            self.boxscore = result.value
        result = self.scraped_data.get_all_pitchfx_logs_for_game(self.bbref_game_id)
        if result.failure:
            return result
        self.pitchfx_logs_for_game = result.value
        if apply_patch_list:
            result = self.scraped_data.apply_patch_list(
                DataSet.BROOKS_PITCHFX,
                self.bbref_game_id,
                self.pitchfx_logs_for_game,
                self.boxscore,
            )
            if result.failure:
                return result
            self.pitchfx_logs_for_game = result.value
        self.avg_pitch_times = TimeBetweenPitches.get_latest_results(self.db_session)
        return Result.Ok()

    def investigate(self, bbref_game_id, apply_patch_list=False):
        self.bbref_game_id = bbref_game_id
        result = self.gather_scraped_data(apply_patch_list)
        if result.failure:
            return {"gather_scraped_data_success": False, "error": result.error}
        self.check_pfx_game_start_time()
        audit_results = {
            "gather_scraped_data_success": True,
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
        audit_results["boxscore"] = result.value
        return audit_results

    def check_pfx_game_start_time(self):
        if not self.game_status.game_start_time:
            self.update_game_start_time()
        for pitchfx_log in self.pitchfx_logs_for_game:
            if pitchfx_log.game_start_time:
                continue
            pitchfx_log.game_date_year = self.game_start_time.year
            pitchfx_log.game_date_month = self.game_start_time.month
            pitchfx_log.game_date_day = self.game_start_time.day
            pitchfx_log.game_time_hour = self.game_start_time.hour
            pitchfx_log.game_time_minute = self.game_start_time.minute
            pitchfx_log.time_zone_name = "America/New_York"
            for pfx in pitchfx_log.pitchfx_log:
                pfx.game_start_time_str = self.game_start_time_str
        return Result.Ok()

    def update_game_start_time(self):
        all_pfx = [pfx for pfx_log in self.pitchfx_logs_for_game for pfx in pfx_log.pitchfx_log]
        all_pfx.sort(key=lambda x: x.park_sv_id)
        first_pitch_thrown = all_pfx[0].time_pitch_thrown
        game_start_time = first_pitch_thrown.replace(second=0)
        self.game_status.game_time_hour = game_start_time.hour
        self.game_status.game_time_minute = game_start_time.minute
        self.game_status.game_time_zone = "America/New_York"
        self.db_session.commit()

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
                game_event.is_complete_at_bat = result.value
            else:
                game_event.is_complete_at_bat = False
            if (
                game_event.is_complete_at_bat
                or self.event_resulted_in_third_out(game_event)
                or self.runner_reached_on_interference(game_event)
            ):
                self.add_new_at_bat_event_group(game_event, at_bat_events)
                at_bat_events = []
        if at_bat_events:
            prev_event = at_bat_events[-1]
            self.update_at_bat_event_groups(prev_event, at_bat_events)
        return Result.Ok()

    def get_player_id_dict_for_game(self):
        player_id_dict = {}
        player_name_dict = self.boxscore.player_name_dict
        for name_team_id, bbref_id in player_name_dict.items():
            split = name_team_id.split(",")
            name = split[0].strip()
            team_id = split[1].strip()
            player = PlayerId.find_by_bbref_id(self.db_session, bbref_id)
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
                "team_id_bbref": team_id,
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

    def runner_reached_on_interference(self, game_event):
        return "reached on interference" in game_event.play_description.lower()

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
        inning = int(game_event.inning_label[1:])
        inning_str = f"{0}{inning}" if inning < 10 else inning
        pteam = get_brooks_team_id(game_event.team_pitching_id_br)
        pid = self.player_id_dict[game_event.pitcher_id_br]["mlb_id"]
        bteam = get_brooks_team_id(game_event.team_batting_id_br)
        bid = self.player_id_dict[game_event.batter_id_br]["mlb_id"]
        return f"{self.bbref_game_id}_{inning_str}_{pteam}_{pid}_{bteam}_{bid}_{instance_number}"

    def get_all_pfx_data_for_game(self):
        self.removed_guid_dupes = []
        for pitchfx_log in self.pitchfx_logs_for_game:
            (pitchfx_log, removed_pfx) = self.remove_duplicate_guids_from_pfx(pitchfx_log)
            if removed_pfx:
                self.removed_guid_dupes.extend(removed_pfx)
        self.all_pfx_data_for_game = []
        for pitchfx_log in self.pitchfx_logs_for_game:
            for pfx in pitchfx_log.pitchfx_log:
                pfx.at_bat_id = self.get_at_bat_id_for_pfx_data(pfx)
            self.all_pfx_data_for_game.extend(pitchfx_log.pitchfx_log)
        self.all_pfx_data_for_game.sort(key=lambda x: (x.ab_id, x.ab_count))
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
        self.update_pfx_attributes()
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
            pitchfx_log.duplicate_guid_removed_count = 0
            pfx_log = pitchfx_log.pitchfx_log
            pitchfx_log.pitch_count_by_inning = self.get_pitch_count_by_inning(pfx_log)
            return (pitchfx_log, [])
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
                best_pfx = dupe_rank_dict[pfx.play_guid].pop(0)
                pfx_log_no_dupes.append(best_pfx)
            else:
                pfx_log_no_dupes.append(pfx)
            dupe_tracker[pfx.play_guid] = True
        pfx_log_no_dupes.sort(key=lambda x: (x.ab_id, x.ab_count))
        removed_dupes = flatten_list2d([dupes for dupes in dupe_rank_dict.values()])
        removed_pfx = self.update_duplicate_guid_pfx(removed_dupes)
        pitchfx_log.duplicate_guid_removed_count = len(pfx_log_copy) - len(pfx_log_no_dupes)
        pitchfx_log.pitchfx_log = pfx_log_no_dupes
        pitchfx_log.pitch_count_by_inning = self.get_pitch_count_by_inning(pfx_log_no_dupes)
        pitchfx_log.total_pitch_count = len(pfx_log_no_dupes)
        pfx_log_copy = None
        return (pitchfx_log, removed_pfx)

    def update_duplicate_guid_pfx(self, removed_dupes):
        for pfx in removed_dupes:
            pfx.is_duplicate_guid = True
            pfx.at_bat_id = self.get_at_bat_id_for_pfx_data(pfx)
            pfx.inning_id = get_inning_id_from_at_bat_id(pfx.at_bat_id)
            id_dict = PlayerId.get_player_ids_from_at_bat_id(self.db_session, pfx.at_bat_id)
            pfx.pitcher_id_bbref = id_dict["pitcher_id_bbref"]
            pfx.batter_id_bbref = id_dict["batter_id_bbref"]
        return sorted(removed_dupes, key=lambda x: (x.ab_id, x.ab_count))

    def get_pitch_count_by_inning(self, pitchfx_log):
        pitch_count_unordered = defaultdict(int)
        for pfx in pitchfx_log:
            pitch_count_unordered[pfx.inning] += 1
        pitch_count_ordered = OrderedDict()
        for k in sorted(pitch_count_unordered.keys()):
            pitch_count_ordered[k] = pitch_count_unordered[k]
        return pitch_count_ordered

    def get_at_bat_id_for_pfx_data(self, pfx, instance_number=0):
        game_id = pfx.bbref_game_id
        inning = f"{0}{pfx.inning}" if pfx.inning < 10 else pfx.inning
        pteam = pfx.pitcher_team_id_bb
        pid = pfx.pitcher_id
        bteam = pfx.opponent_team_id_bb
        bid = pfx.batter_id
        return f"{game_id}_{inning}_{pteam}_{pid}_{bteam}_{bid}_{instance_number}"

    def update_pfx_attributes(self):
        for pfx in self.all_pfx_data_for_game:
            pfx.inning_id = get_inning_id_from_at_bat_id(pfx.at_bat_id)
            id_dict = PlayerId.get_player_ids_from_at_bat_id(self.db_session, pfx.at_bat_id)
            pfx.pitcher_id_bbref = id_dict["pitcher_id_bbref"]
            pfx.batter_id_bbref = id_dict["batter_id_bbref"]

    def combine_pbp_events_with_pfx_data(self):
        (self.at_bat_ids, at_bat_ids_invalid_pfx) = self.reconcile_at_bat_ids()
        self.removed_pfx = []
        at_bat_ids_dupe_guids = []
        at_bat_ids_removed_pfx = []
        self.game_events_combined_data = []
        for ab_id in self.at_bat_ids:
            error = None
            pbp_events_for_at_bat = self.get_all_pbp_events_for_at_bat(ab_id)
            first_event_this_at_bat = pbp_events_for_at_bat[0]
            final_event_this_at_bat = pbp_events_for_at_bat[-1]
            runs_outs_result = final_event_this_at_bat["runs_outs_result"]
            if len(pbp_events_for_at_bat) > 1:
                all_results = [event["runs_outs_result"] for event in pbp_events_for_at_bat]
                runs_outs_result = "".join(all_results)
            pitch_seq = final_event_this_at_bat["pitch_sequence"]
            result = self.get_total_pitches_in_sequence(pitch_seq)
            if result.failure:
                return result
            pitch_count = result.value
            pfx_data = self.get_all_pfx_data_for_at_bat(ab_id)
            (pfx_no_dupes, removed_pfx) = self.remove_duplicate_pitches_pfx(pfx_data)
            extra_pitchfx_count = len(pfx_no_dupes) - pitch_count
            if extra_pitchfx_count > 0:
                if pitch_count == 0:
                    pfx_no_dupes = self.update_invalid_ibb_pfx(pfx_no_dupes)
                    new_removed_pfx = {
                        "removed_count": len(pfx_no_dupes),
                        "removed_dupes": pfx_no_dupes,
                    }
                    removed_pfx = self.combine_removed_pitchfx(removed_pfx, new_removed_pfx)
                    pfx_no_dupes = []
                    extra_pitchfx_count = 0
                else:
                    result = self.find_pfx_out_of_sequence(ab_id, pfx_no_dupes, pitch_count)
                    if result.failure:
                        error = result.error
                    else:
                        (pfx_no_dupes, new_removed_pfx) = result.value
                        removed_pfx = self.combine_removed_pitchfx(removed_pfx, new_removed_pfx)
                        extra_pitchfx_count = len(pfx_no_dupes) - pitch_count
                        if extra_pitchfx_count > 0:
                            error = (
                                "At bat contains extra PitchFX data: "
                                f"Total from Pitch Sequence: {pitch_count}, "
                                f"Total PitchFX Scraped: {len(pfx_no_dupes)}"
                            )
            missing_pitchfx_count = pitch_count - len(pfx_no_dupes)
            missing_pitch_numbers = self.get_missing_pitch_numbers(pfx_no_dupes, pitch_count)
            if missing_pitchfx_count != len(missing_pitch_numbers):
                total_unidentified = pitch_count - len(pfx_no_dupes) - len(missing_pitch_numbers)
                missing_pitch_ids = ", ".join(str(num) for num in missing_pitch_numbers)
                error = (
                    f"Identified {len(missing_pitch_numbers)} pitches missing from "
                    f"PitchFX data ({missing_pitch_ids}), however this "
                    "does not account for all pitches in the at bat: "
                    f"Total from Pitch Sequence: {pitch_count}, "
                    f"Total PitchFX Scraped: {len(pfx_no_dupes)}, "
                    f"Total Identified Missing: {len(missing_pitch_numbers)}, "
                    f"Total Unidentified Missing: {total_unidentified}"
                )
            (since_start, dur, first_pitch, last_pitch) = self.get_at_bat_duration(pfx_no_dupes)
            self.prepare_pfx_data_for_json_serialization(pfx_no_dupes)
            if removed_pfx["removed_count"] > 0:
                self.prepare_pfx_data_for_json_serialization(removed_pfx["removed_dupes"])
                self.removed_pfx.extend(removed_pfx["removed_dupes"])
                at_bat_ids_removed_pfx.append(ab_id)
            dupe_guids = self.get_all_removed_guid_dupes_for_at_bat(ab_id)
            if dupe_guids["removed_count"] > 0:
                self.prepare_pfx_data_for_json_serialization(dupe_guids["removed_dupes"])
                at_bat_ids_dupe_guids.append(ab_id)
            pfx_data_copy = deepcopy(pfx_no_dupes) if not missing_pitch_numbers else None
            result = self.construct_pitch_sequence_description(
                ab_id,
                final_event_this_at_bat,
                pfx_data_copy,
            )
            if result.failure:
                return result
            pitch_sequence_description = result.value
            id_dict = PlayerId.get_player_ids_from_at_bat_id(self.db_session, ab_id)
            at_bat_pitchfx_audit = {
                "pitch_count_bbref": pitch_count,
                "pitch_count_pitchfx": len(pfx_no_dupes),
                "patched_pitchfx_count": len([pfx for pfx in pfx_no_dupes if pfx["is_patched"]]),
                "missing_pitchfx_count": missing_pitchfx_count if missing_pitchfx_count > 0 else 0,
                "missing_pitch_numbers": missing_pitch_numbers,
                "extra_pitchfx_count": extra_pitchfx_count if extra_pitchfx_count > 0 else 0,
                "extra_pitchfx_removed_count": removed_pfx["removed_count"],
                "duplicate_guid_removed_count": dupe_guids["removed_count"],
                "pitchfx_error": error is not None,
                "pitchfx_error_message": error,
            }
            combined_at_bat_data = {
                "at_bat_id": ab_id,
                "inning_id": get_inning_id_from_at_bat_id(ab_id),
                "pitch_app_id": self.get_pitch_app_id_from_at_bat_id(ab_id),
                "pbp_table_row_number": first_event_this_at_bat["pbp_table_row_number"],
                "pitcher_id_bbref": id_dict["pitcher_id_bbref"],
                "pitcher_id_mlb": id_dict["pitcher_id_mlb"],
                "pitcher_name": id_dict["pitcher_name"],
                "batter_id_bbref": id_dict["batter_id_bbref"],
                "batter_id_mlb": id_dict["batter_id_mlb"],
                "batter_name": id_dict["batter_name"],
                "at_bat_pitchfx_audit": at_bat_pitchfx_audit,
                "first_pitch_thrown": first_pitch,
                "last_pitch_thrown": last_pitch,
                "since_game_start": since_start,
                "at_bat_duration": dur,
                "is_complete_at_bat": final_event_this_at_bat["is_complete_at_bat"],
                "score": first_event_this_at_bat["score"],
                "outs_before_play": first_event_this_at_bat["outs_before_play"],
                "runners_on_base": first_event_this_at_bat["runners_on_base"],
                "runs_outs_result": runs_outs_result,
                "play_description": final_event_this_at_bat["play_description"],
                "pitch_sequence_description": pitch_sequence_description,
                "pbp_events": self.at_bat_event_groups[ab_id],
                "pitchfx": pfx_no_dupes,
                "removed_pitchfx": removed_pfx if removed_pfx["removed_count"] > 0 else {},
                "removed_duplicate_guid": dupe_guids if dupe_guids["removed_count"] > 0 else {},
            }
            self.game_events_combined_data.append(combined_at_bat_data)
        self.save_removed_pfx(at_bat_ids_removed_pfx)
        self.save_dupe_guids(at_bat_ids_dupe_guids)
        self.save_invalid_pitchfx(at_bat_ids_invalid_pfx)
        return Result.Ok()

    def reconcile_at_bat_ids(self):
        at_bat_ids_from_box = list(set([ab_id for ab_id in self.at_bat_event_groups.keys()]))
        at_bat_ids_from_box = self.order_at_bat_ids_by_time(at_bat_ids_from_box)
        at_bat_ids_from_pfx = [pfx.at_bat_id for pfx in self.all_pfx_data_for_game]
        at_bat_ids_invalid_pfx = list(set(at_bat_ids_from_pfx) - set(at_bat_ids_from_box))
        return (at_bat_ids_from_box, at_bat_ids_invalid_pfx)

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
        pfx_for_at_bat = [pfx for pfx in self.all_pfx_data_for_game if pfx.at_bat_id == at_bat_id]
        return self.convert_pfx_to_dict_list(pfx_for_at_bat)

    def get_all_removed_guid_dupes_for_at_bat(self, at_bat_id):
        pfx_for_at_bat = [pfx for pfx in self.removed_guid_dupes if pfx.at_bat_id == at_bat_id]
        if not pfx_for_at_bat:
            return {"removed_count": 0, "removed_dupes": []}
        removed_dupes = self.convert_pfx_to_dict_list(pfx_for_at_bat)
        return {"removed_count": len(removed_dupes), "removed_dupes": removed_dupes}

    def get_all_removed_pfx_for_at_bat(self, at_bat_id):
        removed_pfx = [pfx for pfx in self.removed_pfx if pfx["at_bat_id"] == at_bat_id]
        if not removed_pfx:
            return {"removed_count": 0, "removed_dupes": []}
        return {"removed_count": len(removed_pfx), "removed_dupes": removed_pfx}

    def convert_pfx_to_dict_list(self, pfx_list):
        pfx_dict_list = []
        for pfx in pfx_list:
            pfx_dict = pfx.as_dict()
            pfx_dict.pop("__brooks_pitchfx_data__", None)
            pfx_dict["at_bat_id"] = pfx.at_bat_id
            pfx_dict["inning_id"] = pfx.inning_id
            pfx_dict["pitcher_id_bbref"] = pfx.pitcher_id_bbref
            pfx_dict["batter_id_bbref"] = pfx.batter_id_bbref
            pfx_dict["game_start_time"] = pfx.game_start_time
            pfx_dict["time_pitch_thrown"] = pfx.time_pitch_thrown
            pfx_dict["seconds_since_game_start"] = pfx.seconds_since_game_start
            pfx_dict_list.append(pfx_dict)
        pfx_dict_list.sort(key=lambda x: x["ab_count"])
        return pfx_dict_list

    def get_total_pitches_in_sequence(self, pitch_sequence):
        total_pitches = 0
        for abbrev in pitch_sequence:
            result = self.get_pitch_type(abbrev)
            if result.failure:
                return result
            pitch_type = result.value
            total_pitches += pitch_type["pitch_counts"]
        return Result.Ok(total_pitches)

    def get_pitch_type(self, abbrev):
        try:
            pitch_type = PPB_PITCH_LOG_DICT[abbrev]
            return Result.Ok(pitch_type)
        except KeyError as e:
            return Result.Fail(f"Invalid pitch abbreviation: {abbrev}\n{repr(e)}")

    def remove_duplicate_pitches_pfx(self, pfx_data):
        pitch_numbers_pfx = [pfx["ab_count"] for pfx in pfx_data]
        unique_pitch_numbers_pfx = Counter(list(set(pitch_numbers_pfx)))
        duplicate_pitch_numbers_pfx = Counter(pitch_numbers_pfx) - unique_pitch_numbers_pfx
        if not duplicate_pitch_numbers_pfx:
            return (pfx_data, {"removed_count": 0, "removed_dupes": []})
        dupe_rank_dict = defaultdict(list)
        for pfx in pfx_data:
            if pfx["ab_count"] in duplicate_pitch_numbers_pfx:
                dupe_rank_dict[pfx["ab_count"]].append(pfx)
                dupe_rank_dict[pfx["ab_count"]].sort(
                    key=lambda x: (-x["has_zone_location"], x["seconds_since_game_start"])
                )
        pfx_no_dupes = []
        dupe_tracker = {pitch_num: False for pitch_num in unique_pitch_numbers_pfx.keys()}
        for pfx in pfx_data:
            if dupe_tracker[pfx["ab_count"]]:
                continue
            if pfx["ab_count"] in duplicate_pitch_numbers_pfx:
                best_pfx = dupe_rank_dict[pfx["ab_count"]].pop(0)
                pfx_no_dupes.append(best_pfx)
            else:
                pfx_no_dupes.append(pfx)
            dupe_tracker[pfx["ab_count"]] = True
        pfx_no_dupes.sort(key=lambda x: x["ab_count"])
        removed_dupes = flatten_list2d([dupes for dupes in dupe_rank_dict.values()])
        removed_dupes = self.update_duplicate_pitch_number_pfx(removed_dupes)
        removed_pfx = {"removed_count": len(removed_dupes), "removed_dupes": removed_dupes}
        return (pfx_no_dupes, removed_pfx)

    def update_duplicate_pitch_number_pfx(self, removed_dupes):
        for pfx in removed_dupes:
            pfx["is_duplicate_pitch_number"] = True
        return sorted(removed_dupes, key=lambda x: (x["ab_id"], x["ab_count"]))

    def update_invalid_ibb_pfx(self, removed_dupes):
        for pfx in removed_dupes:
            pfx["is_invalid_ibb"] = True
        return sorted(removed_dupes, key=lambda x: (x["ab_id"], x["ab_count"]))

    def find_pfx_out_of_sequence(
        self,
        at_bat_id,
        fix_pfx_data,
        pitch_count,
    ):
        error = (
            f"{self.bbref_game_id} has extra PFX data that can only be investigated with the "
            "'next_at_bat' set of methods, which are currently commented out."
        )
        ab_index = self.at_bat_ids.index(at_bat_id)
        if ab_index == 0:
            LOGGER.info(error)
            return Result.Fail(error)
            # next_ab_id = self.at_bat_ids[ab_index + 1]
            # return self.correct_pfx_data_using_next_at_bat(
            #     at_bat_id, next_ab_id, fix_pfx_data, pitch_count
            # )
        if ab_index == len(self.at_bat_ids) - 1:
            prev_ab_id = self.at_bat_ids[ab_index - 1]
            return self.correct_pfx_data_using_previous_at_bat(
                at_bat_id, prev_ab_id, fix_pfx_data, pitch_count
            )
        error_messages = []
        prev_ab_id = self.at_bat_ids[ab_index - 1]
        # next_ab_id = self.at_bat_ids[ab_index + 1]
        result = self.correct_pfx_data_using_previous_at_bat(
            at_bat_id, prev_ab_id, fix_pfx_data, pitch_count
        )
        if result.success:
            return result
        error_messages.append(result.error)
        error_messages.append(error)
        LOGGER.info(error)
        return Result.Fail("\n".join(error_messages))
        # result = self.correct_pfx_data_using_next_at_bat(
        #     at_bat_id, next_ab_id, fix_pfx_data, pitch_count
        # )
        # if result.success:
        #     return result
        # error_messages.append(result.error)
        # return Result.Fail("\n".join(error_messages))

    def correct_pfx_data_using_previous_at_bat(
        self, fix_ab_id, prev_ab_id, fix_pfx_data, pitch_count
    ):
        result = self.get_last_pitch_thrown_time(prev_ab_id)
        if result.failure:
            return result
        prev_pitch_thrown = result.value
        valid_pfx = []
        possible_pfx = deepcopy(fix_pfx_data)
        for pitch_num in range(1, pitch_count + 1):
            matches = [pfx for pfx in fix_pfx_data if pfx["ab_count"] == pitch_num]
            if not matches:
                error = (
                    f"Unable to determine correct pitch sequence for at bat {fix_ab_id} "
                    f"(PitchFX contains 0 pitches that are identified as Pitch #{pitch_num})"
                )
                return Result.Fail(error)
            if len(matches) == 1:
                valid_pfx.append(matches[0])
                possible_pfx.remove(matches[0])
                prev_pitch_thrown = matches[0]["time_pitch_thrown"]
            else:
                result = self.determine_best_pfx_from_prev_pitch(
                    fix_ab_id, prev_ab_id, deepcopy(possible_pfx), pitch_num, prev_pitch_thrown
                )
                if result.failure:
                    return result
                best_pfx = result.value
                valid_pfx.append(best_pfx)
                possible_pfx.remove(best_pfx)
                prev_pitch_thrown = best_pfx["time_pitch_thrown"]
        possible_pfx = self.update_out_of_sequence_pfx(possible_pfx)
        removed_pfx = {"removed_count": len(possible_pfx), "removed_dupes": possible_pfx}
        return Result.Ok((valid_pfx, removed_pfx))

    def get_last_pitch_thrown_time(self, at_bat_id):
        result = self.get_game_event(at_bat_id)
        if result.failure:
            return result
        at_bat = result.value
        if not at_bat:
            return Result.Fail(f"No game event found with at bat id {at_bat_id}")
        if not self.pitchfx_data_is_complete(at_bat):
            return Result.Fail(f"PitchFX data is incorrect or incomplete for at bat {at_bat_id}")
        last_pitch_thrown_str = at_bat["pitchfx"][-1]["time_pitch_thrown_str"]
        last_pitch_in_at_bat_thrown = datetime.strptime(last_pitch_thrown_str, DT_AWARE)
        return Result.Ok(last_pitch_in_at_bat_thrown)

    def determine_best_pfx_from_prev_pitch(
        self, ab_id, prev_ab_id, possible_pfx, pitch_num, prev_pitch_thrown
    ):
        pitch_times = self.get_pitch_metrics_prev_at_bat(ab_id, prev_ab_id, pitch_num)
        within_boundaries_pfx = []
        impossible_pfx = []
        for pfx in possible_pfx:
            pitch_delta = (pfx["time_pitch_thrown"] - prev_pitch_thrown).total_seconds()
            if pitch_delta < 0:
                impossible_pfx.append((pfx["time_pitch_thrown"].strftime(DT_AWARE), pitch_delta))
                continue
            if pitch_delta < int(pitch_times["min"]):
                impossible_pfx.append((pfx["time_pitch_thrown"].strftime(DT_AWARE), pitch_delta))
                continue
            if pitch_delta > int(pitch_times["max"]):
                impossible_pfx.append((pfx["time_pitch_thrown"].strftime(DT_AWARE), pitch_delta))
                continue
            within_boundaries_pfx.append(pfx)
        if not within_boundaries_pfx:
            impossible_pfx_report = ""
            for num, (pitch_thrown, between_pitches) in enumerate(impossible_pfx, start=1):
                pfx_report = f"invalid_pfx_{num}..: {pitch_thrown} ({between_pitches} seconds)\n"
                impossible_pfx_report += pfx_report
            impossible_pfx_report.rstrip("\n")
            error = (
                f"Unable to determine correct pitch sequence for this at bat. Based on the time "
                "that the previous pitch was thrown, none of the possible pitches "
                f"({len(possible_pfx)} total) were thrown at a time that would be considered "
                "valid for the next pitch in the at bat:\n"
                f"at_bat_id..........: {ab_id}\n"
                f"next_pitch_number..: {pitch_num}\n"
                f"prev_pitch_thrown..: {prev_pitch_thrown.strftime(DT_AWARE)}\n"
                f"valid_range........: {int(pitch_times['min'])} - {int(pitch_times['max'])} seconds\n"
                f"{impossible_pfx_report}"
            )
            return Result.Fail(error)
        if len(within_boundaries_pfx) == 1:
            return Result.Ok(within_boundaries_pfx[0])
        deltas = [
            {
                "pfx": pfx,
                "has_zone_location": pfx["has_zone_location"],
                "delta": self.delta_avg_time_between_pitches(
                    pitch_times["avg"], prev_pitch_thrown, pfx["time_pitch_thrown"]
                ),
            }
            for pfx in within_boundaries_pfx
        ]
        deltas.sort(key=lambda x: (-x["has_zone_location"], x["delta"]))
        return Result.Ok(deltas[0]["pfx"])

    def get_pitch_metrics_prev_at_bat(self, at_bat_id, prev_ab_id, pitch_num):
        same_inning = self.at_bat_ids_are_in_same_inning([at_bat_id, prev_ab_id])
        if pitch_num == 1 and same_inning:
            return {
                "avg": self.avg_pitch_times["time_between_at_bats"]["avg"],
                "min": self.avg_pitch_times["time_between_at_bats"]["min"],
                "max": self.avg_pitch_times["time_between_at_bats"]["max"],
            }
        if pitch_num == 1 and not same_inning:
            return {
                "avg": self.avg_pitch_times["time_between_innings"]["avg"],
                "min": self.avg_pitch_times["time_between_innings"]["min"],
                "max": self.avg_pitch_times["time_between_innings"]["max"],
            }
        return {
            "avg": self.avg_pitch_times["time_between_pitches"]["avg"],
            "min": self.avg_pitch_times["time_between_pitches"]["min"],
            "max": self.avg_pitch_times["time_between_pitches"]["max"],
        }

    def delta_avg_time_between_pitches(self, avg, pitch1_thrown, pitch2_thrown):
        return abs(avg - (pitch2_thrown - pitch1_thrown).total_seconds())

    # def correct_pfx_data_using_next_at_bat(self, fix_ab_id, next_ab_id, fix_pfx_data, pitch_count):
    #     result = self.get_first_pitch_thrown_time(next_ab_id)
    #     if result.failure:
    #         return result
    #     next_pitch_thrown = result.value
    #     valid_pfx = []
    #     possible_pfx = deepcopy(fix_pfx_data)
    #     for pitch_num in reversed(range(1, pitch_count + 1)):
    #         matches = [pfx for pfx in fix_pfx_data if pfx["ab_count"] == pitch_num]
    #         if not matches:
    #             error = (
    #                 f"Unable to determine correct pitch sequence for at bat {fix_ab_id} "
    #                 f"(PitchFX contains 0 pitches that are identified as Pitch #{pitch_num})"
    #             )
    #             return Result.Fail(error)
    #         if len(matches) == 1:
    #             valid_pfx.append(matches[0])
    #             possible_pfx.remove(matches[0])
    #             next_pitch_thrown = matches[0]["time_pitch_thrown"]
    #         else:
    #             result = self.determine_best_pfx_from_next_pitch(
    #                 fix_ab_id,
    #                 next_ab_id,
    #                 deepcopy(possible_pfx),
    #                 pitch_count,
    #                 pitch_num,
    #                 next_pitch_thrown,
    #             )
    #             if result.failure:
    #                 return result
    #             best_pfx = result.value
    #             valid_pfx.append(best_pfx)
    #             possible_pfx.remove(best_pfx)
    #             next_pitch_thrown = best_pfx["time_pitch_thrown"]
    #     valid_pfx.reverse()
    #     possible_pfx = self.update_out_of_sequence_pfx(possible_pfx)
    #     removed_pfx = {"removed_count": len(possible_pfx), "removed_dupes": possible_pfx}
    #     return Result.Ok((valid_pfx, removed_pfx))

    # def get_first_pitch_thrown_time(self, at_bat_id):
    #     result = self.get_game_event(at_bat_id)
    #     if result.failure:
    #         return result
    #     at_bat = result.value
    #     if not at_bat:
    #         return Result.Fail(f"No game event found with at bat id {at_bat_id}")
    #     if not self.pitchfx_data_is_complete(at_bat):
    #         return Result.Fail(f"PitchFX data is incorrect or incomplete for at bat {at_bat_id}")
    #     first_pitch_thrown_str = at_bat["pitchfx"][0]["time_pitch_thrown"]
    #     first_pitch_in_at_bat_thrown = datetime.strptime(first_pitch_thrown_str, DT_AWARE)
    #     return Result.Ok(first_pitch_in_at_bat_thrown)

    # def determine_best_pfx_from_next_pitch(
    #     self,
    #     ab_id,
    #     next_ab_id,
    #     possible_pfx,
    #     pitch_count,
    #     pitch_num,
    #     next_pitch_thrown,
    # ):
    #     pitch_times = self.get_pitch_metrics_next_at_bat(ab_id, next_ab_id, pitch_num, pitch_count)
    #     within_boundaries_pfx = []
    #     impossible_pfx = []
    #     for pfx in possible_pfx:
    #         pitch_delta = (next_pitch_thrown - pfx["time_pitch_thrown"]).total_seconds()
    #         if pitch_delta < 0:
    #             impossible_pfx.append((pfx["time_pitch_thrown"].strftime(DT_AWARE), pitch_delta))
    #             continue
    #         if pitch_delta < int(pitch_times["min"]):
    #             impossible_pfx.append((pfx["time_pitch_thrown"].strftime(DT_AWARE), pitch_delta))
    #             continue
    #         if pitch_delta > int(pitch_times["max"]):
    #             impossible_pfx.append((pfx["time_pitch_thrown"].strftime(DT_AWARE), pitch_delta))
    #             continue
    #         within_boundaries_pfx.append(pfx)
    #     if not within_boundaries_pfx:
    #         impossible_pfx_report = ""
    #         for num, (pitch_thrown, between_pitches) in enumerate(impossible_pfx, start=1):
    #             pfx_report = f"invalid_pfx_{num}..: {pitch_thrown} ({between_pitches} seconds)\n"
    #             impossible_pfx_report += pfx_report
    #         impossible_pfx_report.rstrip("\n")
    #         error = (
    #             f"Unable to determine correct pitch sequence for this at bat. Based on the time "
    #             "that the next pitch was thrown, none of the possible pitches "
    #             f"({len(possible_pfx)} total) were thrown at a time that would be considered "
    #             "valid for the previous pitch in the at bat:\n"
    #             f"at_bat_id..........: {ab_id}\n"
    #             f"prev_pitch_number..: {pitch_num}\n"
    #             f"next_pitch_thrown..: {next_pitch_thrown.strftime(DT_AWARE)}\n"
    #             f"valid_range........: {int(pitch_times['min'])} - {int(pitch_times['max'])} seconds\n"
    #             f"{impossible_pfx_report}"
    #         )
    #         return Result.Fail(error)
    #     if len(within_boundaries_pfx) == 1:
    #         return Result.Ok(within_boundaries_pfx[0])
    #     deltas = [
    #         {
    #             "pfx": pfx,
    #             "has_zone_location": pfx["has_zone_location"],
    #             "delta": self.delta_avg_time_between_pitches(
    #                 pitch_times["avg"], pfx["time_pitch_thrown"], next_pitch_thrown
    #             ),
    #         }
    #         for pfx in within_boundaries_pfx
    #     ]
    #     deltas.sort(key=lambda x: (-x["has_zone_location"], x["delta"]))
    #     return Result.Ok(deltas[0]["pfx"])

    # def get_pitch_metrics_next_at_bat(self, at_bat_id, next_ab_id, pitch_num, pitch_count):
    #     same_inning = self.at_bat_ids_are_in_same_inning([at_bat_id, next_ab_id])
    #     if pitch_num == pitch_count and same_inning:
    #         return {
    #             "avg": self.avg_pitch_times["time_between_at_bats"]["avg"],
    #             "min": self.avg_pitch_times["time_between_at_bats"]["min"],
    #             "max": self.avg_pitch_times["time_between_at_bats"]["max"],
    #         }
    #     if pitch_num == pitch_count and not same_inning:
    #         return {
    #             "avg": self.avg_pitch_times["time_between_innings"]["avg"],
    #             "min": self.avg_pitch_times["time_between_innings"]["min"],
    #             "max": self.avg_pitch_times["time_between_innings"]["max"],
    #         }
    #     return {
    #         "avg": self.avg_pitch_times["time_between_pitches"]["avg"],
    #         "min": self.avg_pitch_times["time_between_pitches"]["min"],
    #         "max": self.avg_pitch_times["time_between_pitches"]["max"],
    #     }

    def get_game_event(self, at_bat_id):
        matches = [
            event for event in self.game_events_combined_data if event["at_bat_id"] == at_bat_id
        ]
        if not matches:
            return Result.Ok(None)
        if len(matches) > 1:
            return Result.Fail(f"Found {len(matches)} at bats with the same id: {at_bat_id}")
        return Result.Ok(matches[0])

    def pitchfx_data_is_complete(self, game_event):
        pitchfx_audit = game_event["at_bat_pitchfx_audit"]
        return (
            pitchfx_audit["pitch_count_pitchfx"] > 0
            and pitchfx_audit["pitch_count_bbref"] == pitchfx_audit["pitch_count_pitchfx"]
            and not pitchfx_audit["pitchfx_error"]
        )

    def at_bat_ids_are_in_same_inning(self, at_bat_ids):
        inning_list = {validate_at_bat_id(ab_id).value["inning_id"] for ab_id in at_bat_ids}
        return len(inning_list) == 1

    def update_out_of_sequence_pfx(self, removed_dupes):
        for pfx in removed_dupes:
            pfx["is_out_of_sequence"] = True
        return sorted(removed_dupes, key=lambda x: (x["ab_id"], x["ab_count"]))

    def combine_removed_pitchfx(self, removed_pfx_1, removed_pfx_2):
        removed_dupes = removed_pfx_1["removed_dupes"] + removed_pfx_2["removed_dupes"]
        return {"removed_count": len(removed_dupes), "removed_dupes": removed_dupes}

    def get_missing_pitch_numbers(self, pfx_data, pitch_count):
        pitch_numbers = set(range(1, pitch_count + 1))
        pitch_numbers_pfx = set(pfx["ab_count"] for pfx in pfx_data)
        return list(pitch_numbers - pitch_numbers_pfx)

    def get_at_bat_duration(self, pfx_data):
        if not pfx_data:
            return (0, 0, None, None)
        since_game_start = pfx_data[0]["seconds_since_game_start"]
        first_pitch_thrown = pfx_data[0]["time_pitch_thrown"]
        last_pitch_thrown = pfx_data[-1]["time_pitch_thrown"]
        if not first_pitch_thrown or not last_pitch_thrown:
            return (since_game_start, 0, None, None)
        at_bat_duration = int((last_pitch_thrown - first_pitch_thrown).total_seconds())
        first_pitch_thrown_str = first_pitch_thrown.strftime(DT_AWARE)
        last_pitch_thrown_str = last_pitch_thrown.strftime(DT_AWARE)
        return (since_game_start, at_bat_duration, first_pitch_thrown_str, last_pitch_thrown_str)

    def get_pitch_app_id_from_at_bat_id(self, at_bat_id):
        return validate_at_bat_id(at_bat_id).value["pitch_app_id"]

    def prepare_pfx_data_for_json_serialization(self, pfx_data):
        for pfx_dict in pfx_data:
            pfx_dict.pop("game_start_time", None)
            pfx_dict.pop("time_pitch_thrown", None)

    def construct_pitch_sequence_description(self, at_bat_id, final_event_in_at_bat, pfx_data=None):
        pitch_sequence = final_event_in_at_bat["pitch_sequence"]
        result = self.get_total_pitches_in_sequence(pitch_sequence)
        if result.failure:
            return result
        total_pitches = result.value
        non_batter_events = self.get_all_other_events_for_at_bat(at_bat_id, final_event_in_at_bat)
        current_pitch = 0
        next_pitch_blocked_by_c = False
        sequence_description = []
        for abbrev in pitch_sequence:
            pitch_number = ""
            pfx_des = ""
            blocked_by_c = ""
            result = self.get_pitch_type(abbrev)
            if result.failure:
                return result
            pitch_type = result.value
            outcome = pitch_type["description"]
            if abbrev == "*":
                next_pitch_blocked_by_c = True
                continue
            if pitch_type["pitch_counts"]:
                current_pitch += 1
                space_count = 1 if total_pitches < 10 or current_pitch >= 10 else 2
                pitch_number = f"Pitch{' '*space_count}{current_pitch}/{total_pitches}"
                if pfx_data:
                    pfx = pfx_data[current_pitch - 1]
                    if abbrev == "X":
                        outcome = pfx["pdes"] if "missing_pdes" not in pfx["pdes"] else pfx["des"]
                    pitch_type = PITCH_TYPE_DICT[pfx["mlbam_pitch_name"]]
                    pfx_des = f'{pfx["start_speed"]:02.0f}mph {pitch_type}'
                if next_pitch_blocked_by_c:
                    blocked_by_c = "\n(pitch was blocked by catcher)"
                    next_pitch_blocked_by_c = False
                sequence_description.append((pitch_number, f"{outcome}{blocked_by_c}", pfx_des))
                continue
            if abbrev != ".":
                sequence_description.append(("", outcome, ""))
            else:
                outcome = self.get_next_event_description(non_batter_events, outcome)
                sequence_description.append(("", outcome, ""))
        while any(not event_dict["processed"] for event_dict in non_batter_events.values()):
            outcome = self.get_next_event_description(non_batter_events)
            if outcome:
                sequence_description.append(("", outcome, ""))
        outcome = replace_char_with_newlines(final_event_in_at_bat["play_description"], ";")
        sequence_description.append(("Result", outcome, ""))
        return Result.Ok(sequence_description)

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

    def save_invalid_pitchfx(self, at_bat_ids_invalid_pfx):
        self.invalid_pitchfx = defaultdict(dict)
        for ab_id in self.order_at_bat_ids_by_park_sv_id(at_bat_ids_invalid_pfx):
            inning_id = get_inning_id_from_at_bat_id(ab_id)
            id_dict = PlayerId.get_player_ids_from_at_bat_id(self.db_session, ab_id)
            pfx_data = self.get_all_pfx_data_for_at_bat(ab_id)
            pitch_count = max(pfx["ab_total"] for pfx in pfx_data)
            (pfx_no_dupes, removed_pfx) = self.remove_duplicate_pitches_pfx(pfx_data)
            extra_pfx_count = len(pfx_no_dupes) - pitch_count
            patched_pfx_count = len([pfx for pfx in pfx_no_dupes if pfx["is_patched"]])
            missing_pfx_count = pitch_count - len(pfx_no_dupes)
            missing_pitch_numbers = self.get_missing_pitch_numbers(pfx_no_dupes, pitch_count)
            self.prepare_pfx_data_for_json_serialization(pfx_no_dupes)
            if removed_pfx["removed_count"] > 0:
                self.prepare_pfx_data_for_json_serialization(removed_pfx["removed_dupes"])
            dupe_guids = self.get_all_removed_guid_dupes_for_at_bat(ab_id)
            if dupe_guids["removed_count"] > 0:
                self.prepare_pfx_data_for_json_serialization(dupe_guids["removed_dupes"])
            at_bat_data = {
                "at_bat_id": ab_id,
                "inning_id": inning_id,
                "pitch_app_id": self.get_pitch_app_id_from_at_bat_id(ab_id),
                "pitcher_id_bbref": id_dict["pitcher_id_bbref"],
                "pitcher_id_mlb": id_dict["pitcher_id_mlb"],
                "pitcher_name": id_dict["pitcher_name"],
                "batter_id_bbref": id_dict["batter_id_bbref"],
                "batter_id_mlb": id_dict["batter_id_mlb"],
                "batter_name": id_dict["batter_name"],
                "at_bat_pitchfx_audit": {
                    "pitch_count_bbref": 0,
                    "pitch_count_pitchfx": len(pfx_no_dupes),
                    "patched_pitchfx_count": patched_pfx_count,
                    "missing_pitchfx_count": missing_pfx_count if missing_pfx_count > 0 else 0,
                    "missing_pitch_numbers": missing_pitch_numbers,
                    "extra_pitchfx_count": extra_pfx_count if extra_pfx_count > 0 else 0,
                    "extra_pitchfx_removed_count": removed_pfx["removed_count"],
                    "duplicate_guid_removed_count": dupe_guids["removed_count"],
                },
                "pitchfx": pfx_no_dupes,
                "removed_pitchfx": removed_pfx if removed_pfx["removed_count"] > 0 else {},
                "removed_duplicate_guid": dupe_guids if dupe_guids["removed_count"] > 0 else {},
            }
            self.invalid_pitchfx[inning_id][ab_id] = at_bat_data

    def save_dupe_guids(self, at_bat_ids_dupe_guids):
        self.all_dupe_guids = defaultdict(dict)
        for ab_id in at_bat_ids_dupe_guids:
            inning_id = get_inning_id_from_at_bat_id(ab_id)
            id_dict = PlayerId.get_player_ids_from_at_bat_id(self.db_session, ab_id)
            dupe_guids = self.get_all_removed_guid_dupes_for_at_bat(ab_id)
            self.prepare_pfx_data_for_json_serialization(dupe_guids["removed_dupes"])
            at_bat_data = {
                "at_bat_id": ab_id,
                "inning_id": inning_id,
                "pitch_app_id": self.get_pitch_app_id_from_at_bat_id(ab_id),
                "pitcher_id_bbref": id_dict["pitcher_id_bbref"],
                "pitcher_id_mlb": id_dict["pitcher_id_mlb"],
                "pitcher_name": id_dict["pitcher_name"],
                "batter_id_bbref": id_dict["batter_id_bbref"],
                "batter_id_mlb": id_dict["batter_id_mlb"],
                "batter_name": id_dict["batter_name"],
                "pitchfx": dupe_guids["removed_dupes"],
            }
            self.all_dupe_guids[inning_id][ab_id] = at_bat_data

    def save_removed_pfx(self, at_bat_ids_removed_pfx):
        self.all_removed_pfx = defaultdict(dict)
        for ab_id in at_bat_ids_removed_pfx:
            inning_id = get_inning_id_from_at_bat_id(ab_id)
            id_dict = PlayerId.get_player_ids_from_at_bat_id(self.db_session, ab_id)
            removed_pfx = self.get_all_removed_pfx_for_at_bat(ab_id)
            self.prepare_pfx_data_for_json_serialization(removed_pfx["removed_dupes"])
            at_bat_data = {
                "at_bat_id": ab_id,
                "inning_id": inning_id,
                "pitch_app_id": self.get_pitch_app_id_from_at_bat_id(ab_id),
                "pitcher_id_bbref": id_dict["pitcher_id_bbref"],
                "pitcher_id_mlb": id_dict["pitcher_id_mlb"],
                "pitcher_name": id_dict["pitcher_name"],
                "batter_id_bbref": id_dict["batter_id_bbref"],
                "batter_id_mlb": id_dict["batter_id_mlb"],
                "batter_name": id_dict["batter_name"],
                "pitchfx": removed_pfx["removed_dupes"],
            }
            self.all_removed_pfx[inning_id][ab_id] = at_bat_data

    def update_boxscore_with_combined_data(self):
        updated_innings_list = [
            self.update_inning_with_combined_data(inning) for inning in self.boxscore.innings_list
        ]
        (pitch_stats_away, pitch_stats_home) = self.update_all_pitch_stats()
        (bat_stats_away, bat_stats_home) = self.update_all_bat_stats()

        away_team_data = self.boxscore.away_team_data.as_dict()
        away_team_data.pop("__bbref_boxscore_team_data__", None)
        away_team_data.pop("batting_stats", None)
        away_team_data.pop("pitching_stats", None)
        away_team_data["batting_stats"] = bat_stats_away
        away_team_data["pitching_stats"] = pitch_stats_away

        home_team_data = self.boxscore.home_team_data.as_dict()
        home_team_data.pop("__bbref_boxscore_team_data__", None)
        home_team_data.pop("batting_stats", None)
        home_team_data.pop("pitching_stats", None)
        home_team_data["batting_stats"] = bat_stats_home
        home_team_data["pitching_stats"] = pitch_stats_home

        game_meta_info = self.boxscore.game_meta_info.as_dict()
        game_meta_info.pop("__bbref_boxscore_meta__", None)
        game_meta_info["game_time_hour"] = self.game_start_time.hour
        game_meta_info["game_time_minute"] = self.game_start_time.minute
        game_meta_info["game_date_time_str"] = self.game_start_time.strftime(DT_AWARE)
        game_meta_info["umpires"] = self.boxscore.as_dict()["umpires"]

        pitchfx_vs_bbref_audit = self.audit_pitchfx_vs_bbref_data(
            pitch_stats_away, pitch_stats_home
        )

        updated_boxscore_dict = {
            "bbref_game_id": self.bbref_game_id,
            "boxscore_url": self.boxscore.boxscore_url,
            "pitchfx_vs_bbref_audit": pitchfx_vs_bbref_audit,
            "game_meta_info": game_meta_info,
            "away_team_data": away_team_data,
            "home_team_data": home_team_data,
            "play_by_play_data": updated_innings_list,
            "duplicate_guids": self.all_dupe_guids,
            "removed_pitchfx": self.all_removed_pfx,
            "invalid_pitchfx": self.invalid_pitchfx,
            "player_id_dict": self.player_id_dict,
        }
        return Result.Ok(updated_boxscore_dict)

    def update_inning_with_combined_data(self, inning):
        inning_events = [
            event
            for event in self.game_events_combined_data
            if event["inning_id"] == inning.inning_id
        ]
        inning_totals = {
            "inning_total_runs": inning.inning_total_runs,
            "inning_total_hits": inning.inning_total_hits,
            "inning_total_errors": inning.inning_total_errors,
            "inning_total_left_on_base": inning.inning_total_left_on_base,
            "away_team_runs_after_inning": inning.away_team_runs_after_inning,
            "home_team_runs_after_inning": inning.home_team_runs_after_inning,
        }
        return {
            "inning_id": inning.inning_id,
            "inning_label": inning.inning_label,
            "begin_inning_summary": inning.begin_inning_summary,
            "end_inning_summary": inning.end_inning_summary,
            "inning_totals": inning_totals,
            "inning_pitchfx_audit": self.generate_audit_report_for_events(inning_events),
            "inning_events": inning_events,
        }

    def generate_audit_report_for_events(self, game_events):
        pitch_count_bbref = sum(
            event["at_bat_pitchfx_audit"]["pitch_count_bbref"] for event in game_events
        )
        pitch_count_pitchfx = sum(
            event["at_bat_pitchfx_audit"]["pitch_count_pitchfx"] for event in game_events
        )
        patched_pitchfx_count = sum(
            event["at_bat_pitchfx_audit"]["patched_pitchfx_count"] for event in game_events
        )
        missing_pitchfx_count = sum(
            event["at_bat_pitchfx_audit"]["missing_pitchfx_count"] for event in game_events
        )
        extra_pitchfx_count = sum(
            event["at_bat_pitchfx_audit"]["extra_pitchfx_count"] for event in game_events
        )
        extra_pitchfx_removed_count = sum(
            event["at_bat_pitchfx_audit"]["extra_pitchfx_removed_count"] for event in game_events
        )
        duplicate_guid_removed_count = sum(
            event["at_bat_pitchfx_audit"]["duplicate_guid_removed_count"] for event in game_events
        )
        pitchfx_error = any(event["at_bat_pitchfx_audit"]["pitchfx_error"] for event in game_events)

        at_bat_ids_pitchfx_complete = list(
            set(
                event["at_bat_id"]
                for event in game_events
                if event["at_bat_pitchfx_audit"]["missing_pitchfx_count"] == 0
                and event["at_bat_pitchfx_audit"]["extra_pitchfx_count"] == 0
                and not event["at_bat_pitchfx_audit"]["pitchfx_error"]
            )
        )
        at_bat_ids_patched_pitchfx = list(
            set(
                event["at_bat_id"]
                for event in game_events
                if event["at_bat_pitchfx_audit"]["patched_pitchfx_count"] > 0
            )
        )
        at_bat_ids_missing_pitchfx = list(
            set(
                event["at_bat_id"]
                for event in game_events
                if event["at_bat_pitchfx_audit"]["missing_pitchfx_count"] > 0
            )
        )
        at_bat_ids_extra_pitchfx = list(
            set(
                event["at_bat_id"]
                for event in game_events
                if event["at_bat_pitchfx_audit"]["extra_pitchfx_count"] > 0
            )
        )
        at_bat_ids_extra_pitchfx_removed = list(
            set(
                event["at_bat_id"]
                for event in game_events
                if event["at_bat_pitchfx_audit"]["extra_pitchfx_removed_count"] > 0
            )
        )
        at_bat_ids_duplicate_guid_removed = list(
            set(
                event["at_bat_id"]
                for event in game_events
                if event["at_bat_pitchfx_audit"]["duplicate_guid_removed_count"] > 0
            )
        )
        at_bat_ids_pitchfx_error = list(
            set(
                event["at_bat_id"]
                for event in game_events
                if event["at_bat_pitchfx_audit"]["pitchfx_error"]
            )
        )

        at_bat_ids_pitchfx_complete = self.order_at_bat_ids_by_time(at_bat_ids_pitchfx_complete)
        at_bat_ids_patched_pitchfx = self.order_at_bat_ids_by_time(at_bat_ids_patched_pitchfx)
        at_bat_ids_missing_pitchfx = self.order_at_bat_ids_by_time(at_bat_ids_missing_pitchfx)
        at_bat_ids_extra_pitchfx = self.order_at_bat_ids_by_time(at_bat_ids_extra_pitchfx)
        at_bat_ids_extra_pitchfx_removed = self.order_at_bat_ids_by_time(
            at_bat_ids_extra_pitchfx_removed
        )
        at_bat_ids_duplicate_guid_removed = self.order_at_bat_ids_by_time(
            at_bat_ids_duplicate_guid_removed
        )
        at_bat_ids_pitchfx_error = self.order_at_bat_ids_by_time(at_bat_ids_pitchfx_error)

        return {
            "pitchfx_error": pitchfx_error,
            "pitch_count_bbref": pitch_count_bbref,
            "pitch_count_pitchfx": pitch_count_pitchfx,
            "patched_pitchfx_count": patched_pitchfx_count,
            "missing_pitchfx_count": missing_pitchfx_count,
            "extra_pitchfx_count": extra_pitchfx_count,
            "duplicate_guid_removed_count": duplicate_guid_removed_count,
            "extra_pitchfx_removed_count": extra_pitchfx_removed_count,
            "total_at_bats_pitchfx_complete": len(at_bat_ids_pitchfx_complete),
            "total_at_bats_patched_pitchfx": len(at_bat_ids_patched_pitchfx),
            "total_at_bats_missing_pitchfx": len(at_bat_ids_missing_pitchfx),
            "total_at_bats_extra_pitchfx": len(at_bat_ids_extra_pitchfx),
            "total_at_bats_extra_pitchfx_removed": len(at_bat_ids_extra_pitchfx_removed),
            "total_at_bats_duplicate_guid_removed": len(at_bat_ids_duplicate_guid_removed),
            "total_at_bats_pitchfx_error": len(at_bat_ids_pitchfx_error),
            "at_bat_ids_pitchfx_complete": at_bat_ids_pitchfx_complete,
            "at_bat_ids_patched_pitchfx": at_bat_ids_patched_pitchfx,
            "at_bat_ids_missing_pitchfx": at_bat_ids_missing_pitchfx,
            "at_bat_ids_extra_pitchfx": at_bat_ids_extra_pitchfx,
            "at_bat_ids_extra_pitchfx_removed": at_bat_ids_extra_pitchfx_removed,
            "at_bat_ids_duplicate_guid_removed": at_bat_ids_duplicate_guid_removed,
            "at_bat_ids_pitchfx_error": at_bat_ids_pitchfx_error,
        }

    def update_all_pitch_stats(self):
        pitch_stats_dict = {}
        all_bbref_pitch_stats = deepcopy(self.boxscore.away_team_data.pitching_stats)
        all_bbref_pitch_stats.extend(deepcopy(self.boxscore.home_team_data.pitching_stats))
        for pitch_stats in all_bbref_pitch_stats:
            mlb_id = self.player_id_dict[pitch_stats.player_id_br]["mlb_id"]
            pitch_stats_dict[mlb_id] = pitch_stats
        updated_pitcher_ids = []
        updated_pitching_stats = defaultdict(list)
        for pfx_log in self.pitchfx_logs_for_game:
            pitch_stats = pitch_stats_dict.pop(pfx_log.pitcher_id_mlb, None)
            if not pitch_stats:
                error = f"Error retrieving boxscore stats for pitch app: {pfx_log.pitch_app_id}"
                return Result.Fail(error)
            (team_id, updated_stats) = self.update_player_pitch_stats(pfx_log, pitch_stats)
            updated_pitching_stats[team_id].append(updated_stats)
            updated_pitcher_ids.append(pfx_log.pitcher_id_mlb)
        for pitch_stats in pitch_stats_dict.values():
            (team_id, updated_stats) = self.update_player_pitch_stats_no_pfx(pitch_stats)
            updated_pitching_stats[team_id].append(updated_stats)
            pitcher_id_mlb = self.player_id_dict[pitch_stats.player_id_br].get("mlb_id")
            updated_pitcher_ids.append(pitcher_id_mlb)
        pitcher_ids_invalid_pfx = self.get_pitcher_ids_with_invalid_pfx()
        invalid_pitcher_ids = list(set(pitcher_ids_invalid_pfx) - set(updated_pitcher_ids))
        if invalid_pitcher_ids:
            for pitcher_id in invalid_pitcher_ids:
                (team_id, updated_stats) = self.generate_pitch_stats_only_invalid_pfx(pitcher_id)
                updated_pitching_stats[team_id].append(updated_stats)
        return (
            updated_pitching_stats[self.away_team_id_br],
            updated_pitching_stats[self.home_team_id_br],
        )

    def update_player_pitch_stats(self, pfx_log, pitch_stats):
        bbref_data = pitch_stats.as_dict()
        bbref_data.pop("player_id_br", None)
        bbref_data.pop("player_team_id_br", None)
        bbref_data.pop("opponent_team_id_br", None)
        pitcher_events = [
            game_event
            for game_event in self.game_events_combined_data
            if game_event["pitcher_id_mlb"] == pfx_log.pitcher_id_mlb
        ]
        batters_faced_pfx = len([event for event in pitcher_events if event["is_complete_at_bat"]])
        audit_report = self.generate_audit_report_for_events(pitcher_events)
        pitch_count_by_pitch_type = self.get_pitch_count_by_pitch_type(pitcher_events)
        invalid_pfx = self.get_invalid_pfx_data_for_pitcher(pfx_log.pitcher_id_mlb)
        pitch_app_pitchfx_audit = {
            "invalid_pitchfx": invalid_pfx["invalid_pitchfx"],
            "pitchfx_error": audit_report["pitchfx_error"],
            "pitch_count_bbref": audit_report["pitch_count_bbref"],
            "pitch_count_pitchfx": audit_report["pitch_count_pitchfx"],
            "patched_pitchfx_count": audit_report["patched_pitchfx_count"],
            "missing_pitchfx_count": audit_report["missing_pitchfx_count"],
            "extra_pitchfx_count": audit_report["extra_pitchfx_count"],
            "invalid_pitchfx_count": invalid_pfx["invalid_pitchfx_count"],
            "extra_pitchfx_removed_count": audit_report["extra_pitchfx_removed_count"],
            "duplicate_guid_removed_count": pfx_log.duplicate_guid_removed_count,
            "batters_faced_bbref": pitch_stats.batters_faced,
            "batters_faced_pitchfx": batters_faced_pfx,
            "total_at_bats_pitchfx_complete": audit_report["total_at_bats_pitchfx_complete"],
            "total_at_bats_extra_pitchfx": audit_report["total_at_bats_extra_pitchfx"],
            "total_at_bats_patched_pitchfx": audit_report["total_at_bats_patched_pitchfx"],
            "total_at_bats_missing_pitchfx": audit_report["total_at_bats_missing_pitchfx"],
            "total_at_bats_extra_pitchfx_removed": audit_report[
                "total_at_bats_extra_pitchfx_removed"
            ],
            "total_at_bats_duplicate_guid_removed": audit_report[
                "total_at_bats_duplicate_guid_removed"
            ],
            "total_at_bats_pitchfx_error": audit_report["total_at_bats_pitchfx_error"],
            "total_at_bats_invalid_pitchfx": invalid_pfx["total_at_bats_invalid_pitchfx"],
            "at_bat_ids_pitchfx_complete": audit_report["at_bat_ids_pitchfx_complete"],
            "at_bat_ids_patched_pitchfx": audit_report["at_bat_ids_patched_pitchfx"],
            "at_bat_ids_missing_pitchfx": audit_report["at_bat_ids_missing_pitchfx"],
            "at_bat_ids_extra_pitchfx": audit_report["at_bat_ids_extra_pitchfx"],
            "at_bat_ids_extra_pitchfx_removed": audit_report["at_bat_ids_extra_pitchfx_removed"],
            "at_bat_ids_duplicate_guid_removed": audit_report["at_bat_ids_duplicate_guid_removed"],
            "at_bat_ids_pitchfx_error": audit_report["at_bat_ids_pitchfx_error"],
            "at_bat_ids_invalid_pitchfx": invalid_pfx["at_bat_ids_invalid_pitchfx"],
        }

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
            "pitch_count_by_pitch_type": pitch_count_by_pitch_type,
            "pitch_app_pitchfx_audit": pitch_app_pitchfx_audit,
            "bbref_data": bbref_data,
        }
        return (pitch_stats.player_team_id_br, updated_stats)

    def get_pitch_count_by_pitch_type(self, pitcher_events):
        all_pfx = flatten_list2d([event["pitchfx"] for event in pitcher_events])
        pitch_count_unordered = defaultdict(int)
        for pfx in all_pfx:
            pitch_count_unordered[pfx["mlbam_pitch_name"]] += 1
        pitch_count_ordered = OrderedDict()
        ptype_tuples = [(pitch_type, count) for pitch_type, count in pitch_count_unordered.items()]
        for t in sorted(ptype_tuples, key=lambda x: -x[1]):
            pitch_count_ordered[t[0]] = t[1]
        return pitch_count_ordered

    def get_invalid_pfx_data_for_pitcher(self, pitcher_id_mlb):
        invalid_pfx = {
            "invalid_pitchfx": False,
            "invalid_pitchfx_count": 0,
            "total_at_bats_invalid_pitchfx": 0,
            "at_bat_ids_invalid_pitchfx": [],
        }
        if not self.invalid_pitchfx:
            return invalid_pfx
        pfx_data = flatten_list2d(
            [
                at_bat_data["pitchfx"]
                for invalid_pfx_at_bat_dict in self.invalid_pitchfx.values()
                for at_bat_data in invalid_pfx_at_bat_dict.values()
                if at_bat_data["pitcher_id_mlb"] == pitcher_id_mlb
            ]
        )
        if not pfx_data:
            return invalid_pfx
        at_bat_ids_invalid_pfx = list(set(pfx["at_bat_id"] for pfx in pfx_data))
        at_bat_ids_invalid_pfx = self.order_at_bat_ids_by_park_sv_id(at_bat_ids_invalid_pfx)
        return {
            "invalid_pitchfx": True,
            "invalid_pitchfx_count": len(pfx_data),
            "total_at_bats_invalid_pitchfx": len(at_bat_ids_invalid_pfx),
            "at_bat_ids_invalid_pitchfx": at_bat_ids_invalid_pfx,
        }

    def get_pitcher_ids_with_invalid_pfx(self):
        pitcher_ids = {
            at_bat_data["pitcher_id_mlb"]
            for invalid_pfx_at_bat_dict in self.invalid_pitchfx.values()
            for at_bat_data in invalid_pfx_at_bat_dict.values()
        }
        return list(pitcher_ids)

    def order_at_bat_ids_by_park_sv_id(self, at_bat_ids):
        park_sv_id_map = [
            {
                "at_bat_id": ab_id,
                "park_sv_id": min(
                    pfx.park_sv_id for pfx in self.all_pfx_data_for_game if pfx.at_bat_id == ab_id
                ),
            }
            for ab_id in at_bat_ids
        ]
        park_sv_id_map.sort(key=lambda x: x["park_sv_id"])
        return [id_map["at_bat_id"] for id_map in park_sv_id_map]

    def update_player_pitch_stats_no_pfx(self, pitch_stats):
        bbref_id = pitch_stats.player_id_br
        mlb_id = self.player_id_dict[bbref_id].get("mlb_id", "")
        pitcher_events = [
            game_event
            for game_event in self.game_events_combined_data
            if game_event["pitcher_id_mlb"] == mlb_id
        ]
        at_bat_ids_missing_pitchfx = list(set([event["at_bat_id"] for event in pitcher_events]))
        at_bat_ids_missing_pitchfx = self.order_at_bat_ids_by_time(at_bat_ids_missing_pitchfx)
        bbref_data = pitch_stats.as_dict()
        bbref_data.pop("player_id_br", None)
        bbref_data.pop("player_team_id_br", None)
        bbref_data.pop("opponent_team_id_br", None)

        pitch_app_pitchfx_audit = {
            "invalid_pitchfx": False,
            "pitchfx_error": False,
            "pitch_count_bbref": pitch_stats.pitch_count,
            "pitch_count_pitchfx": 0,
            "patched_pitchfx_count": 0,
            "missing_pitchfx_count": pitch_stats.pitch_count,
            "extra_pitchfx_count": 0,
            "invalid_pitchfx_count": 0,
            "extra_pitchfx_removed_count": 0,
            "duplicate_guid_removed_count": 0,
            "batters_faced_bbref": pitch_stats.batters_faced,
            "batters_faced_pitchfx": 0,
            "total_at_bats_pitchfx_complete": 0,
            "total_at_bats_patched_pitchfx": 0,
            "total_at_bats_missing_pitchfx": len(at_bat_ids_missing_pitchfx),
            "total_at_bats_extra_pitchfx": 0,
            "total_at_bats_extra_pitchfx_removed": 0,
            "total_at_bats_pitchfx_error": 0,
            "total_at_bats_invalid_pitchfx": 0,
            "at_bat_ids_pitchfx_complete": [],
            "at_bat_ids_patched_pitchfx": [],
            "at_bat_ids_missing_pitchfx": at_bat_ids_missing_pitchfx,
            "at_bat_ids_extra_pitchfx": [],
            "at_bat_ids_extra_pitchfx_removed": [],
            "at_bat_ids_pitchfx_error": [],
            "at_bat_ids_invalid_pitchfx": [],
        }

        updated_stats = {
            "pitcher_name": self.player_id_dict[bbref_id].get("name", ""),
            "pitcher_id_mlb": mlb_id,
            "pitcher_id_bbref": bbref_id,
            "pitch_app_id": f"{self.bbref_game_id}_{mlb_id}",
            "pitcher_team_id_bb": get_brooks_team_id(pitch_stats.player_team_id_br),
            "pitcher_team_id_bbref": pitch_stats.player_team_id_br,
            "opponent_team_id_bb": get_brooks_team_id(pitch_stats.opponent_team_id_br),
            "opponent_team_id_bbref": pitch_stats.opponent_team_id_br,
            "bb_game_id": self.boxscore.bb_game_id,
            "bbref_game_id": self.bbref_game_id,
            "pitch_count_by_inning": [],
            "pitch_app_pitchfx_audit": pitch_app_pitchfx_audit,
            "bbref_data": bbref_data,
        }
        return (pitch_stats.player_team_id_br, updated_stats)

    def generate_pitch_stats_only_invalid_pfx(self, pitcher_id_mlb):
        pitcher_events = flatten_list2d(
            [
                at_bat_data
                for invalid_pfx_at_bat_dict in self.invalid_pitchfx.values()
                for at_bat_data in invalid_pfx_at_bat_dict.values()
                if at_bat_data["pitcher_id_mlb"] == pitcher_id_mlb
            ]
        )
        batters_faced_pfx = len([event for event in pitcher_events if event["is_complete_at_bat"]])
        audit_report = self.generate_audit_report_for_events(pitcher_events)
        invalid_pfx = self.get_invalid_pfx_data_for_pitcher(pitcher_id_mlb)
        pitch_app_pitchfx_audit = {
            "invalid_pitchfx": invalid_pfx["invalid_pitchfx"],
            "pitchfx_error": audit_report["pitchfx_error"],
            "pitch_count_bbref": audit_report["pitch_count_bbref"],
            "pitch_count_pitchfx": audit_report["pitch_count_pitchfx"],
            "patched_pitchfx_count": audit_report["patched_pitchfx_count"],
            "missing_pitchfx_count": audit_report["missing_pitchfx_count"],
            "extra_pitchfx_count": audit_report["extra_pitchfx_count"],
            "invalid_pitchfx_count": invalid_pfx["invalid_pitchfx_count"],
            "extra_pitchfx_removed_count": audit_report["extra_pitchfx_removed_count"],
            "duplicate_guid_removed_count": 0,
            "batters_faced_bbref": 0,
            "batters_faced_pitchfx": batters_faced_pfx,
            "total_at_bats_pitchfx_complete": audit_report["total_at_bats_pitchfx_complete"],
            "total_at_bats_extra_pitchfx": audit_report["total_at_bats_extra_pitchfx"],
            "total_at_bats_patched_pitchfx": audit_report["total_at_bats_patched_pitchfx"],
            "total_at_bats_missing_pitchfx": audit_report["total_at_bats_missing_pitchfx"],
            "total_at_bats_extra_pitchfx_removed": audit_report[
                "total_at_bats_extra_pitchfx_removed"
            ],
            "total_at_bats_duplicate_guid_removed": audit_report[
                "total_at_bats_duplicate_guid_removed"
            ],
            "total_at_bats_pitchfx_error": audit_report["total_at_bats_pitchfx_error"],
            "total_at_bats_invalid_pitchfx": invalid_pfx["total_at_bats_invalid_pitchfx"],
            "at_bat_ids_pitchfx_complete": audit_report["at_bat_ids_pitchfx_complete"],
            "at_bat_ids_patched_pitchfx": audit_report["at_bat_ids_patched_pitchfx"],
            "at_bat_ids_missing_pitchfx": audit_report["at_bat_ids_missing_pitchfx"],
            "at_bat_ids_extra_pitchfx": audit_report["at_bat_ids_extra_pitchfx"],
            "at_bat_ids_extra_pitchfx_removed": audit_report["at_bat_ids_extra_pitchfx_removed"],
            "at_bat_ids_duplicate_guid_removed": audit_report["at_bat_ids_duplicate_guid_removed"],
            "at_bat_ids_pitchfx_error": audit_report["at_bat_ids_pitchfx_error"],
            "at_bat_ids_invalid_pitchfx": invalid_pfx["at_bat_ids_invalid_pitchfx"],
        }

        at_bat_ids = [event["at_bat_id"] for event in pitcher_events]
        id_dict = PlayerId.get_player_ids_from_at_bat_id(self.db_session, at_bat_ids[0])
        all_pfx = flatten_list2d([event["pitchfx"] for event in pitcher_events])
        updated_stats = {
            "pitcher_name": id_dict["pitcher_name"],
            "pitcher_id_mlb": id_dict["pitcher_id_mlb"],
            "pitcher_id_bbref": id_dict["pitcher_id_bbref"],
            "pitch_app_id": f"{self.bbref_game_id}_{id_dict['pitcher_id_mlb']}",
            "pitcher_team_id_bb": get_brooks_team_id(id_dict["pitcher_team"]),
            "pitcher_team_id_bbref": id_dict["pitcher_team"],
            "opponent_team_id_bb": get_brooks_team_id(id_dict["batter_team"]),
            "opponent_team_id_bbref": id_dict["batter_team"],
            "bb_game_id": self.boxscore.bb_game_id,
            "bbref_game_id": self.bbref_game_id,
            "pitch_count_by_inning": self.get_pitch_count_by_inning(all_pfx),
            "pitch_app_pitchfx_audit": pitch_app_pitchfx_audit,
            "bbref_data": {},
        }
        return (id_dict["pitcher_team"], updated_stats)

    def update_all_bat_stats(self):
        all_bbref_bat_stats = self.boxscore.away_team_data.batting_stats
        all_bbref_bat_stats.extend(self.boxscore.home_team_data.batting_stats)
        updated_batting_stats = defaultdict(list)
        for bat_stats in all_bbref_bat_stats:
            bbref_id = bat_stats.player_id_br
            mlb_id = self.player_id_dict[bbref_id]["mlb_id"]
            (team_id, updated_stats) = self.update_player_bat_stats(bbref_id, mlb_id, bat_stats)
            updated_batting_stats[team_id].append(updated_stats)
        return (
            updated_batting_stats[self.away_team_id_br],
            updated_batting_stats[self.home_team_id_br],
        )

    def update_player_bat_stats(self, bbref_id, mlb_id, bat_stats):
        bbref_data = bat_stats.as_dict()
        bbref_data.pop("player_id_br", None)
        batter_team_id_bbref = bbref_data.pop("player_team_id_br", None)
        opponent_team_id_bbref = bbref_data.pop("opponent_team_id_br", None)
        batter_events = [
            game_event
            for game_event in self.game_events_combined_data
            if game_event["batter_id_mlb"] == mlb_id
        ]
        all_at_bat_ids = [event["at_bat_id"] for event in batter_events]
        incomplete_at_bat_ids = [
            event["at_bat_id"] for event in batter_events if not event["is_complete_at_bat"]
        ]
        updated_stats = {
            "batter_name": self.player_id_dict[bbref_id]["name"],
            "batter_id_mlb": mlb_id,
            "batter_id_bbref": bbref_id,
            "batter_team_id_bb": get_brooks_team_id(batter_team_id_bbref),
            "batter_team_id_bbref": batter_team_id_bbref,
            "opponent_team_id_bb": get_brooks_team_id(batter_team_id_bbref),
            "opponent_team_id_bbref": opponent_team_id_bbref,
            "total_pbp_events": len(all_at_bat_ids),
            "total_incomplete_at_bats": len(incomplete_at_bat_ids),
            "total_plate_appearances": bbref_data["plate_appearances"],
            "at_bat_ids": all_at_bat_ids,
            "incomplete_at_bat_ids": incomplete_at_bat_ids,
            "bbref_data": bbref_data,
        }
        return (batter_team_id_bbref, updated_stats)

    def audit_pitchfx_vs_bbref_data(self, away_team_pitching_stats, home_team_pitching_stats):
        batters_faced_bbref_home = sum(
            pitch_stats["pitch_app_pitchfx_audit"]["batters_faced_bbref"]
            for pitch_stats in home_team_pitching_stats
        )
        batters_faced_bbref_away = sum(
            pitch_stats["pitch_app_pitchfx_audit"]["batters_faced_bbref"]
            for pitch_stats in away_team_pitching_stats
        )
        batters_faced_bbref = batters_faced_bbref_home + batters_faced_bbref_away

        batters_faced_pitchfx_home = sum(
            pitch_stats["pitch_app_pitchfx_audit"]["batters_faced_pitchfx"]
            for pitch_stats in home_team_pitching_stats
        )
        batters_faced_pitchfx_away = sum(
            pitch_stats["pitch_app_pitchfx_audit"]["batters_faced_pitchfx"]
            for pitch_stats in away_team_pitching_stats
        )
        batters_faced_pitchfx = batters_faced_pitchfx_home + batters_faced_pitchfx_away

        pitch_count_bbref_stats_table_home = sum(
            pitch_stats["bbref_data"]["pitch_count"] for pitch_stats in home_team_pitching_stats
        )
        pitch_count_bbref_stats_table_away = sum(
            pitch_stats["bbref_data"]["pitch_count"] for pitch_stats in away_team_pitching_stats
        )
        pitch_count_bbref_stats_table = (
            pitch_count_bbref_stats_table_home + pitch_count_bbref_stats_table_away
        )

        duplicate_pfx_removed_home = sum(
            pitch_stats["pitch_app_pitchfx_audit"]["duplicate_guid_removed_count"]
            for pitch_stats in home_team_pitching_stats
        )
        duplicate_pfx_removed_away = sum(
            pitch_stats["pitch_app_pitchfx_audit"]["duplicate_guid_removed_count"]
            for pitch_stats in away_team_pitching_stats
        )
        duplicate_guid_removed_count = duplicate_pfx_removed_home + duplicate_pfx_removed_away
        audit_report = self.generate_audit_report_for_events(self.game_events_combined_data)

        at_bat_ids_invalid_pfx = [
            at_bat_ids
            for invalid_pfx_at_bat_dict in self.invalid_pitchfx.values()
            for at_bat_ids in invalid_pfx_at_bat_dict.keys()
        ]
        at_bat_ids_invalid_pfx = self.order_at_bat_ids_by_park_sv_id(at_bat_ids_invalid_pfx)
        total_pitches_invalid_pfx = sum(
            len(at_bat_data["pitchfx"])
            for invalid_pfx_at_bat_dict in self.invalid_pitchfx.values()
            for at_bat_data in invalid_pfx_at_bat_dict.values()
        )
        invalid_pfx = {
            "invalid_pitchfx": True if self.invalid_pitchfx else False,
            "invalid_pitchfx_count": total_pitches_invalid_pfx,
            "total_at_bats_invalid_pitchfx": len(at_bat_ids_invalid_pfx),
            "at_bat_ids_invalid_pitchfx": at_bat_ids_invalid_pfx,
        }
        pitchfx_vs_bbref_audit = {
            "invalid_pitchfx": invalid_pfx["invalid_pitchfx"],
            "pitchfx_error": audit_report["pitchfx_error"],
            "pitch_count_bbref_stats_table": pitch_count_bbref_stats_table,
            "pitch_count_bbref": audit_report["pitch_count_bbref"],
            "pitch_count_pitchfx": audit_report["pitch_count_pitchfx"],
            "patched_pitchfx_count": audit_report["patched_pitchfx_count"],
            "missing_pitchfx_count": audit_report["missing_pitchfx_count"],
            "extra_pitchfx_count": audit_report["extra_pitchfx_count"],
            "invalid_pitchfx_count": invalid_pfx["invalid_pitchfx_count"],
            "extra_pitchfx_removed_count": audit_report["extra_pitchfx_removed_count"],
            "duplicate_guid_removed_count": duplicate_guid_removed_count,
            "batters_faced_bbref": batters_faced_bbref,
            "batters_faced_pitchfx": batters_faced_pitchfx,
            "total_at_bats_pitchfx_complete": audit_report["total_at_bats_pitchfx_complete"],
            "total_at_bats_extra_pitchfx": audit_report["total_at_bats_extra_pitchfx"],
            "total_at_bats_patched_pitchfx": audit_report["total_at_bats_patched_pitchfx"],
            "total_at_bats_missing_pitchfx": audit_report["total_at_bats_missing_pitchfx"],
            "total_at_bats_extra_pitchfx_removed": audit_report[
                "total_at_bats_extra_pitchfx_removed"
            ],
            "total_at_bats_duplicate_guid_removed": audit_report[
                "total_at_bats_duplicate_guid_removed"
            ],
            "total_at_bats_pitchfx_error": audit_report["total_at_bats_pitchfx_error"],
            "total_at_bats_invalid_pitchfx": invalid_pfx["total_at_bats_invalid_pitchfx"],
            "at_bat_ids_pitchfx_complete": audit_report["at_bat_ids_pitchfx_complete"],
            "at_bat_ids_patched_pitchfx": audit_report["at_bat_ids_patched_pitchfx"],
            "at_bat_ids_missing_pitchfx": audit_report["at_bat_ids_missing_pitchfx"],
            "at_bat_ids_extra_pitchfx": audit_report["at_bat_ids_extra_pitchfx"],
            "at_bat_ids_extra_pitchfx_removed": audit_report["at_bat_ids_extra_pitchfx_removed"],
            "at_bat_ids_duplicate_guid_removed": audit_report["at_bat_ids_duplicate_guid_removed"],
            "at_bat_ids_pitchfx_error": audit_report["at_bat_ids_pitchfx_error"],
            "at_bat_ids_invalid_pitchfx": invalid_pfx["at_bat_ids_invalid_pitchfx"],
        }
        return pitchfx_vs_bbref_audit

    def update_game_status(self, success):
        if self.update_db:
            self.game_status.combined_data_success = 1 if success else 0
            self.game_status.combined_data_fail = 0 if success else 1
            self.db_session.commit()
