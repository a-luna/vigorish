from collections import defaultdict
from copy import deepcopy

from dacite import from_dict
from events import Events
from tabulate import tabulate

from vigorish.enums import DataSet, PatchType
from vigorish.patch.brooks_pitchfx import (
    BrooksPitchFxPatchList,
    PatchBrooksPitchFxBatterId,
    PatchBrooksPitchFxPitcherId,
)
from vigorish.tasks.base import Task
from vigorish.tasks.combine_scraped_data import CombineScrapedDataTask
from vigorish.util.result import Result
from vigorish.util.string_helpers import validate_at_bat_id

DATA_SET = DataSet.BROOKS_PITCHFX
PATCH_STAT_NAMES = [
    "batters_faced_bbref",
    "batters_faced_pitchfx",
    "total_at_bats_pitchfx_complete",
    "total_at_bats_patched_pitchfx",
    "total_at_bats_missing_pitchfx",
    "total_at_bats_extra_pitchfx",
    "total_at_bats_extra_pitchfx_removed",
    "total_at_bats_duplicate_guid_removed",
    "total_at_bats_pitchfx_error",
    "total_at_bats_invalid_pitchfx",
    "pitch_count_bbref",
    "pitch_count_pitchfx",
    "patched_pitchfx_count",
    "missing_pitchfx_count",
    "extra_pitchfx_count",
    "extra_pitchfx_removed_count",
    "total_at_bats_extra_pitchfx_removed",
    "duplicate_guid_removed_count",
    "total_at_bats_pitchfx_error",
    "invalid_pitchfx_count",
]


class PatchInvalidPitchFxTask(Task):
    def __init__(self, app):
        super().__init__(app)
        self.combine_data = CombineScrapedDataTask(app)
        self.events = Events(
            (
                "error_occurred",
                "create_invalid_pfx_map_start",
                "create_invalid_pfx_map_complete",
                "match_missing_pfx_data_start",
                "match_missing_pfx_data_complete",
                "create_patch_list_start",
                "create_patch_list_complete",
                "combine_scraped_data_start",
                "combine_scraped_data_complete",
            )
        )

    def execute(self, bbref_game_id, no_prompts=False):
        self.bbref_game_id = bbref_game_id
        self.no_prompts = no_prompts
        result = self.execute_no_prompts() if no_prompts else self.execute_with_prompts()
        if result.failure:
            self.events.error_occurred(result.error)
        return result

    def execute_with_prompts(self):
        self.initialize_attributes()
        result = self.verify_scraped_data_can_be_combined()
        if result.failure:
            return result
        if not self.game_contains_invalid_pfx():
            return Result.Fail(f"{self.bbref_game_id} does not contain any PitchFX data errors")
        return self.get_invalid_pfx_map()

    def execute_no_prompts(self):
        return (
            self.initialize_attributes()
            .on_success(self.verify_scraped_data_can_be_combined)
            .on_success(self.get_invalid_pfx_map)
            .on_success(self.match_missing_pfx_data)
            .on_success(self.create_patch_list)
            .on_success(self.apply_patch_list)
        )

    def initialize_attributes(self):
        self.invalid_pfx_map = {}
        self.game_events = {}
        self.boxscore = {}
        self.match_results = defaultdict(list)
        self.patch_list = None
        return Result.Ok()

    def verify_scraped_data_can_be_combined(self):
        results = self.combine_data.investigate(self.bbref_game_id, apply_patch_list=True)
        if not results["get_all_pbp_events_success"]:
            error_message = "Failed to process play-by-play events and identify at_bat_ids"
        elif not results["get_all_pfx_data_success"]:
            error_message = "Failed to process PitchFX logs and group by at_bat_id"
        elif not results["combine_data_success"]:
            error_message = "Failed to combine play-by-play and PitchFX data"
        elif not results["update_boxscore_success"]:
            error_message = "Failed to update boxscore with combined play-by-play and PitchFX data"
        else:
            self.game_events = results["game_events_combined_data"]
            self.boxscore = results["boxscore"]
            return Result.Ok()
        return Result.Fail(error_message)

    def game_contains_invalid_pfx(self):
        return self.boxscore["pitchfx_vs_bbref_audit"]["invalid_pitchfx"]

    def game_contains_pfx_errors(self):
        return self.boxscore["pitchfx_vs_bbref_audit"]["pitchfx_error"]

    def game_does_not_contain_any_errors(self):
        return not (self.game_contains_pfx_errors() or self.game_contains_invalid_pfx())

    def get_invalid_pfx_map(self):
        if not self.game_contains_invalid_pfx():
            return Result.Ok()
        self.events.create_invalid_pfx_map_start()
        for inning_id, ab_dict in self.boxscore["invalid_pitchfx"].items():
            for at_bat_id, at_bat_data in ab_dict.items():
                pitch_app_id = at_bat_data["pitch_app_id"]
                if pitch_app_id not in self.invalid_pfx_map:
                    self.invalid_pfx_map[pitch_app_id] = defaultdict(dict)
                self.invalid_pfx_map[pitch_app_id][inning_id][at_bat_id] = {
                    "at_bat_id": at_bat_id,
                    "inning_id": inning_id,
                    "pitch_app_id": pitch_app_id,
                    "at_bat_data": at_bat_data,
                }
        self.events.create_invalid_pfx_map_complete(self.invalid_pfx_map)
        return Result.Ok()

    def match_missing_pfx_data(self):
        if not self.game_contains_invalid_pfx():
            return Result.Ok()
        self.events.match_missing_pfx_data_start()
        for pfx in self.get_invalid_pfx_dict_list():
            matches = self.find_game_events_matching_this_pfx(pfx)
            if not matches:
                match_dict = {"success": False, "invalid_pfx": pfx, "missing_pfx": {}}
                self.match_results["no_matches"].append(match_dict)
                continue
            if len(matches) > 1:
                result = self.check_for_exact_match(pfx)
                if result.failure:
                    match_dict = {"success": False, "invalid_pfx": pfx, "missing_pfx": matches}
                    self.match_results["many_matches"].append(match_dict)
                    continue
                exact_match = result.value
            else:
                exact_match = matches[0]
            match_dict = self.found_successful_match(pfx, exact_match)
            self.match_results["success"].append(match_dict)
        self.events.match_missing_pfx_data_complete(self.match_results)
        return Result.Ok()

    def get_invalid_pfx_dict_list(self):
        return [
            {
                "source": "invalid_pfx",
                "at_bat_id": at_bat_id,
                "inning_id": inning_id[-5:],
                "pitcher_id": ab_dict["at_bat_data"]["pitcher_id_mlb"],
                "pitcher": self.get_pitcher_name_and_team(ab_dict["at_bat_data"]),
                "batter_id": ab_dict["at_bat_data"]["batter_id_mlb"],
                "batter": self.get_batter_name_and_team(ab_dict["at_bat_data"]),
                "pitch_count": len(ab_dict["at_bat_data"]["pitchfx"]),
                "missing_pfx": [],
                "invalid_pfx": sorted(
                    [pfx["ab_count"] for pfx in ab_dict["at_bat_data"]["pitchfx"]]
                ),
            }
            for pitch_app_dict in self.invalid_pfx_map.values()
            for inning_id, inning_dict in pitch_app_dict.items()
            for at_bat_id, ab_dict in inning_dict.items()
        ]

    def get_pitcher_name_and_team(self, at_bat_data):
        ab_dict = validate_at_bat_id(at_bat_data["at_bat_id"]).value
        return f'{at_bat_data["pitcher_name"]} ({ab_dict["pitcher_team"]})'

    def get_batter_name_and_team(self, at_bat_data):
        ab_dict = validate_at_bat_id(at_bat_data["at_bat_id"]).value
        return f'{at_bat_data["batter_name"]} ({ab_dict["batter_team"]})'

    def find_game_events_matching_this_pfx(self, pfx):
        return [
            self.get_event_dict(event)
            for event in self.game_events
            if event["at_bat_pitchfx_audit"]["missing_pitchfx_count"] > 0
            and self.event_matches_pfx(event, pfx)
        ]

    def get_event_dict(self, event):
        ab_dict = validate_at_bat_id(event["at_bat_id"]).value
        return {
            "source": "valid_at_bat",
            "at_bat_id": event["at_bat_id"],
            "inning_id": event["inning_id"][-5:],
            "pitcher_id": event["pitcher_id_mlb"],
            "pitcher": f'{event["pitcher_name"]} ({ab_dict["pitcher_team"]})',
            "batter_id": event["batter_id_mlb"],
            "batter": f'{event["batter_name"]} ({ab_dict["batter_team"]})',
            "pitch_count": event["at_bat_pitchfx_audit"]["pitch_count_bbref"],
            "missing_pfx": event["at_bat_pitchfx_audit"]["missing_pitch_numbers"],
            "invalid_pfx": [],
        }

    def event_matches_pfx(self, event, pfx):
        return (
            event["inning_id"][-5:] == pfx["inning_id"]
            and (
                event["batter_id_mlb"] == pfx["batter_id"]
                or event["pitcher_id_mlb"] == pfx["pitcher_id"]
            )
            and event["at_bat_pitchfx_audit"]["missing_pitchfx_count"] <= pfx["pitch_count"]
            and all(
                p_num in pfx["invalid_pfx"]
                for p_num in event["at_bat_pitchfx_audit"]["missing_pitch_numbers"]
            )
        )

    def check_for_exact_match(self, pfx):
        # given the invalid pfx data passed as an argument, the process to find an exact match is:
        exact_match = [
            self.get_event_dict(event)
            # iterate through all game events, for each game event:
            for event in self.game_events
            # if game event is missing pitchfx data
            if event["at_bat_pitchfx_audit"]["missing_pitchfx_count"] > 0
            and (
                # AND game_event and invalid pfx took place in the same inning
                event["inning_id"][-5:] == pfx["inning_id"]
                and (
                    # AND game event and invalid pfx have the same batter OR the same pitcher
                    event["batter_id_mlb"] == pfx["batter_id"]
                    or event["pitcher_id_mlb"] == pfx["pitcher_id"]
                )
                # AND number of pitches missing from the game event is the same as the number of invalid pfx
                and event["at_bat_pitchfx_audit"]["missing_pitchfx_count"] == pfx["pitch_count"]
                # AND invalid pfx pitch seq. numbers are the same as the pitches missing from the game event
                and all(
                    p_num in pfx["invalid_pfx"]
                    for p_num in event["at_bat_pitchfx_audit"]["missing_pitch_numbers"]
                )
            )
        ]
        if not exact_match:
            # zero game events matched all the criteria -> NO EXACT MATCH
            return Result.Fail("")
        if len(exact_match) != 1:
            # more than one game event matched all the criteria -> NO EXACT MATCH
            return Result.Fail("")
        # one game event matched all the criteria -> EXACT MATCH
        return Result.Ok(exact_match[0])

    def found_successful_match(self, pfx, match):
        match_dict = {"success": True, "invalid_pfx": pfx, "missing_pfx": match}
        match_dict["patch_type"] = (
            PatchType.CHANGE_PITCHER_ID
            if match_dict["invalid_pfx"]["pitcher_id"] != match_dict["missing_pfx"]["pitcher_id"]
            else PatchType.CHANGE_BATTER_ID
        )
        match_dict["patch"] = self.no_prompts
        return match_dict

    def create_patch_list(self):
        patch_list = []
        if not self.game_contains_invalid_pfx() or "success" not in self.match_results:
            return Result.Ok()
        self.events.create_patch_list_start()
        for match_dict in self.match_results["success"]:
            if not match_dict["patch"]:
                continue
            result = self.get_game_event(match_dict["missing_pfx"]["at_bat_id"])
            if result.failure:
                return result
            patch_event = result.value
            result = self.get_invalid_pfx(match_dict["invalid_pfx"]["at_bat_id"])
            if result.failure:
                return result
            patch_pfx = result.value
            patches = []
            if match_dict["patch_type"] == PatchType.CHANGE_BATTER_ID:
                patches = self.create_change_batter_id_patch(patch_event, patch_pfx)
            elif match_dict["patch_type"] == PatchType.CHANGE_PITCHER_ID:
                patches = self.create_change_pitcher_id_patch(patch_event, patch_pfx)
            patch_list.extend(patches)
        existing_patch_list = self.scraped_data.get_patch_list(DATA_SET, self.bbref_game_id)
        if existing_patch_list:
            patch_list = self.combine_patch_lists(patch_list, existing_patch_list.patch_list)
        patch_list.sort(key=lambda x: x.park_sv_id)
        self.patch_list = BrooksPitchFxPatchList(patch_list=patch_list, url_id=self.bbref_game_id)
        result = self.scraped_data.save_patch_list(DATA_SET, self.patch_list)
        if result.failure:
            return result
        self.events.create_patch_list_complete(self.patch_list)
        return Result.Ok()

    def get_game_event(self, at_bat_id):
        matches = [event for event in self.game_events if event["at_bat_id"] == at_bat_id]
        if not matches:
            return Result.Fail(f"Error! Failed to retrieve game event for at_bat_id: {at_bat_id}")
        patch_event = matches[0]
        return Result.Ok(patch_event)

    def get_invalid_pfx(self, at_bat_id):
        ab_dict = validate_at_bat_id(at_bat_id).value
        pitch_app_id = ab_dict["pitch_app_id"]
        inning_id = ab_dict["inning_id"]
        patch_pfx = self.invalid_pfx_map[pitch_app_id][inning_id][at_bat_id]["at_bat_data"]
        return (
            Result.Ok(patch_pfx)
            if patch_pfx
            else Result.Fail(
                f"Error! Failed to retrieve invalid pfx data for at_bat_id: {at_bat_id}"
            )
        )

    def create_change_pitcher_id_patch(self, patch_event, patch_pfx):
        patch_list = []
        for pfx in self.get_all_pfx_data_to_patch(patch_pfx):
            patch = dict(
                bbref_game_id=self.bbref_game_id,
                park_sv_id=pfx["park_sv_id"],
                current_at_bat_id=patch_pfx["at_bat_id"],
                current_pitch_app_id=patch_pfx["pitch_app_id"],
                current_pitcher_id=patch_pfx["pitcher_id_mlb"],
                current_pitcher_name=patch_pfx["pitcher_name"],
                new_at_bat_id=patch_event["at_bat_id"],
                new_pitch_app_id=patch_event["pitch_app_id"],
                new_pitcher_id=patch_event["pitcher_id_mlb"],
                new_pitcher_name=patch_event["pitcher_name"],
            )
            patch_list.append(from_dict(data_class=PatchBrooksPitchFxPitcherId, data=patch))
        return patch_list

    def create_change_batter_id_patch(self, patch_event, patch_pfx):
        patch_list = []
        for pfx in self.get_all_pfx_data_to_patch(patch_pfx):
            patch = dict(
                bbref_game_id=self.bbref_game_id,
                park_sv_id=pfx["park_sv_id"],
                pitch_app_id=patch_pfx["pitch_app_id"],
                current_at_bat_id=patch_pfx["at_bat_id"],
                current_batter_id=patch_pfx["batter_id_mlb"],
                current_batter_name=patch_pfx["batter_name"],
                new_at_bat_id=patch_event["at_bat_id"],
                new_batter_id=patch_event["batter_id_mlb"],
                new_batter_name=patch_event["batter_name"],
            )
            patch_list.append(from_dict(data_class=PatchBrooksPitchFxBatterId, data=patch))
        return patch_list

    def get_all_pfx_data_to_patch(self, invalid_pfx):
        dupe_pfx_count = invalid_pfx["at_bat_pitchfx_audit"]["duplicate_guid_removed_count"]
        if not dupe_pfx_count:
            return invalid_pfx["pitchfx"]
        return invalid_pfx["pitchfx"] + invalid_pfx["removed_duplicate_guid"]["removed_dupes"]

    def combine_patch_lists(self, new_patch_list, old_patch_list):
        new_ids = [p.park_sv_id for p in new_patch_list]
        for p in old_patch_list:
            if p.park_sv_id in new_ids:
                continue
            patch_copy = deepcopy(p)
            new_patch_list.append(patch_copy)
        return new_patch_list

    def apply_patch_list(self):
        if not self.game_contains_invalid_pfx() or not self.patch_list:
            return Result.Ok({"created_patch_list": False})
        self.events.combine_scraped_data_start()
        box_before = deepcopy(self.boxscore)
        result = self.verify_scraped_data_can_be_combined()
        if result.failure:
            return result
        box_after = deepcopy(self.boxscore)
        patch_results = self.summarize_patch_results(box_before, box_after)
        result = self.combine_data_and_update_db()
        if result.failure:
            return result
        self.events.combine_scraped_data_complete(patch_results)
        return Result.Ok(patch_results)

    def summarize_patch_results(self, boxscore_before, boxscore_after):
        (fixed_all_errors, invalid_pfx, pfx_errors) = self.check_for_remaining_pfx_errors()
        patch_diff_report = self.create_patch_diff_report(boxscore_before, boxscore_after)
        return {
            "created_patch_list": True,
            "patch_diff_report": patch_diff_report,
            "fixed_all_errors": fixed_all_errors,
            "invalid_pfx": invalid_pfx,
            "pfx_errors": pfx_errors,
        }

    def check_for_remaining_pfx_errors(self):
        fixed_all_errors = self.game_does_not_contain_any_errors()
        invalid_pfx = self.game_contains_invalid_pfx()
        pfx_errors = self.game_contains_pfx_errors()
        return (fixed_all_errors, invalid_pfx, pfx_errors)

    def create_patch_diff_report(self, boxscore_before, boxscore_after):
        patch_list = self.scraped_data.get_patch_list(DATA_SET, self.bbref_game_id)
        boxscore_changes = self.compare_pfx_vs_bbref_audit_results(
            audit_before=boxscore_before["pitchfx_vs_bbref_audit"],
            audit_after=boxscore_after["pitchfx_vs_bbref_audit"],
        )
        pitch_stats_before = self.get_pitch_app_stats_dict(boxscore_before)
        pitch_stats_after = self.get_pitch_app_stats_dict(boxscore_after)
        pitch_stat_changes_dict = {}
        for pitch_app_id in self.get_patched_pitch_app_ids(patch_list):
            pitch_stat_changes = self.compare_pfx_vs_bbref_audit_results(
                audit_before=pitch_stats_before[pitch_app_id]["pitch_app_pitchfx_audit"],
                audit_after=pitch_stats_after[pitch_app_id]["pitch_app_pitchfx_audit"],
            )
            pitch_stat_changes_dict[pitch_app_id] = pitch_stat_changes
        return {
            "boxscore_changes": boxscore_changes,
            "pitch_stat_changes_dict": pitch_stat_changes_dict,
            "pitch_stats_after": pitch_stats_after,
        }

    def get_pitch_app_stats_dict(self, boxscore):
        away_team_pitch_stats = boxscore["away_team_data"]["pitching_stats"]
        home_team_pitch_stats = boxscore["home_team_data"]["pitching_stats"]
        all_pitch_stats = away_team_pitch_stats + home_team_pitch_stats
        return {pitch_stats["pitch_app_id"]: pitch_stats for pitch_stats in all_pitch_stats}

    def get_patched_pitch_app_ids(self, patch_list):
        patched_pitch_app_ids = []
        for patch in patch_list.patch_list:
            if isinstance(patch, PatchBrooksPitchFxPitcherId):
                patched_pitch_app_ids.append(patch.current_pitch_app_id)
                patched_pitch_app_ids.append(patch.new_pitch_app_id)
            else:
                patched_pitch_app_ids.append(patch.pitch_app_id)
        return list(set(patched_pitch_app_ids))

    def compare_pfx_vs_bbref_audit_results(self, audit_before, audit_after):
        table_rows = [
            {
                "stat": stat_name,
                "before": audit_before[stat_name],
                "after": audit_after[stat_name],
                "changed": self.get_stat_change(audit_before, audit_after, stat_name),
            }
            for stat_name in PATCH_STAT_NAMES
            if stat_name in audit_before
            and stat_name in audit_after
            and (audit_after[stat_name] - audit_before[stat_name]) > 0
        ]
        return tabulate(table_rows, headers="keys")

    def get_stat_change(self, before, after, stat_name):
        change = after[stat_name] - before[stat_name]
        change_sign = "+" if change > 0 else "-" if change < 0 else " "
        return f"{change_sign}{abs(change)}"

    def combine_data_and_update_db(self):
        result = self.combine_data.execute(self.bbref_game_id)
        if not (
            result["gather_scraped_data_success"]
            and result["combined_data_success"]
            and result["update_pitch_apps_success"]
        ):
            # this code should be unreachable
            error = (
                "Critical Error! investigate_errors/combine_data processes produced different "
                f"results for same game: {self.bbref_game_id}"
            )
            return Result.Fail(error)
        return Result.Ok()
