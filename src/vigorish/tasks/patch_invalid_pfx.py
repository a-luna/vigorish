from collections import defaultdict
from copy import deepcopy

from dacite import from_dict
from events import Events
from tabulate import tabulate

from vigorish.enums import PatchType, DataSet
from vigorish.patch.brooks_pitchfx import (
    BrooksPitchFxPatchList,
    PatchBrooksPitchFxBatterId,
    PatchBrooksPitchFxPitcherId,
)
from vigorish.tasks.base import Task
from vigorish.util.result import Result
from vigorish.util.string_helpers import validate_at_bat_id


class PatchInvalidPitchFxTask(Task):
    def __init__(self, app):
        super().__init__(app)
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
        self.invalid_pfx_map = {}
        self.game_events = {}
        self.boxscore = {}
        self.match_results = defaultdict(list)
        result = self.verify_game_contains_invalid_pfx()
        if no_prompts:
            return (
                self.get_invalid_pfx_map()
                .on_success(self.match_missing_pfx_data)
                .on_success(self.create_patch_list)
                .on_success(self.apply_patch_list)
            )
        if result.failure:
            return result
        (has_invalid_pfx, error_message) = result.value
        if not has_invalid_pfx:
            return Result.Fail(error_message)
        return self.get_invalid_pfx_map()

    def verify_game_contains_invalid_pfx(self):
        results = self.scraped_data.investigate_errors(self.bbref_game_id)
        if not results["get_all_pbp_events_success"]:
            error_message = "Failed to process play-by-play events and identify at_bat_ids"
        elif not results["get_all_pfx_data_success"]:
            error_message = "Failed to process PitchFX logs and group by at_bat_id"
        elif not results["combine_data_success"]:
            error_message = "Failed to combine play-by-play and PitchFX data"
        elif not results["update_boxscore_success"]:
            error_message = "Failed to update boxscore with combined play-by-play and PitchFX data"
        else:
            error_message = None
        if error_message:
            self.events.error_occurred(error_message)
            return Result.Fail(error_message)
        self.game_events = results["game_events_combined_data"]
        self.boxscore = results["boxscore"]
        if not self.boxscore["pitchfx_vs_bbref_audit"]["invalid_pitchfx"]:
            error_message = (
                f"The selected game ({self.bbref_game_id}) did not raise any errors when "
                "attempting to combine scraped data. Please try to combine data for this "
                "particular game again."
            )
            return Result.Ok((False, error_message))
        return Result.Ok((True, None))

    def get_invalid_pfx_map(self):
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
        self.events.match_missing_pfx_data_start()
        for pfx in self.get_details_of_invalid_pfx_at_bats():
            matches = [
                self.get_event_dict(event)
                for event in self.game_events
                if event["at_bat_pitchfx_audit"]["missing_pitchfx_count"] > 0
                and self.event_matches_pfx(event, pfx)
            ]
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

    def get_details_of_invalid_pfx_at_bats(self):
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
            for pitch_app_id, pitch_app_dict in self.invalid_pfx_map.items()
            for inning_id, inning_dict in pitch_app_dict.items()
            for at_bat_id, ab_dict in inning_dict.items()
        ]

    def get_pitcher_name_and_team(self, at_bat_data):
        ab_dict = validate_at_bat_id(at_bat_data["at_bat_id"]).value
        return f'{at_bat_data["pitcher_name"]} ({ab_dict["pitcher_team"]})'

    def get_batter_name_and_team(self, at_bat_data):
        ab_dict = validate_at_bat_id(at_bat_data["at_bat_id"]).value
        return f'{at_bat_data["batter_name"]} ({ab_dict["batter_team"]})'

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
        exact_match = [
            self.get_event_dict(event)
            for event in self.game_events
            if event["at_bat_pitchfx_audit"]["missing_pitchfx_count"] > 0
            and (
                event["inning_id"][-5:] == pfx["inning_id"]
                and (
                    event["batter_id_mlb"] == pfx["batter_id"]
                    or event["pitcher_id_mlb"] == pfx["pitcher_id"]
                )
                and event["at_bat_pitchfx_audit"]["missing_pitchfx_count"] == pfx["pitch_count"]
                and all(
                    p_num in pfx["invalid_pfx"]
                    for p_num in event["at_bat_pitchfx_audit"]["missing_pitch_numbers"]
                )
            )
        ]
        if not exact_match:
            return Result.Fail("")
        if len(exact_match) != 1:
            return Result.Fail("")
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
        self.patch_list = []
        if "success" not in self.match_results:
            return Result.Ok(self.patch_list)
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
            if match_dict["patch_type"] == PatchType.CHANGE_BATTER_ID:
                patches = self.create_change_batter_id_patch(patch_event, patch_pfx)
            elif match_dict["patch_type"] == PatchType.CHANGE_PITCHER_ID:
                patches = self.create_change_pitcher_id_patch(patch_event, patch_pfx)
            self.patch_list.extend(patches)
        if self.patch_list:
            self.patch_list = BrooksPitchFxPatchList(
                patch_list=self.patch_list, url_id=self.bbref_game_id
            )
            result = self.scraped_data.save_patch_list(DataSet.BROOKS_PITCHFX, self.patch_list)
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
        for pfx in patch_pfx["pitchfx"]:
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
        for pfx in patch_pfx["pitchfx"]:
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

    def apply_patch_list(self):
        self.events.combine_scraped_data_start()
        boxscore_before = deepcopy(self.boxscore)
        result = self.verify_game_contains_invalid_pfx()
        if result.failure:
            return result
        boxscore_after = deepcopy(self.boxscore)
        (has_invalid_pfx, error_message) = result.value
        has_pfx_error = self.boxscore["pitchfx_vs_bbref_audit"]["pitchfx_error"]
        (has_patch_list, patch_diff_report) = self.create_patch_diff_report(
            boxscore_before, boxscore_after
        )
        self.events.combine_scraped_data_complete((has_patch_list, patch_diff_report))
        fixed_all_errors = not (has_invalid_pfx or has_pfx_error)
        return Result.Ok((has_patch_list, fixed_all_errors, patch_diff_report))

    def create_patch_diff_report(self, boxscore_before, boxscore_after):
        patch_list = self.scraped_data.get_patch_list(DataSet.BROOKS_PITCHFX, self.bbref_game_id)
        if not patch_list:
            return (False, None)
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
                pitch_app_audit=True,
            )
            pitch_stat_changes_dict[pitch_app_id] = pitch_stat_changes
            patch_diff_report = (boxscore_changes, pitch_stat_changes_dict, pitch_stats_after)
        return (True, patch_diff_report)

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

    def compare_pfx_vs_bbref_audit_results(self, audit_before, audit_after, pitch_app_audit=False):
        table_rows = [
            {
                "stat": stat_name,
                "before": audit_before[stat_name],
                "after": audit_after[stat_name],
                "changed": self.get_stat_change(audit_before, audit_after, stat_name),
            }
            for stat_name in self.get_patch_diff_stat_names(pitch_app_audit)
            if self.get_stat_change(audit_before, audit_after, stat_name) > 0
        ]
        return tabulate(table_rows, headers="keys")

    def get_patch_diff_stat_names(self, pitch_app_audit):
        stat_names = [
            "total_at_bats_pitchfx_complete",
            "patched_pitchfx_count",
            "total_at_bats_patched_pitchfx",
            "missing_pitchfx_count",
            "total_at_bats_missing_pitchfx",
            "extra_pitchfx_count",
            "total_at_bats_extra_pitchfx",
            "extra_pitchfx_removed_count",
            "total_at_bats_extra_pitchfx_removed",
            "total_at_bats_pitchfx_error",
        ]
        if pitch_app_audit:
            stat_names.append("batters_faced_bbref")
            stat_names.append("batters_faced_pitchfx")
            stat_names.append("pitch_count_bbref")
            stat_names.append("pitch_count_pitchfx")
            stat_names.append("invalid_pitchfx_count")
            stat_names.append("total_at_bats_invalid_pitchfx")
        return stat_names

    def get_stat_change(self, before, after, stat_name):
        return abs(after[stat_name] - before[stat_name])
