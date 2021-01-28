import subprocess
from copy import deepcopy

from events import Events
from halo import Halo

from vigorish.cli.components import get_random_cli_color, get_random_dots_spinner
from vigorish.tasks.base import Task
from vigorish.tasks.patch_invalid_pfx import PatchInvalidPitchFxTask
from vigorish.util.result import Result


class PatchAllInvalidPitchFxTask(Task):
    def __init__(self, app):
        super().__init__(app)
        self.patch_invalid_pfx = PatchInvalidPitchFxTask(app)
        self.events = Events(
            (
                "error_occurred",
                "patch_all_invalid_pitchfx_started",
                "patch_all_invalid_pitchfx_complete",
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

    def execute(self, year):
        self.subscribe_to_events()
        all_patch_results = {}
        audit_report = self.scraped_data.get_audit_report()
        self.audit_report_before = deepcopy(audit_report)
        if year not in self.audit_report_before:
            return Result.Fail(f"No games for MLB {year} season have been scraped.")
        game_ids = self.audit_report_before[year].get("invalid_pfx", [])
        if not game_ids:
            return Result.Fail(f"No games for MLB {year} season have invalid pitchfx data.")
        self.events.patch_all_invalid_pitchfx_started()
        self.initialize_spinner(game_ids)
        for num, game_id in enumerate(game_ids, start=1):
            self.spinner.text = self.get_spinner_text(game_id, num, len(game_ids))
            result = self.patch_invalid_pfx.execute(game_id, no_prompts=True)
            if result.failure:
                self.spinner.stop()
                return result
            patch_results = result.value
            all_patch_results[game_id] = patch_results
            self.spinner.text = self.get_spinner_text(game_id, num + 1, len(game_ids))
        self.spinner.stop()
        audit_report = self.scraped_data.get_audit_report()
        self.audit_report_after = deepcopy(audit_report)
        (successful_change, invalid_pfx_change) = self.calculate_games_changed(year)
        self.events.patch_all_invalid_pitchfx_complete()
        self.unsubscribe_from_events()
        return Result.Ok(
            {
                "all_patch_results": all_patch_results,
                "successful_change": successful_change,
                "invalid_pfx_change": invalid_pfx_change,
            }
        )

    def initialize_spinner(self, game_ids):
        subprocess.run(["clear"])
        self.spinner = Halo(spinner=get_random_dots_spinner(), color=get_random_cli_color())
        self.spinner.text = self.get_spinner_text(game_ids[0], 1, len(game_ids))
        self.spinner.start()

    def get_spinner_text(self, game_id, game_count, total_games):
        percent = game_count / float(total_games)
        return f"Attempting to patch invalid PitchFX data (Game ID: {game_id}) {percent:.0%}..."

    def calculate_games_changed(self, year):
        successful_before = self.audit_report_before[year]["successful"]
        successful_after = self.audit_report_after[year]["successful"]
        successful_change = len(successful_after) - len(successful_before)
        invalid_pfx_before = self.audit_report_before[year]["invalid_pfx"]
        invalid_pfx_after = self.audit_report_after[year]["invalid_pfx"]
        invalid_pfx_change = len(invalid_pfx_after) - len(invalid_pfx_before)
        return (successful_change, invalid_pfx_change)

    def handle_error_occurred(self, error_message):
        self.events.error_occurred(error_message)

    def create_invalid_pfx_map_start(self):
        self.events.create_invalid_pfx_map_start()

    def create_invalid_pfx_map_complete(self, invalid_pfx_map):
        self.events.create_invalid_pfx_map_complete(invalid_pfx_map)

    def match_missing_pfx_data_start(self):
        self.events.match_missing_pfx_data_start()

    def match_missing_pfx_data_complete(self, match_dict):
        self.events.match_missing_pfx_data_complete(match_dict)

    def create_patch_list_start(self):
        self.events.create_patch_list_start()

    def create_patch_list_complete(self, patch_list):
        self.events.create_patch_list_complete(patch_list)

    def combine_scraped_data_start(self):
        self.events.combine_scraped_data_start()

    def combine_scraped_data_complete(self, patch_diff_report):
        self.events.combine_scraped_data_complete(patch_diff_report)

    def subscribe_to_events(self):
        self.patch_invalid_pfx.events.error_occurred += self.handle_error_occurred
        self.patch_invalid_pfx.events.create_invalid_pfx_map_start += self.create_invalid_pfx_map_start
        self.patch_invalid_pfx.events.create_invalid_pfx_map_complete += self.create_invalid_pfx_map_complete
        self.patch_invalid_pfx.events.match_missing_pfx_data_start += self.match_missing_pfx_data_start
        self.patch_invalid_pfx.events.match_missing_pfx_data_complete += self.match_missing_pfx_data_complete
        self.patch_invalid_pfx.events.create_patch_list_start += self.create_patch_list_start
        self.patch_invalid_pfx.events.create_patch_list_complete += self.create_patch_list_complete
        self.patch_invalid_pfx.events.combine_scraped_data_start += self.combine_scraped_data_start
        self.patch_invalid_pfx.events.combine_scraped_data_complete += self.combine_scraped_data_complete

    def unsubscribe_from_events(self):
        self.patch_invalid_pfx.events.error_occurred -= self.handle_error_occurred
        self.patch_invalid_pfx.events.create_invalid_pfx_map_start -= self.create_invalid_pfx_map_start
        self.patch_invalid_pfx.events.create_invalid_pfx_map_complete -= self.create_invalid_pfx_map_complete
        self.patch_invalid_pfx.events.match_missing_pfx_data_start -= self.match_missing_pfx_data_start
        self.patch_invalid_pfx.events.match_missing_pfx_data_complete -= self.match_missing_pfx_data_complete
        self.patch_invalid_pfx.events.create_patch_list_start -= self.create_patch_list_start
        self.patch_invalid_pfx.events.create_patch_list_complete -= self.create_patch_list_complete
        self.patch_invalid_pfx.events.combine_scraped_data_start -= self.combine_scraped_data_start
        self.patch_invalid_pfx.events.combine_scraped_data_complete -= self.combine_scraped_data_complete
