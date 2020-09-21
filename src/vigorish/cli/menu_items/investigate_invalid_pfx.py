"""Fix at bats with invalid PitchFX data."""
import subprocess
from copy import deepcopy

from halo import Halo
from getch import pause
from tabulate import tabulate

from vigorish.cli.menu_item import MenuItem
from vigorish.cli.prompts import prompt_user_yes_no, select_game_prompt, user_options_prompt
from vigorish.cli.util import (
    get_random_cli_color,
    get_random_dots_spinner,
    print_message,
    print_heading,
    print_error,
    print_success,
)
from vigorish.constants import EMOJI_DICT, MENU_NUMBERS
from vigorish.enums import AuditError, PatchType
from vigorish.tasks.patch_all_invalid_pfx import PatchAllInvalidPitchFxTask
from vigorish.tasks.patch_invalid_pfx import PatchInvalidPitchFxTask
from vigorish.util.result import Result
from vigorish.util.string_helpers import validate_at_bat_id, inning_number_to_string


class InvestigateInvalidPitchFx(MenuItem):
    def __init__(self, app, year, bbref_game_ids):
        super().__init__(app)
        self.patch_all_invalid_pfx = PatchAllInvalidPitchFxTask(app)
        self.patch_invalid_pfx = PatchInvalidPitchFxTask(app)
        self.year = year
        self.bbref_game_ids = bbref_game_ids
        self.menu_item_text = f"{AuditError.INVALID_PITCHFX_DATA} ({self.game_count} Games)"
        self.menu_item_emoji = EMOJI_DICT.get("UPSIDE_DOWN")

    @property
    def game_count(self):
        return len(self.bbref_game_ids)

    def launch(self):
        subprocess.run(["clear"])
        self.subscribe_to_events()
        result = self.select_patch_prompt()
        if result.failure:
            self.unsubscribe_from_events()
            return Result.Ok(True)
        selected_action = result.value
        if selected_action == "ALL":
            result = self.patch_all_invalid_pfx.execute(self.year)
            self.unsubscribe_from_events()
            if result.failure:
                return result
            patch_results_dict = result.value
            self.display_all_patch_results(**patch_results_dict)
            return Result.Ok(True)
        exit_single_game_menu = False
        while not exit_single_game_menu:
            result = select_game_prompt(self.bbref_game_ids)
            if result.failure:
                self.unsubscribe_from_events()
                return Result.Ok(True)
            self.game_id = result.value
            result = self.select_game_operation_prompt()
            if result.failure:
                exit_single_game_menu = True
                continue
            selected_action = result.value
            if selected_action == "INVESTIGATE":
                result = self.patch_invalid_pfx_single_game()
                if result.failure:
                    self.unsubscribe_from_events()
                    return result
            if selected_action == "RETRY":
                result = self.combine_scraped_data_for_game()
                if result.failure:
                    self.unsubscribe_from_events()
                    return result
        self.unsubscribe_from_events()
        return Result.Ok(True)

    def select_patch_prompt(self):
        prompt = (
            "You can attempt to patch all games with invald PitchFX data by selecting the first "
            "option below. Or, you can investigate each game in detail by selecting the second "
            "option."
        )
        choices = {
            f"{MENU_NUMBERS.get(1)}  Patch all Games": "ALL",
            f"{MENU_NUMBERS.get(2)}  Patch a Single Game": "ONE",
            f"{EMOJI_DICT.get('BACK')} Return to Previous Menu": None,
        }
        return user_options_prompt(choices, prompt)

    def select_game_operation_prompt(self):
        prompt = "What would you like to do at this point?"
        choices = {
            f"{MENU_NUMBERS.get(1)}  Analyze Pitching Data": "INVESTIGATE",
            f"{MENU_NUMBERS.get(2)}  Attempt to Combine Data (Again)": "RETRY",
            f"{EMOJI_DICT.get('BACK')} Return to Previous Menu": None,
        }
        return user_options_prompt(choices, prompt)

    def patch_invalid_pfx_single_game(self):
        result = self.patch_invalid_pfx.execute(self.game_id)
        if result.failure:
            header = f"Invalid PitchFX Data for {self.game_id}\n"
            subprocess.run(["clear"])
            print_message(header, wrap=False, bold=True, underline=True)
            print_message(result.error, fg="bright_yellow")
            pause(message="Press any key to continue...")
            return Result.Ok(True)
        subprocess.run(["clear"])
        self.display_invalid_pfx_details()
        if not self.prompt_user_create_patch_list():
            return Result.Ok(True)
        result = self.patch_invalid_pfx.match_missing_pfx_data()
        if result.failure:
            return result
        subprocess.run(["clear"])
        for result, matches in self.patch_invalid_pfx.match_results.items():
            if result == "success":
                for num, match_dict in enumerate(matches, start=1):
                    self.display_potential_data_match(num, len(matches), match_dict)
            if result == "no_matches":
                self.display_no_match_found(matches)
            if result == "many_matches":
                self.display_many_matches_found(matches)
        if "success" not in self.patch_invalid_pfx.match_results:
            header = f"Invalid PitchFX Data for {self.game_id}\n"
            message = (
                "Unable to identify missing data that matches the invalid PitchFX data for this "
                "game. You should inspect the combined data JSON file for this game and "
                "investigate the invalid data manually.\n"
            )
            subprocess.run(["clear"])
            print_message(header, wrap=False, bold=True, underline=True)
            print_message(message, fg="bright_yellow")
            pause(message="Press any key to continue...")
            return Result.Ok(True)
        result = self.patch_invalid_pfx.create_patch_list()
        if result.failure:
            return result
        if not self.patch_invalid_pfx.patch_list or not self.prompt_user_apply_patch_list():
            return Result.Ok(True)
        result = self.patch_invalid_pfx.apply_patch_list()
        if result.failure:
            return result
        self.patch_results = result.value
        print()
        if self.patch_results["fixed_all_errors"]:
            patch_result = (
                f"PitchFX data for {self.game_id} is now completely reconciled (no errors of "
                "any type)!\n"
            )
            print_success(patch_result)
        if self.patch_results["invalid_pfx"]:
            patch_result = (
                f"{self.game_id} still contains invalid PitchFX data after applying the patch "
                "list.\n"
            )
            print_error(patch_result)
        if self.patch_results["pfx_errors"]:
            patch_result = (
                f"{self.game_id} still contains PitchFX data errors associated with valid at "
                "bats.\n"
            )
            print_error(patch_result)
        pause(message="Press any key to continue...")
        subprocess.run(["clear"])
        if self.prompt_user_view_patched_data():
            self.display_patched_data_tables(**self.patch_results["patch_diff_report"])
        return Result.Ok()

    def display_invalid_pfx_details(self):
        invalid_pfx_error_data = []
        for pa_id, pitch_app_dict in self.patch_invalid_pfx.invalid_pfx_map.items():
            for inning_id, inning_dict in pitch_app_dict.items():
                for at_bat_id, ab_dict in inning_dict.items():
                    ab_val = validate_at_bat_id(at_bat_id).value
                    ab_data = ab_dict["at_bat_data"]
                    pfx_audit = ab_data["at_bat_pitchfx_audit"]
                    pitch_numbers = sorted([pfx["ab_count"] for pfx in ab_data["pitchfx"]])
                    pitch_count = max(pfx["ab_total"] for pfx in ab_data["pitchfx"])
                    error_data = {
                        "inning_id": ab_data["inning_id"][-5:],
                        "pitcher": f'{ab_data["pitcher_name"]} ({ab_val["pitcher_team"]})',
                        "batter": f'{ab_data["batter_name"]} ({ab_val["batter_team"]})',
                        "pitch_count": pitch_count,
                        "pfx_count": pfx_audit["pitch_count_pitchfx"],
                        "pfx_ab_numbers": pitch_numbers,
                    }
                    invalid_pfx_error_data.append(error_data)
        invalid_pfx_table = tabulate(invalid_pfx_error_data, headers="keys")
        self.summarize_pfx_errors(self.patch_invalid_pfx.invalid_pfx_map)
        print_message(invalid_pfx_table, wrap=False, fg="bright_magenta")

    def summarize_pfx_errors(self, pfx_error_map):
        at_bat_ids = [
            at_bat_id
            for pa_id, pitch_app_dict in pfx_error_map.items()
            for inning_id, inning_dict in pitch_app_dict.items()
            for at_bat_id in inning_dict.keys()
        ]
        pitch_app_ids = list(pfx_error_map.keys())
        at_bats_plural = "at bats" if len(at_bat_ids) > 1 else "at bat"
        pitch_apps_plural = (
            "pitching appearances" if len(pitch_app_ids) > 1 else "pitching appearance"
        )
        header = f"Invalid PitchFX Data for {self.game_id}\n"
        message = (
            f"{self.game_id} contains PitchFX data that does not belong to any at bat "
            f"that occurred during this game according to the baseball-reference.com boxscore "
            f"({len(at_bat_ids)} {at_bats_plural}, {len(pitch_app_ids)} total "
            f"{pitch_apps_plural}).\n"
        )
        print_message(header, wrap=False, bold=True, underline=True)
        print_message(message)

    def prompt_user_create_patch_list(self):
        print()
        prompt = (
            "Invalid PitchFX data is sometimes valid data where the pitcher or batter ID value "
            "is incorrect, while the inning, pitch count, and pitch numbers are correct. Would "
            "you like to check for at bats that match these criteria?"
        )
        return prompt_user_yes_no(prompt)

    def display_potential_data_match(self, num, total_matches, match_dict):
        self.describe_potential_data_match(num, total_matches, match_dict)
        match_copy = deepcopy(match_dict)
        match_copy["invalid_pfx"].pop("at_bat_id")
        match_copy["invalid_pfx"].pop("pitcher_id")
        match_copy["invalid_pfx"].pop("batter_id")
        match_copy["missing_pfx"].pop("at_bat_id")
        match_copy["missing_pfx"].pop("pitcher_id")
        match_copy["missing_pfx"].pop("batter_id")
        match_rows = [match_copy["invalid_pfx"], match_copy["missing_pfx"]]
        print_message(tabulate(match_rows, headers="keys"), wrap=False, fg="bright_green")
        match_dict["patch"] = self.prompt_user_create_patch(match_dict)

    def describe_potential_data_match(self, num, total, match_dict):
        invalid_pfx = match_dict["invalid_pfx"]
        missing_pfx = match_dict["missing_pfx"]
        at_bat_id = missing_pfx["at_bat_id"]
        inning_str = inning_number_to_string(invalid_pfx["inning_id"])
        pitcher_or_batter = (
            "pitcher" if match_dict["patch_type"] == PatchType.CHANGE_BATTER_ID else "batter"
        )
        match_heading = f"Match {num}/{total} (Patch Type: {match_dict['patch_type']}):"
        match_summary = []
        match_summary = (
            f"The invalid PitchFX data below for an at bat between {invalid_pfx['pitcher']} and "
            f"{invalid_pfx['batter']} in the {inning_str} is a potential "
            "match for missing data that actually occurred in the same inning between "
            f"{missing_pfx['pitcher']} and {missing_pfx['batter']} (at_bat_id: {at_bat_id}).\n"
        )
        match_details_heading = "The data used to make this potential match is given below:"
        match_details = []
        match_details.append(
            f"- Both at bats took place in the same inning ({missing_pfx['inning_id']}) and "
            f"involve the same {pitcher_or_batter}.\n"
        )
        match_details.append(
            "- The number of pitches missing from the valid at bat "
            f"({len(missing_pfx['missing_pfx'])}) is the same (or less than) the total number of "
            f"pitches in the invalid at bat ({invalid_pfx['pitch_count']}).\n"
        )
        match_details.append(
            f"- The pitch sequence numbers of the invalid at bat ({invalid_pfx['invalid_pfx']}) "
            "contain all of the pitch numbers that are missing from the valid at bat "
            f"({missing_pfx['missing_pfx']}).\n"
        )
        subprocess.run(["clear"])
        print_heading(match_heading)
        print_message(match_summary)
        print_heading(match_details_heading, fg="bright_green")
        for message in match_details:
            print_message(message, fg="bright_green")

    def prompt_user_create_patch(self, match_dict):
        if match_dict["patch_type"] == PatchType.CHANGE_BATTER_ID:
            prompt = (
                "Change batter_id on all invalid_pfx data in the table above from "
                f'{self.get_batter_name_and_id(match_dict["invalid_pfx"])} to '
                f'{self.get_batter_name_and_id(match_dict["missing_pfx"])}'
            )
        if match_dict["patch_type"] == PatchType.CHANGE_PITCHER_ID:
            prompt = (
                "Change pitcher_id on all invalid_pfx data in the table above from "
                f'{self.get_pitcher_name_and_id(match_dict["invalid_pfx"])} to '
                f'{self.get_pitcher_name_and_id(match_dict["missing_pfx"])}'
            )
        prompt_heading = "\nWould you like to create a patch file to apply these changes?"
        print_heading(prompt_heading, fg="bright_yellow")
        return prompt_user_yes_no(prompt)

    def get_pitcher_name_and_id(self, at_bat_data):
        player_name = at_bat_data["pitcher"].split("(")[0].strip()
        player_id = at_bat_data["pitcher_id"]
        return f"{player_name} (MLB ID: {player_id})"

    def get_batter_name_and_id(self, at_bat_data):
        player_name = at_bat_data["batter"].split("(")[0].strip()
        player_id = at_bat_data["batter_id"]
        return f"{player_name} (MLB ID: {player_id})"

    def display_no_match_found(self, matches):
        for match_dict in matches:
            match_dict["invalid_pfx"].pop("at_bat_id")
            match_dict["invalid_pfx"].pop("pitcher_id")
            match_dict["invalid_pfx"].pop("batter_id")
        subprocess.run(["clear"])
        error = "No matching at bats were found for the invalid PitchFX data below:\n"
        print_message(error, fg="bright_red", bold=True)
        unmatched_rows = [match_dict["invalid_pfx"] for match_dict in matches]
        print_message(tabulate(unmatched_rows, headers="keys"), wrap=False)
        print()
        pause(message="Press any key to continue...")

    def display_many_matches_found(self, matches):
        for match_dict in matches:
            match_dict["invalid_pfx"].pop("at_bat_id")
            match_dict["invalid_pfx"].pop("pitcher_id")
            match_dict["invalid_pfx"].pop("batter_id")
            for match in match_dict["missing_pfx"]:
                match.pop("at_bat_id")
                match.pop("pitcher_id")
                match.pop("batter_id")
            subprocess.run(["clear"])
            error = (
                "Multiple at bats were found for the invalid PitchFX data below (only one match "
                "is expected):\n"
            )
            print_message(error, fg="bright_yellow", bold=True)
            all_rows = [match_dict["invalid_pfx"]]
            all_rows.extend([match for match in match_dict["missing_pfx"]])
            print_message(tabulate(all_rows, headers="keys"), wrap=False)
            print()
            pause(message="Press any key to continue...")

    def prompt_user_apply_patch_list(self):
        subprocess.run(["clear"])
        prompt = (
            f"A patch list for {self.game_id} was successfully created! Would you like to apply "
            "these changes and attempt to combine the scraped data after doing so?"
        )
        return prompt_user_yes_no(prompt)

    def combine_scraped_data_for_game(self):
        subprocess.run(["clear"])
        spinner = Halo(spinner=get_random_dots_spinner(), color=get_random_cli_color())
        spinner.text = f"Combining scraped data for {self.game_id}..."
        spinner.start()
        result = self.scraped_data.combine_boxscore_and_pfx_data(self.game_id)
        if (
            not result["gather_scraped_data_success"]
            or not result["combined_data_success"]
            or not result["update_pitch_apps_success"]
        ):
            spinner.fail(f"Failed to combine data for {self.game_id}!")
            pause(message="Press any key to continue...")
            return Result.Fail(result["error"])
        pfx_errors = result["results"]["pfx_errors"]
        fail_results = [
            pfx_errors.pop("pitchfx_error", {}),
            pfx_errors.pop("invalid_pitchfx", {}),
        ]
        if not any(len(f) > 0 for f in fail_results):
            spinner.succeed(f"All scraped data for {self.game_id} was successfully combined!")
            pause(message="Press any key to continue...")
            return Result.Ok()
        spinner.stop()
        subprocess.run(["clear"])
        total_pitch_apps = sum(len(f.keys()) for f in fail_results if f)
        pitch_apps_plural = "pitch appearances" if total_pitch_apps > 1 else "pitch appearance"
        total_at_bats = sum(
            len(at_bat_ids) for f in fail_results for at_bat_ids in f.values() if f
        )
        at_bats_plural = "at bats" if total_at_bats > 1 else "at bat"
        error_header = f"PitchFX data could not be reconciled for game: {self.game_id}\n"
        error_message = (
            f"{total_pitch_apps} {pitch_apps_plural} with data errors ({total_at_bats} "
            f"total {at_bats_plural})\n"
        )
        print_message(error_header, wrap=False, fg="bright_red", bold=True, underline=True)
        print_message(error_message, fg="bright_red")
        if not self.prompt_user_investigate_failures():
            pause(message="Press any key to continue...")
            return Result.Ok()
        subprocess.run(["clear"])
        return self.patch_invalid_pfx_single_game()

    def prompt_user_investigate_failures(self):
        prompt = (
            "Would you like to analyze these results? In many cases, the batter/pitcher ID is "
            "incorrect and can be easily fixed by applying a patch file."
        )
        return prompt_user_yes_no(prompt)

    def prompt_user_view_patched_data(self):
        prompt = (
            "Would you like to see a report detailing the changes that were made by applying the "
            "patch list?"
        )
        return prompt_user_yes_no(prompt)

    def display_patched_data_tables(
        self, boxscore_changes, pitch_stat_changes_dict, pitch_stats_after
    ):
        total_tables = len(pitch_stat_changes_dict) + 1
        boxscore_message = f"Table 1/{total_tables}: All Pitch Data for {self.game_id}\n"
        subprocess.run(["clear"])
        print_message(boxscore_message, wrap=False, fg="bright_cyan", bold=True, underline=True)
        print_message(boxscore_changes, wrap=False, fg="bright_cyan")
        pause(message="\nPress any key to continue...")
        for num, (pitch_app_id, pitch_stat_changes) in enumerate(
            pitch_stat_changes_dict.items(), start=2
        ):
            pitch_stats_header = (
                f"Table {num}/{total_tables}: Stats for Pitch Appearance {pitch_app_id}\n"
            )
            name = pitch_stats_after[pitch_app_id]["pitcher_name"]
            line = pitch_stats_after[pitch_app_id]["bbref_data"]
            game_score = f" (GS: {line['game_score']})\n" if line["game_score"] > 0 else "\n"
            earned_runs = (
                f"{line['earned_runs']} ER"
                if line["earned_runs"] == line["runs"]
                else f"{line['runs']} R ({line['earned_runs']} ER)"
            )
            pitch_stats_message = (
                f"{name}: {line['innings_pitched']} IP, {earned_runs}, {line['hits']} H, "
                f"{line['strikeouts']} K, {line['bases_on_balls']} BB{game_score}"
            )
            subprocess.run(["clear"])
            print_heading(pitch_stats_header, fg="bright_cyan")
            print_message(pitch_stats_message, wrap=False, fg="bright_cyan")
            print_message(pitch_stat_changes, wrap=False, fg="bright_cyan")
            pause(message="\nPress any key to continue...")

    def display_all_patch_results(self, all_patch_results, successful_change, invalid_pfx_change):
        col_headers = ["#", "BBRef Game ID", "Status"]
        game_results = [
            [game_id, self.get_scrape_status(patch_results)]
            for game_id, patch_results in all_patch_results.items()
        ]
        game_results.sort(key=lambda x: (x[1], x[0]))
        row_data = [[num, x[0], x[1]] for num, x in enumerate(game_results, start=1)]
        patch_list_count = len([r for r in all_patch_results.values() if r["created_patch_list"]])
        heading = f"Operation: Patch all Invalid PitchFX Data for MLB {self.year}"
        messages = [f"- Processed {len(game_results)} total games\n"]
        if patch_list_count:
            messages.append(f"- Created a patch list for {patch_list_count} games")
        if successful_change:
            messages.append(f"- Eliminated all PitchFX errors from {successful_change} games")
        if invalid_pfx_change:
            messages.append(f"- PitchFX data errors still exist in {invalid_pfx_change} games")
        print_heading(heading)
        for message in messages:
            print_message(message)
        print_message(tabulate(row_data, headers=col_headers), fg="bright_cyan", wrap=False)
        pause(message="\nPress any key to continue...")

    def get_scrape_status(self, patch_results):
        if not patch_results["created_patch_list"]:
            return "failed to create patch list"
        if patch_results["fixed_all_errors"]:
            return "fixed all errors"
        if patch_results["invalid_pfx"] and patch_results["pfx_errors"]:
            return "invalid and valid pfx errors remain"
        if patch_results["invalid_pfx"]:
            return "invalid pfx errors remain"
        if patch_results["pfx_errors"]:
            return "valid pfx errors remain"

    def error_occurred(self, error_message):
        print_error(error_message)

    def patch_all_invalid_pitchfx_started(self):
        pass

    def patch_all_invalid_pitchfx_complete(self):
        pass

    def create_invalid_pfx_map_start(self):
        pass

    def create_invalid_pfx_map_complete(self, invalid_pfx_map):
        self.invalid_pfx_map = invalid_pfx_map

    def match_missing_pfx_data_start(self):
        pass

    def match_missing_pfx_data_complete(self, match_results):
        self.match_results = match_results

    def create_patch_list_start(self):
        pass

    def create_patch_list_complete(self, patch_list):
        self.patch_list = patch_list

    def combine_scraped_data_start(self):
        pass

    def combine_scraped_data_complete(self, patch_results):
        self.patch_results = patch_results

    def subscribe_to_events(self):
        self.patch_all_invalid_pfx.events.patch_all_invalid_pitchfx_started += (
            self.patch_all_invalid_pitchfx_started
        )
        self.patch_all_invalid_pfx.events.patch_all_invalid_pitchfx_complete += (
            self.patch_all_invalid_pitchfx_complete
        )
        self.patch_all_invalid_pfx.events.create_invalid_pfx_map_start += (
            self.create_invalid_pfx_map_start
        )
        self.patch_all_invalid_pfx.events.create_invalid_pfx_map_complete += (
            self.create_invalid_pfx_map_complete
        )
        self.patch_all_invalid_pfx.events.error_occurred += self.error_occurred
        self.patch_all_invalid_pfx.events.match_missing_pfx_data_start += (
            self.match_missing_pfx_data_start
        )
        self.patch_all_invalid_pfx.events.match_missing_pfx_data_complete += (
            self.match_missing_pfx_data_complete
        )
        self.patch_all_invalid_pfx.events.create_patch_list_start += self.create_patch_list_start
        self.patch_all_invalid_pfx.events.create_patch_list_complete += (
            self.create_patch_list_complete
        )
        self.patch_all_invalid_pfx.events.combine_scraped_data_start += (
            self.combine_scraped_data_start
        )
        self.patch_all_invalid_pfx.events.combine_scraped_data_complete += (
            self.combine_scraped_data_complete
        )

    def unsubscribe_from_events(self):
        self.patch_all_invalid_pfx.events.create_invalid_pfx_map_start -= (
            self.create_invalid_pfx_map_start
        )
        self.patch_all_invalid_pfx.events.create_invalid_pfx_map_complete -= (
            self.create_invalid_pfx_map_complete
        )
        self.patch_all_invalid_pfx.events.patch_all_invalid_pitchfx_started -= (
            self.patch_all_invalid_pitchfx_started
        )
        self.patch_all_invalid_pfx.events.patch_all_invalid_pitchfx_complete -= (
            self.patch_all_invalid_pitchfx_complete
        )
        self.patch_all_invalid_pfx.events.error_occurred -= self.error_occurred
        self.patch_all_invalid_pfx.events.match_missing_pfx_data_start -= (
            self.match_missing_pfx_data_start
        )
        self.patch_all_invalid_pfx.events.match_missing_pfx_data_complete -= (
            self.match_missing_pfx_data_complete
        )
        self.patch_all_invalid_pfx.events.create_patch_list_start -= self.create_patch_list_start
        self.patch_all_invalid_pfx.events.create_patch_list_complete -= (
            self.create_patch_list_complete
        )
        self.patch_all_invalid_pfx.events.combine_scraped_data_start -= (
            self.combine_scraped_data_start
        )
        self.patch_all_invalid_pfx.events.combine_scraped_data_complete -= (
            self.combine_scraped_data_complete
        )
