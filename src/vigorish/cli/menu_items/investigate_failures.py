import subprocess

from getch import pause
from halo import Halo

from vigorish.cli.menu_item import MenuItem
from vigorish.cli.prompts import user_options_prompt
from vigorish.cli.util import print_message
from vigorish.constants import EMOJI_DICT, MENU_NUMBERS
from vigorish.status.update_status_combined_data import (
    update_pitch_apps_for_game_audit_successful,
    update_pitch_appearances_audit_failed,
)
from vigorish.util.list_helpers import flatten_list2d
from vigorish.util.string_helpers import parse_pitch_app_details_from_string
from vigorish.util.result import Result


class InvestigateFailuresMenuItem(MenuItem):
    def __init__(self, app, audit_report):
        super().__init__(app)
        self.audit_report = audit_report
        self.failed_game_ids = flatten_list2d(
            [audit["failed"] for audit in self.audit_report.values()]
        )
        self.data_error_game_ids = flatten_list2d(
            [audit["data_error"] for audit in self.audit_report.values()]
        )
        self.menu_item_text = "Investigate Failures"
        self.menu_item_emoji = EMOJI_DICT.get("FLASHLIGHT")
        self.exit_menu = False

    def launch(self):
        subprocess.run(["clear"])
        exit_menu = False
        while not exit_menu:
            result = self.select_failure_type()
            if result.failure:
                exit_menu = True
                continue
            failure_type = result.value
            if failure_type == "AUDIT_FAIL":
                game_ids = self.failed_game_ids
            if failure_type == "DATA_ERROR":
                game_ids = self.data_error_game_ids
            result = self.select_game_prompt(game_ids)
            if result.failure:
                exit_menu = True
                continue
            selected_game_id = result.value
            result = self.select_action_prompt()
            if result.failure:
                continue
            selected_action = result.value
            if selected_action == "REPORT":
                subprocess.run(["clear"])
                result = self.generate_investigative_materials(selected_game_id)
                if result.failure:
                    return Result.Ok(self.exit_menu)
            if selected_action == "RETRY":
                result = self.combine_scraped_data_for_game(selected_game_id)
                if result.failure:
                    return Result.Ok(self.exit_menu)
        return Result.Ok(self.exit_menu)

    def select_failure_type(self):
        prompt = "Select the type of issue to investigate:"
        choices = {}
        if self.failed_game_ids:
            game_count = len(self.failed_game_ids)
            menu_item = f"{EMOJI_DICT.get('SHRUG')} Unable to reconcile data ({game_count} Games)"
            choices[menu_item] = "AUDIT_FAIL"
        if self.data_error_game_ids:
            game_count = len(self.data_error_game_ids)
            menu_item = f"{EMOJI_DICT.get('WEARY')} Extra/Missing pitch data ({game_count} Games)"
            choices[menu_item] = "DATA_ERROR"
        choices[f"{EMOJI_DICT.get('BACK')} Return to Main Menu"] = None
        return user_options_prompt(choices, prompt)

    def select_game_prompt(self, game_ids):
        prompt = "Select a game from the list below:"
        choices = {
            f"{MENU_NUMBERS.get(num, str(num))}  {game_id}": game_id
            for num, game_id in enumerate(game_ids, start=1)
        }
        choices[f"{EMOJI_DICT.get('BACK')} Return to Main Menu"] = None
        return user_options_prompt(choices, prompt)

    def select_action_prompt(self):
        prompt = "What would you like to do at this point?"
        choices = {
            f"{MENU_NUMBERS.get(1)}  Analyze Pitching Data": "REPORT",
            f"{MENU_NUMBERS.get(2)}  Attempt to Combine Data (Again)": "RETRY",
            f"{EMOJI_DICT.get('BACK')} Return to Previous Menu": None,
        }
        return user_options_prompt(choices, prompt)

    def generate_investigative_materials(self, selected_game_id):
        result = self.scraped_data.generate_investigative_materials(selected_game_id)
        if result.failure:
            no_errors = (
                f"The selected game ({selected_game_id}) did not raise any errors when attempting "
                "to combine pitch data. Please try to combine all scraped data for this "
                "particular game again."
            )
            print_message(no_errors, fg="bright_yellow", bold=True)
            pause(message="Press any key to continue...")
            return Result.Fail("")
        game_data = result.value
        print_message(game_data.error_message, wrap=False, fg="bright_red")
        pause(message="Press any key to continue...")
        self.failed_pitch_app_dict = parse_pitch_app_details_from_string(game_data.error_message)
        self.boxscore = game_data.boxscore
        self.player_id_dict = game_data.player_id_dict
        self.at_bat_event_groups = game_data.at_bat_event_groups
        self.pitchfx_logs_for_game = game_data.pitchfx_logs_for_game
        self.all_pfx_data_for_game = game_data.all_pfx_data_for_game
        breakpoint()
        return Result.Ok(False)

    def construct_new_innings_list(self):
        pass

    def combine_scraped_data_for_game(self, combine_game_id):
        subprocess.run(["clear"])
        spinner = Halo(color="yellow", spinner="dots3")
        spinner.start()
        spinner.text = f"Combining scraped data for {combine_game_id}..."
        result = self.scraped_data.combine_boxscore_and_pfx_data(combine_game_id)
        if result.failure:
            spinner.stop()
            spinner.clear()
            failed_pitch_app_dict = parse_pitch_app_details_from_string(result.error)
            pitch_app_ids = failed_pitch_app_dict.keys()
            result = update_pitch_appearances_audit_failed(self.db_session, pitch_app_ids)
            if result.failure:
                return result
            error_message = f"Error prevented scraped data being combined for {combine_game_id}:"
            print_message(error_message, fg="bright_red", bold=True)
            print_message(result.error, fg="bright_red")
        else:
            result = update_pitch_apps_for_game_audit_successful(
                self.db_session, self.scraped_data, combine_game_id
            )
            if result.failure:
                spinner.stop()
                spinner.clear()
                error = f"Error occurred updating database with combined data ({combine_game_id})"
                print_message(error, fg="bright_red", bold=True)
                print_message(result.error, fg="bright_red")
            else:
                spinner.succeed(f"Successfully combined all data for game: {combine_game_id}")
        return Result.Ok()
