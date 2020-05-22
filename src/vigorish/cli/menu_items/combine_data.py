"""Menu item that combines scraped boxscore data and pitchfx data for a single game."""
import subprocess

from getch import pause
from halo import Halo
from tqdm import tqdm

from vigorish.cli.menu_item import MenuItem
from vigorish.cli.util import print_message, user_options_prompt
from vigorish.constants import EMOJI_DICT, MENU_NUMBERS
from vigorish.data.process.combine_boxscore_and_pitchfx_for_game import combine_data
from vigorish.enums import DataSet, ScrapeCondition
from vigorish.status.update_status_combined_data import (
    update_pitch_apps_for_game_audit_successful,
    update_pitch_appearances_audit_failed,
)
from vigorish.util.regex import AT_BAT_ID_REGEX
from vigorish.util.dt_format_strings import DATE_MONTH_NAME
from vigorish.util.string_helpers import validate_bbref_game_id, validate_at_bat_id
from vigorish.util.result import Result


class CombineGameDataMenuItem(MenuItem):
    def __init__(self, app, audit_report):
        super().__init__(app)
        self.audit_report = audit_report
        self.menu_item_text = "Combine Game Data"
        self.menu_item_emoji = EMOJI_DICT.get("BANG", "")
        self.exit_menu = False
        self.combine_condition = self.config.get_current_setting(
            "SCRAPED_DATA_COMBINE_CONDITION", DataSet.ALL
        )

    def launch(self):
        subprocess.run(["clear"])
        exit_menu = False
        while not exit_menu:
            result = self.audit_type_prompt()
            if result.failure:
                exit_menu = True
                continue
            audit_type = result.value
            if audit_type == "SEASON":
                result = self.combine_games_for_season()
            if audit_type == "DATE":
                result = self.combine_games_for_date()
            if audit_type == "GAME":
                result = self.combine_single_game()
                if result.success:
                    exit_menu = True
                    continue
            if result.failure:
                continue
            (audit_errors, update_errors, audit_complete) = result.value
            self.display_results(audit_complete, audit_errors)
            pause(message="Press any key to continue...")
        return Result.Ok(self.exit_menu)

    def combine_games_for_season(self):
        result = self.season_prompt()
        if result.failure:
            return result
        year = result.value
        print()
        combine_games = (
            self.audit_report[year]["scraped"]
            if self.combine_condition == ScrapeCondition.ONLY_MISSING_DATA
            else self.get_all_eligible_game_ids(year)
            if self.combine_condition == ScrapeCondition.ALWAYS
            else None
        )
        msg = f"Combining scraped data for {len(combine_games)} games (Season: MLB {year})..."
        print_message(msg, fg="bright_yellow", bold=True)
        (audit_errors, update_errors, audit_complete) = self.combine_selected_games(combine_games)
        return Result.Ok((audit_errors, update_errors, audit_complete))

    def combine_games_for_date(self):
        result = self.season_prompt()
        if result.failure:
            return result
        year = result.value
        result = self.game_date_prompt(year)
        if result.failure:
            return result
        game_date = result.value
        subprocess.run(["clear"])
        game_date_str = game_date.strftime(DATE_MONTH_NAME)
        combine_games = [
            map["game_id"]
            for map in self.get_scraped_game_date_map(year)
            if map["game_date"] == game_date
        ]
        msg = f"Combining scraped data for {len(combine_games)} games (Date: {game_date_str})..."
        print_message(msg, fg="bright_yellow", bold=True)
        (audit_errors, update_errors, audit_complete) = self.combine_selected_games(combine_games)
        return Result.Ok((audit_errors, update_errors, audit_complete))

    def combine_single_game(self):
        result = self.season_prompt()
        if result.failure:
            return result
        year = result.value
        scraped_games = (
            self.audit_report[year]["scraped"]
            if self.combine_condition == ScrapeCondition.ONLY_MISSING_DATA
            else self.get_all_eligible_game_ids(year)
            if self.combine_condition == ScrapeCondition.ALWAYS
            else None
        )
        result = self.game_id_prompt(scraped_games)
        if result.failure:
            return result
        combine_game_id = result.value
        spinner = Halo(color="yellow", spinner="dots3")
        spinner.start()
        spinner.text = f"Combining scraped data for {combine_game_id}..."
        result = combine_data(self.db_session, self.scraped_data, combine_game_id)
        spinner.stop()
        spinner.clear()
        if result.failure:
            pitch_app_ids = self.parse_pitch_app_ids_from_error_message(result.error)
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
                error = f"Error occurred updating database with combined data ({combine_game_id})"
                print_message(error, fg="bright_red", bold=True)
                print_message(result.error, fg="bright_red")
            else:
                print_message(
                    "All game data was successfully combined", fg="bright_cyan", bold=True
                )
        return Result.Ok()

    def combine_selected_games(self, selected_game_ids):
        audit_errors = {}
        audit_complete = {}
        update_errors = {}
        with tqdm(total=len(selected_game_ids), unit="game", ncols=70) as pbar:
            for bbref_game_id in selected_game_ids:
                pbar.desc = f"{bbref_game_id} ({len(audit_errors)} Errors)"
                result = combine_data(self.db_session, self.scraped_data, bbref_game_id)
                if result.failure:
                    audit_errors[bbref_game_id] = result.error
                    pitch_app_ids = self.parse_pitch_app_ids_from_error_message(result.error)
                    result = update_pitch_appearances_audit_failed(self.db_session, pitch_app_ids)
                    if result.failure:
                        return result
                    pbar.update()
                    continue
                audit_complete[bbref_game_id] = result.value
                result = update_pitch_apps_for_game_audit_successful(
                    self.db_session, self.scraped_data, bbref_game_id
                )
                if result.failure:
                    update_errors[bbref_game_id] = result.error
                    pbar.update()
                    continue
                pbar.update()
        return (audit_errors, update_errors, audit_complete)

    def display_results(self, audit_complete, audit_errors):
        total_games = len(audit_complete) + len(audit_errors)
        if not audit_errors:
            success_message = (
                f"\nAll game data ({total_games} game{'s' if len(audit_complete) > 1 else ''} "
                "total) was successfully combined"
            )
            print_message(success_message, fg="bright_cyan", bold=True)
        else:
            all_errors = (
                f"\n{len(audit_errors)} game{'s' if len(audit_errors) > 1 else ''} could not be "
                "combined, see details below:"
            )
            print_message(f"{all_errors}\n", wrap=False, fg="bright_red", bold=True)
            for game_num, (bbref_game_id, error) in enumerate(audit_errors.items(), start=1):
                error_header = f"{'#'*10} Game #{game_num} (BBRef ID: {bbref_game_id}) {'#'*10}"
                error_details = f"\n{error}\n"
                print_message(error_header, wrap=False, fg="bright_red", bold=True)
                print_message(error_details, wrap=False, fg="bright_red")

    def parse_pitch_app_ids_from_error_message(self, error_message):
        failed_pitch_app_ids = []
        for match in AT_BAT_ID_REGEX.finditer(error_message):
            at_bat_data = validate_at_bat_id(match[0]).value
            failed_pitch_app_ids.append(at_bat_data["pitch_app_id"])
        return list(set(failed_pitch_app_ids))

    def audit_type_prompt(self):
        prompt = (
            "For games where all data has been scraped, would you like to combine and audit the "
            "data for all games in the same season, all games on a single date or for a single "
            "game?"
        )
        choices = {
            f"{MENU_NUMBERS.get(1)}  By Season": "SEASON",
            f"{MENU_NUMBERS.get(2)}  By Date": "DATE",
            f"{MENU_NUMBERS.get(3)}  By Game": "GAME",
            f"{EMOJI_DICT.get('BACK')} Return to Main Menu": None,
        }
        return user_options_prompt(choices, prompt)

    def season_prompt(self):
        prompt = "First, select an MLB season from the list below:"
        choices = {
            f"{MENU_NUMBERS.get(num)}  {year}": year
            for num, year in enumerate(self.audit_report.keys(), start=1)
        }
        choices[f"{EMOJI_DICT.get('BACK')} Return to Previous Menu"] = None
        return user_options_prompt(choices, prompt)

    def game_date_prompt(self, year):
        choices = {}
        for game_date in self.get_all_dates_in_season(year):
            choices[f"{EMOJI_DICT.get('BOLT')} {game_date.strftime(DATE_MONTH_NAME)}"] = game_date
        prompt = "Select a date to combine scraped data for all games that took place on that date"
        return user_options_prompt(choices, prompt)

    def game_id_prompt(self, scraped_games):
        prompt = "Select the BBRef.com Game ID to combine scraped data:"
        choices = {f"{EMOJI_DICT.get('BOLT')} {game_id}": game_id for game_id in scraped_games}
        return user_options_prompt(choices, prompt)

    def get_all_dates_in_season(self, year):
        return sorted(
            list(set([map["game_date"] for map in self.get_scraped_game_date_map(year)]))
        )

    def get_all_eligible_game_ids(self, year):
        return (
            self.audit_report[year]["scraped"]
            + self.audit_report[year]["successful"]
            + self.audit_report[year]["failed"]
        )

    def get_scraped_game_date_map(self, year):
        return (
            self.get_never_audited_game_date_map(year)
            if self.combine_condition == ScrapeCondition.ONLY_MISSING_DATA
            else self.get_all_eligible_game_date_map(year)
            if self.combine_condition == ScrapeCondition.ALWAYS
            else None
        )

    def get_never_audited_game_date_map(self, year):
        return [self.get_game_id_date_map(gid) for gid in self.audit_report[year]["scraped"]]

    def get_successful_game_date_map(self, year):
        return [self.get_game_id_date_map(gid) for gid in self.audit_report[year]["successful"]]

    def get_failed_game_date_map(self, year):
        return [self.get_game_id_date_map(gid) for gid in self.audit_report[year]["failed"]]

    def get_all_eligible_game_date_map(self, year):
        return [self.get_game_id_date_map(gid) for gid in self.get_all_eligible_game_ids(year)]

    def get_game_id_date_map(self, bbref_game_id):
        game_dict = validate_bbref_game_id(bbref_game_id).value
        return {"game_id": bbref_game_id, "game_date": game_dict["game_date"], "count": 1}
