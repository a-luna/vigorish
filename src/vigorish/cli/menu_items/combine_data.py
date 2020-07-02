"""Menu item that combines scraped boxscore data and pitchfx data for a single game."""
import subprocess

from getch import pause
from halo import Halo
from pprint import pformat

from vigorish.cli.menu_item import MenuItem
from vigorish.cli.prompts import user_options_prompt
from vigorish.cli.util import print_message, get_random_cli_color, get_random_dots_spinner
from vigorish.config.database import Season
from vigorish.constants import EMOJI_DICT, MENU_NUMBERS
from vigorish.enums import DataSet, ScrapeCondition
from vigorish.status.update_status_combined_data import (
    update_pitch_apps_for_game_audit_successful,
    update_pitch_appearances_audit_failed,
    get_pitch_app_status,
)
from vigorish.util.dt_format_strings import DATE_MONTH_NAME
from vigorish.util.list_helpers import report_dict
from vigorish.util.string_helpers import (
    validate_bbref_game_id,
    parse_pitch_app_details_from_string,
)
from vigorish.util.result import Result
import snoop


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
            (total_games, audit_errors, update_errors, pitchfx_data_errors) = result.value
            self.display_results(total_games, audit_errors, update_errors, pitchfx_data_errors)
            pause(message="Press any key to continue...")
        return Result.Ok(self.exit_menu)

    def combine_games_for_season(self):
        result = self.season_prompt()
        if result.failure:
            return result
        year = result.value
        season = Season.find_by_year(self.db_session, year)
        subprocess.run(["clear"])
        all_dates_in_season = self.get_all_dates_in_season(year)
        total_games = 0
        spinner = Halo(spinner="weather", color=get_random_cli_color())
        spinner.text = (
            f"Completed {total_games} games (0% Complete) for MLB {season.year} (0 Errors)..."
        )
        spinner.start()
        season_audit_errors = {}
        season_update_errors = {}
        season_pitchfx_data_errors = []
        for num, game_date in enumerate(all_dates_in_season, start=1):
            combine_games = list(
                set(
                    [
                        map["game_id"]
                        for map in self.get_scraped_game_date_map(year)
                        if map["game_date"] == game_date
                    ]
                )
            )
            spinner.stop_and_persist()
            (audit_errors, update_errors, pitchfx_data_errors,) = self.combine_selected_games(
                combine_games
            )
            subprocess.run(["clear"])
            season_audit_errors.update(audit_errors)
            season_update_errors.update(update_errors)
            season_pitchfx_data_errors.extend(pitchfx_data_errors)
            total_games += len(combine_games)
            percent_complete = num / float(len(all_dates_in_season))
            total_errors = (
                len(season_audit_errors)
                + len(season_update_errors)
                + len(season_pitchfx_data_errors)
            )
            spinner.text = (
                f"Completed {total_games} games ({percent_complete:.0%}) for MLB {season.year} "
                f"({total_errors} Errors)..."
            )
            spinner.start()
        spinner.stop()
        spinner.clear()
        return Result.Ok(
            (total_games, season_audit_errors, season_update_errors, season_pitchfx_data_errors)
        )

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
        combine_games = list(
            set(
                [
                    map["game_id"]
                    for map in self.get_scraped_game_date_map(year)
                    if map["game_date"] == game_date
                ]
            )
        )
        game_date_str = game_date.strftime(DATE_MONTH_NAME)
        msg = (
            f"Combining scraped data for {len(combine_games)} games "
            f"(Game Date: {game_date_str})..."
        )
        print_message(msg, fg="bright_magenta", bold=True)
        (audit_errors, update_errors, pitchfx_data_errors) = self.combine_selected_games(
            combine_games
        )
        return Result.Ok((len(combine_games), audit_errors, update_errors, pitchfx_data_errors))

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
        return self.combine_scraped_data_for_game(combine_game_id)

    def combine_selected_games(self, selected_game_ids):
        audit_errors = {}
        update_errors = {}
        pitchfx_data_errors = {}
        spinner = Halo(spinner=get_random_dots_spinner(), color=get_random_cli_color())
        spinner.text = (
            f"Combined 0/{len(selected_game_ids)} Games Successfully, "
            f"0 Errors (Current Game ID: {selected_game_ids[0]})"
        )
        spinner.start()
        for num, bbref_game_id in enumerate(selected_game_ids, start=1):
            result = self.scraped_data.combine_boxscore_and_pfx_data(bbref_game_id)
            if result.failure:
                audit_errors[bbref_game_id] = result.error
                failed_pitch_app_dict = parse_pitch_app_details_from_string(result.error)
                pitch_app_ids = failed_pitch_app_dict.keys()
                result = update_pitch_appearances_audit_failed(self.db_session, pitch_app_ids)
                if result.failure:
                    spinner.stop()
                    spinner.clear()
                    update_errors[bbref_game_id] = result.error
                    return result
                continue
            result = update_pitch_apps_for_game_audit_successful(
                self.db_session, self.scraped_data, bbref_game_id
            )
            if result.failure:
                spinner.stop()
                spinner.clear()
                update_errors[bbref_game_id] = result.error
                continue
            game_results = result.value
            if len(game_results["failed"]):
                pitchfx_data_errors[bbref_game_id] = game_results["failed"]
            total_errors = len(audit_errors) + len(update_errors) + len(pitchfx_data_errors)
            spinner.text = (
                f"Combined {num}/{len(selected_game_ids)} Games Successfully, "
                f"{total_errors} Errors (Current Game ID: {bbref_game_id})"
            )
        spinner.stop()
        spinner.clear()
        return (audit_errors, update_errors, pitchfx_data_errors)

    @snoop
    def display_results(self, total_games, audit_errors, update_errors, pitchfx_data_errors):
        subprocess.run(["clear"])
        if not audit_errors and not update_errors and not pitchfx_data_errors:
            success_message = (
                f"\nAll game data ({total_games} game{'s' if total_games > 1 else ''} "
                "total) was successfully combined"
            )
            print_message(success_message, fg="bright_cyan", bold=True)
        else:
            if audit_errors:
                for game_id, error_detail in audit_errors.items():
                    failed_pitch_app_dict = parse_pitch_app_details_from_string(error_detail)
                    pitch_app_ids = failed_pitch_app_dict.keys()
                    error_message = f"Error prevented scraped data being combined for {game_id}:"
                    print_message(error_message, wrap=False, fg="bright_red", bold=True)
                    print_message(error_detail, wrap=False, fg="bright_red")
            if update_errors:
                error_message = (
                    f"{len(update_errors)} game_ids exited due to error when updating "
                    "the database:"
                )
                print_message(error_message, wrap=False, fg="bright_red", bold=True)
                print_message(pformat(update_errors), wrap=False, fg="bright_red")
            if pitchfx_data_errors:
                for game_id, pitch_app_ids in pitchfx_data_errors.items():
                    all_errors = (
                        f"\n{len(pitch_app_ids)} "
                        f"pitch appearance{'s' if len(pitch_app_ids) > 1 else ''} "
                        "could not be combined:"
                    )
                    print_message(f"{all_errors}\n", wrap=False, fg="bright_red")
                    for num, pitch_app_id in enumerate(pitch_app_ids, start=1):
                        pitch_app_status = get_pitch_app_status(
                            self.db_session, pitch_app_id
                        ).value
                        error_header = (
                            f"{'#'*10} Pitch Appearance #{num} "
                            f"(Pitch Appearance ID: {pitch_app_id}) {'#'*10}"
                        )
                        error_details = f"\n{report_dict(pitch_app_status.as_dict())}\n"
                        print_message(error_header, wrap=False, fg="bright_red", bold=True)
                        print_message(error_details, wrap=False, fg="bright_red")

    def combine_scraped_data_for_game(self, combine_game_id):
        subprocess.run(["clear"])
        spinner = Halo(color="yellow", spinner="dots3")
        spinner.text = f"Combining scraped data for {combine_game_id}..."
        spinner.start()
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
