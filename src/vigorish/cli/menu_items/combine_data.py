"""Menu item that combines scraped boxscore data and pitchfx data for a single game."""
import logging
import subprocess
import time
from collections import defaultdict

from tabulate import tabulate

import enlighten
from getch import pause
from halo import Halo

from vigorish.cli.menu_item import MenuItem
from vigorish.cli.prompts import user_options_prompt, audit_report_season_prompt
from vigorish.cli.util import (
    print_message,
    get_random_cli_color,
    get_random_dots_spinner,
    describe_at_bat,
)
from vigorish.config.database import Season
from vigorish.constants import EMOJI_DICT, MENU_NUMBERS
from vigorish.enums import DataSet, ScrapeCondition, AuditError
from vigorish.util.dt_format_strings import DATE_MONTH_NAME
from vigorish.util.list_helpers import flatten_list2d
from vigorish.util.string_helpers import validate_bbref_game_id
from vigorish.util.result import Result

STATUS_BAR_FORMAT = (
    "{total_combined}/{total_games} Games Combined"
    "{fill}"
    "Combining Data for Games Played On: {date_str}"
    "{fill}"
    "Elapsed: {elapsed}"
)
STATUS_BAR_COLOR = "bold_gray100_on_darkviolet"
DATE_BAR_FORMAT = (
    "{desc}{desc_pad}{percentage:3.0f}% |{bar}| {count:{len_total}d}/{total:d} {unit}"
)

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger("enlighten")


class CombineGameDataMenuItem(MenuItem):
    def __init__(self, app, audit_report):
        super().__init__(app)
        self._pbar_manager = None
        self._season = None
        self._date_game_id_map = {}
        self.audit_report = audit_report
        self.menu_item_text = "Combine Game Data"
        self.menu_item_emoji = EMOJI_DICT.get("BANG", "")
        self.exit_menu = False
        self.combine_condition = self.config.get_current_setting(
            "SCRAPED_DATA_COMBINE_CONDITION", DataSet.ALL
        )
        self.audit_type = None
        self.scrape_year = None
        self.current_game_date = None
        self.combine_data_success_game_ids = []
        self.combine_data_fail_results = {}
        self.all_pfx_errors = defaultdict(dict)
        self.status_bar = None
        self.date_progress_bar = None
        self.game_progress_bar_success = None
        self.game_progress_bar_fail = None
        self.game_progress_bar_error = None

    @property
    def pbar_manager(self):
        if self._pbar_manager:
            return self._pbar_manager
        self._pbar_manager = enlighten.get_manager()
        return self._pbar_manager

    @property
    def terminal(self):
        return self.pbar_manager.term

    @property
    def game_bar_format(self):
        SUCCESS = EMOJI_DICT.get("PASSED", "")
        FAIL = EMOJI_DICT.get("FAILED", "")
        ERROR = EMOJI_DICT.get("CONFUSED", "")
        return (
            "{desc}{desc_pad}{percentage:3.0f}% |{bar}| "
            + self.terminal.green3(f"{SUCCESS}" + " {count_0:{len_total}d}")
            + " | "
            + self.terminal.red2(f"{FAIL}" + " {count_1:{len_total}d}")
            + " | "
            + self.terminal.yellow2(f"{ERROR}" + " {count_2:{len_total}d}")
            + " | ETA: {eta}"
        )

    @property
    def season(self):
        if not self.scrape_year:
            return None
        if self._season:
            return self._season
        self._season = Season.find_by_year(self.db_session, self.scrape_year)
        return self._season

    @property
    def date_game_id_map(self):
        if not self.scrape_year:
            return {}
        if self._date_game_id_map:
            return self._date_game_id_map
        scraped_game_date_map = self.get_scraped_game_date_map(self.scrape_year)
        self._date_game_id_map = defaultdict(list)
        for game_map in scraped_game_date_map:
            self._date_game_id_map[game_map["game_date"]].append(game_map["game_id"])
        return self._date_game_id_map

    @property
    def all_dates_in_season(self):
        return (
            [game_date.date() for game_date in self.season.get_date_range()]
            if self.season
            else None
        )

    @property
    def all_dates_with_eligible_games(self):
        return sorted(list({game_date for game_date in self.date_game_id_map.keys()}))

    @property
    def total_dates(self):
        return len(self.all_dates_in_season) if self.audit_type == "SEASON" else 1

    @property
    def all_eligible_games_in_season(self):
        return sorted(flatten_list2d([game_ids for game_ids in self.date_game_id_map.values()]))

    @property
    def total_games(self):
        return (
            sum(len(game_ids) for game_ids in self.date_game_id_map.values())
            if self.audit_type == "SEASON"
            else len(self.date_game_id_map.get(self.current_game_date, None))
        )

    @property
    def total_combined_success(self):
        return len(self.combine_data_success_game_ids)

    @property
    def total_combined_fail(self):
        return len(self.combine_data_fail_results)

    @property
    def total_combined(self):
        return self.total_combined_success + self.total_combined_fail

    @property
    def pfx_errors(self):
        return self.all_pfx_errors[AuditError.PITCHFX_ERROR]

    @property
    def invalid_pfx(self):
        return self.all_pfx_errors[AuditError.INVALID_PITCHFX_DATA]

    @property
    def total_games_pitchfx_error(self):
        return len(self.pfx_errors)

    @property
    def total_games_invalid_pitchfx(self):
        return len(self.invalid_pfx)

    @property
    def game_ids_any_pitchfx_error(self):
        unique_game_ids = {
            game_id for error_dict in self.all_pfx_errors.values() for game_id in error_dict.keys()
        }
        return list(unique_game_ids)

    @property
    def pitch_apps_pitchfx_error(self):
        return flatten_list2d(
            list(pitch_app_dict.keys()) for pitch_app_dict in self.pfx_errors.values()
        )

    @property
    def pitch_apps_invalid_pitchfx(self):
        return flatten_list2d(
            list(pitch_app_dict.keys()) for pitch_app_dict in self.invalid_pfx.values()
        )

    @property
    def total_pitch_apps_pitchfx_error(self):
        return len(self.pitch_apps_pitchfx_error)

    @property
    def total_pitch_apps_invalid_pitchfx(self):
        return len(self.pitch_apps_invalid_pitchfx)

    @property
    def total_pitch_apps_any_pitchfx_error(self):
        return len(self.pitch_apps_pitchfx_error) + len(self.pitch_apps_invalid_pitchfx)

    @property
    def at_bats_pitchfx_error(self):
        return flatten_list2d(
            pitch_app_dict.values() for pitch_app_dict in self.all_pfx_errors.values()
        )

    @property
    def at_bats_invalid_pitchfx(self):
        return flatten_list2d(
            pitch_app_dict.values() for pitch_app_dict in self.invalid_pfx.values()
        )

    @property
    def total_at_bats_pitchfx_error(self):
        return len(self.at_bats_pitchfx_error)

    @property
    def total_at_bats_invalid_pitchfx(self):
        return len(self.at_bats_invalid_pitchfx)

    @property
    def total_at_bats_any_pitchfx_error(self):
        return +len(self.at_bats_pitchfx_error) + len(self.at_bats_invalid_pitchfx)

    @property
    def failed_game_ids(self):
        return list(game_id for game_id in self.combine_data_fail_results.keys())

    def launch(self):
        subprocess.run(["clear"])
        exit_menu = False
        while not exit_menu:
            result = self.audit_type_prompt()
            if result.failure:
                exit_menu = True
                continue
            self.audit_type = result.value
            if self.audit_type == "SEASON":
                result = self.combine_games_for_season()
            if self.audit_type == "DATE":
                result = self.combine_games_for_date()
            if self.audit_type == "GAME":
                result = self.combine_single_game()
                if result.success:
                    exit_menu = True
                    continue
            if result.failure:
                continue
            self.display_results()
            pause(message="Press any key to continue...")
        return Result.Ok(self.exit_menu)

    def combine_games_for_season(self):
        result = audit_report_season_prompt(self.audit_report)
        if result.failure:
            return result
        self.scrape_year = result.value
        self.init_progress_bars(game_date=self.all_dates_in_season[0])
        subprocess.run(["clear"])
        for game_date in self.all_dates_in_season:
            game_ids = self.date_game_id_map.get(game_date, None)
            if not game_ids:
                desc = f"Nothing to combine for {game_date.strftime(DATE_MONTH_NAME)}"
                self.update_progress_bars(game_date, desc)
                self.date_progress_bar.update()
                time.sleep(0.75)
                continue
            result = self.combine_selected_games(game_date, game_ids)
            if result.failure:
                return result
            self.date_progress_bar.update()
        self.close_progress_bars()
        return Result.Ok()

    def init_progress_bars(self, game_date):
        date_str = game_date.strftime(DATE_MONTH_NAME)
        self.status_bar = self.pbar_manager.status_bar(
            status_format=STATUS_BAR_FORMAT,
            color=STATUS_BAR_COLOR,
            justify=enlighten.Justify.CENTER,
            total_combined=self.total_combined,
            total_games=self.total_games,
            date_str=date_str,
        )
        self.date_progress_bar = self.pbar_manager.counter(
            total=self.total_dates,
            bar_format=DATE_BAR_FORMAT,
            desc=date_str,
            unit="days",
            color="cyan2",
            autorefresh=True,
        )
        self.game_progress_bar_success = self.pbar_manager.counter(
            total=self.total_games,
            bar_format=self.game_bar_format,
            desc="Combining data for game...",
            unit="games",
            color="green3",
            autorefresh=True,
        )
        self.game_progress_bar_fail = self.game_progress_bar_success.add_subcounter("red2")
        self.game_progress_bar_error = self.game_progress_bar_success.add_subcounter("yellow2")

    def combine_selected_games(self, game_date, game_ids):
        for bbref_game_id in game_ids:
            fail_results = []
            self.update_progress_bars(game_date, bbref_game_id)
            LOGGER.info(f"Begin combining scraped data for game: {bbref_game_id}")
            result = self.scraped_data.combine_boxscore_and_pfx_data(bbref_game_id)
            if not result["gather_scraped_data_success"]:
                LOGGER.info(f"Unable to combine data for game: {bbref_game_id}")
                LOGGER.info(f"An error occurred gathering scraped data for game: {bbref_game_id}")
                return Result.Fail(result["error"])
            if not result["combined_data_success"]:
                self.combine_data_fail_results[bbref_game_id] = result["error"]
                self.game_progress_bar_fail.update()
                LOGGER.info(f"Failed to combine scraped data for game: {bbref_game_id}")
                LOGGER.info(result["error"])
                continue
            if not result["update_pitch_apps_success"]:
                LOGGER.info(f"Error occurred updating pitch apps for game: {bbref_game_id}")
                LOGGER.info(result["error"])
                return Result.Fail(result["error"])
            pfx_errors = result["results"]["pfx_errors"]
            if pfx_errors.get("pitchfx_error", []):
                self.pfx_errors[bbref_game_id] = pfx_errors["pitchfx_error"]
                fail_results.append(pfx_errors["pitchfx_error"])
            if pfx_errors.get("invalid_pitchfx", []):
                self.invalid_pfx[bbref_game_id] = pfx_errors["invalid_pitchfx"]
                fail_results.append(pfx_errors["invalid_pitchfx"])
            if fail_results:
                self.game_progress_bar_error.update()
                total_pitch_apps = sum(len(f.keys()) for f in fail_results)
                pitch_apps_plural = (
                    "pitch appearances" if total_pitch_apps > 1 else "pitch appearance"
                )
                total_at_bats = sum(
                    len(at_bat_ids) for f in fail_results for at_bat_ids in f.values()
                )
                at_bats_plural = "at bats" if total_at_bats > 1 else "at bat"
                LOGGER.info(f"PitchFX data could not be reconciled for game: {bbref_game_id}")
                LOGGER.info(
                    f"{total_pitch_apps} {pitch_apps_plural} with data errors ({total_at_bats} "
                    f"total {at_bats_plural})\n"
                )
            else:
                self.combine_data_success_game_ids.append(bbref_game_id)
                self.game_progress_bar_success.update()
                # LOGGER.info(f"Successfully combined scraped data for game: {bbref_game_id}")
        return Result.Ok()

    def update_progress_bars(self, game_date, game_id):
        date_str = game_date.strftime(DATE_MONTH_NAME)
        self.status_bar.update(
            total_combined=self.total_combined,
            total_games=self.total_games,
            date_str=date_str,
        )
        self.date_progress_bar.desc = date_str
        self.game_progress_bar_success.desc = game_id

    def close_progress_bars(self):
        self.status_bar.close()
        self.date_progress_bar.close()
        self.game_progress_bar_success.close()
        self.pbar_manager.stop()

    def display_results(self):
        subprocess.run(["clear"])
        if not self.failed_game_ids and not self.total_games_pitchfx_error:
            success_message = (
                f"\nAll game data ({self.total_games} game{'s' if self.total_games > 1 else ''} "
                "total) was successfully combined"
            )
            print_message(success_message, wrap=False, fg="bright_cyan", bold=True)
            return
        if self.failed_game_ids:
            self.display_games_failed_to_combine()
        if self.all_pfx_errors:
            self.display_pitchfx_errors()

    def display_games_failed_to_combine(self):
        error_message = (
            f"Error prevented scraped data being combined for {len(self.failed_game_ids)} "
            "games:"
        )
        error_details = [
            {"bbref_game_id": game_id, "error": error}
            for game_id, error in self.combine_data_fail_results
        ]
        print_message(error_message, wrap=False, fg="bright_red", bold=True)
        print_message(tabulate(error_details, headers="Keys"), wrap=False, fg="bright_red")

    def display_pitchfx_errors(self):
        error_data = [
            self.get_at_bat_details(at_bat_id)
            for error_type, pfx_error_dict in self.all_pfx_errors.items()
            for game_id, pitch_app_dict in pfx_error_dict.items()
            for pitch_app_id, at_bat_ids in pitch_app_dict.items()
            for at_bat_id in at_bat_ids
        ]
        games_plural = "games contain" if self.total_games_pitchfx_error > 1 else "game contains"
        ab_plural = "at bats" if len(error_data) > 1 else "at bat"
        table_header = (
            f"{self.total_games_pitchfx_error} {games_plural} invalid PitchFX data for a total of "
            f"{len(error_data)} {ab_plural}, you can attempt to fix these errors using the "
            "Investigate Failures menu:\n"
        )
        print_message(table_header, fg="bright_cyan", bold=True)
        print_message(tabulate(error_data, headers="keys"), wrap=False)
        print()

    def get_at_bat_details(self, at_bat_id):
        ab_dict = describe_at_bat(self.db_session, at_bat_id)
        return {
            "game_id": ab_dict["game_id"],
            "inning": ab_dict["inning_label"],
            "pitcher": f'{ab_dict["pitcher_name"]} ({ab_dict["pitcher_team"]})',
            "batter": f'{ab_dict["batter_name"]} ({ab_dict["batter_team"]})',
        }

    def combine_games_for_date(self):
        result = audit_report_season_prompt(self.audit_report)
        if result.failure:
            return result
        self.scrape_year = result.value
        result = self.game_date_prompt()
        if result.failure:
            return result
        self.current_game_date = result.value
        self.init_progress_bars(game_date=self.current_game_date)
        subprocess.run(["clear"])
        game_ids = self.date_game_id_map.get(self.current_game_date, None)
        if not game_ids:
            game_date_str = self.current_game_date.strftime(DATE_MONTH_NAME)
            message = f"All games on {game_date_str} have already been combined."
            print_message(message, fg="bright_cyan", bold=True)
            self.close_progress_bars()
            return Result.Ok()
        result = self.combine_selected_games(self.current_game_date, game_ids)
        self.date_progress_bar.update()
        self.close_progress_bars()
        return result

    def combine_single_game(self):
        result = audit_report_season_prompt(self.audit_report)
        if result.failure:
            return result
        self.scrape_year = result.value
        result = self.game_id_prompt()
        if result.failure:
            return result
        combine_game_id = result.value
        return self.combine_scraped_data_for_game(combine_game_id)

    def combine_scraped_data_for_game(self, combine_game_id):
        subprocess.run(["clear"])
        spinner = Halo(color=get_random_cli_color(), spinner=get_random_dots_spinner())
        spinner.text = f"Combining scraped data for {combine_game_id}..."
        spinner.start()
        result = self.scraped_data.combine_boxscore_and_pfx_data(combine_game_id)
        if (
            not result["gather_scraped_data_success"]
            or not result["combined_data_success"]
            or not result["update_pitch_apps_success"]
        ):
            spinner.fail(f"Failed to combine data for {combine_game_id}!")
            print_message(result["error"], wrap=False, fg="bright_red", bold=True)
            return Result.Fail(result["error"])
        pfx_errors = result["results"]["all_pfx_errors"]
        if pfx_errors.get("pitchfx_error", []):
            self.pfx_errors[combine_game_id] = pfx_errors["pitchfx_error"]
        if pfx_errors.get("invalid_pitchfx", []):
            self.invalid_pfx[combine_game_id] = pfx_errors["invalid_pitchfx"]
        if self.total_pitch_apps_any_pitchfx_error > 0:
            pitch_apps_plural = (
                "pitch appearances"
                if self.total_pitch_apps_any_pitchfx_error > 1
                else "pitch appearance"
            )
            at_bats_plural = "at bats" if self.total_at_bats_any_pitchfx_error > 1 else "at bat"
            LOGGER.info(f"PitchFX data could not be reconciled for game: {combine_game_id}")
            LOGGER.info(
                f"{self.total_pitch_apps_any_pitchfx_error} {pitch_apps_plural} with data errors "
                f"({self.total_at_bats_any_pitchfx_error} total {at_bats_plural})\n"
            )
        else:
            message = f"All scraped data for {combine_game_id} was successfully combined!"
            print_message(message, fg="bright_cyan", bold=True)
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

    def game_date_prompt(self):
        choices = {
            f"{EMOJI_DICT.get('BOLT')} {game_date.strftime(DATE_MONTH_NAME)}": game_date
            for game_date in self.all_dates_with_eligible_games
        }
        choices[f"{EMOJI_DICT.get('BACK')} Return to Previous Menu"] = None
        prompt = "Select a date to combine scraped data for all games that took place on that date"
        return user_options_prompt(choices, prompt)

    def game_id_prompt(self):
        choices = {
            f"{EMOJI_DICT.get('BOLT')} {game_id}": game_id
            for game_id in self.all_eligible_games_in_season
        }
        choices[f"{EMOJI_DICT.get('BACK')} Return to Previous Menu"] = None
        prompt = "Select the BBRef.com Game ID to combine scraped data:"
        return user_options_prompt(choices, prompt)

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

    def get_all_eligible_game_ids(self, year):
        return (
            self.audit_report[year]["scraped"]
            + self.audit_report[year]["successful"]
            + self.audit_report[year]["failed"]
            + self.audit_report[year]["pfx_error"]
            + self.audit_report[year]["invalid_pfx"]
        )

    def get_game_id_date_map(self, bbref_game_id):
        game_dict = validate_bbref_game_id(bbref_game_id).value
        return {"game_id": bbref_game_id, "game_date": game_dict["game_date"], "count": 1}
