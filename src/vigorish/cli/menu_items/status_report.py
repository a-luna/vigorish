"""Menu item that allows the user to initialize/reset the database."""
import subprocess
from pathlib import Path

from bullet import Numbers, Bullet, colors
from getch import pause

from vigorish.cli.menu_item import MenuItem
from vigorish.cli.util import print_message, prompt_user_yes_no, DateInput
from vigorish.config.database import Season
from vigorish.constants import EMOJI_DICT, MENU_NUMBERS
from vigorish.enums import StatusReport
from vigorish.status.report_status import (
    report_status_single_date,
    report_season_status,
    report_date_range_status,
)
from vigorish.util.result import Result
from vigorish.util.string_helpers import wrap_text


class StatusReportMenuItem(MenuItem):
    def __init__(self, db_session, config, scraped_data):
        self.db_session = db_session
        self.config = config
        self.scraped_data = scraped_data
        self.menu_item_text = f"Status Reports"
        self.menu_item_emoji = EMOJI_DICT.get("CHART")
        self.exit_menu = False

    def launch(self):
        subprocess.run(["clear"])
        result = self.report_options_prompt()
        if result.failure:
            return Result.Ok(self.exit_menu)
        report_type = result.value
        if report_type == "SEASON":
            result = self.season_status_report()
        if report_type == "SINGLE_DATE":
            result = self.single_date_report()
        if report_type == "DATE_RANGE":
            result = self.date_range_report()
        return result if result else Result.Ok()

    def report_options_prompt(self):
        choices = {
            f"{MENU_NUMBERS.get(1)}  Season": "SEASON",
            f"{MENU_NUMBERS.get(2)}  Single Date": "SINGLE_DATE",
            f"{MENU_NUMBERS.get(3)}  Date Range": "DATE_RANGE",
            f"{EMOJI_DICT.get('BACK')} Return to Main Menu": None,
        }
        return self.user_options_prompt(choices)

    def get_season_report_type_from_user(self):
        choices = {
            f"{MENU_NUMBERS.get(1)}  Season Summary": StatusReport.SEASON_SUMMARY,
            f"{MENU_NUMBERS.get(2)}  Dates Missing Data (Summary)": StatusReport.DATE_SUMMARY_MISSING_DATA,
            f"{MENU_NUMBERS.get(3)}  All Dates In Season (Summary)": StatusReport.DATE_SUMMARY_ALL_DATES,
            f"{MENU_NUMBERS.get(4)}  Dates Missing Data (Detail)": StatusReport.DATE_DETAIL_MISSING_DATA,
            f"{MENU_NUMBERS.get(5)}  All Dates In Season (Detail)": StatusReport.DATE_DETAIL_ALL_DATES,
            f"{MENU_NUMBERS.get(6)}  All Dates In Season + Missing PitchFx IDs (Detail)": StatusReport.DATE_DETAIL_MISSING_PITCHFX,
            f"{EMOJI_DICT.get('BACK')} Return to Previous Menu": None,
        }
        return self.user_options_prompt(choices)

    def get_single_date_report_type_from_user(self):
        choices = {
            f"{MENU_NUMBERS.get(1)}  Detail Report (No Missing IDs or Game Status)": StatusReport.DATE_DETAIL_ALL_DATES,
            f"{MENU_NUMBERS.get(2)}  Detail Report with Missing PitchFx IDs": StatusReport.DATE_DETAIL_MISSING_PITCHFX,
            f"{MENU_NUMBERS.get(3)}  Detail Report with Missing PitchFx IDs and Game Status": StatusReport.SINGLE_DATE_WITH_GAME_STATUS,
            f"{EMOJI_DICT.get('BACK')} Return to Previous Menu": None,
        }
        return self.user_options_prompt(choices)

    def get_date_range_report_type_from_user(self):
        choices = {
            f"{MENU_NUMBERS.get(1)}  Dates Missing Data (Summary)": StatusReport.DATE_SUMMARY_MISSING_DATA,
            f"{MENU_NUMBERS.get(2)}  All Dates In Range (Summary)": StatusReport.DATE_SUMMARY_ALL_DATES,
            f"{MENU_NUMBERS.get(3)}  Dates Missing Data (Detail)": StatusReport.DATE_DETAIL_MISSING_DATA,
            f"{MENU_NUMBERS.get(4)}  All Dates In Range (Detail)": StatusReport.DATE_DETAIL_ALL_DATES,
            f"{MENU_NUMBERS.get(5)}  All Dates In Range + Missing PitchFx IDs (Detail)": StatusReport.DATE_DETAIL_MISSING_PITCHFX,
            f"{EMOJI_DICT.get('BACK')} Return to Previous Menu": None,
        }
        return self.user_options_prompt(choices)

    def user_options_prompt(self, choices):
        prompt = Bullet(
            "Choose the type of report you wish to generate from the options below:",
            choices=[choice for choice in choices.keys()],
            bullet="",
            shift=1,
            indent=2,
            margin=2,
            bullet_color=colors.foreground["default"],
            background_color=colors.foreground["default"],
            background_on_switch=colors.foreground["default"],
            word_color=colors.foreground["default"],
            word_on_switch=colors.bright(colors.foreground["cyan"]),
        )
        subprocess.run(["clear"])
        choice_text = prompt.launch()
        choice_value = choices.get(choice_text)
        return Result.Ok(choice_value) if choice_value else Result.Fail("")

    def season_status_report(self):
        year = self.get_mlb_season_from_user()
        result = self.get_season_report_type_from_user()
        if result.failure:
            return result
        report = result.value
        refresh = self.prompt_user_refresh_data()
        subprocess.run(["clear"])
        result = report_season_status(self.db_session, self.scraped_data, refresh, year, report)
        if result.failure:
            return result
        pause(message="Press any key to continue...")
        return Result.Ok()

    def single_date_report(self):
        game_date = self.get_date_from_user("Report status for date:")
        result = self.get_single_date_report_type_from_user()
        if result.failure:
            return result
        report = result.value
        refresh = self.prompt_user_refresh_data()
        subprocess.run(["clear"])
        result = report_status_single_date(
            self.db_session, self.scraped_data, refresh, game_date, report
        )
        if result.failure:
            return result
        pause(message="Press any key to continue...")
        return Result.Ok()

    def date_range_report(self):
        start_date = self.get_date_from_user("Report status start date: ")
        end_date = self.get_date_from_user("Report status end date: ")
        result = self.get_date_range_report_type_from_user()
        if result.failure:
            return result
        report = result.value
        refresh = self.prompt_user_refresh_data()
        subprocess.run(["clear"])
        result = report_date_range_status(
            self.db_session, self.scraped_data, refresh, start_date, end_date, report
        )
        if result.failure:
            return result
        pause(message="Press any key to continue...")
        return Result.Ok()

    def get_mlb_season_from_user(self):
        year_is_valid = False
        while not year_is_valid:
            subprocess.run(["clear"])
            prompt = Numbers("Enter a year of an MLB season: ")
            year = prompt.launch()
            season = Season.find_by_year(self.db_session, year)
            if not season:
                continue
            year_is_valid = True
        return year

    def get_date_from_user(self, prompt):
        user_date = None
        while not user_date:
            subprocess.run(["clear"])
            date_prompt = DateInput(prompt=prompt)
            result = date_prompt.launch()
            if result:
                user_date = result
        return user_date

    def prompt_user_refresh_data(self):
        prompt = "Would you like to refresh the data before generating the report?"
        result = prompt_user_yes_no(prompt=prompt)
        return result.value
