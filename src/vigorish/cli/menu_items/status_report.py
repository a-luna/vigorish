"""Menu item that allows the user to initialize/reset the database."""
import subprocess

from vigorish.cli.components import season_prompt, single_date_prompt, user_options_prompt
from vigorish.cli.menu_item import MenuItem
from vigorish.constants import EMOJIS, MENU_NUMBERS
from vigorish.enums import StatusReport as Report
from vigorish.status.report_status import (
    report_date_range_status,
    report_season_status,
    report_status_single_date,
)
from vigorish.util.result import Result

PROMPT_TEXT = "Choose the type of report you wish to generate from the options below:"


class StatusReport(MenuItem):
    def __init__(self, app):
        # TODO: New Status Report menu option - Single Game. User can provide either BB or BR ID
        # TODO: Another Status Report option - Pitch Appearance. Simply call .display() method
        super().__init__(app)
        self.menu_item_text = "Status Reports"
        self.menu_item_emoji = EMOJIS.get("CHART")
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
        if result.failure:
            if "no report" in result.error:
                return Result.Ok(self.exit_menu)
            return result
        report_viewer = result.value
        report_viewer.launch()
        return Result.Ok(self.exit_menu)

    def report_options_prompt(self):
        choices = {
            f"{MENU_NUMBERS.get(1)}  Season": "SEASON",
            f"{MENU_NUMBERS.get(2)}  Single Date": "SINGLE_DATE",
            f"{MENU_NUMBERS.get(3)}  Date Range": "DATE_RANGE",
            f"{EMOJIS.get('BACK')} Return to Main Menu": None,
        }
        return user_options_prompt(choices, PROMPT_TEXT)

    def get_season_report_type_from_user(self):
        choice_text1 = f"{MENU_NUMBERS.get(1)}  Season Summary"
        choice_text2 = f"{MENU_NUMBERS.get(2)}  Dates Missing Data (Summary)"
        choice_text3 = f"{MENU_NUMBERS.get(3)}  All Dates In Season (Summary)"
        choice_text4 = f"{MENU_NUMBERS.get(4)}  Dates Missing Data (Detail)"
        choice_text5 = f"{MENU_NUMBERS.get(5)}  All Dates In Season (Detail)"
        choice_text6 = f"{MENU_NUMBERS.get(6)}  All Dates In Season + Missing PitchFx IDs (Detail)"
        choices = {
            choice_text1: Report.SEASON_SUMMARY,
            choice_text2: Report.DATE_SUMMARY_MISSING_DATA,
            choice_text3: Report.DATE_SUMMARY_ALL_DATES,
            choice_text4: Report.DATE_DETAIL_MISSING_DATA,
            choice_text5: Report.DATE_DETAIL_ALL_DATES,
            choice_text6: Report.DATE_DETAIL_MISSING_PITCHFX,
            f"{EMOJIS.get('BACK')} Return to Previous Menu": None,
        }
        return user_options_prompt(choices, PROMPT_TEXT)

    def get_single_date_report_type_from_user(self):
        choice_text1 = f"{MENU_NUMBERS.get(1)}  Detail Report"
        choice_text2 = f"{MENU_NUMBERS.get(2)}  Detail Report + Missing PitchFx IDs"
        choice_text3 = f"{MENU_NUMBERS.get(3)}  Detail Report + Missing PitchFx IDs + Game Status"
        choices = {
            choice_text1: Report.DATE_DETAIL_ALL_DATES,
            choice_text2: Report.DATE_DETAIL_MISSING_PITCHFX,
            choice_text3: Report.SINGLE_DATE_WITH_GAME_STATUS,
            f"{EMOJIS.get('BACK')} Return to Previous Menu": None,
        }
        return user_options_prompt(choices, PROMPT_TEXT)

    def get_date_range_report_type_from_user(self):
        choice_text1 = f"{MENU_NUMBERS.get(1)}  Dates Missing Data (Summary)"
        choice_text2 = f"{MENU_NUMBERS.get(2)}  All Dates In Range (Summary)"
        choice_text3 = f"{MENU_NUMBERS.get(3)}  Dates Missing Data (Detail)"
        choice_text4 = f"{MENU_NUMBERS.get(4)}  All Dates In Range (Detail)"
        choice_text5 = f"{MENU_NUMBERS.get(5)}  All Dates In Range + Missing PitchFx IDs (Detail)"
        choices = {
            choice_text1: Report.DATE_SUMMARY_MISSING_DATA,
            choice_text2: Report.DATE_SUMMARY_ALL_DATES,
            choice_text3: Report.DATE_DETAIL_MISSING_DATA,
            choice_text4: Report.DATE_DETAIL_ALL_DATES,
            choice_text5: Report.DATE_DETAIL_MISSING_PITCHFX,
            f"{EMOJIS.get('BACK')} Return to Previous Menu": None,
        }
        return user_options_prompt(choices, PROMPT_TEXT)

    def season_status_report(self):
        result = self.get_mlb_season_from_user()
        if result.failure:
            return result
        year = result.value
        result = self.get_season_report_type_from_user()
        if result.failure:
            return result
        report_type = result.value
        subprocess.run(["clear"])
        return report_season_status(self.db_session, year, report_type)

    def single_date_report(self):
        game_date = single_date_prompt("Report status for date:")
        result = self.get_single_date_report_type_from_user()
        if result.failure:
            return result
        report_type = result.value
        subprocess.run(["clear"])
        return report_status_single_date(self.db_session, game_date, report_type)

    def date_range_report(self):
        start_date = single_date_prompt("Report status start date: ")
        end_date = single_date_prompt("Report status end date: ")
        result = self.get_date_range_report_type_from_user()
        if result.failure:
            return result
        report_type = result.value
        subprocess.run(["clear"])
        return report_date_range_status(self.db_session, start_date, end_date, report_type)

    def get_mlb_season_from_user(self):
        result = season_prompt(self.db_session, "Select the MLB Season to report:")
        if result.failure:
            return result
        season = result.value
        return Result.Ok(season.year)
