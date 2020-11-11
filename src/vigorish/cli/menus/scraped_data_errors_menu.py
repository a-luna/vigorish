"""Menu that allows the user to view all jobs grouped by status."""
import subprocess

from vigorish.cli.components import user_options_prompt
from vigorish.cli.menu import Menu
from vigorish.cli.menu_items.investigate_data_failures import InvestigateScrapedDataFailures
from vigorish.cli.menu_items.investigate_invalid_pfx import InvestigateInvalidPitchFx
from vigorish.cli.menu_items.investigate_pitchfx_errors import InvestigatePitchFxErrors
from vigorish.cli.menu_items.return_to_parent import ReturnToParent
from vigorish.constants import EMOJI_DICT, MENU_NUMBERS
from vigorish.util.result import Result


class ScrapedDataErrorsMenu(Menu):
    def __init__(self, app, audit_report):
        super().__init__(app)
        self.audit_report = audit_report
        self.menu_text = "Select the type of error to investigate:"
        self.menu_item_text = "Investigate Failures"
        self.menu_item_emoji = EMOJI_DICT.get("FLASHLIGHT")

    def launch(self):
        exit_menu = False
        while not exit_menu:
            subprocess.run(["clear"])
            result = self.audit_report_season_prompt(self.audit_report)
            subprocess.run(["clear"])
            if result.failure:
                return Result.Ok(exit_menu)
            self.year = result.value
            self.season_report = self.audit_report[self.year]
            self.populate_menu_items()
            result = self.prompt_user_for_menu_selection()
            if result.failure:
                return result
            exit_menu = result.value
            self.menu_items = []
        return Result.Ok(exit_menu)

    def populate_menu_items(self):
        self.menu_items.clear()
        failed_gids = self.season_report.get("failed", [])
        if failed_gids:
            self.menu_items.append(InvestigateScrapedDataFailures(self.app, self.year, failed_gids))
        pfx_error_gids = self.season_report.get("pfx_error", [])
        if pfx_error_gids:
            self.menu_items.append(InvestigatePitchFxErrors(self.app, self.year, pfx_error_gids))
        invalid_pfx_gids = self.season_report.get("invalid_pfx", [])
        if invalid_pfx_gids:
            self.menu_items.append(InvestigateInvalidPitchFx(self.app, self.year, invalid_pfx_gids))
        self.menu_items.append(ReturnToParent(self.app, "Return to Main Menu"))

    def audit_report_season_prompt(self, audit_report):
        prompt = "Select an MLB season from the list below:"
        years_with_errors = [
            year
            for year in audit_report.keys()
            if audit_report[year].get("invalid_pfx", [])
            or audit_report[year].get("pfx_error", [])
            or audit_report[year].get("failed", [])
        ]
        choices = {
            f"{MENU_NUMBERS.get(num)}  {year}": year
            for num, year in enumerate(years_with_errors, start=1)
        }
        choices[f"{EMOJI_DICT.get('BACK')} Return to Previous Menu"] = None
        return user_options_prompt(choices, prompt)
