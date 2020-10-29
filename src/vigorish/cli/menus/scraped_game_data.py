"""Menu that allows the user to view all jobs grouped by status."""
import subprocess

from vigorish.cli.components.prompts import select_game_prompt, user_options_prompt
from vigorish.cli.menu_item import MenuItem
from vigorish.cli.menu_items.view_game_data import ViewGameData
from vigorish.constants import EMOJI_DICT, MENU_NUMBERS
from vigorish.util.result import Result


class ScrapedGameDataMenu(MenuItem):
    def __init__(self, app, audit_report):
        super().__init__(app)
        self.audit_report = audit_report
        self.menu_item_text = "View Scraped Data for Game"
        self.menu_item_emoji = EMOJI_DICT.get("MICROSCOPE", "")
        self.exit_menu = False

    def launch(self):
        subprocess.run(["clear"])
        while True:
            result = self.select_season_prompt()
            if result.failure:
                break
            year = result.value
            game_ids = self.audit_report[year]["successful"]
            while True:
                result = select_game_prompt(game_ids)
                if result.failure:
                    break
                selected_game_id = result.value
                view_game_data = ViewGameData(self.app, selected_game_id)
                view_game_data.launch()
        return Result.Ok()

    def select_season_prompt(self):
        prompt = "Select an MLB season from the list below:"
        choices = {
            f"{MENU_NUMBERS.get(num, str(num))}  {year}": year
            for num, (year, results) in enumerate(self.audit_report.items(), start=1)
            if results["successful"]
        }
        choices[f"{EMOJI_DICT.get('BACK')} Return to Previous Menu"] = None
        return user_options_prompt(choices, prompt)
