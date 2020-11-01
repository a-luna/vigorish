"""Menu that allows the user to view data for games where all data has been scraped."""
import subprocess

from bullet import Input
from getch import pause

from vigorish.cli.components.prompts import select_game_prompt, user_options_prompt
from vigorish.cli.components.util import print_heading, print_error
from vigorish.cli.menu_item import MenuItem
from vigorish.cli.menu_items.view_game_data import ViewGameData
from vigorish.config.database import Team, GameScrapeStatus
from vigorish.constants import EMOJI_DICT, MENU_NUMBERS
from vigorish.util.result import Result
from vigorish.util.string_helpers import validate_bbref_game_id


class ScrapedGameDataMenu(MenuItem):
    def __init__(self, app, audit_report):
        super().__init__(app)
        self.audit_report = audit_report
        self.menu_item_text = "View Scraped Game Data"
        self.menu_item_emoji = EMOJI_DICT.get("MICROSCOPE", "")
        self.exit_menu = False

    def launch(self):
        while True:
            result = self.prompt_user_choose_method_to_select_game()
            if result.failure:
                break
            user_selection = result.value
            while True:
                result = self.prompt_user_for_game_id(user_selection)
                if result.failure:
                    break
                selected_game_id = result.value
                view_game_data = ViewGameData(self.app, selected_game_id)
                view_game_data.launch()
        return Result.Ok()

    def prompt_user_choose_method_to_select_game(self):
        subprocess.run(["clear"])
        print_heading("Scraped Data Viewer - Please choose an option below", fg="bright_yellow")
        prompt = "How would you like to locate the game you wish to view?"
        choices = {
            f"{MENU_NUMBERS.get(1)}  Enter Game ID Manually": "MANUAL",
            f"{MENU_NUMBERS.get(2)}  Browse by Team": "TEAM",
            f"{MENU_NUMBERS.get(3)}  Browse by Season": "SEASON",
            f"{EMOJI_DICT.get('BACK')} Return to Previous Menu": None,
        }
        return user_options_prompt(choices, prompt, clear_screen=False)

    def prompt_user_for_game_id(self, user_selection):
        if user_selection == "MANUAL":
            return self.enter_game_id_prompt()
        while True:
            result = self.select_season_prompt()
            if result.failure:
                return result
            year = result.value
            game_ids = self.audit_report[year]["successful"]
            if user_selection == "SEASON":
                return select_game_prompt(game_ids)
            if user_selection == "TEAM":
                while True:
                    result = self.select_team_prompt(year, game_ids)
                    if result.failure:
                        break
                    team_game_ids = result.value
                    while True:
                        result = select_game_prompt(team_game_ids)
                        if result.failure:
                            break
                        return result

    def enter_game_id_prompt(self):
        while True:
            subprocess.run(["clear"])
            print_heading("Scraped Data Viewer - Enter a BBRef Game ID", fg="bright_yellow")
            check_game_id = Input("Enter a BBRef Game ID: ")
            result = validate_bbref_game_id(check_game_id)
            if result.success:
                return result.value["game_id"]
            error = f"\n'{check_game_id}' is NOT a valid BBRef Game ID, please try again."
            print_error(error)
            pause(message="\nPress any key to continue...")

    def select_season_prompt(self):
        subprocess.run(["clear"])
        print_heading("Scraped Data Viewer - Select MLB Season", fg="bright_yellow")
        prompt = "Select an MLB season from the list below:"
        choices = {
            f"{MENU_NUMBERS.get(num, str(num))}  {year}": year
            for num, (year, results) in enumerate(self.audit_report.items(), start=1)
            if results["successful"]
        }
        choices[f"{EMOJI_DICT.get('BACK')} Return to Previous Menu"] = None
        return user_options_prompt(choices, prompt, clear_screen=False)

    def select_team_prompt(self, year, game_ids):
        team_choices_dict = {
            t.team_id_br: t.name for t in Team.get_all_teams_for_season(self.db_session, year=year)
        }
        subprocess.run(["clear"])
        print_heading(f"Scraped Data Viewer - Select Team (MLB Season: {year})", fg="bright_yellow")
        prompt = "Select a team from the list below::"
        choices = {
            f"{EMOJI_DICT.get('BLUE_DIAMOND')}  {name} ({team_id})": team_id
            for team_id, name in team_choices_dict.items()
        }
        choices[f"{EMOJI_DICT.get('BACK')} Return to Previous Menu"] = None
        result = user_options_prompt(choices, prompt, clear_screen=False)
        if result.failure:
            return result
        selected_team_id = result.value
        return self.get_game_ids_where_team_played(selected_team_id, year, game_ids)

    def get_game_ids_where_team_played(self, team_id, year, all_game_ids):
        all_game_ids_for_team = GameScrapeStatus.get_all_bbref_game_ids_for_team(
            self.db_session, team_id, year
        )
        scraped_game_ids_where_team_played = [
            game_id for game_id in all_game_ids_for_team if game_id in all_game_ids
        ]
        return Result.Ok(scraped_game_ids_where_team_played)
