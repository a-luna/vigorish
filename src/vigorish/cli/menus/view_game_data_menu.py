"""Menu that allows the user to view data for games where all data has been scraped."""
import subprocess

from bullet import Input
from getch import pause

from vigorish.cli.components.prompts import select_game_prompt, user_options_prompt
from vigorish.cli.components.util import print_error, print_heading
from vigorish.cli.menu_item import MenuItem
from vigorish.cli.menu_items.view_game_data import ViewGameData
from vigorish.config.database import GameScrapeStatus, Team
from vigorish.constants import EMOJI_DICT, MENU_NUMBERS
from vigorish.util.result import Result
from vigorish.util.string_helpers import validate_bbref_game_id


class ViewGameDataMenu(MenuItem):
    def __init__(self, app, audit_report):
        super().__init__(app)
        self.audit_report = audit_report
        self.menu_item_text = "View Scraped Game Data"
        self.menu_item_emoji = EMOJI_DICT.get("MICROSCOPE", "")
        self.exit_menu = False
        self.menu_option = None
        self.mlb_season = None
        self.team_id = None
        self.game_id = None

    def launch(self):
        while True:
            result = self.prompt_user_choose_method_to_select_game()
            if result.failure:
                break
            self.menu_option = result.value
            while True:
                if self.menu_option == "MANUAL":
                    result = self.enter_game_id()
                if self.menu_option == "SEASON":
                    result = self.select_season_game_id()
                if self.menu_option == "TEAM":
                    result = self.select_team_game_id()
                if result.failure:
                    break
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

    def enter_game_id(self):
        subprocess.run(["clear"])
        print_heading("Scraped Data Viewer - Enter a BBRef Game ID", fg="bright_yellow")
        check_game_id = Input("Enter a BBRef Game ID: ").launch()
        try:
            result = validate_bbref_game_id(check_game_id)
        except ValueError:
            error = f"\n'{check_game_id}' is NOT a valid BBRef Game ID, please try again."
            print_error(error)
            pause(message="Press any key to continue...")
            return Result.Fail("")
        self.game_id = result.value["game_id"]
        game_date = result.value["game_date"]
        all_valid_game_ids = self.audit_report[game_date.year]["successful"]
        if self.game_id not in all_valid_game_ids:
            error = f"\nRequirements to show data for {check_game_id} have not been met"
            print_error(error)
            pause(message="Press any key to continue...")
            return Result.Fail("")
        view_game_data = ViewGameData(self.app, self.game_id)
        view_game_data.launch()
        return Result.Fail("")

    def select_season_game_id(self):
        while True:
            result = self.select_season_prompt()
            if result.failure:
                return result
            self.mlb_season = result.value
            game_ids = self.audit_report[self.mlb_season]["successful"]
            while True:
                subprocess.run(["clear"])
                heading = f"Scraped Data Viewer - Select Game (MLB Season: {self.mlb_season})"
                print_heading(heading, fg="bright_yellow")
                result = select_game_prompt(game_ids, use_numbers=False, clear_screen=False)
                if result.failure:
                    break
                self.game_id = result.value
                view_game_data = ViewGameData(self.app, self.game_id)
                view_game_data.launch()

    def select_team_game_id(self):
        while True:
            result = self.select_season_prompt()
            if result.failure:
                return result
            self.mlb_season = result.value
            while True:
                result = self.select_team_prompt()
                if result.failure:
                    self.team_id = None
                    break
                game_ids = result.value
                while True:
                    subprocess.run(["clear"])
                    heading = (
                        "Scraped Data Viewer - Select Game "
                        f"(MLB Season: {self.mlb_season}, Team: {self.team_id})"
                    )
                    print_heading(heading, fg="bright_yellow")
                    result = select_game_prompt(game_ids, use_numbers=False, clear_screen=False)
                    if result.failure:
                        break
                    self.game_id = result.value
                    view_game_data = ViewGameData(self.app, self.game_id)
                    view_game_data.launch()

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

    def select_team_prompt(self):
        team_choices_dict = {
            t.team_id_br: t.name
            for t in Team.get_all_teams_for_season(self.db_session, year=self.mlb_season)
        }
        subprocess.run(["clear"])
        heading = f"Scraped Data Viewer - Select Team (MLB Season: {self.mlb_season})"
        print_heading(heading, fg="bright_yellow")
        prompt = "Select a team from the list below::"
        choices = {
            f"{EMOJI_DICT.get('BLUE_DIAMOND')}  {name} ({team_id})": team_id
            for team_id, name in team_choices_dict.items()
        }
        choices[f"{EMOJI_DICT.get('BACK')} Return to Previous Menu"] = None
        result = user_options_prompt(choices, prompt, clear_screen=False)
        if result.failure:
            return result
        self.team_id = result.value
        return self.get_team_game_ids()

    def get_team_game_ids(self):
        all_valid_game_ids = self.audit_report[self.mlb_season]["successful"]
        team_game_ids = GameScrapeStatus.get_all_bbref_game_ids_for_team(
            self.db_session, self.team_id, self.mlb_season
        )
        valid_team_game_ids = list(set(team_game_ids).intersection(set(all_valid_game_ids)))
        game_id_weights = self.get_game_id_weights(valid_team_game_ids)
        valid_team_game_ids.sort(key=lambda x: game_id_weights[x])
        return Result.Ok(valid_team_game_ids)

    def get_game_id_weights(self, game_ids):
        return {g: validate_bbref_game_id(g).value["game_date"] for g in game_ids}
