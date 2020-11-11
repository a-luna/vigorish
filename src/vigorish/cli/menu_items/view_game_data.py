"""View all scraped data for a single game."""
import subprocess

from halo import Halo

from vigorish.cli.components.prompts import user_options_prompt
from vigorish.cli.components.util import (
    get_random_cli_color,
    get_random_dots_spinner,
    print_heading,
    print_message,
)
from vigorish.cli.menu_item import MenuItem
from vigorish.constants import EMOJI_DICT, MENU_NUMBERS
from vigorish.data.all_game_data import AllGameData
from vigorish.util.result import Result


class ViewGameData(MenuItem):
    def __init__(self, app, bbref_game_id):
        super().__init__(app)
        self.bbref_game_id = bbref_game_id
        self.menu_item_text = bbref_game_id
        self.menu_item_emoji = EMOJI_DICT.get("NEWSPAPER")

    @property
    def away_team_id(self):
        return self.game_data.away_team_id

    @property
    def home_team_id(self):
        return self.game_data.home_team_id

    def launch(self):
        subprocess.run(["clear"])
        self.load_boxscore_data()
        while True:
            self.print_matchup_and_linescore()
            result = self.select_data_prompt()
            if result.failure:
                break
            (team_id, menu_option) = result.value
            if menu_option == "META_INFO":
                table_viewer = self.game_data.view_game_meta_info()
                table_viewer.launch()
                continue
            if menu_option == "ALL":
                innings_viewer = self.game_data.view_at_bats_by_inning().value
                result = self.view_at_bats_by_inning(innings_viewer)
                if result.failure:
                    break
                continue
            result = self.view_boxscore(team_id, menu_option)
            if result.failure:
                break
        return Result.Ok()

    def load_boxscore_data(self):
        spinner = Halo(spinner=get_random_dots_spinner(), color=get_random_cli_color())
        spinner.text = f"Loading data for {self.bbref_game_id}..."
        spinner.start()
        self.game_data = AllGameData(self.app, self.bbref_game_id)
        self.game_data.bat_boxscore[self.away_team_id]
        self.game_data.pitch_boxscore[self.away_team_id]
        self.game_data.bat_boxscore[self.home_team_id]
        self.game_data.pitch_boxscore[self.home_team_id]
        spinner.stop()

    def print_matchup_and_linescore(self):
        subprocess.run(["clear"])
        matchup = self.game_data.get_matchup_details()
        linescore = self.game_data.get_linescore()
        print_heading(f"Scraped Data Viewer for Game ID: {self.bbref_game_id}", fg="bright_yellow")
        print_message(matchup, fg="bright_cyan", bold=True, wrap=False)
        print_message(linescore, fg="bright_cyan", wrap=False)

    def select_data_prompt(self):
        prompt = (
            f"\nData for {self.bbref_game_id} can be viewed in several different ways, please choose an "
            "option from the list below:"
        )
        choices = {
            f"{MENU_NUMBERS.get(1)}  {self.bat_stats_text(False)}": (self.away_team_id, "BAT"),
            f"{MENU_NUMBERS.get(2)}  {self.pitch_stats_text(False)}": (self.away_team_id, "PITCH"),
            f"{MENU_NUMBERS.get(3)}  {self.bat_stats_text(True)}": (self.home_team_id, "BAT"),
            f"{MENU_NUMBERS.get(4)}  {self.pitch_stats_text(True)}": (self.home_team_id, "PITCH"),
            f"{MENU_NUMBERS.get(5)}  All At Bats By Inning": (None, "ALL"),
            f"{MENU_NUMBERS.get(6)}  Game Meta Information": (None, "META_INFO"),
            f"{EMOJI_DICT.get('BACK')} Return to Previous Menu": None,
        }
        return user_options_prompt(choices, prompt, clear_screen=False)

    def pitch_stats_text(self, is_home_team):
        team_id = self.home_team_id if is_home_team else self.away_team_id
        return f"{team_id} Pitcher Boxscore"

    def bat_stats_text(self, is_home_team):
        team_id = self.home_team_id if is_home_team else self.away_team_id
        return f"{team_id} Batter Boxscore"

    def view_at_bats_by_inning(self, innings_viewer):
        subprocess.run(["clear"])
        while True:
            result = self.innings_viewer_prompt(innings_viewer)
            if result.failure:
                break
            table_viewer = result.value
            table_viewer.launch()
        return Result.Ok()

    def innings_viewer_prompt(self, innings_viewer):
        choices = {f"{EMOJI_DICT.get('ASTERISK')}  All Innings": innings_viewer["ALL"]}
        for inning, table_viewer in innings_viewer.items():
            if inning == "ALL":
                continue
            choices[f"{MENU_NUMBERS.get(int(inning[-2:]), inning[-2:])}  {inning}"] = table_viewer
        choices[f"{EMOJI_DICT.get('BACK')} Return to Previous Menu"] = None
        prompt = "Select an inning to view:"
        return user_options_prompt(choices, prompt)

    def view_boxscore(self, team_id, player_type):
        boxscore = self.get_boxscore(team_id, player_type)
        while True:
            subprocess.run(["clear"])
            result = self.select_player_prompt(player_type, boxscore)
            if result.failure:
                return Result.Ok()
            mlb_id = result.value
            self.view_at_bats_for_player(player_type, mlb_id)

    def get_boxscore(self, team_id, player_type):
        boxscore_dict = {
            "PITCH": self.game_data.pitch_boxscore,
            "BAT": self.game_data.bat_boxscore,
        }
        return boxscore_dict[player_type][team_id]

    def select_player_prompt(self, player_type, boxscore):
        player_prompt_dict = {
            "PITCH": self.select_pitcher_prompt,
            "BAT": self.select_batter_prompt,
        }
        return player_prompt_dict[player_type](boxscore)

    def view_at_bats_for_player(self, player_type, mlb_id):
        view_at_bats_dict = {
            "PITCH": self.view_at_bats_for_pitcher,
            "BAT": self.view_at_bats_for_batter,
        }
        return view_at_bats_dict[player_type](mlb_id)

    def select_pitcher_prompt(self, pitch_boxscore):
        max_name_length = self.get_name_max_length(pitch_boxscore)
        choices = {
            f"{self.select_pitcher_text(num, box, max_name_length)}": box["mlb_id"]
            for num, box in enumerate(pitch_boxscore.values(), start=1)
        }
        choices[f"{EMOJI_DICT.get('BACK')} Return to Previous Menu"] = None
        prompt = (
            "All pitchers that apppeared in the game are listed below. Select a player and "
            "press ENTER to access information for each player's outing and career statistics:"
        )
        print_heading(f"Scraped Data Viewer for Game ID: {self.bbref_game_id}", fg="bright_yellow")
        return user_options_prompt(choices, prompt, auto_scroll=False, clear_screen=False)

    def get_name_max_length(self, boxscore):
        return max(len(box["name"]) for box in boxscore.values())

    def select_pitcher_text(self, num, box, max_name_length):
        fill1_count = 2 if num < 10 else 1
        fill2_count = max_name_length - len(box["name"])
        return (
            f"{MENU_NUMBERS.get(num, num)}{' '*fill1_count}[{box['pitch_app_type']}] "
            f"{box['name']}{'.'*fill2_count}..: "
            f"{box['game_results']}"
        )

    def view_at_bats_for_pitcher(self, mlb_id):
        subprocess.run(["clear"])
        innings_viewer = self.game_data.view_valid_at_bats_for_pitcher(mlb_id).value
        return self.view_at_bats_by_inning(innings_viewer)

    def select_batter_prompt(self, bat_boxscore):
        max_name_length = self.get_name_max_length(bat_boxscore)
        choices = {
            f"{self.select_batter_text(order_num, box, max_name_length)}": box["mlb_id"]
            for order_num, box in bat_boxscore.items()
        }
        choices[f"{EMOJI_DICT.get('BACK')} Return to Previous Menu"] = None
        prompt = (
            "Starting lineup and all substitutes who made at least one plate appearance are "
            "listed below. Select a player and press ENTER to view details of each at bat:"
        )
        print_heading(f"Scraped Data Viewer for Game ID: {self.bbref_game_id}", fg="bright_yellow")
        return user_options_prompt(choices, prompt, auto_scroll=False, clear_screen=False)

    def select_batter_text(self, lineup_slot, box, max_name_len):
        fill_count = max_name_len - len(box["name"])
        def_pos_fill = "" if len(str(box["def_position"])) >= 2 else " "
        return (
            f"{self.get_emoji_for_lineup_slot(lineup_slot)} "
            f"[{str(box['def_position'])}]{def_pos_fill} "
            f"{box['name']}{'.'*fill_count}..: "
            f"{box['at_bats']} {box['bat_stats']}"
        )

    def get_emoji_for_lineup_slot(self, lineup_slot):
        emoji_num = MENU_NUMBERS.get(lineup_slot)
        if emoji_num:
            return f"{emoji_num} "
        return EMOJI_DICT.get("CAP")

    def view_at_bats_for_batter(self, mlb_id):
        subprocess.run(["clear"])
        table_viewer = self.game_data.view_valid_at_bats_for_batter(mlb_id).value
        table_viewer.launch()
        return Result.Ok()
