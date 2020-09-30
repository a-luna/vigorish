"""View all scraped data for a single game."""
import subprocess

from vigorish.cli.menu_item import MenuItem
from vigorish.cli.prompts import user_options_prompt
from vigorish.constants import EMOJI_DICT, MENU_NUMBERS
from vigorish.data.all_game_data import AllGameData
from vigorish.util.result import Result


class ViewGameData(MenuItem):
    def __init__(self, app, bbref_game_id):
        super().__init__(app)
        self.game_data = AllGameData(self.db_session, self.scraped_data, bbref_game_id)
        self.bbref_game_id = bbref_game_id
        self.menu_item_text = bbref_game_id
        self.menu_item_emoji = EMOJI_DICT.get("NEWSPAPER")

    @property
    def all_player_ids_with_bat_stats(self):
        return self.game_data.all_player_ids_with_bat_stats

    @property
    def all_player_ids_with_pitch_stats(self):
        return self.game_data.all_player_ids_with_pitch_stats

    @property
    def player_id_map(self):
        return self.game_data.player_id_map

    def launch(self):
        subprocess.run(["clear"])
        while True:
            result = self.select_data_prompt()
            if result.failure:
                break
            selected_data = result.value
            if selected_data == "AT_BATS_FOR_PITCHER":
                while True:
                    result = self.select_pitcher_prompt()
                    if result.failure:
                        break
                    mlb_id = result.value
                    self.view_at_bats_for_pitcher(mlb_id)
            if selected_data == "AT_BATS_FOR_BATTER":
                while True:
                    result = self.select_batter_prompt()
                    if result.failure:
                        break
                    mlb_id = result.value
                    self.view_at_bats_for_batter(mlb_id)
        return Result.Ok()

    def select_data_prompt(self):
        prompt = "Please select the data you wish to view:"
        choices = {
            f"{MENU_NUMBERS.get(1)}  All at bats in pitching appearance": "AT_BATS_FOR_PITCHER",
            f"{MENU_NUMBERS.get(2)}  All at bats for batter": "AT_BATS_FOR_BATTER",
            f"{EMOJI_DICT.get('BACK')} Return to Previous Menu": None,
        }
        return user_options_prompt(choices, prompt)

    def select_pitcher_prompt(self):
        choices = {
            f"{EMOJI_DICT.get('CAP')} {self.select_pitcher_text(mlb_id)}": mlb_id
            for mlb_id in self.all_player_ids_with_pitch_stats
        }
        choices[f"{EMOJI_DICT.get('BACK')} Return to Previous Menu"] = None
        prompt = "Select a player from the list below:"
        return user_options_prompt(choices, prompt)

    def select_pitcher_text(self, mlb_id):
        pitcher_name = self.player_id_map.get(mlb_id).mlb_name
        pitch_app_stats = self.game_data.parse_pitch_app_stats(mlb_id).value
        return f"{pitcher_name} ({pitch_app_stats})"

    def view_at_bats_for_pitcher(self, mlb_id):
        subprocess.run(["clear"])
        result = self.game_data.view_at_bats_for_pitcher(mlb_id)
        innings_viewer = result.value
        while True:
            result = self.innings_viewer_prompt(innings_viewer)
            if result.failure:
                break
            table_viewer = result.value
            table_viewer.launch()
        return Result.Ok()

    def innings_viewer_prompt(self, innings_viewer):
        choices = {
            f"{MENU_NUMBERS.get(int(inning[-2:]), inning[-2:])}  {inning}": table_viewer
            for inning, table_viewer in innings_viewer.items()
        }
        choices[f"{EMOJI_DICT.get('BACK')} Return to Previous Menu"] = None
        prompt = "Select an inning to view:"
        return user_options_prompt(choices, prompt)

    def select_batter_prompt(self):
        choices = {
            f"{EMOJI_DICT.get('BASEBALL')} {self.select_batter_text(mlb_id)}": mlb_id
            for mlb_id in self.all_player_ids_with_bat_stats
        }
        choices[f"{EMOJI_DICT.get('BACK')} Return to Previous Menu"] = None
        prompt = "Select a player from the list below:"
        return user_options_prompt(choices, prompt)

    def select_batter_text(self, mlb_id):
        batter_name = self.player_id_map.get(mlb_id).mlb_name
        batter_game_stats = self.game_data.parse_bat_stats_for_game(mlb_id).value
        return f"{batter_name} ({batter_game_stats})"

    def view_at_bats_for_batter(self, mlb_id):
        subprocess.run(["clear"])
        result = self.game_data.view_at_bats_for_batter(mlb_id)
        table_viewer = result.value
        table_viewer.launch()
        return Result.Ok()
