"""Update bbref_player_id_map.json file."""
import subprocess

from getch import pause
from halo import Halo

from vigorish.cli.components import (
    get_random_cli_color,
    get_random_dots_spinner,
    print_heading,
)
from vigorish.cli.components.viewers import DisplayPage, PageViewer, DictListTableViewer
from vigorish.cli.menu_item import MenuItem
from vigorish.constants import EMOJIS
from vigorish.status.update_player_id_team_maps import update_player_id_map, update_player_team_map
from vigorish.tasks import UpdatePlayerIdMapTask, UpdatePlayerTeamMapTask
from vigorish.util.result import Result


class UpdatePlayerIdMap(MenuItem):
    def __init__(self, app):
        super().__init__(app)
        self.id_map_task = UpdatePlayerIdMapTask(self.app)
        self.team_map_task = UpdatePlayerTeamMapTask(self.app)
        self.menu_item_text = " Update Player ID/Team Map"
        self.menu_item_emoji = EMOJIS.get("TABBED_FILES")
        self.menu_heading = self._menu_item_text

    def launch(self, no_prompts=False):
        subprocess.run(["clear"])
        if not no_prompts and not self.prompt_user_run_task():
            return Result.Ok(False)
        result = self.update_player_data(self.id_map_task, "Player-ID map", no_prompts)
        if result.failure:
            return result
        updated_player_ids = result.value
        if updated_player_ids:
            update_player_id_map(self.app, updated_player_ids)
        result = self.update_player_data(self.team_map_task, "Player-Team map", no_prompts)
        if result.failure:
            return result
        updated_player_teams = result.value
        if updated_player_teams:
            update_player_team_map(self.app, updated_player_teams)
        return Result.Ok(False)

    def update_player_data(self, task, data_set, no_prompts):
        subprocess.run(["clear"])
        print_heading(f"Update {data_set}", fg="bright_yellow")
        spinner = Halo(spinner=get_random_dots_spinner(), color=get_random_cli_color())
        spinner.text = "Updating player data..."
        spinner.start()
        result = task.execute()
        if result.failure:
            spinner.stop()
            return result
        spinner.succeed(f"{data_set} was successfully updated!")
        if no_prompts:
            return Result.Ok()
        updated_players = result.value or []
        if not updated_players:
            pause(message="Press any key to continue...")
            return Result.Ok(updated_players)
        heading = f"Updated {data_set}: Results"
        message = f"{len(updated_players)} changes total:"
        table_viewer = DictListTableViewer(
            dict_list=updated_players,
            prompt="Press Enter to continue",
            confirm_only=True,
            table_color="bright_yellow",
            heading=heading,
            heading_color="bright_yellow",
            message=message,
            message_color="blue",
        )
        table_viewer.launch()
        return Result.Ok(updated_players)

    def prompt_user_run_task(self):
        subprocess.run(["clear"])
        heading = "Update Player-ID/Player-Team Maps"
        prompt = "Would you like to check for new player data?"
        pages = [DisplayPage(page, heading) for page in self.get_task_description_pages()]
        page_viewer = PageViewer(pages, prompt=prompt, text_color="bright_yellow", heading_color="bright_yellow")
        return page_viewer.launch()

    def get_task_description_pages(self):
        return [
            [
                (
                    "This task updates the bbref_player_id_map.csv and bbref_player_team_map.csv files which are "
                    "located in the src/vigorish/setup/csv folder. The data in these files is used to populate the "
                    "Assoc_Player_Team, Player, PlayerId, and Team tables when the database is initially created.\n"
                ),
                (
                    "However, players who make their MLB debut during the current season will not be included in "
                    "these files, causing any attempt to parse boxscore or PitchFX data that refers to them to fail."
                ),
            ],
            [
                (
                    "Thankfully, the necessary player data can be obtained at approximately 1:00 AM Pacific/4:00 AM "
                    "Eastern/8:00 AM UTC after all games have finished (usually 3-4 hours after the final game is "
                    "complete).\n"
                ),
                (
                    "The time needed to complete this process varies with the speed of your internet connection, but "
                    "a typical update takes less than a minute to complete."
                ),
            ],
        ]
