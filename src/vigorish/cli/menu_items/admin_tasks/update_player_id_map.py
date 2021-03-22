"""Update bbref_player_id_map.json file."""
import subprocess

from getch import pause
from halo import Halo

from vigorish.cli.components.util import (
    get_random_cli_color,
    get_random_dots_spinner,
    print_heading,
)
from vigorish.cli.components.viewers import DictListTableViewer
from vigorish.cli.menu_item import MenuItem
from vigorish.constants import EMOJIS
from vigorish.tasks import UpdatePlayerIdMapTask
from vigorish.util.result import Result


class UpdatePlayerIdMap(MenuItem):
    def __init__(self, app):
        super().__init__(app)
        self.id_map_task = UpdatePlayerIdMapTask(self.app)
        self.menu_item_text = " Update Player ID Map"
        self.menu_item_emoji = EMOJIS.get("TABBED_FILES")

    def launch(self):
        subprocess.run(["clear"])
        print_heading("Update Player ID Map", fg="bright_yellow")
        spinner = Halo(spinner=get_random_dots_spinner(), color=get_random_cli_color())
        spinner.text = "Checking BBRef player data..."
        spinner.start()
        result = self.id_map_task.execute()
        if result.failure:
            spinner.stop()
            return result
        spinner.succeed("Player ID map was successfully updated!")
        new_player_ids = result.value or []
        if not new_player_ids:
            pause(message="Press any key to continue...")
            return Result.Ok(new_player_ids)
        heading = "Update Player ID Map: Results"
        message = f"{len(new_player_ids)} new players were added to the ID map\n"
        table_viewer = DictListTableViewer(
            dict_list=new_player_ids,
            prompt="Press Enter to return to previous menu",
            confirm_only=True,
            table_color="bright_yellow",
            heading=heading,
            heading_color="bright_yellow",
            message=message,
            message_color="blue",
        )
        table_viewer.launch()
        return Result.Ok(new_player_ids)
