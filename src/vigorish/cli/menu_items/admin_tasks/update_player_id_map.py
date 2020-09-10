"""Update bbref_player_id_map.json file."""
import subprocess
from getch import pause
from halo import Halo
from tabulate import tabulate

from vigorish.cli.menu_item import MenuItem
from vigorish.cli.util import get_random_cli_color, get_random_dots_spinner, print_message
from vigorish.constants import EMOJI_DICT
from vigorish.tasks.update_player_maps import UpdatePlayerIdMap as UpdatePlayerIdMapTask
from vigorish.util.result import Result


class UpdatePlayerIdMap(MenuItem):
    def __init__(self, app):
        super().__init__(app)
        self.update_player_id_map = UpdatePlayerIdMapTask(self.app)
        self.menu_item_text = " Update Player ID Map"
        self.menu_item_emoji = EMOJI_DICT.get("TABBED_FILES")

    def launch(self):
        subprocess.run(["clear"])
        spinner = Halo(spinner=get_random_dots_spinner(), color=get_random_cli_color())
        spinner.text = "Updating Player ID map..."
        spinner.start()
        result = self.update_player_id_map.execute()
        if result.failure:
            spinner.stop()
            return result
        spinner.succeed("Player ID map was successfully updated!")
        new_player_ids = result.value if result.value else []
        if new_player_ids:
            summary = f"\n{len(new_player_ids)} new players were added to the ID map:\n"
            print_message(summary, fg="bright_yellow", bold=True)
            new_player_ids_table = tabulate(new_player_ids, headers="keys")
            print_message(new_player_ids_table, wrap=False, fg="bright_yellow")
            print()
        pause(message="Press any key to continue...")
        return Result.Ok(new_player_ids)
