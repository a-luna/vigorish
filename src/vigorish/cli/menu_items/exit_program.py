"""Menu item that is used to exit the CLI."""
from vigorish.cli.menu_item import MenuItem
from vigorish.constants import EMOJI_DICT
from vigorish.util.result import Result


class ExitProgram(MenuItem):
    def __init__(self, app):
        super().__init__(app)
        self.menu_item_text = "Exit"
        self.menu_item_emoji = EMOJI_DICT.get("EXIT", "")
        self.exit_menu = True

    def launch(self):
        return Result.Ok()
