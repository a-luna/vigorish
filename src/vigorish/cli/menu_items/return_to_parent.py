"""Menu item that returns the user to the previous menu."""
from vigorish.cli.menu_item import MenuItem
from vigorish.constants import EMOJIS
from vigorish.util.result import Result


class ReturnToParent(MenuItem):
    def __init__(self, app, menu_item_text):
        super().__init__(app)
        self.menu_item_text = menu_item_text
        self.menu_item_emoji = EMOJIS.get("BACK", "")
        self.exit_menu = True

    def launch(self):
        return Result.Ok(self.exit_menu)
