from vigorish.cli.menu_item import MenuItem
from vigorish.constants import EMOJI_DICT
from vigorish.util.result import Result


class ReturnToParentMenuItem(MenuItem):
    def __init__(self, menu_item_text: str) -> None:
        self.menu_item_text = menu_item_text
        self.menu_item_emoji = EMOJI_DICT.get("BACK", "")
        self.exit_menu = True

    def launch(self) -> Result:
        return Result.Ok(self.exit_menu)
