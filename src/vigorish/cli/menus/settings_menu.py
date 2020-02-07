"""Menu that allows the user to view and modify all settings in vig.config.json."""
from vigorish.cli.menu import Menu
from vigorish.cli.menu_items.return_to_parent import ReturnToParentMenuItem
from vigorish.config.file import get_config_from_file
from vigorish.constants import EMOJI_DICT
from vigorish.util.result import Result


class SettingsMenu(Menu):
    def __init__(self, menu_item_text: str) -> None:
        self.menu_text = "You can modify any setting in the list below:"
        self.menu_item_text = menu_item_text
        self.menu_item_emoji = EMOJI_DICT.get("WRENCH", "")
        self.exit_menu = False

    def launch(self) -> Result:
        self._populate_menu()
        return super().launch()

    def _populate_menu(self) -> None:
        self.menu_items.clear()
        self.menu_items.append(ReturnToParentMenuItem("Return to main menu"))
