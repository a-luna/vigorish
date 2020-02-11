"""Menu that allows the user to view and modify all settings in vig.config.json."""
from typing import Mapping, Union, List

from vigorish.cli.menu import Menu
from vigorish.cli.menu_items.return_to_parent import ReturnToParentMenuItem
from vigorish.config import Config
from vigorish.constants import CONFIG_EMOJI_DICT
from vigorish.util.result import Result


class ChangeNumericSettingMenu(Menu):
    def __init__(self, config: Config) -> None:
        self.menu_text = config.menu_text
        self.menu_item_text = config.setting_name_title
        self.menu_item_emoji = CONFIG_EMOJI_DICT.get(config.setting_name, "")
        self.current_setting = config
        self.exit_menu = False

    def launch(self) -> Result:
        self._populate_menu()
        return super().launch()

    def _populate_menu(self) -> None:
        self.menu_items.clear()
        self.menu_items.append(ReturnToParentMenuItem("Return to main menu"))
