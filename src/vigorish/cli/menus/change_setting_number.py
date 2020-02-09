"""Menu that allows the user to view and modify all settings in vig.config.json."""
from typing import Mapping, Union, List

from vigorish.cli.menu import Menu
from vigorish.cli.menu_items.return_to_parent import ReturnToParentMenuItem
from vigorish.config import Config
from vigorish.constants import EMOJI_DICT
from vigorish.util.result import Result


class ChangeNumericSettingMenu(Menu):
    def __init__(
        self, name: str, current_setting: List[Mapping[str, Union[None, int, bool]]],
    ) -> None:
        self.menu_text = current_setting.get("DESCRIPTION", "")
        self.menu_item_text = name
        self.menu_item_emoji = EMOJI_DICT.get("DIAMOND", "")
        self.same_for_all_data_sets = current_setting.get("SAME_SETTING_FOR_ALL_DATA_SETS", True)
        self.exit_menu = False

    def launch(self) -> Result:
        self._populate_menu()
        return super().launch()

    def _populate_menu(self) -> None:
        self.menu_items.clear()
        self.menu_items.append(ReturnToParentMenuItem("Return to main menu"))
