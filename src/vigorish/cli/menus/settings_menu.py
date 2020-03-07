"""Menu that allows the user to view and modify all settings in vig.config.json."""
from vigorish.cli.menu import Menu
from vigorish.cli.menus.current_setting import CurrentSettingMenu
from vigorish.cli.menu_items.return_to_parent import ReturnToParentMenuItem
from vigorish.config.types import ConfigFile
from vigorish.constants import EMOJI_DICT
from vigorish.enums import ConfigType
from vigorish.util.list_helpers import report_dict
from vigorish.util.result import Result


class ConfigSettingsMenu(Menu):
    def __init__(self, config_file: ConfigFile) -> None:
        self.config_file = config_file
        self.menu_text = "You can modify any setting in the list below:\n"
        self.menu_item_text = "Settings"
        self.menu_item_emoji = EMOJI_DICT.get("TOOLS", "")

    def populate_menu_items(self) -> None:
        self.menu_items = [
            CurrentSettingMenu(name, self.config_file)
            for name in self.config_file.all_settings.keys()
        ]
        self.menu_items.append(ReturnToParentMenuItem("Main Menu "))
        self.menu_items.insert(0, ReturnToParentMenuItem("Main Menu"))
