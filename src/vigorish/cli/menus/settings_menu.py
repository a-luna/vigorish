"""Menu that allows the user to view and modify all settings in vig.config.json."""
from vigorish.cli.menu import Menu
from vigorish.cli.menu_items.current_setting import CurrentSettingMenuItem
from vigorish.cli.menu_items.return_to_parent import ReturnToParentMenuItem
from vigorish.config.types import ConfigFile
from vigorish.constants import EMOJI_DICT


class ConfigSettingsMenu(Menu):
    def __init__(self, config: ConfigFile) -> None:
        self.config = config
        self.menu_text = "You can modify any setting in the list below:"
        self.menu_item_text = "Settings"
        self.menu_item_emoji = EMOJI_DICT.get("GEAR", "")

    def populate_menu_items(self) -> None:
        self.menu_items = [
            CurrentSettingMenuItem(name, self.config) for name in self.config.all_settings.keys()
        ]
        self.menu_items.append(ReturnToParentMenuItem("Main Menu "))
        self.menu_items.insert(0, ReturnToParentMenuItem("Main Menu"))
