"""Menu that allows the user to view and modify all settings in vig.config.json."""
from vigorish.cli.menu import Menu
from vigorish.cli.menus.current_setting import CurrentSettingMenu
from vigorish.cli.menu_items.return_to_parent import ReturnToParentMenuItem
from vigorish.config.config_file import ConfigFile
from vigorish.constants import EMOJI_DICT
from vigorish.enums import ConfigDataType
from vigorish.util.list_helpers import report_dict
from vigorish.util.result import Result


class ConfigSettingsMenu(Menu):
    def __init__(self, menu_item_text: str, config_file: ConfigFile) -> None:
        self.config_file = config_file
        self.menu_text = "You can modify any setting in the list below:"
        self.menu_item_text = menu_item_text
        self.menu_item_emoji = EMOJI_DICT.get("TOOLS", "")
        self.exit_menu = False
        self.menu_items = self._get_menu_items()

    def _get_menu_items(self) -> None:
        menu_items = [
            CurrentSettingMenu(name, self.config_file)
            for name in self.config_file.all_settings.keys()
        ]
        menu_items.append(ReturnToParentMenuItem("Main Menu"))
        return menu_items
