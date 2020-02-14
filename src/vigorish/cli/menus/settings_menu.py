"""Menu that allows the user to view and modify all settings in vig.config.json."""
from vigorish.cli.menu import Menu
from vigorish.cli.menus.change_setting_enum import ChangeEnumSettingMenu
from vigorish.cli.menus.change_setting_number import ChangeNumericSettingMenu
from vigorish.cli.menus.change_setting_string import ChangeStringSettingMenu
from vigorish.cli.menu_items.return_to_parent import ReturnToParentMenuItem
from vigorish.config import ConfigFile
from vigorish.constants import EMOJI_DICT
from vigorish.enums import ConfigDataType
from vigorish.util.list_helpers import report_dict
from vigorish.util.result import Result


class SettingsMenu(Menu):
    def __init__(self, menu_item_text: str, config: ConfigFile) -> None:
        self.config = config
        self.menu_text = "You can modify any setting in the list below:"
        self.menu_item_text = menu_item_text
        self.menu_item_emoji = EMOJI_DICT.get("TOOLS", "")
        self.exit_menu = False
        self.menu_items = self._get_menu_items()

    def _get_menu_items(self) -> None:
        menu_items = []
        for config in self.config.all_settings:
            if not config.data_type:
                name = config.setting_name_title
                options_str = report_dict(dict=config, title=name)
                error = f'Config setting "{name}" does not have a data type:\n{options_str}'
                return Result.Fail(error)
            if config.data_type == ConfigDataType.STRING:
                menu_items.append(ChangeStringSettingMenu(config))
            if config.data_type == ConfigDataType.ENUM:
                menu_items.append(ChangeEnumSettingMenu(config))
            if config.data_type == ConfigDataType.NUMERIC:
                menu_items.append(ChangeNumericSettingMenu(config))
        menu_items.append(ReturnToParentMenuItem("Return to Main Menu"))
        return menu_items
