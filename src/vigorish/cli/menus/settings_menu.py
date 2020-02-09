"""Menu that allows the user to view and modify all settings in vig.config.json."""
from vigorish.cli.menu import Menu
from vigorish.cli.menus.change_setting_enum import ChangeEnumSettingMenu
from vigorish.cli.menus.change_setting_number import ChangeNumericSettingMenu
from vigorish.cli.menus.change_setting_string import ChangeStringSettingMenu
from vigorish.cli.menu_items.return_to_parent import ReturnToParentMenuItem
from vigorish.config import Config
from vigorish.constants import EMOJI_DICT
from vigorish.util.list_helpers import report_dict
from vigorish.util.result import Result


class SettingsMenu(Menu):
    def __init__(self, menu_item_text: str, config: Config) -> None:
        self.config = config
        self.menu_text = "You can modify any setting in the list below:"
        self.menu_item_text = menu_item_text
        self.menu_item_emoji = EMOJI_DICT.get("WRENCH", "")
        self.exit_menu = False

    def launch(self) -> Result:
        self._populate_menu()
        return super().launch()

    def _populate_menu(self) -> None:
        self.menu_items.clear()
        for name, current_setting in self.config.get_all_settings().items():
            data_type = current_setting.get("DATA_TYPE")
            if not data_type:
                options_str = report_dict(dict=current_setting, title=name)
                error = f'Config setting "{name}" does not have a data type:\n{options_str}'
                return Result.Fail(error)
            if data_type == "str":
                self.menu_items.append(ChangeStringSettingMenu(name, current_setting))
            if data_type == "Enum":
                enum_options = self.config.get_possible_values(current_setting.get("ENUM_NAME"))
                self.menu_items.append(ChangeEnumSettingMenu(name, current_setting, enum_options))
            if data_type == "Object":
                self.menu_items.append(ChangeNumericSettingMenu(name, current_setting))

        self.menu_items.append(ReturnToParentMenuItem("Return to main menu"))
