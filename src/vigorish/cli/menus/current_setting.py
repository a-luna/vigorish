"""Menu that allows the user to view and modify all settings in vig.config.json."""
from vigorish.cli.menu import Menu
from vigorish.cli.menu_items.change_setting import ChangeSetttingMenuItem
from vigorish.cli.menu_items.return_to_parent import ReturnToParentMenuItem
from vigorish.config import Config, ConfigFile, ConfigDataType
from vigorish.constants import CONFIG_EMOJI_DICT
from vigorish.util.result import Result


class CurrentSettingMenu(Menu):
    def __init__(self, setting_name: str, config_file: ConfigFile) -> None:
        self.setting_name = setting_name
        self.config_file = config_file
        setting = self.config_file.all_settings.get(setting_name)
        self.menu_text = setting.menu_text
        self.menu_item_text = setting.setting_name_title
        self.menu_item_emoji = CONFIG_EMOJI_DICT.get(setting_name, "")
        self.data_type = setting.data_type
        self.exit_menu = False
        self.menu_items = self._get_menu_items()

    def launch(self) -> Result:
        exit_menu = False
        result: Result
        while not exit_menu:
            result = super().launch()
            if result.failure:
                exit_menu = True
            self.__update_menu_text()
            exit_menu = result.value
        return result

    def _get_menu_items(self) -> None:
        menu_items = []
        menu_items.append(ChangeSetttingMenuItem(self.setting_name, self.config_file))
        menu_items.append(ReturnToParentMenuItem("All Settings"))
        return menu_items

    def __update_menu_text(self):
        setting = self.config_file.all_settings.get(self.setting_name)
        self.menu_text = setting.menu_text
