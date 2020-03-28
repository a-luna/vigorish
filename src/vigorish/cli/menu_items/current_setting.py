"""Menu that allows the user to view and modify all settings in vig.config.json."""
import subprocess

from vigorish.cli.menu_item import MenuItem
from vigorish.cli.menu_items.change_setting import ChangeSetttingMenuItem
from vigorish.cli.menu_items.return_to_parent import ReturnToParentMenuItem
from vigorish.cli.util import print_message, prompt_user_yes_no
from vigorish.config.types import ConfigFile
from vigorish.constants import EMOJI_DICT
from vigorish.enums import ConfigType
from vigorish.util.result import Result
from vigorish.util.string_helpers import wrap_text


class CurrentSettingMenuItem(MenuItem):
    def __init__(self, setting_name: str, config: ConfigFile) -> None:
        self.setting_name = setting_name
        self.config = config
        self.setting = self.config.all_settings.get(self.setting_name)
        self.setting_name_title = self.setting.setting_name_title
        self.current_settings = self.setting.current_settings_report
        self.menu_item_text = self.setting.setting_name_title
        self.menu_item_emoji = EMOJI_DICT.get("GEAR")
        self.data_type = self.setting.data_type

    def launch(self) -> Result:
        subprocess.run(["clear"])
        setting_name = f"Setting: {self.setting_name_title} (Type: {self.data_type.name})"
        print_message(wrap_text(f"{setting_name}\n", max_len=70), fg="bright_magenta", bold=True)
        print_message(wrap_text(f"{self.setting.description}\n", max_len=70))
        print_message(f"{self.current_settings}\n", fg="bright_yellow", bold=True)
        result = prompt_user_yes_no(prompt="Change current setting?")
        change_setting = result.value
        if change_setting:
            change_setting_menu = ChangeSetttingMenuItem(self.setting_name, self.config)
            return change_setting_menu.launch()
        return Result.Ok(self.exit_menu)
