"""Menu that allows the user to view and modify all settings in vig.config.json."""
import subprocess

from vigorish.cli.components import print_heading, print_message, yes_no_prompt
from vigorish.cli.menu_item import MenuItem
from vigorish.cli.menu_items.change_config_setting import ChangeConfigSettting
from vigorish.constants import EMOJI_DICT
from vigorish.util.result import Result


class ConfigSetting(MenuItem):
    def __init__(self, app, setting_name):
        super().__init__(app)
        self.setting_name = setting_name
        self.setting = self.config.all_settings.get(setting_name)
        self.setting_name_title = self.setting.setting_name_title
        self.current_settings = self.setting.current_settings_report
        self.menu_item_text = self.setting.setting_name_title
        self.menu_item_emoji = EMOJI_DICT.get("GEAR")
        self.data_type = self.setting.data_type

    def launch(self):
        subprocess.run(["clear"])
        setting_heading = f"Setting: {self.setting_name_title} (Type: {self.data_type.name})"
        print_heading(setting_heading, fg="bright_magenta")
        print_message(self.setting.description)
        print()
        print_message(self.current_settings, wrap=False, fg="bright_yellow", bold=True)
        if yes_no_prompt("Change current setting?"):
            change_setting_menu = ChangeConfigSettting(self.app, self.setting_name)
            return change_setting_menu.launch()
        return Result.Ok(self.exit_menu)
