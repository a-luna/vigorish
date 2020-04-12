"""Menu that allows the user to view and modify environment variables."""
import subprocess

from bullet import Input, colors

from vigorish.cli.menu_item import MenuItem
from vigorish.cli.util import print_message, prompt_user_yes_no, prompt_user_yes_no_cancel
from vigorish.config.dotenv import DotEnvFile
from vigorish.constants import EMOJI_DICT
from vigorish.util.result import Result


class DotEnvSettingMenuItem(MenuItem):
    def __init__(self, setting_name: str, dotenv_file: DotEnvFile) -> None:
        self.menu_item_text = setting_name
        self.menu_item_emoji = EMOJI_DICT.get("SPIRAL")
        self.setting_name = setting_name
        self.dotenv_file = dotenv_file
        self.current_setting = self.dotenv_file.get_current_value(self.setting_name)
        self.exit_menu = False

    def launch(self) -> Result:
        subprocess.run(["clear"])
        env_var_name = f"Environment Variable: {self.setting_name}\n"
        env_var_value = f"Current Value: {self.current_setting}\n"
        print_message(env_var_name, fg="bright_magenta", bold=True)
        print_message(env_var_value, fg="bright_yellow", bold=True)
        result = prompt_user_yes_no(prompt="Change current setting?")
        change_setting = result.value
        if not change_setting:
            return Result.Ok(self.exit_menu)
        user_confirmed = False
        while not user_confirmed:
            subprocess.run(["clear"])
            prompt = (
                f"Enter a value for {self.setting_name} "
                f"(Current Value: {self.current_setting}): "
            )
            new_value = Input(prompt, word_color=colors.foreground["default"]).launch()
            result = self.confirm_new_value(new_value)
            if result.failure:
                return Result.Ok(self.exit_menu)
            user_confirmed = result.value
        return self.dotenv_file.change_value(self.setting_name, new_value)

    def confirm_new_value(self, new_value):
        prompt = (
            f"Select YES to update the value below, select NO to enter a different value, "
            f"or select CANCEL to return to the Settings menu:\n{self.setting_name}={new_value}"
        )
        return prompt_user_yes_no_cancel(prompt)
