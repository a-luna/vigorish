"""Menu that allows the user to view and modify environment variables."""
import subprocess
from sys import exit

from bullet import Input, colors
from getch import pause

from vigorish.cli.menu_item import MenuItem
from vigorish.cli.util import print_message, prompt_user_yes_no, prompt_user_yes_no_cancel
from vigorish.constants import EMOJI_DICT
from vigorish.util.result import Result

RESTART_WARNING = "\nApplication must be restarted for these changes to take effect!"


class EnvVarSettingMenuItem(MenuItem):
    def __init__(self, app, setting_name):
        super().__init__(app)
        self.menu_item_text = setting_name
        self.menu_item_emoji = EMOJI_DICT.get("SPIRAL")
        self.setting_name = setting_name
        self.current_setting = self.dotenv.get_current_value(setting_name)
        self.restart_required = self.dotenv.restart_required_on_change(setting_name)
        self.exit_menu = False

    def launch(self):
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
                f"(Current Value: {self.current_setting}):\n"
            )
            new_value = Input(prompt, word_color=colors.foreground["default"]).launch()
            result = self.confirm_new_value(new_value)
            if result.failure:
                return Result.Ok(self.exit_menu)
            user_confirmed = result.value
        result = self.dotenv.change_value(self.setting_name, new_value)
        if not self.restart_required:
            return result
        print_message(RESTART_WARNING, fg="bright_magenta", bold=True)
        pause(message="Press any key to continue...")
        exit(0)

    def confirm_new_value(self, new_value):
        prompt = (
            f"Select YES to update the value below, select NO to enter a different value, "
            f"or select CANCEL to return to the Settings menu:\n{self.setting_name}={new_value}"
        )
        return prompt_user_yes_no_cancel(prompt)
