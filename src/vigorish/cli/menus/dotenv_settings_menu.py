"""Menu that allows the user to view and modify all settings in vig.config.json."""
from vigorish.cli.menu import Menu
from vigorish.cli.menu_items.env_var_setting import DotEnvSettingMenuItem
from vigorish.cli.menu_items.return_to_parent import ReturnToParentMenuItem
from vigorish.config.dotenv import DotEnvFile
from vigorish.constants import EMOJI_DICT, ENV_VAR_NAMES


class DotEnvSettingsMenu(Menu):
    def __init__(self, dotenv: DotEnvFile) -> None:
        self.dotenv = dotenv
        self.menu_text = "You can modify any setting in the list below:"
        self.menu_item_text = "Environment Variable Settings"
        self.menu_item_emoji = EMOJI_DICT.get("SPIRAL", "")

    def populate_menu_items(self) -> None:
        self.menu_items = [DotEnvSettingMenuItem(name, self.dotenv) for name in ENV_VAR_NAMES]
        self.menu_items.append(ReturnToParentMenuItem("Main Menu"))
