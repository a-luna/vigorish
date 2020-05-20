"""Menu that allows the user to view and modify all settings in vig.config.json."""
from vigorish.cli.menu import Menu
from vigorish.cli.menu_items.change_env_var_setting import EnvVarSettingMenuItem
from vigorish.cli.menu_items.return_to_parent import ReturnToParentMenuItem
from vigorish.constants import EMOJI_DICT, ENV_VAR_NAMES


class EnvVarSettingsMenu(Menu):
    def __init__(self, app):
        super().__init__(app)
        self.menu_text = "You can modify any setting in the list below:"
        self.menu_item_text = "Environment Variables"
        self.menu_item_emoji = EMOJI_DICT.get("SPIRAL", "")

    def populate_menu_items(self):
        self.menu_items = [EnvVarSettingMenuItem(self.app, name) for name in ENV_VAR_NAMES]
        self.menu_items.append(ReturnToParentMenuItem(self.app, "Return to Settings/Admin"))
