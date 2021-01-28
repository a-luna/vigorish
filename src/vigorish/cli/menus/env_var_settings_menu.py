"""Menu that allows the user to view and modify all settings in vig.config.json."""
from vigorish.cli.menu import Menu
from vigorish.cli.menu_items.change_env_var_setting import ChangeEnvVarSetting
from vigorish.cli.menu_items.return_to_parent import ReturnToParent
from vigorish.constants import EMOJIS, ENV_VAR_NAMES


class EnvVarSettingsMenu(Menu):
    def __init__(self, app):
        super().__init__(app)
        self.menu_text = "You can modify any setting in the list below:"
        self.menu_item_text = "Environment Variables"
        self.menu_item_emoji = EMOJIS.get("SPIRAL", "")

    def populate_menu_items(self):
        self.menu_items = [ChangeEnvVarSetting(self.app, name) for name in ENV_VAR_NAMES]
        self.menu_items.append(ReturnToParent(self.app, "Return to Settings/Admin"))
