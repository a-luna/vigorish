"""Menu that contains other menus related to configuring/administrating vigorish."""
from vigorish.cli.menu import Menu
from vigorish.cli.menu_items.return_to_parent import ReturnToParent
from vigorish.cli.menus.config_settings_menu import ConfigSettingsMenu
from vigorish.cli.menus.env_var_settings_menu import EnvVarSettingsMenu
from vigorish.constants import EMOJI_DICT


class SettingsMenu(Menu):
    def __init__(self, app):
        super().__init__(app)
        self.menu_text = "You can view/edit Config File settings or Env. Variable settings:"
        self.menu_item_text = "Settings"
        self.menu_item_emoji = EMOJI_DICT.get("TOOLS", "")

    def populate_menu_items(self):
        self.menu_items = []
        self.menu_items.append(ConfigSettingsMenu(self.app))
        self.menu_items.append(EnvVarSettingsMenu(self.app))
        self.menu_items.append(ReturnToParent(self.app, "Return to Main Menu"))
