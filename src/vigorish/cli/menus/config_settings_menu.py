"""Menu that allows the user to view and modify all settings in vig.config.json."""
from vigorish.cli.menu import Menu
from vigorish.cli.menu_items.config_setting import ConfigSetting
from vigorish.cli.menu_items.return_to_parent import ReturnToParent
from vigorish.constants import EMOJIS


class ConfigSettingsMenu(Menu):
    def __init__(self, app):
        super().__init__(app)
        self.menu_text = "You can modify any setting in the list below:"
        self.menu_item_text = "Config File Settings"
        self.menu_item_emoji = EMOJIS.get("GEAR", "")

    def populate_menu_items(self):
        self.menu_items = [ConfigSetting(self.app, name) for name in self.config.all_settings.keys()]
        self.menu_items.append(ReturnToParent(self.app, "Return to Settings/Admin "))
        self.menu_items.insert(0, ReturnToParent(self.app, "Return to Settings/Admin"))
