"""Menu that allows the user to view and modify all settings in vig.config.json."""
from vigorish.cli.menu import Menu
from vigorish.cli.menu_items.config_setting import ConfigSettingMenuItem
from vigorish.cli.menu_items.return_to_parent import ReturnToParentMenuItem
from vigorish.constants import EMOJI_DICT


class ConfigSettingsMenu(Menu):
    def __init__(self, app):
        super().__init__(app)
        self.menu_text = "You can modify any setting in the list below:"
        self.menu_item_text = "Config File Settings"
        self.menu_item_emoji = EMOJI_DICT.get("GEAR", "")

    def populate_menu_items(self):
        self.menu_items = [
            ConfigSettingMenuItem(self.app, name) for name in self.config.all_settings.keys()
        ]
        self.menu_items.append(ReturnToParentMenuItem(self.app, "Return to Settings/Admin "))
        self.menu_items.insert(0, ReturnToParentMenuItem(self.app, "Return to Settings/Admin"))
