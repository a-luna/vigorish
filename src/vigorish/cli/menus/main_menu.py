"""The main menu for the CLI."""
from vigorish.cli.menu import Menu
from vigorish.cli.menus.settings_menu import ConfigSettingsMenu
from vigorish.cli.menu_items.exit_program import ExitProgramMenuItem
from vigorish.util.result import Result


class MainMenu(Menu):
    def __init__(self, vig) -> None:
        self.config_file = vig["config"]
        self.db_session = vig["session"]
        self.menu_text = "Welcome to vigorish!"
        self.menu_item_text = "Main Menu"
        self.exit_menu = False
        self.menu_items = self._get_menu_items()

    def _get_menu_items(self) -> None:
        menu_items = []
        menu_items.append(ConfigSettingsMenu("Settings", self.config_file))
        menu_items.append(ExitProgramMenuItem())
        return menu_items
