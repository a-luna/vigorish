"""The main menu for the CLI."""
from vigorish.cli.menu import Menu
from vigorish.cli.menus.settings_menu import SettingsMenu
from vigorish.cli.menu_items.exit_program import ExitProgramMenuItem
from vigorish.util.result import Result


class MainMenu(Menu):
    def __init__(self, vig) -> None:
        self.config = vig["config"]
        self.db_session = vig["session"]
        self.menu_text = "Welcome to vigorish!"
        self.menu_item_text = "Main Menu"
        self.exit_menu = False

    def launch(self) -> Result:
        self._populate_menu()
        return super().launch()

    def _populate_menu(self) -> None:
        self.menu_items.clear()
        self.menu_items.append(SettingsMenu("Settings", self.config))
        self.menu_items.append(ExitProgramMenuItem())
