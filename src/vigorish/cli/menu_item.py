"""ABC for menu items that execute a command."""
import subprocess
from abc import ABC, abstractmethod

from bullet import colors

from vigorish.cli.components.util import print_heading


class MenuItem(ABC):
    # TODO: Update all menu items to use the menu_heading value

    _menu_item_text = ""
    menu_item_emoji = ""
    menu_heading = "Menu"
    background_color = colors.background["default"]
    background_on_switch = colors.background["default"]
    word_color = colors.foreground["default"]
    word_on_switch = colors.bright(colors.foreground["cyan"])
    exit_menu = False

    def __init__(self, app):
        self.app = app
        self.dotenv = app.dotenv
        self.config = app.config
        self.db_engine = app.db_engine
        self.db_session = app.db_session
        self.scraped_data = app.scraped_data

    @property
    def menu_item_text(self):
        return f"{self.menu_item_emoji} {self._menu_item_text}"

    @menu_item_text.setter
    def menu_item_text(self, menu_item_text):
        self._menu_item_text = menu_item_text

    def get_menu_heading(self, status):
        return f"{self.menu_heading}: {status}"

    def update_menu_heading(self, status, heading_color="bright_yellow"):
        subprocess.run(["clear"])
        print_heading(self.get_menu_heading(status), fg=heading_color)

    @abstractmethod
    def launch(self):
        pass
