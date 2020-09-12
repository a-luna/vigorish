"""ABC for menu items that execute a command."""
from abc import ABC, abstractmethod

from bullet import colors


class MenuItem(ABC):
    _menu_item_text = ""
    menu_item_emoji = ""
    background_color = colors.background["default"]
    background_on_switch = colors.background["default"]
    word_color = colors.foreground["default"]
    word_on_switch = colors.bright(colors.foreground["cyan"])
    exit_menu = False

    def __init__(self, app):
        self.app = app
        self.dotenv = app["dotenv"]
        self.config = app["config"]
        self.db_engine = app["db_engine"]
        self.db_session = app["db_session"]
        self.scraped_data = app["scraped_data"]

    @property
    def menu_item_text(self):
        return f"{self.menu_item_emoji} {self._menu_item_text}"

    @menu_item_text.setter
    def menu_item_text(self, menu_item_text):
        self._menu_item_text = menu_item_text

    @abstractmethod
    def launch(self):
        pass
