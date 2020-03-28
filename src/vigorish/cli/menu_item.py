"""ABC for menu items that execute a command."""
from abc import ABC, abstractmethod

from bullet import colors

from vigorish.util.result import Result


class MenuItem(ABC):
    _menu_item_text: str = ""
    menu_item_emoji: str = ""
    background_color: str = colors.foreground["default"]
    background_on_switch: str = colors.foreground["default"]
    word_color: str = colors.foreground["default"]
    word_on_switch: str = colors.bright(colors.foreground["cyan"])
    exit_menu: bool = False

    @property
    def menu_item_text(self) -> str:
        return f"{self.menu_item_emoji}  {self._menu_item_text}"

    @menu_item_text.setter
    def menu_item_text(self, menu_item_text: str) -> None:
        self._menu_item_text = menu_item_text

    @abstractmethod
    def launch(self) -> Result:
        pass
