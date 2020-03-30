import subprocess
from abc import ABC, abstractmethod
from typing import List, Union, Dict

from bullet import Bullet, ScrollBar, colors

from vigorish.cli.menu_item import MenuItem
from vigorish.util.result import Result
from vigorish.util.string_helpers import ellipsize


class Menu(MenuItem, ABC):
    menu_text: str = ""
    menu_items: List[MenuItem] = []
    selected_menu_item_text: str = ""
    pointer: str = ""
    bullet_color: str = colors.bright(colors.foreground["default"])
    check_color: str = colors.bright(colors.foreground["default"])
    check_on_switch: str = colors.bright(colors.foreground["default"])
    pointer_color: str = colors.bright(colors.foreground["default"])

    @property
    def menu_item_text_list(self) -> List[str]:
        return [menu_item_text for menu_item_text in self.menu_item_dict.keys()]

    @property
    def menu_item_dict(self) -> Dict[str, MenuItem]:
        return {ellipsize(item.menu_item_text, 70): item for item in self.menu_items}

    @property
    def menu_item_count(self) -> int:
        return len(self.menu_items)

    @property
    def selected_menu_item_pos(self) -> int:
        if not self.selected_menu_item_text:
            return 0
        menu_item_pos_dict = {
            ellipsize(item.menu_item_text, 70): i for i, item in enumerate(self.menu_items)
        }
        return menu_item_pos_dict.get(self.selected_menu_item_text, 0)

    @property
    def selected_menu_item(self) -> Union[None, MenuItem]:
        return self.menu_item_dict.get(self.selected_menu_item_text)

    def launch(self) -> Result:
        result: Result
        exit_menu = False
        while not exit_menu:
            subprocess.run(["clear"])
            self.populate_menu_items()
            if self.menu_item_count <= 8:
                menu = self._get_bullet_menu()
                menu.pos = self.selected_menu_item_pos
            else:
                menu = self._get_scroll_menu()
            self.selected_menu_item_text = menu.launch()
            result = self.selected_menu_item.launch()
            if result.failure:
                break
            exit_menu = self.selected_menu_item.exit_menu
        return result

    @abstractmethod
    def populate_menu_items(self) -> None:
        pass

    def _get_bullet_menu(self) -> Bullet:
        return Bullet(
            self.menu_text,
            choices=self.menu_item_text_list,
            bullet=self.pointer,
            shift=1,
            indent=2,
            margin=2,
            bullet_color=self.bullet_color,
            background_color=self.background_color,
            background_on_switch=self.background_on_switch,
            word_color=self.word_color,
            word_on_switch=self.word_on_switch,
        )

    def _get_scroll_menu(self) -> ScrollBar:
        return ScrollBar(
            self.menu_text,
            choices=self.menu_item_text_list,
            height=8,
            pointer=self.pointer,
            shift=1,
            indent=2,
            margin=2,
            pointer_color=self.pointer_color,
            background_color=self.background_color,
            background_on_switch=self.background_on_switch,
            word_color=self.word_color,
            word_on_switch=self.word_on_switch,
        )
