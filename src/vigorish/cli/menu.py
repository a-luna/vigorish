import subprocess
from typing import List, Union

from bullet import Bullet, ScrollBar, colors

from vigorish.constants import EMOJI_DICT
from vigorish.cli.menu_item import MenuItem
from vigorish.util.result import Result
from vigorish.util.string_helpers import ellipsize


class Menu(MenuItem):
    menu_text: str = ""
    menu_items: List[MenuItem] = []
    selected_menu_item_text: str = ""
    pointer: str = EMOJI_DICT.get("HAND_POINTER", "")
    bullet_color: str = colors.bright(colors.foreground["cyan"])
    check_color: str = colors.bright(colors.foreground["cyan"])
    check_on_switch: str = colors.bright(colors.foreground["cyan"])
    pointer_color: str = colors.bright(colors.foreground["cyan"])
    background_color: str = colors.foreground["default"]
    background_on_switch: str = colors.foreground["default"]
    word_color: str = colors.foreground["default"]
    word_on_switch: str = colors.foreground["default"]
    margin: int = 2

    @property
    def menu_item_text_list(self) -> List[str]:
        return [ellipsize(item.menu_item_text, 60) for item in self.menu_items]

    @property
    def menu_item_count(self) -> int:
        return len(self.menu_items)

    @property
    def selected_menu_item(self) -> Union[None, MenuItem]:
        if not self.selected_menu_item_text:
            return None
        menu_item_dict = {
            ellipsize(item.menu_item_text, 60): item for item in self.menu_items
        }
        return menu_item_dict.get(self.selected_menu_item_text)

    @property
    def selected_menu_item_pos(self) -> int:
        if not self.selected_menu_item_text:
            return 0
        menu_item_pos_dict = {
            ellipsize(item.menu_item_text, 60): i
            for i, item in enumerate(self.menu_items)
        }
        return menu_item_pos_dict.get(self.selected_menu_item_text, 0)

    def launch(self) -> Result:
        result: Result
        exit_menu = False
        while not exit_menu:
            subprocess.run(["clear"])
            if self.menu_item_count <= 8:
                menu = self._get_bullet_menu()
                menu.pos = self.selected_menu_item_pos
            else:
                menu = self._get_scroll_menu()
            self.selected_menu_item_text = menu.launch()
            result = self.selected_menu_item.launch()
            exit_menu = self.selected_menu_item.exit_menu
        return result

    def _get_bullet_menu(self) -> Bullet:
        return Bullet(
            self.menu_text,
            choices=self.menu_item_text_list,
            bullet=f" {self.pointer}",
            margin=self.margin,
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
            pointer=f" {self.pointer}",
            margin=self.margin,
            pointer_color=self.pointer_color,
            background_color=self.background_color,
            background_on_switch=self.background_on_switch,
            word_color=self.word_color,
            word_on_switch=self.word_on_switch,
        )
