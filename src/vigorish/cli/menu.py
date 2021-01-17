import subprocess
from abc import ABC, abstractmethod

from bullet import Bullet, colors, ScrollBar

from vigorish.cli.menu_item import MenuItem
from vigorish.util.result import Result
from vigorish.util.string_helpers import ellipsize


class Menu(MenuItem, ABC):
    menu_text = ""
    selected_menu_item_text = ""
    pointer = ""
    menu_items = []
    bullet_color = colors.bright(colors.foreground["default"])
    check_color = colors.bright(colors.foreground["default"])
    check_on_switch = colors.bright(colors.foreground["default"])
    pointer_color = colors.bright(colors.foreground["default"])

    def __init__(self, app):
        super().__init__(app)

    @property
    def menu_item_text_list(self):
        return list(self.menu_item_dict.keys())

    @property
    def menu_item_dict(self):
        return {ellipsize(item.menu_item_text, 70): item for item in self.menu_items}

    @property
    def menu_item_count(self):
        return len(self.menu_items)

    @property
    def selected_menu_item_pos(self):
        if not self.selected_menu_item_text:
            return 0
        menu_item_pos_dict = {ellipsize(item.menu_item_text, 70): i for i, item in enumerate(self.menu_items)}
        return menu_item_pos_dict.get(self.selected_menu_item_text, 0)

    @property
    def selected_menu_item(self):
        return self.menu_item_dict.get(self.selected_menu_item_text)

    def launch(self):
        exit_menu = False
        while not exit_menu:
            subprocess.run(["clear"])
            self.populate_menu_items()
            result = self.prompt_user_for_menu_selection()
            if result.failure:
                return result
            exit_menu = result.value
        return Result.Ok(exit_menu)

    def prompt_user_for_menu_selection(self):
        if self.menu_item_count <= 8:
            menu = self._get_bullet_menu()
            menu.pos = self.selected_menu_item_pos
        else:
            menu = self._get_scroll_menu()
        self.selected_menu_item_text = menu.launch()
        result = self.selected_menu_item.launch()
        exit_menu = self.selected_menu_item.exit_menu
        return Result.Ok(exit_menu) if result.success else result

    @abstractmethod
    def populate_menu_items(self):
        pass

    def _get_bullet_menu(self):
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

    def _get_scroll_menu(self):
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
