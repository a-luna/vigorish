import subprocess
from dataclasses import dataclass

from bullet import Bullet, colors, keyhandler
from bullet.cursor import hide as cursor_hidden
from bullet.utils import moveCursorUp

from vigorish.constants import EMOJI_DICT, MENU_NUMBERS
from vigorish.cli.util import print_heading, print_message

ARROW_KEY_FLAG = 1 << 8
ARROW_RIGHT_KEY = 67 + ARROW_KEY_FLAG
ARROW_LEFT_KEY = 68 + ARROW_KEY_FLAG

NAV_PREV = f"{EMOJI_DICT.get('LEFT')}  Previous"
NAV_NEXT = f"Next {EMOJI_DICT.get('RIGHT')}"
NAV_TOTAL_LEN = 43
NAV_PREV_LEN = 11
NAV_NEXT_LEN = 6


@dataclass(frozen=True)
class DisplayTable:
    table: str
    heading: str = None
    message: str = None


class TableViewer(Bullet):
    def __init__(
        self,
        table_list,
        prompt,
        confirm_only=False,
        yes_choice="YES",
        no_choice="NO",
        heading_color=None,
        message_color=None,
        table_color=None,
    ):
        self.choices_dict = self.get_prompt_choices(confirm_only, yes_choice, no_choice)
        super().__init__(
            prompt=prompt,
            choices=[choice for choice in self.choices_dict.keys()],
            bullet="",
            bullet_color=colors.foreground["default"],
            word_color=colors.foreground["default"],
            word_on_switch=colors.bright(colors.foreground["cyan"]),
            background_color=colors.background["default"],
            background_on_switch=colors.background["default"],
            shift=0,
            indent=0,
            margin=2,
            pad_right=0,
            align=0,
            return_index=False,
        )
        self.table_list = table_list
        self.confirm_only = confirm_only
        self.heading_color = heading_color
        self.message_color = message_color
        self.table_color = table_color
        self.table_count = len(table_list)
        self.table_index = 0
        self.needs_update = False

    @property
    def first_table_displayed(self):
        return self.table_index == 0

    @property
    def last_table_displayed(self):
        return self.table_index == self.table_count - 1

    @property
    def current_table(self):
        return self.table_list[self.table_index]

    @keyhandler.register(ARROW_LEFT_KEY)
    def prev_table(self):
        if not self.first_table_displayed:
            self.table_index -= 1
            self.needs_update = True

    @keyhandler.register(ARROW_RIGHT_KEY)
    def next_table(self):
        if not self.last_table_displayed:
            self.table_index += 1
            self.needs_update = True

    def get_prompt_choices(self, confirm_only, yes_choice, no_choice):
        yes_no_choices = {
            f"{MENU_NUMBERS.get(1)}  {yes_choice}": True,
            f"{MENU_NUMBERS.get(2)}  {no_choice}": False,
        }
        confirm_choice = {f"{MENU_NUMBERS.get(1)}  OK": True}
        return confirm_choice if confirm_only else yes_no_choices

    def launch(self):
        while True:
            subprocess.run(["clear"])
            self.print_table(self.current_table)
            self.needs_update = False
            if self.prompt:
                self.print_prompt()
            if not self.confirm_only:
                self.renderBullets()
            moveCursorUp(len(self.choices) - self.pos)
            with cursor_hidden():
                while True:
                    user_selection = self.handle_input()
                    if self.needs_update:
                        break
                    if user_selection is not None:
                        return self.choices_dict.get(user_selection)

    def print_table(self, table):
        if table.heading:
            print_heading(table.heading, fg=self.heading_color)
        if table.message:
            print_message(table.message, fg=self.message_color, wrap=False)
        print_message(table.table, fg=self.table_color, wrap=False)
        self.print_table_nav()

    def print_prompt(self):
        prompt = f'\n{" " * self.indent}{self.prompt}\n'
        is_bold = self.confirm_only
        print_message(prompt, fg=self.message_color, bold=is_bold)

    def print_table_nav(self):
        if self.table_count <= 1:
            return
        page_number = f"({self.table_index + 1}/{self.table_count})"
        (pad_left, pad_right) = self.get_pad_lengths(len(page_number))
        nav_prev = NAV_PREV if not self.first_table_displayed else f'{" " * NAV_PREV_LEN}'
        nav_next = NAV_NEXT if not self.last_table_displayed else f'{" " * NAV_NEXT_LEN}'
        table_nav = f'\n{nav_prev}{" " * pad_left}{page_number}{" " * pad_right}{nav_next}'
        print_message(table_nav, fg=self.table_color)

    def get_pad_lengths(self, page_number_len):
        remaining = NAV_TOTAL_LEN - NAV_PREV_LEN - NAV_NEXT_LEN - page_number_len
        pad = int(remaining / 2)
        return (pad, pad + 1) if remaining % 2 else (pad, pad)
