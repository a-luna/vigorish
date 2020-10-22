import subprocess

from bullet import Bullet, colors, keyhandler
from bullet.charDef import ARROW_LEFT_KEY, ARROW_RIGHT_KEY
from bullet.cursor import hide as cursor_hidden
from bullet.utils import moveCursorUp

from vigorish.cli.components.util import print_message
from vigorish.constants import EMOJI_DICT, MENU_NUMBERS

NAV_PREV = f"{EMOJI_DICT.get('LEFT')}  Previous"
NAV_NEXT = f"Next {EMOJI_DICT.get('RIGHT')}"
NAV_TOTAL_LEN = 43
NAV_PREV_LEN = len(NAV_PREV)
NAV_NEXT_LEN = len(NAV_NEXT)


class PageViewer(Bullet):
    def __init__(
        self,
        pages,
        prompt,
        confirm_only=False,
        yes_choice="YES",
        no_choice="NO",
        heading_color=None,
        text_color=None,
    ):
        self.choices_dict = self.get_prompt_choices(confirm_only, yes_choice, no_choice)
        super(PageViewer, self).__init__(
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
        self.pages = pages
        self.confirm_only = confirm_only
        self.heading_color = heading_color
        self.text_color = text_color
        self.page_count = len(pages)
        self.page_index = 0
        self.needs_update = False

    @property
    def current_page(self):
        return self.pages[self.page_index]

    @keyhandler.register(ARROW_LEFT_KEY)
    def prev_page(self):
        if not hasattr(self, "first_page_displayed"):
            return
        if not self.first_page_displayed():
            self.page_index -= 1
            self.needs_update = True

    @keyhandler.register(ARROW_RIGHT_KEY)
    def next_page(self):
        if not hasattr(self, "last_page_displayed"):
            return
        if not self.last_page_displayed():
            self.page_index += 1
            self.needs_update = True

    def first_page_displayed(self):
        return self.page_index == 0

    def last_page_displayed(self):
        return self.page_index == self.page_count - 1

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
            self.current_page.display(text_color=self.text_color, heading_color=self.heading_color)
            self.print_page_nav()
            self.needs_update = False
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

    def print_prompt(self):
        prompt = f'\n{" " * self.indent}{self.prompt}\n'
        print_message(prompt, bold=self.confirm_only)

    def print_page_nav(self):
        if self.page_count <= 1:
            return
        page_number = f"({self.page_index + 1}/{self.page_count})"
        (pad_left, pad_right) = self.get_pad_lengths(len(page_number))
        nav_prev = NAV_PREV if not self.first_page_displayed() else f'{" " * NAV_PREV_LEN}'
        nav_next = NAV_NEXT if not self.last_page_displayed() else f'{" " * NAV_NEXT_LEN}'
        page_nav = f'\n{nav_prev}{" " * pad_left}{page_number}{" " * pad_right}{nav_next}'
        print_message(page_nav, fg=self.text_color)

    def get_pad_lengths(self, page_number_len):
        remaining = NAV_TOTAL_LEN - NAV_PREV_LEN - NAV_NEXT_LEN - page_number_len
        pad = int(remaining / 2)
        return (pad, pad + 1) if remaining % 2 else (pad, pad)
