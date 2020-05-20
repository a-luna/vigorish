"""Shared functions and menus for CLI."""
from random import randint

import click
import subprocess
from dateutil import parser
from bullet import Bullet, ScrollBar, Check, colors, Input
from getch import pause

from vigorish.config.database import Season
from vigorish.constants import MENU_NUMBERS, CLI_COLORS, EMOJI_DICT, DATA_SET_NAMES_LONG
from vigorish.enums import DataSet
from vigorish.util.result import Result
from vigorish.util.string_helpers import wrap_text


def get_random_cli_color():
    colors = ["red", "blue", "green", "cyan", "magenta", "yellow"]
    return colors[randint(0, len(colors) - 1)]


def print_message(
    message, wrap=True, max_line_len=70, fg=None, bg=None, bold=None, underline=None
):
    if (fg and fg not in CLI_COLORS) or (bg and bg not in CLI_COLORS):
        fg = None
        bg = None
    if wrap:
        message = wrap_text(message, max_len=max_line_len)
    click.secho(message, fg=fg, bg=bg, bold=bold, underline=underline)


def prompt_user_yes_no(prompt):
    choices = {
        f"{MENU_NUMBERS.get(1)}  YES": True,
        f"{MENU_NUMBERS.get(2)}  NO": False,
    }
    prompt = Bullet(
        wrap_text(prompt, 70),
        choices=[choice for choice in choices.keys()],
        bullet="",
        shift=1,
        indent=2,
        margin=2,
        bullet_color=colors.foreground["default"],
        background_color=colors.foreground["default"],
        background_on_switch=colors.foreground["default"],
        word_color=colors.foreground["default"],
        word_on_switch=colors.bright(colors.foreground["cyan"]),
    )
    choice_text = prompt.launch()
    choice_value = choices.get(choice_text)
    return Result.Ok(choice_value)


def prompt_user_yes_no_cancel(prompt):
    choices = {
        f"{MENU_NUMBERS.get(1)}  YES": True,
        f"{MENU_NUMBERS.get(2)}  NO": False,
        f"{MENU_NUMBERS.get(3)}  CANCEL": None,
    }
    prompt = Bullet(
        wrap_text(prompt, 70),
        choices=[choice for choice in choices.keys()],
        bullet="",
        shift=1,
        indent=2,
        margin=2,
        bullet_color=colors.foreground["default"],
        background_color=colors.foreground["default"],
        background_on_switch=colors.foreground["default"],
        word_color=colors.foreground["default"],
        word_on_switch=colors.bright(colors.foreground["cyan"]),
    )
    choice_text = prompt.launch()
    choice_value = choices.get(choice_text)
    return Result.Fail("") if "CANCEL" in choice_text else Result.Ok(choice_value)


class DateInput(Input):
    def launch(self):
        parsed_date = None
        while not parsed_date:
            result = super().launch()
            if result:
                try:
                    parsed_date = parser.parse(result)
                except ValueError:
                    continue
        return parsed_date
