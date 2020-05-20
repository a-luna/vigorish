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
                    error = (
                        f'"{result}" could not be parsed as a valid date. You can use any format '
                        f"recognized by dateutil.parser. For example, all of the strings below "
                        "are valid ways to represent the same date:\n"
                    )
                    examples = '"2018-5-13" -or- "05/13/2018" -or- "May 13 2018"'
                    print_message(error, fg="bright_red")
                    print_message(examples, fg="bright_red")
                    pause(message="Press any key to continue...")
        return parsed_date


def single_date_prompt(prompt):
    user_date = None
    while not user_date:
        subprocess.run(["clear"])
        date_prompt = DateInput(prompt=prompt)
        result = date_prompt.launch()
        if result:
            user_date = result
    return user_date


def user_options_prompt(choices, prompt):
    if len(choices) > 8:
        choices[f"{EMOJI_DICT.get('BACK')} Return to Previous Menu"] = None
        scroll_choices = [choice for choice in choices.keys()]
        scroll_choices.insert(0, f"{EMOJI_DICT.get('BACK')} Return to Previous Menu")
        options_menu = ScrollBar(
            wrap_text(prompt, 70),
            choices=scroll_choices,
            height=8,
            pointer="",
            shift=1,
            indent=2,
            margin=2,
            pointer_color=colors.foreground["default"],
            background_color=colors.foreground["default"],
            background_on_switch=colors.foreground["default"],
            word_color=colors.foreground["default"],
            word_on_switch=colors.bright(colors.foreground["cyan"]),
        )
    else:
        options_menu = Bullet(
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
    subprocess.run(["clear"])
    choice_text = options_menu.launch()
    choice_value = choices.get(choice_text)
    return Result.Ok(choice_value) if choice_value else Result.Fail("")


def season_prompt(db_session, prompt=None):
    if not prompt:
        prompt = "Please select a MLB Season from the list below:"
    choices = {
        f"{MENU_NUMBERS.get(num)}  {season.year}": season.year
        for num, season in enumerate(Season.all_regular_seasons(db_session), start=1)
    }
    choices[f"{EMOJI_DICT.get('BACK')} Return to Previous Menu"] = None
    return user_options_prompt(choices, prompt)
