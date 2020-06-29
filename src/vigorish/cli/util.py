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


def get_random_dots_spinner():
    return f"dots{randint(2, 9)}"


def print_message(
    message, wrap=True, max_line_len=70, fg=None, bg=None, bold=None, underline=None
):
    if (fg and fg not in CLI_COLORS) or (bg and bg not in CLI_COLORS):
        fg = None
        bg = None
    if wrap:
        message = wrap_text(message, max_len=max_line_len)
    click.secho(message, fg=fg, bg=bg, bold=bold, underline=underline)


def prompt_user_yes_no(prompt, wrap=True, max_line_len=70):
    if wrap:
        prompt = wrap_text(prompt, max_len=max_line_len)
    choices = {
        f"{MENU_NUMBERS.get(1)}  YES": True,
        f"{MENU_NUMBERS.get(2)}  NO": False,
    }
    prompt = Bullet(
        prompt,
        choices=[choice for choice in choices.keys()],
        bullet="",
        shift=1,
        indent=0,
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


def prompt_user_yes_no_cancel(prompt, wrap=True, max_line_len=70):
    if wrap:
        prompt = wrap_text(prompt, max_len=max_line_len)
    choices = {
        f"{MENU_NUMBERS.get(1)}  YES": True,
        f"{MENU_NUMBERS.get(2)}  NO": False,
        f"{MENU_NUMBERS.get(3)}  CANCEL": None,
    }
    prompt = Bullet(
        prompt,
        choices=[choice for choice in choices.keys()],
        bullet="",
        shift=1,
        indent=0,
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


def validate_scrape_dates(db_session, start_date, end_date):
    result = Season.validate_date_range(db_session, start_date, end_date)
    if result.failure:
        error = f"The dates you entered are invalid:\n{result.error}"
        print_message(error, fg="bright_red", bold=True)
        pause(message="Press any key to continue...")
        return Result.Fail("")
    season = result.value
    return Result.Ok(season)


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
            indent=0,
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
            indent=0,
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
        f"{MENU_NUMBERS.get(num)}  {season.year}": season
        for num, season in enumerate(Season.all_regular_seasons(db_session), start=1)
    }
    choices[f"{EMOJI_DICT.get('BACK')} Return to Previous Menu"] = None
    return user_options_prompt(choices, prompt)


def data_sets_prompt(prompt):
    if not prompt:
        prompt = "Select one or multiple data sets from the list below:"
    instructions = "(use SPACE BAR to select each data set, ENTER to confirm your selections)"
    data_sets_prompt = Check(
        prompt=f"{prompt}\n{instructions}",
        check=EMOJI_DICT.get("CHECK", ""),
        choices=[data_set for data_set in DATA_SET_NAMES_LONG.keys() if data_set != DataSet.ALL],
        shift=1,
        indent=0,
        margin=2,
        check_color=colors.foreground["default"],
        check_on_switch=colors.foreground["default"],
        background_color=colors.foreground["default"],
        background_on_switch=colors.foreground["default"],
        word_color=colors.foreground["default"],
        word_on_switch=colors.bright(colors.foreground["cyan"]),
    )
    data_sets = []
    while not data_sets:
        subprocess.run(["clear"])
        result = data_sets_prompt.launch()
        if result:
            data_sets = {DATA_SET_NAMES_LONG[sel]: sel for sel in result}
    return data_sets
