"""Shared functions and menus for CLI."""
from functools import partial
from random import randint

import click
from getch import pause

from vigorish.config.database import Season, PlayerId
from vigorish.constants import CLI_COLORS
from vigorish.util.result import Result
from vigorish.util.string_helpers import wrap_text, validate_at_bat_id


def get_random_cli_color():
    colors = ["red", "blue", "green", "cyan", "magenta", "yellow"]
    return colors[randint(0, len(colors) - 1)]


def get_random_bright_cli_color():
    colors = [
        "bright_red",
        "bright_blue",
        "bright_green",
        "bright_cyan",
        "bright_magenta",
        "bright_yellow",
    ]
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


def print_heading(message, fg=None, bg=None):
    print_message(f"{message}\n", wrap=False, fg=fg, bg=bg, bold=True, underline=True)


print_error = partial(print_message, fg="bright_red", bg=None, bold=True, underline=None)
print_success = partial(print_message, fg="bright_green", bg=None, bold=True, underline=None)


def validate_scrape_dates(db_session, start_date, end_date):
    result = Season.validate_date_range(db_session, start_date, end_date)
    if result.failure:
        print_message(result.error, wrap=False, fg="bright_red", bold=True)
        pause(message="Press any key to continue...")
        return Result.Fail("")
    season = result.value
    return Result.Ok(season)


def describe_at_bat(db_session, at_bat_id):
    id_dict = PlayerId.get_player_ids_from_at_bat_id(db_session, at_bat_id)
    at_bat = validate_at_bat_id(at_bat_id).value
    at_bat["pitcher_bbref_id"] = id_dict["pitcher_id_bbref"]
    at_bat["pitcher_name"] = id_dict["pitcher_name"]
    at_bat["batter_bbref_id"] = id_dict["batter_id_bbref"]
    at_bat["batter_name"] = id_dict["batter_name"]
    return at_bat
