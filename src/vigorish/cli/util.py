"""Shared functions and menus for CLI."""
from random import randint

import click
from getch import pause

from vigorish.config.database import Season
from vigorish.constants import CLI_COLORS
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


def validate_scrape_dates(db_session, start_date, end_date):
    result = Season.validate_date_range(db_session, start_date, end_date)
    if result.failure:
        print_message(result.error, wrap=False, fg="bright_red", bold=True)
        pause(message="Press any key to continue...")
        return Result.Fail("")
    season = result.value
    return Result.Ok(season)
