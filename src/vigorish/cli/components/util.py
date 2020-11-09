"""Shared functions and menus for CLI."""
import subprocess
from functools import partial
from random import randint

import click
from getch import pause

from vigorish.config.database import Season
from vigorish.constants import CLI_COLORS, FIGLET_FONTS
from vigorish.util.result import Result
from vigorish.util.string_helpers import wrap_text


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


def get_random_figlet_font():
    return FIGLET_FONTS[randint(0, len(FIGLET_FONTS) - 1)]


def print_message(message, wrap=True, max_line_len=70, fg=None, bg=None, bold=None, underline=None):
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
    if result.success:
        season = result.value
        return Result.Ok(season)
    error_heading = "Error! Invalid value for start and/or end dates"
    print()
    print_heading(error_heading, fg="bright_red")
    for s in result.error:
        print_message(s, fg="bright_red")
    print()
    pause(message="Press any key to continue...")
    return Result.Fail("")


def shutdown_cli_immediately():
    subprocess.run(["clear"])
    print_heading("Restart Required!", fg="bright_magenta")
    warning = "Application must be restarted for these changes to take effect."
    print_message(warning, fg="bright_magenta", bold=True)
    print_message("Shutting down vigorish!\n", fg="bright_magenta", bold=True)
    pause(message="Press any key to continue...")
    subprocess.run(["clear"])
    exit(0)
