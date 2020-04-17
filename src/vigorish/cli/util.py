"""Shared functions and menus for CLI."""
import click
import subprocess
from dateutil import parser
from bullet import Bullet, colors, Input

from getch import pause

from vigorish.config.database import Season
from vigorish.constants import MENU_NUMBERS, CLI_COLORS
from vigorish.enums import DataSet
from vigorish.util.result import Result
from vigorish.util.string_helpers import wrap_text

DISPLAY_NAME_DICT = {
    "brooksbaseball.net Games for Date": DataSet.BROOKS_GAMES_FOR_DATE,
    "brooksbaseball.net Pitch Logs for Game": DataSet.BROOKS_PITCH_LOGS,
    "brooksbaseball.net PitchFX Logs": DataSet.BROOKS_PITCHFX,
    "bbref.com Games for Date": DataSet.BBREF_GAMES_FOR_DATE,
    "bbref.com Boxscores": DataSet.BBREF_BOXSCORES,
}


def get_data_set_display_name(data_set: DataSet) -> str:
    display_name_dict = {
        data_set: display_name for display_name, data_set in DISPLAY_NAME_DICT.items()
    }
    return display_name_dict[data_set]


def print_message(message, fg=None, bg=None, bold=None, underline=None):
    if (fg and fg not in CLI_COLORS) or (bg and bg not in CLI_COLORS):
        fg = None
        bg = None
    click.secho(message, fg=fg, bg=bg, bold=bold, underline=underline)


def validate_scrape_dates(db_session, start_date, end_date):
    result = Season.validate_date_range(db_session, start_date, end_date)
    if result.failure:
        error = f"The dates you entered are invalid:\n{result.error}"
        print_message(error, fg="bright_red", bold=True)
        pause(message="Press any key to continue...")
        return Result.Fail("")
    season = result.value
    return Result.Ok(season)


def prompt_user_yes_no(prompt: str) -> Result:
    choices = {
        f"{MENU_NUMBERS.get(1)}  YES": True,
        f"{MENU_NUMBERS.get(2)}  NO": False,
    }
    prompt = Bullet(
        f"\n{wrap_text(prompt, 70)}",
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
    choice_text = prompt.launch()
    choice_value = choices.get(choice_text)
    return Result.Ok(choice_value)


def prompt_user_yes_no_cancel(prompt: str) -> Result:
    choices = {
        f"{MENU_NUMBERS.get(1)}  YES": True,
        f"{MENU_NUMBERS.get(2)}  NO": False,
        f"{MENU_NUMBERS.get(3)}  CANCEL": None,
    }
    prompt = Bullet(
        f"\n{wrap_text(prompt, 70)}",
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
