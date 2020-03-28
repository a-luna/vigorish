"""Shared functions and menus for CLI."""
import click
from dateutil import parser
from bullet import Bullet, colors, Input, keyhandler
from bullet.charDef import NEWLINE_KEY

from vigorish.constants import EMOJI_DICT, MENU_NUMBERS, CLI_COLORS
from vigorish.enums import DataSet
from vigorish.util.result import Result
from vigorish.util.string_helpers import ellipsize

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
    click.secho(f"{message}", fg=fg, bg=bg, bold=bold, underline=underline)


def prompt_user_yes_no(prompt: str) -> Result:
    choices = {
        f"{MENU_NUMBERS.get(1)}  YES": True,
        f"{MENU_NUMBERS.get(2)}  NO": False,
    }
    prompt = Bullet(
        ellipsize(prompt, 70),
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


def prompt_user_yes_no_cancel(prompt: str) -> Result:
    choices = {
        f"{MENU_NUMBERS.get(1)}  YES": True,
        f"{MENU_NUMBERS.get(2)}  NO": False,
        f"{MENU_NUMBERS.get(3)}  CANCEL": None,
    }
    prompt = Bullet(
        ellipsize(prompt, 70),
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