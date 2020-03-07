"""Shared functions and menus for CLI."""
import click
from bullet import Bullet, colors

from vigorish.constants import EMOJI_DICT, MENU_NUMBERS, CLI_COLORS
from vigorish.util.result import Result


def print_message(message, fg=None, bg=None, bold=None, underline=None):
    if (fg and fg not in CLI_COLORS) or (bg and bg not in CLI_COLORS):
        fg = None
        bg = None
    click.secho(f"{message}", fg=fg, bg=bg, bold=bold, underline=underline)


def prompt_user_yes_no(prompt: str, pointer: str = None) -> Result:
    if not pointer:
        pointer = EMOJI_DICT.get("HAND_POINTER", "")
    choices = {
        f"{MENU_NUMBERS.get(1)}  YES": True,
        f"{MENU_NUMBERS.get(2)}  NO": False,
    }
    prompt = Bullet(
        prompt,
        choices=[choice for choice in choices.keys()],
        bullet=f" {pointer}",
        margin=2,
        bullet_color=colors.bright(colors.foreground["cyan"]),
        background_color=colors.foreground["default"],
        background_on_switch=colors.foreground["default"],
        word_color=colors.foreground["default"],
        word_on_switch=colors.foreground["default"],
    )
    choice_text = prompt.launch()
    choice_value = choices.get(choice_text)
    return Result.Ok(choice_value)


def prompt_user_yes_no_cancel(prompt: str, pointer: str = None) -> Result:
    if not pointer:
        pointer = EMOJI_DICT.get("HAND_POINTER", "")
    choices = {
        f"{MENU_NUMBERS.get(1)}  YES": True,
        f"{MENU_NUMBERS.get(2)}  NO": False,
        f"{MENU_NUMBERS.get(3)}  CANCEL": None,
    }
    prompt = Bullet(
        prompt,
        choices=[choice for choice in choices.keys()],
        bullet=f" {pointer}",
        margin=2,
        bullet_color=colors.bright(colors.foreground["cyan"]),
        background_color=colors.foreground["default"],
        background_on_switch=colors.foreground["default"],
        word_color=colors.foreground["default"],
        word_on_switch=colors.foreground["default"],
    )
    choice_text = prompt.launch()
    choice_value = choices.get(choice_text)
    return Result.Fail("") if "CANCEL" in choice_text else Result.Ok(choice_value)
