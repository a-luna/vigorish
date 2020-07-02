"""Reusable menu prompts to get various values/data types from the user."""
import subprocess
from bullet import Bullet, ScrollBar, colors

from vigorish.cli.input_types import DataSetCheck, DateInput
from vigorish.config.database import Season
from vigorish.constants import MENU_NUMBERS, EMOJI_DICT, DATA_SET_NAMES_LONG
from vigorish.enums import DataSet
from vigorish.util.result import Result
from vigorish.util.string_helpers import wrap_text


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


def data_sets_prompt(prompt, checked_data_sets=None):
    if not prompt:
        prompt = "Select one or multiple data sets from the list below:"
    instructions = "(use SPACE BAR to select each data set, ENTER to confirm your selections)"
    data_sets_prompt = DataSetCheck(
        prompt=f"{prompt}\n{instructions}",
        choices=[data_set for data_set in DATA_SET_NAMES_LONG.keys() if data_set != DataSet.ALL],
        checked_data_sets=checked_data_sets,
        check=EMOJI_DICT.get("CHECK", ""),
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
