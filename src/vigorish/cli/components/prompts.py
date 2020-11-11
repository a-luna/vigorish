"""Reusable menu prompts to get various values/data types from the user."""
import subprocess

from bullet import Bullet, Check, colors, ScrollBar
from getch import pause

from vigorish.cli.components.data_set_check import DataSetCheck
from vigorish.cli.components.date_input import DateInput
from vigorish.cli.components.util import print_message
from vigorish.config.database import Season
from vigorish.constants import DATA_SET_NAMES_LONG, EMOJI_DICT, MENU_NUMBERS
from vigorish.enums import DataSet, VigFile
from vigorish.util.result import Result
from vigorish.util.string_helpers import wrap_text


def yes_no_prompt(prompt, wrap=True, max_line_len=70):
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
        word_color=colors.foreground["default"],
        word_on_switch=colors.bright(colors.foreground["cyan"]),
        background_color=colors.background["default"],
        background_on_switch=colors.background["default"],
    )
    print()
    choice_text = prompt.launch()
    return choices.get(choice_text)


def yes_no_cancel_prompt(prompt, wrap=True, max_line_len=70):
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
        word_color=colors.foreground["default"],
        word_on_switch=colors.bright(colors.foreground["cyan"]),
        background_color=colors.background["default"],
        background_on_switch=colors.background["default"],
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


def user_options_prompt(choices, prompt, wrap=True, clear_screen=True, auto_scroll=True):
    if wrap:
        prompt = wrap_text(prompt, 70)
    if auto_scroll and len(choices) > 8:
        scroll_choices = [choice for choice in choices.keys()]
        scroll_choices.insert(0, f"{EMOJI_DICT.get('BACK')} Return to Previous Menu")
        options_menu = ScrollBar(
            prompt,
            choices=scroll_choices,
            height=8,
            pointer="",
            shift=1,
            indent=0,
            margin=2,
            pointer_color=colors.foreground["default"],
            word_color=colors.foreground["default"],
            word_on_switch=colors.bright(colors.foreground["cyan"]),
            background_color=colors.background["default"],
            background_on_switch=colors.background["default"],
        )
    else:
        bullet_choices = [choice for choice in choices.keys()]
        if not auto_scroll:
            bullet_choices.insert(0, f"{EMOJI_DICT.get('BACK')} Return to Previous Menu")
        options_menu = Bullet(
            prompt,
            choices=bullet_choices,
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
    if clear_screen:
        subprocess.run(["clear"])
        print()
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


def audit_report_season_prompt(audit_report, prompt=None):
    if not prompt:
        prompt = "Select an MLB season from the list below:"
    choices = {
        f"{MENU_NUMBERS.get(num)}  {year}": year
        for num, year in enumerate(audit_report.keys(), start=1)
    }
    choices[f"{EMOJI_DICT.get('BACK')} Return to Previous Menu"] = None
    return user_options_prompt(choices, prompt)


def data_sets_prompt(prompt, valid_data_sets=0, checked_data_sets=None):
    data_set_name_map = {v: k for k, v in DATA_SET_NAMES_LONG.items()}
    if not prompt:
        prompt = "Select one or multiple data sets from the list below:"
    if not valid_data_sets:
        valid_data_sets = int(DataSet.ALL)
    instructions = "(use SPACE BAR to select each data set, ENTER to confirm your selections)"
    choices = {
        f"{data_set_name_map[ds]}": ds
        for ds in DataSet
        if ds != DataSet.ALL and valid_data_sets & ds == ds
    }
    if checked_data_sets:
        checked_int = sum(int(ds) for ds in checked_data_sets.keys())
        checked_data_sets = [
            f"{data_set_name_map[ds]}"
            for ds in DataSet
            if ds != DataSet.ALL and checked_int & ds == ds
        ]
    ds_prompt = DataSetCheck(
        prompt=instructions,
        choices=[data_set for data_set in choices.keys()],
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
        print_message(prompt, fg="bright_yellow", bold=True, underline=True)
        result = ds_prompt.launch()
        if not result:
            print_message("\nYou must select at least one file type!", fg="bright_red", bold=True)
            pause(message="Press any key to continue...")
            continue
        data_sets = {DATA_SET_NAMES_LONG[sel]: sel for sel in result}
    return data_sets


def file_types_prompt(prompt, valid_file_types=VigFile.ALL):
    if not prompt:
        prompt = "Select one or multiple file types from the list below:"
    choices = {
        f"{file_type}": file_type
        for file_type in VigFile
        if file_type != VigFile.ALL and int(file_type) & valid_file_types == file_type
    }
    instructions = "(use SPACE BAR to select each file type, ENTER to confirm your selections)"
    file_types_prompt = Check(
        prompt=instructions,
        choices=[file_type for file_type in choices.keys()],
        check=EMOJI_DICT.get("CHECK", ""),
        shift=1,
        indent=0,
        margin=2,
        check_color=colors.foreground["default"],
        check_on_switch=colors.foreground["default"],
        word_color=colors.foreground["default"],
        word_on_switch=colors.bright(colors.foreground["cyan"]),
        background_color=colors.background["default"],
        background_on_switch=colors.background["default"],
    )
    file_types = []
    while not file_types:
        subprocess.run(["clear"])
        print_message(prompt, fg="bright_yellow", bold=True, underline=True)
        result = file_types_prompt.launch()
        if not result:
            print_message("\nYou must select at least one file type!", fg="bright_red", bold=True)
            pause(message="Press any key to continue...")
            continue
        file_types = [choices[sel] for sel in result]
    return file_types


def select_game_prompt(game_ids, prompt=None, use_numbers=True, clear_screen=True):
    if not prompt:
        prompt = "Select a game from the list below:"
    choices = {
        f"{_get_menu_item_emoji(use_numbers, num)}  {game_id}": game_id
        for num, game_id in enumerate(game_ids, start=1)
    }
    choices[f"{EMOJI_DICT.get('BACK')} Return to Main Menu"] = None
    return user_options_prompt(choices, prompt, clear_screen=clear_screen)


def _get_menu_item_emoji(use_numbers, num=None):
    if use_numbers and num:
        return MENU_NUMBERS.get(num, str(num))
    return EMOJI_DICT.get("BLUE_DIAMOND")
