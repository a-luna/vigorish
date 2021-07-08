"""Reusable menu prompts to get various values/data types from the user."""
import subprocess
from typing import List, Optional

from bullet import Check, colors
from getch import pause

import vigorish.database as db
from vigorish.cli.components.data_set_check import DataSetCheck
from vigorish.cli.components.date_input import DateInput
from vigorish.cli.components.util import (
    create_bullet_prompt,
    create_scrolling_prompt,
    print_error,
    print_heading,
    print_message,
)
from vigorish.constants import DATA_SET_FROM_NAME_MAP, DATA_SET_TO_NAME_MAP, EMOJIS, MENU_NUMBERS
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
    selected = create_bullet_prompt(prompt, list(choices.keys()), wrap, max_line_len).launch()
    return choices.get(selected)


def yes_no_cancel_prompt(prompt, wrap=True, max_line_len=70):
    if wrap:
        prompt = wrap_text(prompt, max_len=max_line_len)
    choices = {
        f"{MENU_NUMBERS.get(1)}  YES": True,
        f"{MENU_NUMBERS.get(2)}  NO": False,
        f"{MENU_NUMBERS.get(3)}  CANCEL": None,
    }
    selected = create_bullet_prompt(prompt, list(choices.keys()), wrap, max_line_len).launch()
    choice_value = choices.get(selected)
    return Result.Fail("") if "CANCEL" in selected else Result.Ok(choice_value)


def single_date_prompt(prompt):
    user_date = None
    while not user_date:
        subprocess.run(["clear"])
        date_prompt = DateInput(prompt=prompt)
        result = date_prompt.launch()
        if result:
            user_date = result
    return user_date


def user_options_prompt(
    choice_map, prompt, wrap=True, clear_screen=True, auto_scroll=True, first_item_is_exit_menu=False
):
    choices = list(choice_map.keys())
    if first_item_is_exit_menu:
        choices.insert(0, f"{EMOJIS.get('BACK')} Return to Previous Menu")
    options_prompt = (
        create_scrolling_prompt(prompt, choices, wrap, max_line_len=70)
        if auto_scroll and len(choices) > 8
        else create_bullet_prompt(prompt, choices, wrap, max_line_len=70)
    )
    if clear_screen:
        subprocess.run(["clear"])
        print()
    selection = options_prompt.launch()
    user_choice = choice_map.get(selection)
    return Result.Ok(user_choice) if user_choice else Result.Fail("")


def season_prompt(db_session, prompt=None, clear_screen=True):
    if not prompt:
        prompt = "Please select a MLB Season from the list below:"
    choices = {
        f"{MENU_NUMBERS.get(num)}  {season.year}": season
        for num, season in enumerate(db.Season.get_all_regular_seasons(db_session), start=1)
    }
    choices[f"{EMOJIS.get('BACK')} Return to Previous Menu"] = None
    return user_options_prompt(choices, prompt=prompt, clear_screen=clear_screen)


def audit_report_season_prompt(audit_report, prompt=None):
    if not prompt:
        prompt = "Select an MLB season from the list below:"
    choices = {f"{MENU_NUMBERS.get(num)}  {year}": year for num, year in enumerate(audit_report.keys(), start=1)}
    choices[f"{EMOJIS.get('BACK')} Return to Previous Menu"] = None
    return user_options_prompt(choices, prompt)


def data_sets_prompt(
    heading: str = None,
    prompt: str = None,
    valid_data_sets: Optional[List[DataSet]] = None,
    checked_data_sets: Optional[List[DataSet]] = None,
):
    if not prompt:
        prompt = "Select one or multiple data sets from the list below:"
    if not valid_data_sets:
        valid_data_sets = [DataSet.ALL]
    instructions = "(use SPACE BAR to select each data set, ENTER to confirm your selections)"
    valid_data_sets_int = sum(int(ds) for ds in valid_data_sets)
    choices = {f"{DATA_SET_TO_NAME_MAP[ds]}": ds for ds in DataSet if valid_data_sets_int & ds == ds}
    if checked_data_sets:
        checked_int = sum(int(ds) for ds in checked_data_sets)
        checked_data_sets = [f"{DATA_SET_TO_NAME_MAP[ds]}" for ds in DataSet if checked_int & ds == ds]
    ds_prompt = DataSetCheck(
        prompt=instructions,
        choices=list(choices.keys()),
        checked_data_sets=checked_data_sets,
        check=EMOJIS.get("CHECK", ""),
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
        if heading:
            print_heading(heading, fg="bright_yellow")
        print_message(prompt, wrap=True)
        result = ds_prompt.launch()
        if not result:
            print_error("\nYou must select at least one data set!")
            pause(message="Press any key to continue...")
            continue
        data_sets = [DATA_SET_FROM_NAME_MAP[sel] for sel in result]
    return data_sets


def multi_season_prompt(db_session, prompt=None, heading=None):
    if not prompt:
        prompt = "Select one or multiple seasons from the list below:"
    all_seasons = db.Season.get_all_regular_seasons(db_session)
    choices = {f"{season.year}": season.year for season in all_seasons}
    instructions = "(use SPACE BAR to select each file type, ENTER to confirm your selections)"
    seasons_prompt = Check(
        prompt=instructions,
        choices=list(choices.keys()),
        check=EMOJIS.get("CHECK", ""),
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
    selected_years = []
    while not selected_years:
        subprocess.run(["clear"])
        if heading:
            print_heading(heading, fg="bright_yellow")
        print_message(prompt, wrap=True)
        result = seasons_prompt.launch()
        if not result:
            print_error("\nYou must select at least one season!")
            pause(message="Press any key to continue...")
            continue
        selected_years = [choices[sel] for sel in result]
    return selected_years


def file_types_prompt(prompt, valid_file_types=VigFile.ALL):
    if not prompt:
        prompt = "Select one or multiple file types from the list below:"
    choices = {f"{f}": f for f in VigFile if int(f) & valid_file_types == f}
    instructions = "(use SPACE BAR to select each file type, ENTER to confirm your selections)"
    file_types_prompt = Check(
        prompt=instructions,
        choices=list(choices.keys()),
        check=EMOJIS.get("CHECK", ""),
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
            print_error("\nYou must select at least one file type!")
            pause(message="Press any key to continue...")
            continue
        file_types = [choices[sel] for sel in result]
    return file_types


def select_game_prompt(game_ids, prompt=None, use_numbers=True, clear_screen=True):
    if not prompt:
        prompt = "Select a game from the list below:"
    choices = {f"{_get_menu_item_emoji(use_numbers, n)}  {gid}": gid for n, gid in enumerate(game_ids, start=1)}
    choices[f"{EMOJIS.get('BACK')} Return to Main Menu"] = None
    return user_options_prompt(choices, prompt, clear_screen=clear_screen, first_item_is_exit_menu=True)


def _get_menu_item_emoji(use_numbers, num=None):
    if use_numbers and num:
        return MENU_NUMBERS.get(num, str(num))
    return EMOJIS.get("BLUE_DIAMOND")
