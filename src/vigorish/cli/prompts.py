"""Reusable menu prompts to get various values/data types from the user."""
import subprocess

from bullet import Bullet, Check, ScrollBar, colors
from getch import pause

from vigorish.cli.input_types import DataSetCheck, DateInput
from vigorish.cli.util import print_message
from vigorish.config.database import Season
from vigorish.constants import MENU_NUMBERS, EMOJI_DICT, DATA_SET_NAMES_LONG, BULLET_COLORS
from vigorish.enums import DataSet, VigFile
from vigorish.util.result import Result
from vigorish.util.string_helpers import wrap_text


def prompt_user_yes_no(prompt, wrap=True, max_line_len=70, fg=None):
    if wrap:
        prompt = wrap_text(prompt, max_len=max_line_len)
    word_color = colors.foreground["default"]
    if fg:
        word_color = BULLET_COLORS.get(fg, colors.foreground["default"])
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
        word_color=word_color,
        word_on_switch=colors.bright(colors.foreground["cyan"]),
    )
    choice_text = prompt.launch()
    return choices.get(choice_text)


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
        background_color=colors.foreground["default"],
        background_on_switch=colors.foreground["default"],
        word_color=colors.foreground["default"],
        word_on_switch=colors.bright(colors.foreground["cyan"]),
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


def select_game_prompt(game_ids, prompt=None, prompt_sort_order=False):
    sort_order = "DATE"
    if prompt_sort_order:
        result = prompt_sort_options()
        if result.failure:
            return result
        sort_order = result.value
        if sort_order == "TEAM":
            game_ids.sort()
    if not prompt:
        prompt = "Select a game from the list below:"
    choices = {
        f"{MENU_NUMBERS.get(num, str(num))}  {game_id}": game_id
        for num, game_id in enumerate(game_ids, start=1)
    }
    choices[f"{EMOJI_DICT.get('BACK')} Return to Main Menu"] = None
    return user_options_prompt(choices, prompt)


def prompt_sort_options():
    prompt = "How would you prefer to view the list of game IDs?"
    choices = {
        f"{MENU_NUMBERS.get(1)}  Sort By Date": "DATE",
        f"{MENU_NUMBERS.get(2)}  Sort Alphabetically (By Home Team)": "TEAM",
        f"{EMOJI_DICT.get('BACK')} Return to Previous Menu": None,
    }
    return user_options_prompt(choices, prompt)
