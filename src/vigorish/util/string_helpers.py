"""Utility functions that transform and/or produce string values."""
import re
from datetime import datetime
from typing import Union

from fuzzywuzzy import process

from vigorish.util.regex import PITCH_APP_REGEX
from vigorish.util.result import Result


ELLIPSIS = b"\xe2\x80\xa6".decode("utf-8")
WORD_REGEX = re.compile(r"\s?(?P<word>\b\w+\b)\s?")


def fuzzy_match(s, choices):
    (match, score) = process.extractOne(s, choices)
    return dict(best_match=match, score=score)


def ellipsize(input_str: str, max_len: int) -> str:
    if len(input_str) <= max_len:
        return input_str
    trunc = f"{input_str[:max_len - 1]} {ELLIPSIS}"
    for match in reversed([match for match in WORD_REGEX.finditer(input_str)]):
        if match.end("word") > max_len:
            continue
        trunc = f"{input_str[:match.end('word')]} {ELLIPSIS}"
        break
    return trunc


def wrap_text(input_str: str, max_len: int) -> str:
    last_word_boundary: int
    trunc_lines = []
    processing_text = True
    while processing_text:
        if len(input_str) <= max_len:
            trunc_lines.append(input_str)
            processing_text = False
            continue
        for match in WORD_REGEX.finditer(input_str):
            if match.end("word") > max_len:
                break
            last_word_boundary = match.end("word") + 1
        trunc_lines.append(f"{input_str[:last_word_boundary]}".strip())
        input_str = input_str[last_word_boundary:]
    return "\n".join(trunc_lines)


def try_parse_int(input_str: str) -> Union[None, int]:
    try:
        parsed = int(input_str)
        return parsed
    except ValueError:
        return None


def get_brooks_team_id(bbref_team_id: str) -> str:
    bbref_id_to_brooks_id_map = {
        "CHW": "CHA",
        "CHC": "CHN",
        "KCR": "KCA",
        "LAA": "ANA",
        "LAD": "LAN",
        "NYY": "NYA",
        "NYM": "NYN",
        "SDP": "SDN",
        "SFG": "SFN",
        "STL": "SLN",
        "TBR": "TBA",
        "WSN": "WAS",
    }
    return bbref_id_to_brooks_id_map.get(bbref_team_id, bbref_team_id)


def validate_bbref_game_id(input_str):
    if len(input_str) != 12:
        error = (
            f"String is not a valid bbref game id: {input_str}\n" "Reason: len(input_str) != 12"
        )
        return Result.Fail(error)

    home_team_id = input_str[:3]
    year_str = input_str[3:7]
    month_str = input_str[7:9]
    day_str = input_str[9:11]
    game_number_str = input_str[-1]

    try:
        year = int(year_str)
        month = int(month_str)
        day = int(day_str)
        parsed = int(game_number_str)
        if parsed < 2:
            game_number = 1
        else:
            game_number = parsed
    except ValueError as e:
        error = f"Failed to parse int value from bbref_game_id:\n{repr(e)}"
        return Result.Fail(error)

    try:
        game_date = datetime(year, month, day).date()
    except Exception as e:
        error = f"Failed to parse game_date from game_id:\n{repr(e)}"
        return Result.Fail(error)

    game_dict = dict(
        game_id=input_str, game_date=game_date, home_team_id=home_team_id, game_number=game_number,
    )
    return Result.Ok(game_dict)


def validate_bbref_game_id_list(game_ids):
    return [
        validate_bbref_game_id(gid).value
        for gid in game_ids
        if validate_bbref_game_id(gid).success
    ]


def validate_brooks_game_id(input_str):
    if len(input_str) != 30:
        error = (
            f"String is not a valid brooks baseball game id: {input_str}\n"
            "Reason: len(input_str) != 30"
        )
        return Result.Fail(error)

    split = input_str.split("_")
    if len(split) != 7:
        error = (
            f"String is not a valid brooks baseball game id: {input_str}\n"
            "Reason: len(input_str.split('_')) != 7"
        )
        return Result.Fail(error)

    try:
        year = int(split[1])
        month = int(split[2])
        day = int(split[3])
        game_number = int(split[6])
    except ValueError as e:
        error = f"Failed to parse int value from game_id:\n{repr(e)}"
        return Result.Fail(error)

    try:
        game_date = datetime(year, month, day)
    except Exception as e:
        error = f"Failed to parse game_date from game_id:\n{repr(e)}"
        return Result.Fail(error)

    if len(split[4]) != 6:
        error = f"Failed to parse away team id from game_id:\n{repr(e)}"
        return Result.Fail(error)

    if len(split[5]) != 6:
        error = f"Failed to parse home team id from game_id:\n{repr(e)}"
        return Result.Fail(error)

    away_team_id = split[4][:3].upper()
    home_team_id = split[5][:3].upper()

    game_dict = dict(
        game_id=input_str,
        game_date=game_date,
        away_team_id=away_team_id,
        home_team_id=home_team_id,
        game_number=game_number,
    )
    return Result.Ok(game_dict)


def validate_pitch_app_id(pitch_app_id):
    match = PITCH_APP_REGEX.search(pitch_app_id)
    if not match:
        return Result.Fail(f"pitch_app_id: {pitch_app_id} is invalid")
    id_dict = match.groupdict()
    result = validate_bbref_game_id(id_dict["game_id"])
    if result.failure:
        return result
    game_dict = result.value
    game_dict["pitcher_id"] = id_dict["mlb_id"]
    return Result.Ok(game_dict)
