import re
import string
from datetime import datetime

from fuzzywuzzy import process

from app.main.constants import TEAM_ID_DICT
from app.main.util.regex import (
    TIMESTAMP_REGEX,
    PUNCTUATION_REGEX,
    WHITESPACE_REGEX,
    PITCH_APP_REGEX,
)
from app.main.util.result import Result


def string_is_null_or_blank(s: str) -> bool:
    """Check if a string is null or consists entirely of whitespace."""
    return not s or s.isspace()


def fuzzy_match(s, choices):
    (match, score) = process.extractOne(s, choices)
    return dict(best_match=match, score=score)


def normalize(input: str, sub_whitespace: str = "-") -> str:
    output = list(input)
    for punc in reversed([match for match in PUNCTUATION_REGEX.finditer(input)]):
        output = output[: punc.start()] + output[punc.start() + 1 :]
    return WHITESPACE_REGEX.sub(sub_whitespace, "".join(output)).lower().strip()


def parse_timestamp(input):
    if string_is_null_or_blank(input):
        return dict(hour=0, minute=0)
    match = TIMESTAMP_REGEX.search(input)
    if not match:
        return dict(hour=0, minute=0)
    time_dict = match.groupdict()
    return dict(hour=int(time_dict["hour"]), minute=int(time_dict["minute"]))


def parse_total_minutes_from_duration(duration):
    errors = []
    e = "\nError: game_duration string was not in expected format: {} (expected: h:mm)".format(
        duration
    )

    split = duration.split(":")
    if len(split) != 2:
        errors.append(e)
        return errors

    try:
        h = int(split[0])
        m = int(split[1])
        total = h * 60 + m
    except Exception as e:
        errors.append(e)
        return errors
    return total


def validate_bbref_game_id(input_str):
    if len(input_str) != 12:
        error = (
            f"String is not a valid bbref game id: {input_str}\n"
            "Reason: len(input_str) != 12"
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
        game_id=input_str,
        game_date=game_date,
        home_team_id=home_team_id,
        game_number=game_number,
    )
    return Result.Ok(game_dict)


def validate_bbref_game_id_list(game_ids):
    return [
        validate_bbref_game_id(gid).value
        for gid in game_ids
        if validate_bbref_game_id(gid).success
    ]


def validate_bb_game_id(input_str):
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
