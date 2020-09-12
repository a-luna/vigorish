"""Utility functions that transform and/or produce string values."""
import re
from datetime import datetime

from rapidfuzz import process

from vigorish.constants import TEAM_ID_DICT
from vigorish.util.regex import (
    PITCH_APP_REGEX,
    TIMESTAMP_REGEX,
    BBREF_GAME_ID_REGEX,
    BB_GAME_ID_REGEX,
    AT_BAT_ID_REGEX,
    INNING_LABEL_REGEX,
)
from vigorish.util.result import Result


ELLIPSIS = b"\xe2\x80\xa6".decode("utf-8")
WORD_REGEX = re.compile(r"\s?(?P<word>\b\w+\b)\s?")


def fuzzy_match(query, mapped_choices, score_cutoff=88):
    best_matches = [
        {"match": match, "score": score, "result": result}
        for (match, score, result) in process.extract(
            query, mapped_choices, score_cutoff=score_cutoff
        )
    ]
    return (
        best_matches
        if best_matches
        else [
            {"match": match, "score": score, "result": result}
            for (match, score, result) in process.extract(query, mapped_choices, limit=3)
        ]
    )


def ellipsize(input_str, max_len):
    if len(input_str) <= max_len:
        return input_str
    trunc = f"{input_str[:max_len - 1]} {ELLIPSIS}"
    for match in reversed([match for match in WORD_REGEX.finditer(input_str)]):
        if match.end("word") > max_len:
            continue
        trunc = f"{input_str[:match.end('word')]} {ELLIPSIS}"
        break
    return trunc


def wrap_text(input_str, max_len):
    trunc_lines = []
    last_word_boundary = max_len
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


def try_parse_int(input_str):
    try:
        parsed = int(input_str)
        return parsed
    except ValueError:
        return None


def parse_date(input_str):
    if not input_str:
        raise ValueError("Input string was empty or None")
    if len(input_str) != 8:
        raise ValueError(f"String is not in the expected YYYYMMDD format! (len({input_str}) != 8)")
    year_str = input_str[0:4]
    month_str = input_str[4:6]
    day_str = input_str[6:8]
    try:
        year = int(year_str)
        month = int(month_str)
        day = int(day_str)
        parsed_date = datetime(year, month, day)
        return parsed_date
    except Exception as e:
        error = f"Failed to parse date from input_str ({input_str}):\n{repr(e)}"
        return Result.Fail(error)


def get_brooks_team_id(bbref_team_id):
    return TEAM_ID_DICT.get(bbref_team_id, bbref_team_id)


def parse_timestamp(input):
    if string_is_null_or_blank(input):
        return dict(hour=0, minute=0)
    match = TIMESTAMP_REGEX.search(input)
    if not match:
        return dict(hour=0, minute=0)
    time_dict = match.groupdict()
    return dict(hour=int(time_dict["hour"]), minute=int(time_dict["minute"]))


def string_is_null_or_blank(s):
    """Check if a string is null or consists entirely of whitespace."""
    return not s or s.isspace()


def validate_bbref_game_id(input_str):
    match = BBREF_GAME_ID_REGEX.search(input_str)
    if not match:
        raise ValueError(f"String is not a valid bbref game id: {input_str}")
    captured = match.groupdict()
    try:
        year = int(captured["year"])
        month = int(captured["month"])
        day = int(captured["day"])
        parsed = int(captured["game_num"])
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
    game_dict = {
        "game_id": match[0],
        "game_date": game_date,
        "home_team_id": captured["home_team"],
        "game_number": game_number,
    }
    return Result.Ok(game_dict)


def validate_bbref_game_id_list(game_ids):
    return [
        validate_bbref_game_id(gid).value
        for gid in game_ids
        if validate_bbref_game_id(gid).success
    ]


def validate_brooks_game_id(input_str):
    match = BB_GAME_ID_REGEX.search(input_str)
    if not match:
        raise ValueError(f"String is not a valid bb game id: {input_str}")
    captured = match.groupdict()

    try:
        year = int(captured["year"])
        month = int(captured["month"])
        day = int(captured["day"])
        game_number = int(captured["game_num"])
    except ValueError as e:
        error = f"Failed to parse int value from game_id:\n{repr(e)}"
        return Result.Fail(error)

    try:
        game_date = datetime(year, month, day)
    except Exception as e:
        error = f"Failed to parse game_date from game_id:\n{repr(e)}"
        return Result.Fail(error)

    away_team_id = captured["home_team"].upper()
    home_team_id = captured["away_team"].upper()

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
    pitch_app_id = match[0]
    captured = match.groupdict()
    result = validate_bbref_game_id(pitch_app_id)
    game_dict = result.value
    game_dict["pitch_app_id"] = pitch_app_id
    game_dict["pitcher_id"] = captured["mlb_id"]
    pitch_app_dict = {
        "pitch_app_id": pitch_app_id,
        "game_id": game_dict["game_id"],
        "game_date": game_dict["game_date"],
        "home_team_id": game_dict["home_team_id"],
        "game_number": game_dict["game_number"],
        "pitcher_id": captured["mlb_id"],
    }
    return Result.Ok(pitch_app_dict)


def validate_at_bat_id(at_bat_id):
    match = AT_BAT_ID_REGEX.search(at_bat_id)
    if not match:
        return Result.Fail(f"at_bat_id: {at_bat_id} is invalid")
    at_bat_id = match[0]
    captured = match.groupdict()
    result = validate_bbref_game_id(at_bat_id)
    game_dict = result.value
    away_team_id = (
        captured["batter_team"]
        if game_dict["home_team_id"] == captured["pitcher_team"]
        else captured["pitcher_team"]
    )
    inning_half = "t" if game_dict["home_team_id"] == captured["pitcher_team"] else "b"
    inning_label = f"{inning_half}{captured['inning']}"
    which_half = "TOP" if inning_label[0] == "t" else "BOT"
    inning = int(captured["inning"])
    inning_str = f"{0}{inning}" if inning < 10 else inning
    inning_id = f'{game_dict["game_id"]}_INN_{which_half}{inning_str}'

    at_bat_dict = {
        "at_bat_id": at_bat_id,
        "pitch_app_id": f"{game_dict['game_id']}_{captured['pitcher_mlb_id']}",
        "game_id": game_dict["game_id"],
        "game_date": game_dict["game_date"],
        "game_number": game_dict["game_number"],
        "away_team_id": away_team_id,
        "home_team_id": game_dict["home_team_id"],
        "inning_id": inning_id,
        "inning_label": inning_label,
        "inning": captured["inning"],
        "pitcher_team": captured["pitcher_team"],
        "pitcher_mlb_id": captured["pitcher_mlb_id"],
        "batter_team": captured["batter_team"],
        "batter_mlb_id": captured["batter_mlb_id"],
        "at_bat_num": captured["at_bat_num"],
    }
    return Result.Ok(at_bat_dict)


def parse_pitch_app_details_from_string(input):
    at_bat_dicts = []
    pitch_app_ids = []
    failed_pitch_app_dict = {}
    for match in AT_BAT_ID_REGEX.finditer(input):
        at_bat_data = validate_at_bat_id(match[0]).value
        at_bat_dicts.append(at_bat_data)
        pitch_app_ids.append(at_bat_data["pitch_app_id"])
    for pitch_app_id in list(set(pitch_app_ids)):
        failed_pitch_app_dict[pitch_app_id] = [
            at_bat["at_bat_id"]
            for at_bat in at_bat_dicts
            if at_bat["pitch_app_id"] == pitch_app_id
        ]
    return failed_pitch_app_dict


def get_inning_id_from_at_bat_id(at_bat_id):
    at_bat_dict = validate_at_bat_id(at_bat_id).value
    inning_label = at_bat_dict["inning_label"]
    which_half = "TOP" if inning_label[0] == "t" else "BOT"
    inning = int(at_bat_dict["inning"])
    inning_str = f"{0}{inning}" if inning < 10 else inning
    return f'{at_bat_dict["game_id"]}_INN_{which_half}{inning_str}'


def inning_number_to_string(inning_str):
    match = INNING_LABEL_REGEX.search(inning_str)
    if not match:
        return Result.Fail(f"inning_str: {inning_str} is invalid")
    inn_half = match[1]
    inn_number = match[2]
    inn_half_str = "top" if inn_half == "TOP" else "bottom"
    inning_num_map = {
        "01": "first",
        "02": "second",
        "03": "third",
        "04": "fourth",
        "05": "fifth",
        "06": "sixth",
        "07": "seventh",
        "08": "eighth",
        "09": "ninth",
    }
    inn_num_str = inning_num_map.get(inn_number, f"{inn_number}th")
    return f"{inn_half_str} of the {inn_num_str} inning"
