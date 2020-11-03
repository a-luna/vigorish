"""Utility functions that transform and/or produce string values."""
import re
from datetime import datetime

from rapidfuzz import process

from vigorish.constants import BB_BR_TEAM_ID_MAP, BR_BB_TEAM_ID_MAP
from vigorish.util.list_helpers import flatten_list2d
from vigorish.util.regex import (
    AT_BAT_ID_REGEX,
    BB_GAME_ID_REGEX,
    BBREF_GAME_ID_REGEX,
    INNING_LABEL_REGEX,
    PITCH_APP_REGEX,
    TIMESTAMP_REGEX,
)
from vigorish.util.result import Result

ELLIPSIS = b"\xe2\x80\xa6".decode("utf-8")
NEWLINE_REGEX = re.compile(r"\n")
WORD_REGEX = re.compile(r"(?:\s|\b)?(?P<word>[\w\S]+)(?:\s|\B)?")


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
    for match in reversed(list(WORD_REGEX.finditer(input_str))):
        if match.end("word") > max_len:
            continue
        trunc = f"{input_str[:match.end('word')]} {ELLIPSIS}"
        break
    return trunc


def wrap_text(input_str, max_len):
    input_strings = [s for s in input_str.split("\n") if s]
    wrapped = flatten_list2d([_wrap_string(s, max_len) for s in input_strings])
    wrapped = _replace_newlines(input_str, wrapped)
    return "\n".join(wrapped)


def _wrap_string(s, max_len):
    substrings = []
    while True:
        if len(s) <= max_len:
            substrings.append(s)
            break
        (wrapped, s) = _word_wrap(s, max_len)
        substrings.append(wrapped)
    return substrings


def _word_wrap(s, max_len):
    last_word_boundary = max_len
    for match in WORD_REGEX.finditer(s):
        if match.end("word") > max_len:
            break
        last_word_boundary = match.end("word") + 1
    wrapped = s[:last_word_boundary]
    s = s[last_word_boundary:]
    return (wrapped.strip(), s.strip())


def _replace_newlines(input_str, wrapped):
    current_index = 0
    for newline_match in list(NEWLINE_REGEX.finditer(input_str)):
        (wrapped, current_index) = _replace_newline(wrapped, current_index, newline_match)
    return wrapped


def _replace_newline(wrapped, current_index, newline_match):
    newline_index = newline_match.start()
    for num, s in enumerate(wrapped, start=1):
        current_index += len(s)
        if current_index < newline_index:
            if num != len(wrapped):
                continue
            wrapped[num - 1] = f"{s}{newline_match.group()}"
            break
        wrapped[num - 1] = f"{s[:newline_index]}{newline_match.group()}{s[newline_index:]}"
        break
    return (wrapped, current_index)


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
        raise ValueError(f"Failed to parse date from input_str ({input_str}):\n{repr(e)}")


def get_brooks_team_id(bbref_team_id):
    return BR_BB_TEAM_ID_MAP.get(bbref_team_id, bbref_team_id)


def get_bbref_team_id(brooks_team_id):
    return BB_BR_TEAM_ID_MAP.get(brooks_team_id, brooks_team_id)


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
    groups = match.groupdict()
    (year, month, day, game_number) = _parse_ints_from_regex_groups(groups)
    try:
        game_date = datetime(year, month, day).date()
    except Exception as e:
        error = f"Failed to parse game_date from game_id:\n{repr(e)}"
        return Result.Fail(error)
    game_dict = {
        "game_id": match[0],
        "game_date": game_date,
        "home_team_id": groups["home_team"],
        "game_number": game_number,
    }
    return Result.Ok(game_dict)


def _parse_ints_from_regex_groups(match):
    year = int(match["year"])
    month = int(match["month"])
    day = int(match["day"])
    parsed = int(match["game_num"])
    if parsed < 2:
        game_number = 1
    else:
        game_number = parsed
    return (year, month, day, game_number)


def validate_bbref_game_id_list(game_ids):
    return [
        validate_bbref_game_id(gid).value for gid in game_ids if validate_bbref_game_id(gid).success
    ]


def validate_brooks_game_id(input_str):
    match = BB_GAME_ID_REGEX.search(input_str)
    if not match:
        raise ValueError(f"String is not a valid bb game id: {input_str}")
    captured = match.groupdict()
    year = int(captured["year"])
    month = int(captured["month"])
    day = int(captured["day"])
    game_number = int(captured["game_num"])

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


def replace_char_with_newlines(input_str, replace):
    if replace not in input_str:
        return input_str
    return "\n".join([s.strip() for s in input_str.split(replace)])


def csv_sanitize(input):
    return input.replace(",", "")
