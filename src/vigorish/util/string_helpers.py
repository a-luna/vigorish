"""Utility functions that transform and/or produce string values."""
import re
from typing import Union


ELLIPSIS = b"\xe2\x80\xa6".decode("utf-8")
WORD_REGEX = re.compile(r"\s?(?P<word>\b\w+\b)\s?")


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
