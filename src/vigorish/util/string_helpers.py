"""Utility functions that transform and/or produce string values."""
import re


def ellipsize(input: str, max_len: int) -> str:
    if len(input) <= max_len:
        return input
    ellipsis = b"\xe2\x80\xa6".decode("utf-8")
    trunc = f"{input[:max_len - 1]}{ellipsis}"
    word_regex = re.compile(r"\s?(?P<word>\b\w+\b)\s?")
    for match in reversed([match for match in word_regex.finditer(input)]):
        if match.end("word") > max_len:
            continue
        trunc = f"{input[:match.end('word')]} {ellipsis}"
        break
    return trunc
