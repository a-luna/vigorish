"""Utility functions that transform and/or produce string values."""
import re


ELLIPSIS = b"\xe2\x80\xa6".decode("utf-8")
WORD_REGEX = re.compile(r"\s?(?P<word>\b\w+\b)\s?")


def ellipsize(input: str, max_len: int) -> str:
    if len(input) <= max_len:
        return input
    trunc = f"{input[:max_len - 1]} {ELLIPSIS}"
    for match in reversed([match for match in WORD_REGEX.finditer(input)]):
        if match.end("word") > max_len:
            continue
        trunc = f"{input[:match.end('word')]} {ELLIPSIS}"
        break
    return trunc


def wrap_text(input: str, max_len: int) -> str:
    last_word_boundary: int
    trunc_lines = []
    processing_text = True
    while processing_text:
        if len(input) <= max_len:
            trunc_lines.append(input)
            processing_text = False
            continue
        for match in WORD_REGEX.finditer(input):
            if match.end("word") > max_len:
                break
            last_word_boundary = match.end("word") + 1
        trunc_lines.append(f"{input[:last_word_boundary]}".strip())
        input = input[last_word_boundary:]
    return "\n".join(trunc_lines)
