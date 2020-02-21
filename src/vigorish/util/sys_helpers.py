"""Utility functions that interact with the terminal."""
import os
import platform
from pathlib import Path
from typing import Tuple


def is_windows():
    return any(platform.win32_ver())


def validate_folder_path(input):
    test_path = Path(input)
    return test_path.exists() and test_path.is_dir()


def validate_file_path(input):
    test_path = Path(input)
    return test_path.exists() and test_path.is_file()


def get_terminal_size(fallback: Tuple[int, int] = (80, 24)):
    for i in range(0, 3):
        try:
            columns, rows = os.get_terminal_size(i)
        except OSError:
            continue
        break
    else:  # use fallback dimensions if loop completes without finding a terminal
        columns, rows = fallback
    return columns, rows


def get_terminal_width():
    columns, _ = get_terminal_size()
    return columns
