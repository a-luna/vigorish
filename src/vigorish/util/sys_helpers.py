"""Utility functions that interact with the terminal."""
import os
import platform
from pathlib import Path
from typing import Tuple, Union

from vigorish.util.result import Result


def is_windows():
    return any(platform.win32_ver())


def validate_folder_path(input_path: Union[Path, str]):
    if not input_path:
        return Result.Fail("NoneType or empty string is not a valid folder path.")
    if isinstance(input_path, str):
        folder_path = Path(input_path).resolve()
    elif not isinstance(input_path, Path):
        error = f'"input_path" parameter must be str or Path value (not "{type(input_path)}").'
        return Result.Fail(error)
    else:
        folder_path = input_path
    if not folder_path.exists():
        return Result.Fail(f'No directory exists at path: "{folder_path}"')
    if not folder_path.is_dir():
        return Result.Fail(f'The provided path is NOT a directory: "{folder_path}"')
    if is_windows() and folder_path.is_reserved():
        return Result.Fail(f'The provided path is reserved under Windows: "{folder_path}"')
    return Result.Ok(folder_path)


def validate_file_path(input):
    if not input:
        return Result.Fail("NoneType or empty string is not a valid file path.")
    file_path = Path(input).resolve()
    if not file_path.exists():
        return Result.Fail(f'File does not exist: "{file_path}"')
    if not file_path.is_file():
        return Result.Fail(f'The provided path is NOT a file: "{file_path}"')
    return Result.Ok(file_path)


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
