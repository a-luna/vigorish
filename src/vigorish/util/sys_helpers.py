"""Utility functions that interact with the terminal."""
import os
import platform
import re
import subprocess
from pathlib import Path
from typing import Tuple, Union

from Naked.toolshed.shell import execute_js
from Naked.toolshed.shell import execute as execute_shell_command

from vigorish.util.result import Result


def run_command(command, cwd=None, shell=True, text=True):
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        cwd=cwd,
        shell=shell,
        text=text,
    )
    for line in iter(process.stdout.readline, ""):
        if line:
            line = line[:-1] if line.endswith("\n") else line
            yield line
    print()


def execute_nodejs_script(script_file_path, script_args):
    result = validate_file_path(script_file_path)
    if result.failure:
        return result
    valid_filepath = result.value
    if program_is_installed("node"):
        success = execute_js(str(valid_filepath), arguments=script_args)
    elif program_is_installed("nodejs"):
        success = execute_shell_command(f"nodejs {valid_filepath} {script_args}")
    else:
        return Result.Fail("Node.js is NOT installed!")
    return Result.Ok() if success else Result.Fail("nodejs script failed")


def node_is_installed():
    return any(program_is_installed(node_alias) for node_alias in ["node", "nodejs"])


def program_is_installed(exe_name, version_option="--version"):
    try:
        check_version = subprocess.run(
            [exe_name, version_option],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            encoding="utf-8",
        )
        output = check_version.stdout.strip()
        return True if re.compile(r"[vV]?\d+\.").match(output) else False
    except FileNotFoundError:
        return False


def node_modules_folder_exists():
    app_folder = Path(__file__).parent.parent
    node_modules_folder = app_folder.joinpath("nightmarejs/node_modules")
    return node_modules_folder.exists()


def is_windows():
    return any(platform.win32_ver())


def get_file_size_bytes(filepath):
    result = validate_file_path(filepath)
    if result.failure:
        return result
    valid_filepath = result.value
    return Result.Ok(valid_filepath.stat().st_size)


def validate_folder_path(input_path: Union[Path, str]):
    if not input_path:
        return Result.Fail("NoneType or empty string is not a valid folder path.")
    if isinstance(input_path, str):
        folderpath = Path(input_path)
    elif not isinstance(input_path, Path):
        error = f'"input_path" parameter must be str or Path value (not "{type(input_path)}").'
        return Result.Fail(error)
    else:
        folderpath = input_path
    if not folderpath.exists():
        return Result.Fail(f'Directory does NOT exist: "{folderpath}"')
    if not folderpath.is_dir():
        return Result.Fail(f'The provided path is NOT a directory: "{folderpath}"')
    if is_windows() and folderpath.is_reserved():
        return Result.Fail(f'The provided path is reserved under Windows: "{folderpath}"')
    return Result.Ok(folderpath)


def validate_file_path(input_path: Union[Path, str]):
    if not input_path:
        return Result.Fail("NoneType or empty string is not a valid file path.")
    if isinstance(input_path, str):
        filepath = Path(input_path)
    elif not isinstance(input_path, Path):
        error = f'"input_path" parameter must be str or Path value (not "{type(input_path)}").'
        return Result.Fail(error)
    else:
        filepath = input_path
    if not filepath.exists():
        return Result.Fail(f'File does not exist: "{filepath}"')
    if not filepath.is_file():
        return Result.Fail(f'The provided path is NOT a file: "{filepath}"')
    return Result.Ok(filepath)


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
