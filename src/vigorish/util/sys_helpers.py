"""Utility functions that interact with the terminal."""
import hashlib
import os
import platform
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Tuple, Union
from zipfile import ZipFile

from Naked.toolshed.shell import execute as execute_shell_command
from Naked.toolshed.shell import execute_js

from vigorish.cli.components.viewers import DisplayPage, PageViewer
from vigorish.util.datetime_util import dtaware_fromtimestamp
from vigorish.util.dt_format_strings import DT_NAIVE
from vigorish.util.result import Result

ONE_KB = 1000
ONE_MB = ONE_KB * 1000
ONE_GB = ONE_MB * 1000
CHUNK_SIZE = 8192


def run_command(command, cwd=None, shell=True, text=True):  # pragma: no cover
    try:
        subprocess.check_call(command, stdout=sys.stdout, stderr=subprocess.STDOUT, cwd=cwd, shell=shell, text=text)
        return Result.Ok()
    except subprocess.CalledProcessError as e:
        error = (
            f"An error occurred while executing the command below:\n"
            f"\tCommand: {e.cmd} (return code = {e.returncode})"
        )
        if e.stderr:
            error += f"\n\tError: {e.stderr}"
        return Result.Fail(error)


def run_remote_command(user, host, command, cwd=None):
    return run_command(f"ssh {user}@{host} {command}", cwd=cwd)


def execute_nodejs_script(script_file_path, script_args):  # pragma: no cover
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


def node_is_installed():  # pragma: no cover
    return any(program_is_installed(node_alias) for node_alias in ["node", "nodejs"])


def program_is_installed(exe_name, version_option="--version"):  # pragma: no cover
    try:
        check_version = subprocess.run(
            [exe_name, version_option],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            encoding="utf-8",
        )
        output = check_version.stdout.strip()
        return bool(re.compile(r"[vV]?\d+\.").search(output))
    except FileNotFoundError:
        return False


def node_modules_folder_exists():  # pragma: no cover
    app_folder = Path(__file__).parent.parent
    node_modules_folder = app_folder.joinpath("nightmarejs/node_modules")
    return node_modules_folder.exists()


def is_windows():  # pragma: no cover
    return any(platform.win32_ver())


def get_file_size_bytes(filepath):  # pragma: no cover
    result = validate_file_path(filepath)
    if result.failure:
        return result
    valid_filepath = result.value
    return Result.Ok(valid_filepath.stat().st_size)


def get_last_mod_time_utc(filepath):
    return dtaware_fromtimestamp(filepath.stat().st_mtime, use_tz=timezone.utc)


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


def get_terminal_size(fallback: Tuple[int, int] = (80, 24)):  # pragma: no cover
    columns, rows = fallback
    for i in range(3):
        try:
            columns, rows = os.get_terminal_size(i)
        except OSError:
            continue
        break
    return columns, rows


def get_terminal_width():  # pragma: no cover
    columns, _ = get_terminal_size()
    return columns


def zip_file_report(zip_file_path, heading_color=None, text_color=None):
    report = []
    with ZipFile(zip_file_path, "r") as zip:
        zip_original_size = 0
        for file_number, info in enumerate(zip.infolist(), start=1):
            heading = f"##### FILE {file_number}/{len(zip.infolist())} #####"
            zip_original_size += info.file_size
            compression_ratio = 1 - info.compress_size / float(info.file_size)
            (compressed_str, original_str) = align_file_sizes(info.compress_size, info.file_size)
            file_info = [
                f"Filename.......: {Path(info.filename).name}",
                f"Modified.......: {datetime(*info.date_time).strftime(DT_NAIVE)} UTC",
                f"Zipped Size....: {compressed_str} ({compression_ratio:.0%} Reduction)",
                f"Original Size..: {original_str}",
            ]
            report.append(DisplayPage(file_info, heading))
        heading = f"##### ZIP FILE REPORT: {zip_file_path.name} #####"
        zip_compressed_size = zip_file_path.stat().st_size
        compression_ratio = 1 - zip_compressed_size / float(zip_original_size)
        (compressed_str, original_str) = align_file_sizes(zip_compressed_size, zip_original_size)
        zip_file_modified = dtaware_fromtimestamp(zip_file_path.stat().st_mtime, use_tz=timezone.utc)
        file_info = [
            f"Filename.......: {Path(zip_file_path).name}",
            f"Modified.......: {zip_file_modified.strftime(DT_NAIVE)} UTC",
            f"Zipped Size....: {compressed_str} ({compression_ratio:.0%} Reduction)",
            f"Original Size..: {original_str}",
        ]
        report.insert(0, DisplayPage(file_info, heading))
    return PageViewer(
        report,
        prompt="Press Enter to return to the previous menu",
        confirm_only=True,
        heading_color=heading_color,
        text_color=text_color,
    )


def align_file_sizes(compressed_size, original_size):
    compressed_str = file_size_str(compressed_size)
    original_str = file_size_str(original_size)
    if len(original_str) == len(compressed_str):
        return (compressed_str, original_str)
    if len(original_str) > len(compressed_str):
        pad_length = len(original_str) - len(compressed_str)
        compressed_str = f"{' '*pad_length}{compressed_str}"
    else:
        pad_length = len(compressed_str) - len(original_str)
        original_str = f"{' '*pad_length}{original_str}"
    return (compressed_str, original_str)


def file_size_str(size_bytes):
    return (
        f"{size_bytes/float(ONE_GB):.1f} GB"
        if size_bytes > ONE_GB
        else f"{size_bytes/float(ONE_MB):.1f} MB"
        if size_bytes > ONE_MB
        else f"{size_bytes/float(ONE_KB):.1f} KB"
        if size_bytes > ONE_KB
        else f"{size_bytes} bytes"
    )


def hash_all_files_in_folder(folderpath: Path, create_hash_files: bool = True) -> Dict[Path, str]:
    remove_ds_store_file_from_folder(folderpath)
    file_hash_map = {
        file.name: calc_file_hash(file) for file in folderpath.glob("*.*") if not file.name.endswith("md5")
    }
    if create_hash_files:
        for file, md5_hash in file_hash_map.items():
            hash_file = Path(folderpath).joinpath(f"{file}.md5")
            hash_file.write_text(md5_hash)
    return file_hash_map


def remove_ds_store_file_from_folder(folderpath: Path):
    for file in folderpath.glob("*.DS_Store"):
        file.unlink()


def calc_file_hash(filepath: Path) -> str:
    md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        while chunk := f.read(CHUNK_SIZE):
            md5.update(chunk)
    return md5.hexdigest()
