from pathlib import Path

from vigorish.util.sys_helpers import validate_file_path, validate_folder_path


def test_validate_file_path():
    result = validate_file_path(None)
    assert result.failure
    result = validate_file_path(123)
    assert result.failure
    valid_file_path = Path(__file__)
    result = validate_file_path(valid_file_path)
    assert result.success
    valid_folder_path = Path(__file__).parent
    result = validate_file_path(valid_folder_path)
    assert result.failure
    valid_file_path_str = str(valid_file_path)
    result = validate_file_path(valid_file_path_str)
    assert result.success
    valid_folder_path_str = str(valid_folder_path)
    result = validate_file_path(valid_folder_path_str)
    assert result.failure


def test_validate_folder_path():
    result = validate_folder_path(None)
    assert result.failure
    result = validate_folder_path(123)
    assert result.failure
    valid_file_path = Path(__file__)
    result = validate_folder_path(valid_file_path)
    assert result.failure
    valid_folder_path = Path(__file__).parent
    result = validate_folder_path(valid_folder_path)
    assert result.success
    valid_file_path_str = str(valid_file_path)
    result = validate_folder_path(valid_file_path_str)
    assert result.failure
    valid_folder_path_str = str(valid_folder_path)
    result = validate_folder_path(valid_folder_path_str)
    assert result.success
