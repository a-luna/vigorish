"""Functions that enable reading/writing the config file."""
import json
from pathlib import Path

from vigorish.util.result import Result

APP_ROOT = Path(__file__).parent.parent.parent.parent
CONFIG_FILE = APP_ROOT / "vig.config.json"


def get_config_from_file():
    if not CONFIG_FILE.exists():
        return Result.Fail(f"Configuration file does not exist (${CONFIG_FILE})")
    if not CONFIG_FILE.is_file():
        return Result.Fail(f"Unable to open config file at ${CONFIG_FILE}")
    try:
        return Result.Ok(json.loads(CONFIG_FILE.read_text()))
    except Exception as e:
        error = f"Error: {repr(e)}"
        return Result.Fail(error)
