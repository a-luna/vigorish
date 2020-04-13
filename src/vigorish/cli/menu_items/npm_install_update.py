"""Menu item that allows the user to initialize/reset the database."""
import subprocess
from pathlib import Path

from getch import pause

from vigorish.cli.menu_item import MenuItem
from vigorish.cli.util import print_message, prompt_user_yes_no
from vigorish.constants import EMOJI_DICT
from vigorish.util.result import Result
from vigorish.util.string_helpers import wrap_text
from vigorish.util.sys_helpers import node_installed, node_modules_folder_exists, run_command

APP_FOLDER = Path(__file__).parent.parent.parent
NIGHTMAREJS_FOLDER = APP_FOLDER.joinpath("nightmarejs").resolve()
INSTALL_ERROR = "Error! Node.js is not installed, see README for install instructions."
INSTALL_MESSAGE = (
    "Nightmare is not installed, you must install it and other node dependencies in order to "
    "scrape any data."
)
UPDATE_MESSAGE = (
    "Nightmare and all node dependencies are installed. Select YES to check for updates and "
    "install any new versions."
)


class NpmInstallUpdate(MenuItem):
    def __init__(self) -> None:
        self.menu_item_text = "NPM Packages"
        self.menu_item_emoji = EMOJI_DICT.get("BASKET")

    def launch(self) -> Result:
        subprocess.run(["clear"])
        if not node_installed() and not node_installed(exe_name="nodejs"):
            print_message(wrap_text(INSTALL_ERROR, max_len=70), fg="bright_red", bold=True)
            pause(message="Press any key to continue...")
            return
        if node_modules_folder_exists():
            prompt = UPDATE_MESSAGE
            command = "npm update"
        else:
            prompt = INSTALL_MESSAGE
            command = "npm install"
        print_message(wrap_text(prompt, max_len=70))
        result = prompt_user_yes_no("Would you like to continue?")
        yes_response = result.value
        if not yes_response:
            return Result.Ok(self.exit_menu)
        subprocess.run(["clear"])
        exit_code = run_command(command, cwd=str(NIGHTMAREJS_FOLDER))
        if exit_code == 0:
            pause(message="Press any key to continue...")
            return Result.Ok(self.exit_menu)
        error = f"Error! {command} command did not complete successfully."
        Result.Fail(error)
