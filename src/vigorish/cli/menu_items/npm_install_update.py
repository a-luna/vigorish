"""Menu item that allows the user to initialize/reset the database."""
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory

from getch import pause

from vigorish.cli.menu_item import MenuItem
from vigorish.cli.prompts import prompt_user_yes_no
from vigorish.cli.util import print_message
from vigorish.constants import EMOJI_DICT
from vigorish.util.result import Result
from vigorish.util.sys_helpers import node_is_installed, node_modules_folder_exists, run_command

APP_FOLDER = Path(__file__).parent.parent.parent
NIGHTMAREJS_FOLDER = APP_FOLDER.joinpath("nightmarejs").resolve()
NODEJS_INBOX = NIGHTMAREJS_FOLDER.joinpath("inbox").resolve()
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
    def __init__(self, app):
        super().__init__(app)
        self.menu_item_text = "Node Packages"
        self.menu_item_emoji = EMOJI_DICT.get("PACKAGE")

    def launch(self):
        subprocess.run(["clear"])
        if not node_is_installed():
            print_message(INSTALL_ERROR, fg="bright_red", bold=True)
            pause(message="Press any key to continue...")
            return
        if not NODEJS_INBOX.exists():
            NODEJS_INBOX.mkdir(parents=True, exist_ok=True)
        if node_modules_folder_exists():
            prompt = UPDATE_MESSAGE
            temp_folder = None
            command = "npm update --timeout=9999999"
        else:
            prompt = INSTALL_MESSAGE
            temp_folder = TemporaryDirectory(dir=NIGHTMAREJS_FOLDER)
            command = f"npm install --timeout=9999999 --cache={temp_folder.name}"
        print_message(prompt)
        result = prompt_user_yes_no("Would you like to continue?")
        yes_response = result.value
        if not yes_response:
            return Result.Ok(self.exit_menu)
        subprocess.run(["clear"])
        for line in run_command(command, cwd=str(NIGHTMAREJS_FOLDER)):
            print(line)
        if temp_folder:
            temp_folder.cleanup()
        pause(message="Press any key to continue...")
        return Result.Ok(self.exit_menu)
