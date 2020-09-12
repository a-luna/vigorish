"""Menu item that allows the user to initialize/reset the database."""
import subprocess
from tempfile import TemporaryDirectory

from getch import pause

from vigorish.cli.menu_item import MenuItem
from vigorish.cli.prompts import prompt_user_yes_no
from vigorish.cli.util import print_message
from vigorish.config.project_paths import NIGHTMAREJS_FOLDER, NODEJS_INBOX
from vigorish.constants import EMOJI_DICT
from vigorish.util.result import Result
from vigorish.util.sys_helpers import node_is_installed, node_modules_folder_exists, run_command

INSTALL_ERROR = "Error! Node.js is not installed, see README for install instructions."
INSTALL_HEADER = "You must install all required node packages in order to scrape any data.\n"
INSTALL_MESSAGE = (
    "Select YES install Nightmare, Xfvb, and other node packages\n"
    "Select NO to return to the previous menu"
)
UPDATE_HEADER = "Nightmare and all node dependencies are installed.\n"
UPDATE_MESSAGE = (
    "Select YES to update all installed packages to the newest version.\n"
    "Select NO to return to the previous menu."
)


class NpmInstallUpdate(MenuItem):
    def __init__(self, app):
        super().__init__(app)
        self.menu_item_text = (
            "Install Node Packages" if not node_is_installed() else "Update Node Packages"
        )
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
            header = UPDATE_HEADER
            prompt = UPDATE_MESSAGE
            temp_folder = None
            command = "npm update --timeout=9999999"
        else:
            header = INSTALL_HEADER
            prompt = INSTALL_MESSAGE
            temp_folder = TemporaryDirectory(dir=NIGHTMAREJS_FOLDER)
            command = f"npm install --timeout=9999999 --cache={temp_folder.name}"
        print_message(header, wrap=False, fg="bright_yellow", bold=True, underline=True)
        if not prompt_user_yes_no(prompt):
            return Result.Ok(self.exit_menu)
        subprocess.run(["clear"])
        for line in run_command(command, cwd=str(NIGHTMAREJS_FOLDER)):
            print(line)
        if temp_folder:
            temp_folder.cleanup()
        pause(message="Press any key to continue...")
        return Result.Ok(self.exit_menu)
