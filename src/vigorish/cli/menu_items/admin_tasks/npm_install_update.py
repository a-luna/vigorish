"""Menu item that allows the user to initialize/reset the database."""
import subprocess
from tempfile import TemporaryDirectory

from getch import pause
from halo import Halo

from vigorish.cli.components import print_heading, print_message, yes_no_prompt
from vigorish.cli.components.util import get_random_cli_color, get_random_dots_spinner
from vigorish.cli.menu_item import MenuItem
from vigorish.config.project_paths import NIGHTMAREJS_FOLDER, NODEJS_INBOX
from vigorish.constants import EMOJIS
from vigorish.util.result import Result
from vigorish.util.sys_helpers import node_is_installed, node_modules_folder_exists, run_command

INSTALL_ERROR = "Error! Node.js is not installed, see README for install instructions."
INSTALL_MESSAGE = "You must install all required node packages in order to scrape any data."
INSTALL_PROMPT = (
    "\nSelect YES install Nightmare, Xfvb, and other node packages\n" "Select NO to return to the previous menu"
)
UPDATE_MESSAGE = "Nightmare and all node dependencies are installed."
UPDATE_PROMPT = (
    "\nSelect YES to update all installed packages to the newest version.\n" "Select NO to return to the previous menu."
)


class NpmInstallUpdate(MenuItem):
    def __init__(self, app):
        super().__init__(app)
        self.menu_heading = "Update Node Packages" if node_modules_folder_exists() else "Install Node Packages"
        self.menu_item_text = self.menu_heading
        self.menu_item_emoji = EMOJIS.get("PACKAGE")

    def launch(self):
        subprocess.run(["clear"])
        print_heading(self.menu_heading, fg="bright_yellow")
        if not node_is_installed():
            print_message(INSTALL_ERROR, fg="bright_red", bold=True)
            pause(message="Press any key to continue...")
            return
        if not NODEJS_INBOX.exists():
            NODEJS_INBOX.mkdir(parents=True, exist_ok=True)
        if node_modules_folder_exists():
            message = UPDATE_MESSAGE
            prompt = UPDATE_PROMPT
            temp_folder = None
            command = "npm update --timeout=9999999"
        else:
            message = INSTALL_MESSAGE
            prompt = INSTALL_PROMPT
            temp_folder = TemporaryDirectory(dir=NIGHTMAREJS_FOLDER)
            command = f"npm install --timeout=9999999 --cache={temp_folder.name}"
        print_message(message, fg="bright_yellow")
        if not yes_no_prompt(prompt, wrap=False):
            return Result.Ok(self.exit_menu)
        subprocess.run(["clear"])
        print_heading(self.menu_heading, fg="bright_yellow")
        spinner = Halo(spinner=get_random_dots_spinner(), color=get_random_cli_color())
        spinner.text = "Updating node packages..." if node_modules_folder_exists() else "Installing node packages..."
        spinner.start()
        cmd_output = list(run_command(command, cwd=str(NIGHTMAREJS_FOLDER)))

        subprocess.run(["clear"])
        print_heading(self.menu_heading, fg="bright_yellow")
        spinner.succeed(
            "All node packages are up-to-date!"
            if node_modules_folder_exists()
            else "Successfully installed all node dependencies!"
        )
        for s in cmd_output:
            print_message(s)
        if temp_folder:
            temp_folder.cleanup()
        pause(message="\nPress any key to continue...")
        return Result.Ok(self.exit_menu)
