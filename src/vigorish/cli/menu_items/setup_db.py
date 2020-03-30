"""Menu item that allows the user to initialize/reset the database."""
import subprocess

from getch import pause

from vigorish.cli.menu_item import MenuItem
from vigorish.cli.util import print_message, prompt_user_yes_no
from vigorish.config.database import initialize_database, db_setup_complete
from vigorish.constants import EMOJI_DICT
from vigorish.util.result import Result
from vigorish.util.string_helpers import wrap_text

SETUP_MESSAGE = (
    "Before you can begin scraping data, you must initialize the database with initial player, "
    "team and season data.\n"
)
RESET_MESSAGE = "Would you like to reset the database with initial player, team and season data?"
WARNING = (
    "WARNING! Before the setup process begins, all existing data will be "
    "deleted. This cannot be undone.\n"
)


class SetupDBMenuItem(MenuItem):
    def __init__(self, menu_item_text, db_engine, db_session) -> None:
        self.db_engine = db_engine
        self.db_session = db_session
        self.menu_item_text = menu_item_text
        self.menu_item_emoji = EMOJI_DICT.get("SPIRAL", "")

    def launch(self) -> Result:
        subprocess.run(["clear"])
        if db_setup_complete(self.db_engine, self.db_session):
            print_message(wrap_text(RESET_MESSAGE, max_len=70))
            print_message(wrap_text(WARNING, max_len=70), fg="bright_red", bold=True)
        else:
            print_message(wrap_text(SETUP_MESSAGE, max_len=70))
        result = prompt_user_yes_no("Would you like to continue?")
        did_confirm_yes = result.value
        if not did_confirm_yes:
            return Result.Ok(self.exit_menu)

        subprocess.run(["clear"])
        result = initialize_database(self.db_engine, self.db_session)
        if result.failure:
            return result
        print_message(
            "\nDatabase has been successfully initialized.", fg="bright_green", bold=True
        )
        pause(message="Press any key to continue...")
        return Result.Ok(self.exit_menu)
