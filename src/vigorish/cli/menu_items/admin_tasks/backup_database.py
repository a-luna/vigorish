import subprocess

from getch import pause
from halo import Halo

from vigorish.cli.components.prompts import yes_no_prompt
from vigorish.cli.components.util import (
    get_random_cli_color,
    get_random_dots_spinner,
    print_message,
    print_success,
)
from vigorish.cli.menu_item import MenuItem
from vigorish.constants import EMOJI_DICT
from vigorish.tasks.backup_database import BackupDatabaseTask
from vigorish.util.result import Result

TASK_DESCRIPTION = [
    "This task exports each table listed below to a CSV file:\n",
    "* scrape_status_date",
    "* scrape_status_game",
    "* scrape_status_pitch_app",
    "* pitchfx\n",
    "When resetting/setting up the database, you will be prompted to restore these tables "
    "if the CSV backup files are located based on the value of the DB_BACKUP_FOLDER_PATH config "
    "setting.\n",
    "PLEASE NOTE: The time needed to backup your database depends on the amount of data that has "
    "been scraped, as well as the compute and memory specs of the machine that is running the "
    "export. For a large database, the time required to complete the backup process should take "
    "anywhere from 2-10 minutes.",
]


class BackupDatabase(MenuItem):
    def __init__(self, app):
        super().__init__(app)
        self.backup_db = BackupDatabaseTask(self.app)
        self.menu_item_text = " Export Database to CSV"
        self.menu_item_emoji = EMOJI_DICT.get("TABBED_FILES")
        self.exit_menu = False
        self.spinner = Halo(spinner=get_random_dots_spinner(), color=get_random_cli_color())
        self.table_count = 0
        self.total_tables = 0
        self.row_count = 0
        self.total_rows = 0

    def launch(self):
        if not self.prompt_user_run_task():
            return Result.Ok(True)
        self.subscribe_to_events()
        result = self.backup_db.execute()
        self.spinner.stop()
        self.unsubscribe_from_events()
        if result.failure:
            return result
        subprocess.run(["clear"])
        print_success("Successfully backed up all necessary database tables!")
        pause(message="\nPress any key to continue...")
        return Result.Ok(True)

    def prompt_user_run_task(self):
        subprocess.run(["clear"])
        for line in TASK_DESCRIPTION:
            print_message(line, fg="bright_yellow")
        return yes_no_prompt("Would you like to run this task?")

    def backup_database_start(self, total_tables):
        subprocess.run(["clear"])
        self.total_tables = total_tables
        self.spinner.start()

    def backup_table_start(self, table_count, table):
        self.table_count = table_count
        self.current_table = table
        self.spinner.text = self.get_spinner_text(0)

    def backup_table_progress(self, percent):
        self.spinner.text = self.get_spinner_text(percent)

    def get_spinner_text(self, percent):
        return (
            f"Exporting {self.current_table.__name__} to CSV... "
            f"(Table {self.table_count}/{self.total_tables}) {percent:.0%}..."
        )

    def subscribe_to_events(self):
        self.backup_db.events.backup_database_start += self.backup_database_start
        self.backup_db.events.backup_table_start += self.backup_table_start
        self.backup_db.events.backup_table_progress += self.backup_table_progress

    def unsubscribe_from_events(self):
        self.backup_db.events.backup_database_start -= self.backup_database_start
        self.backup_db.events.backup_table_start -= self.backup_table_start
        self.backup_db.events.backup_table_progress -= self.backup_table_progress
