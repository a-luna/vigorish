import subprocess

from getch import pause
from halo import Halo

from vigorish.cli.components.models import DisplayPage
from vigorish.cli.components.page_viewer import PageViewer
from vigorish.cli.components.util import (
    get_random_cli_color,
    get_random_dots_spinner,
    print_heading,
    print_message,
)
from vigorish.cli.menu_item import MenuItem
from vigorish.constants import EMOJI_DICT
from vigorish.tasks.backup_database import BackupDatabaseTask
from vigorish.util.result import Result
from vigorish.util.sys_helpers import zip_file_report


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
        self.unsubscribe_from_events()
        self.spinner.stop()
        if result.failure:
            return result
        zip_file = result.value
        subprocess.run(["clear"])
        self.display_zip_file_details(zip_file)
        pause(message="\nPress any key to continue...")
        return Result.Ok(True)

    def prompt_user_run_task(self):
        subprocess.run(["clear"])
        prompt = "Would you like to run this task and backup the database?"
        pages = [DisplayPage(page, None) for page in self.get_task_description_pages()]
        page_viewer = PageViewer(pages, prompt=prompt, text_color="bright_yellow")
        print_heading("Backup Database to CSV", fg="bright_yellow")
        return page_viewer.launch()

    def backup_database_start(self, total_tables):
        subprocess.run(["clear"])
        print_heading("Backup Database to CSV", fg="bright_yellow")
        self.total_tables = total_tables
        self.spinner.start()

    def backup_table_start(self, table_count, table):
        self.table_count = table_count
        self.current_table = table
        self.spinner.text = self.get_spinner_text(0)

    def backup_table_progress(self, percent):
        self.spinner.text = self.get_spinner_text(percent)

    def create_zip_file_start(self):
        self.spinner.text = "Compressing CSV files and adding to ZIP archive..."

    def get_spinner_text(self, percent):
        return (
            f"Exporting {self.current_table.__name__} to CSV... "
            f"(Table {self.table_count}/{self.total_tables}) {percent:.0%}..."
        )

    def display_zip_file_details(self, zip_file):
        print_heading("Backup Database to CSV", fg="bright_yellow", bg=None)
        print_message("Successfully backed up database to .zip file:", bg=None)
        print_message(f"{zip_file}\n", wrap=False)
        print_message("Zipped File Info:\n", fg="bright_cyan", bold=True)
        print_message(zip_file_report(zip_file), fg="bright_cyan", wrap=False)

    def subscribe_to_events(self):
        self.backup_db.events.backup_database_start += self.backup_database_start
        self.backup_db.events.backup_table_start += self.backup_table_start
        self.backup_db.events.backup_table_progress += self.backup_table_progress
        self.backup_db.events.create_zip_file_start += self.create_zip_file_start

    def unsubscribe_from_events(self):
        self.backup_db.events.backup_database_start -= self.backup_database_start
        self.backup_db.events.backup_table_start -= self.backup_table_start
        self.backup_db.events.backup_table_progress -= self.backup_table_progress
        self.backup_db.events.create_zip_file_start -= self.create_zip_file_start

    def get_task_description_pages(self):
        return [
            [
                "This task creates a backup of the database tables listed below:\n",
                "* scrape_status_date",
                "* scrape_status_game",
                "* scrape_status_pitch_app",
                "* pitchfx\n",
                (
                    "After exporting each table as a CSV file, the files are added to a zip "
                    "archive. Then, the zip file is compressed and the CSV files are removed."
                ),
            ],
            [
                (
                    "When resetting/setting up the database, you will be prompted to restore "
                    "these tables if backup files are located based on the value of the "
                    "DB_BACKUP_FOLDER_PATH config setting.\n"
                ),
                (
                    "PLEASE NOTE: The time needed to backup your database depends on the amount "
                    "of data that has been scraped, as well as the compute and memory specs of "
                    "the machine performing the export. For a large database, the time required "
                    "to complete the backup process could take anywhere from 2-10 minutes."
                ),
            ],
        ]
