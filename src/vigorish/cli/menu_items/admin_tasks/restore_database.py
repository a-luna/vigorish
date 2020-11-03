from datetime import datetime
from pathlib import Path
import subprocess

from getch import pause
from halo import Halo

from vigorish.cli.components.models import DisplayPage
from vigorish.cli.components.page_viewer import PageViewer
from vigorish.cli.components.prompts import user_options_prompt
from vigorish.cli.components.util import (
    get_random_cli_color,
    get_random_dots_spinner,
    print_error,
    print_heading,
)
from vigorish.cli.menu_item import MenuItem
from vigorish.config.database import prepare_database_for_restore
from vigorish.constants import EMOJI_DICT, MENU_NUMBERS
from vigorish.enums import DataSet
from vigorish.tasks.restore_database import RestoreDatabaseTask
from vigorish.util.dt_format_strings import FILE_TIMESTAMP
from vigorish.util.result import Result


class RestoreDatabase(MenuItem):
    def __init__(self, app):
        super().__init__(app)
        self.menu_item_text = "Restore Database from Backup"
        self.menu_item_emoji = EMOJI_DICT.get("SPIRAL")
        self.exit_menu = False
        self.spinner = Halo(spinner=get_random_dots_spinner(), color=get_random_cli_color())
        backup_folder_setting = self.config.all_settings.get("DB_BACKUP_FOLDER_PATH")
        self.backup_folder = backup_folder_setting.current_setting(DataSet.ALL).resolve()
        self.backup_zip_files = []
        self.table_count = 0
        self.total_tables = 0
        self.row_count = 0
        self.total_rows = 0

    def launch(self):
        result = self.find_db_backups()
        if result.failure:
            print_error(result.error)
            pause(message="\nPress any key to continue...")
            return Result.Ok(True)
        if not self.prompt_user_run_task():
            return Result.Ok(True)
        if not self.prompt_user_select_backup_to_restore():
            return Result.Ok(True)
        zip_file = result.value
        self.subscribe_to_events()
        result = prepare_database_for_restore(self.app)
        if result.failure:
            return result
        self.app = result.value
        self.restore_db = RestoreDatabaseTask(self.app)
        result = self.restore_db.execute(zip_file)
        self.unsubscribe_from_events()
        self.spinner.stop()
        if result.failure:
            return result
        pause(message="\nPress any key to continue...")
        return Result.Ok(True)

    def prompt_user_run_task(self):
        subprocess.run(["clear"])
        heading = "Restore Database from Backup"
        prompt = "Would you like to run this task and restore the database?"
        pages = [DisplayPage(page, heading) for page in self.get_task_description_pages()]
        page_viewer = PageViewer(
            pages, prompt=prompt, text_color="bright_yellow", heading_color="bright_yellow"
        )
        return page_viewer.launch()

    def find_db_backups(self):
        backup_zip_files = [zip_file for zip_file in Path(self.backup_folder).glob("*.zip")]
        if not backup_zip_files:
            return Result.Fail(f"Error! No backups found in folder:\n{self.backup_folder}")
        self.backup_zip_files.sort(key=lambda x: x.name, reverse=True)
        return Result.Ok()

    def prompt_user_select_backup_to_restore(self):
        subprocess.run(["clear"])
        print_heading("Restore Database from Backup", fg="bright_yellow")
        choices = {
            f"{MENU_NUMBERS.get(num,num)}  {self.get_backup_time(zip_file.name)}": zip_file
            for num, zip_file in enumerate(self.backup_zip_files)
        }
        choices[f"{EMOJI_DICT.get('BACK')} Return to Previous Menu"] = None
        prompt = "Select a backup to restore (most recent is first):"
        return user_options_prompt(choices, prompt, clear_screen=False)

    def get_backup_time(self, timestamp):
        return datetime.strptime(timestamp, FILE_TIMESTAMP)

    def unzip_backup_file_start(self):
        subprocess.run(["clear"])
        print_heading("Restore Database from Backup", fg="bright_yellow")
        self.spinner.start()
        self.spinner.text = "Unzipping backup CSV files..."

    def restore_table_start(self, table):
        self.current_table = table
        self.spinner.text = self.get_spinner_text(0)

    def restore_table_progress(self, percent):
        self.spinner.text = self.get_spinner_text(percent)

    def get_spinner_text(self, percent):
        return f"Restoring {self.current_table.__name__} table... {percent:.0%}..."

    def subscribe_to_events(self):
        self.restore_db.events.unzip_backup_file_start += self.unzip_backup_file_start
        self.restore_db.events.restore_table_start += self.restore_table_start
        self.restore_db.events.restore_table_progress += self.restore_table_progress

    def unsubscribe_from_events(self):
        self.restore_db.events.unzip_backup_file_start -= self.unzip_backup_file_start
        self.restore_db.events.restore_table_start -= self.restore_table_start
        self.restore_db.events.restore_table_progress -= self.restore_table_progress

    def get_task_description_pages(self):
        return [
            [
                (
                    "This task restores database tables that track the progress made scraping all "
                    "data sets for all MLB seasons.\n"
                ),
                (
                    "Backup files are stored in the location specified by the DB_BACKUP_FOLDER_PATH "
                    "config setting."
                ),
            ]
        ]
