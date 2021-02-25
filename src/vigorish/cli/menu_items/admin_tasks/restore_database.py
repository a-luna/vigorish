import subprocess
from datetime import datetime, timezone
from pathlib import Path

from getch import pause
from halo import Halo

from vigorish.cli.components.prompts import user_options_prompt
from vigorish.cli.components.util import (
    get_random_cli_color,
    get_random_dots_spinner,
    print_error,
    print_heading,
    shutdown_cli_immediately,
)
from vigorish.cli.components.viewers import DisplayPage, PageViewer
from vigorish.cli.menu_item import MenuItem
from vigorish.constants import EMOJIS, MENU_NUMBERS
from vigorish.tasks import RestoreDatabaseTask
from vigorish.util.datetime_util import format_timedelta_str, get_local_utcoffset
from vigorish.util.dt_format_strings import DT_NAIVE, FILE_TIMESTAMP
from vigorish.util.result import Result


class RestoreDatabase(MenuItem):
    def __init__(self, app):
        super().__init__(app)
        self.menu_item_text = "Restore Database from Backup"
        self.menu_item_emoji = EMOJIS.get("SPIRAL")
        self.exit_menu = False
        self.restore_db = RestoreDatabaseTask(self.app)
        self.backup_start_time = None
        self.spinner = Halo(spinner=get_random_dots_spinner(), color=get_random_cli_color())
        self.backup_folder = self.app.get_current_setting("DB_BACKUP_FOLDER_PATH")
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
        result = self.prompt_user_select_backup_to_restore()
        if result.failure:
            return Result.Ok(True)
        zip_file = result.value
        self.subscribe_to_events()
        result = self.restore_db.execute(zip_file=zip_file)
        self.unsubscribe_from_events()
        if result.failure:
            self.spinner.stop()
            return result
        pause(message="\nPress any key to continue...")
        shutdown_cli_immediately()

    def find_db_backups(self):
        self.backup_zip_files = list(Path(self.backup_folder).glob("*.zip"))
        if not self.backup_zip_files:
            return Result.Fail(f"Error! No backups found in folder:\n{self.backup_folder}")
        self.backup_zip_files.sort(key=lambda x: x.name, reverse=True)
        return Result.Ok()

    def prompt_user_run_task(self):
        subprocess.run(["clear"])
        heading = "Restore Database from Backup"
        prompt = "Would you like to run this task and restore the database?"
        pages = [DisplayPage(page, heading) for page in self.get_task_description_pages()]
        page_viewer = PageViewer(pages, prompt=prompt, text_color="bright_yellow", heading_color="bright_yellow")
        return page_viewer.launch()

    def prompt_user_select_backup_to_restore(self):
        subprocess.run(["clear"])
        print_heading("Restore Database from Backup", fg="bright_yellow")
        choices = {
            f"{MENU_NUMBERS.get(num,num)}  {self.get_backup_time(zip_file.stem)}": zip_file
            for num, zip_file in enumerate(self.backup_zip_files, start=1)
        }
        choices[f"{EMOJIS.get('BACK')} Return to Previous Menu"] = None
        prompt = "Select a backup to restore (time shown is when the backup process was started):"
        return user_options_prompt(choices, prompt, clear_screen=False)

    def get_backup_time(self, timestamp):
        backup_created = datetime.strptime(timestamp, FILE_TIMESTAMP).replace(tzinfo=timezone.utc)
        time_span = format_timedelta_str(datetime.now(timezone.utc) - backup_created, precise=False)
        backup_created_aware = backup_created.astimezone(get_local_utcoffset())
        return f"{backup_created_aware.strftime(DT_NAIVE)} ({time_span} ago)"

    def prepare_database(self):
        subprocess.run(["clear"])
        print_heading("Restore Database from Backup", fg="bright_yellow")
        return self.app.prepare_database_for_restore()

    def unzip_backup_files_start(self):
        subprocess.run(["clear"])
        print_heading("Restore Database from Backup", fg="bright_yellow")
        self.backup_start_time = datetime.now()
        self.spinner.start()
        self.spinner.text = "Unzipping backup CSV files..."

    def unzip_backup_files_complete(self):
        self.spinner.stop()
        self.spinner = None

    def restore_database_start(self):
        subprocess.run(["clear"])
        self.spinner = Halo(spinner=get_random_dots_spinner(), color=get_random_cli_color())
        self.spinner.start()

    def restore_table_start(self, table):
        self.current_table = table
        self.spinner.text = self.get_spinner_text(0)

    def batch_insert_performed(self, batch_count):
        self.spinner.text = self.get_spinner_text(batch_count)

    def get_spinner_text(self, batch_count):
        return f"Restoring {self.current_table.__name__} table ({batch_count} Batches Added)..."

    def restore_database_complete(self):
        elapsed = format_timedelta_str(datetime.now() - self.backup_start_time)
        self.spinner.succeed(f"Successfully restored database from backup! (elapsed: {elapsed})")

    def subscribe_to_events(self):
        self.restore_db.events.unzip_backup_files_start += self.unzip_backup_files_start
        self.restore_db.events.unzip_backup_files_complete += self.unzip_backup_files_complete
        self.restore_db.events.restore_database_start += self.restore_database_start
        self.restore_db.events.restore_table_start += self.restore_table_start
        self.restore_db.events.batch_insert_performed += self.batch_insert_performed
        self.restore_db.events.restore_database_complete += self.restore_database_complete

    def unsubscribe_from_events(self):
        self.restore_db.events.unzip_backup_files_start -= self.unzip_backup_files_start
        self.restore_db.events.restore_table_start -= self.restore_table_start
        self.restore_db.events.batch_insert_performed -= self.batch_insert_performed
        self.restore_db.events.restore_database_complete -= self.restore_database_complete

    def get_task_description_pages(self):
        return [
            [
                (
                    "This task restores database tables that track the progress made scraping all "
                    "data sets for all MLB seasons.\n"
                ),
                ("Backup files are stored in the folder specified by the DB_BACKUP_FOLDER_PATH config setting."),
            ]
        ]
