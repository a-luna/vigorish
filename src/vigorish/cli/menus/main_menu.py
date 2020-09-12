"""The main menu for the CLI."""
import subprocess

from halo import Halo
from tabulate import tabulate

from vigorish.cli.menu import Menu
from vigorish.cli.menus.all_jobs_menu import AllJobsMenu
from vigorish.cli.menus.admin_tasks_menu import AdminTasksMenu
from vigorish.cli.menus.scraped_data_errors_menu import ScrapedDataErrorsMenu
from vigorish.cli.menus.settings_menu import SettingsMenu
from vigorish.cli.menu_items.combine_data import CombineGameDataMenuItem
from vigorish.cli.menu_items.create_job import CreateJobMenuItem
from vigorish.cli.menu_items.exit_program import ExitProgramMenuItem
from vigorish.cli.menu_items.status_report import StatusReportMenuItem
from vigorish.cli.menu_items.setup_db import SetupDBMenuItem
from vigorish.cli.util import print_message, get_random_cli_color, get_random_dots_spinner
from vigorish.config.database import db_setup_complete
from vigorish.util.result import Result
from vigorish.util.sys_helpers import node_is_installed, node_modules_folder_exists


class MainMenu(Menu):
    def __init__(self, app):
        super().__init__(app)
        self.node_is_installed = False
        self.node_modules_folder_exists = False
        self.db_setup_complete = False
        self.initial_status_check = True
        self.audit_report = {}

    @property
    def initial_setup_complete(self):
        return (
            self.node_is_installed and self.node_modules_folder_exists and self.db_setup_complete
        )

    @property
    def is_refresh_needed(self):
        check_refresh_dict = {
            SetupDBMenuItem: True,
            CombineGameDataMenuItem: True,
            ScrapedDataErrorsMenu: True,
        }
        selected_menu_item = type(self.selected_menu_item)
        return check_refresh_dict.get(selected_menu_item, False)

    def launch(self):
        exit_menu = False
        subprocess.run(["clear"])
        self.check_app_status()
        while not exit_menu:
            subprocess.run(["clear"])
            if self.is_refresh_needed:
                self.check_app_status()
            self.display_menu_text()
            self.populate_menu_items()
            result = self.prompt_user_for_menu_selection()
            if result.failure:
                return result
            exit_menu = result.value
        return Result.Ok(exit_menu)

    def check_app_status(self):
        spinner = Halo(spinner=get_random_dots_spinner(), color=get_random_cli_color())
        spinner.text = "Initializing..." if self.initial_status_check else "Updating metrics..."
        spinner.start()
        self.perform_simple_tasks()
        if self.db_setup_complete:
            self.audit_report = self.scraped_data.get_audit_report()
        spinner.stop()
        self.initial_status_check = False

    def perform_simple_tasks(self):
        self.node_is_installed = node_is_installed()
        self.node_modules_folder_exists = node_modules_folder_exists()
        self.db_setup_complete = db_setup_complete(self.db_engine, self.db_session)

    def display_menu_text(self):
        print_message("Welcome to vigorish!\n", fg="bright_cyan", bold=True)
        self.perform_simple_tasks()
        if self.initial_setup_complete:
            self.display_audit_report()
        else:
            self.display_initial_task_status()
        print_message("\nMain Menu:\n", fg="bright_yellow", bold=True, underline=True)

    def display_audit_report(self):
        if self.audit_report:
            table_rows = []
            for year, report in self.audit_report.items():
                row = {}
                row["season"] = f"MLB {year} ({report['total_games']} games)"
                row["scraped"] = len(report["scraped"])
                row["combined"] = len(report["successful"])
                row["failed"] = (
                    len(report["failed"]) + len(report["pfx_error"]) + len(report["invalid_pfx"])
                )
                table_rows.append(row)
            print_message(tabulate(table_rows, headers="keys"), wrap=False)
        else:
            message = "All prerequisites are installed and database is initialized."
            print_message(message)

    def display_initial_task_status(self):
        if self.node_is_installed:
            print_message("Node.js Installed.............: YES", fg="bright_green", bold=True)
        else:
            print_message("Node.js Installed.............: NO", fg="bright_red", bold=True)
        if self.node_modules_folder_exists:
            print_message("Electron/Nightmare Installed..: YES", fg="bright_green", bold=True)
        else:
            print_message("Electron/Nightmare Installed..: NO", fg="bright_red", bold=True)
        if self.db_setup_complete:
            print_message("SQLite DB Initialized.........: YES", fg="bright_green", bold=True)
        else:
            print_message("SQLite DB Initialized.........: NO", fg="bright_red", bold=True)

    def populate_menu_items(self):
        main_menu_items = self.get_menu_items()
        self.menu_items = [menu_item for menu_item in main_menu_items.values()]
        if not self.db_setup_complete:
            self.menu_items.remove(main_menu_items["create_job"])
            self.menu_items.remove(main_menu_items["all_jobs"])
            self.menu_items.remove(main_menu_items["status_reports"])
        else:
            self.menu_items.remove(main_menu_items["setup_db"])
        if not self.audit_report:
            self.menu_items.remove(main_menu_items["combine_data"])
        if not self.data_failures_exist():
            self.menu_items.remove(main_menu_items["scraped_data_errors"])

    def get_menu_items(self):
        return {
            "setup_db": SetupDBMenuItem(self.app),
            "create_job": CreateJobMenuItem(self.app),
            "all_jobs": AllJobsMenu(self.app),
            "combine_data": CombineGameDataMenuItem(self.app, self.audit_report),
            "scraped_data_errors": ScrapedDataErrorsMenu(self.app, self.audit_report),
            "status_reports": StatusReportMenuItem(self.app),
            "settings": SettingsMenu(self.app),
            "admin_tasks": AdminTasksMenu(self.app),
            "exit_program": ExitProgramMenuItem(self.app),
        }

    def data_failures_exist(self):
        audit_failures_exist = (
            any(len(audit_report["failed"]) > 0 for audit_report in self.audit_report.values())
            if self.audit_report
            else False
        )
        pfx_errors_exist = (
            any(len(audit_report["pfx_error"]) > 0 for audit_report in self.audit_report.values())
            if self.audit_report
            else False
        )
        invalid_pfx_errors_exist = (
            any(
                len(audit_report["invalid_pfx"]) > 0 for audit_report in self.audit_report.values()
            )
            if self.audit_report
            else False
        )
        return audit_failures_exist or pfx_errors_exist or invalid_pfx_errors_exist
