"""The main menu for the CLI."""
import subprocess

from halo import Halo

from vigorish.cli.menu import Menu
from vigorish.cli.menus.all_jobs_menu import AllJobsMenu
from vigorish.cli.menus.admin_menu import SettingsAdminMenu
from vigorish.cli.menu_items.combine_data import CombineGameDataMenuItem
from vigorish.cli.menu_items.create_job import CreateJobMenuItem
from vigorish.cli.menu_items.exit_program import ExitProgramMenuItem
from vigorish.cli.menu_items.investigate_failures import InvestigateFailuresMenuItem
from vigorish.cli.menu_items.status_report import StatusReportMenuItem
from vigorish.cli.menu_items.setup_db import SetupDBMenuItem
from vigorish.cli.util import print_message, get_random_cli_color
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
            CreateJobMenuItem: True,
            AllJobsMenu: True,
            CombineGameDataMenuItem: True,
            StatusReportMenuItem: False,
            SettingsAdminMenu: False,
        }
        selected_menu_item = type(self.selected_menu_item)
        return check_refresh_dict.get(selected_menu_item, False)

    def get_main_menu(self):
        return {
            "create_job": CreateJobMenuItem(self.app),
            "all_jobs": AllJobsMenu(self.app),
            "combine_data": CombineGameDataMenuItem(self.app, self.audit_report),
            "investigate_failures": InvestigateFailuresMenuItem(self.app, self.audit_report),
            "status_reports": StatusReportMenuItem(self.app),
            "settings_admin": SettingsAdminMenu(self.app),
            "exit_program": ExitProgramMenuItem(self.app),
        }

    def launch(self):
        exit_menu = False
        subprocess.run(["clear"])
        self.check_app_status()
        self.initial_status_check = False
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
        spinner = Halo(spinner="dots3", color=get_random_cli_color())
        spinner.text = "Initializing..." if self.initial_status_check else "Updating metrics..."
        spinner.start()
        self.node_is_installed = node_is_installed()
        self.node_modules_folder_exists = node_modules_folder_exists()
        self.db_setup_complete = db_setup_complete(self.db_engine, self.db_session)
        if self.db_setup_complete:
            self.audit_report = self.scraped_data.get_audit_report()
        spinner.stop()
        spinner.clear()

    def display_menu_text(self):
        print_message("Welcome to vigorish!\n", fg="bright_cyan", bold=True)
        if self.initial_setup_complete:
            self.display_audit_report()
        else:
            self.display_initial_task_status()
        print_message("\nMain Menu:\n", fg="bright_yellow", bold=True, underline=True)

    def display_audit_report(self):
        if self.audit_report:
            for year, report in self.audit_report.items():
                report_str = (
                    f"MLB {year}: {len(report['scraped'])} Scraped, "
                    f"{len(report['successful'])} Combined, {len(report['failed'])} Failed"
                )
                print_message(report_str, wrap=False)
        else:
            message = "All prerequisites are installed and database is initialized."
            print_message(message)

    def display_initial_task_status(self):
        if self.node_is_installed:
            print_message("Node.js Installed........: YES", fg="bright_green", bold=True)
        else:
            print_message("Node.js Installed........: NO", fg="bright_red", bold=True)
        if self.node_modules_folder_exists:
            print_message("Node Packages Installed..: YES", fg="bright_green", bold=True)
        else:
            print_message("Node Packages Installed..: NO", fg="bright_red", bold=True)
        if self.db_setup_complete:
            print_message("SQLite DB Initialized....: YES", fg="bright_green", bold=True)
        else:
            print_message("SQLite DB Initialized....: NO", fg="bright_red", bold=True)

    def populate_menu_items(self):
        main_menu_items = self.get_main_menu()
        self.menu_items = [menu_item for menu_item in main_menu_items.values()]
        if not self.db_setup_complete:
            self.menu_items.remove(main_menu_items["create_job"])
            self.menu_items.remove(main_menu_items["all_jobs"])
            self.menu_items.remove(main_menu_items["status_reports"])
            self.menu_items.insert(0, SetupDBMenuItem(self.app))
        if not self.audit_report:
            self.menu_items.remove(main_menu_items["combine_data"])
            if not self.data_failures_exist():
                self.menu_items.remove(main_menu_items["investigate_failures"])

    def data_failures_exist(self):
        audit_failures_exist = (
            any(len(audit_report["failed"]) > 0 for audit_report in self.audit_report.values())
            if self.audit_report
            else False
        )
        data_errors_exist = (
            any(len(audit_report["data_error"]) > 0 for audit_report in self.audit_report.values())
            if self.audit_report
            else False
        )
        return audit_failures_exist or data_errors_exist
