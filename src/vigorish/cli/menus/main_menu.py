"""The main menu for the CLI."""
import subprocess

from halo import Halo
from tabulate import tabulate

from vigorish import __version__
from vigorish.cli.components import (
    get_random_cli_color,
    get_random_dots_spinner,
    print_heading,
    print_message,
)
from vigorish.cli.menu import Menu
from vigorish.cli.menu_items.combine_data import CombineGameDataMenuItem
from vigorish.cli.menu_items.create_job import CreateJobMenuItem
from vigorish.cli.menu_items.exit_program import ExitProgramMenuItem
from vigorish.cli.menu_items.setup_db import SetupDBMenuItem
from vigorish.cli.menu_items.status_report import StatusReportMenuItem
from vigorish.cli.menus.admin_tasks_menu import AdminTasksMenu
from vigorish.cli.menus.all_jobs_menu import AllJobsMenu
from vigorish.cli.menus.scraped_data_errors_menu import ScrapedDataErrorsMenu
from vigorish.cli.menus.scraped_game_data import ScrapedGameDataMenu
from vigorish.cli.menus.settings_menu import SettingsMenu
from vigorish.config.database import db_setup_complete
from vigorish.util.result import Result
from vigorish.util.sys_helpers import node_is_installed, node_modules_folder_exists


class MainMenu(Menu):
    def __init__(self, app):
        super().__init__(app)
        self.initialized = False
        self.audit_report = {}

    @property
    def initial_setup_complete(self):
        return (
            node_is_installed()
            and node_modules_folder_exists()
            and db_setup_complete(self.db_engine, self.db_session)
        )

    @property
    def needs_refresh(self):
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
        self.initialized = True
        while not exit_menu:
            subprocess.run(["clear"])
            if self.needs_refresh:
                self.check_app_status()
            self.populate_menu_items()
            self.display_menu_text()
            result = self.prompt_user_for_menu_selection()
            if result.failure:
                return result
            exit_menu = result.value
        return Result.Ok(exit_menu)

    def check_app_status(self):
        if not db_setup_complete(self.db_engine, self.db_session):
            return
        spinner = Halo(spinner=get_random_dots_spinner(), color=get_random_cli_color())
        spinner.text = "Updating metrics..." if self.initialized else "Initializing..."
        spinner.start()
        self.audit_report = self.scraped_data.get_audit_report()
        spinner.stop()

    def display_menu_text(self):
        print_heading(f"vigorish v{__version__}", fg="bright_cyan")
        if self.audit_report:
            self.display_audit_report()
        elif self.initial_setup_complete:
            print_message("All prerequisites are installed and database is initialized.")
        else:
            self.display_initial_task_status()
        print_message("\nMain Menu:\n", fg="bright_yellow", bold=True, underline=True)

    def display_audit_report(self):
        table_rows = [
            {
                "season": f"MLB {year} ({report['total_games']} games)",
                "scraped": len(report["scraped"]),
                "combined": len(report["successful"]),
                "failed": len(report["failed"])
                + len(report["pfx_error"])
                + len(report["invalid_pfx"]),
            }
            for year, report in self.audit_report.items()
        ]
        print_message(tabulate(table_rows, headers="keys"), wrap=False)

    def display_initial_task_status(self):
        if node_is_installed():
            print_message("Node.js Installed.............: YES", fg="bright_green", bold=True)
        else:
            print_message("Node.js Installed.............: NO", fg="bright_red", bold=True)
        if node_modules_folder_exists():
            print_message("Electron/Nightmare Installed..: YES", fg="bright_green", bold=True)
        else:
            print_message("Electron/Nightmare Installed..: NO", fg="bright_red", bold=True)
        if db_setup_complete(self.db_engine, self.db_session):
            print_message("SQLite DB Initialized.........: YES", fg="bright_green", bold=True)
        else:
            print_message("SQLite DB Initialized.........: NO", fg="bright_red", bold=True)

    def populate_menu_items(self):
        main_menu_items = self.get_menu_items()
        self.menu_items = [menu_item for menu_item in main_menu_items.values()]
        if not db_setup_complete(self.db_engine, self.db_session):
            self.menu_items.remove(main_menu_items["create_job"])
            self.menu_items.remove(main_menu_items["all_jobs"])
            self.menu_items.remove(main_menu_items["status_reports"])
        else:
            self.menu_items.remove(main_menu_items["setup_db"])
        if not self.audit_report:
            self.menu_items.remove(main_menu_items["combine_data"])
        if not self.data_failures_exist():
            self.menu_items.remove(main_menu_items["scraped_data_errors"])
        if not self.games_combined_exist():
            self.menu_items.remove(main_menu_items["scraped_game_data"])

    def prompt_user_for_menu_selection(self):
        menu = self._get_bullet_menu()
        menu.pos = self.selected_menu_item_pos
        self.selected_menu_item_text = menu.launch()
        result = self.selected_menu_item.launch()
        exit_menu = self.selected_menu_item.exit_menu
        return Result.Ok(exit_menu) if result.success else result

    def get_menu_items(self):
        return {
            "setup_db": SetupDBMenuItem(self.app),
            "create_job": CreateJobMenuItem(self.app),
            "all_jobs": AllJobsMenu(self.app),
            "combine_data": CombineGameDataMenuItem(self.app, self.audit_report),
            "scraped_data_errors": ScrapedDataErrorsMenu(self.app, self.audit_report),
            "scraped_game_data": ScrapedGameDataMenu(self.app, self.audit_report),
            "status_reports": StatusReportMenuItem(self.app),
            "settings": SettingsMenu(self.app),
            "admin_tasks": AdminTasksMenu(self.app, self.audit_report),
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
            any(len(audit_report["invalid_pfx"]) > 0 for audit_report in self.audit_report.values())
            if self.audit_report
            else False
        )
        return audit_failures_exist or pfx_errors_exist or invalid_pfx_errors_exist

    def games_combined_exist(self):
        return (
            any(len(audit_report["successful"]) > 0 for audit_report in self.audit_report.values())
            if self.audit_report
            else False
        )
