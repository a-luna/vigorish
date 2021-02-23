"""The main menu for the CLI."""
import subprocess

from halo import Halo
from pyfiglet import Figlet
from tabulate import tabulate

from vigorish import __version__
from vigorish.cli.components import (
    get_random_cli_color,
    get_random_dots_spinner,
    get_random_figlet_font,
    print_heading,
    print_message,
)
from vigorish.cli.menu import Menu
from vigorish.cli.menu_items import (
    CombineScrapedData,
    CreateJob,
    ExitProgram,
    SetupDatabase,
    StatusReport,
)
from vigorish.cli.menu_items.admin_tasks import NpmInstallUpdate, RestoreDatabase
from vigorish.cli.menus import (
    AdminTasksMenu,
    AllJobsMenu,
    ScrapedDataErrorsMenu,
    SettingsMenu,
    ViewGameDataMenu,
)
from vigorish.util.result import Result
from vigorish.util.sys_helpers import node_is_installed, node_modules_folder_exists


class MainMenu(Menu):
    def __init__(self, app):
        super().__init__(app)
        self.initialized = False
        self.audit_report = None

    @property
    def db_setup_complete(self):
        return self.app.db_setup_complete

    @property
    def initial_setup_complete(self):
        return node_is_installed() and node_modules_folder_exists() and self.db_setup_complete

    @property
    def needs_refresh(self):
        check_refresh_dict = {
            SetupDatabase: True,
            CreateJob: True,
            AllJobsMenu: True,
            CombineScrapedData: True,
            ScrapedDataErrorsMenu: True,
            AdminTasksMenu: True,
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
        if not self.db_setup_complete:
            return
        color = get_random_cli_color()
        if not self.initialized:
            f = Figlet(font=get_random_figlet_font(), width=120)
            print_message(f.renderText("vigorish"), wrap=False, fg=f"bright_{color}")
        spinner = Halo(spinner=get_random_dots_spinner(), color=color)
        spinner.text = "Updating metrics..." if self.initialized else "Loading..."
        spinner.start()
        if self.initialized:
            del self.app.audit_report
        self.audit_report = self.app.audit_report
        spinner.stop()

    def display_menu_text(self):
        print_heading(f"vigorish v{__version__}", fg="bright_yellow")
        if self.audit_report:
            self.display_audit_report()
        elif self.initial_setup_complete:
            print_message("All prerequisites are installed and database is initialized.")
        else:
            self.display_initial_task_status()
        print_message("\nMain Menu:\n", fg="bright_green", bold=True, underline=True)

    def display_audit_report(self):
        table_rows = [
            {
                "season": f"MLB {year} ({report['total_games']} games)",
                "scraped": len(report["scraped"]),
                "combined": len(report["successful"]),
                "failed": len(report["failed"]) + len(report["pfx_error"]) + len(report["invalid_pfx"]),
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
        if self.db_setup_complete:
            print_message("SQLite DB Initialized.........: YES", fg="bright_green", bold=True)
        else:
            print_message("SQLite DB Initialized.........: NO", fg="bright_red", bold=True)

    def populate_menu_items(self):
        main_menu_items = self.get_menu_items()
        self.menu_items = list(main_menu_items.values())
        if node_modules_folder_exists():
            self.menu_items.remove(main_menu_items["npm_install"])
        if not self.db_setup_complete:
            self.menu_items.remove(main_menu_items["create_job"])
            self.menu_items.remove(main_menu_items["all_jobs"])
            self.menu_items.remove(main_menu_items["status_reports"])
        else:
            self.menu_items.remove(main_menu_items["setup_db"])
            self.menu_items.remove(main_menu_items["restore_db"])
        if not self.audit_report:
            self.menu_items.remove(main_menu_items["combine_data"])
        if self.audit_report and not self.data_failures_exist():
            self.menu_items.remove(main_menu_items["scraped_data_errors"])
        if self.audit_report and not self.games_combined_exist():
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
            "npm_install": NpmInstallUpdate(self.app),
            "setup_db": SetupDatabase(self.app),
            "restore_db": RestoreDatabase(self.app),
            "create_job": CreateJob(self.app),
            "all_jobs": AllJobsMenu(self.app),
            "combine_data": CombineScrapedData(self.app),
            "scraped_data_errors": ScrapedDataErrorsMenu(self.app),
            "scraped_game_data": ViewGameDataMenu(self.app),
            "status_reports": StatusReport(self.app),
            "settings": SettingsMenu(self.app),
            "admin_tasks": AdminTasksMenu(self.app),
            "exit_program": ExitProgram(self.app),
        }

    def data_failures_exist(self):
        audit_failures_exist = any(audit["failed"] for audit in self.audit_report.values())
        pfx_errors_exist = any(audit["pfx_error"] for audit in self.audit_report.values())
        invalid_pfx_errors_exist = any(audit["invalid_pfx"] for audit in self.audit_report.values())
        return audit_failures_exist or pfx_errors_exist or invalid_pfx_errors_exist

    def games_combined_exist(self):
        return any(audit["successful"] for audit in self.audit_report.values())
