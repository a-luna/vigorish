"""Menu that allows the user to view and modify all settings in vig.config.json."""
from vigorish.cli.menu import Menu
from vigorish.cli.menu_items.admin_tasks.add_pitchfx_to_database import AddPitchFxToDatabase
from vigorish.cli.menu_items.admin_tasks.backup_database import BackupDatabase
from vigorish.cli.menu_items.admin_tasks.calculate_avg_pitch_times import (
    CalculatePitchTimes,
)
from vigorish.cli.menu_items.admin_tasks.import_scraped_data import ImportScrapedData
from vigorish.cli.menu_items.admin_tasks.npm_install_update import NpmInstallUpdate
from vigorish.cli.menu_items.admin_tasks.restore_database import RestoreDatabase
from vigorish.cli.menu_items.admin_tasks.sync_scraped_data import SyncScrapedData
from vigorish.cli.menu_items.admin_tasks.update_player_id_map import UpdatePlayerIdMap
from vigorish.cli.menu_items.return_to_parent import ReturnToParent
from vigorish.cli.menu_items.setup_db import SetupDatabase
from vigorish.config.database import db_setup_complete
from vigorish.constants import EMOJI_DICT
from vigorish.util.result import Result
from vigorish.util.sys_helpers import node_is_installed


class AdminTasksMenu(Menu):
    def __init__(self, app, audit_report):
        # TODO: Add Refresh Game Data menu option
        # TODO: Add Admin Task menu item to edit season dates and add new seasons
        super().__init__(app)
        self.audit_report = audit_report
        self.menu_text = "Select a task from the list:"
        self.menu_item_text = "Tasks/Admin"
        self.menu_item_emoji = EMOJI_DICT.get("PAGER", "")

    def populate_menu_items(self):
        self.menu_items.clear()
        if node_is_installed():
            self.menu_items.append(NpmInstallUpdate(self.app))
        self.menu_items.append(UpdatePlayerIdMap(self.app))
        self.menu_items.append(SyncScrapedData(self.app))
        if db_setup_complete(self.db_engine, self.db_session):
            self.menu_items.append(ImportScrapedData(self.app))
            self.menu_items.append(AddPitchFxToDatabase(self.app, self.audit_report))
            self.menu_items.append(BackupDatabase(self.app))
            self.menu_items.append(RestoreDatabase(self.app))
            self.menu_items.append(CalculatePitchTimes(self.app))
            self.menu_items.append(SetupDatabase(self.app))
        self.menu_items.append(ReturnToParent(self.app, "Return to Main Menu"))
        # self.menu_items.insert(0, ReturnToParentMenuItem(self.app, "Return to Settings/Admin"))

    def prompt_user_for_menu_selection(self):
        menu = self._get_bullet_menu()
        menu.pos = self.selected_menu_item_pos
        self.selected_menu_item_text = menu.launch()
        result = self.selected_menu_item.launch()
        exit_menu = self.selected_menu_item.exit_menu
        return Result.Ok(exit_menu) if result.success else result
