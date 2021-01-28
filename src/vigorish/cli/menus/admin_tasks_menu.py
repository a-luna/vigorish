"""Menu that allows the user to view and modify all settings in vig.config.json."""
from vigorish.cli.menu import Menu
from vigorish.cli.menu_items import ReturnToParent, SetupDatabase
from vigorish.cli.menu_items.admin_tasks import (
    AddToDatabase,
    BackupDatabase,
    CalculatePitchTimes,
    ImportScrapedData,
    NpmInstallUpdate,
    RestoreDatabase,
    SyncScrapedData,
    UpdatePlayerIdMap,
)
from vigorish.constants import EMOJIS
from vigorish.util.result import Result
from vigorish.util.sys_helpers import node_is_installed


class AdminTasksMenu(Menu):
    def __init__(self, app):
        # TODO: Add Admin Task menu item to edit season dates and add new seasons
        super().__init__(app)
        self.menu_text = "Select a task from the list:"
        self.menu_item_text = "Tasks/Admin"
        self.menu_item_emoji = EMOJIS.get("PAGER", "")

    def populate_menu_items(self):
        self.menu_items.clear()
        if node_is_installed():
            self.menu_items.append(NpmInstallUpdate(self.app))
        self.menu_items.append(UpdatePlayerIdMap(self.app))
        self.menu_items.append(SyncScrapedData(self.app))
        if self.app.db_setup_complete:
            self.menu_items.append(ImportScrapedData(self.app))
            self.menu_items.append(AddToDatabase(self.app))
            self.menu_items.append(BackupDatabase(self.app))
            self.menu_items.append(RestoreDatabase(self.app))
            self.menu_items.append(CalculatePitchTimes(self.app))
            self.menu_items.append(SetupDatabase(self.app))
        self.menu_items.append(ReturnToParent(self.app, "Return to Main Menu"))
        self.menu_items.insert(0, ReturnToParent(self.app, "Return to Main Menu"))

    def prompt_user_for_menu_selection(self):
        menu = self._get_bullet_menu()
        menu.pos = self.selected_menu_item_pos
        self.selected_menu_item_text = menu.launch()
        result = self.selected_menu_item.launch()
        exit_menu = self.selected_menu_item.exit_menu
        return Result.Ok(exit_menu) if result.success else result
