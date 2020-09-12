"""Menu that allows the user to view and modify all settings in vig.config.json."""
from vigorish.cli.menu import Menu
from vigorish.cli.menu_items.admin_tasks.import_scraped_data import ImportScrapedDataTask
from vigorish.cli.menu_items.admin_tasks.update_player_id_map import UpdatePlayerIdMap
from vigorish.cli.menu_items.admin_tasks.npm_install_update import NpmInstallUpdate
from vigorish.cli.menu_items.admin_tasks.sync_scraped_data import SyncScrapedData
from vigorish.cli.menu_items.return_to_parent import ReturnToParentMenuItem
from vigorish.cli.menu_items.setup_db import SetupDBMenuItem
from vigorish.constants import EMOJI_DICT
from vigorish.config.database import db_setup_complete
from vigorish.util.sys_helpers import node_is_installed


class AdminTasksMenu(Menu):
    def __init__(self, app):
        super().__init__(app)
        self.menu_text = "Select a task from the list:"
        self.menu_item_text = "Tasks/Admin"
        self.menu_item_emoji = EMOJI_DICT.get("PAGER", "")

    def populate_menu_items(self):
        self.menu_items.clear()
        self.menu_items.append(UpdatePlayerIdMap(self.app))
        self.menu_items.append(SyncScrapedData(self.app))
        self.menu_items.append(ImportScrapedDataTask(self.app))
        if node_is_installed():
            self.menu_items.append(NpmInstallUpdate(self.app))
        if db_setup_complete(self.db_engine, self.db_session):
            self.menu_items.append(SetupDBMenuItem(self.app))
        self.menu_items.append(ReturnToParentMenuItem(self.app, "Return to Settings/Admin"))
        # self.menu_items.insert(0, ReturnToParentMenuItem(self.app, "Return to Settings/Admin"))
