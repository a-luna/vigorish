"""Menu that contains other menus related to configuring/administrating vigorish."""
from vigorish.cli.menu import Menu
from vigorish.cli.menus.config_settings_menu import ConfigSettingsMenu
from vigorish.cli.menus.env_var_settings_menu import EnvVarSettingsMenu
from vigorish.cli.menu_items.s3_bulk_download import BulkDownloadHtmlFromS3
from vigorish.cli.menu_items.npm_install_update import NpmInstallUpdate
from vigorish.cli.menu_items.setup_db import SetupDBMenuItem
from vigorish.cli.menu_items.return_to_parent import ReturnToParentMenuItem
from vigorish.constants import EMOJI_DICT
from vigorish.config.database import db_setup_complete
from vigorish.util.sys_helpers import node_is_installed


class SettingsAdminMenu(Menu):
    def __init__(self, app):
        super().__init__(app)
        self.menu_text = "Admin menus/options:"
        self.menu_item_text = "Settings/Admin"
        self.menu_item_emoji = EMOJI_DICT.get("TOOLS", "")

    def populate_menu_items(self):
        self.menu_items = []
        self.menu_items.append(ConfigSettingsMenu(self.app))
        self.menu_items.append(EnvVarSettingsMenu(self.app))
        self.menu_items.append(BulkDownloadHtmlFromS3(self.app))
        if node_is_installed():
            self.menu_items.append(NpmInstallUpdate(self.app))
        if db_setup_complete(self.db_engine, self.db_session):
            self.menu_items.append(SetupDBMenuItem(self.app))
        self.menu_items.append(ReturnToParentMenuItem(self.app, "Return to Main Menu"))
