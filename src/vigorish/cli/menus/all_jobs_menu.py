"""Menu that allows the user to view all jobs grouped by status."""
from vigorish.cli.menu import Menu
from vigorish.cli.menus.jobs_menu import JobsMenu
from vigorish.cli.menu_items.return_to_parent import ReturnToParentMenuItem
from vigorish.config.database import ScrapeJob
from vigorish.constants import EMOJI_DICT


class AllJobsMenu(Menu):
    def __init__(self, app):
        super().__init__(app)
        self.menu_text = "Jobs are grouped according to their current status:"
        self.menu_item_text = "View All Jobs"
        self.menu_item_emoji = EMOJI_DICT.get("ROBOT", "")

    def populate_menu_items(self):
        jobs_grouped = ScrapeJob.get_all_jobs_grouped_sorted(self.db_session)
        self.menu_items = [
            JobsMenu(self.app, jobs_grouped[status], status, menu_number)
            for menu_number, status in enumerate(jobs_grouped.keys(), start=1)
        ]
        self.menu_items.append(ReturnToParentMenuItem(self.app, "Return to Main Menu"))
