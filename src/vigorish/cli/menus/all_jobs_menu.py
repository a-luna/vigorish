"""Menu that allows the user to view all jobs grouped by status."""
import vigorish.database as db
from vigorish.cli.menu import Menu
from vigorish.cli.menu_items.return_to_parent import ReturnToParent
from vigorish.cli.menus.jobs_menu import JobsMenu
from vigorish.constants import EMOJIS


class AllJobsMenu(Menu):
    def __init__(self, app):
        super().__init__(app)
        self.menu_text = "Jobs are grouped according to their current status:"
        self.menu_item_text = "View All Jobs"
        self.menu_item_emoji = EMOJIS.get("ROBOT", "")

    def populate_menu_items(self):
        jobs_grouped = db.ScrapeJob.get_all_jobs_grouped_sorted(self.db_session)
        self.menu_items = [
            JobsMenu(self.app, jobs_grouped[status], status, menu_number)
            for menu_number, status in enumerate(jobs_grouped.keys(), start=1)
        ]
        self.menu_items.append(ReturnToParent(self.app, "Return to Main Menu"))
