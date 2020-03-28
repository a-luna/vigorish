"""Menu that allows the user to view all jobs grouped by status."""
import subprocess

from vigorish.cli.menu import Menu
from vigorish.cli.menus.jobs_menu import JobsMenu
from vigorish.cli.menu_items.return_to_parent import ReturnToParentMenuItem
from vigorish.config.database import ScrapeJob
from vigorish.constants import EMOJI_DICT
from vigorish.util.result import Result


class AllJobsMenu(Menu):
    def __init__(self, db_session, config, scraped_data) -> None:
        self.db_session = db_session
        self.config = config
        self.scraped_data = scraped_data
        self.menu_text = "Jobs are grouped according to their current status:"
        self.menu_item_text = "View All Jobs"
        self.menu_item_emoji = EMOJI_DICT.get("ROBOT", "")

    def launch(self) -> Result:
        exit_menu = False
        result: Result
        while not exit_menu:
            result = super().launch()
            if result.failure:
                return result
            exit_menu = result.value
        return Result.Ok(exit_menu)

    def populate_menu_items(self) -> None:
        jobs_grouped = ScrapeJob.get_all_jobs_grouped_sorted(self.db_session)
        self.menu_items = [
            JobsMenu(
                self.db_session,
                self.config,
                self.scraped_data,
                jobs_grouped[group],
                group,
                menu_number,
            )
            for menu_number, group in enumerate(jobs_grouped.keys(), start=1)
        ]
        self.menu_items.append(ReturnToParentMenuItem("Main Menu"))
