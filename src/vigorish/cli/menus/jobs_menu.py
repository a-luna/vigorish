"""Menu that allows the user to execute incomplete jobs and view status of completed jobs."""
from vigorish.cli.menu import Menu
from vigorish.cli.menu_items.job_details import JobDetailsMenuItem
from vigorish.cli.menu_items.return_to_parent import ReturnToParentMenuItem
from vigorish.constants import MENU_NUMBERS
from vigorish.util.result import Result


class JobsMenu(Menu):
    def __init__(
        self, db_session, config, scraped_data, jobs, job_status, menu_item_number
    ) -> None:
        self.db_session = db_session
        self.config = config
        self.scraped_data = scraped_data
        self.jobs = jobs
        self.job_status = job_status
        self.menu_text = f"Status = {self.job_status.name.title()} ({len(jobs)} total jobs)"
        self.menu_item_text = f" {self.job_status.name.title()} Jobs ({len(jobs)})"
        self.menu_item_emoji = MENU_NUMBERS.get(menu_item_number, str(menu_item_number))

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
        self.menu_items = [
            JobDetailsMenuItem(
                self.db_session, self.config, self.scraped_data, job, menu_item_number
            )
            for menu_item_number, job in enumerate(self.jobs, start=1)
        ]
        self.menu_items.append(ReturnToParentMenuItem("All Jobs"))
