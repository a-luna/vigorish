"""Menu that allows the user to execute incomplete jobs and view status of completed jobs."""
from vigorish.cli.menu import Menu
from vigorish.cli.menu_items.job_details import JobDetails
from vigorish.cli.menu_items.return_to_parent import ReturnToParent
from vigorish.constants import MENU_NUMBERS


class JobsMenu(Menu):
    def __init__(self, app, jobs, job_status, menu_item_number):
        super().__init__(app)
        self.jobs = jobs
        self.job_status = job_status
        self.menu_text = f"Status = {self.job_status.name.title()} ({len(jobs)} total jobs)"
        self.menu_item_text = f" {self.job_status.name.title()} Jobs ({len(jobs)})"
        self.menu_item_emoji = MENU_NUMBERS.get(menu_item_number, str(menu_item_number))

    def populate_menu_items(self):
        self.menu_items = [
            JobDetails(self.app, job, menu_item_number)
            for menu_item_number, job in enumerate(self.jobs, start=1)
        ]
        self.menu_items.append(ReturnToParent(self.app, "Return to All Jobs Menu "))
        if len(self.jobs) > 8:
            self.menu_items.insert(0, ReturnToParent(self.app, "Return to All Jobs Menu"))
