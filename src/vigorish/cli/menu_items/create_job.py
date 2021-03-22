"""Menu item that allows the user to create a record of a new scrape job."""
import subprocess

from vigorish.cli.components import (
    data_sets_prompt,
    DateInput,
    JobNameInput,
    print_heading,
    print_message,
    validate_scrape_dates,
    yes_no_prompt,
)
from vigorish.cli.menu_item import MenuItem
from vigorish.constants import DATA_SET_TO_NAME_MAP, EMOJIS
from vigorish.scrape.job_runner import JobRunner
from vigorish.util.dt_format_strings import DATE_ONLY_2
from vigorish.util.result import Result


class CreateJob(MenuItem):
    def __init__(self, app):
        # TODO: Improve Create Job UX:
        #  - Create new input type that is initialized with a list of valid values
        #  - Order list of values and the selected value can be changed with <- and -> arrow keys
        #  - This input type does not replace the options prompt with numbered items
        #  - This should be used for numbers and dates, like a slider element
        #  - In this menu, it will be used to select start/end dates
        #    - Separate inputs for month and day (user will select year first before data sets)
        super().__init__(app)
        self.menu_item_text = "Create New Job"
        self.menu_item_emoji = EMOJIS.get("KNIFE", "")
        self.menu_heading = self._menu_item_text

    def launch(self):
        job_confirmed = False
        data_sets = []
        start = None
        end = None
        job_name = None
        prompt = "Select all data sets to scrape:"
        while not job_confirmed:
            heading = self.get_menu_heading("Select Data Sets")
            data_sets = data_sets_prompt(heading, prompt, checked_data_sets=data_sets)
            dates_validated = False
            while not dates_validated:
                start = self.prompt_user_scrape_start_date(start)
                end = self.prompt_user_scrape_stop_date(end)
                result = validate_scrape_dates(self.db_session, start, end)
                if result.failure:
                    continue
                dates_validated = True
            job_name = self.prompt_user_job_name(job_name)
            job_confirmed = self.confirm_job_details(data_sets, start, end, job_name)

        new_job = self.app.create_scrape_job(data_sets, start, end, job_name).value
        subprocess.run(["clear"])
        if yes_no_prompt(prompt="\nWould you like to begin executing this job?"):
            job_runner = JobRunner(app=self.app, db_job=new_job)
            result = job_runner.execute()
            if result.failure:
                return result
        return Result.Ok(self.exit_menu)

    def prompt_user_scrape_start_date(self, start_date):
        date_prompt = DateInput(
            prompt="start date: ",
            heading="Enter date to START scraping",
            heading_color="bright_yellow",
            default=start_date,
        )
        return date_prompt.launch()

    def prompt_user_scrape_stop_date(self, stop_date):
        date_prompt = DateInput(
            prompt="stop date: ",
            heading="Enter date to STOP scraping",
            heading_color="bright_yellow",
            default=stop_date,
        )
        return date_prompt.launch()

    def prompt_user_job_name(self, job_name):
        name_prompt = JobNameInput(
            prompt="job name: ",
            heading="Enter a name for this job (Optional)",
            heading_color="bright_yellow",
            message="This value is optional, press ENTER to use the default value",
            default=job_name,
        )
        return name_prompt.launch()

    def confirm_job_details(self, data_sets, start_date, end_date, job_name):
        subprocess.run(["clear"])
        heading = "Confirm job details"
        data_set_space = "\n\t      "
        confirm_job_name = ""
        if job_name:
            confirm_job_name = f"Job Name....: {job_name}\n"
        job_details = (
            f"{confirm_job_name}"
            f"Start date..: {start_date.strftime(DATE_ONLY_2)}\n"
            f"End Date....: {end_date.strftime(DATE_ONLY_2)}\n"
            f"Data Sets...: {data_set_space.join([DATA_SET_TO_NAME_MAP[ds] for ds in data_sets])}"
        )
        print_heading(heading, fg="bright_yellow")
        print_message(job_details, wrap=False, fg="bright_yellow")
        return yes_no_prompt(prompt="\nAre the details above correct?")
