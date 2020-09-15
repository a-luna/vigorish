"""Menu item that allows the user to create a record of a new scrape job."""
import subprocess

from bullet import VerticalPrompt

from vigorish.cli.menu_item import MenuItem
from vigorish.cli.input_types import DEFAULT_JOB_NAME, DateInput, JobNameInput
from vigorish.cli.prompts import prompt_user_yes_no, data_sets_prompt
from vigorish.cli.util import print_message, validate_scrape_dates
from vigorish.config.database import ScrapeJob
from vigorish.constants import EMOJI_DICT
from vigorish.scrape.job_runner import JobRunner
from vigorish.util.dt_format_strings import DATE_ONLY_2
from vigorish.util.result import Result


class CreateJobMenuItem(MenuItem):
    def __init__(self, app):
        super().__init__(app)
        self.menu_item_text = "Create New Job"
        self.menu_item_emoji = EMOJI_DICT.get("KNIFE", "")

    def launch(self):
        job_confirmed = False
        data_sets = {}
        start_date = None
        end_date = None
        job_name = None
        while not job_confirmed:
            data_sets = data_sets_prompt(
                "Select all data sets to scrape:", checked_data_sets=data_sets
            )
            dates_validated = False
            while not dates_validated:
                (start_date, end_date) = self.get_scrape_dates_from_user(start_date, end_date)
                result = validate_scrape_dates(self.db_session, start_date, end_date)
                if result.failure:
                    continue
                season = result.value
                dates_validated = True
            job_name = self.get_job_name_from_user(job_name)
            job_confirmed = self.confirm_job_details(data_sets, start_date, end_date, job_name)

        new_scrape_job = self.create_new_scrape_job(
            data_sets, start_date, end_date, season, job_name
        )
        subprocess.run(["clear"])
        if prompt_user_yes_no(prompt="Would you like to begin executing this job?"):
            job_runner = JobRunner(
                db_job=new_scrape_job,
                db_session=self.db_session,
                config=self.config,
                scraped_data=self.scraped_data,
            )
            result = job_runner.execute()
            if result.failure:
                return result
        return Result.Ok(self.exit_menu)

    def get_scrape_dates_from_user(self, start_date, end_date):
        subprocess.run(["clear"])
        scrape_dates_prompt = VerticalPrompt(
            [
                DateInput(prompt="Enter date to START scraping: ", default=start_date),
                DateInput(prompt="Enter date to STOP scraping: ", default=end_date),
            ],
            spacing=1,
        )
        scrape_dates = scrape_dates_prompt.launch()
        start_date = scrape_dates[0][1]
        end_date = scrape_dates[1][1]
        return (start_date, end_date)

    def get_job_name_from_user(self, job_name):
        job_name = JobNameInput(prompt="Enter a name for this job: ", default=job_name).launch()
        return job_name if job_name != DEFAULT_JOB_NAME else None

    def confirm_job_details(self, data_sets, start_date, end_date, job_name):
        subprocess.run(["clear"])
        data_set_space = "\n\t      "
        confirm_job_name = ""
        if job_name:
            confirm_job_name = f"Job Name....: {job_name}\n"
        job_details = (
            f"{confirm_job_name}"
            f"Start date..: {start_date.strftime(DATE_ONLY_2)}\n"
            f"End Date....: {end_date.strftime(DATE_ONLY_2)}\n"
            f"Data Sets...: {data_set_space.join(data_sets.values())}\n"
        )
        print_message(f"{job_details}\n", wrap=False, fg="bright_yellow")
        return prompt_user_yes_no(prompt="Are the details above correct?")

    def create_new_scrape_job(self, data_sets, start_date, end_date, season, job_name):
        new_job_dict = {
            "data_sets_int": sum(int(ds) for ds in data_sets.keys()),
            "start_date": start_date,
            "end_date": end_date,
            "season_id": season.id,
        }
        new_scrape_job = ScrapeJob(**new_job_dict)
        self.db_session.add(new_scrape_job)
        self.db_session.commit()
        if job_name:
            new_scrape_job.name = job_name
        else:
            new_scrape_job.name = str(new_scrape_job.id)
        self.db_session.commit()
        return new_scrape_job
