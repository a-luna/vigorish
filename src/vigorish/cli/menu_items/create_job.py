"""Menu item that allows the user to create a record of a new scrape job."""
import subprocess

from bullet import Input, VerticalPrompt

from vigorish.cli.menu_item import MenuItem
from vigorish.cli.util import (
    DateInput,
    prompt_user_yes_no,
    validate_scrape_dates,
    print_message,
    data_sets_prompt,
)
from vigorish.config.database import ScrapeJob
from vigorish.constants import EMOJI_DICT
from vigorish.enums import DataSet
from vigorish.scrape.job_runner import JobRunner
from vigorish.util.dt_format_strings import DATE_ONLY_2
from vigorish.util.result import Result
from vigorish.util.regex import JOB_NAME_PATTERN


class CreateJobMenuItem(MenuItem):
    def __init__(self, app):
        super().__init__(app)
        self.menu_item_text = "Create New Job"
        self.menu_item_emoji = EMOJI_DICT.get("KNIFE", "")

    def launch(self):
        job_confirmed = False
        while not job_confirmed:
            data_sets = data_sets_prompt("Select all data sets to scrape:")
            dates_validated = False
            while not dates_validated:
                job_details = self.get_scrape_dates_from_user()
                start_date = job_details[0][1]
                end_date = job_details[1][1]
                result = validate_scrape_dates(self.db_session, start_date, end_date)
                if result.failure:
                    continue
                season = result.value
                dates_validated = True
            job_name = self.get_job_name_from_user()
            job_confirmed = self.confirm_job_details(data_sets, start_date, end_date, job_name)

        new_scrape_job = self.create_new_scrape_job(
            data_sets, start_date, end_date, season, job_name
        )
        subprocess.run(["clear"])
        result = prompt_user_yes_no(prompt="Would you like to begin executing this job?")
        start_now = result.value
        if start_now:
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

    def get_scrape_dates_from_user(self):
        subprocess.run(["clear"])
        job_details_prompt = VerticalPrompt(
            [
                DateInput(prompt="Enter date to START scraping: "),
                DateInput(prompt="Enter date to STOP scraping: "),
            ]
        )
        return job_details_prompt.launch()

    def get_date_from_user(self, prompt):
        user_date = None
        while not user_date:
            date_prompt = DateInput(prompt=prompt)
            result = date_prompt.launch()
            if result:
                user_date = result
        return user_date

    def confirm_job_details(self, data_sets, start_date, end_date, job_name):
        subprocess.run(["clear"])
        data_set_space = "\n\t      "
        job_details = (
            f"Job Name....: {job_name}\n"
            f"Start date..: {start_date.strftime(DATE_ONLY_2)}\n"
            f"End Date....: {end_date.strftime(DATE_ONLY_2)}\n"
            f"Data Sets...: {data_set_space.join(data_sets.values())}\n"
        )
        print_message(f"{job_details}\n", fg="bright_yellow")
        result = prompt_user_yes_no(prompt="Are the details above correct?")
        return result.value

    def create_new_scrape_job(self, data_sets, start_date, end_date, season, job_name):
        scrape_job_dict = self.get_scrape_job_dict(
            selected_data_sets=data_sets.keys(),
            start_date=start_date,
            end_date=end_date,
            season=season,
            job_name=job_name,
        )
        new_scrape_job = ScrapeJob(**scrape_job_dict)
        self.db_session.add(new_scrape_job)
        self.db_session.commit()
        return new_scrape_job

    def get_scrape_job_dict(self, selected_data_sets, start_date, end_date, season, job_name):
        scrape_job_dict = {
            "start_date": start_date,
            "end_date": end_date,
            "name": job_name,
            "season_id": season.id,
        }
        ds_int = 0
        for ds in DataSet:
            if ds == DataSet.ALL:
                continue
            if ds in selected_data_sets:
                ds_int += int(ds)
        scrape_job_dict["data_sets_int"] = ds_int
        return scrape_job_dict
