"""Menu item that allows the user to create a record of a new scrape job."""
import subprocess

from bullet import Check, Input, VerticalPrompt, colors
from getch import pause

from vigorish.cli.menu_item import MenuItem
from vigorish.cli.util import DateInput, prompt_user_yes_no, print_message, validate_scrape_dates
from vigorish.config.database import ScrapeJob, Season
from vigorish.constants import EMOJI_DICT
from vigorish.enums import DataSet
from vigorish.scrape.job_runner import JobRunner
from vigorish.cli.util import DISPLAY_NAME_DICT
from vigorish.util.dt_format_strings import DATE_ONLY_2
from vigorish.util.result import Result
from vigorish.util.regex import JOB_NAME_PATTERN


class CreateJobMenuItem(MenuItem):
    def __init__(self, db_session, config, scraped_data) -> None:
        self.db_session = db_session
        self.config = config
        self.scraped_data = scraped_data
        self.menu_item_text = "Create New Job"
        self.menu_item_emoji = EMOJI_DICT.get("KNIFE", "")

    def launch(self) -> Result:
        job_confirmed = False
        dates_validated = False
        while not job_confirmed:
            data_sets = self.get_data_sets_to_scrape()
            while not dates_validated:
                job_details = self.get_scrape_job_details()
                start_date = job_details[0][1]
                end_date = job_details[1][1]
                job_name = job_details[2][1]
                result = validate_scrape_dates(start_date, end_date)
                if result.failure:
                    continue
                season = result.value
                dates_validated = True
            job_confirmed = self.confirm_job_details(data_sets, start_date, end_date, job_name)

        new_scrape_job = self.create_new_scrape_job(
            data_sets, start_date, end_date, season, job_name
        )
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
        return Result.Ok()

    def get_data_sets_to_scrape(self):
        data_sets = []
        while not data_sets:
            subprocess.run(["clear"])
            data_sets_prompt = self.get_data_sets_prompt()
            result = data_sets_prompt.launch()
            if result:
                data_sets = {DISPLAY_NAME_DICT[sel]: sel for sel in result}
        return data_sets

    def get_data_sets_prompt(self):
        return Check(
            prompt=(
                "Select all data sets to scrape:\n"
                "(use SPACE BAR to select a data set, ENTER to confirm your selections)"
            ),
            check=EMOJI_DICT.get("CHECK", ""),
            choices=[data_set for data_set in DISPLAY_NAME_DICT.keys() if data_set != DataSet.ALL],
            shift=1,
            indent=2,
            margin=2,
            check_color=colors.foreground["default"],
            check_on_switch=colors.foreground["default"],
            background_color=colors.foreground["default"],
            background_on_switch=colors.foreground["default"],
            word_color=colors.foreground["default"],
            word_on_switch=colors.bright(colors.foreground["cyan"]),
        )

    def get_scrape_job_details(self):
        job_details_prompt = self.get_scrape_job_details_prompt()
        return job_details_prompt.launch()

    def get_scrape_job_details_prompt(self):
        subprocess.run(["clear"])
        return VerticalPrompt(
            [
                DateInput(prompt="Enter date to START scraping: "),
                DateInput(prompt="Enter date to STOP scraping: "),
                Input(prompt="Enter a name for this job: ", pattern=JOB_NAME_PATTERN),
            ]
        )

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
        print(
            f"Job Name....: {job_name}\n"
            f"Data Sets...: {', '.join(data_sets.values())}\n"
            f"Start date..: {start_date.strftime(DATE_ONLY_2)}\n"
            f"End Date....: {end_date.strftime(DATE_ONLY_2)}\n"
        )
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
