"""Menu item that allows the user to create a record of a new scrape job."""
import subprocess
from datetime import date

from bullet import Check, Input, VerticalPrompt, colors
from dateutil import parser
from getch import pause

from vigorish.cli.menu_item import MenuItem
from vigorish.cli.util import DateInput, prompt_user_yes_no, print_message
from vigorish.config.database import ScrapeJob, Season
from vigorish.constants import EMOJI_DICT
from vigorish.enums import DataSet
from vigorish.util.dt_format_strings import DATE_ONLY_2
from vigorish.util.result import Result
from vigorish.util.list_helpers import display_dict

DATA_SET_MAP = {
    "bbref.com Daily Games": DataSet.BBREF_GAMES_FOR_DATE,
    "brooksbaseball.com Daily Games": DataSet.BROOKS_GAMES_FOR_DATE,
    "bbref.com Boxscores": DataSet.BBREF_BOXSCORES,
    "brooksbaseball.com Pitch Logs": DataSet.BROOKS_PITCH_LOGS,
    "brooksbaseball.com PitchFx": DataSet.BROOKS_PITCHFX,
}


class CreateJobMenuItem(MenuItem):
    def __init__(self, db_engine, db_session) -> None:
        self.db_engine = db_engine
        self.db_session = db_session
        self.menu_item_text = "New Scrape Job"
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
                result = self.validate_scrape_dates(start_date, end_date)
                if result.failure:
                    continue
                season = result.value
                dates_validated = True
            job_confirmed = self.confirm_job_details(data_sets, start_date, end_date, job_name)
        self.create_new_scrape_job(data_sets, start_date, end_date, season, job_name)

        result = prompt_user_yes_no(prompt="Would you like to begin executing this job?")
        start_now = result.value
        if start_now:
            print_message("Placeholder! RunJob command not implemented.")
        else:
            print_message("Placeholder! ViewJobs command not implemented.")
        pause(message="Press any key to continue...")
        return Result.Ok(new_scrape_job)

    def get_data_sets_to_scrape(self):
        data_sets = []
        while not data_sets:
            subprocess.run(["clear"])
            data_sets_prompt = self.get_data_sets_prompt()
            result = data_sets_prompt.launch()
            if result:
                data_sets = {DATA_SET_MAP[sel]: sel for sel in result}
        return data_sets

    def get_data_sets_prompt(self):
        return Check(
            prompt=(
                "Select all data sets to scrape:\n"
                "(use SPACE BAR to select a data set, ENTER to confirm your selections)\n"
            ),
            check=EMOJI_DICT.get("CHECK", ""),
            choices=[data_set for data_set in DATA_SET_MAP.keys() if data_set != DataSet.ALL],
            margin=2,
            indent=2,
            background_color=colors.background["default"],
            background_on_switch=colors.background["default"],
            word_color=colors.foreground["default"],
            word_on_switch=colors.bright(colors.foreground["magenta"]),
            check_color=colors.foreground["default"],
            check_on_switch=colors.foreground["default"],
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
                Input(prompt="Enter a name for this job: ", pattern=r"^[\w-]+$"),
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

    def validate_scrape_dates(self, start_date, end_date):
        result = Season.validate_date_range(self.db_session, start_date, end_date)
        if result.failure:
            print_message(
                f"The dates you entered are invalid:\n{result.error}\n",
                fg="bright_red",
                bold=True,
            )
            pause(message="Press any key to continue...")
            return Result.Fail("")
        season = result.value
        return Result.Ok(season)

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

    def get_scrape_job_dict(self, selected_data_sets, start_date, end_date, season, job_name):
        scrape_job_dict = {
            "start_date": start_date,
            "end_date": end_date,
            "name": job_name,
            "season_id": season.id,
        }
        for ds in DataSet:
            if ds == DataSet.ALL:
                continue
            if ds in selected_data_sets:
                scrape_job_dict[ds.name.lower()] = True
            else:
                scrape_job_dict[ds.name.lower()] = False
        return scrape_job_dict
