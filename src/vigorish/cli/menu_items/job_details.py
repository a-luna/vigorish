"""Menu item that provides detailed information for a single job."""
import subprocess

from bullet import Bullet, colors
from getch import pause

from vigorish.cli.menu_item import MenuItem
from vigorish.cli.util import print_message
from vigorish.constants import MENU_NUMBERS, EMOJI_DICT
from vigorish.enums import JobGroup, JobStatus
from vigorish.scrape.job_runner import JobRunner
from vigorish.util.list_helpers import report_dict
from vigorish.util.result import Result


class JobDetailsMenuItem(MenuItem):
    def __init__(self, db_session, config, scraped_data, db_job, menu_item_number) -> None:
        self.db_session = db_session
        self.config = config
        self.scraped_data = scraped_data
        self.db_job = db_job
        self.job_group = db_job.group
        self.job_details = self.db_job.job_details
        self.menu_item_text = f"{db_job.name} (Status: {db_job.status.name}, ID: {db_job.id})"
        self.menu_item_emoji = MENU_NUMBERS.get(menu_item_number, f"{menu_item_number}. ")
        self.exit_menu = True

    def launch(self) -> Result:
        subprocess.run(["clear"])
        print_message("*** Job Details ***", fg="bright_yellow", bold=True)
        job_details = report_dict(self.job_details, title="", title_prefix="", title_suffix="")
        print_message(f"{job_details}\n", fg="bright_yellow")
        if self.job_group == JobGroup.ACTIVE or self.job_group == JobGroup.INCOMPLETE:
            result = self.job_options_prompt()
            if result.failure:
                return Result.Ok(self.exit_menu)
            user_choice = result.value
            if user_choice == "RUN":
                job_runner = JobRunner(
                    db_job=self.db_job,
                    db_session=self.db_session,
                    config=self.config,
                    scraped_data=self.scraped_data,
                )
                result = job_runner.execute()
                if result.failure:
                    return result
                return Result.Ok(self.exit_menu)
            if user_choice == "CANCEL":
                self.db_job.status = JobStatus.CANCELLED
                self.db_session.commit()
                subprocess.run(["clear"])
                print_message("Job was successfully cancelled.", fg="bright_red", bold=True)
                pause(message="Press any key to continue...")
                return Result.Ok(self.exit_menu)
        else:
            pause(message="Press any key to continue...")
            return Result.Ok(False)

    def job_options_prompt(self):
        choices = {
            f"{MENU_NUMBERS.get(1)}  Execute Job": "RUN",
            f"{MENU_NUMBERS.get(2)}  Cancel Job": "CANCEL",
            f"{EMOJI_DICT.get('BACK')} Return to Active Jobs": None,
        }
        prompt = Bullet(
            "Current options:",
            choices=[choice for choice in choices.keys()],
            bullet="",
            shift=1,
            indent=2,
            margin=2,
            bullet_color=colors.foreground["default"],
            background_color=colors.foreground["default"],
            background_on_switch=colors.foreground["default"],
            word_color=colors.foreground["default"],
            word_on_switch=colors.bright(colors.foreground["cyan"]),
        )
        choice_text = prompt.launch()
        choice_value = choices.get(choice_text)
        return Result.Ok(choice_value) if choice_value else Result.Fail("")
