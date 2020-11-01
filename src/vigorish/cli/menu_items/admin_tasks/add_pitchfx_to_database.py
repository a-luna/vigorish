import subprocess

from getch import pause
from halo import Halo

from vigorish.cli.components.prompts import user_options_prompt, yes_no_prompt
from vigorish.cli.components.util import (
    get_random_cli_color,
    get_random_dots_spinner,
    print_message,
)
from vigorish.cli.menu_item import MenuItem
from vigorish.constants import EMOJI_DICT, MENU_NUMBERS
from vigorish.tasks.add_pitchfx_to_database import AddPitchFxToDatabase as AddPitchFxToDatabaseTask
from vigorish.util.result import Result


class AddPitchFxToDatabase(MenuItem):
    def __init__(self, app, audit_report):
        super().__init__(app)
        self.audit_report = audit_report
        self.add_pfx_to_db = AddPitchFxToDatabaseTask(self.app)
        self.menu_item_text = "Add PitchFx Data to Database"
        self.menu_item_emoji = EMOJI_DICT.get("BASEBALL")
        self.exit_menu = False
        self.spinner = Halo(spinner=get_random_dots_spinner(), color=get_random_cli_color())
        self.game_ids = []

    @property
    def total_games(self):
        return len(self.game_ids)

    def launch(self):
        if not self.prompt_user_run_task():
            return Result.Ok(True)
        result = self.select_season_prompt()
        if result.failure:
            return Result.Ok()
        year = result.value
        self.initialize_spinner()
        self.subscribe_to_events()
        result = self.add_pfx_to_db.execute(self.audit_report, year)
        self.spinner.stop()
        self.unsubscribe_from_events()
        if result.failure:
            return result
        subprocess.run(["clear"])
        print_message(f"Added PitchFX data for {len(self.game_ids)} games to database!")
        pause(message="\nPress any key to continue...")
        return Result.Ok()

    def prompt_user_run_task(self):
        task_description = [
            "This task identifies all games that fulfill the two requirements below:\n",
            "1. All data sets have been scraped",
            "2. Data was successfully combined (does not need to be error free)\n",
            "PitchFX data for all games that meet these requirements will be added to the "
            "database.",
        ]
        subprocess.run(["clear"])
        for line in task_description:
            print_message(line, fg="bright_yellow")
        return yes_no_prompt("Would you like to run this task?", wrap=False)

    def select_season_prompt(self):
        prompt = "Select an MLB season from the list below:"
        choices = {f"{MENU_NUMBERS.get(1)}  ALL": "ALL"}
        for num, (year, results) in enumerate(self.audit_report.items(), start=2):
            if results["successful"]:
                choices[f"{MENU_NUMBERS.get(num, str(num))}  {year}"] = year
        choices[f"{EMOJI_DICT.get('BACK')} Return to Previous Menu"] = None
        result = user_options_prompt(choices, prompt)
        if result.failure:
            return result
        year = result.value if isinstance(result.value, int) else None
        return Result.Ok(year)

    def initialize_spinner(self):
        subprocess.run(["clear"])
        self.spinner.text = "Preparing to import PitchFx data..."
        self.spinner.start()

    def add_pitchfx_to_db_start(self, year, game_ids):
        self.game_ids = game_ids
        self.spinner.text = self.get_progress_text(0, year, game_ids[0])

    def add_pitchfx_to_db_progress(self, num_complete, year, game_id):
        self.spinner.text = self.get_progress_text(num_complete, year, game_id)

    def get_progress_text(self, num_complete, year, game_id):
        percent = num_complete / float(self.total_games)
        return f"Adding PitchFx for MLB {year} to Database... (Game ID: {game_id}) {percent:.0%}..."

    def subscribe_to_events(self):
        self.add_pfx_to_db.events.add_pitchfx_to_db_start += self.add_pitchfx_to_db_start
        self.add_pfx_to_db.events.add_pitchfx_to_db_progress += self.add_pitchfx_to_db_progress

    def unsubscribe_from_events(self):
        self.add_pfx_to_db.events.add_pitchfx_to_db_start -= self.add_pitchfx_to_db_start
        self.add_pfx_to_db.events.add_pitchfx_to_db_progress -= self.add_pitchfx_to_db_progress
