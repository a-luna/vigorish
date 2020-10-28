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
from vigorish.tasks.add_pitchfx_to_database import (
    AddPitchFxToDatabase as AddPitchFxToDatabaseTask,
)
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
            "This task identifies all games in a season that fulfill the two requirements below:\n",
            "1. All data has been scrape",
            "2. Data was successfully combined (does not need to be error free)\n",
            "Then, all PitchFX data for the games identified is added to the database.",
        ]
        subprocess.run(["clear"])
        for line in task_description:
            print_message(line, fg="bright_yellow")
        return yes_no_prompt("Would you like to run this task?", wrap=False)

    def select_season_prompt(self):
        prompt = "Select an MLB season from the list below:"
        choices = {
            f"{MENU_NUMBERS.get(num, str(num))}  {year}": year
            for num, (year, results) in enumerate(self.audit_report.items(), start=1)
            if results["successful"]
        }
        choices[f"{EMOJI_DICT.get('BACK')} Return to Previous Menu"] = None
        return user_options_prompt(choices, prompt)

    def add_pitchfx_to_db_start(self, game_ids):
        subprocess.run(["clear"])
        self.game_ids = game_ids
        self.spinner.text = f"Adding PitchFx to Database... (Game ID: {game_ids[0]}) 0%..."
        self.spinner.start()

    def add_pitchfx_to_db_progress(self, num_complete, game_id):
        percent = num_complete / float(self.total_games)
        self.spinner.text = f"Adding PitchFx to Database... (Game ID: {game_id}) {percent:.0%}..."

    def subscribe_to_events(self):
        self.add_pfx_to_db.events.add_pitchfx_to_db_start += self.add_pitchfx_to_db_start
        self.add_pfx_to_db.events.add_pitchfx_to_db_progress += self.add_pitchfx_to_db_progress

    def unsubscribe_from_events(self):
        self.add_pfx_to_db.events.add_pitchfx_to_db_start -= self.add_pitchfx_to_db_start
        self.add_pfx_to_db.events.add_pitchfx_to_db_progress -= self.add_pitchfx_to_db_progress
