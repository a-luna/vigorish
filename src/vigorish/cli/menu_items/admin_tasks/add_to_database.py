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
from vigorish.constants import EMOJIS, MENU_NUMBERS
from vigorish.tasks import AddToDatabaseTask
from vigorish.util.result import Result


class AddToDatabase(MenuItem):
    def __init__(self, app):
        super().__init__(app)
        self.add_to_db = AddToDatabaseTask(app)
        self.menu_item_text = "Add Combined Game Data to Database"
        self.menu_item_emoji = EMOJIS.get("BASEBALL")
        self.exit_menu = False
        self.spinner = Halo(spinner=get_random_dots_spinner(), color=get_random_cli_color())
        self.game_ids = []

    @property
    def total_games(self):
        return len(self.game_ids)

    def launch(self, year=None, no_prompts=False):
        return self.launch_no_prompts(year) if no_prompts else self.launch_interactive(year)

    def launch_no_prompts(self, year):
        self.subscribe_to_events()
        result = self.add_to_db.execute(year)
        self.spinner.stop()
        self.unsubscribe_from_events()
        if result.failure:
            return result
        subprocess.run(["clear"])
        print_message(f"Added PitchFX data for {len(self.game_ids)} games to database!")
        return Result.Ok()

    def launch_interactive(self, year):
        if not self.prompt_user_run_task():
            return Result.Ok(True)
        if not year:
            result = self.select_season_prompt()
            if result.failure:
                return Result.Ok()
            year = result.value
        self.subscribe_to_events()
        result = self.add_to_db.execute(year)
        self.spinner.stop()
        self.unsubscribe_from_events()
        if result.failure:
            return result
        subprocess.run(["clear"])
        print_message(f"Added PitchFX data for {len(self.game_ids)} games to database!")
        pause(message="\nPress any key to continue...")
        return Result.Ok(True)

    def prompt_user_run_task(self):
        task_description = [
            "This task identifies all games that fulfill the two requirements below:\n",
            "1. All data sets have been scraped",
            "2. Data was successfully combined (does not need to be error free)\n",
            "For all games that meet these requirements, the data listed below will be added to ",
            "the database:\n",
            "1. Individual player batting stats",
            "2. Individual player pitching stats",
            "3. PitchFX data for all pitches thrown",
        ]
        subprocess.run(["clear"])
        for line in task_description:
            print_message(line, fg="bright_yellow")
        return yes_no_prompt("\nWould you like to run this task?", wrap=False)

    def select_season_prompt(self):
        prompt = "Select an MLB season from the list below:"
        choices = {f"{MENU_NUMBERS.get(1)}  ALL": "ALL"}
        for num, (year, results) in enumerate(self.app.audit_report.items(), start=2):
            if results["successful"]:
                choices[f"{MENU_NUMBERS.get(num, str(num))}  {year}"] = year
        choices[f"{EMOJIS.get('BACK')} Return to Previous Menu"] = None
        result = user_options_prompt(choices, prompt)
        if result.failure:
            return result
        year = result.value if isinstance(result.value, int) else None
        return Result.Ok(year)

    def add_data_to_db_start(self, year, game_ids):
        subprocess.run(["clear"])
        self.game_ids = game_ids
        self.spinner.text = self.get_progress_text(0, year, game_ids[0])
        self.spinner.start()

    def add_data_to_db_progress(self, num_complete, year, game_id):
        self.spinner.text = self.get_progress_text(num_complete, year, game_id)

    def get_progress_text(self, num_complete, year, game_id):
        percent = num_complete / float(self.total_games)
        return f"Adding combined game data for MLB {year} to database... (Game ID: {game_id}) {percent:.0%}..."

    def subscribe_to_events(self):
        self.add_to_db.events.add_data_to_db_start += self.add_data_to_db_start
        self.add_to_db.events.add_data_to_db_progress += self.add_data_to_db_progress

    def unsubscribe_from_events(self):
        self.add_to_db.events.add_data_to_db_start -= self.add_data_to_db_start
        self.add_to_db.events.add_data_to_db_progress -= self.add_data_to_db_progress
