import subprocess

from getch import pause
from halo import Halo
from tabulate import tabulate

import vigorish.database as db
from vigorish.cli.components.util import (
    get_random_cli_color,
    get_random_dots_spinner,
    print_heading,
    print_message,
)
from vigorish.cli.components.viewers import DisplayPage, PageViewer
from vigorish.cli.menu_item import MenuItem
from vigorish.constants import EMOJIS
from vigorish.tasks import CalculateAvgPitchTimesTask
from vigorish.util.result import Result

STAT_NAMES = ["avg", "min", "max"]


class CalculatePitchTimes(MenuItem):
    def __init__(self, app):
        super().__init__(app)
        self.calc_pitch_times = CalculateAvgPitchTimesTask(app)
        self.menu_item_text = "Calculate Avg. Time Between Pitches"
        self.menu_item_emoji = EMOJIS.get("CLOCK")
        self.exit_menu = False
        self.spinner = Halo(spinner=get_random_dots_spinner(), color=get_random_cli_color())
        self.game_ids = []

    @property
    def total_games(self):
        return len(self.game_ids)

    def launch(self):
        if not self.prompt_user_run_task():
            return Result.Ok(True)
        self.subscribe_to_events()
        result = self.calc_pitch_times.execute()
        self.spinner.stop()
        self.unsubscribe_from_events()
        if result.failure:
            return result
        subprocess.run(["clear"])
        results = result.value
        time_between_pitches = db.TimeBetweenPitches.from_calc_results(self.db_session, results)
        self.display_pitch_metrics(time_between_pitches.as_dict())
        pause(message="\nPress any key to continue...")
        return Result.Ok()

    def prompt_user_run_task(self):
        subprocess.run(["clear"])
        prompt = "Would you like to run this task and calculate these metrics?"
        pages = [DisplayPage(page, None) for page in self.get_task_description_pages()]
        page_viewer = PageViewer(pages, prompt=prompt, text_color="bright_yellow")
        return page_viewer.launch()

    def find_eligible_games_start(self):
        subprocess.run(["clear"])
        self.spinner.text = "Identifying games which meet all requirements for task..."
        self.spinner.start()

    def find_eligible_games_complete(self, game_ids):
        self.spinner.stop()
        self.game_ids = game_ids
        self.spinner = Halo(spinner=get_random_dots_spinner(), color=get_random_cli_color())
        self.spinner.text = self.get_progress_text(0)
        self.spinner.start()

    def calculate_pitch_metrics_progress(self, games_complete):
        self.spinner.text = self.get_progress_text(games_complete)

    def get_progress_text(self, games_complete):
        game_id = self.game_ids[games_complete - 1]
        percent = games_complete / float(self.total_games)
        return f"Calculating time between pitches... (Game ID: {game_id}) {percent:.0%}..."

    def display_pitch_metrics(self, metrics):
        heading = f"Avg. Time Between Pitches for {len(self.game_ids)} MLB Games"
        table = self.construct_stats_table(metrics)
        print_heading(heading, fg="bright_yellow")
        print_message(table, wrap=False, fg="bright_cyan")

    def construct_stats_table(self, metrics):
        table_data = []
        for stat_name in STAT_NAMES:
            row = {"stat_name": stat_name}
            for group_name, pitch_metrics in metrics.items():
                if group_name == "timestamp":
                    continue
                row[group_name] = pitch_metrics[stat_name]
            table_data.append(row)
        return tabulate(table_data, headers="keys", numalign="right", stralign="right")

    def subscribe_to_events(self):
        self.calc_pitch_times.events.find_eligible_games_start += self.find_eligible_games_start
        self.calc_pitch_times.events.find_eligible_games_complete += self.find_eligible_games_complete
        self.calc_pitch_times.events.calculate_pitch_metrics_progress += self.calculate_pitch_metrics_progress

    def unsubscribe_from_events(self):
        self.calc_pitch_times.events.find_eligible_games_start -= self.find_eligible_games_start
        self.calc_pitch_times.events.find_eligible_games_complete -= self.find_eligible_games_complete
        self.calc_pitch_times.events.calculate_pitch_metrics_progress -= self.calculate_pitch_metrics_progress

    def get_task_description_pages(self):
        return [
            [
                (
                    "This task first identifies games where all data has been scraped and the "
                    "process of reconciling and combining the scraped data was entirely "
                    "successful with zero data errors of any type (e.g., zero missing/extra "
                    "pitches, no invalid PitchFX data).\n"
                ),
                (
                    "Next, each game is stepped through pitch-by-pitch and the length of time "
                    "between each pitch is calculated and stored using the timestamps "
                    "available on PitchFX data."
                ),
            ],
            [
                "There are three distinct types of between-pitch durations which are collected:\n",
                "1. (between pitches)",
                "time between pitches in the same at bat\n",
                "2. (between at bats)",
                (
                    "time between the last pitch of an at bat and the first pitch of the next at "
                    "bat in the same inning"
                ),
            ],
            [
                "3. (between innings)",
                (
                    "time between the last pitch of the last at bat in an inning and the first "
                    "pitch of the next at bat in the subsequent inning\n"
                ),
                (
                    "Finally, the data sets are processed to remove outliers before calculating "
                    "the average, maximum and minimum durations for each category."
                ),
            ],
        ]
