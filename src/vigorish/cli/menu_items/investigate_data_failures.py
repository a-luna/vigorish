"""Investigate games where boxscore and PitchFx data could not be reconciled."""
import subprocess

from getch import pause

from vigorish.cli.components import print_heading, print_message
from vigorish.cli.menu_item import MenuItem
from vigorish.constants import EMOJIS
from vigorish.enums import AuditError
from vigorish.util.result import Result


class InvestigateScrapedDataFailures(MenuItem):
    def __init__(self, app, year, bbref_game_ids):
        super().__init__(app)
        self.year = year
        self.bbref_game_ids = bbref_game_ids
        self.menu_item_text = f"{AuditError.FAILED_TO_COMBINE} ({self.game_count} Games)"
        self.menu_item_emoji = EMOJIS.get("WEARY")

    @property
    def game_count(self):
        return len(self.bbref_game_ids)

    def launch(self):
        subprocess.run(["clear"])
        print_heading("This feature is not currently available", fg="bright_yellow")
        print_message("Sorry for any inconvenience, I promise to finish this soon!")
        pause(message="\nPress any key to return to the previous menu...")
        return Result.Ok(True)
