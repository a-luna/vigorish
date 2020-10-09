"""Fix at bats with missing/extra PitchFX data."""
import subprocess

from getch import pause

from vigorish.cli.components import print_heading, print_message
from vigorish.cli.menu_item import MenuItem
from vigorish.constants import EMOJI_DICT
from vigorish.enums import AuditError
from vigorish.util.result import Result


class InvestigatePitchFxErrors(MenuItem):
    def __init__(self, app, year, bbref_game_ids):
        super().__init__(app)
        self.year = year
        self.bbref_game_ids = bbref_game_ids
        self.menu_item_text = f"{AuditError.PITCHFX_ERROR} ({self.game_count} Games)"
        self.menu_item_emoji = EMOJI_DICT.get("CRYING")

    @property
    def game_count(self):
        return len(self.bbref_game_ids)

    def launch(self):
        subprocess.run(["clear"])
        print_heading("This feature is not currently available", fg="bright_yellow")
        print_message("Sorry for any inconvenience, I promise to finish this soon!")
        pause(message="\nPress any key to return to the previous menu...")
        return Result.Ok(True)
