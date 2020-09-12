"""Fix at bats with missing/extra PitchFX data."""
import subprocess

from vigorish.cli.menu_item import MenuItem
from vigorish.constants import EMOJI_DICT
from vigorish.enums import AuditError


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
