"""Update bbref_player_id_map.json file."""
import subprocess

from getch import pause
from halo import Halo

from vigorish.cli.components import (
    get_random_cli_color,
    get_random_dots_spinner,
    print_heading,
)
from vigorish.cli.components.viewers import DictListTableViewer
from vigorish.cli.menu_item import MenuItem
from vigorish.constants import EMOJIS
from vigorish.tasks import FixOrphanedPlayerIdsTask
from vigorish.util.result import Result


class FixOrphanedPlayerIds(MenuItem):
    def __init__(self, app):
        super().__init__(app)
        self.task = FixOrphanedPlayerIdsTask(self.app)
        self.menu_item_text = "Fix Orphaned Player IDs"
        self.menu_item_emoji = EMOJIS.get("FAMILY")
        self.menu_heading = self._menu_item_text
        self.spinner = None
        self.total_player_ids = 0
        self.fixed = []
        self.skipped = []
        self.last_player_processed = None

    @property
    def total_processed(self):
        return len(self.skipped) + len(self.fixed)

    @property
    def total_remaining(self):
        return self.total_player_ids - self.total_processed

    def launch(self):
        subprocess.run(["clear"])
        print_heading("Fix Orphaned Player IDs: In Progress...", fg="bright_yellow")
        self.spinner = Halo(spinner=get_random_dots_spinner(), color=get_random_cli_color())
        self.spinner.text = "0% Complete..."
        self.spinner.start()
        self.subscribe_to_events()
        result = self.task.execute()
        self.unsubscribe_from_events()
        if result.failure:
            self.spinner.fail("Error occurred!")
            return result
        fixed_player_ids = result.value
        if not fixed_player_ids:
            self.spinner.info("No orphaned player_ids meet the debut year requirement")
            return Result.Ok(False)
        self.spinner.succeed("Successfully fixed all eligible player_ids!")
        pause(message="Press any key to continue...")

        heading = "Fix Orphaned Player IDs: Results"
        message = f"{len(fixed_player_ids)} changes total:"
        table_viewer = DictListTableViewer(
            dict_list=[p.as_dict() for p in fixed_player_ids],
            prompt="Press Enter to continue",
            confirm_only=True,
            table_color="bright_yellow",
            heading=heading,
            heading_color="bright_yellow",
            message=message,
            message_color="blue",
        )
        table_viewer.launch()
        return Result.Ok(False)

    def fix_orphaned_player_ids_start(self, player_ids):
        self.total_player_ids = len(player_ids)
        self.spinner.text = f"0% Complete [0 Fixed, 0 Skipped, {self.total_player_ids} Remaining]"

    def requesting_player_data(self, player_id):
        self.last_player_processed = player_id
        self.update_spinner_text()

    def skipped_player_does_not_meet_debut_limit(self, player_id):
        self.skipped.append(player_id)
        self.spinner.text = self.update_spinner_text()

    def player_bio_added_to_database(self, player):
        self.fixed.append(player)
        self.spinner.text = self.update_spinner_text()

    def update_spinner_text(self):
        percent = self.total_processed / float(self.total_player_ids)
        return (
            f"{percent:.0%} Complete "
            f"[{len(self.fixed)} Fixed, {len(self.skipped)} Skipped {self.total_remaining} Remaining, "
            f"Now: {self.last_player_processed.mlb_name} ({self.last_player_processed.mlb_id})]"
        )

    def subscribe_to_events(self):
        self.task.events.fix_orphaned_player_ids_start += self.fix_orphaned_player_ids_start
        self.task.events.requesting_player_data += self.requesting_player_data
        self.task.events.skipped_player_does_not_meet_debut_limit += self.skipped_player_does_not_meet_debut_limit
        self.task.events.player_bio_added_to_database += self.player_bio_added_to_database

    def unsubscribe_from_events(self):
        self.task.events.fix_orphaned_player_ids_start -= self.fix_orphaned_player_ids_start
        self.task.events.requesting_player_data -= self.requesting_player_data
        self.task.events.skipped_player_does_not_meet_debut_limit -= self.skipped_player_does_not_meet_debut_limit
        self.task.events.player_bio_added_to_database -= self.player_bio_added_to_database
