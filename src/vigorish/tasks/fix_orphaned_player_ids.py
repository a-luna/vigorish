import time
from random import randint

from events import Events

import vigorish.database as db
from vigorish.tasks.base import Task
from vigorish.tasks.scrape_mlb_player_info import ScrapeMlbPlayerInfoTask
from vigorish.util.result import Result


class FixOrphanedPlayerIdsTask(Task):
    def __init__(self, app):
        super().__init__(app)
        self.events = Events(
            (
                "error_occurred",
                "fix_orphaned_player_ids_start",
                "requesting_player_data",
                "skipped_player_does_not_meet_debut_limit",
                "player_bio_added_to_database",
                "fix_orphaned_player_ids_complete",
            )
        )

    def execute(self):
        earliest_season = min(s.year for s in db.Season.get_all_regular_seasons(self.db_session))
        debut_limit = earliest_season - 30
        orphaned_player_ids = [
            player_id for player_id in self.db_session.query(db.PlayerId).all() if not player_id.db_player_id
        ]
        fixed_player_ids = []
        self.events.fix_orphaned_player_ids_start(orphaned_player_ids)
        for player_id in reversed(orphaned_player_ids):
            time.sleep(randint(2, 4))
            scrape_task = ScrapeMlbPlayerInfoTask(self.app)
            self.events.requesting_player_data(player_id)
            result = scrape_task.find_by_mlb_id(player_id.mlb_id, player_id.bbref_id, debut_limit)
            if result.failure:
                raise ValueError(result.error)
            player = result.value
            if not player:
                self.events.skipped_player_does_not_meet_debut_limit(player_id)
                continue
            player_id.db_player_id = player.id
            player.add_to_db_backup = True
            fixed_player_ids.append(player_id)
            self.db_session.commit()
            self.events.player_bio_added_to_database(player)
        self.events.fix_orphaned_player_ids_complete(fixed_player_ids)
        return Result.Ok(fixed_player_ids)
