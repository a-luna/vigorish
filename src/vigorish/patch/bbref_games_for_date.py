from dataclasses import dataclass, field
from datetime import datetime

import vigorish.database as db
from vigorish.enums import DataSet
from vigorish.patch.base import Patch, PatchList
from vigorish.util.dt_format_strings import DATE_ONLY
from vigorish.util.result import Result


@dataclass
class BBRefGamesForDatePatchList(PatchList):
    @property
    def game_date(self):
        return datetime.strptime(self.url_id, DATE_ONLY) if self.url_id else None

    def __post_init__(self):
        self.data_set = DataSet.BBREF_GAMES_FOR_DATE
        self.patch_list_id = "__bbref_games_for_date_patch_list__"

    def apply(self, data, db_session):
        for patch in self.patch_list:
            data = patch.apply(data)
            patch.apply_to_database(data, db_session)
        return Result.Ok(data)


@dataclass
class PatchBBRefGamesForDateGameID(Patch):
    game_date: str = field(repr=False)
    invalid_bbref_game_id: str
    valid_bbref_game_id: str

    def __post_init__(self):
        self.data_set = DataSet.BBREF_GAMES_FOR_DATE
        self.patch_id = "__patch_bbref_games_for_date_game_id__"

    def apply(self, data):
        for game_info in data.games:
            if game_info.bbref_game_id == self.invalid_bbref_game_id:
                game_info.bbref_game_id = self.valid_bbref_game_id
                break
        return data

    def apply_to_database(self, data, db_session):
        game_status = db.GameScrapeStatus.find_by_bbref_game_id(db_session, self.invalid_bbref_game_id)
        if game_status:
            game_status.bbref_game_id = self.valid_bbref_game_id
            db_session.commit()
