from dataclasses import dataclass, field
from datetime import datetime

from vigorish.database import GameScrapeStatus
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
            result = patch.apply(data)
            if result.failure:
                return result
            data = result.value
        result = self.post_process_data(data, db_session)
        if result.failure:
            return result
        data = result.value
        return Result.Ok(data)

    def post_process_data(self, data, db_session):
        for patch in self.patch_list:
            game_status = GameScrapeStatus.find_by_bbref_game_id(db_session, patch.old_game_id)
            if not game_status:
                error = (
                    "Unable to apply patch, database does not contain any records for game id "
                    f'"{patch.old_game_id}"'
                )
                return Result.Fail(error)
            game_status.bbref_game_id = patch.new_game_id
        db_session.commit()
        return Result.Ok(data)


@dataclass
class PatchBBRefGamesForDateGameID(Patch):
    game_date: str = field(repr=False)
    old_game_id: str
    new_game_id: str

    def __post_init__(self):
        self.data_set = DataSet.BBREF_GAMES_FOR_DATE
        self.patch_id = "__patch_bbref_games_for_date_game_id__"

    def apply(self, data):
        if self.old_game_id not in data.all_bbref_game_ids:
            error = (
                f'Unable to apply patch, "{self.old_game_id}" is not a valid BBRef game id for '
                f"bbref_games_for_date {self.game_date}"
            )
            return Result.Fail(error)
        for game_info in data.games:
            if game_info.bbref_game_id == self.old_game_id:
                game_info.bbref_game_id = self.new_game_id
                break
        return Result.Ok(data)
