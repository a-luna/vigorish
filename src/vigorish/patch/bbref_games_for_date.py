from dataclasses import dataclass, field
from datetime import datetime

from vigorish.enums import DataSet
from vigorish.patch.base import Patch, PatchList
from vigorish.util.dt_format_strings import DATE_ONLY


@dataclass
class BBRefGamesForDatePatchList(PatchList):
    @property
    def game_date(self):
        return datetime.strptime(self.url_id, DATE_ONLY) if self.url_id else None

    def __post_init__(self):
        self.data_set = DataSet.BBREF_GAMES_FOR_DATE
        self.patch_list_id = "__bbref_games_for_date_patch_list__"


@dataclass
class PatchBBRefGamesForDateGameID(Patch):
    game_date: str = field(repr=False)
    old_game_id: str
    new_game_id: str

    def __post_init__(self):
        self.data_set = DataSet.BBREF_GAMES_FOR_DATE
        self.patch_id = "__patch_bbref_games_for_date_game_id__"

    def apply(self, data):
        pass
