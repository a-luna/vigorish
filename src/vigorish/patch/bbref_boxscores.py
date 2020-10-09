from dataclasses import dataclass, field

from vigorish.enums import DataSet
from vigorish.patch.base import Patch, PatchList
from vigorish.util.result import Result
from vigorish.util.string_helpers import validate_bbref_game_id


@dataclass
class BBRefBoxscorePatchList(PatchList):
    @property
    def game_date(self):
        result = validate_bbref_game_id(self.url_id)
        return result.value["game_date"] if result.success else None

    def __post_init__(self):
        self.data_set = DataSet.BBREF_BOXSCORES
        self.patch_list_id = "__bbref_boxscore_patch_list__"


@dataclass
class PatchBBRefBoxscorePitchSequence(Patch):
    bbref_game_id: str
    inning_id: str
    pbp_table_row_number: int = field(repr=False)
    old_pitch_sequence: str = field(repr=False)
    new_pitch_sequence: str = field(repr=False)

    def __post_init__(self):
        self.data_set = DataSet.BBREF_BOXSCORES
        self.patch_id = "__patch_bbref_boxscore_pitch_sequence__"

    def apply(self, data):
        inning_matches = [
            inning for inning in data.innings_list if inning.inning_id == self.inning_id
        ]
        if not inning_matches:
            error = f"Unable to locate the inning identified in this patch: {self.inning_id}"
            return Result.Fail(error)
        if len(inning_matches) > 1:
            error = (
                "More than one inning was found that matches the inning identified in this "
                f"patch: {self.inning_id}"
            )
            return Result.Fail(error)
        inning = inning_matches[0]
        event_matches = [
            event
            for event in inning.game_events
            if event.pbp_table_row_number == self.pbp_table_row_number
        ]
        if not event_matches:
            error = (
                "Unable to locate the game event identified by pbp_table_row_number in this "
                f"patch: {self.pbp_table_row_number}"
            )
            return Result.Fail(error)
        if len(event_matches) > 1:
            error = (
                "More than one game event was found that matches the pbp_table_row_number "
                f"identified in this patch: {self.pbp_table_row_number}"
            )
            return Result.Fail(error)
        event_matches[0].pitch_sequence = self.new_pitch_sequence
        return Result.Ok(data)
