from dataclasses import dataclass, field
from datetime import datetime

from vigorish.database import (
    DateScrapeStatus,
    GameScrapeStatus,
    PitchAppScrapeStatus,
    PitchFx,
)
from vigorish.enums import DataSet
from vigorish.patch.base import Patch, PatchList
from vigorish.util.dt_format_strings import DATE_ONLY
from vigorish.util.result import Result


@dataclass
class BrooksGamesForDatePatchList(PatchList):
    @property
    def game_date(self):
        return datetime.strptime(self.url_id, DATE_ONLY) if self.url_id else None

    def __post_init__(self):
        self.data_set = DataSet.BROOKS_GAMES_FOR_DATE
        self.patch_list_id = "__brooks_games_for_date_patch_list__"

    def apply(self, data, db_session):
        for patch in self.patch_list:
            result = patch.apply(data)
            if result.failure:
                return result
            data = result.value
        result = self.apply_to_database(data, db_session)
        if result.failure:
            return result
        return Result.Ok(data)

    def apply_to_database(self, data, db_session):
        for patch in self.patch_list:
            result = patch.apply_to_database(data, db_session)
            if result.failure:
                return result
        return Result.Ok()


@dataclass
class PatchBrooksGamesForDateBBRefGameID(Patch):
    game_date: str = field(repr=False)
    invalid_bbref_game_id: str
    valid_bbref_game_id: str

    def __post_init__(self):
        self.data_set = DataSet.BROOKS_GAMES_FOR_DATE
        self.patch_id = "__patch_brooks_games_for_date_bbref_game_id__"

    def apply(self, data):
        if self.invalid_bbref_game_id not in data.all_bbref_game_ids:
            error = (
                f'Unable to apply patch, "{self.invalid_bbref_game_id}" is not a valid BBRef '
                f"game id for brooks_games_for_date {self.game_date}"
            )
            return Result.Fail(error)
        for game_info in data.games:
            if game_info.bbref_game_id == self.invalid_bbref_game_id:
                game_info.bbref_game_id = self.valid_bbref_game_id
                break
        return Result.Ok(data)

    def apply_to_database(self, data, db_session):
        delete_pitchfx_with_invalid_id(db_session, self.invalid_bbref_game_id)
        delete_pitch_apps_with_invalid_id(db_session, self.invalid_bbref_game_id)
        result = delete_game_status(db_session, self.invalid_bbref_game_id)
        if result.failure:
            return result
        result = update_game_status(db_session, data, self.valid_bbref_game_id)
        if result.failure:
            return result
        db_session.commit()
        return Result.Ok()


@dataclass
class PatchBrooksGamesForDateRemoveGame(Patch):
    game_date: str = field(repr=False)
    remove_bbref_game_id: str

    def __post_init__(self):
        self.data_set = DataSet.BROOKS_GAMES_FOR_DATE
        self.patch_id = "__patch_brooks_games_for_date_remove_game__"

    def apply(self, data):
        if self.remove_bbref_game_id not in data.all_bbref_game_ids:
            error = (
                f'Unable to apply patch, "{self.remove_game_id}" is not a valid BBRef game id for '
                f"brooks_games_for_date {self.game_date}"
            )
            return Result.Fail(error)
        data.games = list(
            filter(lambda x: x.bbref_game_id != self.remove_bbref_game_id, data.games)
        )
        data.game_count = len(data.games)
        return Result.Ok(data)

    def apply_to_database(self, data, db_session):
        delete_pitchfx_with_invalid_id(db_session, self.remove_bbref_game_id)
        delete_pitch_apps_with_invalid_id(db_session, self.remove_bbref_game_id)
        result = delete_game_status(db_session, self.remove_bbref_game_id)
        if result.failure:
            return result
        update_date_status(db_session, data)
        db_session.commit()
        return Result.Ok()


def delete_pitchfx_with_invalid_id(db_session, bbref_game_id):
    all_pfx_with_invalid_id = db_session.query(PitchFx).filter_by(bbref_game_id=bbref_game_id).all()
    for pfx in all_pfx_with_invalid_id:
        db_session.delete(pfx)


def delete_pitch_apps_with_invalid_id(db_session, bbref_game_id):
    all_pitch_apps_with_invalid_id = (
        db_session.query(PitchAppScrapeStatus).filter_by(bbref_game_id=bbref_game_id).all()
    )
    for pitch_app in all_pitch_apps_with_invalid_id:
        db_session.delete(pitch_app)


def delete_game_status(db_session, bbref_game_id):
    game_status = GameScrapeStatus.find_by_bbref_game_id(db_session, bbref_game_id)
    if game_status:
        db_session.delete(game_status)
    return Result.Ok()


def update_game_status(db_session, data, bbref_game_id):
    game_status = GameScrapeStatus.find_by_bbref_game_id(db_session, bbref_game_id)
    if not game_status:
        error = (
            "Unable to apply patch, database does not contain any records for game id "
            f'"{bbref_game_id}"'
        )
        return Result.Fail(error)
    for game_info in data.games:
        if game_info.bbref_game_id == bbref_game_id:
            game_status.bb_game_id = game_info.bb_game_id
            game_status.game_time_hour = game_info.game_time_hour
            game_status.game_time_minute = game_info.game_time_minute
            game_status.game_time_zone = game_info.time_zone_name
            game_status.pitch_app_count_brooks = game_info.pitcher_appearance_count
            break
    return Result.Ok()


def update_date_status(db_session, data):
    date_status = DateScrapeStatus.find_by_date(db_session, data.game_date)
    date_status.game_count_brooks = data.game_count
