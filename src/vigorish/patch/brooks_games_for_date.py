from dataclasses import dataclass, field
from datetime import datetime

import vigorish.database as db
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
            data = patch.apply(data)
            patch.apply_to_database(data, db_session)
        return Result.Ok(data)


@dataclass
class PatchBrooksGamesForDateBBRefGameID(Patch):
    game_date: str = field(repr=False)
    invalid_bbref_game_id: str
    valid_bbref_game_id: str

    def __post_init__(self):
        self.data_set = DataSet.BROOKS_GAMES_FOR_DATE
        self.patch_id = "__patch_brooks_games_for_date_bbref_game_id__"

    def apply(self, data):
        if self.invalid_bbref_game_id in data.all_bbref_game_ids:
            for game_info in data.games:
                if game_info.bbref_game_id == self.invalid_bbref_game_id:
                    game_info.bbref_game_id = self.valid_bbref_game_id
                    break
        return data

    def apply_to_database(self, data, db_session):
        delete_pitchfx_with_invalid_id(db_session, self.invalid_bbref_game_id)
        delete_pitch_apps_with_invalid_id(db_session, self.invalid_bbref_game_id)
        delete_game_status(db_session, self.invalid_bbref_game_id)
        update_game_status(db_session, data, self.valid_bbref_game_id)
        db_session.commit()


@dataclass
class PatchBrooksGamesForDateRemoveGame(Patch):
    game_date: str = field(repr=False)
    remove_bbref_game_id: str

    def __post_init__(self):
        self.data_set = DataSet.BROOKS_GAMES_FOR_DATE
        self.patch_id = "__patch_brooks_games_for_date_remove_game__"

    def apply(self, data):
        data.games = list(filter(lambda x: x.bbref_game_id != self.remove_bbref_game_id, data.games))
        data.game_count = len(data.games)
        return data

    def apply_to_database(self, data, db_session):
        delete_pitchfx_with_invalid_id(db_session, self.remove_bbref_game_id)
        delete_pitch_apps_with_invalid_id(db_session, self.remove_bbref_game_id)
        delete_game_status(db_session, self.remove_bbref_game_id)
        update_date_status(db_session, data)
        db_session.commit()


def delete_pitchfx_with_invalid_id(db_session, bbref_game_id):
    all_pfx_with_invalid_id = db_session.query(db.PitchFx).filter_by(bbref_game_id=bbref_game_id).all()
    for pfx in all_pfx_with_invalid_id:
        db_session.delete(pfx)


def delete_pitch_apps_with_invalid_id(db_session, bbref_game_id):
    all_pitch_apps_with_invalid_id = (
        db_session.query(db.PitchAppScrapeStatus).filter_by(bbref_game_id=bbref_game_id).all()
    )
    for pitch_app in all_pitch_apps_with_invalid_id:
        db_session.delete(pitch_app)


def delete_game_status(db_session, bbref_game_id):
    game_status = db.GameScrapeStatus.find_by_bbref_game_id(db_session, bbref_game_id)
    if game_status:
        db_session.delete(game_status)


def update_game_status(db_session, data, bbref_game_id):
    game_status = db.GameScrapeStatus.find_by_bbref_game_id(db_session, bbref_game_id)
    if game_status:
        for game_info in data.games:
            if game_info.bbref_game_id == bbref_game_id:
                game_status.bb_game_id = game_info.bb_game_id
                game_status.game_time_hour = game_info.game_time_hour
                game_status.game_time_minute = game_info.game_time_minute
                game_status.game_time_zone = game_info.time_zone_name
                game_status.pitch_app_count_brooks = game_info.pitcher_appearance_count
                break


def update_date_status(db_session, data):
    date_status = db.DateScrapeStatus.find_by_date(db_session, data.game_date)
    date_status.game_count_brooks = data.game_count
