from dataclasses import dataclass
from datetime import datetime

from dataclass_csv import accept_whitespaces, dateformat
from dateutil import tz
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

from vigorish.config.database import Base
from vigorish.models.season import Season
from vigorish.util.dataclass_helpers import dict_from_dataclass, sanitize_row_dict
from vigorish.util.dt_format_strings import (
    DATE_ONLY,
    DATE_ONLY_2,
    DATE_ONLY_TABLE_ID,
    DT_STR_FORMAT,
)
from vigorish.util.list_helpers import display_dict
from vigorish.util.string_helpers import get_brooks_team_id, validate_brooks_game_id


class GameScrapeStatus(Base):

    __tablename__ = "scrape_status_game"
    id = Column(Integer, primary_key=True)
    game_date = Column(DateTime)
    game_time_hour = Column(Integer)
    game_time_minute = Column(Integer)
    game_time_zone = Column(String)
    bbref_game_id = Column(String)
    bb_game_id = Column(String)
    scraped_bbref_boxscore = Column(Integer, default=0)
    scraped_brooks_pitch_logs = Column(Integer, default=0)
    combined_data_success = Column(Integer, default=0)
    combined_data_fail = Column(Integer, default=0)
    pitch_app_count_bbref = Column(Integer, default=0)
    pitch_app_count_brooks = Column(Integer, default=0)
    total_pitch_count_bbref = Column(Integer, default=0)
    scrape_status_date_id = Column(Integer, ForeignKey("scrape_status_date.id"))
    season_id = Column(Integer, ForeignKey("season.id"))

    scrape_status_pitchfx = relationship("PitchAppScrapeStatus", backref="scrape_status_game")
    pitch_app_status = relationship(
        "Game_PitchApp_View",
        backref="original",
        uselist=False,
        primaryjoin="GameScrapeStatus.id==Game_PitchApp_View.id",
        foreign_keys="Game_PitchApp_View.id",
    )

    @hybrid_property
    def game_start_time(self):
        if not self.game_time_hour:
            return None
        game_start_time = datetime(
            year=self.game_date.year,
            month=self.game_date.month,
            day=self.game_date.day,
            hour=self.game_time_hour,
            minute=self.game_time_minute,
        )
        return game_start_time.replace(tzinfo=tz.gettz(self.game_time_zone))

    @hybrid_property
    def game_date_time_str(self):
        return (
            self.game_start_time.strftime(DT_STR_FORMAT)
            if self.game_start_time
            else self.game_date.strftime(DATE_ONLY_2)
            if self.game_date
            else None
        )

    @hybrid_property
    def away_team_id_bb(self):
        game_dict = validate_brooks_game_id(self.bb_game_id).value
        return game_dict["away_team_id"]

    @hybrid_property
    def home_team_id_bb(self):
        game_dict = validate_brooks_game_id(self.bb_game_id).value
        return game_dict["home_team_id"]

    @hybrid_property
    def pitch_app_ids(self):
        return [pfx.pitch_app_id for pfx in self.scrape_status_pitchfx]

    @hybrid_property
    def pitch_app_ids_no_pitchfx_data(self):
        return [pfx.pitch_app_id for pfx in self.scrape_status_pitchfx if pfx.no_pitchfx_data == 1]

    @hybrid_property
    def pitch_app_count_pitchfx(self):
        return self.pitch_app_status.total_pitchfx if self.pitch_app_status.total_pitchfx else 0

    @hybrid_property
    def total_pitch_apps_scraped_pitchfx(self):
        return (
            self.pitch_app_status.total_pitchfx_scraped
            if self.pitch_app_status.total_pitchfx_scraped
            else 0
        )

    @hybrid_property
    def total_pitch_apps_no_pitchfx_data(self):
        return (
            self.pitch_app_status.total_no_pitchfx_data
            if self.pitch_app_status.total_no_pitchfx_data
            else 0
        )

    @hybrid_property
    def total_pitch_apps_with_pitchfx_data(self):
        return self.pitch_app_count_pitchfx - self.total_pitch_apps_no_pitchfx_data

    @hybrid_property
    def total_pitch_apps_combined_data(self):
        return (
            self.pitch_app_status.total_combined_pitchfx_bbref_data
            if self.pitch_app_status.total_combined_pitchfx_bbref_data
            else 0
        )

    @hybrid_property
    def total_pitch_apps_contains_patched_data(self):
        return len(pfx for pfx in self.scrape_status_pitchfx if pfx.contains_patched_data)

    @hybrid_property
    def total_pitch_apps_pitchfx_error(self):
        total_extra_pfx = self.pitch_app_status.total_pitchfx_error
        return total_extra_pfx if total_extra_pfx else 0

    @hybrid_property
    def total_pitch_apps_invalid_pitchfx(self):
        total_invalid_pfx = self.pitch_app_status.total_invalid_pitchfx
        return total_invalid_pfx if total_invalid_pfx else 0

    @hybrid_property
    def total_pitch_apps_pitchfx_is_valid(self):
        return sum(
            not (pfx.pitchfx_error or pfx.invalid_pitchfx) for pfx in self.scrape_status_pitchfx
        )

    @hybrid_property
    def pitch_app_ids_with_pitchfx_data_not_only_patched(self):
        return [
            pfx.pitch_app_id
            for pfx in self.scrape_status_pitchfx
            if pfx.combined_pitchfx_bbref_data
            and not pfx.no_pitchfx_data
            and not pfx.contains_only_patched_data
        ]

    @hybrid_property
    def total_pitch_count_pitch_logs(self):
        return (
            self.pitch_app_status.total_pitch_count_pitch_log
            if self.pitch_app_status.total_pitch_count_pitch_log
            else 0
        )

    @hybrid_property
    def total_pitch_count_bbref_audited(self):
        return (
            self.pitch_app_status.total_pitch_count_bbref
            if self.pitch_app_status.total_pitch_count_bbref
            else 0
        )

    @hybrid_property
    def total_pitch_count_pitchfx(self):
        return (
            self.pitch_app_status.total_pitch_count_pitchfx
            if self.pitch_app_status.total_pitch_count_pitchfx
            else 0
        )

    @hybrid_property
    def total_pitch_count_pitchfx_audited(self):
        return (
            self.pitch_app_status.total_pitch_count_pitchfx_audited
            if self.pitch_app_status.total_pitch_count_pitchfx_audited
            else 0
        )

    @hybrid_property
    def total_duplicate_pitchfx_removed_count(self):
        return (
            self.pitch_app_status.total_duplicate_pitchfx_removed_count
            if self.pitch_app_status.total_duplicate_pitchfx_removed_count
            else 0
        )

    @hybrid_property
    def total_patched_pitchfx_count(self):
        return (
            self.pitch_app_status.total_patched_pitchfx_count
            if self.pitch_app_status.total_patched_pitchfx_count
            else 0
        )

    @hybrid_property
    def total_missing_pitchfx_count(self):
        return (
            self.pitch_app_status.total_missing_pitchfx_count
            if self.pitch_app_status.total_missing_pitchfx_count
            else 0
        )

    @hybrid_property
    def total_extra_pitchfx_count(self):
        return (
            self.pitch_app_status.total_extra_pitchfx_count
            if self.pitch_app_status.total_extra_pitchfx_count
            else 0
        )

    @hybrid_property
    def total_extra_pitchfx_removed_count(self):
        return (
            self.pitch_app_status.total_extra_pitchfx_removed_count
            if self.pitch_app_status.total_extra_pitchfx_removed_count
            else 0
        )

    @hybrid_property
    def total_batters_faced_bbref(self):
        return (
            self.pitch_app_status.total_batters_faced_bbref
            if self.pitch_app_status.total_batters_faced_bbref
            else 0
        )

    @hybrid_property
    def total_batters_faced_pitchfx(self):
        return (
            self.pitch_app_status.total_batters_faced_pitchfx
            if self.pitch_app_status.total_batters_faced_pitchfx
            else 0
        )

    @hybrid_property
    def total_at_bats_pitchfx_complete(self):
        return (
            self.pitch_app_status.total_at_bats_pitchfx_complete
            if self.pitch_app_status.total_at_bats_pitchfx_complete
            else 0
        )

    @hybrid_property
    def total_at_bats_patched_pitchfx(self):
        return (
            self.pitch_app_status.total_at_bats_patched_pitchfx
            if self.pitch_app_status.total_at_bats_patched_pitchfx
            else 0
        )

    @hybrid_property
    def total_at_bats_missing_pitchfx(self):
        return (
            self.pitch_app_status.total_at_bats_missing_pitchfx
            if self.pitch_app_status.total_at_bats_missing_pitchfx
            else 0
        )

    @hybrid_property
    def total_at_bats_extra_pitchfx(self):
        return (
            self.pitch_app_status.total_at_bats_extra_pitchfx
            if self.pitch_app_status.total_at_bats_extra_pitchfx
            else 0
        )

    @hybrid_property
    def total_at_bats_extra_pitchfx_removed(self):
        return (
            self.pitch_app_status.total_at_bats_extra_pitchfx_removed
            if self.pitch_app_status.total_at_bats_extra_pitchfx_removed
            else 0
        )

    @hybrid_property
    def total_at_bats_pitchfx_error(self):
        return (
            self.pitch_app_status.total_at_bats_pitchfx_error
            if self.pitch_app_status.total_at_bats_pitchfx_error
            else 0
        )

    @hybrid_property
    def total_at_bats_invalid_pitchfx(self):
        return (
            self.pitch_app_status.total_at_bats_invalid_pitchfx
            if self.pitch_app_status.total_at_bats_invalid_pitchfx
            else 0
        )

    @hybrid_property
    def no_pitchfx_data_for_all_pitch_apps(self):
        if not self.scraped_brooks_pitch_logs:
            return False
        return all(pfx.no_pitchfx_data for pfx in self.scrape_status_pitchfx)

    @hybrid_property
    def no_pitchfx_data_for_any_pitch_apps(self):
        if not self.scraped_brooks_pitch_logs:
            return False
        return any(pfx.no_pitchfx_data for pfx in self.scrape_status_pitchfx)

    @hybrid_property
    def scraped_all_pitchfx_logs(self):
        if not self.scraped_brooks_pitch_logs:
            return False
        if not self.scrape_status_pitchfx:
            return True
        return self.pitch_app_count_pitchfx == self.total_pitch_apps_scraped_pitchfx

    @hybrid_property
    def combined_data_for_all_pitchfx_logs(self):
        if not self.scraped_all_pitchfx_logs:
            return False
        if not self.scrape_status_pitchfx:
            return False
        return self.pitch_app_count_pitchfx == self.total_pitch_apps_combined_data

    @hybrid_property
    def pitchfx_error_for_any_pitchfx_logs(self):
        if not self.scraped_all_pitchfx_logs:
            return False
        if not self.scrape_status_pitchfx:
            return False
        return any((pfx.pitchfx_error or pfx.invalid_pitchfx) for pfx in self.scrape_status_pitchfx)

    @hybrid_property
    def pitchfx_is_valid_for_all_pitchfx_logs(self):
        if not self.scraped_all_pitchfx_logs:
            return True
        if not self.scrape_status_pitchfx:
            return True
        return all(
            not (pfx.pitchfx_error or pfx.invalid_pitchfx) for pfx in self.scrape_status_pitchfx
        )

    def __repr__(self):
        return f"<GameScrapeStatus bbref_game_id={self.bbref_game_id}>"

    def as_dict(self):
        d = {}
        d["bbref_game_id"] = self.bbref_game_id
        d["bb_game_id"] = self.bb_game_id
        d["game_date_time_str"] = self.game_date_time_str
        d["scraped_bbref_boxscore"] = self.scraped_bbref_boxscore
        d["scraped_brooks_pitch_logs"] = self.scraped_brooks_pitch_logs
        d["scraped_all_pitchfx_logs"] = self.scraped_all_pitchfx_logs
        d["combined_data_success"] = self.combined_data_success
        d["combined_data_fail"] = self.combined_data_fail
        d["pitch_app_count_bbref"] = self.pitch_app_count_bbref
        d["pitch_app_count_brooks"] = self.pitch_app_count_brooks
        d["pitch_app_count_pitchfx"] = self.pitch_app_count_pitchfx
        d["total_pitch_apps_scraped_pitchfx"] = self.total_pitch_apps_scraped_pitchfx
        d["total_pitch_apps_no_pitchfx_data"] = self.total_pitch_apps_no_pitchfx_data
        d["total_pitch_apps_with_pitchfx_data"] = self.total_pitch_apps_with_pitchfx_data
        d["total_pitch_apps_combined_data"] = self.total_pitch_apps_combined_data
        d["total_pitch_apps_pitchfx_error"] = self.total_pitch_apps_pitchfx_error
        d["total_pitch_apps_invalid_pitchfx"] = self.total_pitch_apps_invalid_pitchfx
        d["total_pitch_apps_pitchfx_is_valid"] = self.total_pitch_apps_pitchfx_is_valid
        d["total_pitch_count_pitch_logs"] = self.total_pitch_count_pitch_logs
        d["total_pitch_count_bbref"] = self.total_pitch_count_bbref
        d["total_pitch_count_bbref_audited"] = self.total_pitch_count_bbref_audited
        d["total_pitch_count_pitchfx"] = self.total_pitch_count_pitchfx
        d["total_pitch_count_pitchfx_audited"] = self.total_pitch_count_pitchfx_audited
        d["total_duplicate_pitchfx_removed_count"] = self.total_duplicate_pitchfx_removed_count
        d["total_missing_pitchfx_count"] = self.total_missing_pitchfx_count
        d["total_extra_pitchfx_count"] = self.total_extra_pitchfx_count
        d["total_extra_pitchfx_removed_count"] = self.total_extra_pitchfx_removed_count
        d["total_batters_faced_bbref"] = self.total_batters_faced_bbref
        d["total_batters_faced_pitchfx"] = self.total_batters_faced_pitchfx
        d["total_at_bats_pitchfx_complete"] = self.total_at_bats_pitchfx_complete
        d["total_at_bats_missing_pitchfx"] = self.total_at_bats_missing_pitchfx
        d["total_at_bats_extra_pitchfx"] = self.total_at_bats_extra_pitchfx
        d["total_at_bats_extra_pitchfx_removed"] = self.total_at_bats_extra_pitchfx_removed
        d["total_at_bats_pitchfx_error"] = self.total_at_bats_pitchfx_error
        d["total_at_bats_invalid_pitchfx"] = self.total_at_bats_invalid_pitchfx
        d["scraped_all_pitchfx_logs"] = self.scraped_all_pitchfx_logs
        d["combined_data_for_all_pitchfx_logs"] = self.combined_data_for_all_pitchfx_logs
        d["pitchfx_error_for_any_pitchfx_logs"] = self.pitchfx_error_for_any_pitchfx_logs
        d["pitchfx_is_valid_for_all_pitchfx_logs"] = self.pitchfx_is_valid_for_all_pitchfx_logs
        return d

    def display(self):
        game_status_dict = self.as_dict()
        title = f"SCRAPE STATUS FOR GAME: {self.bbref_game_id}"
        display_dict(game_status_dict, title=title)

    def status_report(self):
        bbref_game_id = self.bbref_game_id if self.bbref_game_id else "BBREF_GAME_ID IS MISSING!"
        bb_game_id = self.bb_game_id if self.bb_game_id else "BB_GAME_ID IS MISSING!"
        scraped_bbref_boxscore = "YES" if self.scraped_bbref_boxscore == 1 else "NO"
        scraped_brooks_pitch_logs = "YES" if self.scraped_brooks_pitch_logs == 1 else "NO"
        scraped_all_pitchfx_logs = "YES" if self.scraped_all_pitchfx_logs else "NO"
        combined_data_for_all_pitchfx_logs = (
            "YES" if self.combined_data_for_all_pitchfx_logs else "NO"
        )
        pitchfx_error_for_any_pitchfx_logs = (
            "YES" if self.pitchfx_error_for_any_pitchfx_logs else "NO"
        )
        total_pitchfx_removed_count = (
            self.total_duplicate_pitchfx_removed_count + self.total_extra_pitchfx_removed_count
        )
        return (
            f"BBRef Game ID................................: {bbref_game_id}\n"
            f"brooksbaseball.net Game ID...................: {bb_game_id}\n"
            f"Game Date-Time...............................: {self.game_date_time_str}\n"
            f"Scraped BBRef Boxscore.......................: {scraped_bbref_boxscore}\n"
            f"Scraped Brooks Pitch Logs....................: {scraped_brooks_pitch_logs}\n"
            f"PitchFx Logs Scraped.........................: {scraped_all_pitchfx_logs} "
            f"{self.total_pitch_apps_scraped_pitchfx}/{self.pitch_app_count_pitchfx}\n"
            "Combined BBRef/PitchFX Data..................: "
            f"{combined_data_for_all_pitchfx_logs}\n"
            f"PitchFX Data Errors (Valid AB/Invalid AB)....: {pitchfx_error_for_any_pitchfx_logs} "
            f"{self.total_pitch_apps_pitchfx_error}/{self.total_pitch_apps_invalid_pitchfx}\n"
            f"Pitch App Count (BBRef/Brooks)...............: "
            f"{self.pitch_app_count_bbref}/{self.pitch_app_count_brooks}\n"
            f"Pitch App Count (PFx/data/no data)...........: {self.pitch_app_count_pitchfx}/"
            f"{self.total_pitch_apps_with_pitchfx_data}/{self.total_pitch_apps_no_pitchfx_data}\n"
            f"Pitch Count (BBRef/Brooks/PFx)...............: {self.total_pitch_count_bbref}/"
            f"{self.total_pitch_count_pitch_logs}/{self.total_pitch_count_pitchfx}\n"
            "Pitch Count Audited (BBRef/PFx/Removed)......: "
            f"{self.total_pitch_count_bbref_audited}/{self.total_pitch_count_pitchfx_audited}/"
            f"{total_pitchfx_removed_count}\n"
        )

    def as_csv_dict(self):
        return dict_from_dataclass(self, GameScrapeStatusCsvRow, date_format=DATE_ONLY)

    @classmethod
    def get_csv_col_names(cls):
        return [name for name in GameScrapeStatusCsvRow.__dataclass_fields__.keys()]

    @classmethod
    def export_table_as_csv(cls, db_session):
        col_names = ",".join(cls.get_csv_col_names())
        csv_dicts = (obj.as_csv_dict() for obj in db_session.query(cls).all())
        csv_rows = (",".join(sanitize_row_dict(d, date_format=DATE_ONLY)) for d in csv_dicts)
        yield col_names
        for row in csv_rows:
            yield row

    @classmethod
    def find_by_bbref_game_id(cls, db_session, bbref_game_id):
        return db_session.query(cls).filter_by(bbref_game_id=bbref_game_id).first()

    @classmethod
    def find_by_bb_game_id(cls, db_session, bb_game_id):
        return db_session.query(cls).filter_by(bb_game_id=bb_game_id).first()

    @classmethod
    def get_all_scraped_bbref_game_ids_for_season(cls, db_session, season_id):
        return [
            game_status.bbref_game_id
            for game_status in db_session.query(cls).filter_by(season_id=season_id).all()
            if game_status.scraped_bbref_boxscore == 1
        ]

    @classmethod
    def get_all_unscraped_bbref_game_ids_for_season(cls, db_session, season_id):
        return [
            game_status.bbref_game_id
            for game_status in db_session.query(cls).filter_by(season_id=season_id).all()
            if game_status.scraped_bbref_boxscore == 0
        ]

    @classmethod
    def get_all_scraped_brooks_game_ids_for_season(cls, db_session, season_id):
        return [
            game_status.bb_game_id
            for game_status in db_session.query(cls).filter_by(season_id=season_id).all()
            if game_status.scraped_brooks_pitch_logs == 1
        ]

    @classmethod
    def get_all_unscraped_brooks_game_ids_for_season(cls, db_session, season_id):
        return [
            game_status.bb_game_id
            for game_status in db_session.query(cls).filter_by(season_id=season_id).all()
            if game_status.scraped_brooks_pitch_logs == 0
        ]

    @classmethod
    def get_all_bbref_game_ids(cls, db_session, season_id):
        return [
            game_status.bbref_game_id
            for game_status in db_session.query(cls).filter_by(season_id=season_id).all()
        ]

    @classmethod
    def get_game_id_dict(cls, db_session, season_id):
        return {
            game_status.bbref_game_id: game_status.id
            for game_status in db_session.query(cls).filter_by(season_id=season_id).all()
        }

    @classmethod
    def get_all_bbref_game_ids_for_date(cls, db_session, date):
        date_id = date.strftime(DATE_ONLY_TABLE_ID)
        return [
            game.bbref_game_id
            for game in db_session.query(cls).filter_by(scrape_status_date_id=int(date_id)).all()
        ]

    @classmethod
    def get_all_brooks_game_ids_for_date(cls, db_session, date):
        date_id = date.strftime(DATE_ONLY_TABLE_ID)
        return [
            game.bb_game_id
            for game in db_session.query(cls)
            .filter_by(scrape_status_date_id=int(date_id))
            .order_by(GameScrapeStatus.scrape_status_date_id)
            .all()
        ]

    @classmethod
    def get_all_bbref_game_ids_for_team(cls, db_session, team_id_br, year):
        season = Season.find_by_year(db_session, year)
        if not season:
            return None
        games_for_season = [
            game for game in db_session.query(cls).filter_by(season_id=season.id).all()
        ]
        games_for_season.sort(key=lambda x: x.scrape_status_date_id)
        bb_team_id = get_brooks_team_id(team_id_br)
        return [
            game.bbref_game_id
            for game in games_for_season
            if game.away_team_id_bb == bb_team_id or game.home_team_id_bb == bb_team_id
        ]

    @classmethod
    def get_game_id_map(cls, db_session):
        all_games = db_session.query(cls).all()
        return {g.bbref_game_id: g.id for g in all_games}


@accept_whitespaces
@dateformat(DATE_ONLY)
@dataclass
class GameScrapeStatusCsvRow:
    id: int
    game_date: datetime = None
    game_time_hour: int = 0
    game_time_minute: int = 0
    game_time_zone: str = None
    bbref_game_id: str = None
    bb_game_id: str = None
    scraped_bbref_boxscore: int = 0
    scraped_brooks_pitch_logs: int = 0
    combined_data_success: int = 0
    combined_data_fail: int = 0
    pitch_app_count_bbref: int = 0
    pitch_app_count_brooks: int = 0
    total_pitch_count_bbref: int = 0
