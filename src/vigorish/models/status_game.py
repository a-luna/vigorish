from dataclasses import dataclass
from datetime import datetime

from dataclass_csv import accept_whitespaces, dateformat
from dateutil import tz
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship
from sqlalchemy.sql.elements import or_

import vigorish.database as db
from vigorish.util.dt_format_strings import (
    DATE_ONLY,
    DATE_ONLY_2,
    DATE_ONLY_TABLE_ID,
    DT_STR_FORMAT,
)
from vigorish.util.string_helpers import get_brooks_team_id, validate_brooks_game_id


class GameScrapeStatus(db.Base):
    __tablename__ = "scrape_status_game"
    id = Column(Integer, primary_key=True)
    game_date = Column(DateTime)
    game_time_hour = Column(Integer)
    game_time_minute = Column(Integer)
    game_time_zone = Column(String)
    bbref_game_id = Column(String, unique=True)
    bb_game_id = Column(String, unique=True)
    away_team_id_br = Column(String)
    home_team_id_br = Column(String)
    away_team_runs_scored = Column(Integer)
    home_team_runs_scored = Column(Integer)
    scraped_bbref_boxscore = Column(Integer, default=0)
    scraped_brooks_pitch_logs = Column(Integer, default=0)
    combined_data_success = Column(Integer, default=0)
    combined_data_fail = Column(Integer, default=0)
    imported_bat_stats = Column(Integer, default=0)
    imported_pitch_stats = Column(Integer, default=0)
    pitch_app_count_bbref = Column(Integer, default=0)
    pitch_app_count_brooks = Column(Integer, default=0)
    total_pitch_count_bbref = Column(Integer, default=0)
    scrape_status_date_id = Column(Integer, ForeignKey("scrape_status_date.id"))
    season_id = Column(Integer, ForeignKey("season.id"))

    pitch_apps = relationship("PitchAppScrapeStatus")
    pitchfx = relationship("PitchFx")

    @hybrid_property
    def game_start_time(self):
        game_start = datetime(
            year=self.game_date.year,
            month=self.game_date.month,
            day=self.game_date.day,
            hour=self.game_time_hour,
            minute=self.game_time_minute,
        )
        return (
            game_start.replace(tzinfo=tz.gettz(self.game_time_zone))
            if self.game_time_hour != 0 or self.game_time_minute != 0
            else None
        )

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
    def pitch_app_count_pitchfx(self):
        return len(self.pitch_apps)

    @hybrid_property
    def total_pitch_apps_scraped_pitchfx(self):
        return sum(pitch_app.scraped_pitchfx for pitch_app in self.pitch_apps)

    @hybrid_property
    def total_pitch_apps_no_pitchfx_data(self):
        return sum(pitch_app.no_pitchfx_data for pitch_app in self.pitch_apps)

    @hybrid_property
    def total_pitch_apps_with_pitchfx_data(self):
        return self.pitch_app_count_pitchfx - self.total_pitch_apps_no_pitchfx_data

    @hybrid_property
    def total_pitch_apps_combined_data(self):
        return sum(pitch_app.combined_pitchfx_bbref_data for pitch_app in self.pitch_apps)

    @hybrid_property
    def total_pitch_apps_pitchfx_error(self):
        return sum(pitch_app.pitchfx_error for pitch_app in self.pitch_apps)

    @hybrid_property
    def total_pitch_apps_invalid_pitchfx(self):
        return sum(pitch_app.invalid_pitchfx for pitch_app in self.pitch_apps)

    @hybrid_property
    def total_pitch_apps_pitchfx_is_valid(self):
        return sum(not (pfx.pitchfx_error or pfx.invalid_pitchfx) for pfx in self.pitch_apps)

    @hybrid_property
    def total_pitch_count_pitch_logs(self):
        return sum(pitch_app.pitch_count_pitch_log for pitch_app in self.pitch_apps)

    @hybrid_property
    def total_pitch_count_bbref_audited(self):
        return sum(pitch_app.pitch_count_bbref for pitch_app in self.pitch_apps)

    @hybrid_property
    def total_pitch_count_pitchfx(self):
        return sum(pitch_app.pitch_count_pitchfx for pitch_app in self.pitch_apps)

    @hybrid_property
    def total_pitch_count_pitchfx_audited(self):
        return sum(pitch_app.pitch_count_pitchfx_audited for pitch_app in self.pitch_apps)

    @hybrid_property
    def total_missing_pitchfx_count(self):
        return sum(pitch_app.missing_pitchfx_count for pitch_app in self.pitch_apps)

    @hybrid_property
    def total_removed_pitchfx_count(self):
        return sum(pitch_app.removed_pitchfx_count for pitch_app in self.pitch_apps)

    @hybrid_property
    def total_batters_faced_bbref(self):
        return sum(pitch_app.batters_faced_bbref for pitch_app in self.pitch_apps)

    @hybrid_property
    def total_batters_faced_pitchfx(self):
        return sum(pitch_app.batters_faced_pitchfx for pitch_app in self.pitch_apps)

    @hybrid_property
    def total_at_bats_pitchfx_complete(self):
        return sum(pitch_app.total_at_bats_pitchfx_complete for pitch_app in self.pitch_apps)

    @hybrid_property
    def total_at_bats_missing_pitchfx(self):
        return sum(pitch_app.total_at_bats_missing_pitchfx for pitch_app in self.pitch_apps)

    @hybrid_property
    def total_at_bats_removed_pitchfx(self):
        return sum(pitch_app.total_at_bats_removed_pitchfx for pitch_app in self.pitch_apps)

    @hybrid_property
    def total_at_bats_pitchfx_error(self):
        return sum(pitch_app.total_at_bats_pitchfx_error for pitch_app in self.pitch_apps)

    @hybrid_property
    def total_at_bats_invalid_pitchfx(self):
        return sum(pitch_app.total_at_bats_invalid_pitchfx for pitch_app in self.pitch_apps)

    @hybrid_property
    def scraped_all_pitchfx_logs(self):
        return (
            True
            if not self.pitch_apps
            else False
            if not self.scraped_brooks_pitch_logs
            else self.pitch_app_count_pitchfx == self.total_pitch_apps_scraped_pitchfx
        )

    @hybrid_property
    def combined_data_for_all_pitchfx_logs(self):
        return (
            False
            if not self.pitch_apps or not self.scraped_all_pitchfx_logs
            else self.pitch_app_count_pitchfx == self.total_pitch_apps_combined_data
        )

    @hybrid_property
    def pitchfx_error_for_any_pitchfx_logs(self):
        return (
            False
            if not self.pitch_apps or not self.scraped_all_pitchfx_logs
            else any((pfx.pitchfx_error or pfx.invalid_pitchfx) for pfx in self.pitch_apps)
        )

    @hybrid_property
    def pitchfx_is_valid_for_all_pitchfx_logs(self):
        return (
            False
            if not self.pitch_apps or not self.scraped_all_pitchfx_logs
            else all(not (pfx.pitchfx_error or pfx.invalid_pitchfx) for pfx in self.pitch_apps)
        )

    def __repr__(self):
        return f"<GameScrapeStatus bbref_game_id={self.bbref_game_id}>"

    def as_dict(self):
        return {
            "bbref_game_id": self.bbref_game_id,
            "bb_game_id": self.bb_game_id,
            "game_date_time_str": self.game_date_time_str,
            "scraped_bbref_boxscore": self.scraped_bbref_boxscore,
            "scraped_brooks_pitch_logs": self.scraped_brooks_pitch_logs,
            "combined_data_success": self.combined_data_success,
            "combined_data_fail": self.combined_data_fail,
            "pitch_app_count_bbref": self.pitch_app_count_bbref,
            "pitch_app_count_brooks": self.pitch_app_count_brooks,
            "pitch_app_count_pitchfx": self.pitch_app_count_pitchfx,
            "total_pitch_apps_scraped_pitchfx": self.total_pitch_apps_scraped_pitchfx,
            "total_pitch_apps_no_pitchfx_data": self.total_pitch_apps_no_pitchfx_data,
            "total_pitch_apps_with_pitchfx_data": self.total_pitch_apps_with_pitchfx_data,
            "total_pitch_apps_combined_data": self.total_pitch_apps_combined_data,
            "total_pitch_apps_pitchfx_error": self.total_pitch_apps_pitchfx_error,
            "total_pitch_apps_invalid_pitchfx": self.total_pitch_apps_invalid_pitchfx,
            "total_pitch_apps_pitchfx_is_valid": self.total_pitch_apps_pitchfx_is_valid,
            "total_pitch_count_pitch_logs": self.total_pitch_count_pitch_logs,
            "total_pitch_count_bbref": self.total_pitch_count_bbref,
            "total_pitch_count_bbref_audited": self.total_pitch_count_bbref_audited,
            "total_pitch_count_pitchfx": self.total_pitch_count_pitchfx,
            "total_pitch_count_pitchfx_audited": self.total_pitch_count_pitchfx_audited,
            "total_missing_pitchfx_count": self.total_missing_pitchfx_count,
            "total_removed_pitchfx_count": self.total_removed_pitchfx_count,
            "total_batters_faced_bbref": self.total_batters_faced_bbref,
            "total_batters_faced_pitchfx": self.total_batters_faced_pitchfx,
            "total_at_bats_pitchfx_complete": self.total_at_bats_pitchfx_complete,
            "total_at_bats_missing_pitchfx": self.total_at_bats_missing_pitchfx,
            "total_at_bats_removed_pitchfx": self.total_at_bats_removed_pitchfx,
            "total_at_bats_pitchfx_error": self.total_at_bats_pitchfx_error,
            "total_at_bats_invalid_pitchfx": self.total_at_bats_invalid_pitchfx,
            "scraped_all_pitchfx_logs": self.scraped_all_pitchfx_logs,
            "combined_data_for_all_pitchfx_logs": self.combined_data_for_all_pitchfx_logs,
            "pitchfx_error_for_any_pitchfx_logs": self.pitchfx_error_for_any_pitchfx_logs,
            "pitchfx_is_valid_for_all_pitchfx_logs": self.pitchfx_is_valid_for_all_pitchfx_logs,
        }

    def status_report(self):
        bbref_game_id = self.bbref_game_id or "BBREF_GAME_ID IS MISSING!"
        bb_game_id = self.bb_game_id or "BB_GAME_ID IS MISSING!"
        scraped_bbref_boxscore = "YES" if self.scraped_bbref_boxscore == 1 else "NO"
        scraped_brooks_pitch_logs = "YES" if self.scraped_brooks_pitch_logs == 1 else "NO"
        scraped_all_pitchfx_logs = "YES" if self.scraped_all_pitchfx_logs else "NO"
        combined_data_for_all_pitchfx_logs = "YES" if self.combined_data_for_all_pitchfx_logs else "NO"
        pitchfx_error_for_any_pitchfx_logs = "YES" if self.pitchfx_error_for_any_pitchfx_logs else "NO"
        return [
            f"BBRef Game ID................................: {bbref_game_id}",
            f"brooksbaseball.net Game ID...................: {bb_game_id}",
            f"Game Date-Time...............................: {self.game_date_time_str}",
            f"Scraped BBRef Boxscore.......................: {scraped_bbref_boxscore}",
            f"Scraped Brooks Pitch Logs....................: {scraped_brooks_pitch_logs}",
            (
                f"PitchFx Logs Scraped.........................: {scraped_all_pitchfx_logs} "
                f"{self.total_pitch_apps_scraped_pitchfx}/{self.pitch_app_count_pitchfx}"
            ),
            f"Combined BBRef/PitchFX Data..................: {combined_data_for_all_pitchfx_logs}",
            (
                f"PitchFX Data Errors (Valid AB/Invalid AB)....: "
                f"{pitchfx_error_for_any_pitchfx_logs} "
                f"{self.total_pitch_apps_pitchfx_error}/{self.total_pitch_apps_invalid_pitchfx}"
            ),
            (
                f"Pitch App Count (BBRef/Brooks)...............: "
                f"{self.pitch_app_count_bbref}/{self.pitch_app_count_brooks}"
            ),
            (
                f"Pitch App Count (PFx/data/no data)...........: "
                f"{self.pitch_app_count_pitchfx}/"
                f"{self.total_pitch_apps_with_pitchfx_data}/"
                f"{self.total_pitch_apps_no_pitchfx_data}"
            ),
            (
                f"Pitch Count (BBRef/Brooks/PFx)...............: {self.total_pitch_count_bbref}/"
                f"{self.total_pitch_count_pitch_logs}/{self.total_pitch_count_pitchfx}"
            ),
            (
                "Pitch Count Audited (BBRef/PFx/Removed)......: "
                f"{self.total_pitch_count_bbref_audited}/"
                f"{self.total_pitch_count_pitchfx_audited}/"
                f"{self.total_removed_pitchfx_count}"
            ),
        ]

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
    def get_all_scraped_brooks_game_ids_for_season(cls, db_session, season_id):
        return [
            game_status.bb_game_id
            for game_status in db_session.query(cls).filter_by(season_id=season_id).all()
            if game_status.scraped_brooks_pitch_logs == 1
        ]

    @classmethod
    def get_all_bbref_game_ids(cls, db_session, season_id):
        return [game_status.bbref_game_id for game_status in db_session.query(cls).filter_by(season_id=season_id).all()]

    @classmethod
    def get_all_brooks_game_ids_for_date(cls, db_session, date):
        date_id = date.strftime(DATE_ONLY_TABLE_ID)
        return [
            game.bb_game_id
            for game in db_session.query(cls)
            .filter_by(scrape_status_date_id=int(date_id))
            .order_by(GameScrapeStatus.bb_game_id)
            .all()
        ]

    @classmethod
    def get_all_bbref_game_ids_for_team(cls, db_session, team_id_br, year):
        season = db.Season.find_by_year(db_session, year)
        if not season:
            return None
        games_for_season = list(db_session.query(cls).filter_by(season_id=season.id).all())
        games_for_season.sort(key=lambda x: x.scrape_status_date_id)
        bb_team_id = get_brooks_team_id(team_id_br)
        return [
            game.bbref_game_id
            for game in games_for_season
            if game.away_team_id_bb == bb_team_id or game.home_team_id_bb == bb_team_id
        ]

    @classmethod
    def get_all_games_for_team(cls, db_session, team_id_br, year, game_date=None):
        if not game_date:
            game_date = db.Season.get_most_recent_scraped_date(db_session, year)
        if game_date.year != year:
            raise ValueError('"year" and "game_date" parameters must belong to the same season')
        date_id = game_date.strftime(DATE_ONLY_TABLE_ID)
        season = db.Season.find_by_year(db_session, year)
        if not season:
            return None
        return (
            db_session.query(cls)
            .filter(cls.season_id == season.id)
            .filter(cls.scrape_status_date_id <= date_id)
            .filter(or_(cls.away_team_id_br == team_id_br, cls.home_team_id_br == team_id_br))
            .filter(cls.scraped_bbref_boxscore == 1)
            .order_by(cls.scrape_status_date_id)
            .all()
        )

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
    imported_bat_stats: int = 0
    imported_pitch_stats: int = 0
    pitch_app_count_bbref: int = 0
    pitch_app_count_brooks: int = 0
    total_pitch_count_bbref: int = 0
