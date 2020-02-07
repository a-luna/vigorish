from datetime import datetime
from dateutil import tz

from sqlalchemy import (
    Index,
    Column,
    Boolean,
    Integer,
    String,
    DateTime,
    ForeignKey,
    select,
    func,
    join,
)
from sqlalchemy import Column, Boolean, Integer, String, DateTime, ForeignKey
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

from app.main.models.base import Base
from app.main.models.status_pitch_appearance import PitchAppearanceScrapeStatus
from app.main.models.views.materialized_view import MaterializedView
from app.main.models.views.materialized_view_factory import create_mat_view
from app.main.util.dt_format_strings import DT_STR_FORMAT, DATE_ONLY_TABLE_ID
from app.main.util.list_functions import display_dict


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
    pitch_app_count_bbref = Column(Integer, default=0)
    pitch_app_count_brooks = Column(Integer, default=0)
    total_pitch_count_bbref = Column(Integer, default=0)
    total_pitch_count_brooks = Column(Integer, default=0)
    scrape_status_date_id = Column(Integer, ForeignKey("scrape_status_date.id"))
    season_id = Column(Integer, ForeignKey("season.id"))

    scrape_status_pitchfx = relationship(
        "PitchAppearanceScrapeStatus", backref="scrape_status_game"
    )

    @hybrid_property
    def game_start_time(self):
        hour_adjusted = self.game_time_hour
        if self.game_time_hour != 11 and self.game_time_hour != 12:
            hour_adjusted = self.game_time_hour + 12
        game_start_time = datetime(
            year=self.game_date.year,
            month=self.game_date.month,
            day=self.game_date.day,
            hour=hour_adjusted,
            minute=self.game_time_minute,
        )
        return game_start_time.replace(tzinfo=tz.gettz(self.game_time_zone))

    @hybrid_property
    def game_date_time_str(self):
        return (
            self.game_start_time.strftime(DT_STR_FORMAT)
            if self.game_start_time
            else None
        )

    @hybrid_property
    def total_pitch_count_pitchfx(self):
        return (
            sum(pfx.pitch_count_pitchfx for pfx in self.scrape_status_pitchfx)
            if self.scrape_status_pitchfx
            else 0
        )

    @hybrid_property
    def total_pitch_count_pitchfx_audited(self):
        return (
            sum(pfx.pitch_count_pitchfx_audited for pfx in self.scrape_status_pitchfx)
            if self.scrape_status_pitchfx
            else 0
        )

    @hybrid_property
    def total_pitch_count_bbref_audited(self):
        return (
            sum(pfx.pitch_count_bbref for pfx in self.scrape_status_pitchfx)
            if self.scrape_status_pitchfx
            else 0
        )

    @hybrid_property
    def total_duplicate_pitchfx_removed_count(self):
        return (
            sum(
                pfx.duplicate_pitchfx_removed_count for pfx in self.scrape_status_pitchfx
            )
            if self.scrape_status_pitchfx
            else 0
        )

    @hybrid_property
    def pitch_app_count_pitchfx(self):
        return len(self.scrape_status_pitchfx) if self.scrape_status_pitchfx else 0

    @hybrid_property
    def pitch_app_count_pitchfx_audited(self):
        return sum(pfx.pitch_app_audited for pfx in self.scrape_status_pitchfx)

    @hybrid_property
    def pitch_app_count_missing_pfx_is_valid(self):
        return sum(pfx.missing_pitchfx_is_valid for pfx in self.scrape_status_pitchfx)

    @hybrid_property
    def total_pitch_apps_no_pitchfx_data(self):
        return sum(pfx.no_pitchfx_data for pfx in self.scrape_status_pitchfx)

    @hybrid_property
    def total_pitchfx_logs_scraped(self):
        return sum(pfx.scraped_pitchfx for pfx in self.scrape_status_pitchfx)

    @hybrid_property
    def total_pitch_apps_with_pitchfx_data(self):
        return self.pitch_app_count_pitchfx - self.total_pitch_apps_no_pitchfx_data

    @hybrid_property
    def percent_complete_pitchfx_logs_scraped(self):
        return (
            self.total_pitchfx_logs_scraped / float(self.pitch_app_count_pitchfx)
            if self.pitch_app_count_pitchfx > 0
            else 0.0
        )

    @hybrid_property
    def scraped_all_pitchfx_logs(self):
        if not self.scraped_brooks_pitch_logs:
            return False
        if not self.scrape_status_pitchfx:
            return True
        return all(pfx.scraped_pitchfx for pfx in self.scrape_status_pitchfx)

    @hybrid_property
    def pitch_data_was_audited(self):
        if not self.scraped_all_pitchfx_logs:
            return False
        if not self.scrape_status_pitchfx:
            return True
        return all(pfx.pitch_app_audited for pfx in self.scrape_status_pitchfx)

    @hybrid_property
    def all_missing_pitchfx_is_valid(self):
        if not self.scraped_all_pitchfx_logs:
            return False
        if not self.scrape_status_pitchfx:
            return True
        return all(pfx.missing_pitchfx_is_valid for pfx in self.scrape_status_pitchfx)

    def __repr__(self):
        return f"<GameScrapeStatus bbref_game_id={self.bbref_game_id}>"

    def as_dict(self):
        d = {}
        d["bb_game_id"] = self.bb_game_id
        d["game_date_time_str"] = self.game_date_time_str
        d["scraped_bbref_boxscore"] = self.scraped_bbref_boxscore
        d["scraped_brooks_pitch_logs"] = self.scraped_brooks_pitch_logs
        d["scraped_all_pitchfx_logs"] = self.scraped_all_pitchfx_logs
        d["pitch_app_count_bbref"] = self.pitch_app_count_bbref
        d["pitch_app_count_brooks"] = self.pitch_app_count_brooks
        d["pitch_app_count_pitchfx"] = self.pitch_app_count_pitchfx
        d["total_pitch_apps_no_pitchfx_data"] = self.total_pitch_apps_no_pitchfx_data
        d["total_pitch_apps_with_pitchfx_data"] = self.total_pitch_apps_with_pitchfx_data
        d["total_pitchfx_logs_scraped"] = self.total_pitchfx_logs_scraped
        d[
            "percent_complete_pitchfx_logs_scraped"
        ] = self.percent_complete_pitchfx_logs_scraped
        d["total_pitch_count_bbref"] = self.total_pitch_count_bbref
        d["total_pitch_count_brooks"] = self.total_pitch_count_brooks
        d["total_pitch_count_pitchfx"] = self.total_pitch_count_pitchfx
        return d

    def display(self):
        season_dict = self.as_dict()
        season_dict["game_start_time"] = self.game_date_time_str
        title = f"SCRAPE STATUS FOR GAME: {self.bbref_game_id}"
        display_dict(season_dict, title=title)

    def status_report(self):
        scraped_bbref_boxscore = "YES" if self.scraped_bbref_boxscore == 1 else "NO"
        scraped_brooks_pitch_logs = (
            "YES" if self.scraped_brooks_pitch_logs == 1 else "NO"
        )
        scraped_all_pitchfx_logs = "YES" if self.scraped_all_pitchfx_logs else "NO"
        pitchfx_bbref_audit_performed = "YES" if self.pitch_data_was_audited else "NO"
        all_missing_pitchfx_is_valid = (
            "YES" if self.all_missing_pitchfx_is_valid else "NO"
        )
        return (
            f"BBRef Game ID.........................: {self.bbref_game_id}\n"
            f"BrooksBaseball Game ID................: {self.bb_game_id}\n"
            f"Date-Time.............................: {self.game_date_time_str}\n"
            f"Scraped BBRef Boxscore................: {scraped_bbref_boxscore}\n"
            f"Scraped Brooks Pitch Logs.............: {scraped_brooks_pitch_logs}\n"
            f"PitchFx Logs Scraped..................: {scraped_all_pitchfx_logs} {self.total_pitchfx_logs_scraped}/{self.pitch_app_count_pitchfx} ({self.percent_complete_pitchfx_logs_scraped:.0%})\n"
            f"Pitch App Count (BBRef/Brooks/PFx)....: {self.pitch_app_count_bbref}/{self.pitch_app_count_brooks}/{self.pitch_app_count_pitchfx}\n"
            f"Pitch App Count (PFx/data/no data)....: {self.pitch_app_count_pitchfx}/{self.total_pitch_apps_with_pitchfx_data}/{self.total_pitch_apps_no_pitchfx_data}\n"
            f"Pitch Count (BBRef/Brooks/PFx)........: {self.total_pitch_count_bbref}/{self.total_pitch_count_brooks}/{self.total_pitch_count_pitchfx}\n"
            f"PitchFx-BBRef Audit Performed.........: {pitchfx_bbref_audit_performed}\n"
            f"All Missing PitchFx Is Valid..........: {all_missing_pitchfx_is_valid}\n"
            f"Pitch Count Audited (BBRef/PFx/Dupe)..: {self.total_pitch_count_bbref_audited}/{self.total_pitch_count_pitchfx_audited}/{self.total_duplicate_pitchfx_removed_count}\n"
        )

    @classmethod
    def find_by_bbref_game_id(cls, session, bbref_game_id):
        return session.query(cls).filter_by(bbref_game_id=bbref_game_id).first()

    @classmethod
    def find_by_bb_game_id(cls, session, bb_game_id):
        return session.query(cls).filter_by(bb_game_id=bb_game_id).first()

    @classmethod
    def get_all_scraped_bbref_game_ids_for_season(cls, session, season_id):
        return [
            game_status.bbref_game_id
            for game_status in session.query(cls).filter_by(season_id=season_id).all()
            if game_status.scraped_bbref_boxscore == 1
        ]

    @classmethod
    def get_all_unscraped_bbref_game_ids_for_season(cls, session, season_id):
        return [
            game_status.bbref_game_id
            for game_status in session.query(cls).filter_by(season_id=season_id).all()
            if game_status.scraped_bbref_boxscore == 0
        ]

    @classmethod
    def get_all_scraped_brooks_game_ids_for_season(cls, session, season_id):
        return [
            game_status.bb_game_id
            for game_status in session.query(cls).filter_by(season_id=season_id).all()
            if game_status.scraped_brooks_pitch_logs == 1
        ]

    @classmethod
    def get_all_unscraped_brooks_game_ids_for_season(cls, session, season_id):
        return [
            game_status.bb_game_id
            for game_status in session.query(cls).filter_by(season_id=season_id).all()
            if game_status.scraped_brooks_pitch_logs == 0
        ]

    @classmethod
    def get_all_bbref_game_ids(cls, session, season_id):
        return [
            game_status.bbref_game_id
            for game_status in session.query(cls).filter_by(season_id=season_id).all()
        ]

    @classmethod
    def get_game_id_dict(cls, session, season_id):
        return {
            game_status.bbref_game_id: game_status.id
            for game_status in session.query(cls).filter_by(season_id=season_id).all()
        }

    @classmethod
    def get_all_bbref_game_ids_for_date(cls, session, date):
        date_id = date.strftime(DATE_ONLY_TABLE_ID)
        return [
            game.bbref_game_id
            for game in session.query(cls)
            .filter_by(scrape_status_date_id=int(date_id))
            .all()
        ]

    @classmethod
    def get_all_brooks_game_ids_for_date(cls, session, date):
        date_id = date.strftime(DATE_ONLY_TABLE_ID)
        return [
            game.bb_game_id
            for game in session.query(cls)
            .filter_by(scrape_status_date_id=int(date_id))
            .all()
        ]
