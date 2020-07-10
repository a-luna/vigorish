from datetime import datetime
from dateutil import tz

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

from vigorish.config.database import Base
from vigorish.util.dt_format_strings import DT_STR_FORMAT, DATE_ONLY_TABLE_ID, DATE_ONLY_2
from vigorish.util.list_helpers import display_dict


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
        )

    @hybrid_property
    def pitch_app_count_pitchfx(self):
        return len(self.scrape_status_pitchfx) if self.scrape_status_pitchfx else 0

    @hybrid_property
    def total_pitch_apps_scraped_pitchfx(self):
        return sum(pfx.scraped_pitchfx for pfx in self.scrape_status_pitchfx)

    @hybrid_property
    def total_pitch_apps_no_pitchfx_data(self):
        return sum(pfx.no_pitchfx_data for pfx in self.scrape_status_pitchfx)

    @hybrid_property
    def total_pitch_apps_with_pitchfx_data(self):
        return self.pitch_app_count_pitchfx - self.total_pitch_apps_no_pitchfx_data

    @hybrid_property
    def total_pitch_apps_combined_data(self):
        return sum(pfx.combined_pitchfx_bbref_data for pfx in self.scrape_status_pitchfx)

    @hybrid_property
    def total_pitch_count_pitch_logs(self):
        return (
            sum(pfx.pitch_count_pitch_log for pfx in self.scrape_status_pitchfx)
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
    def total_duplicate_pitchfx_removed_count(self):
        return (
            sum(pfx.duplicate_pitchfx_removed_count for pfx in self.scrape_status_pitchfx)
            if self.scrape_status_pitchfx
            else 0
        )

    @hybrid_property
    def total_extra_pitchfx_removed_count(self):
        return (
            sum(pfx.extra_pitchfx_removed_count for pfx in self.scrape_status_pitchfx)
            if self.scrape_status_pitchfx
            else 0
        )

    @hybrid_property
    def total_pitch_apps_pitchfx_data_error(self):
        return sum(pfx.pitchfx_data_error for pfx in self.scrape_status_pitchfx)

    @hybrid_property
    def total_pitch_apps_pitchfx_is_valid(self):
        return sum(not pfx.pitchfx_data_error for pfx in self.scrape_status_pitchfx)

    @hybrid_property
    def percent_complete_pitchfx_logs_scraped(self):
        return (
            self.total_pitch_apps_scraped_pitchfx / float(self.pitch_app_count_pitchfx)
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
    def combined_data_for_all_pitchfx_logs(self):
        if not self.scraped_all_pitchfx_logs:
            return False
        if not self.scrape_status_pitchfx:
            return False
        return all(pfx.combined_pitchfx_bbref_data for pfx in self.scrape_status_pitchfx)

    @hybrid_property
    def pitchfx_data_error_for_any_pitchfx_logs(self):
        if not self.scraped_all_pitchfx_logs:
            return False
        if not self.scrape_status_pitchfx:
            return False
        return any(pfx.pitchfx_data_error for pfx in self.scrape_status_pitchfx)

    @hybrid_property
    def pitchfx_is_valid_for_all_pitchfx_logs(self):
        if not self.scraped_all_pitchfx_logs:
            return True
        if not self.scrape_status_pitchfx:
            return True
        return all(not pfx.pitchfx_data_error for pfx in self.scrape_status_pitchfx)

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
        d["combined_data_for_all_pitchfx_logs"] = self.combined_data_for_all_pitchfx_logs
        d["pitchfx_data_error_for_any_pitchfx_logs"] = self.pitchfx_data_error_for_any_pitchfx_logs
        d["pitchfx_is_valid_for_all_pitchfx_logs"] = self.pitchfx_is_valid_for_all_pitchfx_logs
        d["pitch_app_count_bbref"] = self.pitch_app_count_bbref
        d["pitch_app_count_brooks"] = self.pitch_app_count_brooks
        d["pitch_app_count_pitchfx"] = self.pitch_app_count_pitchfx
        d["total_pitch_apps_scraped_pitchfx"] = self.total_pitch_apps_scraped_pitchfx
        d["total_pitch_apps_no_pitchfx_data"] = self.total_pitch_apps_no_pitchfx_data
        d["total_pitch_apps_with_pitchfx_data"] = self.total_pitch_apps_with_pitchfx_data
        d["total_pitch_apps_combined_data"] = self.total_pitch_apps_combined_data
        d["total_pitch_count_pitch_logs"] = self.total_pitch_count_pitch_logs
        d["total_pitch_count_bbref"] = self.total_pitch_count_bbref
        d["total_pitch_count_bbref_audited"] = self.total_pitch_count_bbref_audited
        d["total_pitch_count_pitchfx"] = self.total_pitch_count_pitchfx
        d["total_pitch_count_pitchfx_audited"] = self.total_pitch_count_pitchfx_audited
        d["total_duplicate_pitchfx_removed_count"] = self.total_duplicate_pitchfx_removed_count
        d["total_extra_pitchfx_removed_count"] = self.total_extra_pitchfx_removed_count
        d["total_pitch_apps_pitchfx_data_error"] = self.total_pitch_apps_pitchfx_data_error
        d["total_pitch_apps_pitchfx_is_valid"] = self.total_pitch_apps_pitchfx_is_valid
        d["percent_complete_pitchfx_logs_scraped"] = self.percent_complete_pitchfx_logs_scraped
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
        pitchfx_data_error_for_any_pitchfx_logs = (
            "YES" if self.pitchfx_data_error_for_any_pitchfx_logs else "NO"
        )
        total_pitchfx_removed_count = (
            self.total_duplicate_pitchfx_removed_count + self.total_extra_pitchfx_removed_count
        )
        return (
            f"BBRef Game ID............................: {bbref_game_id}\n"
            f"brooksbaseball.net Game ID...............: {bb_game_id}\n"
            f"Game Date-Time...........................: {self.game_date_time_str}\n"
            f"Scraped BBRef Boxscore...................: {scraped_bbref_boxscore}\n"
            f"Scraped Brooks Pitch Logs................: {scraped_brooks_pitch_logs}\n"
            f"PitchFx Logs Scraped.....................: {scraped_all_pitchfx_logs} "
            f"{self.total_pitch_apps_scraped_pitchfx}/{self.pitch_app_count_pitchfx}\n"
            f"Combined BBRef/PitchFX Data..............: {combined_data_for_all_pitchfx_logs}\n"
            f"PitchFX Data Errors (Error/Total)........: {pitchfx_data_error_for_any_pitchfx_logs} "
            f"{self.total_pitch_apps_pitchfx_data_error}/{self.pitch_app_count_pitchfx}\n"
            f"Pitch App Count (BBRef/Brooks)...........: "
            f"{self.pitch_app_count_bbref}/{self.pitch_app_count_brooks}\n"
            f"Pitch App Count (PFx/data/no data).......: {self.pitch_app_count_pitchfx}/"
            f"{self.total_pitch_apps_with_pitchfx_data}/{self.total_pitch_apps_no_pitchfx_data}\n"
            f"Pitch Count (BBRef/Brooks/PFx)...........: {self.total_pitch_count_bbref}/"
            f"{self.total_pitch_count_pitch_logs}/{self.total_pitch_count_pitchfx}\n"
            f"Pitch Count Audited (BBRef/PFx/Removed)..: {self.total_pitch_count_bbref_audited}/"
            f"{self.total_pitch_count_pitchfx_audited}/{total_pitchfx_removed_count}\n"
        )

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
            for game in db_session.query(cls).filter_by(scrape_status_date_id=int(date_id)).all()
        ]
