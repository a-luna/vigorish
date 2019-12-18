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
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

from app.main.constants import MLB_DATA_SETS
from app.main.models.base import Base
from app.main.models.boxscore import Boxscore
from app.main.models.status_game import GameScrapeStatus
from app.main.models.status_pitch_appearance import PitchAppearanceScrapeStatus
from app.main.models.views.materialized_view import MaterializedView
from app.main.models.views.materialized_view_factory import create_mat_view
from app.main.util.list_functions import display_dict, report_dict
from app.main.util.dt_format_strings import DATE_ONLY, DATE_ONLY_TABLE_ID


class DateScrapeStatus(Base):

    __tablename__ = "scrape_status_date"
    id = Column(Integer, primary_key=True)
    game_date = Column(DateTime)
    scraped_daily_dash_bbref = Column(Integer, default=0)
    scraped_daily_dash_brooks = Column(Integer, default=0)
    game_count_bbref = Column(Integer, default=0)
    game_count_brooks = Column(Integer, default=0)
    season_id = Column(Integer, ForeignKey("season.id"))

    boxscores = relationship("Boxscore", backref="scrape_status_date")
    scrape_status_games = relationship("GameScrapeStatus", backref="scrape_status_date")
    scrape_status_pitchfx = relationship("PitchAppearanceScrapeStatus", backref="scrape_status_date")

    @hybrid_property
    def game_date_str(self):
        return self.game_date.strftime(DATE_ONLY)

    @hybrid_property
    def total_games(self):
        return len(self.scrape_status_games) if self.scrape_status_games else 0

    @hybrid_property
    def total_bbref_boxscores_scraped(self):
        return sum(game.scraped_bbref_boxscore for game in self.scrape_status_games)

    @hybrid_property
    def percent_complete_bbref_boxscores_scraped(self):
        return self.total_bbref_boxscores_scraped / float(self.total_games) \
            if self.total_games > 0 else 0.0

    @hybrid_property
    def scraped_all_bbref_boxscores(self):
        return all(game.scraped_bbref_boxscore == 1 for game in self.scrape_status_games)

    @hybrid_property
    def total_brooks_games_scraped(self):
        return sum(game.scraped_brooks_pitch_logs for game in self.scrape_status_games)

    @hybrid_property
    def percent_complete_brooks_games_scraped(self):
        return self.total_brooks_games_scraped / float(self.total_games) \
            if self.total_games > 0 else 0.0

    @hybrid_property
    def scraped_all_brooks_pitch_logs(self):
        return all(game.scraped_brooks_pitch_logs == 1 for game in self.scrape_status_games)

    @hybrid_property
    def pitch_appearance_count_bbref(self):
        return sum(game.pitch_app_count_bbref for game in self.scrape_status_games)

    @hybrid_property
    def pitch_appearance_count_brooks(self):
        return sum(game.pitch_app_count_brooks for game in self.scrape_status_games)

    @hybrid_property
    def pitch_appearance_count_pitchfx(self):
        return len(self.scrape_status_pitchfx)

    @hybrid_property
    def pitch_appearance_count_difference(self):
        return abs(self.pitch_appearance_count_bbref - self.pitch_appearance_count_brooks)

    @hybrid_property
    def pitch_appearance_count_differs(self):
        return self.pitch_appearance_count_bbref != self.pitch_appearance_count_brooks

    @hybrid_property
    def total_pitch_count_bbref(self):
        return sum(game.total_pitch_count_bbref for game in self.scrape_status_games)

    @hybrid_property
    def total_pitch_count_brooks(self):
        return sum(game.total_pitch_count_brooks for game in self.scrape_status_games)

    @hybrid_property
    def total_pitch_count_difference(self):
        return abs(self.total_pitch_count_bbref - self.total_pitch_count_brooks)

    @hybrid_property
    def total_pitch_count_differs(self):
        return self.total_pitch_count_bbref != self.total_pitch_count_brooks

    @hybrid_property
    def total_pitch_apps_no_pitchfx_data(self):
        return sum(pfx.no_pitchfx_data for pfx in self.scrape_status_pitchfx)

    @hybrid_property
    def total_pitchfx_logs_scraped(self):
        return sum(pfx.scraped_pitchfx for pfx in self.scrape_status_pitchfx)

    @hybrid_property
    def total_pitch_apps_with_pitchfx_data(self):
        return self.pitch_appearance_count_pitchfx() - self.total_pitch_apps_no_pitchfx_data

    @hybrid_property
    def percent_complete_pitchfx_logs_scraped(self):
        return self.total_pitchfx_logs_scraped / float(self.pitch_appearance_count_pitchfx) \
            if self.pitch_appearance_count_brooks > 0 else 0.0

    @hybrid_property
    def scraped_all_pitchfx_logs(self):
        return self.total_pitchfx_logs_scraped == self.pitch_appearance_count_pitchfx

    @hybrid_property
    def scraped_no_data(self):
        return self.scraped_daily_dash_bbref == 0 and self.scraped_daily_dash_brooks == 0

    @hybrid_property
    def scraped_only_bbref_daily_dash(self):
        return self.scraped_daily_dash_bbref == 1 and self.scraped_daily_dash_brooks == 0 \
            and not self.scraped_all_bbref_boxscores and not self.scraped_all_brooks_pitch_logs \
            and not self.scraped_all_pitchfx_logs

    @hybrid_property
    def scraped_only_brooks_daily_dash(self):
        return self.scraped_daily_dash_bbref == 0 and self.scraped_daily_dash_brooks == 1 \
            and not self.scraped_all_bbref_boxscores and not self.scraped_all_brooks_pitch_logs \
            and not self.scraped_all_pitchfx_logs

    @hybrid_property
    def scraped_only_both_daily_dash(self):
        return self.scraped_daily_dash_bbref == 1 and self.scraped_daily_dash_brooks == 1 \
            and not self.scraped_all_bbref_boxscores and not self.scraped_all_brooks_pitch_logs \
            and not self.scraped_all_pitchfx_logs

    @hybrid_property
    def scraped_only_bbref_boxscores(self):
        if (
            self.scraped_all_bbref_boxscores
            and not self.scraped_all_brooks_pitch_logs
            and not self.scraped_all_pitchfx_logs
        ):
            return True
        return False

    @hybrid_property
    def scraped_only_brooks_pitch_logs(self):
        if (
            self.scraped_all_brooks_pitch_logs
            and not self.scraped_all_bbref_boxscores
            and not self.scraped_all_pitchfx_logs
        ):
            return True
        return False

    @hybrid_property
    def scraped_only_both_bbref_boxscores_and_brooks_pitch_logs(self):
        if (
            self.scraped_all_bbref_boxscores
            and self.scraped_all_brooks_pitch_logs
            and not self.scraped_all_pitchfx_logs
        ):
            return True
        return False

    @hybrid_property
    def scraped_all_game_data(self):
        return self.scraped_all_bbref_boxscores and self.scraped_all_brooks_pitch_logs \
            and self.scraped_all_pitchfx_logs

    @hybrid_property
    def scrape_status_description(self):
        if self.scraped_all_game_data:
            return "Scraped all game data"
        elif self.scraped_only_both_bbref_boxscores_and_brooks_pitch_logs:
            return f"Missing Brooks pitchfx logs ({self.total_pitchfx_logs_scraped}/{self.pitch_appearance_count_brooks}, {self.percent_complete_pitchfx_logs_scraped:.0%})"
        elif self.scraped_only_brooks_pitch_logs:
            return "Scraped Brooks pitch logs, missing BBref boxscores"
        elif self.scraped_only_bbref_boxscores:
            return "Scraped BBref boxscores, missing Brooks pitch logs"
        elif self.scraped_only_both_daily_dash:
            return "Missing BBref boxscores and Brooks pitch logs"
        elif self.scraped_only_brooks_daily_dash:
            return "Scraped Brooks daily dashboard, missing BBref daily dash"
        elif self.scraped_only_bbref_daily_dash:
            return "Scraped BBref daily dashboard, missing Brooks daily dash"
        elif self.scraped_no_data:
            return "No data has been scraped"
        else:
            return "N/A"


    def __init__(self, game_date, season_id):
        date_str = game_date.strftime(DATE_ONLY_TABLE_ID)
        self.id = int(date_str)
        self.game_date = game_date
        self.season_id = season_id

    def __repr__(self):
        return f"<DateScrapeStatus date={self.game_date_str}, season_id={self.season_id}>"

    def as_dict(self):
        d = {c.name: getattr(self, c.name) for c in self.__table__.columns}
        d["game_date_str"] = self.game_date_str
        d["total_games"] = self.total_games
        d["total_bbref_boxscores_scraped"] = self.total_bbref_boxscores_scraped
        d["percent_complete_bbref_boxscores_scraped"] = f"{self.percent_complete_bbref_boxscores_scraped:01.0f}%"
        d["scraped_all_bbref_boxscores"] = self.scraped_all_bbref_boxscores
        d["total_brooks_games_scraped"] = self.total_brooks_games_scraped
        d["percent_complete_brooks_games_scraped"] = f"{self.percent_complete_brooks_games_scraped:01.0f}%"
        d["scraped_all_brooks_pitch_logs"] = self.scraped_all_brooks_pitch_logs
        d["pitch_appearance_count_bbref"] = self.pitch_appearance_count_bbref
        d["pitch_appearance_count_brooks"] = self.pitch_appearance_count_brooks
        d["pitch_appearance_count_difference"] = self.pitch_appearance_count_difference
        d["pitch_appearance_count_differs"] = self.pitch_appearance_count_differs
        d["total_pitch_apps_no_pitchfx_data"] = self.total_pitch_apps_no_pitchfx_data
        d["total_pitchfx_logs_scraped"] = self.total_pitchfx_logs_scraped
        d["total_pitch_apps_with_pitchfx_data"] = self.total_pitch_apps_with_pitchfx_data
        d["percent_complete_pitchfx_logs_scraped"] = self.percent_complete_pitchfx_logs_scraped
        d["scraped_all_pitchfx_logs"] = self.scraped_all_pitchfx_logs
        d["total_pitch_count_bbref"] = self.total_pitch_count_bbref
        d["total_pitch_count_brooks"] = self.total_pitch_count_brooks
        d["total_pitch_count_difference"] = self.total_pitch_count_difference
        d["total_pitch_count_differs"] = self.total_pitch_count_differs
        d["scraped_no_data"] = self.scraped_only_bbref_daily_dash
        d["scraped_only_bbref_daily_dash"] = self.scraped_only_bbref_daily_dash
        d["scraped_only_brooks_daily_dash"] = self.scraped_only_brooks_daily_dash
        d["scraped_only_both_daily_dash"] = self.scraped_only_both_daily_dash
        d["scraped_only_bbref_boxscores"] = self.scraped_only_bbref_boxscores
        d["scraped_only_brooks_pitch_logs"] = self.scraped_only_brooks_pitch_logs
        d["scraped_only_both_bbref_boxscores_and_brooks_pitch_logs"] = self.scraped_only_both_bbref_boxscores_and_brooks_pitch_logs
        d["scraped_all_game_data"] = self.scraped_all_game_data
        d["scrape_status_description"] = self.scrape_status_description
        return d

    def status_report(self):
        scraped_daily_bbref = "YES" if self.scraped_daily_dash_bbref == 1 else "NO"
        scraped_daily_brooks = "YES" if self.scraped_daily_dash_brooks == 1 else "NO"
        scraped_bbref_boxscores = "YES" if self.scraped_all_bbref_boxscores else "NO"
        scraped_brooks_pitch_logs = "YES" if self.scraped_all_brooks_pitch_logs else "NO"
        scraped_brooks_pitchfx = "YES" if self.scraped_all_pitchfx_logs else "NO"
        return (
            f"Overall Status For Date...................: {self.scrape_status_description}\n"
            f"Scraped Daily Dashboard (BBRef/Brooks)....: {scraped_daily_bbref}/{scraped_daily_brooks}\n"
            f"BBref Boxscores Scraped...................: {scraped_bbref_boxscores} {self.total_bbref_boxscores_scraped}/{self.total_games}\n"
            f"Brooks Games Scraped......................: {scraped_brooks_pitch_logs} {self.total_brooks_games_scraped}/{self.total_games}\n"
            f"PitchFX Logs Scraped......................: {scraped_brooks_pitchfx} {self.total_pitchfx_logs_scraped}/{self.pitch_appearance_count_pitchfx()} ({self.percent_complete_pitchfx_logs_scraped:.0%})\n"
            f"Pitch App Counts (br/bb/pfx_data/pfx_no)..: {self.pitch_appearance_count_bbref}/{self.pitch_appearance_count_brooks}/{self.total_pitch_apps_with_pitchfx_data}/{self.total_pitch_apps_no_pitchfx_data}\n"
            f"Pitch Count (BBRef/Brooks/Diff)...........: {self.total_pitch_count_bbref}/{self.total_pitch_count_brooks}/{self.total_pitch_count_difference}\n")

    @classmethod
    def find_by_date(cls, session, game_date):
        date_str = game_date.strftime(DATE_ONLY_TABLE_ID)
        return session.query(cls).get(int(date_str))

    @classmethod
    def get_all_bbref_scraped_dates_for_season(cls, session, season_id):
        return [
            date_status.game_date
            for date_status in session.query(cls).filter_by(season_id=season_id).all()
            if date_status.scraped_daily_dash_bbref == 1]

    @classmethod
    def get_all_bbref_unscraped_dates_for_season(cls, session, season_id):
        return [
            date_status.game_date
            for date_status in session.query(cls).filter_by(season_id=season_id).all()
            if date_status.scraped_daily_dash_bbref == 0]

    @classmethod
    def get_all_brooks_scraped_dates_for_season(cls, session, season_id):
        return [
            date_status.game_date
            for date_status in session.query(cls).filter_by(season_id=season_id).all()
            if date_status.scraped_daily_dash_brooks == 1]

    @classmethod
    def get_all_brooks_unscraped_dates_for_season(cls, session, season_id):
        return [
            date_status.game_date
            for date_status in session.query(cls).filter_by(season_id=season_id).all()
            if date_status.scraped_daily_dash_brooks == 0]

    @classmethod
    def get_unscraped_pitch_appearances_for_date(cls, session, game_date):
        date_status = cls.find_by_date(session, game_date)
        return [
            pitch_app for pitch_app in date_status.scrape_status_pitchfx
            if pitch_app.scraped_pitchfx == 0]

    @classmethod
    def get_unscraped_pitch_app_ids_for_date(cls, session, game_date):
        unscraped_pitch_apps = cls.get_unscraped_pitch_appearances_for_date(session, game_date)
        return [pitch_app.pitch_app_id for pitch_app in unscraped_pitch_apps]

    @classmethod
    def verify_bbref_daily_dashboard_scraped_for_date(cls, session, game_date):
        date_status = cls.find_by_date(session, game_date)
        return date_status.scraped_daily_dash_bbref == 1 if date_status else False

    @classmethod
    def verify_brooks_daily_dashboard_scraped_for_date(cls, session, game_date):
        date_status = cls.find_by_date(session, game_date)
        return date_status.scraped_daily_dash_brooks == 1 if date_status else False

    @classmethod
    def verify_all_bbref_boxscores_scraped_for_date(cls, session, game_date):
        date_status = cls.find_by_date(session, game_date)
        return date_status.scraped_all_bbref_boxscores if date_status else False

    @classmethod
    def verify_all_brooks_pitch_logs_scraped_for_date(cls, session, game_date):
        date_status = cls.find_by_date(session, game_date)
        return date_status.scraped_all_brooks_pitch_logs if date_status else False

    @classmethod
    def verify_all_brooks_pitchfx_scraped_for_date(cls, session, game_date):
        date_status = cls.find_by_date(session, game_date)
        return date_status.scraped_all_pitchfx_logs if date_status else False

    @classmethod
    def get_all_bbref_game_ids_for_date(cls, session, game_date):
        date_status = cls.find_by_date(session, game_date)
        if not date_status:
            return None
        return [
            game_status.bbref_game_id
            for game_status
            in date_status.scrape_status_games]

    @classmethod
    def get_all_brooks_game_ids_for_date(cls, session, game_date):
        date_status = cls.find_by_date(session, game_date)
        if not date_status:
            return None
        return [
            game_status.bb_game_id
            for game_status
            in date_status.scrape_status_games]
