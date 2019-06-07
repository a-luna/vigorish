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
    mat_view_scrape_status_game = relationship(
        "Date_Game_ScrapeStatusMV",
        backref="original",
        uselist=False,
        primaryjoin="DateScrapeStatus.id==Date_Game_ScrapeStatusMV.id",
        foreign_keys="Date_Game_ScrapeStatusMV.id")
    mat_view_scrape_status_pitch_app = relationship(
        "Date_PitchApp_ScrapeStatusMV",
        backref="original",
        uselist=False,
        primaryjoin="DateScrapeStatus.id==Date_PitchApp_ScrapeStatusMV.id",
        foreign_keys="Date_PitchApp_ScrapeStatusMV.id")

    @hybrid_property
    def game_date_str(self):
        return self.game_date.strftime(DATE_ONLY)

    @hybrid_property
    def total_games(self):
        return len(self.scrape_status_games) if self.scrape_status_games else 0

    @hybrid_property
    def total_bbref_boxscores_scraped(self):
        return self.mat_view_scrape_status_game.total_bbref_boxscores_scraped \
            if self.mat_view_scrape_status_game \
            and self.mat_view_scrape_status_game.total_bbref_boxscores_scraped else 0

    @hybrid_property
    def percent_complete_bbref_boxscores(self):
        if self.total_games == 0:
            return 0.0
        perc = self.total_bbref_boxscores_scraped / float(self.total_games)
        return perc * 100;

    @hybrid_property
    def scraped_all_bbref_boxscores(self):
        return self.percent_complete_bbref_boxscores == 100

    @hybrid_property
    def total_brooks_games_scraped(self):
        return self.mat_view_scrape_status_game.total_brooks_games_scraped \
            if self.mat_view_scrape_status_game \
            and self.mat_view_scrape_status_game.total_brooks_games_scraped else 0

    @hybrid_property
    def percent_complete_brooks_games(self):
        if self.total_games == 0:
            return 0.0
        perc = self.total_brooks_games_scraped / float(self.total_games)
        return perc * 100;

    @hybrid_property
    def scraped_all_brooks_pitch_logs(self):
        return self.percent_complete_brooks_games == 100

    @hybrid_property
    def total_pitch_appearances_bbref(self):
        return self.mat_view_scrape_status_game.total_pitch_appearances_bbref \
            if self.mat_view_scrape_status_game \
            and self.mat_view_scrape_status_game.total_pitch_appearances_bbref else 0

    @hybrid_property
    def total_pitch_appearances_brooks(self):
        return self.mat_view_scrape_status_game.total_pitch_appearances_brooks \
            if self.mat_view_scrape_status_game \
            and self.mat_view_scrape_status_game.total_pitch_appearances_brooks else 0

    @hybrid_property
    def pitch_appearance_count_difference(self):
        return abs(self.total_pitch_appearances_bbref - self.total_pitch_appearances_brooks)

    @hybrid_property
    def pitch_appearance_count_differs(self):
        return self.total_pitch_appearances_bbref != self.total_pitch_appearances_brooks

    @hybrid_property
    def total_pitch_count_bbref(self):
        return self.mat_view_scrape_status_game.total_pitch_count_bbref \
            if self.mat_view_scrape_status_game \
            and self.mat_view_scrape_status_game.total_pitch_count_bbref else 0

    @hybrid_property
    def total_pitch_count_brooks(self):
        return self.mat_view_scrape_status_game.total_pitch_count_brooks \
            if self.mat_view_scrape_status_game \
            and self.mat_view_scrape_status_game.total_pitch_count_brooks else 0

    @hybrid_property
    def pitch_count_difference(self):
        return abs(self.total_pitch_count_bbref - self.total_pitch_count_brooks)

    @hybrid_property
    def total_pitch_count_differs(self):
        return self.total_pitch_count_bbref != self.total_pitch_count_brooks

    @hybrid_property
    def total_pitchfx_logs_scraped(self):
        return self.mat_view_scrape_status_pitch_app.total_pitchfx_logs_scraped \
            if self.mat_view_scrape_status_pitch_app \
            and self.mat_view_scrape_status_pitch_app.total_pitchfx_logs_scraped else 0

    @hybrid_property
    def percent_complete_pitchfx_logs(self):
        if self.total_pitch_appearances_brooks == 0:
            return 0.0
        perc = self.total_pitchfx_logs_scraped / float(self.total_pitch_appearances_brooks)
        return perc * 100;

    @hybrid_property
    def scraped_all_pitchfx_logs(self):
        return self.percent_complete_pitchfx_logs == 100

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
            return "Scraped All Game Data"
        elif self.scraped_only_both_bbref_boxscores_and_brooks_pitch_logs:
            return "Scraped Only Both BBref Boxscores and Brooks Pitch Logs"
        elif self.scraped_only_brooks_pitch_logs:
            return "Scraped Only Brooks Pitch Logs"
        elif self.scraped_only_bbref_boxscores:
            return "Scraped Only BBref Boxscores"
        elif self.scraped_only_both_daily_dash:
            return "Scraped Only Both Daily Dashboards"
        elif self.scraped_only_brooks_daily_dash:
            return "Scraped Only Brooks Daily Dashboard"
        elif self.scraped_only_bbref_daily_dash:
            return "Scraped Only BBref Daily Dashboard"
        elif self.scraped_no_data:
            return "No Data Has Been Scraped"
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
        d["percent_complete_bbref_boxscores"] = f"{self.percent_complete_bbref_boxscores:01.0f}%"
        d["total_brooks_games_scraped"] = self.total_brooks_games_scraped
        d["percent_complete_brooks_games"] = f"{self.percent_complete_brooks_games:01.0f}%"
        d["total_pitch_appearances_bbref"] = self.total_pitch_appearances_bbref
        d["total_pitch_appearances_brooks"] = self.total_pitch_appearances_brooks
        d["total_pitch_count_bbref"] = self.total_pitch_count_bbref
        d["total_pitch_count_brooks"] = self.total_pitch_count_brooks
        return d

    def status_report(self):
        scraped_daily_bbref = "YES" if self.scraped_daily_dash_bbref == 1 else "NO"
        scraped_daily_brooks = "YES" if self.scraped_daily_dash_brooks == 1 else "NO"
        return (
            f"Overall Status For Date.................: {self.scrape_status_description}\n"
            f"Scraped Daily Dashboard (BBRef/Brooks)..: {scraped_daily_bbref}/{scraped_daily_brooks}\n"
            f"Games Scraped (Total/BBRef/Brooks)......: {self.total_games}/{self.total_bbref_boxscores_scraped}/{self.total_brooks_games_scraped}\n"
            f"PitchFX Logs Scraped (Total/Brooks).....: {self.total_pitch_appearances_brooks}/{self.total_pitchfx_logs_scraped}\n"
            f"Pitch Apperances (BBRef/Brooks/Diff)....: {self.total_pitch_appearances_bbref}/{self.total_pitch_appearances_brooks}/{self.pitch_appearance_count_difference}\n"
            f"Pitch Count (BBRef/Brooks/Diff).........: {self.total_pitch_count_bbref}/{self.total_pitch_count_brooks}/{self.pitch_count_difference}\n")

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


class Date_Game_ScrapeStatusMV(MaterializedView):
    __table__ = create_mat_view(
        Base.metadata,
        "date_game_status_mv",
        select([
            DateScrapeStatus.id.label("id"),
            func.sum(GameScrapeStatus.scraped_bbref_boxscore).label("total_bbref_boxscores_scraped"),
            func.sum(GameScrapeStatus.scraped_brooks_pitch_logs_for_game).label("total_brooks_games_scraped"),
            func.sum(GameScrapeStatus.pitch_app_count_bbref).label("total_pitch_appearances_bbref"),
            func.sum(GameScrapeStatus.pitch_app_count_brooks).label("total_pitch_appearances_brooks"),
            func.sum(GameScrapeStatus.total_pitch_count_bbref).label("total_pitch_count_bbref"),
            func.sum(GameScrapeStatus.total_pitch_count_brooks).label("total_pitch_count_brooks")])
        .select_from(join(
            DateScrapeStatus,
            GameScrapeStatus,
            DateScrapeStatus.id == GameScrapeStatus.scrape_status_date_id,
            isouter=True))
        .group_by(DateScrapeStatus.id))


class Date_PitchApp_ScrapeStatusMV(MaterializedView):
    __table__ = create_mat_view(
        Base.metadata,
        "date_pitch_app_status_mv",
        select([
            DateScrapeStatus.id.label("id"),
            func.sum(PitchAppearanceScrapeStatus.scraped_pitchfx).label("total_pitchfx_logs_scraped"),
            func.sum(PitchAppearanceScrapeStatus.pitch_count_pitch_log).label("total_pitch_count_pitch_log"),
            func.sum(PitchAppearanceScrapeStatus.pitch_count_pitchfx).label("total_pitch_count_pitchfx")])
        .select_from(join(
            DateScrapeStatus,
            PitchAppearanceScrapeStatus,
            DateScrapeStatus.id == PitchAppearanceScrapeStatus.scrape_status_date_id,
            isouter=True))
        .group_by(DateScrapeStatus.id))


Index("date_game_status_mv_id_idx", Date_Game_ScrapeStatusMV.id, unique=True)
Index("date_pitch_app_status_mv_id_idx", Date_PitchApp_ScrapeStatusMV.id, unique=True)
