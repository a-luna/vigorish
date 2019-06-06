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
from app.main.util.dt_format_strings import DT_STR_FORMAT_ALL
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
    scraped_brooks_pitch_logs_for_game = Column(Integer, default=0)
    pitch_app_count_bbref = Column(Integer, default=0)
    pitch_app_count_brooks = Column(Integer, default=0)
    total_pitch_count_bbref = Column(Integer, default=0)
    total_pitch_count_brooks = Column(Integer, default=0)
    scrape_status_date_id = Column(Integer, ForeignKey("scrape_status_date.id"))
    season_id = Column(Integer, ForeignKey("season.id"))

    scrape_status_pitchfx = relationship("PitchAppearanceScrapeStatus", backref="scrape_status_game")
    mat_view = relationship(
        "Game_PitchApp_ScrapeStatusMV",
        backref="original",
        uselist=False,
        primaryjoin="GameScrapeStatus.id==Game_PitchApp_ScrapeStatusMV.id",
        foreign_keys="Game_PitchApp_ScrapeStatusMV.id")

    @hybrid_property
    def game_date_time(self):
        return datetime(
            year=self.game_date.year,
            month=self.game_date.month,
            day=self.game_date.day,
            hour=self.game_time_hour,
            minute=self.game_time_minute,
            tzinfo=tz.gettz(self.game_time_zone))

    @property
    def game_date_time_str(self):
        return self.game_date_time.strftime(DT_STR_FORMAT_ALL) if self.game_date_time else None

    def __repr__(self):
        return f"<GameScrapeStatus bbref_game_id={self.bbref_game_id}>"

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def display(self):
        season_dict = self.as_dict()
        season_dict['game_date_time'] = self.game_date_time_str
        title = f"SCRAPE STATUS FOR GAME: {self.bbref_game_id}"
        display_dict(season_dict, title=title)

    @classmethod
    def find_by_bbref_game_id(cls, session, bbref_game_id):
        return session.query(cls).filter_by(bbref_game_id=bbref_game_id).first()

    @classmethod
    def find_by_bb_game_id(cls, session, bb_game_id):
        return session.query(cls).filter_by(bb_game_id=bb_game_id).first()

    @classmethod
    def get_all_scraped_bbref_game_ids_for_season(cls, session, season_id):
        return [
            game_status.bbref_game_id for game_status
            in session.query(cls).filter_by(season_id=season_id).all()
            if game_status.scraped_bbref_boxscore == 1]

    @classmethod
    def get_all_unscraped_bbref_game_ids_for_season(cls, session, season_id):
        return [
            game_status.bbref_game_id for game_status
            in session.query(cls).filter_by(season_id=season_id).all()
            if game_status.scraped_bbref_boxscore == 0]

    @classmethod
    def get_all_scraped_brooks_game_ids_for_season(cls, session, season_id):
        return [
            game_status.bb_game_id for game_status
            in session.query(cls).filter_by(season_id=season_id).all()
            if game_status.scraped_brooks_pitch_logs_for_game == 1]

    @classmethod
    def get_all_unscraped_brooks_game_ids_for_season(cls, session, season_id):
        return [
            game_status.bb_game_id for game_status
            in session.query(cls).filter_by(season_id=season_id).all()
            if game_status.scraped_brooks_pitch_logs_for_game == 0]

    @classmethod
    def get_all_bbref_game_ids(cls, session, season_id):
        return [
            game_status.bbref_game_id for game_status
            in session.query(cls).filter_by(season_id=season_id).all()]


class Game_PitchApp_ScrapeStatusMV(MaterializedView):
    __table__ = create_mat_view(
        Base.metadata,
        "game_pitch_app_status_mv",
        select([
            GameScrapeStatus.id.label("id"),
            func.sum(PitchAppearanceScrapeStatus.scraped_pitchfx).label("total_pitchfx_logs_scraped"),
            func.sum(PitchAppearanceScrapeStatus.pitch_count_pitch_log).label("total_pitch_count_pitch_log"),
            func.sum(PitchAppearanceScrapeStatus.pitch_count_pitchfx).label("total_pitch_count_pitchfx")])
        .select_from(join(
            GameScrapeStatus,
            PitchAppearanceScrapeStatus,
            GameScrapeStatus.id == PitchAppearanceScrapeStatus.scrape_status_game_id,
            isouter=True))
        .group_by(GameScrapeStatus.id))


Index("game_pitch_app_status_mv_id_idx", Game_PitchApp_ScrapeStatusMV.id, unique=True)
