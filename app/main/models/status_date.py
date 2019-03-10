from sqlalchemy import (
    Index, Column, Boolean, Integer, String, DateTime, ForeignKey, select, func, join
)
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

from app.main.constants import MLB_DATA_SETS
from app.main.models.base import Base
from app.main.models.boxscore import Boxscore
from app.main.models.status_game import GameScrapeStatus
from app.main.models.views.materialized_view import MaterializedView
from app.main.models.views.materialized_view_factory import create_mat_view
from app.main.util.list_functions import display_dict
from app.main.util.dt_format_strings import DATE_ONLY, DATE_ONLY_TABLE_ID

class DateScrapeStatus(Base):

    __tablename__ = 'scrape_status_date'
    id = Column(Integer, primary_key=True)
    game_date = Column(DateTime)
    scraped_daily_dash_bbref = Column(Integer, default=0)
    scraped_daily_dash_brooks = Column(Integer, default=0)
    game_count_bbref = Column(Integer, default=0)
    game_count_brooks = Column(Integer, default=0)
    season_id = Column(Integer, ForeignKey('season.id'))

    boxscores = relationship('Boxscore', backref='scrape_status_date')
    scrape_status_games = relationship('GameScrapeStatus', backref='scrape_status_date')
    mat_view = relationship(
        'DateScrapeStatusMV', backref='original',
        uselist=False,
        primaryjoin='DateScrapeStatus.id==DateScrapeStatusMV.id',
        foreign_keys='DateScrapeStatusMV.id'
    )

    @hybrid_property
    def game_date_str(self):
        return self.game_date.strftime(DATE_ONLY)

    @hybrid_property
    def total_games(self):
        if self.mat_view is not None:
            return self.mat_view.total_games

    @hybrid_property
    def total_bbref_boxscores_scraped(self):
        if self.mat_view is not None:
            return self.mat_view.total_bbref_boxscores_scraped

    @hybrid_property
    def percent_complete_bbref_boxscores(self):
        if self.mat_view is not None:
            if self.total_games and self.total_games > 0:
                perc = self.total_bbref_boxscores_scraped/float(self.total_games)
                return f'{perc*100:02.0f}%'

    @hybrid_property
    def total_brooks_games_scraped(self):
        if self.mat_view is not None:
            return self.mat_view.total_brooks_games_scraped

    @hybrid_property
    def percent_complete_brooks_games(self):
        if self.mat_view is not None:
            if self.total_games and self.total_games > 0:
                perc = self.total_brooks_games_scraped/float(self.total_games)
                return f'{perc*100:02.0f}%'

    @hybrid_property
    def total_pitch_appearances_bbref(self):
        if self.mat_view is not None:
            return self.mat_view.total_pitch_appearances_bbref

    @hybrid_property
    def total_pitch_appearances_brooks(self):
        if self.mat_view is not None:
            return self.mat_view.total_pitch_appearances_brooks

    @hybrid_property
    def total_pitch_count_bbref(self):
        if self.mat_view is not None:
            return self.mat_view.total_pitch_count_bbref

    @hybrid_property
    def total_pitch_count_brooks(self):
        if self.mat_view is not None:
            return self.mat_view.total_pitch_count_brooks

    def __init__(self, game_date, season_id):
        date_str = game_date.strftime(DATE_ONLY_TABLE_ID)
        self.id = int(date_str)
        self.game_date = game_date
        self.season_id = season_id

    def __repr__(self):
        return f'<DateScrapeStatus(date={self.game_date_str}, season_id={self.season_id})>'

    def as_dict(self):
        d = {c.name: getattr(self, c.name) for c in self.__table__.columns}
        d['game_date_str'] = self.game_date_str
        d['total_games'] = self.total_games
        d['total_bbref_boxscores_scraped'] = self.total_bbref_boxscores_scraped
        d['percent_complete_bbref_boxscores'] = self.percent_complete_bbref_boxscores
        d['total_brooks_games_scraped'] = self.total_brooks_games_scraped
        d['percent_complete_brooks_games'] = self.percent_complete_brooks_games
        d['total_pitch_appearances_bbref'] = self.total_pitch_appearances_bbref
        d['total_pitch_appearances_brooks'] = self.total_pitch_appearances_brooks
        d['total_pitch_count_bbref'] = self.total_pitch_count_bbref
        d['total_pitch_count_brooks'] = self.total_pitch_count_brooks
        return d

    def display(self):
        season_dict = self.as_dict()
        title = f'SCRAPE STATUS FOR DATE: {self.game_date_str}'
        display_dict(season_dict, title=title)

    @classmethod
    def find_by_date(cls, session, game_date):
        date_str = game_date.strftime(DATE_ONLY_TABLE_ID)
        return session.query(cls).get(int(date_str))


class DateScrapeStatusMV(MaterializedView):
    __table__ = create_mat_view(
        Base.metadata,
        "date_status_mv",
        select([
            DateScrapeStatus.id.label('id'),
            func.count(GameScrapeStatus.id).label('total_games'),
            func.sum(GameScrapeStatus.scraped_bbref_boxscore).label('total_bbref_boxscores_scraped'),
            func.sum(GameScrapeStatus.scraped_brooks_pitch_logs_for_game).label('total_brooks_games_scraped'),
            func.sum(GameScrapeStatus.pitch_app_count_bbref).label('total_pitch_appearances_bbref'),
            func.sum(GameScrapeStatus.pitch_app_count_brooks).label('total_pitch_appearances_brooks'),
            func.sum(GameScrapeStatus.total_pitch_count_bbref).label('total_pitch_count_bbref'),
            func.sum(GameScrapeStatus.total_pitch_count_brooks).label('total_pitch_count_brooks'),
        ]).select_from(join(
            DateScrapeStatus,
            GameScrapeStatus,
            DateScrapeStatus.id == GameScrapeStatus.scrape_status_date_id,
            isouter=True)
        ).group_by(DateScrapeStatus.id)
    )

Index('date_status_mv_id_idx', DateScrapeStatusMV.id, unique=True)