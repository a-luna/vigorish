"""Db model that describes a MLB season and tracks data scraping progress."""
from sqlalchemy import Column, Boolean, Index, Integer, DateTime, select, func, join
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

from app.main.constants import SEASON_TYPE_DICT
from app.main.models.base import Base
from app.main.models.status_date import DateScrapeStatus
from app.main.models.views.materialized_view import MaterializedView
from app.main.models.views.materialized_view_factory import create_mat_view
from app.main.util.datetime_util import get_date_range
from app.main.util.dt_format_strings import DATE_ONLY
from app.main.util.list_functions import display_dict
from app.main.util.result import Result


class Season(Base):
    """Db model that describes a MLB season and tracks data scraping progress."""

    __tablename__ = 'season'
    id = Column(Integer, primary_key=True)
    year = Column(Integer, nullable=False)
    start_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime, nullable=False)
    asg_date = Column(DateTime, nullable=True)
    season_type = Column(
        postgresql.ENUM(
            'None',
            SEASON_TYPE_DICT['pre'],
            SEASON_TYPE_DICT['reg'],
            SEASON_TYPE_DICT['post'],
            name='season_type_enum'
        ), default='None'
    )

    scrape_status_dates = relationship('DateScrapeStatus', backref='season')
    scrape_status_games = relationship('GameScrapeStatus', backref='season')
    boxscores = relationship('Boxscore', backref='season')
    pitching_stats = relationship('GamePitchStats', backref='season')
    batting_stats = relationship('GameBatStats', backref='season')

    mat_view = relationship(
        'SeasonStatusMV', backref='original',
        uselist=False,
        primaryjoin='Season.id==SeasonStatusMV.id',
        foreign_keys='SeasonStatusMV.id'
    )

    @hybrid_property
    def name(self):
        return f'MLB {self.year} {self.season_type}'

    @hybrid_property
    def start_date_str(self):
        return self.start_date.strftime(DATE_ONLY)

    @hybrid_property
    def end_date_str(self):
        return self.end_date.strftime(DATE_ONLY)

    @hybrid_property
    def total_days(self):
        if self.mat_view is not None: # if None, mat_view needs refreshing
            return self.mat_view.total_days

    @hybrid_property
    def total_days_scraped_bbref(self):
        if self.mat_view is not None:
            return self.mat_view.total_days_scraped_bbref

    @hybrid_property
    def percent_complete_bbref_games_for_date(self):
        if self.mat_view is not None:
            if self.total_days and self.total_days > 0:
                perc = self.total_days_scraped_bbref/float(self.total_days)
                return f'{perc*100:02.0f}%'

    @hybrid_property
    def total_days_scraped_brooks(self):
        if self.mat_view is not None:
            return self.mat_view.total_days_scraped_brooks

    @hybrid_property
    def percent_complete_brooks_games_for_date(self):
        if self.mat_view is not None:
            if self.total_days and self.total_days > 0:
                return self.total_days_scraped_brooks/float(self.total_days)

    @hybrid_property
    def total_games_bbref(self):
        if self.mat_view is not None:
            return self.mat_view.total_games_bbref

    @hybrid_property
    def total_games_brooks(self):
        if self.mat_view is not None:
            return self.mat_view.total_games_brooks

    #@hybrid_property
    #def total_games_tracked(self):
    #    if self.mat_view is not None:
    #        return self.mat_view.total_games_tracked

    #@hybrid_property
    #def total_bbref_boxscores_scraped(self):
    #    if self.mat_view is not None:
    #        return self.mat_view.total_bbref_boxscores_scraped

    #@hybrid_property
    #def percent_complete_bbref_boxscores_scraped(self):
    #    if self.mat_view is not None:
    #        if self.total_games_tracked and self.total_games_tracked > 0:
    #            perc = self.total_bbref_boxscores_scraped/float(self.total_games_tracked)
    #            return f'{perc*100:02.0f}%'

    #@hybrid_property
    #def total_brooks_games_scraped(self):
    #    if self.mat_view is not None:
    #        return self.mat_view.total_bbref_boxscores_scraped

    #@hybrid_property
    #def percent_complete_brooks_games_scraped(self):
    #    if self.mat_view is not None:
    #       if self.total_games_tracked and self.total_games_tracked > 0:
    #           perc = self.total_brooks_games_scraped/float(self.total_games_tracked)
     #           return f'{perc*100:02.0f}%'

    #@hybrid_property
    #def total_pitch_appearances_bbref(self):
    #    if self.mat_view is not None:
    #        return self.mat_view.total_games_bbref

    #@hybrid_property
    #def total_pitch_appearances_brooks(self):
    #   if self.mat_view is not None:
    #        return self.mat_view.total_games_brooks

    #@hybrid_property
    #def total_pitch_count_bbref(self):
    #    if self.mat_view is not None:
    #        return self.mat_view.total_games_bbref

    #@hybrid_property
    #def total_pitch_count_brooks(self):
    #    if self.mat_view is not None:
    #        return self.mat_view.total_games_brooks

    #def __repr__(self):
    #    return f'<Season(name="{self.name}", id={self.id})>'

    def as_dict(self):
        d = {c.name: getattr(self, c.name) for c in self.__table__.columns}
        d['name'] = self.name
        d['total_days'] = self.total_days
        d['total_days_scraped_bbref'] = self.total_days_scraped_bbref
        d['percent_complete_bbref_games_for_date'] = self.percent_complete_bbref_games_for_date
        d['total_days_scraped_brooks'] = self.total_days_scraped_brooks
        d['percent_complete_brooks_games_for_date'] = self.percent_complete_brooks_games_for_date
        d['total_games_bbref'] = self.total_games_bbref
        d['total_games_brooks'] = self.total_games_brooks
        return d

    def display(self):
        season_dict = self.as_dict()
        title = self.name
        display_dict(season_dict, title=title)

    def get_date_range(self):
        return get_date_range(self.start_date, self.end_date)

    @classmethod
    def find_by_year(cls, session, year, season_type='Regular Season'):
        return session.query(cls).filter_by(season_type=season_type)\
            .filter_by(year=year).first()

    @classmethod
    def is_date_in_season(cls, session, check_date, season_type='Regular Season'):
        season = cls.find_by_year(session, check_date.year)
        if not season:
            error = (
                f'Database does not contain info for MLB {season_type} '
                f'{check_date.year}'
            )
            return Result.Fail(error)

        date_str = check_date.strftime(DATE_ONLY)
        if check_date < season.start_date or check_date > season.end_date:
            error = f'{date_str} is not within the scope of the {season.name}'
            return Result.Fail(error)
        return Result.Ok(season)

    @classmethod
    def all_regular_seasons(cls, session):
        return [season
                for season
                in session.query(cls).filter_by(
                    season_type=SEASON_TYPE_DICT['reg'])]


class SeasonStatusMV(MaterializedView):
    __table__ = create_mat_view(
        Base.metadata,
        "season_status_mv",
        select([
            Season.id.label('id'),
            func.count(DateScrapeStatus.id).label('total_days'),
            func.sum(DateScrapeStatus.scraped_daily_dash_bbref).label('total_days_scraped_bbref'),
            func.sum(DateScrapeStatus.scraped_daily_dash_brooks).label('total_days_scraped_brooks'),
            func.sum(DateScrapeStatus.game_count_bbref).label('total_games_bbref'),
            func.sum(DateScrapeStatus.game_count_brooks).label('total_games_brooks'),
            #func.sum(DateScrapeStatus.total_games).label('total_games_tracked'),
            #func.sum(DateScrapeStatus.total_bbref_boxscores_scraped).label('total_bbref_boxscores_scraped'),
            #unc.sum(DateScrapeStatus.total_brooks_games_scraped).label('total_brooks_games_scraped'),
            #func.sum(DateScrapeStatus.total_pitch_appearances_bbref).label('total_pitch_appearances_bbref'),
            #func.sum(DateScrapeStatus.total_pitch_appearances_brooks).label('total_pitch_appearances_brooks'),
            #func.sum(DateScrapeStatus.total_pitch_count_bbref).label('total_pitch_count_bbref'),
            #func.sum(DateScrapeStatus.total_brooks_games_scraped).label('total_pitch_count_brooks'),
        ]).select_from(join(
            Season,
            DateScrapeStatus,
            Season.id == DateScrapeStatus.season_id,
            isouter=True)
        ).where(Season.season_type == SEASON_TYPE_DICT['reg']
        ).group_by(Season.id)
    )

Index('season_status_mv_id_idx', SeasonStatusMV.id, unique=True)