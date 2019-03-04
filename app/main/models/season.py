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

    #status = relationship('SeasonStatus', back_populates='season')
    dates = relationship('DateScrapeStatus')
    #boxscores = relationship('Boxscore', back_populates='season')
    #pitching_stats = relationship('PitchingStats', back_populates='season')
    #batting_stats = relationship('BattingStats', back_populates='season')

    mat_view = relationship(
        'SeasonStatusMV', backref='original',
        uselist=False, # makes it a one-to-one relationship
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
    def days_total(self):
        if self.mat_view is not None: # if None, mat_view needs refreshing
            return self.mat_view.days_total

    @hybrid_property
    def days_scraped_bbref_total(self):
        if self.mat_view is not None:
            return self.mat_view.days_scraped_bbref_total

    @hybrid_property
    def percent_complete_bbref_games_for_date(self):
        if self.mat_view is not None:
            if self.days_total and self.days_total > 0:
                return self.days_scraped_bbref_total/float(self.days_total)

    @hybrid_property
    def days_scraped_brooks_total(self):
        if self.mat_view is not None:
            return self.mat_view.days_scraped_brooks_total

    @hybrid_property
    def percent_complete_brooks_games_for_date(self):
        if self.mat_view is not None:
            if self.days_total and self.days_total > 0:
                return self.days_scraped_brooks_total/float(self.days_total)

    @hybrid_property
    def games_total_bbref(self):
        if self.mat_view is not None:
            return self.mat_view.games_total_bbref

    @hybrid_property
    def games_total_brooks(self):
        if self.mat_view is not None:
            return self.mat_view.games_total_brooks

    @hybrid_property
    def might_be_postponed_total_brooks(self):
        if self.mat_view is not None:
            return self.mat_view.might_be_postponed_total_brooks

    @hybrid_property
    def pitch_app_total_bbref(self):
        if self.mat_view is not None:
            return self.mat_view.pitch_app_total_bbref

    @hybrid_property
    def pitch_app_total_brooks(self):
        if self.mat_view is not None:
            return self.mat_view.pitch_app_total_brooks

    @hybrid_property
    def boxscores_scraped_total(self):
        if self.mat_view is not None:
            return self.mat_view.boxscores_scraped_total

    @hybrid_property
    def boxscores_missing_total(self):
        if self.mat_view is not None:
            return self.mat_view.boxscores_missing_total

    @hybrid_property
    def pitch_logs_scraped_total(self):
        if self.mat_view is not None:
            return self.mat_view.pitch_logs_scraped_total

    @hybrid_property
    def pitch_logs_missing_total(self):
        if self.mat_view is not None:
            return self.mat_view.pitch_logs_missing_total

    def __repr__(self):
        return (f'<Season(name="{self.name}", id={self.id})>')

    def as_dict(self):
        d = {c.name: getattr(self, c.name) for c in self.__table__.columns}
        d['name'] = self.name
        d['days_total'] = self.days_total
        d['days_scraped_bbref_total'] = self.days_scraped_bbref_total
        d['percent_complete_bbref_games_for_date'] = self.percent_complete_bbref_games_for_date
        d['days_scraped_brooks_total'] = self.days_scraped_brooks_total
        d['percent_complete_brooks_games_for_date'] = self.percent_complete_brooks_games_for_date
        d['games_total_bbref'] = self.games_total_bbref
        d['games_total_brooks'] = self.games_total_brooks
        d['might_be_postponed_total_brooks'] = self.might_be_postponed_total_brooks
        d['pitch_app_total_bbref'] = self.pitch_app_total_bbref
        d['pitch_app_total_brooks'] = self.pitch_app_total_brooks
        d['boxscores_scraped_total'] = self.boxscores_scraped_total
        d['boxscores_missing_total'] = self.boxscores_missing_total
        d['pitch_logs_scraped_total'] = self.pitch_logs_scraped_total
        d['pitch_logs_missing_total'] = self.pitch_logs_missing_total
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
            func.count(DateScrapeStatus.id).label('days_total'),
            func.sum(DateScrapeStatus.scraped_daily_dash_bbref).label('days_scraped_bbref_total'),
            func.sum(DateScrapeStatus.scraped_daily_dash_brooks).label('days_scraped_brooks_total'),
            func.sum(DateScrapeStatus.game_count_bbref).label('games_total_bbref'),
            func.sum(DateScrapeStatus.game_count_brooks).label('games_total_brooks'),
            func.sum(DateScrapeStatus.might_be_postponed_count_brooks).label('might_be_postponed_total_brooks'),
            func.sum(DateScrapeStatus.pitch_app_count_bbref).label('pitch_app_total_bbref'),
            func.sum(DateScrapeStatus.pitch_app_count_brooks).label('pitch_app_total_brooks'),
            func.sum(DateScrapeStatus.scraped_boxscore_count).label('boxscores_scraped_total'),
            func.sum(DateScrapeStatus.missing_boxscore_count).label('boxscores_missing_total'),
            func.sum(DateScrapeStatus.scraped_pitch_logs_count).label('pitch_logs_scraped_total'),
            func.sum(DateScrapeStatus.missing_pitch_logs_count).label('pitch_logs_missing_total'),
        ]).select_from(join(
            Season,
            DateScrapeStatus,
            Season.id == DateScrapeStatus.season_id,
            isouter=True)
        ).where(Season.season_type == SEASON_TYPE_DICT['reg']
        ).group_by(Season.id)
    )

Index('season_status_mv_id_idx', SeasonStatusMV.id, unique=True)