"""Db model that describes a MLB season and tracks data scraping progress."""
from sqlalchemy import Column, Boolean, Integer, DateTime
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.hybrid import hybrid_property

from app.main.constants import SEASON_TYPE_DICT
from app.main.models.base import Base
from app.main.util.list_functions import display_dict
from app.main.util.dt_format_strings import DATE_ONLY


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
    initialized_tables = Column(Boolean, default=False)
    all_boxscore_urls_scraped = Column(Boolean, default=False)
    all_boxscores_scraped = Column(Boolean, default=False)
    all_pitch_apps_scraped = Column(Boolean, default=False)
    all_pitchfx_scraped = Column(Boolean, default=False)

    #dates = relationship('SeasonDate', back_populates='season')
    #boxscores = relationship('Boxscore', back_populates='season')
    #itching_stats = relationship('PitchingStats', back_populates='season')
    #batting_stats = relationship('BattingStats', back_populates='season')

    @hybrid_property
    def name(self):
        return f'MLB {self.year} {self.season_type}'

    @hybrid_property
    def start_date_str(self):
        return self.start_date.strftime(DATE_ONLY)

    @hybrid_property
    def end_date_str(self):
        return self.end_date.strftime(DATE_ONLY)

    def __repr__(self):
        return (f'<Season(name="{self.name}", id={self.id})>')

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def display(self):
        season_dict = self.as_dict()
        title = self.name
        display_dict(season_dict, title=title)

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
            return dict(success=False, message=error)

        date_str = check_date.strftime(DATE_ONLY)
        if check_date < season.start_date or check_date > season.end_date:
            error = f'{date_str} is not within the scope of the {season.name}'
            return dict(success=False, message=error)
        message = f'{date_str} is within the scope of the {season.name}'
        return dict(success=True, message=message, result=season)