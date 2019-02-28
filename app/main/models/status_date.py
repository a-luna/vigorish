from sqlalchemy import Column, Boolean, Integer, String, DateTime, ForeignKey
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

from app.main.constants import MLB_DATA_SETS
from app.main.models.base import Base
from app.main.models.season import Season
from app.main.util.list_functions import display_dict
from app.main.util.dt_format_strings import DATE_ONLY, DATE_ONLY_TABLE_ID

class DateScrapeStatus(Base):

    __tablename__ = 'status_date'
    id = Column(Integer, primary_key=True)
    game_date = Column(DateTime)
    game_count = Column(Integer, default=0)
    scraped_daily_dash_bbref = Column(Boolean, default=False)
    scraped_daily_dash_brooks = Column(Boolean, default=False)
    scraped_all_pitch_logs = Column(Boolean, default=False)
    scraped_pitch_logs_count = Column(Integer, default=0)
    missing_pitch_logs_count = Column(Integer, default=0)
    scraped_all_boxscores = Column(Boolean, default=False)
    scraped_boxscore_count = Column(Integer, default=0)
    missing_boxscore_count = Column(Integer, default=0)
    season_id = Column(Integer, ForeignKey('season.id'))

    @hybrid_property
    def game_date_str(self):
        return self.game_date.strftime(DATE_ONLY)

    season = relationship('Season', back_populates='dates')
    #boxscores = relationship('Boxscore', back_populates='date')

    def __init__(self, game_date, season_id):
        date_str = game_date.strftime(DATE_ONLY_TABLE_ID)
        self.id = int(date_str)
        self.game_date = game_date
        self.season_id = season_id

    def __repr__(self):
        return '<DateScrapeStatus(date={})>'.format(self.game_date_str)

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def display(self):
        season_dict = self.as_dict()
        title = f'SCRAPE STATUS FOR {self.game_date_str}'
        display_dict(season_dict, title=title)

    @classmethod
    def data_set_report_for_date(cls, session, report_date, data_set):
        if data_set not in MLB_DATA_SETS:
            error = f'"{data_set}"" is not a valid data set name.'
            return dict(success=False, message=error)

        status = session.query(cls).filter_by(game_date=report_date)