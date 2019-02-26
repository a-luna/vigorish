from sqlalchemy import Column, Boolean, Integer, String, DateTime, ForeignKey
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

from app.main.models.base import Base

class DayScrapeStatus(Base):

    __tablename__ = 'scrape_status_date'
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

    #season = relationship('Season', back_populates='dates')
    #boxscores = relationship('Boxscore', back_populates='date')

    def __repr__(self):
        return '<SingleDayStatus {}>'.format(self.game_date)

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}