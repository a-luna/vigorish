from sqlalchemy import Column, Boolean, Integer, ForeignKey
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

from app.main.models.base import Base

class SeasonScrapeStatus(Base):

    __tablename__ = 'scrape_status_season'
    id = Column(Integer, primary_key=True)
    game_count_bbref = Column(Integer, default=0)
    game_count_brooks = Column(Integer, default=0)
    all_days_scraped_bbref = Column(Boolean, default=False)
    all_days_scraped_brooks = Column(Boolean, default=False)
    all_boxscores_scraped = Column(Boolean, default=False)
    all_pitch_logs_scraped = Column(Boolean, default=False)
    all_pitchfx_scraped = Column(Boolean, default=False)
    season_id = Column(Integer, ForeignKey('season.id'))

    #season = relationship('Season', back_populates='status')