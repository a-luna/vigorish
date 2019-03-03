from sqlalchemy import Column, Boolean, Integer, String, DateTime, ForeignKey
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

from app.main.models.base import Base

class GameScrapeStatus(Base):

    __tablename__ = 'scrape_status_game'
    id = Column(Integer, primary_key=True)
    bbref_game_id = Column(String)
    bb_game_id = Column(String)
    scraped_bbref_boxscore = Column(Integer, default=0)
    pitch_app_count_bbref = Column(Integer, default=0)
    pitch_app_count_brooks = Column(Integer, default=0)

    scrape_status_date_id = Column(Integer, ForeignKey('scrape_status_date.id'))
