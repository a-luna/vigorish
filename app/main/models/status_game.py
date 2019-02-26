from sqlalchemy import Column, Boolean, Integer, String, DateTime, ForeignKey
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

from app.main.models.base import Base

class GameScrapeStatus(Base):

    __tablename__ = 'scrape_status_game'
    id = Column(Integer, primary_key=True)
    