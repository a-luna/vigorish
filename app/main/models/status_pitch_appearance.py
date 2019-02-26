from sqlalchemy import Column, Boolean, Integer, String, DateTime, ForeignKey
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

from app.main.models.base import Base

class PitchAppearanceScrapeStatus(Base):

    __tablename__ = 'scrape_status_pitch_app'
    id = Column(Integer, primary_key=True)
    scraped_pitchfx = Column(Boolean, default=False)
    pitch_count_brooks_pitch_log = Column(Integer, default=0)
    pitch_count_brooks_pitchfx = Column(Integer, default=0)
    pitch_count_bbref = Column(Integer, default=0)