"""Record of a job to scrape data for a specified date range."""
from datetime import date
from uuid import uuid4

from sqlalchemy import Column, Boolean, DateTime, ForeignKey, Integer, String
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship
from sqlalchemy.types import Enum

from vigorish.enums import JobStatus
from vigorish.config.database import Base
from vigorish.util.datetime_util import get_date_range


class ScrapeJob(Base):
    """Record of a job to scrape data for a specified date range."""

    __tablename__ = "scrape_job"
    id = Column(Integer, primary_key=True)
    name = Column(String, default=lambda: f"Job{str(uuid4())[:4]}")
    status = Column(Enum(JobStatus), default=JobStatus.NOT_STARTED)
    start_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime, nullable=False)
    last_scraped_date = Column(DateTime, default=date.min)
    bbref_games_for_date = Column(Boolean, default=False)
    bbref_boxscores = Column(Boolean, default=False)
    brooks_games_for_date = Column(Boolean, default=False)
    brooks_pitch_logs = Column(Boolean, default=False)
    brooks_pitchfx = Column(Boolean, default=False)
    season_id = Column(Integer, ForeignKey("season.id"))

    errors = relationship("ScrapeError", backref="job")

    @hybrid_property
    def total_days(self):
        return len(self.get_job_date_range())

    @hybrid_property
    def days_scraped(self):
        return (
            len(get_date_range(self.start_date, self.last_scraped_date))
            if self.last_scraped_date >= self.start_date
            else 0
        )

    @hybrid_property
    def percent_complete(self):
        return self.days_scraped / float(self.total_days) if self.total_days > 0 else 0.0

    def __repr__(self):
        return f"<ScrapeJob name={self.name}, status={self.status}>"

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def get_job_date_range(self):
        return get_date_range(self.start_date, self.end_date)
