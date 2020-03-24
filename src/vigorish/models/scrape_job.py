"""Record of a job to scrape data for a specified date range."""
from datetime import date
from uuid import uuid4
from pathlib import Path

from sqlalchemy import Column, Boolean, DateTime, ForeignKey, Integer, String
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship
from sqlalchemy.types import Enum

from vigorish.enums import JobStatus, DataSet
from vigorish.config.database import Base
from vigorish.util.datetime_util import get_date_range

SRC_FOLDER = Path(__file__).parent.parent.parent
NODEJS_INBOX = SRC_FOLDER / "nightmarejs" / "inbox"


class ScrapeJob(Base):
    """Record of a job to scrape data for a specified date range."""

    __tablename__ = "scrape_job"
    id = Column(String, primary_key=True, default=lambda: f"{str(uuid4())[:4]}")
    name = Column(String)
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
    def nodejs_filepath(self):
        return NODEJS_INBOX / f"{self.id}.json"

    @hybrid_property
    def data_sets(self):
        data_sets = []
        if self.brooks_games_for_date:
            data_sets.append(DataSet.BROOKS_GAMES_FOR_DATE)
        if self.brooks_pitch_logs:
            data_sets.append(DataSet.BROOKS_PITCH_LOGS)
        if self.brooks_pitchfx:
            data_sets.append(DataSet.BROOKS_PITCHFX)
        if self.bbref_games_for_date:
            data_sets.append(DataSet.BBREF_GAMES_FOR_DATE)
        if self.bbref_boxscores:
            data_sets.append(DataSet.BBREF_BOXSCORES)
        return sorted(data_sets)

    @hybrid_property
    def date_range(self):
        return get_date_range(self.start_date, self.end_date)

    @hybrid_property
    def total_days(self):
        return len(self.date_range)

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
        return f"<ScrapeJob id={self.id}, status={self.status}>"

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    @classmethod
    def find_by_id(cls, session, job_id):
        return session.query(cls).filter_by(id=job_id).first()
