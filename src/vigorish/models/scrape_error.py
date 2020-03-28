"""Errors encountered while executing scrape jobs."""
from datetime import date

from sqlalchemy import Boolean, Column, Integer, DateTime, ForeignKey, String
from sqlalchemy.types import Enum

from vigorish.config.database import Base
from vigorish.enums import DataSet
from vigorish.util.datetime_util import utc_now


class ScrapeError(Base):
    """Errors encountered while executing scrape jobs."""

    __tablename__ = "scrape_error"
    id = Column(Integer, primary_key=True)
    occurred_at = Column(DateTime, default=utc_now)
    data_set = Column(Enum(DataSet), nullable=False)
    error_message = Column(String, nullable=False)
    fixed = Column(Boolean, default=False)
    job_id = Column(Integer, ForeignKey("scrape_job.id"))

    def __repr__(self):
        return f"<ScrapeError job_name={self.job.name}, job_id={self.job_id}, message={self.error_message}>"

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}