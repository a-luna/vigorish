"""Errors encountered while executing scrape jobs."""
from datetime import timezone

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.types import Enum

import vigorish.database as db
from vigorish.enums import DataSet
from vigorish.util.datetime_util import (
    get_local_utcoffset,
    localized_dt_string,
    make_tzaware,
    utc_now,
)


class ScrapeError(db.Base):
    """Errors encountered while executing scrape jobs."""

    __tablename__ = "scrape_error"
    id = Column(Integer, primary_key=True)
    occurred_at = Column(DateTime, default=utc_now)
    data_set = Column(Enum(DataSet), nullable=False)
    error_message = Column(String, nullable=False)
    fixed = Column(Boolean, default=False)
    job_id = Column(Integer, ForeignKey("scrape_job.id"))

    @hybrid_property
    def occurred_at_str(self):
        occurred_at_utc = make_tzaware(self.occurred_at, use_tz=timezone.utc, localize=False)
        return localized_dt_string(occurred_at_utc, use_tz=get_local_utcoffset())

    def __repr__(self):
        return f"<ScrapeError job_name={self.job.name}, job_id={self.job_id}, message={self.error_message}>"

    def __str__(self):
        return f"{self.occurred_at_str} | {self.error_message}"

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
