"""Record of a job to scrape data for a specified date range."""
from datetime import timezone
from uuid import uuid4
from pathlib import Path

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship
from sqlalchemy.types import Enum

from vigorish.enums import JobStatus, JobGroup, DataSet
from vigorish.config.database import Base
from vigorish.constants import JOB_STATUS_TO_GROUP_MAP
from vigorish.util.datetime_util import (
    get_date_range,
    utc_now,
    make_tzaware,
    localized_dt_string,
    get_local_utcoffset,
)
from vigorish.util.dt_format_strings import DATE_ONLY
from vigorish.util.list_helpers import group_and_sort_list

APP_FOLDER = Path(__file__).parent.parent
NODEJS_INBOX = APP_FOLDER / "nightmarejs" / "inbox"


class ScrapeJob(Base):
    """Record of a job to scrape data for a specified date range."""

    __tablename__ = "scrape_job"
    id = Column(String, primary_key=True, default=lambda: f"{str(uuid4())[:4]}")
    name = Column(String)
    status = Column(Enum(JobStatus), default=JobStatus.NOT_STARTED)
    start_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime, nullable=False)
    created_date = Column(DateTime, default=utc_now)
    data_sets_int = Column(Integer, nullable=False)
    season_id = Column(Integer, ForeignKey("season.id"))

    errors = relationship("ScrapeError", backref="job")

    @hybrid_property
    def bbref_games_for_date(self):
        return self.data_sets_int & DataSet.BBREF_GAMES_FOR_DATE == DataSet.BBREF_GAMES_FOR_DATE

    @hybrid_property
    def bbref_boxscores(self):
        return self.data_sets_int & DataSet.BBREF_BOXSCORES == DataSet.BBREF_BOXSCORES

    @hybrid_property
    def brooks_games_for_date(self):
        return self.data_sets_int & DataSet.BROOKS_GAMES_FOR_DATE == DataSet.BROOKS_GAMES_FOR_DATE

    @hybrid_property
    def brooks_pitch_logs(self):
        return self.data_sets_int & DataSet.BROOKS_PITCH_LOGS == DataSet.BROOKS_PITCH_LOGS

    @hybrid_property
    def brooks_pitchfx(self):
        return self.data_sets_int & DataSet.BROOKS_PITCHFX == DataSet.BROOKS_PITCHFX

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
    def job_details(self):
        job_name = f"{self.name} (ID: {self.id})" if self.name != self.id else self.id
        return {
            "Job Name": job_name,
            "Status": self.status.name,
            "MLB Season": self.season.name,
            "Start Date": self.start_date.strftime(DATE_ONLY),
            "End Date": self.end_date.strftime(DATE_ONLY),
            "Created": self.created_date_str,
            "Data Sets": "\n\t".join([str(ds) for ds in self.data_sets]),
        }

    @hybrid_property
    def error_messages(self):
        return "\n".join([str(error) for error in self.errors])

    def __repr__(self):
        return f"<ScrapeJob id={self.id}, status={self.status}>"

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    @classmethod
    def find_by_id(cls, session, job_id):
        return session.query(cls).filter_by(id=job_id).first()

    @classmethod
    def get_all_incomplete_jobs(cls, session):
        return [job for job in session.query(cls).all() if job.group == JobGroup.INCOMPLETE]

    @classmethod
    def get_all_active_jobs(cls, session):
        return [job for job in session.query(cls).all() if job.group == JobGroup.ACTIVE]

    @classmethod
    def get_all_failed_jobs(cls, session):
        return [job for job in session.query(cls).all() if job.group == JobGroup.FAILED]

    @classmethod
    def get_all_cancelled_jobs(cls, session):
        return [job for job in session.query(cls).all() if job.group == JobGroup.CANCELLED]

    @classmethod
    def get_all_complete_jobs(cls, session):
        return [job for job in session.query(cls).all() if job.group == JobGroup.COMPLETE]

    @classmethod
    def get_all_jobs_grouped_sorted(cls, session):
        all_jobs = session.query(cls).all()
        return group_and_sort_list(all_jobs, "group", "created_date", sort_all_desc=True)
