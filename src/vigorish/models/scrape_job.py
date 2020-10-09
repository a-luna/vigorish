"""Record of a job to scrape data for a specified date range."""
from datetime import timezone
from uuid import uuid4

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship
from sqlalchemy.types import Enum

from vigorish.config.database import Base
from vigorish.config.project_paths import NODEJS_INBOX, NODEJS_OUTBOX
from vigorish.constants import DATA_SET_NAME_MAP
from vigorish.enums import DataSet, JobStatus
from vigorish.util.datetime_util import (
    get_date_range,
    get_local_utcoffset,
    localized_dt_string,
    make_tzaware,
    utc_now,
)
from vigorish.util.dt_format_strings import DATE_ONLY
from vigorish.util.list_helpers import group_and_sort_list


class ScrapeJob(Base):
    """Record of a job to scrape data for a specified date range."""

    __tablename__ = "scrape_job"
    id = Column(String, primary_key=True, default=lambda: f"{str(uuid4())[:4]}")
    name = Column(String)
    status = Column(Enum(JobStatus), default=JobStatus.INCOMPLETE)
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
    def created_date_str(self):
        created_date_utc = make_tzaware(self.created_date, use_tz=timezone.utc, localize=False)
        return localized_dt_string(created_date_utc, use_tz=get_local_utcoffset())

    @hybrid_property
    def url_set_filepath(self):
        return NODEJS_INBOX.joinpath(f"{self.id}.json")

    @hybrid_property
    def scraped_html_root_folder(self):
        return NODEJS_OUTBOX.joinpath(self.id)

    @hybrid_property
    def scraped_html_folders(self):
        folder_dict = {
            data_set: self.scraped_html_root_folder.joinpath(data_set.name.lower())
            for data_set in self.data_sets
        }
        for folderpath in folder_dict.values():
            folderpath.mkdir(parents=True, exist_ok=True)
        return folder_dict

    @hybrid_property
    def date_range(self):
        return get_date_range(self.start_date, self.end_date)

    @hybrid_property
    def total_days(self):
        return len(self.date_range)

    @hybrid_property
    def job_details(self):
        job_name = f"{self.name} (ID: {self.id})" if self.name != self.id else self.id
        data_set_dict = {value: name for name, value in DATA_SET_NAME_MAP.items()}
        return {
            "Job Name": job_name,
            "Status": self.status.name,
            "MLB Season": self.season.name,
            "Start Date": self.start_date.strftime(DATE_ONLY),
            "End Date": self.end_date.strftime(DATE_ONLY),
            "Created At": self.created_date_str,
            "Data Sets": "\n\t      ".join([data_set_dict[ds] for ds in self.data_sets]),
        }

    @hybrid_property
    def error_messages(self):
        return "\n".join([str(error) for error in self.errors])

    def __repr__(self):
        return f"<ScrapeJob id={self.id}, status={self.status}>"

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    @classmethod
    def find_by_id(cls, db_session, job_id):
        return db_session.query(cls).filter_by(id=job_id).first()

    @classmethod
    def get_all_active_jobs(cls, db_session):
        return [job for job in db_session.query(cls).all() if job.status == JobStatus.INCOMPLETE]

    @classmethod
    def get_all_failed_jobs(cls, db_session):
        return [job for job in db_session.query(cls).all() if job.status == JobStatus.ERROR]

    @classmethod
    def get_all_complete_jobs(cls, db_session):
        return [job for job in db_session.query(cls).all() if job.status == JobStatus.COMPLETE]

    @classmethod
    def get_all_jobs_grouped_sorted(cls, db_session):
        all_jobs = db_session.query(cls).all()
        return group_and_sort_list(all_jobs, "status", "created_date", sort_all_desc=True)
