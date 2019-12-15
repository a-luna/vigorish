"""Db model that describes a MLB season and tracks data scraping progress."""
from datetime import date

from sqlalchemy import Column, Boolean, Index, Integer, DateTime, select, func, join
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

from app.main.constants import SEASON_TYPE_DICT
from app.main.models.base import Base
from app.main.models.status_date import DateScrapeStatus
from app.main.models.status_game import GameScrapeStatus
from app.main.models.status_pitch_appearance import PitchAppearanceScrapeStatus
from app.main.models.views.materialized_view import MaterializedView
from app.main.models.views.materialized_view_factory import create_mat_view
from app.main.util.datetime_util import get_date_range
from app.main.util.dt_format_strings import DATE_ONLY
from app.main.util.list_functions import display_dict
from app.main.util.result import Result


class Season(Base):
    """Db model that describes a MLB season and tracks data scraping progress."""

    __tablename__ = "season"
    id = Column(Integer, primary_key=True)
    year = Column(Integer, nullable=False)
    start_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime, nullable=False)
    asg_date = Column(DateTime, nullable=True)
    season_type = Column(
        postgresql.ENUM(
            "None",
            SEASON_TYPE_DICT["pre"],
            SEASON_TYPE_DICT["reg"],
            SEASON_TYPE_DICT["post"],
            name="season_type_enum",
        ),
        default="None",
    )

    scrape_status_dates = relationship("DateScrapeStatus", backref="season")
    scrape_status_games = relationship("GameScrapeStatus", backref="season")
    scrape_status_pitchfx = relationship("PitchAppearanceScrapeStatus", backref="season")
    boxscores = relationship("Boxscore", backref="season")
    pitching_stats = relationship("GamePitchStats", backref="season")
    batting_stats = relationship("GameBatStats", backref="season")

    @hybrid_property
    def name(self):
        return f"MLB {self.year} {self.season_type}"

    @hybrid_property
    def start_date_str(self):
        return self.start_date.strftime(DATE_ONLY)

    @hybrid_property
    def end_date_str(self):
        return self.end_date.strftime(DATE_ONLY)

    @hybrid_property
    def total_days(self):
        today = date.today()
        if today.year == self.year and today < self.end_date.date():
            return (today - self.start_date.date()).days
        return len(self.scrape_status_dates)

    @hybrid_property
    def total_days_scraped_bbref(self):
        return sum(date_status.scraped_daily_dash_bbref for date_status in self.scrape_status_dates)

    @hybrid_property
    def percent_complete_bbref_games_for_date(self):
        return self.total_days_scraped_bbref / float(self.total_days) \
            if self.total_days > 0 else 0.0

    @hybrid_property
    def total_days_scraped_brooks(self):
        return sum(date_status.scraped_daily_dash_brooks for date_status in self.scrape_status_dates)

    @hybrid_property
    def percent_complete_brooks_games_for_date(self):
        return self.total_days_scraped_brooks / float(self.total_days) \
            if self.total_days > 0 else 0.0

    @hybrid_property
    def total_games(self):
        return sum(date_status.total_games for date_status in self.scrape_status_dates)

    @hybrid_property
    def total_bbref_boxscores_scraped(self):
        return sum(game_status.scraped_bbref_boxscore for game_status in self.scrape_status_games)

    @hybrid_property
    def percent_complete_bbref_boxscores_scraped(self):
        return self.total_bbref_boxscores_scraped / float(self.total_games) \
            if self.total_games > 0 else 0.0

    @hybrid_property
    def total_brooks_games_scraped(self):
        return sum(game_status.scraped_brooks_pitch_logs for game_status in self.scrape_status_games)

    @hybrid_property
    def percent_complete_brooks_games_scraped(self):
        return self.total_brooks_games_scraped / float(self.total_games) \
            if self.total_games > 0 else 0.0

    @hybrid_property
    def pitch_appearance_count_bbref(self):
        return sum(game_status.pitch_app_count_bbref for game_status in self.scrape_status_games)

    @hybrid_property
    def pitch_appearance_count_brooks(self):
        return sum(game_status.pitch_app_count_brooks for game_status in self.scrape_status_games)

    @hybrid_property
    def pitch_appearance_count_difference(self):
        return abs(self.pitch_appearance_count_bbref - self.pitch_appearance_count_brooks)

    @hybrid_property
    def total_pitch_apps_no_pitchfx_data(self):
        return sum(pfx.no_pitchfx_data for pfx in self.scrape_status_pitchfx)

    @hybrid_property
    def total_pitchfx_logs_scraped(self):
        return sum(pfx.scraped_pitchfx for pfx in self.scrape_status_pitchfx)

    @hybrid_property
    def total_pitch_apps_with_pitchfx_data(self):
        return self.pitch_appearance_count_brooks - self.total_pitch_apps_no_pitchfx_data

    @hybrid_property
    def percent_complete_pitchfx_logs_scraped(self):
        return self.total_pitchfx_logs_scraped / float(self.pitch_appearance_count_brooks) \
            if self.pitch_appearance_count_brooks > 0 else 0.0

    @hybrid_property
    def scraped_all_pitchfx_logs(self):
        return self.percent_complete_pitchfx_logs_scraped == 1

    @hybrid_property
    def total_pitch_count_bbref(self):
        return sum(game_status.total_pitch_count_bbref for game_status in self.scrape_status_games)

    @hybrid_property
    def total_pitch_count_brooks(self):
        return sum(game_status.total_pitch_count_brooks for game_status in self.scrape_status_games)

    @hybrid_property
    def total_pitch_count_pitchfx(self):
        return sum(pfx.pitch_count_pitchfx for pfx in self.scrape_status_pitchfx)

    @hybrid_property
    def total_pitch_count_difference(self):
        return abs(self.total_pitch_count_bbref - self.total_pitch_count_brooks)

    def __repr__(self):
        return f"<Season name={self.name}, id={self.id})>"

    def as_dict(self):
        d = {c.name: getattr(self, c.name) for c in self.__table__.columns}
        d["name"] = self.name
        d["start_date_str"] = self.start_date_str
        d["end_date_str"] = self.end_date_str
        d["total_days"] = self.total_days
        d["total_days_scraped_bbref"] = self.total_days_scraped_bbref
        d["percent_complete_bbref_games_for_date"] = self.percent_complete_bbref_games_for_date
        d["total_days_scraped_brooks"] = self.total_days_scraped_brooks
        d["percent_complete_brooks_games_for_date"] = self.percent_complete_brooks_games_for_date
        d["total_games"] = self.total_games
        d["total_bbref_boxscores_scraped"] = self.total_bbref_boxscores_scraped
        d["percent_complete_bbref_boxscores_scraped"] = self.percent_complete_bbref_boxscores_scraped
        d["total_brooks_games_scraped"] = self.total_brooks_games_scraped
        d["percent_complete_brooks_games_scraped"] = self.percent_complete_brooks_games_scraped
        d["pitch_appearance_count_bbref"] = self.pitch_appearance_count_bbref
        d["pitch_appearance_count_brooks"] = self.pitch_appearance_count_brooks
        d["pitch_appearance_count_difference"] = self.pitch_appearance_count_difference
        d["total_pitch_apps_no_pitchfx_data"] = self.total_pitch_apps_no_pitchfx_data
        d["total_pitchfx_logs_scraped"] = self.total_pitchfx_logs_scraped
        d["total_pitch_apps_with_pitchfx_data"] = self.total_pitch_apps_with_pitchfx_data
        d["percent_complete_pitchfx_logs_scraped"] = self.percent_complete_pitchfx_logs_scraped
        d["total_pitch_count_bbref"] = self.total_pitch_count_bbref
        d["total_pitch_count_brooks"] = self.total_pitch_count_brooks
        d["total_pitch_count_pitchfx"] = self.total_pitch_count_pitchfx
        d["total_pitch_count_difference"] = self.total_pitch_count_difference
        return d

    def display(self):
        season_dict = self.as_dict()
        title = self.name
        display_dict(season_dict, title=title)

    def status_report(self):
        return (
            f"BBref Daily Dash Scraped...: {self.total_days_scraped_bbref:,}/{self.total_days:,} days ({self.percent_complete_bbref_games_for_date:.0%})\n"
            f"Brooks Daily Dash Scraped..: {self.total_days_scraped_brooks:,}/{self.total_days:,} days ({self.percent_complete_brooks_games_for_date:.0%})\n"
            f"BBref Boxscores Scraped....: {self.total_bbref_boxscores_scraped:,}/{self.total_games:,} games ({self.percent_complete_bbref_boxscores_scraped:.0%})\n"
            f"Brooks Games Scraped.......: {self.total_brooks_games_scraped:,}/{self.total_games:,} games ({self.percent_complete_brooks_games_scraped:.0%})\n"
            f"Brooks PitchFX Scraped.....: {self.total_pitchfx_logs_scraped:,}/{self.total_pitch_apps_with_pitchfx_data:,} pitch apps, {self.total_pitch_apps_no_pitchfx_data} no data ({self.percent_complete_pitchfx_logs_scraped:.0%})\n"
            f"Total Pitch Appearances....: {self.pitch_appearance_count_bbref:,} (BBref) {self.pitch_appearance_count_brooks:,} (Brooks) {self.total_pitchfx_logs_scraped:,} (PitchFX)\n"
            f"Total Pitch Count..........: {self.total_pitch_count_bbref:,} (BBref) {self.total_pitch_count_brooks:,} (Brooks) {self.total_pitch_count_pitchfx:,} (PitchFX)\n"
        )

    def get_date_range(self):
        return get_date_range(self.start_date, self.end_date)

    @classmethod
    def find_by_year(cls, session, year, season_type="Regular Season"):
        return (
            session.query(cls)
            .filter_by(season_type=season_type)
            .filter_by(year=year)
            .first())

    @classmethod
    def is_date_in_season(cls, session, check_date, season_type="Regular Season"):
        season = cls.find_by_year(session, check_date.year)
        if not season:
            error = f"Database does not contain info for MLB {season_type} {check_date.year}"
            return Result.Fail(error)

        date_str = check_date.strftime(DATE_ONLY)
        if check_date < season.start_date or check_date > season.end_date:
            error = f"{date_str} is not within the scope of the {season.name}"
            return Result.Fail(error)
        return Result.Ok(season)

    @classmethod
    def validate_date_range(cls, session, start, end):
        if start.year != end.year:
            error = (
                "Start and end dates must both be in the same year and within "
                "the scope of that year's MLB Regular Season.")
            return Result.Fail(error)
        if start > end:
            start_str = start.strftime(DATE_ONLY)
            end_str = end.strftime(DATE_ONLY)
            error = (
                '"start" must be a date before (or the same date as) "end":\n'
                f"start: {start_str}\n"
                f"end: {end_str}")
            return Result.Fail(error)
        season = cls.find_by_year(session, start.year)
        start_date_valid = cls.is_date_in_season(session, start).success
        end_date_valid = cls.is_date_in_season(session, end).success
        if not start_date_valid or not end_date_valid:
            error = (
                f"Start and end date must both be within the {season.name}:\n"
                f"season_start_date: {season.start_date_str}\n"
                f"season_end_date: {season.end_date_str}")
            return Result.Fail(error)
        return Result.Ok(season)

    @classmethod
    def all_regular_seasons(cls, session):
        matching_seasons = session.query(cls).filter_by(season_type=SEASON_TYPE_DICT["reg"])
        return [season for season in matching_seasons]

    @classmethod
    def is_this_the_asg_date(cls, session, game_date):
        season = cls.find_by_year(session, game_date.year)
        return game_date == season.asg_date if season else None

    @classmethod
    def get_unscraped_bbref_pitch_app_ids_for_season(cls, session, year):
        season = cls.find_by_year(session, year)
        return [
            pfx.bbref_pitch_app_id for pfx
            in season.scrape_status_pitchfx if pfx.scraped_pitchfx == 0
        ]
