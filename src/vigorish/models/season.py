"""Db model that describes a MLB season and tracks data scraping progress."""
from datetime import date

from sqlalchemy import Column, Integer, DateTime
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship
from sqlalchemy.types import Enum

from vigorish.enums import SeasonType
from vigorish.config.database import Base
from vigorish.util.datetime_util import get_date_range as get_date_range_util
from vigorish.util.dt_format_strings import DATE_ONLY
from vigorish.util.list_helpers import display_dict
from vigorish.util.result import Result


class Season(Base):
    """Database model that describes a MLB season and tracks data scraping progress."""

    __tablename__ = "season"
    id = Column(Integer, primary_key=True)
    year = Column(Integer, nullable=False)
    start_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime, nullable=False)
    asg_date = Column(DateTime, nullable=True)
    season_type = Column(Enum(SeasonType), default=SeasonType.NONE)

    scrape_status_dates = relationship("DateScrapeStatus", backref="season")
    scrape_status_games = relationship("GameScrapeStatus", backref="season")
    scrape_status_pitchfx = relationship("PitchAppScrapeStatus", backref="season")
    boxscores = relationship("Boxscore", backref="season")
    pitching_stats = relationship("GamePitchStats", backref="season")
    batting_stats = relationship("GameBatStats", backref="season")
    scrape_jobs = relationship("ScrapeJob", backref="season")

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
    def asg_date_str(self):
        return self.asg_date.strftime(DATE_ONLY)

    @hybrid_property
    def total_days(self):
        today = date.today()
        if today.year == self.year and today < self.end_date.date():
            return (today - self.start_date.date()).days
        return len(self.scrape_status_dates)

    @hybrid_property
    def total_days_scraped_bbref(self):
        return sum(
            date_status.scraped_daily_dash_bbref for date_status in self.scrape_status_dates
        )

    @hybrid_property
    def percent_complete_bbref_games_for_date(self):
        return (
            self.total_days_scraped_bbref / float(self.total_days) if self.total_days > 0 else 0.0
        )

    @hybrid_property
    def scraped_all_bbref_games_for_date(self):
        return (
            all(date_status.scraped_daily_dash_bbref for date_status in self.scrape_status_dates)
            if self.total_days > 0
            else False
        )

    @hybrid_property
    def total_days_scraped_brooks(self):
        return sum(
            date_status.scraped_daily_dash_brooks for date_status in self.scrape_status_dates
        )

    @hybrid_property
    def percent_complete_brooks_games_for_date(self):
        return (
            self.total_days_scraped_brooks / float(self.total_days) if self.total_days > 0 else 0.0
        )

    @hybrid_property
    def scraped_all_brooks_games_for_date(self):
        return (
            all(date_status.scraped_daily_dash_brooks for date_status in self.scrape_status_dates)
            if self.total_days > 0
            else False
        )

    @hybrid_property
    def total_games(self):
        return len(self.scrape_status_games) if self.scrape_status_games else 0

    @hybrid_property
    def total_games_combined_success(self):
        return sum(game.combined_data_success for game in self.scrape_status_games)

    @hybrid_property
    def total_games_combined_fail(self):
        return sum(game.combined_data_fail for game in self.scrape_status_games)

    @hybrid_property
    def total_bbref_boxscores_scraped(self):
        return sum(game.scraped_bbref_boxscore for game in self.scrape_status_games)

    @hybrid_property
    def percent_complete_bbref_boxscores_scraped(self):
        return (
            self.total_bbref_boxscores_scraped / float(len(self.scrape_status_games))
            if len(self.scrape_status_games) > 0
            else 0.0
        )

    @hybrid_property
    def scraped_all_bbref_boxscores(self):
        return (
            all(game_status.scraped_bbref_boxscore for game_status in self.scrape_status_games)
            if self.total_games > 0
            else False
        )

    @hybrid_property
    def total_brooks_pitch_logs_scraped(self):
        return sum(game.scraped_brooks_pitch_logs for game in self.scrape_status_games)

    @hybrid_property
    def percent_complete_brooks_pitch_logs(self):
        return (
            self.total_brooks_pitch_logs_scraped / float(len(self.scrape_status_games))
            if len(self.scrape_status_games) > 0
            else 0.0
        )

    @hybrid_property
    def scraped_all_brooks_pitch_logs(self):
        return (
            all(game_status.scraped_brooks_pitch_logs for game_status in self.scrape_status_games)
            if self.total_games > 0
            else False
        )

    @hybrid_property
    def pitch_app_count_bbref(self):
        return sum(game.pitch_app_count_bbref for game in self.scrape_status_games)

    @hybrid_property
    def pitch_app_count_brooks(self):
        return sum(game.pitch_app_count_brooks for game in self.scrape_status_games)

    @hybrid_property
    def total_pitch_count_bbref(self):
        return sum(game.total_pitch_count_bbref for game in self.scrape_status_games)

    @hybrid_property
    def pitch_app_count_pitchfx(self):
        return len(self.scrape_status_pitchfx) if self.scrape_status_pitchfx else 0

    @hybrid_property
    def total_pitch_apps_scraped_pitchfx(self):
        return sum(pfx.scraped_pitchfx for pfx in self.scrape_status_pitchfx)

    @hybrid_property
    def total_pitch_apps_no_pitchfx_data(self):
        return sum(pfx.no_pitchfx_data for pfx in self.scrape_status_pitchfx)

    @hybrid_property
    def total_pitch_apps_with_pitchfx_data(self):
        return self.pitch_app_count_pitchfx - self.total_pitch_apps_no_pitchfx_data

    @hybrid_property
    def total_pitch_apps_combined_data(self):
        return sum(pfx.combined_pitchfx_bbref_data for pfx in self.scrape_status_pitchfx)

    @hybrid_property
    def total_pitch_count_pitch_logs(self):
        return sum(pfx.pitch_count_pitch_log for pfx in self.scrape_status_pitchfx)

    @hybrid_property
    def total_pitch_count_bbref_audited(self):
        return sum(pfx.pitch_count_bbref for pfx in self.scrape_status_pitchfx)

    @hybrid_property
    def total_pitch_count_pitchfx(self):
        return sum(pfx.pitch_count_pitchfx for pfx in self.scrape_status_pitchfx)

    @hybrid_property
    def total_pitch_count_pitchfx_audited(self):
        return sum(pfx.pitch_count_pitchfx_audited for pfx in self.scrape_status_pitchfx)

    @hybrid_property
    def total_duplicate_pitchfx_removed_count(self):
        return sum(pfx.duplicate_pitchfx_removed_count for pfx in self.scrape_status_pitchfx)

    @hybrid_property
    def total_extra_pitchfx_removed_count(self):
        return sum(pfx.extra_pitchfx_removed_count for pfx in self.scrape_status_pitchfx)

    @hybrid_property
    def total_pitch_apps_pitchfx_data_error(self):
        return sum(pfx.pitchfx_data_error for pfx in self.scrape_status_pitchfx)

    @hybrid_property
    def total_pitch_apps_pitchfx_is_valid(self):
        return sum(not pfx.pitchfx_data_error for pfx in self.scrape_status_pitchfx)

    @hybrid_property
    def percent_complete_pitchfx_logs_scraped(self):
        return (
            self.total_pitch_apps_scraped_pitchfx / float(self.pitch_app_count_pitchfx)
            if self.pitch_app_count_pitchfx > 0
            else 0.0
        )

    @hybrid_property
    def scraped_all_pitchfx_logs(self):
        if not self.scraped_all_brooks_pitch_logs:
            return False
        if not self.scrape_status_pitchfx:
            return True
        return all(pfx.scraped_pitchfx for pfx in self.scrape_status_pitchfx)

    @hybrid_property
    def combined_data_for_all_pitchfx_logs(self):
        if not self.scraped_all_pitchfx_logs:
            return False
        if not self.scrape_status_pitchfx:
            return False
        return all(pfx.combined_pitchfx_bbref_data for pfx in self.scrape_status_pitchfx)

    @hybrid_property
    def pitchfx_data_error_for_any_pitchfx_logs(self):
        if not self.scraped_all_pitchfx_logs:
            return False
        if not self.scrape_status_pitchfx:
            return False
        return any(pfx.pitchfx_data_error for pfx in self.scrape_status_pitchfx)

    @hybrid_property
    def pitchfx_is_valid_for_all_pitchfx_logs(self):
        if not self.scraped_all_pitchfx_logs:
            return True
        if not self.scrape_status_pitchfx:
            return True
        return all(not pfx.pitchfx_data_error for pfx in self.scrape_status_pitchfx)

    def __repr__(self):
        return f"<Season name={self.name}, id={self.id})>"

    def as_dict(self):
        d = {}
        d["name"] = self.name
        d["year"] = self.year
        d["start_date_str"] = self.start_date_str
        d["end_date_str"] = self.end_date_str
        d["asg_date_str"] = self.asg_date_str
        d["total_days"] = self.total_days
        d["total_days_scraped_bbref"] = self.total_days_scraped_bbref
        d["percent_complete_bbref_games_for_date"] = self.percent_complete_bbref_games_for_date
        d["scraped_all_bbref_games_for_date"] = self.scraped_all_bbref_games_for_date
        d["total_days_scraped_brooks"] = self.total_days_scraped_brooks
        d["percent_complete_brooks_games_for_date"] = self.percent_complete_brooks_games_for_date
        d["scraped_all_brooks_games_for_date"] = self.scraped_all_brooks_games_for_date
        d["total_games"] = self.total_games
        d["total_games_combined_success"] = self.total_games_combined_success
        d["total_games_combined_fail"] = self.total_games_combined_fail
        d["total_bbref_boxscores_scraped"] = self.total_bbref_boxscores_scraped
        d[
            "percent_complete_bbref_boxscores_scraped"
        ] = f"{self.percent_complete_bbref_boxscores_scraped:01.0f}%"
        d["scraped_all_bbref_boxscores"] = self.scraped_all_bbref_boxscores
        d["total_brooks_pitch_logs_scraped"] = self.total_brooks_pitch_logs_scraped
        d[
            "percent_complete_brooks_pitch_logs"
        ] = f"{self.percent_complete_brooks_pitch_logs:01.0f}%"
        d["scraped_all_brooks_pitch_logs"] = self.scraped_all_brooks_pitch_logs
        d["pitch_app_count_bbref"] = self.pitch_app_count_bbref
        d["pitch_app_count_brooks"] = self.pitch_app_count_brooks
        d["total_pitch_count_bbref"] = self.total_pitch_count_bbref
        d["pitch_app_count_pitchfx"] = self.pitch_app_count_pitchfx
        d["total_pitch_apps_scraped_pitchfx"] = self.total_pitch_apps_scraped_pitchfx
        d["total_pitch_apps_no_pitchfx_data"] = self.total_pitch_apps_no_pitchfx_data
        d["total_pitch_apps_with_pitchfx_data"] = self.total_pitch_apps_with_pitchfx_data
        d["total_pitch_apps_combined_data"] = self.total_pitch_apps_combined_data
        d["total_pitch_count_pitch_logs"] = self.total_pitch_count_pitch_logs
        d["total_pitch_count_bbref_audited"] = self.total_pitch_count_bbref_audited
        d["total_pitch_count_pitchfx"] = self.total_pitch_count_pitchfx
        d["total_pitch_count_pitchfx_audited"] = self.total_pitch_count_pitchfx_audited
        d["total_duplicate_pitchfx_removed_count"] = self.total_duplicate_pitchfx_removed_count
        d["total_extra_pitchfx_removed_count"] = self.total_extra_pitchfx_removed_count
        d["total_pitch_apps_pitchfx_data_error"] = self.total_pitch_apps_pitchfx_data_error
        d["total_pitch_apps_pitchfx_is_valid"] = self.total_pitch_apps_pitchfx_is_valid
        d["percent_complete_pitchfx_logs_scraped"] = self.percent_complete_pitchfx_logs_scraped
        d["scraped_all_pitchfx_logs"] = self.scraped_all_pitchfx_logs
        d["combined_data_for_all_pitchfx_logs"] = self.combined_data_for_all_pitchfx_logs
        d["pitchfx_data_error_for_any_pitchfx_logs"] = self.pitchfx_data_error_for_any_pitchfx_logs
        d["pitchfx_is_valid_for_all_pitchfx_logs"] = self.pitchfx_is_valid_for_all_pitchfx_logs
        return d

    def display(self):
        season_dict = self.as_dict()
        title = self.name
        display_dict(season_dict, title=title)

    def status_report(self):
        scraped_bbref_boxscores = "YES" if self.scraped_all_bbref_boxscores else "NO"
        scraped_brooks_pitch_logs = "YES" if self.scraped_all_brooks_pitch_logs else "NO"
        scraped_all_pitchfx_logs = "YES" if self.scraped_all_pitchfx_logs else "NO"
        combined_data_for_all_pitchfx_logs = (
            "YES" if self.combined_data_for_all_pitchfx_logs else "NO"
        )
        pitchfx_data_error_for_any_pitchfx_logs = (
            "YES" if self.pitchfx_data_error_for_any_pitchfx_logs else "NO"
        )
        total_pitchfx_removed_count = (
            self.total_duplicate_pitchfx_removed_count + self.total_extra_pitchfx_removed_count
        )
        return (
            f"BBref Daily Dash Scraped....................: "
            f"{self.total_days_scraped_bbref:,}/{self.total_days:,} days "
            f"({self.percent_complete_bbref_games_for_date:.0%})\n"
            f"Brooks Daily Dash Scraped...................: "
            f"{self.total_days_scraped_brooks:,}/{self.total_days:,} days "
            f"({self.percent_complete_brooks_games_for_date:.0%})\n"
            f"BBref Boxscores Scraped.....................: {scraped_bbref_boxscores} "
            f"{self.total_bbref_boxscores_scraped}/{self.total_games}\n"
            f"Brooks Games Scraped........................: {scraped_brooks_pitch_logs} "
            f"{self.total_brooks_pitch_logs_scraped}/{self.total_games}\n"
            f"PitchFx Logs Scraped........................: {scraped_all_pitchfx_logs} "
            f"{self.total_pitch_apps_scraped_pitchfx}/{self.pitch_app_count_pitchfx} "
            f"({self.percent_complete_pitchfx_logs_scraped:.0%})\n"
            f"Combined BBRef/PitchFX Data (Success/Fail)..: {combined_data_for_all_pitchfx_logs} "
            f"{self.total_games_combined_success}/{self.total_games_combined_fail}\n"
            f"PitchFX Data Errors (Error/Total)...........: {pitchfx_data_error_for_any_pitchfx_logs} "
            f"{self.total_pitch_apps_pitchfx_data_error}/{self.pitch_app_count_pitchfx}\n"
            f"Pitch App Count (BBRef/Brooks)..............: "
            f"{self.pitch_app_count_bbref}/{self.pitch_app_count_brooks}\n"
            f"Pitch App Count (PFx/data/no data)..........: {self.pitch_app_count_pitchfx}/"
            f"{self.total_pitch_apps_with_pitchfx_data}/{self.total_pitch_apps_no_pitchfx_data}\n"
            f"Pitch Count (BBRef/Brooks/PFx)..............: {self.total_pitch_count_bbref}/"
            f"{self.total_pitch_count_pitch_logs}/{self.total_pitch_count_pitchfx}\n"
            f"Pitch Count Audited (BBRef/PFx/Removed).....: {self.total_pitch_count_bbref_audited}/"
            f"{self.total_pitch_count_pitchfx_audited}/{total_pitchfx_removed_count}\n"
        )

    def get_date_range(self):
        return get_date_range_util(self.start_date, self.end_date)

    def get_all_bbref_game_ids_eligible_for_audit(self):
        return [
            game.bbref_game_id
            for game in self.scrape_status_games
            if game.scraped_all_pitchfx_logs and not game.combined_data_for_all_pitchfx_logs
        ]

    def get_all_bbref_game_ids_combined_data_success(self):
        return [
            game.bbref_game_id for game in self.scrape_status_games if game.combined_data_success
        ]

    def get_all_bbref_game_ids_combined_data_fail(self):
        return [game.bbref_game_id for game in self.scrape_status_games if game.combined_data_fail]

    def get_all_bbref_game_ids_pitchfx_data_error(self):
        return [
            game.bbref_game_id
            for game in self.scrape_status_games
            if game.pitchfx_data_error_for_any_pitchfx_logs
        ]

    def get_all_bbref_game_ids_all_pitchfx_logs_are_valid(self):
        return [
            game.bbref_game_id
            for game in self.scrape_status_games
            if game.combined_data_for_all_pitchfx_logs
            and game.pitchfx_is_valid_for_all_pitchfx_logs
        ]

    @classmethod
    def find_by_year(cls, db_session, year, season_type=SeasonType.REGULAR_SEASON):
        return (
            db_session.query(cls).filter_by(season_type=season_type).filter_by(year=year).first()
        )

    @classmethod
    def is_date_in_season(cls, db_session, check_date, season_type=SeasonType.REGULAR_SEASON):
        season = cls.find_by_year(db_session, check_date.year)
        if not season:
            error = f"Database does not contain info for MLB {season_type} {check_date.year}"
            return Result.Fail(error)

        date_str = check_date.strftime(DATE_ONLY)
        if check_date < season.start_date or check_date > season.end_date:
            error = f"{date_str} is not within the scope of the {season.name}"
            return Result.Fail(error)
        return Result.Ok(season)

    @classmethod
    def validate_date_range(cls, db_session, start, end):
        if start.year != end.year:
            error = (
                "Start and end dates must both be in the same year and within "
                "the scope of that year's MLB Regular Season."
            )
            return Result.Fail(error)
        if start > end:
            start_str = start.strftime(DATE_ONLY)
            end_str = end.strftime(DATE_ONLY)
            error = (
                '"start" must be a date before (or the same date as) "end":\n'
                f"start..: {start_str}\n"
                f"end....: {end_str}"
            )
            return Result.Fail(error)
        season = cls.find_by_year(db_session, start.year)
        start_date_valid = cls.is_date_in_season(db_session, start).success
        end_date_valid = cls.is_date_in_season(db_session, end).success
        if not start_date_valid or not end_date_valid:
            error = (
                f"Start and end date must both be within the {season.name}:\n"
                f"{season.name} Start Date..: {season.start_date_str}\n"
                f"{season.name} End Date....: {season.end_date_str}"
            )
            return Result.Fail(error)
        return Result.Ok(season)

    @classmethod
    def all_regular_seasons(cls, db_session):
        matching_seasons = db_session.query(cls).filter_by(season_type=SeasonType.REGULAR_SEASON)
        return [season for season in matching_seasons]

    @classmethod
    def is_this_the_asg_date(cls, db_session, game_date):
        season = cls.find_by_year(db_session, game_date.year)
        return game_date == season.asg_date if season else None
