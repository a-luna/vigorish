"""Db model that describes a MLB season and tracks data scraping progress."""
from datetime import date

from sqlalchemy import Column, DateTime, Integer
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship
from sqlalchemy.types import Enum

from vigorish.config.database import Base
from vigorish.enums import SeasonType
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
    asg_date = Column(DateTime, default=date.min)
    season_type = Column(Enum(SeasonType), default=SeasonType.NONE)

    scrape_status_dates = relationship("DateScrapeStatus", backref="season")
    scrape_status_games = relationship("GameScrapeStatus", backref="season")
    scrape_status_pitchfx = relationship("PitchAppScrapeStatus", backref="season")
    scrape_jobs = relationship("ScrapeJob", backref="season")
    pitch_app_status = relationship(
        "Season_PitchApp_View",
        backref="original",
        uselist=False,
        primaryjoin="Season.id==Season_PitchApp_View.id",
        foreign_keys="Season_PitchApp_View.id",
    )
    game_status = relationship(
        "Season_Game_View",
        backref="original",
        uselist=False,
        primaryjoin="Season.id==Season_Game_View.id",
        foreign_keys="Season_Game_View.id",
    )
    game_pitch_app_status = relationship(
        "Season_Game_PitchApp_View",
        backref="original",
        uselist=False,
        primaryjoin="Season.id == Season_Game_PitchApp_View.id",
        foreign_keys="Season_Game_PitchApp_View.id",
    )
    date_status = relationship(
        "Season_Date_View",
        backref="original",
        uselist=False,
        primaryjoin="Season.id==Season_Date_View.id",
        foreign_keys="Season_Date_View.id",
    )

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
        return self.date_status.total_days

    @hybrid_property
    def total_days_scraped_bbref(self):
        return (
            self.date_status.total_scraped_daily_dash_bbref
            if self.date_status.total_scraped_daily_dash_bbref
            else 0
        )

    @hybrid_property
    def percent_complete_bbref_games_for_date(self):
        return (
            self.total_days_scraped_bbref / float(self.total_days) if self.total_days > 0 else 0.0
        )

    @hybrid_property
    def scraped_all_bbref_games_for_date(self):
        return self.total_days == self.total_days_scraped_bbref if self.total_days > 0 else False

    @hybrid_property
    def total_days_scraped_brooks(self):
        return (
            self.date_status.total_scraped_daily_dash_brooks
            if self.date_status.total_scraped_daily_dash_brooks
            else 0
        )

    @hybrid_property
    def percent_complete_brooks_games_for_date(self):
        return (
            self.total_days_scraped_brooks / float(self.total_days) if self.total_days > 0 else 0.0
        )

    @hybrid_property
    def scraped_all_brooks_games_for_date(self):
        return self.total_days == self.total_days_scraped_brooks if self.total_days > 0 else False

    @hybrid_property
    def total_games(self):
        return self.game_status.total_games if self.game_status.total_games else 0

    @hybrid_property
    def total_games_combined_success(self):
        return (
            self.game_status.total_combined_data_success
            if self.game_status.total_combined_data_success
            else 0
        )

    @hybrid_property
    def total_games_combined_fail(self):
        return (
            self.game_status.total_combined_data_fail
            if self.game_status.total_combined_data_fail
            else 0
        )

    @hybrid_property
    def total_games_combined(self):
        return self.total_games_combined_success + self.total_games_combined_fail

    @hybrid_property
    def total_bbref_boxscores_scraped(self):
        return (
            self.game_status.total_scraped_bbref_boxscore
            if self.game_status.total_scraped_bbref_boxscore
            else 0
        )

    @hybrid_property
    def percent_complete_bbref_boxscores_scraped(self):
        return (
            self.total_bbref_boxscores_scraped / float(self.total_games)
            if self.total_games > 0
            else 0.0
        )

    @hybrid_property
    def scraped_all_bbref_boxscores(self):
        return (
            self.total_games == self.total_bbref_boxscores_scraped
            if self.total_games > 0
            else False
        )

    @hybrid_property
    def total_brooks_pitch_logs_scraped(self):
        return (
            self.game_status.total_scraped_brooks_pitch_logs
            if self.game_status.total_scraped_brooks_pitch_logs
            else 0
        )

    @hybrid_property
    def percent_complete_brooks_pitch_logs(self):
        return (
            self.total_brooks_pitch_logs_scraped / float(self.total_games)
            if self.total_games > 0
            else 0.0
        )

    @hybrid_property
    def scraped_all_brooks_pitch_logs(self):
        return (
            self.total_games == self.total_brooks_pitch_logs_scraped
            if self.total_games > 0
            else False
        )

    @hybrid_property
    def pitch_app_count_bbref(self):
        return (
            self.game_status.total_pitch_app_count_bbref
            if self.game_status.total_pitch_app_count_bbref
            else 0
        )

    @hybrid_property
    def pitch_app_count_brooks(self):
        return (
            self.game_status.total_pitch_app_count_brooks
            if self.game_status.total_pitch_app_count_brooks
            else 0
        )

    @hybrid_property
    def total_pitch_count_bbref(self):
        return (
            self.game_status.total_pitch_count_bbref
            if self.game_status.total_pitch_count_bbref
            else 0
        )

    @hybrid_property
    def pitch_app_count_pitchfx(self):
        return self.pitch_app_status.total_pitchfx if self.pitch_app_status.total_pitchfx else 0

    @hybrid_property
    def total_pitch_apps_scraped_pitchfx(self):
        return (
            self.pitch_app_status.total_pitchfx_scraped
            if self.pitch_app_status.total_pitchfx_scraped
            else 0
        )

    @hybrid_property
    def total_pitch_apps_no_pitchfx_data(self):
        return (
            self.pitch_app_status.total_no_pitchfx_data
            if self.pitch_app_status.total_no_pitchfx_data
            else 0
        )

    @hybrid_property
    def total_pitch_apps_with_pitchfx_data(self):
        return self.pitch_app_count_pitchfx - self.total_pitch_apps_no_pitchfx_data

    @hybrid_property
    def total_pitch_apps_combined_data(self):
        return (
            self.pitch_app_status.total_combined_pitchfx_bbref_data
            if self.pitch_app_status.total_combined_pitchfx_bbref_data
            else 0
        )

    @hybrid_property
    def total_pitch_apps_pitchfx_error(self):
        return (
            self.pitch_app_status.total_pitchfx_error
            if self.pitch_app_status.total_pitchfx_error
            else 0
        )

    @hybrid_property
    def total_pitch_apps_invalid_pitchfx(self):
        return (
            self.pitch_app_status.total_invalid_pitchfx
            if self.pitch_app_status.total_invalid_pitchfx
            else 0
        )

    @hybrid_property
    def total_pitch_apps_pitchfx_is_valid(self):
        return sum(
            not (pfx.pitchfx_error or pfx.invalid_pitchfx) for pfx in self.scrape_status_pitchfx
        )

    @hybrid_property
    def total_pitch_count_pitch_logs(self):
        return (
            self.pitch_app_status.total_pitch_count_pitch_log
            if self.pitch_app_status.total_pitch_count_pitch_log
            else 0
        )

    @hybrid_property
    def total_pitch_count_bbref_audited(self):
        return (
            self.pitch_app_status.total_pitch_count_bbref
            if self.pitch_app_status.total_pitch_count_bbref
            else 0
        )

    @hybrid_property
    def total_pitch_count_pitchfx(self):
        return (
            self.pitch_app_status.total_pitch_count_pitchfx
            if self.pitch_app_status.total_pitch_count_pitchfx
            else 0
        )

    @hybrid_property
    def total_pitch_count_pitchfx_audited(self):
        return (
            self.pitch_app_status.total_pitch_count_pitchfx_audited
            if self.pitch_app_status.total_pitch_count_pitchfx_audited
            else 0
        )

    @hybrid_property
    def total_duplicate_pitchfx_removed_count(self):
        return (
            self.pitch_app_status.total_duplicate_pitchfx_removed_count
            if self.pitch_app_status.total_duplicate_pitchfx_removed_count
            else 0
        )

    @hybrid_property
    def total_missing_pitchfx_count(self):
        return (
            self.pitch_app_status.total_missing_pitchfx_count
            if self.pitch_app_status.total_missing_pitchfx_count
            else 0
        )

    @hybrid_property
    def total_extra_pitchfx_count(self):
        return (
            self.pitch_app_status.total_extra_pitchfx_count
            if self.pitch_app_status.total_extra_pitchfx_count
            else 0
        )

    @hybrid_property
    def total_extra_pitchfx_removed_count(self):
        return (
            self.pitch_app_status.total_extra_pitchfx_removed_count
            if self.pitch_app_status.total_extra_pitchfx_removed_count
            else 0
        )

    @hybrid_property
    def total_batters_faced_bbref(self):
        return (
            self.pitch_app_status.total_batters_faced_bbref
            if self.pitch_app_status.total_batters_faced_bbref
            else 0
        )

    @hybrid_property
    def total_batters_faced_pitchfx(self):
        return (
            self.pitch_app_status.total_batters_faced_pitchfx
            if self.pitch_app_status.total_batters_faced_pitchfx
            else 0
        )

    @hybrid_property
    def total_at_bats_missing_pitchfx(self):
        return (
            self.pitch_app_status.total_at_bats_missing_pitchfx
            if self.pitch_app_status.total_at_bats_missing_pitchfx
            else 0
        )

    @hybrid_property
    def total_at_bats_extra_pitchfx(self):
        return (
            self.pitch_app_status.total_at_bats_extra_pitchfx
            if self.pitch_app_status.total_at_bats_extra_pitchfx
            else 0
        )

    @hybrid_property
    def total_at_bats_extra_pitchfx_removed(self):
        return (
            self.pitch_app_status.total_at_bats_extra_pitchfx_removed
            if self.pitch_app_status.total_at_bats_extra_pitchfx_removed
            else 0
        )

    @hybrid_property
    def total_at_bats_pitchfx_error(self):
        return (
            self.pitch_app_status.total_at_bats_pitchfx_error
            if self.pitch_app_status.total_at_bats_pitchfx_error
            else 0
        )

    @hybrid_property
    def total_at_bats_invalid_pitchfx(self):
        return (
            self.pitch_app_status.total_at_bats_invalid_pitchfx
            if self.pitch_app_status.total_at_bats_invalid_pitchfx
            else 0
        )

    @hybrid_property
    def scraped_all_pitchfx_logs(self):
        if not self.scraped_all_brooks_pitch_logs:
            return False
        if not self.scrape_status_pitchfx:
            return True
        return self.pitch_app_count_pitchfx == self.total_pitch_apps_scraped_pitchfx

    @hybrid_property
    def combined_data_for_all_pitchfx_logs(self):
        if not self.scraped_all_pitchfx_logs:
            return False
        if not self.scrape_status_pitchfx:
            return False
        return self.pitch_app_count_pitchfx == self.total_pitch_apps_combined_data

    @hybrid_property
    def pitchfx_error_for_any_pitchfx_logs(self):
        if not self.scraped_all_pitchfx_logs:
            return False
        if not self.scrape_status_pitchfx:
            return False
        return any((pfx.pitchfx_error or pfx.invalid_pitchfx) for pfx in self.scrape_status_pitchfx)

    @hybrid_property
    def pitchfx_is_valid_for_all_pitchfx_logs(self):
        if not self.scraped_all_pitchfx_logs:
            return True
        if not self.scrape_status_pitchfx:
            return True
        return all(
            not (pfx.pitchfx_error or pfx.invalid_pitchfx) for pfx in self.scrape_status_pitchfx
        )

    @hybrid_property
    def percent_complete_pitchfx_logs_scraped(self):
        return (
            self.total_pitch_apps_scraped_pitchfx / float(self.pitch_app_count_pitchfx)
            if self.pitch_app_count_pitchfx > 0
            else 0.0
        )

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
        d["total_games_combined"] = self.total_games_combined
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
        d["total_pitch_apps_pitchfx_error"] = self.total_pitch_apps_pitchfx_error
        d["total_pitch_apps_invalid_pitchfx"] = self.total_pitch_apps_invalid_pitchfx
        d["total_pitch_apps_pitchfx_error"] = self.total_pitch_apps_pitchfx_error
        d["total_pitch_apps_pitchfx_is_valid"] = self.total_pitch_apps_pitchfx_is_valid
        d["total_pitch_count_pitch_logs"] = self.total_pitch_count_pitch_logs
        d["total_pitch_count_bbref"] = self.total_pitch_count_bbref
        d["total_pitch_count_bbref_audited"] = self.total_pitch_count_bbref_audited
        d["total_pitch_count_pitchfx"] = self.total_pitch_count_pitchfx
        d["total_pitch_count_pitchfx_audited"] = self.total_pitch_count_pitchfx_audited
        d["total_duplicate_pitchfx_removed_count"] = self.total_duplicate_pitchfx_removed_count
        d["total_missing_pitchfx_count"] = self.total_missing_pitchfx_count
        d["total_extra_pitchfx_count"] = self.total_extra_pitchfx_count
        d["total_extra_pitchfx_removed_count"] = self.total_extra_pitchfx_removed_count
        d["total_batters_faced_bbref"] = self.total_batters_faced_bbref
        d["total_batters_faced_pitchfx"] = self.total_batters_faced_pitchfx
        d["total_at_bats_missing_pitchfx"] = self.total_at_bats_missing_pitchfx
        d["total_at_bats_extra_pitchfx"] = self.total_at_bats_extra_pitchfx
        d["total_at_bats_extra_pitchfx_removed"] = self.total_at_bats_extra_pitchfx_removed
        d["total_at_bats_pitchfx_error"] = self.total_at_bats_pitchfx_error
        d["total_at_bats_invalid_pitchfx"] = self.total_at_bats_invalid_pitchfx
        d["scraped_all_pitchfx_logs"] = self.scraped_all_pitchfx_logs
        d["combined_data_for_all_pitchfx_logs"] = self.combined_data_for_all_pitchfx_logs
        d["pitchfx_error_for_any_pitchfx_logs"] = self.pitchfx_error_for_any_pitchfx_logs
        d["pitchfx_is_valid_for_all_pitchfx_logs"] = self.pitchfx_is_valid_for_all_pitchfx_logs
        d["percent_complete_pitchfx_logs_scraped"] = self.percent_complete_pitchfx_logs_scraped
        return d

    def display(self):
        display_dict(self.as_dict(), title=self.name)

    def status_report(self):
        scraped_bbref_boxscores = "YES" if self.scraped_all_bbref_boxscores else "NO"
        scraped_brooks_pitch_logs = "YES" if self.scraped_all_brooks_pitch_logs else "NO"
        scraped_all_pitchfx_logs = "YES" if self.scraped_all_pitchfx_logs else "NO"
        combined_data_for_all_pitchfx_logs = (
            "YES" if self.combined_data_for_all_pitchfx_logs else "NO"
        )
        pitchfx_error_for_any_pitchfx_logs = (
            "YES" if self.pitchfx_error_for_any_pitchfx_logs else "NO"
        )
        total_pitchfx_removed_count = (
            self.total_duplicate_pitchfx_removed_count + self.total_extra_pitchfx_removed_count
        )
        return (
            f"BBref Daily Dash Scraped.....................: "
            f"{self.total_days_scraped_bbref:,}/{self.total_days:,} days "
            f"({self.percent_complete_bbref_games_for_date:.0%})\n"
            f"Brooks Daily Dash Scraped....................: "
            f"{self.total_days_scraped_brooks:,}/{self.total_days:,} days "
            f"({self.percent_complete_brooks_games_for_date:.0%})\n"
            f"BBref Boxscores Scraped......................: {scraped_bbref_boxscores} "
            f"{self.total_bbref_boxscores_scraped}/{self.total_games}\n"
            f"Brooks Games Scraped.........................: {scraped_brooks_pitch_logs} "
            f"{self.total_brooks_pitch_logs_scraped}/{self.total_games}\n"
            f"PitchFx Logs Scraped.........................: {scraped_all_pitchfx_logs} "
            f"{self.total_pitch_apps_scraped_pitchfx}/{self.pitch_app_count_pitchfx} "
            f"({self.percent_complete_pitchfx_logs_scraped:.0%})\n"
            f"Combined BBRef/PitchFX Data (Success/Total)..: {combined_data_for_all_pitchfx_logs} "
            f"{self.total_games_combined_success}/{self.total_games_combined}\n"
            f"Pitch App Count (BBRef/Brooks)...............: "
            f"{self.pitch_app_count_bbref}/{self.pitch_app_count_brooks}\n"
            f"Pitch App Count (PFx/data/no data)...........: {self.pitch_app_count_pitchfx}/"
            f"{self.total_pitch_apps_with_pitchfx_data}/{self.total_pitch_apps_no_pitchfx_data}\n"
            f"PitchFX Data Errors (Valid AB/Invalid AB)....: {pitchfx_error_for_any_pitchfx_logs} "
            f"{self.total_pitch_apps_pitchfx_error}/{self.total_pitch_apps_invalid_pitchfx}\n"
            f"Pitch Count (BBRef/Brooks/PFx)...............: {self.total_pitch_count_bbref}/"
            f"{self.total_pitch_count_pitch_logs}/{self.total_pitch_count_pitchfx}\n"
            "Pitch Count Audited (BBRef/PFx/Removed)......: "
            f"{self.total_pitch_count_bbref_audited}/{self.total_pitch_count_pitchfx_audited}/"
            f"{total_pitchfx_removed_count}\n"
        )

    def get_date_range(self):
        return get_date_range_util(self.start_date, self.end_date)

    def get_all_bbref_game_ids_no_pitchfx_data_for_all_pitch_apps(self):
        return [
            game.bbref_game_id
            for game in self.scrape_status_games
            if game.no_pitchfx_data_for_all_pitch_apps
        ]

    def get_all_bbref_game_ids_no_pitchfx_data_for_any_pitch_apps(self):
        return [
            game.bbref_game_id
            for game in self.scrape_status_games
            if game.no_pitchfx_data_for_any_pitch_apps
        ]

    def get_all_bbref_game_ids_combined_data_success(self):
        return [
            game.bbref_game_id for game in self.scrape_status_games if game.combined_data_success
        ]

    def get_all_bbref_game_ids_combined_data_fail(self):
        return [game.bbref_game_id for game in self.scrape_status_games if game.combined_data_fail]

    def get_all_bbref_game_ids_eligible_for_audit(self):
        return [
            game.bbref_game_id
            for game in self.game_pitch_app_status
            if game.total_pitchfx == game.total_pitchfx_scraped
            and game.total_pitchfx != game.total_combined_pitchfx_bbref_data
        ]

    def get_all_bbref_game_ids_all_pitchfx_logs_are_valid(self):
        return [
            game.bbref_game_id
            for game in self.game_pitch_app_status
            if game.total_pitchfx == game.total_combined_pitchfx_bbref_data
            and game.total_pitchfx_error == 0
            and game.total_invalid_pitchfx == 0
        ]

    def get_all_bbref_game_ids_pitchfx_error(self):
        return [
            game.bbref_game_id
            for game in self.game_pitch_app_status
            if game.total_pitchfx_error and game.total_pitchfx_error > 0
        ]

    def get_all_bbref_game_ids_invalid_pitchfx(self):
        return [
            game.bbref_game_id
            for game in self.game_pitch_app_status
            if game.total_invalid_pitchfx and game.total_invalid_pitchfx > 0
        ]

    @classmethod
    def find_by_year(cls, db_session, year, season_type=SeasonType.REGULAR_SEASON):
        return db_session.query(cls).filter_by(season_type=season_type).filter_by(year=year).first()

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
            error = [
                "Start and end dates must both be in the same year and within "
                "the scope of that year's MLB Regular Season."
            ]
            return Result.Fail(error)
        if start > end:
            start_str = start.strftime(DATE_ONLY)
            end_str = end.strftime(DATE_ONLY)
            error = [
                '"start" must be a date before (or the same date as) "end":',
                f"start..: {start_str}",
                f"end....: {end_str}",
            ]
            return Result.Fail(error)
        season = cls.find_by_year(db_session, start.year)
        start_date_valid = cls.is_date_in_season(db_session, start).success
        end_date_valid = cls.is_date_in_season(db_session, end).success
        if not start_date_valid or not end_date_valid:
            error = [
                f"Start and end date must both be within the {season.name}:",
                f"{season.name} Start Date..: {season.start_date_str}",
                f"{season.name} End Date....: {season.end_date_str}",
            ]
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

    @classmethod
    def regular_season_map(cls, db_session):
        regular_seasons = cls.all_regular_seasons(db_session)
        return {s.year: s.id for s in regular_seasons}
