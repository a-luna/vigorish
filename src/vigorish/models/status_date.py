from dataclasses import dataclass
from datetime import datetime

from dataclass_csv import accept_whitespaces, dateformat
from sqlalchemy import Column, DateTime, ForeignKey, Integer
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

from vigorish.config.database import Base
from vigorish.util.dataclass_helpers import dict_from_dataclass, sanitize_row_dict
from vigorish.util.dt_format_strings import DATE_ONLY, DATE_ONLY_2, DATE_ONLY_TABLE_ID


class DateScrapeStatus(Base):

    __tablename__ = "scrape_status_date"
    id = Column(Integer, primary_key=True)
    game_date = Column(DateTime)
    scraped_daily_dash_bbref = Column(Integer, default=0)
    scraped_daily_dash_brooks = Column(Integer, default=0)
    game_count_bbref = Column(Integer, default=0)
    game_count_brooks = Column(Integer, default=0)
    season_id = Column(Integer, ForeignKey("season.id"))

    scrape_status_games = relationship("GameScrapeStatus", backref="scrape_status_date")
    scrape_status_pitchfx = relationship("PitchAppScrapeStatus", backref="scrape_status_date")
    pitch_app_status = relationship(
        "Date_PitchApp_View",
        backref="original",
        uselist=False,
        primaryjoin="DateScrapeStatus.id==Date_PitchApp_View.id",
        foreign_keys="Date_PitchApp_View.id",
    )

    @hybrid_property
    def game_date_str(self):
        return self.game_date.strftime(DATE_ONLY_2)

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
    def total_games_combined(self):
        return self.total_games_combined_success + self.total_games_combined_fail

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
        if not self.scraped_daily_dash_bbref or not self.scraped_daily_dash_brooks:
            return False
        return all(game.scraped_bbref_boxscore == 1 for game in self.scrape_status_games)

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
        if not self.scraped_daily_dash_bbref or not self.scraped_daily_dash_brooks:
            return False
        return all(game.scraped_brooks_pitch_logs == 1 for game in self.scrape_status_games)

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
    def total_pitch_apps_any_pitchfx_error(self):
        return +self.total_pitch_apps_pitchfx_error + self.total_pitch_apps_invalid_pitchfx

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
    def total_at_bats_pitchfx_complete(self):
        return (
            self.pitch_app_status.total_at_bats_pitchfx_complete
            if self.pitch_app_status.total_at_bats_pitchfx_complete
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
            if self.pitch_app_status.total_at_bats_pitchfx_error
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

    @hybrid_property
    def scraped_no_data(self):
        return self.scraped_daily_dash_bbref == 0 and self.scraped_daily_dash_brooks == 0

    @hybrid_property
    def scraped_only_bbref_daily_dash(self):
        return (
            self.scraped_daily_dash_bbref == 1
            and self.scraped_daily_dash_brooks == 0
            and not self.scraped_all_bbref_boxscores
            and not self.scraped_all_brooks_pitch_logs
            and not self.scraped_all_pitchfx_logs
        )

    @hybrid_property
    def scraped_only_brooks_daily_dash(self):
        return (
            self.scraped_daily_dash_bbref == 0
            and self.scraped_daily_dash_brooks == 1
            and not self.scraped_all_bbref_boxscores
            and not self.scraped_all_brooks_pitch_logs
            and not self.scraped_all_pitchfx_logs
        )

    @hybrid_property
    def scraped_only_both_daily_dash(self):
        return (
            self.scraped_daily_dash_bbref == 1
            and self.scraped_daily_dash_brooks == 1
            and not self.scraped_all_bbref_boxscores
            and not self.scraped_all_brooks_pitch_logs
            and not self.scraped_all_pitchfx_logs
        )

    @hybrid_property
    def scraped_only_bbref_boxscores(self):
        if (
            self.scraped_all_bbref_boxscores
            and not self.scraped_all_brooks_pitch_logs
            and not self.scraped_all_pitchfx_logs
        ):
            return True
        return False

    @hybrid_property
    def scraped_only_brooks_pitch_logs(self):
        if (
            self.scraped_all_brooks_pitch_logs
            and not self.scraped_all_bbref_boxscores
            and not self.scraped_all_pitchfx_logs
        ):
            return True
        return False

    @hybrid_property
    def scraped_only_both_bbref_boxscores_and_brooks_pitch_logs(self):
        if (
            self.scraped_all_bbref_boxscores
            and self.scraped_all_brooks_pitch_logs
            and not self.scraped_all_pitchfx_logs
        ):
            return True
        return False

    @hybrid_property
    def scraped_all_game_data(self):
        return (
            self.scraped_all_bbref_boxscores
            and self.scraped_all_brooks_pitch_logs
            and self.scraped_all_pitchfx_logs
        )

    @hybrid_property
    def scrape_status_description(self):
        if self.scraped_all_game_data:
            return "Scraped all game data"
        elif self.scraped_only_both_bbref_boxscores_and_brooks_pitch_logs:
            return (
                "Missing Brooks pitchfx logs "
                f"({self.total_pitch_apps_scraped_pitchfx}/{self.pitch_app_count_brooks}, "
                f"{self.percent_complete_pitchfx_logs_scraped:.0%})"
            )
        elif self.scraped_only_brooks_pitch_logs:
            return "Scraped Brooks pitch logs, missing BBref boxscores"
        elif self.scraped_only_bbref_boxscores:
            return "Scraped BBref boxscores, missing Brooks pitch logs"
        elif self.scraped_only_both_daily_dash:
            return "Missing BBref boxscores and Brooks pitch logs"
        elif self.scraped_only_brooks_daily_dash:
            return "Scraped Brooks daily dashboard, missing BBref daily dash"
        elif self.scraped_only_bbref_daily_dash:
            return "Scraped BBref daily dashboard, missing Brooks daily dash"
        elif self.scraped_no_data:
            return "No data has been scraped"
        else:
            return "N/A"

    def __init__(self, **kwargs):
        super(DateScrapeStatus, self).__init__(**kwargs)
        self.id = self.game_date.strftime(DATE_ONLY_TABLE_ID)

    def __repr__(self):
        return f"<DateScrapeStatus date={self.game_date_str}, season_id={self.season_id}>"

    def as_dict(self):
        d = {c.name: getattr(self, c.name) for c in self.__table__.columns}
        d["game_date_str"] = self.game_date_str
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
        d["total_pitch_apps_pitchfx_is_valid"] = self.total_pitch_apps_pitchfx_is_valid
        d["total_pitch_count_pitch_logs"] = self.total_pitch_count_pitch_logs
        d["total_pitch_count_bbref_audited"] = self.total_pitch_count_bbref_audited
        d["total_pitch_count_pitchfx"] = self.total_pitch_count_pitchfx
        d["total_pitch_count_pitchfx_audited"] = self.total_pitch_count_pitchfx_audited
        d["total_duplicate_pitchfx_removed_count"] = self.total_duplicate_pitchfx_removed_count
        d["total_missing_pitchfx_count"] = self.total_missing_pitchfx_count
        d["total_extra_pitchfx_count"] = self.total_extra_pitchfx_count
        d["total_extra_pitchfx_removed_count"] = self.total_extra_pitchfx_removed_count
        d["total_batters_faced_bbref"] = self.total_batters_faced_bbref
        d["total_batters_faced_pitchfx"] = self.total_batters_faced_pitchfx
        d["total_at_bats_pitchfx_complete"] = self.total_at_bats_pitchfx_complete
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
        d["scraped_no_data"] = self.scraped_only_bbref_daily_dash
        d["scraped_only_bbref_daily_dash"] = self.scraped_only_bbref_daily_dash
        d["scraped_only_brooks_daily_dash"] = self.scraped_only_brooks_daily_dash
        d["scraped_only_both_daily_dash"] = self.scraped_only_both_daily_dash
        d["scraped_only_bbref_boxscores"] = self.scraped_only_bbref_boxscores
        d["scraped_only_brooks_pitch_logs"] = self.scraped_only_brooks_pitch_logs
        d[
            "scraped_only_both_bbref_boxscores_and_brooks_pitch_logs"
        ] = self.scraped_only_both_bbref_boxscores_and_brooks_pitch_logs
        d["scraped_all_game_data"] = self.scraped_all_game_data
        d["scrape_status_description"] = self.scrape_status_description
        return d

    def status_report(self):
        scraped_daily_bbref = "YES" if self.scraped_daily_dash_bbref == 1 else "NO"
        scraped_daily_brooks = "YES" if self.scraped_daily_dash_brooks == 1 else "NO"
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
            f"Overall Status For Date......................: {self.scrape_status_description}\n"
            f"Scraped Daily Dashboard (BBRef/Brooks).......: "
            f"{scraped_daily_bbref}/{scraped_daily_brooks}\n"
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

    def games_status_report(self):
        return "\n".join([game_status.status_report() for game_status in self.scrape_status_games])

    def as_csv_dict(self):
        return dict_from_dataclass(self, DateScrapeStatusCsvRow, date_format=DATE_ONLY)

    @classmethod
    def create_new(cls, game_date, season_id):
        game_date_str = game_date.strftime(DATE_ONLY_TABLE_ID)
        return cls(id=int(game_date_str), game_date=game_date, season_id=season_id)

    @classmethod
    def get_csv_col_names(cls):
        return [name for name in DateScrapeStatusCsvRow.__dataclass_fields__.keys()]

    @classmethod
    def export_table_as_csv(cls, db_session):
        col_names = ",".join(cls.get_csv_col_names())
        csv_dicts = (obj.as_csv_dict() for obj in db_session.query(cls).all())
        csv_rows = (",".join(sanitize_row_dict(d, date_format=DATE_ONLY)) for d in csv_dicts)
        yield col_names
        for row in csv_rows:
            yield row

    @classmethod
    def find_by_date(cls, db_session, game_date):
        date_str = game_date.strftime(DATE_ONLY_TABLE_ID)
        return db_session.query(cls).get(int(date_str))

    @classmethod
    def get_all_bbref_scraped_dates_for_season(cls, db_session, season_id):
        return [
            date_status.game_date
            for date_status in db_session.query(cls).filter_by(season_id=season_id).all()
            if date_status.scraped_daily_dash_bbref == 1
        ]

    @classmethod
    def get_all_bbref_unscraped_dates_for_season(cls, db_session, season_id):
        return [
            date_status.game_date
            for date_status in db_session.query(cls).filter_by(season_id=season_id).all()
            if date_status.scraped_daily_dash_bbref == 0
        ]

    @classmethod
    def get_all_brooks_scraped_dates_for_season(cls, db_session, season_id):
        return [
            date_status.game_date
            for date_status in db_session.query(cls).filter_by(season_id=season_id).all()
            if date_status.scraped_daily_dash_brooks == 1
        ]

    @classmethod
    def get_all_brooks_unscraped_dates_for_season(cls, db_session, season_id):
        return [
            date_status.game_date
            for date_status in db_session.query(cls).filter_by(season_id=season_id).all()
            if date_status.scraped_daily_dash_brooks == 0
        ]

    @classmethod
    def get_unscraped_pitch_appearances_for_date(cls, db_session, game_date):
        date_status = cls.find_by_date(db_session, game_date)
        return [
            pitch_app
            for pitch_app in date_status.scrape_status_pitchfx
            if pitch_app.scraped_pitchfx == 0
        ]

    @classmethod
    def get_unscraped_pitch_app_ids_for_date(cls, db_session, game_date):
        unscraped_pitch_apps = cls.get_unscraped_pitch_appearances_for_date(db_session, game_date)
        return [pitch_app.pitch_app_id for pitch_app in unscraped_pitch_apps]

    @classmethod
    def verify_bbref_daily_dashboard_scraped_for_date(cls, db_session, game_date):
        date_status = cls.find_by_date(db_session, game_date)
        return date_status.scraped_daily_dash_bbref == 1 if date_status else False

    @classmethod
    def verify_brooks_daily_dashboard_scraped_for_date(cls, db_session, game_date):
        date_status = cls.find_by_date(db_session, game_date)
        return date_status.scraped_daily_dash_brooks == 1 if date_status else False

    @classmethod
    def verify_all_bbref_boxscores_scraped_for_date(cls, db_session, game_date):
        date_status = cls.find_by_date(db_session, game_date)
        return date_status.scraped_all_bbref_boxscores if date_status else False

    @classmethod
    def verify_all_brooks_pitch_logs_scraped_for_date(cls, db_session, game_date):
        date_status = cls.find_by_date(db_session, game_date)
        return date_status.scraped_all_brooks_pitch_logs if date_status else False

    @classmethod
    def verify_all_brooks_pitchfx_scraped_for_date(cls, db_session, game_date):
        date_status = cls.find_by_date(db_session, game_date)
        return date_status.scraped_all_pitchfx_logs if date_status else False

    @classmethod
    def get_all_bbref_game_ids_for_date(cls, db_session, game_date):
        date_status = cls.find_by_date(db_session, game_date)
        if not date_status:
            return None
        return [game_status.bbref_game_id for game_status in date_status.scrape_status_games]

    @classmethod
    def get_all_brooks_game_ids_for_date(cls, db_session, game_date):
        date_status = cls.find_by_date(db_session, game_date)
        if not date_status:
            return None
        return [game_status.bb_game_id for game_status in date_status.scrape_status_games]

    @classmethod
    def get_date_id_map(cls, db_session):
        all_dates = db_session.query(cls).all()
        return {d.game_date: d.id for d in all_dates}


@accept_whitespaces
@dateformat(DATE_ONLY)
@dataclass
class DateScrapeStatusCsvRow:
    id: int
    game_date: datetime = None
    scraped_daily_dash_bbref: int = 0
    scraped_daily_dash_brooks: int = 0
    game_count_bbref: int = 0
    game_count_brooks: int = 0
