import click
from dateutil import parser as date_parser

from vigorish.config.database import Season
from vigorish.util.regex import JOB_NAME_REGEX


class DateString(click.ParamType):
    name = "date-string"

    def convert(self, value, param, ctx):
        try:
            date = date_parser.parse(value)
            return date
        except Exception:
            error = (
                f'"{value}" could not be parsed as a valid date. You can '
                "use any format recognized by dateutil.parser. For example, "
                "all of the strings below are valid ways to represent the "
                "same date\n"
                '"2018-5-13" -or- "05/13/2018" -or- "May 13 2018"'
            )
            self.fail(error, param, ctx)


class MlbSeason(click.ParamType):
    name = "year-number"

    def convert(self, value, param, ctx):
        db_session = ctx.obj["db_session"]
        try:
            year = int(value)
        except Exception:
            error = f'Unable to parse "{value}" as an integer.'
            self.fail(error, param, ctx)

        try:
            season = Season.find_by_year(db_session, year)
            if season:
                return season.year
            valid_years = [s.year for s in Season.all_regular_seasons(db_session)]
            year_min = min(sorted(valid_years))
            year_max = max(sorted(valid_years))
            error = (
                f'"{value}" is not a valid value. Only MLB Seasons in '
                f"the range {year_min}-{year_max} are supported in this "
                "version of vig."
            )
            self.fail(error, param, ctx)
        except Exception:
            self.fail(error, param, ctx)


class JobName(click.ParamType):
    name = "job-name"

    def convert(self, value, param, ctx):
        if JOB_NAME_REGEX.match(value):
            return value
        error = (
            f"'{value}' contains one or more invalid characters. Job name must "
            "contain only letters, numbers, hyphen and underscore characters."
        )
        self.fail(error, param, ctx)
