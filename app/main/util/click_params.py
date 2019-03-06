from dateutil import parser

import click

from app.main.models.season import Season

class DateString(click.ParamType):
    name = 'date-string'
    def convert(self, value, param, ctx):
        try:
            date = parser.parse(value)
            return date
        except Exception:
            error = (
                f'"{value}" could not be parsed as a valid date. You can '
                'use any format recognized by dateutil.parser. For example, '
                'all of the strings below are valid ways to represent the '
                'same date\n'
                '"2018-5-13" -or- "05/13/2018" -or- "May 13 2018"'
            )
            self.fail(error, param, ctx)

class MlbSeason(click.ParamType):
    name = 'number-year'
    def convert(self, value, param, ctx):
        session = ctx.obj['session']
        try:
            year = int(value)
        except Exception:
            error = (
                f'Unable to parse "{value}" as an integer.'
            )
            self.fail(error, param, ctx)

        try:
            season = Season.find_by_year(session, year)
            if season:
                return season.year
            valid_years = [
                s.year
                for s
                in Season.all_regular_seasons(session)
            ]
            year_min = min(sorted(valid_years))
            year_max = max(sorted(valid_years))
            error = (
                f'"{value}" is not a valid value. Only MLB Seasons in '
                f'the range {year_min}-{year_max} are supported in this '
                'version of vig.'
            )
            self.fail(error, param, ctx)
        except Exception:
            self.fail(error, param, ctx)