import click
from dateutil import parser as date_parser

import vigorish.database as db
from vigorish.enums import DataSet, VigFile
from vigorish.util.regex import JOB_NAME_REGEX


class DateString(click.ParamType):
    name = "date-string"

    def convert(self, value, param, ctx):
        try:
            return date_parser.parse(value)
        except Exception:
            error = (
                f'"{value}" could not be parsed as a valid date. You can '
                "use any format recognized by dateutil.parser. For example, "
                "all of the strings below are valid ways to represent the "
                "same date\n"
                '"2018-5-13" -or- "05/13/2018" -or- "May 13 2018"'
            )
            self.fail(error, param, ctx)


class DataSetName(click.ParamType):
    name = "data-set"

    def convert(self, value, param, ctx):
        if value is None:
            return DataSet.NONE
        if value.upper() == "ALL":
            return DataSet.ALL
        if param.multiple:
            value_list = [s.strip() for s in value.split(",")]
            data_set_list = [(DataSet.from_str(val), val) for val in value_list]
            bad_values = list(filter(lambda x: x[0] == DataSet.NONE, data_set_list))
            if bad_values:
                bad_val_plural = "values" if len(bad_values) > 1 else "value"
                all_val_plural = "values" if len(value_list) > 1 else "value"
                is_plural = "are not valid data set names" if len(bad_values) > 1 else "is not a valid data set name"
                error = (
                    f"{len(bad_values)} {bad_val_plural} (of {len(value_list)} total {all_val_plural} "
                    f'provided) {is_plural}: {", ".join(v[1] for v in bad_values)}\n'
                    f'Valid data set names are: {", ".join(ds.name for ds in DataSet)}, ALL'
                )
                self.fail(error, param, ctx)
            return [ds[0] for ds in data_set_list]
        else:
            data_set = DataSet.from_str(value)
            if data_set:
                return data_set
            error = (
                f'"{value}" is not a valid name for a data set. Valid names are: '
                f'{", ".join(ds.name for ds in DataSet)}, ALL'
            )
            self.fail(error, param, ctx)


class FileTypeName(click.ParamType):
    name = "file-type"

    def convert(self, value, param, ctx):
        if value is None:
            return VigFile.NONE
        if value.upper() == "ALL":
            return VigFile.ALL
        if param.multiple:
            value_list = [s.strip() for s in value.split(",")]
            file_type_list = [(VigFile.from_str(val), val) for val in value_list]
            bad_values = list(filter(lambda x: x[0] == VigFile.NONE, file_type_list))
            if bad_values:
                bad_val_plural = "values" if len(bad_values) > 1 else "value"
                all_val_plural = "values" if len(value_list) > 1 else "value"
                is_plural = "are not valid file type names" if len(bad_values) > 1 else "is not a valid file type name"
                error = (
                    f"{len(bad_values)} {bad_val_plural} (of {len(value_list)} total {all_val_plural} "
                    f'provided) {is_plural}: {", ".join(v[1] for v in bad_values)}\n'
                    f'Valid file type names are: {", ".join(f.name for f in VigFile)}, ALL'
                )
                self.fail(error, param, ctx)
            return [f[0] for f in file_type_list]
        else:
            file_type = VigFile.from_str(value)
            if file_type:
                return file_type
            error = (
                f'"{value}" is not a valid file type name. Valid names are: '
                f'{", ".join(f.name for f in VigFile)}, ALL'
            )
            self.fail(error, param, ctx)


class MlbSeason(click.ParamType):
    name = "year-number"

    def convert(self, value, param, ctx):
        db_session = ctx.obj.db_session
        try:
            year = int(value)
        except Exception:
            error = f'Unable to parse "{value}" as an integer.'
            self.fail(error, param, ctx)

        try:
            season = db.Season.find_by_year(db_session, year)
            if season:
                return season.year
            valid_years = [s.year for s in db.Season.get_all_regular_seasons(db_session)]
            year_min = min(sorted(valid_years))
            year_max = max(sorted(valid_years))
            error = (
                f'"{value}" is not a valid value. Only MLB Seasons in '
                f"the range {year_min}-{year_max} are supported in this "
                "version of vig."
            )
            self.fail(error, param, ctx)
        except Exception as ex:
            self.fail(repr(ex), param, ctx)


class JobName(click.ParamType):
    name = "job-name"

    def convert(self, value, param, ctx):
        if not value:
            return value
        if JOB_NAME_REGEX.match(value):
            return value
        error = (
            f"'{value}' contains one or more invalid characters. Job name must "
            "contain only letters, numbers, hyphen and underscore characters."
        )
        self.fail(error, param, ctx)
