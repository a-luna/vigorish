"""Helper functions that handle and produce numeric values."""
import math
from datetime import date
from typing import Union
from vigorish.util.string_helpers import try_parse_int


def is_nan(x):
    return isinstance(x, float) and math.isnan(x)


def sanitize(x):
    if math.isnan(x):
        return 0
    return x


def validate_year_value(year: Union[int, str]) -> bool:
    if not year:
        error = 'No value provided for "year" parameter, unable to resolve folder path.'
        raise ValueError(error)
    if isinstance(year, str):
        year_str = year
        year = try_parse_int(year_str)
        if not year:
            raise ValueError(f'Failed to parse int value from string "{year_str}"')
    if not isinstance(year, int):
        raise TypeError(f'"year" parameter must be int value (not "{type(year)}").')
    if year < 1900:
        raise ValueError(f"Data is not collected for year={year}")
    if year > date.today().year:
        raise ValueError(f'"{year}" is not valid since it is a future year')
