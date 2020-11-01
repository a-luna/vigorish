"""Helper functions that handle and produce numeric values."""
import math
from datetime import date

from vigorish.util.string_helpers import try_parse_int

ONE_KB = 1024
ONE_MB = ONE_KB * 1024
ONE_GB = ONE_MB * 1024
ONE_PERCENT = 0.01


def is_even(x):
    return x & 1 == 0


def validate_year_value(year):
    if not year:
        error = 'No value provided for "year" parameter, unable to resolve folder path.'
        raise ValueError(error)
    if isinstance(year, str):
        year_str = year
        year = try_parse_int(year_str)
        if not year:
            raise ValueError(f'Failed to parse int value from string "{year_str}"')
    if isinstance(year, bool) or not isinstance(year, int):
        raise TypeError(f'"year" parameter must be int value (not "{type(year)}").')
    if year < 1900:
        raise ValueError(f"Data is not collected for year={year}")
    if year > date.today().year:
        raise ValueError(f'"{year}" is not valid since it is a future year')
    return year


def trim_data_set(samples, st_dev_limit=2):
    if not samples or not isinstance(samples, list):
        raise ValueError("Input list is either empty or invalid")
    mean = sum(samples) / len(samples)
    st_dev = get_standard_deviation(samples)
    return [
        x
        for x in samples
        if x > (mean - st_dev_limit * st_dev) and x < (mean + st_dev_limit * st_dev)
    ]


def get_standard_deviation(samples):
    if not samples or not isinstance(samples, list):
        raise ValueError("Input list is either empty or invalid")
    mean = sum(samples) / len(samples)
    summed_squares = sum(math.pow((sample - mean), 2) for sample in samples)
    return math.sqrt(summed_squares / (len(samples) - 1))
