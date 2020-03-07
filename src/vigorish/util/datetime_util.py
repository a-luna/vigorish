"""Helpful datetime converters and formatters."""
from datetime import timedelta


def get_date_range(start, end, inc=timedelta(days=1)):
    result = []
    current = start
    while current <= end:
        result.append(current)
        current += inc
    return result
