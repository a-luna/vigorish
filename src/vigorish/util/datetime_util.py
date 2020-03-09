"""Helpful datetime converters and formatters."""
from datetime import datetime, timedelta, timezone


def get_date_range(start, end, inc=timedelta(days=1)):
    result = []
    current = start
    while current <= end:
        result.append(current)
        current += inc
    return result


def utc_now():
    """Current UTC date and time with the microsecond value normalized to zero."""
    return datetime.now(timezone.utc).replace(microsecond=0)
