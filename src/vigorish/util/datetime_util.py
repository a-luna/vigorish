"""Helpful datetime converters and formatters."""
from datetime import datetime, timedelta, timezone

from dateutil import tz

from vigorish.util.dt_format_strings import DATE_ONLY_2

TIME_ZONE_NEW_YORK = tz.gettz("America/New_York")


def get_date_range(start, end, inc=timedelta(days=1)):
    if start > end:
        start_str = start.strftime(DATE_ONLY_2)
        end_str = end.strftime(DATE_ONLY_2)
        raise ValueError(f"Start date ({start_str}) must be earlier than end date ({end_date})")

    result = []
    current = start
    while current <= end:
        result.append(current)
        current += inc
    return result


def utc_now():
    """Current UTC date and time with the microsecond value normalized to zero."""
    return datetime.now(timezone.utc).replace(microsecond=0)
