"""Helpful datetime converters and formatters."""
import time
from datetime import datetime, timedelta, timezone, date

from dateutil import tz

from vigorish.util.dt_format_strings import DATE_ONLY_2, DT_AWARE, DT_NAIVE

TIME_ZONE_NEW_YORK = tz.gettz("America/New_York")


def get_date_range(start, end, inc=timedelta(days=1)):
    if start > end:
        start_str = start.strftime(DATE_ONLY_2)
        end_str = end.strftime(DATE_ONLY_2)
        raise ValueError(f"Start date ({start_str}) must be earlier than end date ({end_str})")

    result = []
    current = start
    while current <= end:
        result.append(current)
        current += inc
    return result


def utc_now():
    """Current UTC date and time with the microsecond value normalized to zero."""
    return datetime.now(timezone.utc).replace(microsecond=0)


def localized_dt_string(dt, use_tz=None):
    """Convert datetime value to a string, localized for the specified timezone."""
    if not dt.tzinfo and not use_tz:
        return dt.strftime(DT_NAIVE)
    if not dt.tzinfo:
        return dt.replace(tzinfo=use_tz).strftime(DT_AWARE)
    return dt.astimezone(use_tz).strftime(DT_AWARE) if use_tz else dt.strftime(DT_AWARE)


def make_tzaware(dt, use_tz=None, localize=True):
    """Make a naive datetime object timezone-aware."""
    if not use_tz:
        use_tz = get_local_utcoffset()
    return dt.astimezone(use_tz) if localize else dt.replace(tzinfo=use_tz)


def get_local_utcoffset():
    """Get UTC offset from local system and return as timezone object."""
    utc_offset = timedelta(seconds=time.localtime().tm_gmtoff)
    return timezone(offset=utc_offset)


def dtaware_fromtimestamp(timestamp, use_tz=None):
    """Time-zone aware datetime object from UNIX timestamp."""
    timestamp_naive = datetime.fromtimestamp(timestamp)
    timestamp_aware = timestamp_naive.replace(tzinfo=get_local_utcoffset())
    return timestamp_aware.astimezone(use_tz) if use_tz else timestamp_aware


def today_str():
    return date.today().strftime(DATE_ONLY_2)


def current_year():
    return datetime.now().year
