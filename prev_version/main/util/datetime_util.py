"""Helpful datetime converters and formatters."""
from datetime import datetime, timezone, timedelta, date
from dateutil import tz
from dateutil.relativedelta import relativedelta
from email.utils import parsedate_to_datetime, format_datetime

from tzlocal import get_localzone

from app.main.util.check_functions import checkattr
from app.main.util.dt_format_strings import (
    DT_STR_FORMAT,
    DT_STR_FORMAT_NAIVE,
    DT_FORMAT_ISO,
    DT_FORMAT_SHORT,
    DT_FORMAT_XML,
    DATE_ONLY_2,
)

TIME_ZONE_NEW_YORK = tz.gettz("America/New_York")


def convert_dt_for_display(obj, attr_name, user_tz=None, str_format=DT_STR_FORMAT):
    """Convert a datetime value to formatted string.

    If 'obj' has an attribute 'attr_name' or if 'obj' is a dict and contains
    the key 'attr_name', the datetime value of 'attr_name' will be converted
    to a formatted string using 'str_format'. The default value for
    'str_format' will produce a string like: '01 Jan 2000 00:00 AM PST'.

    If 'attr_name' does not exist or if the value is not a datetime object,
    an empty string is returned.
    """
    result = checkattr(obj, attr_name)
    if result.failure:
        return result
    try:
        s = convert_dt_to_user_tz_str(
            result.value, user_tz=user_tz, str_format=str_format
        )
        return s
    except ValueError:
        return ""
    except OverflowError:
        return ""


def convert_dt_to_user_tz(dt, user_tz=None, str_format=DT_STR_FORMAT):
    if not user_tz:
        user_tz = get_localzone()
    try:
        return dt.replace(tzinfo=timezone.utc).astimezone(user_tz)
    except ValueError:
        return ""
    except OverflowError:
        return ""


def convert_dt_to_user_tz_str(dt, user_tz=None, str_format=DT_STR_FORMAT):
    return convert_dt_to_user_tz(dt, user_tz, str_format).strftime(str_format)


def get_dt_iso_format_utc(obj, attr_name):
    result = checkattr(obj, attr_name)
    if result.failure:
        return result
    try:
        dt = result.value
        return dt.replace(tzinfo=timezone.utc).strftime(DT_FORMAT_ISO)
    except ValueError:
        return ""
    except OverflowError:
        return ""


def format_timedelta_digits(td):
    td_days = None
    td_str = str(td)
    if "day" in td_str:
        splat = td_str.split(",")
        td_days = splat[0]
        td_str = splat[1].strip()
    split = td_str.split(":")
    splot = split[2].split(".")
    td_hours = int(split[0])
    td_min = int(split[1])
    td_sec = int(splot[0])

    if td_days:
        return "{d}, {h:02d}:{m:02d}:{s:02d}".format(
            d=td_days, h=td_hours, m=td_min, s=td_sec
        )
    if td_hours > 0:
        return "{h:02d}:{m:02d}:{s:02d}".format(h=td_hours, m=td_min, s=td_sec)
    return "{m:02d}:{s:02d}".format(m=td_min, s=td_sec)


def get_date_range(start, end, inc=timedelta(days=1)):
    result = []
    current = start
    while current <= end:
        result.append(current)
        current += inc
    return result


def format_timedelta_str(td):
    (milliseconds, microseconds) = divmod(td.microseconds, 1000)
    (minutes, seconds) = divmod(td.seconds, 60)
    (hours, minutes) = divmod(minutes, 60)
    if td.days > 0:
        return f"{td.days}d {hours:.0f}h {minutes:.0f}m {seconds}s"
    if hours > 0:
        return f"{hours:.0f}h {minutes:.0f}m {seconds}s"
    if minutes > 0:
        return f"{minutes:.0f}m {seconds}s"
    if td.seconds > 0:
        return f"{td.seconds}s {milliseconds:.0f}ms"
    if milliseconds > 0:
        return f"{milliseconds}ms"
    return f"{td.microseconds}us"


def today_str():
    return date.today().strftime(DATE_ONLY_2)


def current_year():
    return datetime.now().year
