from datetime import datetime, timezone

import pytest
from dateutil import tz

from vigorish.util.datetime_util import (
    current_year,
    dtaware_fromtimestamp,
    get_date_range,
    localized_dt_string,
    make_tzaware,
    today_str,
    utc_now,
)
from vigorish.util.dt_format_strings import DATE_ONLY_2, DT_AWARE, DT_NAIVE


def test_date_range_raises_valueerror():
    dt1 = datetime(2019, 1, 1)
    dt2 = datetime(2019, 12, 31)
    with pytest.raises(ValueError):
        date_range = get_date_range(dt2, dt1)
        assert date_range


def test_utc_now():
    dt_now = utc_now()
    assert dt_now.tzinfo == timezone.utc


def test_localized_dt_string():
    dt_naive = datetime(2019, 12, 31)
    dt_aware = dt_naive.replace(tzinfo=timezone.utc)
    timezone_est = tz.gettz("America/New_York")
    str_naive = localized_dt_string(dt_naive)
    str_naive_est = localized_dt_string(dt_naive, use_tz=timezone_est)
    str_aware = localized_dt_string(dt_aware)
    str_aware_est = localized_dt_string(dt_aware, use_tz=timezone_est)
    assert str_naive == "12/31/2019 12:00:00 AM"
    assert str_naive_est == "12/31/2019 12:00:00 AM -0500"
    assert str_aware == "12/31/2019 12:00:00 AM +0000"
    assert str_aware_est == "12/30/2019 07:00:00 PM -0500"


def test_make_tzaware():
    dt_naive = datetime(2019, 12, 31)
    dt_aware_1 = make_tzaware(dt_naive, use_tz=timezone.utc, localize=False)
    dt_aware_2 = make_tzaware(dt_naive, use_tz=timezone.utc, localize=True)
    dt_aware_3 = make_tzaware(dt_naive, localize=False)
    assert dt_naive.strftime(DT_NAIVE) == "12/31/2019 12:00:00 AM"
    assert dt_aware_1.strftime(DT_AWARE) == "12/31/2019 12:00:00 AM +0000"
    assert dt_aware_2.tzinfo == timezone.utc
    assert dt_aware_3.year == 2019
    assert dt_aware_3.month == 12
    assert dt_aware_3.day == 31
    assert dt_aware_3.hour == 0
    assert dt_aware_3.minute == 0
    assert dt_aware_3.second == 0


def test_dtaware_fromtimestamp():
    dt_naive = datetime(2019, 12, 31)
    dt_aware = dtaware_fromtimestamp(dt_naive.timestamp())
    assert dt_aware.year == 2019
    assert dt_aware.month == 12
    assert dt_aware.day == 31
    assert dt_aware.hour == 0
    assert dt_aware.minute == 0
    assert dt_aware.second == 0


def test_current_day_and_year():
    dt = datetime.now()
    assert dt.date().strftime(DATE_ONLY_2) == today_str()
    assert dt.year == current_year()
