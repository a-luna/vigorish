from datetime import datetime

import pytest

from vigorish.cli.menu_items.admin_tasks.calculate_avg_pitch_times import (
    CalculatePitchTimes,
)
from vigorish.util.string_helpers import ellipsize, parse_date, wrap_text


def test_ellipsize():
    max_len = 70
    s1 = "This is a string that does not need to be ellipsized"
    assert ellipsize(s1, max_len) == s1
    s2 = "This string is slightly longer than 70 chars, and therefore, must be ellipsized"
    assert (
        ellipsize(s2, max_len)
        == "This string is slightly longer than 70 chars, and therefore, must be …"
    )


def test_wrap_text(vig_app):
    calc_pitch_times = CalculatePitchTimes(vig_app)
    task_description_pages = calc_pitch_times.get_task_description_pages()
    wrapped_pages = []
    for page in task_description_pages:
        wrapped_lines = [wrap_text(line, max_len=70) for line in page]
        wrapped_pages.append("\n".join(wrapped_lines))

    assert (
        wrapped_pages[0] == "This task first identifies games where all data has been scraped and\n"
        "the process of reconciling and combining the scraped data was entirely\n"
        "successful with zero data errors of any type (e.g., zero missing/extra\n"
        "pitches, no invalid PitchFX data).\n"
        "\n"
        "Next, each game is stepped through pitch-by-pitch and the length of\n"
        "time between each pitch is calculated and stored using the timestamps\n"
        "available on PitchFX data."
    )

    assert (
        wrapped_pages[1] == "There are three distinct types of between-pitch durations which are\n"
        "collected:\n"
        "\n"
        "1. (between pitches)\n"
        "time between pitches in the same at bat\n"
        "\n"
        "2. (between at bats)\n"
        "time between the last pitch of an at bat and the first pitch of the\n"
        "next at bat in the same inning"
    )

    assert (
        wrapped_pages[2] == "3. (between innings)\n"
        "time between the last pitch of the last at bat in an inning and the\n"
        "first pitch of the next at bat in the subsequent inning\n"
        "\n"
        "Finally, the data sets are processed to remove outliers before\n"
        "calculating the average, maximum and minimum durations for each\n"
        "category."
    )


def test_parse_date():
    date_str = "20190521"
    check_date = datetime(2019, 5, 21)
    assert check_date == parse_date(date_str)

    with pytest.raises(ValueError):
        parse_date(None)

    with pytest.raises(ValueError):
        parse_date("2019-05-21")

    with pytest.raises(ValueError):
        parse_date("20190231")
