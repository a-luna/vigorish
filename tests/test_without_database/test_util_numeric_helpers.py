from datetime import date

import pytest

from vigorish.util.numeric_helpers import trim_data_set, validate_year_value


def test_validate_year_value():
    year = 2019
    year = validate_year_value(year)
    assert year == 2019

    with pytest.raises(ValueError):
        year = date.today().year + 1
        year = validate_year_value(year)

    with pytest.raises(ValueError):
        year = 1893
        year = validate_year_value(year)

    with pytest.raises(TypeError):
        year = True
        year = validate_year_value(year)

    with pytest.raises(ValueError):
        year = "the year two thousand and nineteen"
        year = validate_year_value(year)

    with pytest.raises(ValueError):
        year = False
        year = validate_year_value(year)


def test_trim_data_set():
    samples = [
        10,
        386,
        479,
        627,
        20,
        523,
        482,
        483,
        542,
        699,
        535,
        617,
        577,
        471,
        615,
        583,
        441,
        562,
        563,
        527,
        453,
        530,
        433,
        541,
        585,
        704,
        443,
        569,
        430,
        637,
        331,
        511,
        552,
        496,
        484,
        566,
        554,
        472,
        335,
        440,
        579,
        341,
        545,
        615,
        548,
        604,
        439,
        556,
        442,
        461,
        624,
        611,
        444,
        578,
        405,
        487,
        490,
        496,
        398,
        512,
        422,
        455,
        449,
        432,
        607,
        679,
        434,
        597,
        639,
        565,
        415,
        486,
        668,
        414,
        665,
        763,
        557,
        304,
        404,
        454,
        689,
        610,
        483,
        441,
        657,
        590,
        492,
        476,
        437,
        483,
        529,
        363,
        711,
        543,
    ]

    trimmed = trim_data_set(samples)
    assert len(samples) == 94
    assert len(trimmed) == 91
    assert 10 in samples
    assert 20 in samples
    assert 763 in samples
    assert 10 not in trimmed
    assert 20 not in trimmed
    assert 763 not in trimmed
