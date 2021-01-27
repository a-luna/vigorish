import pytest

from tests.util import seed_database_with_2019_test_data
from vigorish.tasks import CalculateAvgPitchTimesTask


@pytest.fixture(scope="module", autouse=True)
def create_test_data(vig_app):
    """Initialize DB with data to verify test functions in this module."""
    seed_database_with_2019_test_data(vig_app)
    return True


def test_task_calculate_avg_pitch_times(vig_app):
    calc_pitch_times = CalculateAvgPitchTimesTask(vig_app)
    result = calc_pitch_times.execute(trim_data_sets=False)
    assert result.success
    pitch_metrics = result.value
    assert pitch_metrics
    assert "time_between_pitches" in pitch_metrics
    time_between_pitches = pitch_metrics["time_between_pitches"]
    assert "total" in time_between_pitches
    assert time_between_pitches["total"] == 26862
    assert time_between_pitches["count"] == 927
    assert time_between_pitches["avg"] == 29.0
    assert time_between_pitches["max"] == 1924
    assert time_between_pitches["min"] == 9
    assert time_between_pitches["range"] == 1915

    assert "time_between_at_bats" in pitch_metrics
    time_between_at_bats = pitch_metrics["time_between_at_bats"]
    assert "total" in time_between_at_bats
    assert time_between_at_bats["total"] == 12817
    assert time_between_at_bats["count"] == 252
    assert time_between_at_bats["avg"] == 50.9
    assert time_between_at_bats["max"] == 204
    assert time_between_at_bats["min"] == 8
    assert time_between_at_bats["range"] == 196

    assert "time_between_innings" in pitch_metrics
    time_between_innings = pitch_metrics["time_between_innings"]
    assert "total" in time_between_innings
    assert time_between_innings["total"] == 10604
    assert time_between_innings["count"] == 67
    assert time_between_innings["avg"] == 158.3
    assert time_between_innings["max"] == 588
    assert time_between_innings["min"] == 125
    assert time_between_innings["range"] == 463
