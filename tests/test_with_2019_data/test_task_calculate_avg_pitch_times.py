from vigorish.tasks import CalculateAvgPitchTimesTask


def test_task_calculate_avg_pitch_times(vig_app):
    calc_pitch_times = CalculateAvgPitchTimesTask(vig_app)
    result = calc_pitch_times.execute(trim_data_sets=False)
    assert result.success
    pitch_metrics = result.value
    assert pitch_metrics
    assert "time_between_pitches" in pitch_metrics
    assert pitch_metrics["time_between_pitches"] == {
        "avg": 30.9,
        "count": 1077,
        "max": 1924,
        "min": 9,
        "range": 1915,
        "total": 33287,
        "trim": False,
    }

    assert "time_between_at_bats" in pitch_metrics
    assert pitch_metrics["time_between_at_bats"] == {
        "avg": 54.2,
        "count": 295,
        "max": 652,
        "min": 8,
        "range": 644,
        "total": 15989,
        "trim": False,
    }

    assert "time_between_innings" in pitch_metrics
    assert pitch_metrics["time_between_innings"] == {
        "avg": 156.0,
        "count": 83,
        "max": 588,
        "min": 125,
        "range": 463,
        "total": 12945,
        "trim": False,
    }
