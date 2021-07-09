from vigorish.tasks import CalculateAvgPitchTimesTask


def test_task_calculate_avg_pitch_times(vig_app):
    calc_pitch_times = CalculateAvgPitchTimesTask(vig_app)
    result = calc_pitch_times.execute(trim_data_sets=False)
    assert result.success
    pitch_metrics = result.value
    assert pitch_metrics
    assert "time_between_pitches" in pitch_metrics
    assert pitch_metrics["time_between_pitches"] == {
        "total": 32058,
        "count": 1342,
        "avg": 23.9,
        "max": 172,
        "min": 10,
        "range": 162,
        "trim": False,
    }

    assert "time_between_at_bats" in pitch_metrics
    assert pitch_metrics["time_between_at_bats"] == {
        "total": 20236,
        "count": 369,
        "avg": 54.8,
        "max": 217,
        "min": 27,
        "range": 190,
        "trim": False,
    }

    assert "time_between_innings" in pitch_metrics
    assert pitch_metrics["time_between_innings"] == {
        "total": 16045,
        "count": 100,
        "avg": 160.4,
        "max": 355,
        "min": 128,
        "range": 227,
        "trim": False,
    }
