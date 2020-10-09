from vigorish.tasks.calculate_avg_pitch_times import CalculateAverageTimeBetweenPitches


def test_task_calculate_avg_pitch_times(vig_app):
    calc_pitch_times = CalculateAverageTimeBetweenPitches(vig_app)
    result = calc_pitch_times.execute(trim_data_sets=False)
    assert result.success
    pitch_metrics = result.value
    assert pitch_metrics
    assert "time_between_pitches" in pitch_metrics
    time_between_pitches = pitch_metrics["time_between_pitches"]
    assert "total" in time_between_pitches
    assert time_between_pitches["total"] == 12936
    assert time_between_pitches["count"] == 467
    assert time_between_pitches["avg"] == 27.7
    assert time_between_pitches["max"] == 1427
    assert time_between_pitches["min"] == 9
    assert time_between_pitches["range"] == 1418

    assert "time_between_at_bats" in pitch_metrics
    time_between_at_bats = pitch_metrics["time_between_at_bats"]
    assert "total" in time_between_at_bats
    assert time_between_at_bats["total"] == 6460
    assert time_between_at_bats["count"] == 123
    assert time_between_at_bats["avg"] == 52.5
    assert time_between_at_bats["max"] == 204
    assert time_between_at_bats["min"] == 25
    assert time_between_at_bats["range"] == 179

    assert "time_between_innings" in pitch_metrics
    time_between_innings = pitch_metrics["time_between_innings"]
    assert "total" in time_between_innings
    assert time_between_innings["total"] == 5188
    assert time_between_innings["count"] == 34
    assert time_between_innings["avg"] == 152.6
    assert time_between_innings["max"] == 240
    assert time_between_innings["min"] == 126
    assert time_between_innings["range"] == 114
