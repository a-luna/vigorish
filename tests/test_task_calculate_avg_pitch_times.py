from vigorish.status.update_status_combined_data import update_pitch_apps_for_game_combined_data
from vigorish.tasks.calculate_avg_pitch_times import CalculateAverageTimeBetweenPitches

from tests.test_combine_scraped_data import (
    combine_scraped_data_for_game,
    GAME_ID_NO_ERRORS,
    GAME_ID_PATCH_PFX,
)
from tests.util import revert_pitch_logs_to_state_before_combined_data


def test_task_calculate_avg_pitch_times(vig_app):
    db_session = vig_app["db_session"]
    scraped_data = vig_app["scraped_data"]
    combined_data_1 = combine_scraped_data_for_game(db_session, scraped_data, GAME_ID_NO_ERRORS)
    assert combined_data_1
    result = update_pitch_apps_for_game_combined_data(db_session, combined_data_1)
    assert result.success
    combined_data_2 = combine_scraped_data_for_game(
        db_session, scraped_data, GAME_ID_PATCH_PFX, apply_patch_list=True
    )
    assert combined_data_2
    result = update_pitch_apps_for_game_combined_data(db_session, combined_data_2)
    assert result.success
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

    result = revert_pitch_logs_to_state_before_combined_data(
        db_session, scraped_data, GAME_ID_NO_ERRORS
    )
    assert result.success
    result = revert_pitch_logs_to_state_before_combined_data(
        db_session, scraped_data, GAME_ID_PATCH_PFX
    )
    assert result.success
