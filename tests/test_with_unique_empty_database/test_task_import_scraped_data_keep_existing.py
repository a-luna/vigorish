from datetime import datetime

import pytest

from tests.conftest import CSV_FOLDER, TESTS_FOLDER
from tests.util import (
    COMBINED_DATA_GAME_DICT,
    update_scraped_bbref_games_for_date,
    update_scraped_boxscore,
    update_scraped_brooks_games_for_date,
    update_scraped_pitch_logs,
    update_scraped_pitchfx_logs,
)
from vigorish.app import Vigorish
from vigorish.database import Season
from vigorish.enums import DataSet
from vigorish.tasks.combine_scraped_data import CombineScrapedDataTask
from vigorish.tasks.import_scraped_data import ImportScrapedDataTask

TEST_ID = "NO_ERRORS"
GAME_DICT = COMBINED_DATA_GAME_DICT[TEST_ID]
GAME_DATE = GAME_DICT["game_date"]
BBREF_GAME_ID = GAME_DICT["bbref_game_id"]
BB_GAME_ID = GAME_DICT["bb_game_id"]
APPLY_PATCH_LIST = GAME_DICT["apply_patch_list"]


@pytest.fixture()
def vig_app(request):
    """Returns an instance of the application configured to use the test DB and test config file."""
    app = Vigorish()
    app.initialize_database(csv_folder=CSV_FOLDER)
    assert app.db_setup_complete
    update_scraped_bbref_games_for_date(app, GAME_DATE)
    update_scraped_brooks_games_for_date(app, GAME_DATE)
    update_scraped_boxscore(app, BBREF_GAME_ID)
    update_scraped_pitch_logs(app, GAME_DATE, BBREF_GAME_ID)
    update_scraped_pitchfx_logs(app, BB_GAME_ID)
    CombineScrapedDataTask(app).execute(BBREF_GAME_ID, APPLY_PATCH_LIST)
    app.db_session.commit()

    def fin():
        app.db_session.close()
        for file in TESTS_FOLDER.glob("vig_*.db"):
            file.unlink()

    request.addfinalizer(fin)
    return app


def test_import_and_keep_existing(vig_app, mocker):
    def get_2018_2019_seasons(db_session):
        s2018 = Season.find_by_year(db_session, 2018)
        s2019 = Season.find_by_year(db_session, 2019)
        return [s2018, s2019]

    mocker.patch("vigorish.tasks.import_scraped_data.db.Season.get_all_regular_seasons", get_2018_2019_seasons)

    season_18 = Season.find_by_year(vig_app.db_session, 2018)
    season_19 = Season.find_by_year(vig_app.db_session, 2019)
    scraped_ids_2018_before = {}
    scraped_ids_2018_before[DataSet.BBREF_GAMES_FOR_DATE] = vig_app.scraped_data.get_scraped_ids_from_database(
        DataSet.BBREF_GAMES_FOR_DATE, season_18
    )
    scraped_ids_2018_before[DataSet.BROOKS_GAMES_FOR_DATE] = vig_app.scraped_data.get_scraped_ids_from_database(
        DataSet.BROOKS_GAMES_FOR_DATE, season_18
    )
    scraped_ids_2018_before[DataSet.BBREF_BOXSCORES] = vig_app.scraped_data.get_scraped_ids_from_database(
        DataSet.BBREF_BOXSCORES, season_18
    )
    scraped_ids_2018_before[DataSet.BROOKS_PITCH_LOGS] = vig_app.scraped_data.get_scraped_ids_from_database(
        DataSet.BROOKS_PITCH_LOGS, season_18
    )
    scraped_ids_2018_before[DataSet.BROOKS_PITCHFX] = vig_app.scraped_data.get_scraped_ids_from_database(
        DataSet.BROOKS_PITCHFX, season_18
    )
    assert not scraped_ids_2018_before[DataSet.BBREF_GAMES_FOR_DATE]
    assert not scraped_ids_2018_before[DataSet.BROOKS_GAMES_FOR_DATE]
    assert not scraped_ids_2018_before[DataSet.BBREF_BOXSCORES]
    assert not scraped_ids_2018_before[DataSet.BROOKS_PITCH_LOGS]
    assert not scraped_ids_2018_before[DataSet.BROOKS_PITCHFX]

    scraped_ids_2019_before = {}
    scraped_ids_2019_before[DataSet.BBREF_GAMES_FOR_DATE] = vig_app.scraped_data.get_scraped_ids_from_database(
        DataSet.BBREF_GAMES_FOR_DATE, season_19
    )
    scraped_ids_2019_before[DataSet.BROOKS_GAMES_FOR_DATE] = vig_app.scraped_data.get_scraped_ids_from_database(
        DataSet.BROOKS_GAMES_FOR_DATE, season_19
    )
    scraped_ids_2019_before[DataSet.BBREF_BOXSCORES] = vig_app.scraped_data.get_scraped_ids_from_database(
        DataSet.BBREF_BOXSCORES, season_19
    )
    scraped_ids_2019_before[DataSet.BROOKS_PITCH_LOGS] = vig_app.scraped_data.get_scraped_ids_from_database(
        DataSet.BROOKS_PITCH_LOGS, season_19
    )
    scraped_ids_2019_before[DataSet.BROOKS_PITCHFX] = vig_app.scraped_data.get_scraped_ids_from_database(
        DataSet.BROOKS_PITCHFX, season_19
    )
    assert scraped_ids_2019_before[DataSet.BBREF_GAMES_FOR_DATE] == [GAME_DATE]
    assert scraped_ids_2019_before[DataSet.BROOKS_GAMES_FOR_DATE] == [GAME_DATE]
    assert scraped_ids_2019_before[DataSet.BBREF_BOXSCORES] == [BBREF_GAME_ID]
    assert scraped_ids_2019_before[DataSet.BROOKS_PITCH_LOGS] == [BB_GAME_ID]
    assert all(
        pitch_app_id.startswith(BBREF_GAME_ID) for pitch_app_id in scraped_ids_2019_before[DataSet.BROOKS_PITCHFX]
    )

    import_task = ImportScrapedDataTask(vig_app)
    result = import_task.execute(overwrite_existing=True)
    assert result.success

    scraped_ids_2018_after = {}
    scraped_ids_2018_after[DataSet.BBREF_GAMES_FOR_DATE] = vig_app.scraped_data.get_scraped_ids_from_database(
        DataSet.BBREF_GAMES_FOR_DATE, season_18
    )
    scraped_ids_2018_after[DataSet.BROOKS_GAMES_FOR_DATE] = vig_app.scraped_data.get_scraped_ids_from_database(
        DataSet.BROOKS_GAMES_FOR_DATE, season_18
    )
    scraped_ids_2018_after[DataSet.BBREF_BOXSCORES] = vig_app.scraped_data.get_scraped_ids_from_database(
        DataSet.BBREF_BOXSCORES, season_18
    )
    scraped_ids_2018_after[DataSet.BROOKS_PITCH_LOGS] = vig_app.scraped_data.get_scraped_ids_from_database(
        DataSet.BROOKS_PITCH_LOGS, season_18
    )
    scraped_ids_2018_after[DataSet.BROOKS_PITCHFX] = vig_app.scraped_data.get_scraped_ids_from_database(
        DataSet.BROOKS_PITCHFX, season_18
    )
    assert not scraped_ids_2018_after[DataSet.BBREF_BOXSCORES]
    assert not scraped_ids_2018_after[DataSet.BROOKS_PITCHFX]
    assert scraped_ids_2018_after[DataSet.BBREF_GAMES_FOR_DATE] == [
        datetime(2018, 3, 29),
        datetime(2018, 4, 1),
        datetime(2018, 4, 17),
        datetime(2018, 6, 17),
    ]
    assert scraped_ids_2018_after[DataSet.BROOKS_GAMES_FOR_DATE] == [
        datetime(2018, 4, 1),
        datetime(2018, 6, 17),
    ]
    assert scraped_ids_2018_after[DataSet.BROOKS_PITCH_LOGS] == ["gid_2018_04_01_anamlb_oakmlb_1"]

    scraped_ids_2019_after = {}
    scraped_ids_2019_after[DataSet.BBREF_GAMES_FOR_DATE] = vig_app.scraped_data.get_scraped_ids_from_database(
        DataSet.BBREF_GAMES_FOR_DATE, season_19
    )
    scraped_ids_2019_after[DataSet.BROOKS_GAMES_FOR_DATE] = vig_app.scraped_data.get_scraped_ids_from_database(
        DataSet.BROOKS_GAMES_FOR_DATE, season_19
    )
    scraped_ids_2019_after[DataSet.BBREF_BOXSCORES] = vig_app.scraped_data.get_scraped_ids_from_database(
        DataSet.BBREF_BOXSCORES, season_19
    )
    scraped_ids_2019_after[DataSet.BROOKS_PITCH_LOGS] = vig_app.scraped_data.get_scraped_ids_from_database(
        DataSet.BROOKS_PITCH_LOGS, season_19
    )
    scraped_ids_2019_after[DataSet.BROOKS_PITCHFX] = vig_app.scraped_data.get_scraped_ids_from_database(
        DataSet.BROOKS_PITCHFX, season_19
    )
    assert len(scraped_ids_2019_after[DataSet.BBREF_GAMES_FOR_DATE]) == 8
    assert len(scraped_ids_2019_after[DataSet.BROOKS_GAMES_FOR_DATE]) == 8
    assert len(scraped_ids_2019_after[DataSet.BBREF_BOXSCORES]) == 9
    assert len(scraped_ids_2019_after[DataSet.BROOKS_PITCH_LOGS]) == 9
    assert len(scraped_ids_2019_after[DataSet.BROOKS_PITCHFX]) == 79
