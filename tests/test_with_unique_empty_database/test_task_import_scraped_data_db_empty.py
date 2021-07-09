from datetime import datetime

import pytest

from tests.conftest import CSV_FOLDER, DOTENV_FILE
from vigorish.app import Vigorish
from vigorish.database import Season
from vigorish.enums import DataSet
from vigorish.tasks.import_scraped_data import ImportScrapedDataTask


@pytest.fixture()
def vig_app(request):
    """Returns an instance of the application configured to use the test DB and test config file."""
    app = Vigorish()
    app.initialize_database(csv_folder=CSV_FOLDER)
    assert app.db_setup_complete

    def fin():
        app.db_session.close()
        if DOTENV_FILE.exists():
            DOTENV_FILE.unlink()

    request.addfinalizer(fin)
    return app


def test_import_with_empty_database(vig_app, mocker):
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
    assert not scraped_ids_2019_before[DataSet.BBREF_GAMES_FOR_DATE]
    assert not scraped_ids_2019_before[DataSet.BROOKS_GAMES_FOR_DATE]
    assert not scraped_ids_2019_before[DataSet.BBREF_BOXSCORES]
    assert not scraped_ids_2019_before[DataSet.BROOKS_PITCH_LOGS]
    assert not scraped_ids_2019_before[DataSet.BROOKS_PITCHFX]

    import_task = ImportScrapedDataTask(vig_app)
    result = import_task.execute()
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
