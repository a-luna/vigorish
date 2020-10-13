from datetime import datetime
from pathlib import Path

import pytest
from click.testing import CliRunner

from tests.util import seed_database_with_2019_test_data
from vigorish.cli.vig import cli
from vigorish.util.dt_format_strings import DATE_ONLY_2


@pytest.fixture(scope="module", autouse=True)
def create_test_data(db_session, scraped_data):
    """Initialize DB with data to verify test functions in test_cli module."""
    seed_database_with_2019_test_data(db_session, scraped_data)
    return True


@pytest.fixture(autouse=True)
def mock_env_vars(mocker):
    tests_folder = Path(__file__).parent
    test_vars = {
        "ENV": "TEST",
        "DOTENV_FILE": str(tests_folder.joinpath(".env")),
    }
    mocker.patch.dict("os.environ", test_vars)
    return True


def test_status_single_date_without_games_without_missing_pfx():
    game_date = datetime(2019, 6, 17).strftime(DATE_ONLY_2)
    runner = CliRunner()
    result = runner.invoke(cli, ["status", "date", game_date])
    assert result.exit_code == 0
    assert "### OVERALL STATUS FOR Jun 17 2019 ###" in result.output
    assert "### MISSING PITCHFX LOGS FOR Jun 17 2019 ###" not in result.output
    assert "### STATUS FOR EACH GAME PLAYED Jun 17 2019 ###" not in result.output


def test_status_single_date_without_games_with_missing_pfx():
    game_date = datetime(2019, 6, 17).strftime(DATE_ONLY_2)
    runner = CliRunner()
    result = runner.invoke(cli, ["status", "date", "--missing-ids", game_date])
    assert result.exit_code == 0
    assert "### OVERALL STATUS FOR Jun 17 2019 ###" in result.output
    assert "### MISSING PITCHFX LOGS FOR Jun 17 2019 ###" in result.output
    assert "### STATUS FOR EACH GAME PLAYED Jun 17 2019 ###" not in result.output


def test_status_single_date_with_games_with_missing_pfx():
    game_date = datetime(2019, 6, 17).strftime(DATE_ONLY_2)
    runner = CliRunner()
    result = runner.invoke(cli, ["status", "date", "--missing-ids", "--with-games", game_date])
    assert result.exit_code == 0
    assert "### OVERALL STATUS FOR Jun 17 2019 ###" in result.output
    assert "### MISSING PITCHFX LOGS FOR Jun 17 2019 ###" in result.output
    assert "### STATUS FOR EACH GAME PLAYED Jun 17 2019 ###" in result.output


def test_status_season_overall_summary():
    year = 2019
    runner = CliRunner()
    result = runner.invoke(cli, ["status", "season", "-v", year])
    assert result.exit_code == 0
    assert "### STATUS REPORT FOR MLB 2019 Regular Season ###" in result.output
    assert "BBref Daily Dash Scraped.....................: 6/186 days (3%)" in result.output
    assert "Brooks Daily Dash Scraped....................: 6/186 days (3%)" in result.output
    assert "BBref Boxscores Scraped......................: NO 6/81" in result.output
    assert "Brooks Games Scraped.........................: NO 6/81" in result.output
    assert "PitchFx Logs Scraped.........................: NO 56/56 (100%)" in result.output
    assert "Combined BBRef/PitchFX Data (Success/Total)..: NO 0/0" in result.output
    assert "Pitch App Count (BBRef/Brooks)...............: 56/715" in result.output
    assert "Pitch App Count (PFx/data/no data)...........: 56/54/2" in result.output
    assert "PitchFX Data Errors (Valid AB/Invalid AB)....: NO 0/0" in result.output
    assert "Pitch Count (BBRef/Brooks/PFx)...............: 1899/1875/1875" in result.output
    assert "Pitch Count Audited (BBRef/PFx/Removed)......: 0/0/0" in result.output
