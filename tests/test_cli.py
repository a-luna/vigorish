from datetime import datetime
from pathlib import Path

import pytest
from click.testing import CliRunner

from vigorish.cli.vig import cli
from vigorish.util.dt_format_strings import DATE_ONLY_2


@pytest.fixture(autouse=True)
def _mock_env_vars(mocker):
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
