from datetime import datetime

import pytest
from click.testing import CliRunner

from tests.util import seed_database_with_2019_test_data
from vigorish.cli.vig import cli
from vigorish.util.dt_format_strings import DATE_ONLY_2


@pytest.fixture(scope="module", autouse=True)
def create_test_data(vig_app):
    """Initialize DB with data to verify test functions in test_cli module."""
    seed_database_with_2019_test_data(vig_app)
    return True


def test_status_single_date_without_games_without_missing_pfx():
    game_date = datetime(2019, 6, 17).strftime(DATE_ONLY_2)
    runner = CliRunner()
    result = runner.invoke(cli, f"status date {game_date}")
    assert result.exit_code == 0
    assert "### OVERALL STATUS FOR Jun 17 2019 ###" in result.output
    assert "### MISSING PITCHFX LOGS FOR Jun 17 2019 ###" not in result.output
    assert "### STATUS FOR EACH GAME PLAYED Jun 17 2019 ###" not in result.output


def test_status_single_date_without_games_with_missing_pfx():
    game_date = datetime(2019, 6, 17).strftime(DATE_ONLY_2)
    runner = CliRunner()
    result = runner.invoke(cli, f"status date {game_date} --missing-ids")
    assert result.exit_code == 0
    assert "### OVERALL STATUS FOR Jun 17 2019 ###" in result.output
    assert "### MISSING PITCHFX LOGS FOR Jun 17 2019 ###" in result.output
    assert "### STATUS FOR EACH GAME PLAYED Jun 17 2019 ###" not in result.output


def test_status_single_date_with_games_with_missing_pfx():
    game_date = datetime(2019, 6, 17).strftime(DATE_ONLY_2)
    runner = CliRunner()
    result = runner.invoke(cli, f"status date {game_date} --missing-ids --with-games")
    assert result.exit_code == 0
    assert "### OVERALL STATUS FOR Jun 17 2019 ###" in result.output
    assert "### MISSING PITCHFX LOGS FOR Jun 17 2019 ###" in result.output
    assert "### STATUS FOR EACH GAME PLAYED Jun 17 2019 ###" in result.output


def test_status_season_overall_summary():
    year = 2019
    runner = CliRunner()
    result = runner.invoke(cli, f"status season {year} -v")
    assert result.exit_code == 0
    assert "### STATUS REPORT FOR MLB 2019 Regular Season ###" in result.output
    assert "BBref Daily Dash Scraped.....................: 6/186 days (3%)" in result.output
    assert "Brooks Daily Dash Scraped....................: 6/186 days (3%)" in result.output
    assert "BBref Boxscores Scraped......................: NO 7/81" in result.output
    assert "Brooks Games Scraped.........................: NO 7/81" in result.output
    assert "PitchFx Logs Scraped.........................: NO 69/69 (100%)" in result.output
    assert "Combined BBRef/PitchFX Data (Success/Total)..: NO 7/7" in result.output
    assert "Pitch App Count (BBRef/Brooks)...............: 69/715" in result.output
    assert "Pitch App Count (PFx/data/no data)...........: 69/68/1" in result.output
    assert "PitchFX Data Errors (Valid AB/Invalid AB)....: NO 0/3" in result.output
    assert "Pitch Count (BBRef/Brooks/PFx)...............: 2243/2254/2254" in result.output
    assert "Pitch Count Audited (BBRef/PFx/Removed)......: 2243/2173/62" in result.output


def test_status_season_date_range_summary_only_missing():
    year = 2019
    runner = CliRunner()
    result = runner.invoke(cli, f"status season {year} -vv")
    assert result.exit_code == 0


def test_status_season_date_range_summary_all():
    year = 2019
    runner = CliRunner()
    result = runner.invoke(cli, f"status season {year} -vvv")
    assert result.exit_code == 0


def test_status_season_date_range_detail_only_missing():
    year = 2019
    runner = CliRunner()
    result = runner.invoke(cli, f"status season {year} -vvvv")
    assert result.exit_code == 0


def test_status_season_date_range_detail_all():
    year = 2019
    runner = CliRunner()
    result = runner.invoke(cli, f"status season {year} -vvvvv")
    assert result.exit_code == 0


def test_status_season_date_range_detail_all_with_missing_ids():
    year = 2019
    runner = CliRunner()
    result = runner.invoke(cli, f"status season {year} -vvvvvv")
    assert result.exit_code == 0
