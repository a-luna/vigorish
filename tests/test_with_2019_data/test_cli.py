from datetime import datetime

from click.testing import CliRunner

from vigorish.cli.vig import cli
from vigorish.util.dt_format_strings import DATE_ONLY_2


def test_status_single_date_without_games_without_missing_pfx():
    game_date = datetime(2019, 6, 17).strftime(DATE_ONLY_2)
    runner = CliRunner()
    result = runner.invoke(cli, f"status date {game_date}")
    assert result.exit_code == 0
    assert "### OVERALL STATUS FOR Jun 17 2019 ###" in result.output
    assert "### STATUS FOR SEA201906170 (Game " not in result.output
    assert "### STATUS FOR ATL201906170 (Game " not in result.output
    assert "### STATUS FOR OAK201906170 (Game " not in result.output
    assert "### STATUS FOR SLN201906170 (Game " not in result.output
    assert "### STATUS FOR TEX201906170 (Game " not in result.output
    assert "### STATUS FOR SDN201906170 (Game " not in result.output
    assert "### STATUS FOR LAN201906170 (Game " not in result.output
    assert "### STATUS FOR CIN201906170 (Game " not in result.output
    assert "### STATUS FOR NYA201906170 (Game " not in result.output
    assert "### STATUS FOR MIN201906170 (Game " not in result.output
    assert "### STATUS FOR TOR201906170 (Game " not in result.output
    assert "### MISSING PITCHFX DATA FOR Jun 17 2019 ###" not in result.output


def test_status_single_date_without_games_with_missing_pfx():
    game_date = datetime(2019, 6, 17).strftime(DATE_ONLY_2)
    runner = CliRunner()
    result = runner.invoke(cli, f"status date {game_date} -vv")
    assert result.exit_code == 0
    assert "### OVERALL STATUS FOR Jun 17 2019 ###" in result.output
    assert "### STATUS FOR SEA201906170 (Game " not in result.output
    assert "### STATUS FOR ATL201906170 (Game " not in result.output
    assert "### STATUS FOR OAK201906170 (Game " not in result.output
    assert "### STATUS FOR SLN201906170 (Game " not in result.output
    assert "### STATUS FOR TEX201906170 (Game " not in result.output
    assert "### STATUS FOR SDN201906170 (Game " not in result.output
    assert "### STATUS FOR LAN201906170 (Game " not in result.output
    assert "### STATUS FOR CIN201906170 (Game " not in result.output
    assert "### STATUS FOR NYA201906170 (Game " not in result.output
    assert "### STATUS FOR MIN201906170 (Game " not in result.output
    assert "### STATUS FOR TOR201906170 (Game " not in result.output
    assert "### MISSING PITCHFX DATA FOR Jun 17 2019 ###" in result.output


def test_status_single_date_with_games_with_missing_pfx():
    game_date = datetime(2019, 6, 17).strftime(DATE_ONLY_2)
    runner = CliRunner()
    result = runner.invoke(cli, f"status date {game_date} -vvv")
    assert result.exit_code == 0
    assert "### OVERALL STATUS FOR Jun 17 2019 ###" in result.output
    assert "### STATUS FOR SEA201906170 (Game " in result.output
    assert "### STATUS FOR ATL201906170 (Game " in result.output
    assert "### STATUS FOR OAK201906170 (Game " in result.output
    assert "### STATUS FOR SLN201906170 (Game " in result.output
    assert "### STATUS FOR TEX201906170 (Game " in result.output
    assert "### STATUS FOR SDN201906170 (Game " in result.output
    assert "### STATUS FOR LAN201906170 (Game " in result.output
    assert "### STATUS FOR CIN201906170 (Game " in result.output
    assert "### STATUS FOR NYA201906170 (Game " in result.output
    assert "### STATUS FOR MIN201906170 (Game " in result.output
    assert "### STATUS FOR TOR201906170 (Game " in result.output
    assert "### MISSING PITCHFX DATA FOR Jun 17 2019 ###" in result.output


def test_status_season_overall_summary():
    year = 2019
    runner = CliRunner()
    result = runner.invoke(cli, f"status season {year} -v")
    assert result.exit_code == 0
    assert "### STATUS REPORT FOR MLB 2019 Regular Season ###" in result.output


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


def test_status_date_range_summary_only_missing():
    start_date = datetime(2019, 3, 28).strftime(DATE_ONLY_2)
    end_date = datetime(2019, 9, 29).strftime(DATE_ONLY_2)
    runner = CliRunner()
    result = runner.invoke(cli, f"status range --start={start_date} --end={end_date}")
    assert result.exit_code == 0


def test_status_date_range_summary_all():
    start_date = datetime(2019, 3, 28).strftime(DATE_ONLY_2)
    end_date = datetime(2019, 9, 29).strftime(DATE_ONLY_2)
    runner = CliRunner()
    result = runner.invoke(cli, f"status range --start={start_date} --end={end_date} -vv")
    assert result.exit_code == 0


def test_status_date_range_detail_only_missing():
    start_date = datetime(2019, 3, 28).strftime(DATE_ONLY_2)
    end_date = datetime(2019, 9, 29).strftime(DATE_ONLY_2)
    runner = CliRunner()
    result = runner.invoke(cli, f"status range --start={start_date} --end={end_date} -vvv")
    assert result.exit_code == 0


def test_status_date_range_detail_all():
    start_date = datetime(2019, 3, 28).strftime(DATE_ONLY_2)
    end_date = datetime(2019, 9, 29).strftime(DATE_ONLY_2)
    runner = CliRunner()
    result = runner.invoke(cli, f"status range --start={start_date} --end={end_date} -vvvv")
    assert result.exit_code == 0


def test_status_date_range_detail_all_with_missing_ids():
    start_date = datetime(2019, 3, 28).strftime(DATE_ONLY_2)
    end_date = datetime(2019, 9, 29).strftime(DATE_ONLY_2)
    runner = CliRunner()
    result = runner.invoke(cli, f"status range --start={start_date} --end={end_date} -vvvvv")
    assert result.exit_code == 0
