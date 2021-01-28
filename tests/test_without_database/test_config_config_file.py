from pathlib import Path

from tests.conftest import TESTS_FOLDER
from vigorish.config.config_file import ConfigFile
from vigorish.enums import DataSet, ScrapeCondition


def test_config_file():
    default_config_path = Path(TESTS_FOLDER).joinpath("default.config.json")
    if default_config_path.exists():
        default_config_path.unlink()

    config_file = ConfigFile(config_file_path=default_config_path)
    config_file.create_default_config_file()
    assert default_config_path.exists()

    config_file.change_setting("SCRAPED_DATA_COMBINE_CONDITION", DataSet.ALL, ScrapeCondition.ONLY_MISSING_DATA)
    config_file.change_setting("SCRAPE_CONDITION", DataSet.BROOKS_PITCHFX, ScrapeCondition.ALWAYS)

    config_file.change_setting("URL_SCRAPE_DELAY", DataSet.ALL, (True, True, None, 3, 6))
    result = config_file.check_url_delay_settings([DataSet.ALL])
    assert result.success

    result = config_file.change_setting("URL_SCRAPE_DELAY", DataSet.ALL, (False, True, None, 5, 10))
    assert result.failure
    assert "URL delay cannot be disabled!" in result.error

    result = config_file.change_setting("URL_SCRAPE_DELAY", DataSet.ALL, (True, True, None, 1, 2))
    assert result.failure
    assert "URL delay min value must be greater than 2 seconds!" in result.error

    config_file.config_json["URL_SCRAPE_DELAY"]["ALL"]["URL_SCRAPE_DELAY_IS_REQUIRED"] = False
    result = config_file.check_url_delay_settings([DataSet.ALL])
    assert result.failure
    assert "URL delay cannot be disabled and must be at least 3 seconds" in result.error
    url_delay_setting = config_file.get_current_setting("URL_SCRAPE_DELAY", DataSet.ALL)
    assert "No delay after each URL" in str(url_delay_setting)

    config_file.config_json["URL_SCRAPE_DELAY"]["ALL"]["URL_SCRAPE_DELAY_IS_REQUIRED"] = True
    config_file.config_json["URL_SCRAPE_DELAY"]["ALL"]["URL_SCRAPE_DELAY_IS_RANDOM"] = False
    config_file.config_json["URL_SCRAPE_DELAY"]["ALL"]["URL_SCRAPE_DELAY_IN_SECONDS"] = 1
    result = config_file.check_url_delay_settings([DataSet.ALL])
    assert result.failure
    assert "URL delay cannot be disabled and must be at least 3 seconds" in result.error
    url_delay_setting = config_file.get_current_setting("URL_SCRAPE_DELAY", DataSet.ALL)
    assert "Delay is uniform (1 seconds)" in str(url_delay_setting)
    assert url_delay_setting.delay_uniform_ms == 1000

    config_file.config_json["URL_SCRAPE_DELAY"]["ALL"]["URL_SCRAPE_DELAY_IS_REQUIRED"] = True
    config_file.config_json["URL_SCRAPE_DELAY"]["ALL"]["URL_SCRAPE_DELAY_IS_RANDOM"] = True
    config_file.config_json["URL_SCRAPE_DELAY"]["ALL"]["URL_SCRAPE_DELAY_IN_SECONDS_MIN"] = 2
    result = config_file.check_url_delay_settings([DataSet.ALL])
    assert result.failure
    assert "URL delay cannot be disabled and must be at least 3 seconds" in result.error
    url_delay_setting = config_file.get_current_setting("URL_SCRAPE_DELAY", DataSet.ALL)
    assert "Delay is random (2-6 seconds)" in str(url_delay_setting)

    config_file.change_setting("URL_SCRAPE_DELAY", DataSet.ALL, (True, True, None, 3, 6))
    result = config_file.check_url_delay_settings([DataSet.ALL])
    assert result.success

    url_delay_setting = config_file.get_current_setting("URL_SCRAPE_DELAY", DataSet.ALL)
    assert "Delay is random (3-6 seconds)" in str(url_delay_setting)

    config_file.change_setting("BATCH_SCRAPE_DELAY", DataSet.BBREF_BOXSCORES, (True, True, None, 10, 20))
    config_file.change_setting("BATCH_SCRAPE_DELAY", DataSet.BBREF_GAMES_FOR_DATE, (True, True, None, 10, 20))
    config_file.change_setting("BATCH_SCRAPE_DELAY", DataSet.BROOKS_GAMES_FOR_DATE, (True, True, None, 10, 20))
    config_file.change_setting("BATCH_SCRAPE_DELAY", DataSet.BROOKS_PITCH_LOGS, (True, False, 8, None, None))
    config_file.change_setting("BATCH_SCRAPE_DELAY", DataSet.BROOKS_PITCHFX, (False, None, None, None, None))

    batch_delay_1 = config_file.get_current_setting("BATCH_SCRAPE_DELAY", DataSet.BROOKS_PITCHFX)
    assert "No delay after each batch" in str(batch_delay_1)

    batch_delay_2 = config_file.get_current_setting("BATCH_SCRAPE_DELAY", DataSet.BROOKS_PITCH_LOGS)
    assert "Delay is uniform (8 minutes)" in str(batch_delay_2)
    assert batch_delay_2.delay_uniform_ms == 480000

    batch_delay_3 = config_file.get_current_setting("BATCH_SCRAPE_DELAY", DataSet.BBREF_BOXSCORES)
    assert "Delay is random (10-20 minutes)" in str(batch_delay_3)

    config_file.change_setting("BATCH_JOB_SETTINGS", DataSet.BBREF_BOXSCORES, (True, False, 5, None, None))
    config_file.change_setting("BATCH_JOB_SETTINGS", DataSet.BBREF_GAMES_FOR_DATE, (False, None, None, None, None))
    config_file.change_setting("BATCH_JOB_SETTINGS", DataSet.BROOKS_GAMES_FOR_DATE, (True, True, None, 20, 30))
    config_file.change_setting("BATCH_JOB_SETTINGS", DataSet.BROOKS_PITCH_LOGS, (True, True, None, 30, 40))
    config_file.change_setting("BATCH_JOB_SETTINGS", DataSet.BROOKS_PITCHFX, (True, True, None, 40, 50))

    batch_job_1 = config_file.get_current_setting("BATCH_JOB_SETTINGS", DataSet.BBREF_GAMES_FOR_DATE)
    assert "Batched scraping is not enabled" in str(batch_job_1)
    batch_job_2 = config_file.get_current_setting("BATCH_JOB_SETTINGS", DataSet.BBREF_BOXSCORES)
    assert "Batch size is uniform (5 URLs)" in str(batch_job_2)
    batch_settings_3 = config_file.get_current_setting("BATCH_JOB_SETTINGS", DataSet.BROOKS_GAMES_FOR_DATE)
    assert "Batch size is random (20-30 URLs)" in str(batch_settings_3)

    config_file.change_setting("S3_BUCKET", DataSet.ALL, "test-all")
    config_file.change_setting("S3_BUCKET", DataSet.BBREF_GAMES_FOR_DATE, "test-single")

    script_params = config_file.get_nodejs_script_args(DataSet.BBREF_BOXSCORES, TESTS_FOLDER.joinpath("urlset.json"))
    assert script_params

    script_params = config_file.get_nodejs_script_args(
        DataSet.BBREF_GAMES_FOR_DATE, TESTS_FOLDER.joinpath("urlset.json")
    )
    assert script_params

    s3_required = config_file.s3_bucket_required([DataSet.ALL])
    assert not s3_required

    default_config_path.unlink()
