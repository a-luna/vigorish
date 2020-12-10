from pathlib import Path

from tests.conftest import TESTS_FOLDER
from vigorish.config.config_file import ConfigFile

# from vigorish.config.types.batch_job_settings import BatchJobSettings
# from vigorish.config.types.batch_scrape_delay import BatchScrapeDelay
# from vigorish.config.types.url_scrape_delay import UrlScrapeDelay
from vigorish.enums import DataSet, ScrapeCondition


def test_config_file():
    default_config_path = Path(TESTS_FOLDER).joinpath("default.config.json")
    if default_config_path.exists():
        default_config_path.unlink()

    config_file = ConfigFile(config_file_path=default_config_path)
    config_file.create_default_config_file()
    assert default_config_path.exists()

    config_file.change_setting(
        "SCRAPED_DATA_COMBINE_CONDITION", DataSet.ALL, ScrapeCondition.ONLY_MISSING_DATA
    )
    config_file.change_setting("SCRAPE_CONDITION", DataSet.BROOKS_PITCHFX, ScrapeCondition.ALWAYS)

    config_file.change_setting("URL_SCRAPE_DELAY", DataSet.ALL, (True, True, None, 3, 6))
    config_file.change_setting("BATCH_SCRAPE_DELAY", DataSet.ALL, (True, True, None, 10, 20))

    config_file.change_setting(
        "BATCH_JOB_SETTINGS", DataSet.BBREF_BOXSCORES, (True, True, None, 5, 10)
    )
    config_file.change_setting(
        "BATCH_JOB_SETTINGS", DataSet.BBREF_GAMES_FOR_DATE, (None, None, None, None, None)
    )
    config_file.change_setting(
        "BATCH_JOB_SETTINGS", DataSet.BROOKS_GAMES_FOR_DATE, (True, True, None, 20, 30)
    )
    config_file.change_setting(
        "BATCH_JOB_SETTINGS", DataSet.BROOKS_PITCH_LOGS, (True, True, None, 30, 40)
    )
    config_file.change_setting(
        "BATCH_JOB_SETTINGS", DataSet.BROOKS_PITCHFX, (True, True, None, 40, 50)
    )

    config_file.change_setting("S3_BUCKET", DataSet.ALL, "test-all")
    config_file.change_setting("S3_BUCKET", DataSet.BBREF_GAMES_FOR_DATE, "test-single")

    # batch_job_settings = BatchJobSettings(True, True, None, 50, 80)
    # url_delay_settings = UrlScrapeDelay(True, True, None, 3, 6)
    # batch_delay_settings = BatchScrapeDelay(True, True, None, 30, 45)
    script_params = config_file.get_nodejs_script_args(
        DataSet.BBREF_BOXSCORES, TESTS_FOLDER.joinpath("urlset.json")
    )
    assert script_params

    script_params = config_file.get_nodejs_script_args(
        DataSet.BBREF_GAMES_FOR_DATE, TESTS_FOLDER.joinpath("urlset.json")
    )
    assert script_params

    s3_required = config_file.s3_bucket_required([DataSet.ALL])
    assert not s3_required

    result = config_file.change_setting("URL_SCRAPE_DELAY", DataSet.ALL, (False, True, None, 5, 10))
    assert result.failure
    assert "URL delay cannot be disabled!" in result.error
    result = config_file.change_setting("URL_SCRAPE_DELAY", DataSet.ALL, (True, True, None, 1, 2))
    assert result.failure
    assert "URL delay min value must be greater than 2 seconds!" in result.error
    config_file.change_setting("URL_SCRAPE_DELAY", DataSet.ALL, (True, True, None, 3, 6))

    default_config_path.unlink()
