from collections import namedtuple
from datetime import datetime, timezone
from unittest.mock import call, patch, PropertyMock

from click.testing import CliRunner

from tests.conftest import TESTS_FOLDER
from vigorish.cli.vig import cli
from vigorish.enums import SyncDirection

S3MockObject = namedtuple("S3MockObject", ["key", "size", "last_modified"])

BR_DAILY_OBJECTS_MOCK_DATA = [
    S3MockObject(
        "2017/bbref_games_for_date/bbref_games_for_date_2017-05-26.json",
        2687,
        datetime(2021, 1, 13, 17, 50, 44, 679472, tzinfo=timezone.utc),
    ),
    S3MockObject(
        "2017/bbref_games_for_date/bbref_games_for_date_2017-05-27.json",
        2853,
        datetime(2021, 1, 13, 17, 50, 44, 679472, tzinfo=timezone.utc),
    ),
    S3MockObject(
        "2017/bbref_games_for_date/bbref_games_for_date_2017-09-15.json",
        2687,
        datetime(2021, 1, 13, 17, 50, 44, 683472, tzinfo=timezone.utc),
    ),
    # File in local folder is newer than the version in S3
    S3MockObject(
        "2017/bbref_games_for_date/bbref_games_for_date_2017-09-15_PATCH_LIST.json",
        310,
        datetime(2021, 1, 3, 20, 45, 27, tzinfo=timezone.utc),
    ),
]

BB_DAILY_OBJECTS_MOCK_DATA = [
    S3MockObject(
        "2017/brooks_games_for_date/brooks_games_for_date_2017-05-26.json",
        33747,
        datetime(2021, 1, 13, 17, 50, 44, 683472, tzinfo=timezone.utc),
    ),
    # File in S3 is newer than the version in local folder
    S3MockObject(
        "2017/brooks_games_for_date/brooks_games_for_date_2017-05-26_PATCH_LIST.json",
        812,
        datetime(2021, 2, 11, 13, 21, 19, tzinfo=timezone.utc),
    ),
    # File does not exist in S3
    # S3MockObject(
    #     "2017/brooks_games_for_date/brooks_games_for_date_2017-05-27.json",
    #     35022,
    #     datetime(2021, 1, 13, 17, 50, 44, 683472, tzinfo=timezone.utc),
    # ),
]

BR_BOX_OBJECTS_MOCK_DATA = [
    # File in local folder is newer than the version in S3
    S3MockObject(
        "2017/bbref_boxscores/CHA201705260.json", 180830, datetime(2021, 1, 10, 7, 25, 19, 920012, tzinfo=timezone.utc)
    ),
    # File in S3 is newer than the version in local folder
    S3MockObject(
        "2017/bbref_boxscores/CHA201705272.json", 110660, datetime(2021, 1, 15, 10, 3, 28, 695848, tzinfo=timezone.utc)
    ),
]

BB_PLOG_OBJECTS_MOCK_DATA = [
    # File does not exist in local folder
    S3MockObject(
        "2017/brooks_pitch_logs/gid_2017_05_26_oakmlb_nyamlb_1.json",
        6496,
        datetime(2021, 1, 4, 5, 22, 19, tzinfo=timezone.utc),
    ),
    # File does not exist in local folder
    S3MockObject(
        "2017/brooks_pitch_logs/gid_2017_09_15_sdnmlb_colmlb_1.json",
        9499,
        datetime(2021, 1, 2, 17, 56, 33, tzinfo=timezone.utc),
    ),
]

BB_PFX_OBJECTS_MOCK_DATA = [
    # File does not exist in local folder
    S3MockObject(
        "2017/brooks_pitchfx/HOU201705270_489119.json",
        251036,
        datetime(2021, 1, 12, 7, 57, 28, 695848, tzinfo=timezone.utc),
    ),
    # File does not exist in local folder
    S3MockObject(
        "2017/brooks_pitchfx/COL201705270_572096.json",
        83271,
        datetime(2021, 1, 12, 7, 59, 13, 920012, tzinfo=timezone.utc),
    ),
]

ALL_S3_OBJECTS_MOCK_DATA = (
    BR_DAILY_OBJECTS_MOCK_DATA
    + BB_DAILY_OBJECTS_MOCK_DATA
    + BR_BOX_OBJECTS_MOCK_DATA
    + BB_PLOG_OBJECTS_MOCK_DATA
    + BB_PFX_OBJECTS_MOCK_DATA
)


def test_cli_sync_up_parsed_json(mocker):
    def send_file_side_effect(sync_direction, local_path, s3_key):
        print(f"send_file called with {sync_direction}, {local_path}, {s3_key}")

    with patch(
        "vigorish.tasks.sync_scraped_data.SyncScrapedDataTask.all_s3_objects", new_callable=PropertyMock
    ) as all_s3_objects_mock:
        with patch("vigorish.tasks.sync_scraped_data.SyncScrapedDataTask.send_file") as mock_send_file:
            all_s3_objects_mock.return_value = ALL_S3_OBJECTS_MOCK_DATA
            mock_send_file.side_effect = send_file_side_effect
            runner = CliRunner()
            result = runner.invoke(cli, "sync up 2017 --file-type=PARSED_JSON")
            assert result.exit_code == 0
            expected_calls = [
                call(
                    SyncDirection.UP_TO_S3,
                    str(
                        TESTS_FOLDER.joinpath(
                            "json_storage/2017/brooks_games_for_date/brooks_games_for_date_2017-05-27.json"
                        )
                    ),
                    "2017/brooks_games_for_date/brooks_games_for_date_2017-05-27.json",
                ),
                call(
                    SyncDirection.UP_TO_S3,
                    str(TESTS_FOLDER.joinpath("json_storage/2017/bbref_boxscores/CHA201705260.json")),
                    "2017/bbref_boxscores/CHA201705260.json",
                ),
            ]
            assert mock_send_file.call_args_list == expected_calls


def test_cli_sync_down_parsed_json(mocker):
    def send_file_side_effect(sync_direction, local_path, s3_key):
        print(f"send_file called with {sync_direction}, {local_path}, {s3_key}")

    with patch(
        "vigorish.tasks.sync_scraped_data.SyncScrapedDataTask.all_s3_objects", new_callable=PropertyMock
    ) as all_s3_objects_mock:
        with patch("vigorish.tasks.sync_scraped_data.SyncScrapedDataTask.send_file") as mock_send_file:
            all_s3_objects_mock.return_value = ALL_S3_OBJECTS_MOCK_DATA
            mock_send_file.side_effect = send_file_side_effect
            runner = CliRunner()
            result = runner.invoke(cli, "sync down 2017 --file-type=PARSED_JSON")
            assert result.exit_code == 0
            expected_calls = [
                call(
                    SyncDirection.DOWN_TO_LOCAL,
                    str(TESTS_FOLDER.joinpath("json_storage/2017/bbref_boxscores/CHA201705272.json")),
                    "2017/bbref_boxscores/CHA201705272.json",
                ),
                call(
                    SyncDirection.DOWN_TO_LOCAL,
                    str(
                        TESTS_FOLDER.joinpath("json_storage/2017/brooks_pitch_logs/gid_2017_05_26_oakmlb_nyamlb_1.json")
                    ),
                    "2017/brooks_pitch_logs/gid_2017_05_26_oakmlb_nyamlb_1.json",
                ),
                call(
                    SyncDirection.DOWN_TO_LOCAL,
                    str(
                        TESTS_FOLDER.joinpath("json_storage/2017/brooks_pitch_logs/gid_2017_09_15_sdnmlb_colmlb_1.json")
                    ),
                    "2017/brooks_pitch_logs/gid_2017_09_15_sdnmlb_colmlb_1.json",
                ),
                call(
                    SyncDirection.DOWN_TO_LOCAL,
                    str(TESTS_FOLDER.joinpath("json_storage/2017/brooks_pitchfx/COL201705270_572096.json")),
                    "2017/brooks_pitchfx/COL201705270_572096.json",
                ),
                call(
                    SyncDirection.DOWN_TO_LOCAL,
                    str(TESTS_FOLDER.joinpath("json_storage/2017/brooks_pitchfx/HOU201705270_489119.json")),
                    "2017/brooks_pitchfx/HOU201705270_489119.json",
                ),
            ]
            assert mock_send_file.call_args_list == expected_calls


def test_cli_sync_up_patch_list(mocker):
    def send_file_side_effect(sync_direction, local_path, s3_key):
        print(f"send_file called with {sync_direction}, {local_path}, {s3_key}")

    with patch(
        "vigorish.tasks.sync_scraped_data.SyncScrapedDataTask.all_s3_objects", new_callable=PropertyMock
    ) as all_s3_objects_mock:
        with patch("vigorish.tasks.sync_scraped_data.SyncScrapedDataTask.send_file") as mock_send_file:
            all_s3_objects_mock.return_value = ALL_S3_OBJECTS_MOCK_DATA
            mock_send_file.side_effect = send_file_side_effect
            runner = CliRunner()
            result = runner.invoke(cli, "sync up 2017 --file-type=PATCH_LIST")
            assert result.exit_code == 0
            expected_calls = [
                call(
                    SyncDirection.UP_TO_S3,
                    str(
                        TESTS_FOLDER.joinpath(
                            "json_storage/2017/bbref_games_for_date/bbref_games_for_date_2017-09-15_PATCH_LIST.json"
                        )
                    ),
                    "2017/bbref_games_for_date/bbref_games_for_date_2017-09-15_PATCH_LIST.json",
                ),
            ]
            assert mock_send_file.call_args_list == expected_calls


def test_cli_sync_down_patch_list(mocker):
    def send_file_side_effect(sync_direction, local_path, s3_key):
        print(f"send_file called with {sync_direction}, {local_path}, {s3_key}")

    with patch(
        "vigorish.tasks.sync_scraped_data.SyncScrapedDataTask.all_s3_objects", new_callable=PropertyMock
    ) as all_s3_objects_mock:
        with patch("vigorish.tasks.sync_scraped_data.SyncScrapedDataTask.send_file") as mock_send_file:
            all_s3_objects_mock.return_value = ALL_S3_OBJECTS_MOCK_DATA
            mock_send_file.side_effect = send_file_side_effect
            runner = CliRunner()
            result = runner.invoke(cli, "sync down 2017 --file-type=PATCH_LIST")
            assert result.exit_code == 0
            expected_calls = [
                call(
                    SyncDirection.DOWN_TO_LOCAL,
                    str(
                        TESTS_FOLDER.joinpath(
                            "json_storage/2017/brooks_games_for_date/brooks_games_for_date_2017-05-26_PATCH_LIST.json"
                        )
                    ),
                    "2017/brooks_games_for_date/brooks_games_for_date_2017-05-26_PATCH_LIST.json",
                ),
            ]
            assert mock_send_file.call_args_list == expected_calls
