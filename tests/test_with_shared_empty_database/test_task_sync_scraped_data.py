from collections import namedtuple
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import call, patch, PropertyMock

from click.testing import CliRunner

from tests.conftest import ROOT_FOLDER
from vigorish.cli.vig import cli
from vigorish.enums import DataSet, SyncDirection, VigFile
from vigorish.util.datetime_util import dtaware_fromtimestamp

MLB_SEASON = 2017
S3ObjectMock = namedtuple("S3ObjectMock", ["key", "size", "last_modified"])


def get_s3_key(vig_app, file_id, file_type, data_set):
    s3_folder = vig_app.scraped_data.file_helper.get_s3_folderpath(file_type, data_set, year=MLB_SEASON)
    return f"{s3_folder}/{file_id}.json"


def get_local_filepath(vig_app, file_id, file_type, data_set):
    config_folder = vig_app.scraped_data.file_helper.get_local_folderpath(file_type, data_set, year=MLB_SEASON)
    local_folder = config_folder if Path(config_folder).is_absolute() else ROOT_FOLDER.joinpath(config_folder)
    return Path(local_folder).joinpath(f"{file_id}.json")


def create_s3_object_mock(vig_app, file_id, file_type, data_set, mod_size=0, mod_mtime=None):
    s3_key = get_s3_key(vig_app, file_id, file_type, data_set)
    local_file = get_local_filepath(vig_app, file_id, file_type, data_set)
    size = local_file.stat().st_size
    if mod_size:
        size += mod_size
    last_modified = dtaware_fromtimestamp(local_file.stat().st_mtime, use_tz=timezone.utc)
    if mod_mtime:
        last_modified += mod_mtime
    return S3ObjectMock(s3_key, size, last_modified)


def create_call_result(vig_app, file_id, sync_direction, file_type, data_set):
    local_file = str(get_local_filepath(vig_app, file_id, file_type, data_set))
    s3_key = get_s3_key(vig_app, file_id, file_type, data_set)
    return call(sync_direction, local_file, s3_key)


def create_br_daily_objects_mock_data(vig_app):
    FILE_TYPE = VigFile.PARSED_JSON
    DATA_SET = DataSet.BBREF_GAMES_FOR_DATE
    # All three files: versions on S3 and local folder are exactly the same
    FILE_1_ID = "bbref_games_for_date_2017-05-26"
    FILE_2_ID = "bbref_games_for_date_2017-05-27"
    FILE_3_ID = "bbref_games_for_date_2017-09-15"
    return [
        create_s3_object_mock(vig_app, FILE_1_ID, FILE_TYPE, DATA_SET),
        create_s3_object_mock(vig_app, FILE_2_ID, FILE_TYPE, DATA_SET),
        create_s3_object_mock(vig_app, FILE_3_ID, FILE_TYPE, DATA_SET),
    ]


def create_br_daily_patch_list_objects_mock_data(vig_app):
    FILE_TYPE = VigFile.PATCH_LIST
    DATA_SET = DataSet.BBREF_GAMES_FOR_DATE
    # File in local folder is newer than the version in S3
    FILE_1_ID = "bbref_games_for_date_2017-09-15_PATCH_LIST"
    FILE_1_MOD_SIZE = 125
    FILE_1_MOD_MTIME = timedelta(hours=-15)
    return [create_s3_object_mock(vig_app, FILE_1_ID, FILE_TYPE, DATA_SET, FILE_1_MOD_SIZE, FILE_1_MOD_MTIME)]


def create_bb_daily_objects_mock_data(vig_app):
    FILE_TYPE = VigFile.PARSED_JSON
    DATA_SET = DataSet.BROOKS_GAMES_FOR_DATE
    # Both files are exactly the same
    FILE_1_ID = "brooks_games_for_date_2017-05-26"
    return [create_s3_object_mock(vig_app, FILE_1_ID, FILE_TYPE, DATA_SET)]


def create_bb_daily_patch_list_objects_mock_data(vig_app):
    FILE_TYPE = VigFile.PATCH_LIST
    DATA_SET = DataSet.BROOKS_GAMES_FOR_DATE
    # File in S3 is newer than the version in local folder
    FILE_1_ID = "brooks_games_for_date_2017-05-26_PATCH_LIST"
    FILE_1_MOD_SIZE = 250
    FILE_1_MOD_MTIME = timedelta(days=3)
    return [create_s3_object_mock(vig_app, FILE_1_ID, FILE_TYPE, DATA_SET, FILE_1_MOD_SIZE, FILE_1_MOD_MTIME)]


def create_br_box_objects_mock_data(vig_app):
    FILE_TYPE = VigFile.PARSED_JSON
    DATA_SET = DataSet.BBREF_BOXSCORES
    # File in local folder is newer than the version in S3
    FILE_1_ID = "CHA201705260"
    FILE_1_MOD_SIZE = 72042
    FILE_1_MOD_MTIME = timedelta(hours=-15)
    # File in S3 is newer than the version in local folder
    FILE_2_ID = "CHA201705272"
    FILE_2_MOD_SIZE = 9423
    FILE_2_MOD_MTIME = timedelta(days=1, hours=3)

    return [
        create_s3_object_mock(vig_app, FILE_1_ID, FILE_TYPE, DATA_SET, FILE_1_MOD_SIZE, FILE_1_MOD_MTIME),
        create_s3_object_mock(vig_app, FILE_2_ID, FILE_TYPE, DATA_SET, FILE_2_MOD_SIZE, FILE_2_MOD_MTIME),
    ]


def create_bb_plog_objects_mock_data(vig_app):
    FILE_TYPE = VigFile.PARSED_JSON
    DATA_SET = DataSet.BROOKS_PITCH_LOGS

    # Both files do not exist in local folder
    FILE_1_ID = "gid_2017_05_26_oakmlb_nyamlb_1"
    FILE_1_SIZE = 6496
    FILE_1_MTIME = datetime(2021, 1, 4, 5, 22, 19, tzinfo=timezone.utc)
    file_1_s3_key = get_s3_key(vig_app, FILE_1_ID, FILE_TYPE, DATA_SET)

    FILE_2_ID = "gid_2017_09_15_sdnmlb_colmlb_1"
    FILE_2_SIZE = 9499
    FILE_2_MTIME = datetime(2021, 1, 2, 17, 56, 33, tzinfo=timezone.utc)
    file_2_s3_key = get_s3_key(vig_app, FILE_2_ID, FILE_TYPE, DATA_SET)

    return [
        S3ObjectMock(file_1_s3_key, FILE_1_SIZE, FILE_1_MTIME),
        S3ObjectMock(file_2_s3_key, FILE_2_SIZE, FILE_2_MTIME),
    ]


def create_bb_pfx_objects_mock_data(vig_app):
    FILE_TYPE = VigFile.PARSED_JSON
    DATA_SET = DataSet.BROOKS_PITCHFX

    # Both files do not exist in local folder
    FILE_1_ID = "HOU201705270_489119"
    FILE_1_SIZE = 251036
    FILE_1_MTIME = datetime(2021, 1, 12, 7, 57, 28, 695848, tzinfo=timezone.utc)
    file_1_s3_key = get_s3_key(vig_app, FILE_1_ID, FILE_TYPE, DATA_SET)

    FILE_2_ID = "COL201705270_572096"
    FILE_2_SIZE = 83271
    FILE_2_MTIME = datetime(2021, 1, 12, 7, 59, 13, 920012, tzinfo=timezone.utc)
    file_2_s3_key = get_s3_key(vig_app, FILE_2_ID, FILE_TYPE, DATA_SET)

    return [
        S3ObjectMock(file_1_s3_key, FILE_1_SIZE, FILE_1_MTIME),
        S3ObjectMock(file_2_s3_key, FILE_2_SIZE, FILE_2_MTIME),
    ]


def test_cli_sync_up_parsed_json(vig_app):
    def send_file_side_effect(sync_direction, local_path, s3_key):
        print(f"send_file called with {sync_direction}, {local_path}, {s3_key}")

    ALL_S3_OBJECTS_MOCK_DATA = (
        create_br_daily_objects_mock_data(vig_app)
        + create_br_daily_patch_list_objects_mock_data(vig_app)
        + create_bb_daily_objects_mock_data(vig_app)
        + create_bb_daily_patch_list_objects_mock_data(vig_app)
        + create_br_box_objects_mock_data(vig_app)
        + create_bb_plog_objects_mock_data(vig_app)
        + create_bb_pfx_objects_mock_data(vig_app)
    )

    with patch(
        "vigorish.tasks.sync_scraped_data.SyncScrapedDataTask.all_s3_objects", new_callable=PropertyMock
    ) as all_s3_objects_mock:
        with patch("vigorish.tasks.sync_scraped_data.SyncScrapedDataTask.send_file") as send_file_mock:
            SYNC_DIRECTION = SyncDirection.UP_TO_S3
            FILE_TYPE = VigFile.PARSED_JSON
            FILE_1_ID = "brooks_games_for_date_2017-05-27"
            FILE_1_DATA_SET = DataSet.BROOKS_GAMES_FOR_DATE
            FILE_2_ID = "CHA201705260"
            FILE_2_DATA_SET = DataSet.BBREF_BOXSCORES

            all_s3_objects_mock.return_value = ALL_S3_OBJECTS_MOCK_DATA
            send_file_mock.side_effect = send_file_side_effect
            runner = CliRunner()
            result = runner.invoke(cli, f"sync up 2017 --file-type={FILE_TYPE}")
            assert result.exit_code == 0

            expected_calls = [
                create_call_result(vig_app, FILE_1_ID, SYNC_DIRECTION, FILE_TYPE, FILE_1_DATA_SET),
                create_call_result(vig_app, FILE_2_ID, SYNC_DIRECTION, FILE_TYPE, FILE_2_DATA_SET),
            ]
            assert send_file_mock.call_args_list == expected_calls


def test_cli_sync_down_parsed_json(vig_app):
    def send_file_side_effect(sync_direction, local_path, s3_key):
        print(f"send_file called with {sync_direction}, {local_path}, {s3_key}")

    ALL_S3_OBJECTS_MOCK_DATA = (
        create_br_daily_objects_mock_data(vig_app)
        + create_br_daily_patch_list_objects_mock_data(vig_app)
        + create_bb_daily_objects_mock_data(vig_app)
        + create_bb_daily_patch_list_objects_mock_data(vig_app)
        + create_br_box_objects_mock_data(vig_app)
        + create_bb_plog_objects_mock_data(vig_app)
        + create_bb_pfx_objects_mock_data(vig_app)
    )

    with patch(
        "vigorish.tasks.sync_scraped_data.SyncScrapedDataTask.all_s3_objects", new_callable=PropertyMock
    ) as all_s3_objects_mock:
        with patch("vigorish.tasks.sync_scraped_data.SyncScrapedDataTask.send_file") as send_file_mock:
            SYNC_DIRECTION = SyncDirection.DOWN_TO_LOCAL
            FILE_TYPE = VigFile.PARSED_JSON
            FILE_1_ID = "CHA201705272"
            FILE_1_DATA_SET = DataSet.BBREF_BOXSCORES
            FILE_2_ID = "gid_2017_05_26_oakmlb_nyamlb_1"
            FILE_2_DATA_SET = DataSet.BROOKS_PITCH_LOGS
            FILE_3_ID = "gid_2017_09_15_sdnmlb_colmlb_1"
            FILE_3_DATA_SET = DataSet.BROOKS_PITCH_LOGS
            FILE_4_ID = "COL201705270_572096"
            FILE_4_DATA_SET = DataSet.BROOKS_PITCHFX
            FILE_5_ID = "HOU201705270_489119"
            FILE_5_DATA_SET = DataSet.BROOKS_PITCHFX

            all_s3_objects_mock.return_value = ALL_S3_OBJECTS_MOCK_DATA
            send_file_mock.side_effect = send_file_side_effect
            runner = CliRunner()
            result = runner.invoke(cli, f"sync down 2017 --file-type={FILE_TYPE}")
            assert result.exit_code == 0
            expected_calls = [
                create_call_result(vig_app, FILE_1_ID, SYNC_DIRECTION, FILE_TYPE, FILE_1_DATA_SET),
                create_call_result(vig_app, FILE_2_ID, SYNC_DIRECTION, FILE_TYPE, FILE_2_DATA_SET),
                create_call_result(vig_app, FILE_3_ID, SYNC_DIRECTION, FILE_TYPE, FILE_3_DATA_SET),
                create_call_result(vig_app, FILE_4_ID, SYNC_DIRECTION, FILE_TYPE, FILE_4_DATA_SET),
                create_call_result(vig_app, FILE_5_ID, SYNC_DIRECTION, FILE_TYPE, FILE_5_DATA_SET),
            ]
            assert send_file_mock.call_args_list == expected_calls


def test_cli_sync_up_patch_list(vig_app):
    def send_file_side_effect(sync_direction, local_path, s3_key):
        print(f"send_file called with {sync_direction}, {local_path}, {s3_key}")

    ALL_S3_OBJECTS_MOCK_DATA = (
        create_br_daily_objects_mock_data(vig_app)
        + create_br_daily_patch_list_objects_mock_data(vig_app)
        + create_bb_daily_objects_mock_data(vig_app)
        + create_bb_daily_patch_list_objects_mock_data(vig_app)
        + create_br_box_objects_mock_data(vig_app)
        + create_bb_plog_objects_mock_data(vig_app)
        + create_bb_pfx_objects_mock_data(vig_app)
    )

    with patch(
        "vigorish.tasks.sync_scraped_data.SyncScrapedDataTask.all_s3_objects", new_callable=PropertyMock
    ) as all_s3_objects_mock:
        with patch("vigorish.tasks.sync_scraped_data.SyncScrapedDataTask.send_file") as send_file_mock:
            SYNC_DIRECTION = SyncDirection.UP_TO_S3
            FILE_TYPE = VigFile.PATCH_LIST
            DATA_SET = DataSet.BBREF_GAMES_FOR_DATE
            FILE_ID = "bbref_games_for_date_2017-09-15_PATCH_LIST"

            all_s3_objects_mock.return_value = ALL_S3_OBJECTS_MOCK_DATA
            send_file_mock.side_effect = send_file_side_effect
            runner = CliRunner()
            result = runner.invoke(cli, f"sync up 2017 --file-type={FILE_TYPE}")
            assert result.exit_code == 0
            expected_calls = [create_call_result(vig_app, FILE_ID, SYNC_DIRECTION, FILE_TYPE, DATA_SET)]
            assert send_file_mock.call_args_list == expected_calls


def test_cli_sync_down_patch_list(vig_app):
    def send_file_side_effect(sync_direction, local_path, s3_key):
        print(f"send_file called with {sync_direction}, {local_path}, {s3_key}")

    ALL_S3_OBJECTS_MOCK_DATA = (
        create_br_daily_objects_mock_data(vig_app)
        + create_br_daily_patch_list_objects_mock_data(vig_app)
        + create_bb_daily_objects_mock_data(vig_app)
        + create_bb_daily_patch_list_objects_mock_data(vig_app)
        + create_br_box_objects_mock_data(vig_app)
        + create_bb_plog_objects_mock_data(vig_app)
        + create_bb_pfx_objects_mock_data(vig_app)
    )

    with patch(
        "vigorish.tasks.sync_scraped_data.SyncScrapedDataTask.all_s3_objects", new_callable=PropertyMock
    ) as all_s3_objects_mock:
        with patch("vigorish.tasks.sync_scraped_data.SyncScrapedDataTask.send_file") as send_file_mock:
            SYNC_DIRECTION = SyncDirection.DOWN_TO_LOCAL
            FILE_TYPE = VigFile.PATCH_LIST
            DATA_SET = DataSet.BROOKS_GAMES_FOR_DATE
            FILE_ID = "brooks_games_for_date_2017-05-26_PATCH_LIST"

            all_s3_objects_mock.return_value = ALL_S3_OBJECTS_MOCK_DATA
            send_file_mock.side_effect = send_file_side_effect
            runner = CliRunner()
            result = runner.invoke(cli, f"sync down 2017 --file-type={FILE_TYPE}")
            assert result.exit_code == 0
            expected_calls = [create_call_result(vig_app, FILE_ID, SYNC_DIRECTION, FILE_TYPE, DATA_SET)]
            assert send_file_mock.call_args_list == expected_calls
