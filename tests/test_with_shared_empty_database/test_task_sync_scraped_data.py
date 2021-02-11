from collections import namedtuple
from datetime import datetime, timedelta, timezone
from unittest.mock import call, patch, PropertyMock

from click.testing import CliRunner

from tests.conftest import TESTS_FOLDER
from vigorish.cli.vig import cli
from vigorish.enums import DataSet, SyncDirection, VigFile
from vigorish.util.datetime_util import dtaware_fromtimestamp

MLB_SEASON = 2017
BR_DAILY_FOLDER = TESTS_FOLDER.joinpath("json_storage/2017/bbref_games_for_date")
BB_DAILY_FOLDER = TESTS_FOLDER.joinpath("json_storage/2017/brooks_games_for_date")
BR_BOX_FOLDER = TESTS_FOLDER.joinpath("json_storage/2017/bbref_boxscores")
BB_PLOG_FOLDER = TESTS_FOLDER.joinpath("json_storage/2017/brooks_pitch_logs")
BB_PFX_FOLDER = TESTS_FOLDER.joinpath("json_storage/2017/brooks_pitchfx")

S3ObjectMock = namedtuple("S3ObjectMock", ["key", "size", "last_modified"])


def get_s3_folderpath(vig_app, file_type, data_set):
    return vig_app.scraped_data.file_helper.get_s3_folderpath(file_type, data_set, year=MLB_SEASON)


def create_s3_object_mock(vig_app, file, file_type, data_set, mod_size=0, mod_mtime=None):
    s3_folder = get_s3_folderpath(vig_app, file_type, data_set)
    size = file.stat().st_size
    if mod_size:
        size += mod_size
    last_modified = dtaware_fromtimestamp(file.stat().st_mtime, use_tz=timezone.utc)
    if mod_mtime:
        last_modified += mod_mtime
    return S3ObjectMock(f"{s3_folder}/{file.name}", size, last_modified)


def create_br_daily_objects_mock_data(vig_app):
    FILE_TYPE = VigFile.PARSED_JSON
    DATA_SET = DataSet.BBREF_GAMES_FOR_DATE
    local_file_1 = BR_DAILY_FOLDER.joinpath("bbref_games_for_date_2017-05-26.json")
    s3_obj_mock_1 = create_s3_object_mock(vig_app, local_file_1, FILE_TYPE, DATA_SET)
    local_file_2 = BR_DAILY_FOLDER.joinpath("bbref_games_for_date_2017-05-27.json")
    s3_obj_mock_2 = create_s3_object_mock(vig_app, local_file_2, FILE_TYPE, DATA_SET)
    local_file_3 = BR_DAILY_FOLDER.joinpath("bbref_games_for_date_2017-09-15.json")
    s3_obj_mock_3 = create_s3_object_mock(vig_app, local_file_3, FILE_TYPE, DATA_SET)
    return [s3_obj_mock_1, s3_obj_mock_2, s3_obj_mock_3]


def create_br_daily_patch_list_objects_mock_data(vig_app):
    FILE_TYPE = VigFile.PATCH_LIST
    DATA_SET = DataSet.BBREF_GAMES_FOR_DATE
    local_file_1 = BR_DAILY_FOLDER.joinpath("bbref_games_for_date_2017-09-15_PATCH_LIST.json")
    # File in local folder is newer than the version in S3
    s3_obj_mock_1 = create_s3_object_mock(
        vig_app, local_file_1, FILE_TYPE, DATA_SET, mod_size=125, mod_mtime=timedelta(days=-5)
    )
    return [s3_obj_mock_1]


def create_bb_daily_objects_mock_data(vig_app):
    FILE_TYPE = VigFile.PARSED_JSON
    DATA_SET = DataSet.BROOKS_GAMES_FOR_DATE
    local_file_1 = BB_DAILY_FOLDER.joinpath("brooks_games_for_date_2017-05-26.json")
    s3_obj_mock_1 = create_s3_object_mock(vig_app, local_file_1, FILE_TYPE, DATA_SET)
    return [s3_obj_mock_1]


def create_bb_daily_patch_list_objects_mock_data(vig_app):
    FILE_TYPE = VigFile.PATCH_LIST
    DATA_SET = DataSet.BROOKS_GAMES_FOR_DATE
    local_file_1 = BB_DAILY_FOLDER.joinpath("brooks_games_for_date_2017-05-26_PATCH_LIST.json")
    # File in S3 is newer than the version in local folder
    s3_obj_mock_1 = create_s3_object_mock(
        vig_app, local_file_1, FILE_TYPE, DATA_SET, mod_size=250, mod_mtime=timedelta(days=3)
    )
    return [s3_obj_mock_1]


def create_br_box_objects_mock_data(vig_app):
    FILE_TYPE = VigFile.PARSED_JSON
    DATA_SET = DataSet.BBREF_BOXSCORES
    local_file_1 = BR_BOX_FOLDER.joinpath("CHA201705260.json")
    # File in local folder is newer than the version in S3
    s3_obj_mock_1 = create_s3_object_mock(
        vig_app, local_file_1, FILE_TYPE, DATA_SET, mod_size=72042, mod_mtime=timedelta(hours=-15)
    )
    local_file_2 = BR_BOX_FOLDER.joinpath("CHA201705272.json")
    # File in S3 is newer than the version in local folder
    s3_obj_mock_2 = create_s3_object_mock(
        vig_app, local_file_2, FILE_TYPE, DATA_SET, mod_size=9423, mod_mtime=timedelta(days=1, hours=3)
    )
    return [s3_obj_mock_1, s3_obj_mock_2]


def create_bb_plog_objects_mock_data(vig_app):
    FILE_TYPE = VigFile.PARSED_JSON
    DATA_SET = DataSet.BROOKS_PITCH_LOGS
    # Both files do not exist in local folder
    s3_folder = get_s3_folderpath(vig_app, FILE_TYPE, DATA_SET)
    file1_name = "gid_2017_05_26_oakmlb_nyamlb_1.json"
    file1_size = 6496
    file1_last_mod = datetime(2021, 1, 4, 5, 22, 19, tzinfo=timezone.utc)
    s3_obj_mock_1 = S3ObjectMock(f"{s3_folder}/{file1_name}", file1_size, file1_last_mod)
    file2_name = "gid_2017_09_15_sdnmlb_colmlb_1.json"
    file2_size = 9499
    file2_last_mod = datetime(2021, 1, 2, 17, 56, 33, tzinfo=timezone.utc)
    s3_obj_mock_2 = S3ObjectMock(f"{s3_folder}/{file2_name}", file2_size, file2_last_mod)
    return [s3_obj_mock_1, s3_obj_mock_2]


def create_bb_pfx_objects_mock_data(vig_app):
    FILE_TYPE = VigFile.PARSED_JSON
    DATA_SET = DataSet.BROOKS_PITCHFX
    # Both files do not exist in local folder
    s3_folder = get_s3_folderpath(vig_app, FILE_TYPE, DATA_SET)
    file1_name = "HOU201705270_489119.json"
    file1_size = 251036
    file1_last_mod = datetime(2021, 1, 12, 7, 57, 28, 695848, tzinfo=timezone.utc)
    s3_obj_mock_1 = S3ObjectMock(f"{s3_folder}/{file1_name}", file1_size, file1_last_mod)
    file2_name = "COL201705270_572096.json"
    file2_size = 83271
    file2_last_mod = (datetime(2021, 1, 12, 7, 59, 13, 920012, tzinfo=timezone.utc),)
    s3_obj_mock_2 = S3ObjectMock(f"{s3_folder}/{file2_name}", file2_size, file2_last_mod)
    return [s3_obj_mock_1, s3_obj_mock_2]


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
