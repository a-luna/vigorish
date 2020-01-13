"""Functions for downloading from and uploading files to an Amazon S3 bucket."""
import errno
import json
import os
from datetime import datetime
from dateutil import parser
from enum import Enum, auto
from pathlib import Path
from string import Template

import boto3
from botocore.exceptions import ClientError

from app.main.constants import DataSet
from app.main.models.status_date import DateScrapeStatus
from app.main.models.status_pitch_appearance import PitchAppearanceScrapeStatus
from app.main.util.dt_format_strings import DATE_ONLY, DATE_ONLY_2, DATE_ONLY_TABLE_ID
from app.main.util.file_util import (
    T_BROOKS_GAMESFORDATE_FILENAME,
    read_brooks_games_for_date_from_file,
    write_brooks_games_for_date_to_file,
    T_BBREF_GAMESFORDATE_FILENAME,
    read_bbref_games_for_date_from_file,
    write_bbref_games_for_date_to_file,
    T_BBREF_BOXSCORE_FILENAME,
    read_bbref_boxscore_from_file,
    write_bbref_boxscore_to_file,
    T_BROOKS_PITCHLOGSFORGAME_FILENAME,
    read_brooks_pitch_logs_for_game_from_file,
    write_brooks_pitch_logs_for_game_to_file,
    T_BROOKS_PITCHFXLOG_FILENAME,
    read_brooks_pitchfx_log_from_file,
    write_brooks_pitchfx_log_to_file
)
from app.main.util.regex import (
    BR_DAILY_KEY_REGEX,
    BR_GAME_KEY_REGEX,
    BB_DAILY_KEY_REGEX,
    PITCH_APP_REGEX,
)
from app.main.util.result import Result
from app.main.util.string_functions import validate_bb_game_id, validate_bbref_game_id

JSON_FOLDER_KEY_TEMPLATE = "${year}/${data_set}"
JSON_FILE_KEY_TEMPLATE = "${year}/${data_set/${filename}"
HTML_FOLDER_KEY_TEMPLATE = "${year}/${data_set}/html"
HTML_FILE_KEY_TEMPLATE = "${year}/${data_set/html/${filename}"

T_BB_DATE_FOLDER = "${year}/brooks_games_for_date"
T_BB_DATE_KEY = "${year}/brooks_games_for_date/${filename}"
T_BB_DATE_HTML_FOLDER = "${year}/brooks_games_for_date/html"
T_BB_DATE_HTML_KEY = "${year}/brooks_games_for_date/html/${filename}"

T_BB_LOG_FOLDER = "${year}/brooks_pitch_logs"
T_BB_LOG_KEY = "${year}/brooks_pitch_logs/${filename}"
T_BB_LOG_HTML_FOLDER = "${year}/brooks_pitch_logs/html"
T_BB_LOG_HTML_KEY = "${year}/brooks_pitch_logs/html/${filename}"

T_BB_PFX_FOLDER = "${year}/brooks_pitchfx"
T_BB_PFX_KEY = "${year}/brooks_pitchfx/${filename}"
T_BB_PFX_HTML_FOLDER = "${year}/brooks_pitchfx/html"
T_BB_PFX_HTML_KEY = "${year}/brooks_pitchfx/html/${filename}"

T_BR_DATE_FOLDER = "${year}/bbref_games_for_date"
T_BR_DATE_KEY = "${year}/bbref_games_for_date/${filename}"
T_BR_DATE_HTML_FOLDER = "${year}/bbref_games_for_date/html"
T_BR_DATE_HTML_KEY = "${year}/bbref_games_for_date/html/${filename}"

T_BR_GAME_FOLDER = "${year}/bbref_boxscore"
T_BR_GAME_KEY = "${year}/bbref_boxscore/${filename}"
T_BR_GAME_HTML_FOLDER = "${year}/bbref_boxscore/html"
T_BR_GAME_HTML_KEY = "${year}/bbref_boxscore/html/${filename}"

S3_BUCKET = "vig-data"
s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")
bucket = s3_resource.Bucket(S3_BUCKET)

class S3Task(Enum):
    UPLOAD = auto()
    DOWNLOAD = auto()
    DELETE = auto()
    RENAME = auto()

class DocFormat(Enum):
    JSON = auto()
    HTML = auto()

KEY_TEMPLATE_DICT = {
    DocFormat.HTML: HTML_FILE_KEY_TEMPLATE,
    DocFormat.JSON: JSON_FILE_KEY_TEMPLATE
}


def get_brooks_games_for_date_from_s3(scrape_date, folderpath=None, delete_file=True):
    """Retrieve BrooksGamesForDate object from json encoded file stored in S3."""
    folderpath = folderpath if folderpath else Path.cwd()
    result = download_json_brooks_games_for_date(scrape_date, folderpath)
    if result.failure:
        return result
    filepath = result.value
    return read_brooks_games_for_date_from_file(
        scrape_date, folderpath=filepath.parent, delete_file=delete_file
    )


def get_brooks_pitch_logs_for_game_from_s3(bb_game_id, folderpath=None, delete_file=True):
    """Retrieve BrooksPitchLogsForGame object from json encoded file stored in S3."""
    folderpath = folderpath if folderpath else Path.cwd()
    result = download_json_brooks_pitch_logs_for_game(bb_game_id, folderpath)
    if result.failure:
        return result
    filepath = result.value
    return read_brooks_pitch_logs_for_game_from_file(
        bb_game_id,
        folderpath=filepath.parent,
        delete_file=delete_file)


def get_all_brooks_pitch_logs_for_date_from_s3(session, game_date, folderpath=None, delete_file=True):
    """Retrieve a list of BrooksPitchLogsForGame objects for all games that occurred on a date."""
    brooks_game_ids = DateScrapeStatus.get_all_brooks_game_ids_for_date(session, game_date)
    pitch_logs = []
    for game_id in brooks_game_ids:
        result = get_brooks_pitch_logs_for_game_from_s3(game_id, folderpath, delete_file)
        if result.failure:
            continue
        pitch_logs.append(result.value)
    return Result.Ok(pitch_logs)


def get_brooks_pitchfx_log_from_s3(pitch_app_id, folderpath=None, delete_file=True):
    """Retrieve BrooksPitchFxLog object from json encoded file stored in S3."""
    match = PITCH_APP_REGEX.search(pitch_app_id)
    if not match:
        return Result.Fail(f"pitch_app_id: {pitch_app_id} is invalid")
    id_dict = match.groupdict()
    result = validate_bbref_game_id(id_dict["game_id"])
    if result.failure:
        return result
    game_date = result.value["game_date"]
    folderpath = folderpath if folderpath else Path.cwd()
    result = download_json_brooks_pitchfx_log(pitch_app_id, game_date.year, folderpath)
    if result.failure:
        return result
    filepath = result.value
    return read_brooks_pitchfx_log_from_file(
        pitch_app_id,
        folderpath=filepath.parent,
        delete_file=delete_file)


def get_all_pitchfx_logs_for_game_from_s3(session, bbref_game_id):
    pitch_app_ids = PitchAppearanceScrapeStatus.get_all_pitch_app_ids_for_game(session, bbref_game_id)
    fetch_tasks = [get_brooks_pitchfx_log_from_s3(pitch_app_id) for pitch_app_id in pitch_app_ids]
    task_failed = any(result.failure for result in fetch_tasks)
    if task_failed:
        s3_errors = "\n".join([f"Error: {result.error}" for result in fetch_tasks if result.failure])
        error = f"The following errors occurred attempting to retrieve all pitchfx logs for game {bbref_game_id}:\n{s3_errors}"
        return Result.Fail(error)
    pitchfx_logs = [result.value for result in fetch_tasks]
    return Result.Ok(pitchfx_logs)


def get_bbref_games_for_date_from_s3(scrape_date, folderpath=None, delete_file=True):
    """Retrieve BBRefGamesForDate object from json encoded file stored in S3."""
    folderpath = folderpath if folderpath else Path.cwd()
    result = download_json_bbref_games_for_date(scrape_date, folderpath)
    if result.failure:
        return result
    filepath = result.value
    return read_bbref_games_for_date_from_file(scrape_date, folderpath=filepath.parent, delete_file=delete_file)


def get_bbref_boxscore_from_s3(bbref_game_id, folderpath=None, delete_file=True):
    """Retrieve BBRefBoxscore object from json encoded file stored in S3."""
    folderpath = folderpath if folderpath else Path.cwd()
    result = download_json_bbref_boxscore(bbref_game_id, folderpath)
    if result.failure:
        return result
    filepath = result.value
    return read_bbref_boxscore_from_file(bbref_game_id, folderpath=filepath.parent, delete_file=delete_file)


def get_all_brooks_dates_scraped_from_s3_test(year):
    json_folder = Template(JSON_FOLDER_KEY_TEMPLATE).substitute(
        year=year, data_set=DataSet.brooks_games_for_date
    )
    html_folder = Template(HTML_FOLDER_KEY_TEMPLATE).substitute(
        year=year, data_set=DataSet.brooks_games_for_date
    )
    scraped_dates = [
        parser.parse(Path(obj.key).stem[-10:])
        for obj in bucket.objects.all()
        if json_folder in obj.key
        and html_folder not in obj.key
    ]
    return Result.Ok(scraped_dates)


def get_all_brooks_dates_scraped_from_s3(year):
    s3_folder = Template(JSON_FOLDER_KEY_TEMPLATE).substitute(
        data_set=DataSet.brooks_games_for_date, year=year
    )
    scraped_keys = [obj.key for obj in bucket.objects.all() if s3_folder in obj.key]
    scraped_dates = []
    for key in scraped_keys:
        try:
            match = BB_DAILY_KEY_REGEX.search(key)
            if not match:
                continue
            group_dict = match.groupdict()
            game_date = parser.parse(group_dict['date_str'])
            scraped_dates.append(game_date)
        except Exception as e:
            return Result.Fail(f"Error: {repr(e)}")
    return Result.Ok(scraped_dates)


def get_all_scraped_brooks_game_ids(year):
    s3_folder = Template(T_BB_LOG_FOLDER).substitute(year=year)
    scraped_keys = [obj.key for obj in bucket.objects.all() if s3_folder in obj.key]
    scraped_gameids = [Path(key).stem for key in scraped_keys]
    return Result.Ok(scraped_gameids)


def get_all_pitch_app_ids_scraped(year):
    s3_folder = Template(T_BB_PFX_FOLDER).substitute(year=year)
    scraped_keys = [obj.key for obj in bucket.objects.all() if s3_folder in obj.key]
    scraped_pitch_app_ids = [Path(key).stem for key in scraped_keys]
    return Result.Ok(scraped_pitch_app_ids)


def get_all_bbref_dates_scraped(year):
    s3_folder = Template(T_BR_DATE_FOLDER).substitute(year=year)
    scraped_keys = [obj.key for obj in bucket.objects.all() if s3_folder in obj.key]
    scraped_dates = []
    for key in scraped_keys:
        try:
            match = BR_DAILY_KEY_REGEX.search(key)
            if not match:
                continue
            group_dict = match.groupdict()
            game_date = parser.parse(group_dict['date_str'])
            scraped_dates.append(game_date)
        except Exception as e:
            return Result.Fail(f"Error: {repr(e)}")
    return Result.Ok(scraped_dates)


def get_all_scraped_bbref_game_ids(year):
    s3_folder = Template(T_BR_GAME_FOLDER).substitute(year=year)
    scraped_keys = [obj.key for obj in bucket.objects.all() if s3_folder in obj.key]
    scraped_gameids = []
    for key in scraped_keys:
        match = BR_GAME_KEY_REGEX.search(key)
        if not match:
            continue
        group_dict = match.groupdict()
        scraped_gameids.append(group_dict["game_id"])
    return Result.Ok(scraped_gameids)


def upload_brooks_games_for_date(games_for_date):
    """Upload a file to S3 containing json encoded BrooksGamesForDate object."""
    result = write_brooks_games_for_date_to_file(games_for_date)
    if result.failure:
        return result
    filepath = result.value
    game_date = games_for_date.game_date
    return perform_task(
        task=S3Task.UPLOAD,
        format=DocFormat.JSON,
        data_set=DataSet.brooks_games_for_date,
        year=game_date.year,
        filepath=filepath
    )


def upload_brooks_pitch_logs_for_game(pitch_logs_for_game):
    """Upload a file to S3 containing json encoded BrooksPitchLogsForGame object."""
    result = write_brooks_pitch_logs_for_game_to_file(pitch_logs_for_game)
    if result.failure:
        return result
    filepath = result.value
    game_date = pitch_logs_for_game.game_date
    return perform_task(
        task=S3Task.UPLOAD,
        format=DocFormat.JSON,
        data_set=DataSet.brooks_pitch_logs,
        year=game_date.year,
        filepath=filepath
    )


def upload_brooks_pitchfx_log(pitchfx_log):
    """Upload a file to S3 containing json encoded BrooksPitchFxLog object."""
    result = write_brooks_pitchfx_log_to_file(pitchfx_log)
    if result.failure:
        return result
    filepath = result.value
    game_date = pitchfx_log.game_date
    return perform_task(
        task=S3Task.UPLOAD,
        format=DocFormat.JSON,
        data_set=DataSet.brooks_pitchfx,
        year=game_date.year,
        filepath=filepath
    )


def upload_bbref_games_for_date(games_for_date):
    """Upload a file to S3 containing json encoded BBRefGamesForDate object."""
    result = write_bbref_games_for_date_to_file(games_for_date)
    if result.failure:
        return result
    filepath = result.value
    game_date = games_for_date.game_date
    return perform_task(
        task=S3Task.UPLOAD,
        format=DocFormat.JSON,
        data_set=DataSet.bbref_games_for_date,
        year=game_date.year,
        filepath=filepath
    )


def upload_bbref_boxscore(boxscore):
    """Upload a file to S3 containing json encoded BBRefBoxscore object."""
    result = write_bbref_boxscore_to_file(boxscore)
    if result.failure:
        return result
    filepath = result.value
    game_date = boxscore.game_date
    return perform_task(
        task=S3Task.UPLOAD,
        format=DocFormat.JSON,
        data_set=DataSet.bbref_boxscore,
        year=game_date.year,
        filepath=filepath
    )


def download_html_brooks_games_for_date(scrape_date, folderpath=None):
    """Download raw HTML for brooks daily scoreboard page."""
    folderpath = folderpath if folderpath else Path.cwd()
    date_str = scrape_date.strftime(DATE_ONLY_TABLE_ID)
    filename = f"{date_str}.html"
    filepath = folderpath / filename
    return perform_task(
        task=S3Task.DOWNLOAD,
        format=DocFormat.HTML,
        data_set=DataSet.brooks_games_for_date,
        year=scrape_date.year,
        filepath=filepath
    )


def download_json_brooks_games_for_date(scrape_date, folderpath=None):
    """Download a file from S3 containing json encoded BrooksGamesForDate object."""
    folderpath = folderpath if folderpath else Path.cwd()
    date_str = scrape_date.strftime(DATE_ONLY)
    filename = Template(T_BROOKS_GAMESFORDATE_FILENAME).substitute(date=date_str)
    filepath = folderpath / filename
    return perform_task(
        task=S3Task.DOWNLOAD,
        format=DocFormat.JSON,
        data_set=DataSet.brooks_games_for_date,
        year=scrape_date.year,
        filepath=filepath
    )


def download_html_brooks_pitch_log_page(pitch_app_id, folderpath=None):
    """Download raw HTML for brooks pitch log page for a single pitching appearance."""
    split = pitch_app_id.split("_")
    if split.length != 2:
        error = f"Pitch appearance ID is not formatted correctly ({pitch_app_id})"
        return Result.Fail(error)
    year = split[0][3:7]
    filename = f"{pitch_app_id}.html"
    filepath = folderpath / filename
    return perform_task(
        task=S3Task.DOWNLOAD,
        format=DocFormat.HTML,
        data_set=DataSet.brooks_pitch_logs,
        year=year,
        filepath=filepath
    )


def download_json_brooks_pitch_logs_for_game(bb_game_id, folderpath=None):
    """Download a file from S3 containing json encoded BrooksPitchLogsForGame object."""
    result = validate_bb_game_id(bb_game_id)
    if result.failure:
        return result
    game_dict = result.value
    game_date = game_dict["game_date"]
    folderpath = folderpath if folderpath else Path.cwd()
    filename = Template(T_BROOKS_PITCHLOGSFORGAME_FILENAME).substitute(gid=bb_game_id)
    filepath = folderpath / filename
    return perform_task(
        task=S3Task.DOWNLOAD,
        format=DocFormat.JSON,
        data_set=DataSet.brooks_pitch_logs,
        year=game_date.year,
        filepath=filepath
    )


def download_html_brooks_pitchfx_log(pitch_app_id, folderpath=None):
    """Download raw HTML for brooks pitchfx data for a single pitching appearance."""
    split = pitch_app_id.split("_")
    if split.length != 2:
        error = f"Pitch appearance ID is not formatted correctly ({pitch_app_id})"
        return Result.Fail(error)
    year = split[0][3:7]
    folderpath = folderpath if folderpath else Path.cwd()
    filename = f"{pitch_app_id}.html"
    filepath = folderpath / filename
    return perform_task(
        task=S3Task.DOWNLOAD,
        format=DocFormat.HTML,
        data_set=DataSet.brooks_pitchfx,
        year=year,
        filepath=filepath
    )


def download_json_brooks_pitchfx_log(pitch_app_id, year, folderpath=None):
    """Download a file from S3 containing json encoded BrooksPitchFxLog object."""
    split = pitch_app_id.split("_")
    if split.length != 2:
        error = f"Pitch appearance ID is not formatted correctly ({pitch_app_id})"
        return Result.Fail(error)
    year = split[0][3:7]
    folderpath = folderpath if folderpath else Path.cwd()
    filename = Template(T_BROOKS_PITCHFXLOG_FILENAME).substitute(pid=pitch_app_id)
    filepath = folderpath / filename
    return perform_task(
        task=S3Task.DOWNLOAD,
        format=DocFormat.JSON,
        data_set=DataSet.brooks_pitchfx,
        year=year,
        filepath=filepath
    )


def download_html_bbref_games_for_date(scrape_date, folderpath=None):
    """Download raw HTML for bbref daily scoreboard page."""
    folderpath = folderpath if folderpath else Path.cwd()
    date_str = scrape_date.strftime(DATE_ONLY_TABLE_ID)
    filename = f"{date_str}.html"
    filepath = folderpath / filename
    return perform_task(
        task=S3Task.DOWNLOAD,
        format=DocFormat.HTML,
        data_set=DataSet.bbref_games_for_date,
        year=scrape_date.year,
        filepath=filepath
    )


def download_json_bbref_games_for_date(scrape_date, folderpath=None):
    """Download a file from S3 containing json encoded BBRefGamesForDate object."""
    folderpath = folderpath if folderpath else Path.cwd()
    date_str = scrape_date.strftime(DATE_ONLY)
    filename = Template(T_BBREF_GAMESFORDATE_FILENAME).substitute(date=date_str)
    filepath = folderpath / filename
    return perform_task(
        task=S3Task.DOWNLOAD,
        format=DocFormat.JSON,
        data_set=DataSet.bbref_games_for_date,
        year=scrape_date.year,
        filepath=filepath
    )


def download_html_bbref_boxscore(bbref_game_id, folderpath=None):
    """Download raw HTML for bbref daily scoreboard page."""
    folderpath = folderpath if folderpath else Path.cwd()
    filename = f"{bbref_game_id}.html"
    filepath = folderpath / filename
    year = bbref_game_id[3:7]
    return perform_task(
        task=S3Task.DOWNLOAD,
        format=DocFormat.HTML,
        data_set=DataSet.bbref_boxscore,
        year=year,
        filepath=filepath
    )


def download_json_bbref_boxscore(bbref_game_id, folderpath=None):
    """Download a file from S3 containing json encoded BBRefBoxscore object."""
    folderpath = folderpath if folderpath else Path.cwd()
    filename = Template(T_BBREF_BOXSCORE_FILENAME).substitute(gid=bbref_game_id)
    filepath = folderpath / filename
    if len(bbref_game_id) != 12:
        error = f"bbref_game_id is not in expected format: {bbref_game_id}"
        return Result.Fail(error)

    year = bbref_game_id[3:7]
    return perform_task(
        task=S3Task.DOWNLOAD,
        format=DocFormat.JSON,
        data_set=DataSet.bbref_boxscore,
        year=year,
        filepath=filepath
    )


def delete_brooks_games_for_date_from_s3(date):
    """Delete brooks_games_for_date from s3."""
    date_str = date.strftime(DATE_ONLY)
    filename = Template(T_BROOKS_GAMESFORDATE_FILENAME).substitute(date=date_str)
    return perform_task(
        task=S3Task.DELETE,
        format=DocFormat.JSON,
        data_set=DataSet.brooks_games_for_date,
        year=date.year,
        filepath=Path(filename)
    )


def delete_brooks_pitch_logs_for_game_from_s3(bb_game_id):
    """Delete brooks pitch logs for game from s3."""
    result = validate_bb_game_id(bb_game_id)
    if result.failure:
        return result
    game_date = result.value["game_date"]
    filename = Template(T_BROOKS_PITCHLOGSFORGAME_FILENAME).substitute(gid=bb_game_id)
    return perform_task(
        task=S3Task.DELETE,
        format=DocFormat.JSON,
        data_set=DataSet.brooks_pitch_logs,
        year=game_date.year,
        filepath=Path(filename)
    )


def delete_brooks_pitchfx_log_from_s3(pitch_app_id):
    """Delete a pitchfx log from s3 based on the pitch_app_id value."""
    match = PITCH_APP_REGEX.search(pitch_app_id)
    if not match:
        return Result.Fail(f"pitch_app_id: {pitch_app_id} is invalid")
    id_dict = match.groupdict()
    result = validate_bbref_game_id(id_dict["game_id"])
    if result.value:
        return result
    game_id_dict = result.value
    game_date = game_id_dict["game_date"]
    filename = Template(T_BROOKS_PITCHFXLOG_FILENAME).substitute(pid=pitch_app_id)
    return perform_task(
        task=S3Task.DELETE,
        format=DocFormat.JSON,
        data_set=DataSet.brooks_pitchfx,
        year=game_date.year,
        filepath=Path(filename)
    )


def delete_bbref_games_for_date_from_s3(scrape_date):
    """Delete bbref_games_for_date from s3."""
    date_str = scrape_date.strftime(DATE_ONLY)
    filename = Template(T_BBREF_GAMESFORDATE_FILENAME).substitute(date=date_str)
    return perform_task(
        task=S3Task.DELETE,
        format=DocFormat.JSON,
        data_set=DataSet.bbref_games_for_date,
        year=scrape_date.year,
        filepath=Path(filename)
    )


def delete_bbref_boxscore_from_s3(bbref_game_id):
    """Delete a bbref boxscore from s3."""
    result = validate_bbref_game_id(bbref_game_id)
    if result.failure:
        return result
    game_date = result.value['game_date']
    filename = Template(T_BBREF_BOXSCORE_FILENAME).substitute(gid=bbref_game_id)
    return perform_task(
        task=S3Task.DELETE,
        format=DocFormat.JSON,
        data_set=DataSet.bbref_boxscore,
        year=game_date.year,
        filepath=Path(filename)
    )


def rename_brooks_pitchfx_log(old_pitch_app_id, new_pitch_app_id, year):
    old_filename = Template(T_BROOKS_PITCHFXLOG_FILENAME).substitute(pid=old_pitch_app_id)
    old_key = Template(T_BB_PFX_KEY).substitute(year=year, filename=old_filename)
    new_filename = Template(T_BROOKS_PITCHFXLOG_FILENAME).substitute(pid=new_pitch_app_id)
    new_key = Template(T_BB_PFX_KEY).substitute(year=year, filename=new_filename)
    return rename_s3_object(old_key, new_key)


def perform_task(task, format, data_set, year, filepath):
    s3_key = Template(KEY_TEMPLATE_DICT[format]).substitute(
        data_set=str(data_set), year=year, filename=filepath.name
    )
    if task == S3Task.UPLOAD:
        return upload_to_s3(s3_key, filepath)
    if task == S3Task.DOWNLOAD:
        return download_from_s3(s3_key, filepath)
    if task == S3Task.DELETE:
        return delete_from_s3(s3_key)


def upload_to_s3(s3_key, filepath):
    """Upload the specified file to S3. """
    try:
        s3_client.upload_file(str(filepath), S3_BUCKET, s3_key)
        filepath.unlink()
        return Result.Ok()
    except Exception as e:
        return Result.Fail(f"Error: {repr(e)}")


def download_from_s3(s3_key, filepath):
    try:
        s3_resource.Bucket(S3_BUCKET).download_file(s3_key, str(filepath))
        return Result.Ok(filepath)
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            error = f'The object "{s3_key}" does not exist.'
        else:
            error = repr(e)
        return Result.Fail(error)


def delete_from_s3(s3_key):
    try:
        s3_resource.Object(S3_BUCKET, s3_key).delete()
        return Result.Ok()
    except ClientError as e:
        return Result.Fail(repr(e))


def rename_s3_object(old_key, new_key):
    try:
        s3_resource.Object(S3_BUCKET, new_key).copy_from(CopySource=f"{S3_BUCKET}/{old_key}")
        s3_resource.Object(S3_BUCKET, old_key).delete()
        return Result.Ok()
    except ClientError as e:
        return Result.Fail(repr(e))
