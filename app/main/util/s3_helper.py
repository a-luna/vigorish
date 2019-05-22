"""Functions for downloading from and uploading files to an Amazon S3 bucket."""
import errno
import json
import os
from dateutil import parser
from pathlib import Path
from string import Template

import boto3
import botocore

from app.main.util.decorators import measure_time
from app.main.util.dt_format_strings import DATE_ONLY
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
)
from app.main.util.result import Result
from app.main.util.string_functions import validate_bb_game_id

S3_BUCKET = "vig-data"
T_BB_DATE_FOLDER = "${year}/brooks_games_for_date"
T_BB_LOG_FOLDER = "${year}/brooks_pitch_logs"
T_BR_DATE_FOLDER = "${year}/bbref_games_for_date"
T_BR_GAME_FOLDER = "${year}/bbref_boxscore"
T_BB_DATE_KEY = "${year}/brooks_games_for_date/${filename}"
T_BB_LOG_KEY = "${year}/brooks_pitch_logs/${filename}"
T_BR_DATE_KEY = "${year}/bbref_games_for_date/${filename}"
T_BR_GAME_KEY = "${year}/bbref_boxscore/${filename}"

s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")

@measure_time
def upload_brooks_games_for_date(games_for_date, scrape_date):
    """Upload a file to S3 containing json encoded BrooksGamesForDate object."""
    result = write_brooks_games_for_date_to_file(games_for_date)
    if result.failure:
        return result
    filepath = result.value
    s3_key = Template(T_BB_DATE_KEY).substitute(
        year=scrape_date.year, filename=filepath.name
    )

    try:
        s3_client.upload_file(str(filepath), S3_BUCKET, s3_key)
        filepath.unlink()
        return Result.Ok()
    except Exception as e:
        return Result.Fail(f"Error: {repr(e)}")


@measure_time
def download_brooks_games_for_date(scrape_date, folderpath=None):
    """Download a file from S3 containing json encoded BrooksGamesForDate object."""
    folderpath = folderpath if folderpath else Path.cwd()
    date_str = scrape_date.strftime(DATE_ONLY)
    filename = Template(T_BROOKS_GAMESFORDATE_FILENAME).substitute(date=date_str)
    filepath = folderpath / filename
    s3_key = Template(T_BB_DATE_KEY).substitute(
        year=scrape_date.year, filename=filename
    )

    try:
        s3_resource.Bucket(S3_BUCKET).download_file(s3_key, str(filepath))
        return Result.Ok(filepath)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            error = f'The object "{s3_key}" does not exist.'
        else:
            error = repr(e)
        return Result.Fail(error)


@measure_time
def get_brooks_games_for_date_from_s3(scrape_date, folderpath=None, delete_file=True):
    """Retrieve BrooksGamesForDate object from json encoded file stored in S3."""
    folderpath = folderpath if folderpath else Path.cwd()
    result = download_brooks_games_for_date(scrape_date, folderpath)
    if result.failure:
        return result
    filepath = result.value
    return read_brooks_games_for_date_from_file(
        scrape_date, folderpath=filepath.parent, delete_file=True
    )


@measure_time
def get_all_brooks_dates_scraped(year):
    s3_brooks_games_folder = Template(T_BB_DATE_FOLDER).substitute(year=year)
    bucket = boto3.resource("s3").Bucket(S3_BUCKET)
    scraped_keys = [
        obj.key for obj in bucket.objects.all() if s3_brooks_games_folder in obj.key
    ]

    scraped_dates = []
    for key in scraped_keys:
        try:
            date_str = Path(key).stem.split("_")[-1]
            parsed_date = parser.parse(date_str)
            scraped_dates.append(parsed_date)
        except Exception as e:
            return Result.Fail(f"Error: {repr(e)}")
    return Result.Ok(scraped_dates)


@measure_time
def upload_brooks_pitch_logs_for_game(pitch_logs_for_game, scrape_date):
    """Upload a file to S3 containing json encoded BrooksPitchLogsForGame object."""
    result = write_brooks_pitch_logs_for_game_to_file(pitch_logs_for_game)
    if result.failure:
        return result
    filepath = result.value
    s3_key = Template(T_BB_LOG_KEY).substitute(
        year=scrape_date.year, filename=filepath.name
    )

    try:
        s3_client.upload_file(str(filepath), S3_BUCKET, s3_key)
        filepath.unlink()
        return Result.Ok()
    except Exception as e:
        return Result.Fail(f"Error: {repr(e)}")


@measure_time
def download_brooks_pitch_logs_for_game(bb_game_id, folderpath=None):
    """Download a file from S3 containing json encoded BrooksPitchLogsForGame object."""
    result = validate_bb_game_id(bb_game_id)
    if result.failure:
        return result
    game_dict = result.value
    scrape_date = game_dict["game_date"]

    folderpath = folderpath if folderpath else Path.cwd()
    filename = Template(T_BROOKS_PITCHLOGSFORGAME_FILENAME).substitute(gid=bb_game_id)
    filepath = folderpath / filename

    s3_key = Template(T_BB_LOG_KEY).substitute(year=scrape_date.year, filename=filename)
    try:
        s3_resource.Bucket(S3_BUCKET).download_file(s3_key, str(filepath))
        return Result.Ok(filepath)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            error = f'The object "{s3_key}" does not exist.'
        else:
            error = repr(e)
        return Result.Fail(error)


@measure_time
def get_brooks_pitch_logs_for_game_from_s3(
    bb_game_id, folderpath=None, delete_file=True
):
    """Retrieve BrooksPitchLogsForGame object from json encoded file stored in S3."""
    folderpath = folderpath if folderpath else Path.cwd()
    result = download_brooks_pitch_logs_for_game(bb_game_id, folderpath)
    if result.failure:
        return result
    filepath = result.value
    return read_brooks_pitch_logs_for_game_from_file(
        bb_game_id, folderpath=filepath.parent, delete_file=True
    )


@measure_time
def get_all_brooks_pitch_logs_scraped(year):
    s3_folder = Template(T_BB_LOG_FOLDER).substitute(year=year)
    bucket = boto3.resource("s3").Bucket(S3_BUCKET)
    scraped_keys = [obj.key for obj in bucket.objects.all() if s3_folder in obj.key]
    scraped_gameids = [Path(key).stem for key in scraped_keys]
    return Result.Ok(scraped_gameids)


@measure_time
def upload_bbref_games_for_date(games_for_date, scrape_date):
    """Upload a file to S3 containing json encoded BBRefGamesForDate object."""
    result = write_bbref_games_for_date_to_file(games_for_date)
    if result.failure:
        return result
    filepath = result.value
    s3_key = Template(T_BR_DATE_KEY).substitute(year=scrape_date.year, filename=filepath.name)

    try:
        s3_client.upload_file(str(filepath), S3_BUCKET, s3_key)
        filepath.unlink()
        return Result.Ok()
    except Exception as e:
        return Result.Fail(f"Error: {repr(e)}")


@measure_time
def download_bbref_games_for_date(scrape_date, folderpath=None):
    """Download a file from S3 containing json encoded BBRefGamesForDate object."""
    folderpath = folderpath if folderpath else Path.cwd()
    date_str = scrape_date.strftime(DATE_ONLY)
    filename = Template(T_BBREF_GAMESFORDATE_FILENAME).substitute(date=date_str)
    filepath = folderpath / filename
    s3_key = Template(T_BR_DATE_KEY).substitute(year=scrape_date.year, filename=filename)

    try:
        s3_resource.Bucket(S3_BUCKET).download_file(s3_key, str(filepath))
        return Result.Ok(filepath)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            error = f'The object "{s3_key}" does not exist.'
        else:
            error = repr(e)
        return Result.Fail(error)


@measure_time
def get_bbref_games_for_date_from_s3(scrape_date, folderpath=None, delete_file=True):
    """Retrieve BBRefGamesForDate object from json encoded file stored in S3."""
    folderpath = folderpath if folderpath else Path.cwd()
    result = download_bbref_games_for_date(scrape_date, folderpath)
    if result.failure:
        return result
    filepath = result.value
    return read_bbref_games_for_date_from_file(scrape_date, folderpath=filepath.parent, delete_file=True)


@measure_time
def get_all_bbref_dates_scraped(year):
    s3_folder = Template(T_BR_DATE_FOLDER).substitute(year=year)
    bucket = boto3.resource("s3").Bucket(S3_BUCKET)
    scraped_keys = [obj.key for obj in bucket.objects.all() if s3_folder in obj.key]
    scraped_dates = []
    for key in scraped_keys:
        try:
            date_str = Path(key).stem.split("_")[-1]
            parsed_date = parser.parse(date_str)
            scraped_dates.append(parsed_date)
        except Exception as e:
            return Result.Fail(f"Error: {repr(e)}")
    return Result.Ok(scraped_dates)


@measure_time
def upload_bbref_boxscore(boxscore, scrape_date):
    """Upload a file to S3 containing json encoded BBRefBoxscore object."""
    result = write_bbref_boxscore_to_file(boxscore)
    if result.failure:
        return result
    filepath = result.value
    s3_key = Template(T_BR_GAME_KEY).substitute(
        year=scrape_date.year, filename=filepath.name
    )

    try:
        s3_client.upload_file(str(filepath), S3_BUCKET, s3_key)
        filepath.unlink()
        return Result.Ok()
    except Exception as e:
        return Result.Fail(f"Error: {repr(e)}")


@measure_time
def download_bbref_boxscore(bbref_game_id, folderpath=None):
    """Download a file from S3 containing json encoded BBRefBoxscore object."""
    folderpath = folderpath if folderpath else Path.cwd()
    filename = Template(T_BBREF_BOXSCORE_FILENAME).substitute(gid=bbref_game_id)
    filepath = folderpath / filename
    if len(bbref_game_id) != 12:
        error = f"bbref_game_id is not in expected format: {bbref_game_id}"
        return Result.Fail(error)

    year = bbref_game_id[3:7]
    s3_key = Template(T_BR_GAME_KEY).substitute(year=year, filename=filename)
    try:
        s3_resource.Bucket(S3_BUCKET).download_file(s3_key, str(filepath))
        return Result.Ok(filepath)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            error = f'The object "{s3_key}" does not exist.'
        else:
            error = repr(e)
        return Result.Fail(error)


@measure_time
def get_bbref_boxscore_from_s3(bbref_game_id, folderpath=None, delete_file=True):
    """Retrieve BBRefBoxscore object from json encoded file stored in S3."""
    folderpath = folderpath if folderpath else Path.cwd()
    result = download_bbref_boxscore(bbref_game_id, folderpath)
    if result.failure:
        return result
    filepath = result.value
    return read_bbref_boxscore_from_file(
        bbref_game_id, folderpath=filepath.parent, delete_file=True
    )


@measure_time
def get_all_bbref_boxscores_scraped(year):
    s3_folder = Template(T_BR_GAME_FOLDER).substitute(year=year)
    bucket = boto3.resource("s3").Bucket(S3_BUCKET)
    scraped_keys = [obj.key for obj in bucket.objects.all() if s3_folder in obj.key]

    scraped_gameids = [Path(key).stem for key in scraped_keys]
    return Result.Ok(scraped_gameids)
