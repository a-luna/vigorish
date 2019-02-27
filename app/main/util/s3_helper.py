"""Functions for downloading from and uploading files to an Amazon S3 bucket."""
import errno
import json
import os
from pathlib import Path
from string import Template

import boto3
import botocore

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
    write_bbref_boxscore_to_file
)

S3_BUCKET = 'vig-data'
T_BROOKS_GAMESFORDATE_KEY = '${year}/brooks_games_for_date/${filename}'
T_BBREF_GAMESFORDATE_KEY = '${year}/bbref_games_for_date/${filename}'
T_BBREF_BOXSCORE_KEY = '${year}/bbref_boxscore/${filename}'

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

def upload_brooks_games_for_date(games_for_date, scrape_date):
    """Upload a file to S3 containing json encoded BrooksGamesForDate object."""
    result = write_brooks_games_for_date_to_file(games_for_date)
    if not result['success']:
        return result
    filepath = result['filepath']
    s3_key = Template(T_BROOKS_GAMESFORDATE_KEY)\
        .substitute(year=scrape_date.year, filename=filepath.name)

    try:
        s3_client.upload_file(str(filepath), S3_BUCKET, s3_key)
        filepath.unlink()
        return dict(success=True)
    except Exception as e:
        error = 'Error: {error}'.format(error=repr(e))
        return dict(success=False, message=error)

def download_brooks_games_for_date(scrape_date, folderpath=None):
    """Download a file from S3 containing json encoded BrooksGamesForDate object."""
    folderpath = folderpath if folderpath else Path.cwd()
    date_str = scrape_date.strftime(DATE_ONLY)
    filename = Template(T_BROOKS_GAMESFORDATE_FILENAME)\
        .substitute(date=date_str)
    filepath = folderpath / filename

    s3_key = Template(T_BROOKS_GAMESFORDATE_KEY)\
        .substitute(year=scrape_date.year, filename=filename)
    try:
        s3_resource.Bucket(S3_BUCKET).download_file(s3_key, str(filepath))
        return dict(success=True, filepath=filepath)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object '{f}' does not exist.".format(f=s3_key))
        else:
            raise
        return False

def get_brooks_games_for_date_from_s3(scrape_date, folderpath=None, delete_file=True):
    """Retrieve BrooksGamesForDate object from json encoded file stored in S3."""
    folderpath = folderpath if folderpath else Path.cwd()
    result = download_brooks_games_for_date(scrape_date, folderpath)
    if not result['success']:
        return result
    filepath = result['filepath']
    return read_brooks_games_for_date_from_file(
        scrape_date,
        folderpath=filepath.parent,
        delete_file=True
    )

def upload_bbref_games_for_date(games_for_date, scrape_date):
    """Upload a file to S3 containing json encoded BBRefGamesForDate object."""
    result = write_bbref_games_for_date_to_file(games_for_date)
    if not result['success']:
        return result
    filepath = result['filepath']
    s3_key = Template(T_BBREF_GAMESFORDATE_KEY)\
        .substitute(year=scrape_date.year, filename=filepath.name)

    try:
        s3_client.upload_file(str(filepath), S3_BUCKET, s3_key)
        filepath.unlink()
        return dict(success=True)
    except Exception as e:
        error = 'Error: {error}'.format(error=repr(e))
        return dict(success=False, message=error)

def download_bbref_games_for_date(scrape_date, folderpath=None):
    """Download a file from S3 containing json encoded BBRefGamesForDate object."""
    folderpath = folderpath if folderpath else Path.cwd()
    date_str = scrape_date.strftime(DATE_ONLY)
    filename = Template(T_BBREF_GAMESFORDATE_FILENAME)\
        .substitute(date=date_str)
    filepath = folderpath / filename

    s3_key = Template(T_BBREF_GAMESFORDATE_KEY)\
        .substitute(year=scrape_date.year, filename=filename)
    try:
        s3_resource.Bucket(S3_BUCKET).download_file(s3_key, str(filepath))
        return dict(success=True, filepath=filepath)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object '{f}' does not exist.".format(f=s3_key))
        else:
            raise
        return False

def get_bbref_games_for_date_from_s3(scrape_date, folderpath=None, delete_file=True):
    """Retrieve BBRefGamesForDate object from json encoded file stored in S3."""
    folderpath = folderpath if folderpath else Path.cwd()
    result = download_bbref_games_for_date(scrape_date, folderpath)
    if not result['success']:
        return result
    filepath = result['filepath']
    return read_bbref_games_for_date_from_file(
        scrape_date,
        folderpath=filepath.parent,
        delete_file=True
    )

def upload_bbref_boxscore(boxscore, scrape_date):
    """Upload a file to S3 containing json encoded BBRefBoxscore object."""
    result = write_bbref_boxscore_to_file(boxscore)
    if not result['success']:
        return result
    filepath = result['filepath']
    s3_key = Template(T_BBREF_BOXSCORE_KEY)\
        .substitute(year=scrape_date.year, filename=filepath.name)

    try:
        s3_client.upload_file(str(filepath), S3_BUCKET, s3_key)
        filepath.unlink()
        return dict(success=True)
    except Exception as e:
        error = 'Error: {error}'.format(error=repr(e))
        return dict(success=False, message=error)

def download_bbref_boxscore(bbref_game_id, folderpath=None):
    """Download a file from S3 containing json encoded BBRefBoxscore object."""
    folderpath = folderpath if folderpath else Path.cwd()
    filename = Template(T_BBREF_BOXSCORE_FILENAME)\
        .substitute(gid=bbref_game_id)
    filepath = folderpath / filename
    if len(bbref_game_id) != 12:
        error = f'bbref_game_id is not in expected format: {bbref_game_id}'
        return dict(success=False, message=error)
    year = bbref_game_id[3:7]

    s3_key = Template(T_BBREF_BOXSCORE_KEY)\
        .substitute(year=year, filename=filename)
    try:
        s3_resource.Bucket(S3_BUCKET).download_file(s3_key, str(filepath))
        return dict(success=True, filepath=filepath)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object '{f}' does not exist.".format(f=s3_key))
        else:
            raise
        return False

def get_bbref_boxscore_from_s3(bbref_game_id, folderpath=None, delete_file=True):
    """Retrieve BBRefBoxscore object from json encoded file stored in S3."""
    folderpath = folderpath if folderpath else Path.cwd()
    result = download_bbref_boxscore(bbref_game_id, folderpath)
    if not result['success']:
        return result
    filepath = result['filepath']
    return read_bbref_boxscore_from_file(
        bbref_game_id,
        folderpath=filepath.parent,
        delete_file=True
    )
