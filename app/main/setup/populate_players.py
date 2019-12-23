"""Populate player_id and player tables with initial data."""
import os
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from app.main.models.player import Player
from app.main.models.player_id import PlayerId
from app.main.util.numeric_functions import sanitize, is_nan
from app.main.util.result import Result

PLAYER_ID_CSV = Path(__file__).resolve().parent / "csv/idmap.csv"
PLAYER_CSV = Path(__file__).resolve().parent / "csv/People.csv"

def populate_players(session):
    """Populate player_id and player tables with initial data."""
    result = __import_idmap_csv(session)
    if result.failure:
        return result
    session.commit()
    result = __import_people_csv(session)
    if result.failure:
        return result
    session.commit()

    return Result.Ok()

def __import_idmap_csv(session):
    try:
        df_ids = pd.read_csv(
            PLAYER_ID_CSV,
            usecols=[
                'mlb_id',
                'mlb_name',
                'bp_id',
                'bref_id',
                'bref_name',
                'espn_id',
                'espn_name',
                'fg_id',
                'fg_name',
                'lahman_id',
                'nfbc_id',
                'retro_id',
                'yahoo_id',
                'yahoo_name',
                'ottoneu_id',
                'rotowire_id',
                'rotowire_name'
            ]
        )
        df_ids.columns = df_ids.columns.str.strip()

        with tqdm(
            total=len(df_ids),
            desc='Populating player_id table...',
            unit='row',
            mininterval=0.12,
            maxinterval=5,
            unit_scale=True
        ) as pbar:
            for _, row in df_ids.iterrows():
                if is_nan(row['mlb_id']) or is_nan(row['bref_id']):
                    pbar.update()
                    continue
                if is_nan(row['retro_id']):
                    retro_id = None
                else:
                    retro_id = row['retro_id']

                pid = PlayerId(
                    mlb_id=int(sanitize(row['mlb_id'])),
                    mlb_name=row['mlb_name'],
                    bp_id=int(sanitize(row['bp_id'])),
                    bbref_id=row['bref_id'],
                    bbref_name=row['bref_name'],
                    espn_id=int(sanitize(row['espn_id'])),
                    espn_name=row['espn_name'],
                    fg_id=row['fg_id'],
                    fg_name=row['fg_name'],
                    lahman_id=row['lahman_id'],
                    nfbc_id=int(sanitize(row['nfbc_id'])),
                    retro_id=retro_id,
                    yahoo_id=int(sanitize(row['yahoo_id'])),
                    yahoo_name=row['yahoo_name'],
                    ottoneu_id=int(sanitize(row['ottoneu_id'])),
                    rotowire_id=int(sanitize(row['rotowire_id'])),
                    rotowire_name=row['rotowire_name']
                )
                session.add(pid)
                pbar.update()
        return Result.Ok()
    except Exception as e:
        error = 'Error: {error}'.format(error=repr(e))
        session.rollback()
        return Result.Fail(error)

def __import_people_csv(session):
    try:
        df_player = pd.read_csv(
            PLAYER_CSV,
            usecols=[
                'nameFirst',
                'nameLast',
                'nameGiven',
                'bats',
                'throws',
                'weight',
                'height',
                'debut',
                'birthYear',
                'birthMonth',
                'birthDay',
                'birthCountry',
                'birthState',
                'birthCity',
                'playerID',
                'retroID',
                'bbrefID'
            ]
        )
        df_player.columns = df_player.columns.str.strip()

        with tqdm(
            total=len(df_player),
            desc='Populating player table......',
            unit='row',
            mininterval=0.12,
            maxinterval=5,
            unit_scale=True
        ) as pbar:
            for _, row in df_player.iterrows():
                if is_nan(row['birthYear'])\
                or is_nan(row['birthMonth'])\
                or is_nan(row['birthDay']):
                    pbar.update()
                    continue

                player_id = PlayerId.find_by_retro_id(session, str(row['retroID']))
                if not player_id:
                    player_id = PlayerId.find_by_bbref_id(session, str(row['bbrefID']))
                if not player_id:
                    pbar.update()
                    continue

                p = Player(
                    mlb_id=player_id.mlb_id,
                    retro_id=row['retroID'],
                    bbref_id=row['bbrefID'],
                    name_first=row['nameFirst'],
                    name_last=row['nameLast'],
                    name_given=row['nameGiven'],
                    bats=row['bats'],
                    throws=row['throws'],
                    weight=int(sanitize(row['weight'])),
                    height=int(sanitize(row['height'])),
                    debut=row['debut'],
                    birth_year=int(sanitize(row['birthYear'])),
                    birth_month=int(sanitize(row['birthMonth'])),
                    birth_day=int(sanitize(row['birthDay'])),
                    birth_country=row['birthCountry'],
                    birth_state=row['birthState'],
                    birth_city=row['birthCity'],
                    missing_mlb_id=False
                )
                session.add(p)
                pbar.update()
        return Result.Ok()
    except Exception as e:
        error = 'Error: {error}'.format(error=repr(e))
        session.rollback()
        return Result.Fail(error)
