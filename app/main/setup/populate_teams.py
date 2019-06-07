"""Populate team table with initial data."""
import os

import pandas as pd
from tqdm import tqdm

from app.main.models.team import Team
from app.main.util.numeric_functions import sanitize
from app.main.util.result import Result

TEAM_CSV_FILE_PATH = os.path.join(os.path.dirname(__file__), 'csv/Teams.csv')

def populate_teams(session):
    """Populate team table with initial data."""
    result = __import_teams_csv(session)
    if result.failure:
        return result
    session.commit()
    return Result.Ok()

def __import_teams_csv(session):
    try:
        df_team = pd.read_csv(
            TEAM_CSV_FILE_PATH,
            usecols=[
                'yearID',
                'lgID',
                'teamID',
                'franchID',
                'divID',
                'G',
                'Ghome',
                'W',
                'L',
                'R',
                'AB',
                'H',
                '2B',
                '3B',
                'HR',
                'BB',
                'SO',
                'SB',
                'CS',
                'RA',
                'ER',
                'SV',
                'IPouts',
                'E',
                'name',
                'park',
                'BPF',
                'PPF',
                'teamIDBR',
                'teamIDretro'
            ]
        )
        df_team.columns = df_team.columns.str.strip()

        with tqdm(
            total=len(df_team),
            desc='Populating team table........',
            unit='row',
            mininterval=0.12,
            maxinterval=5,
            unit_scale=True
        ) as pbar:
            for _, row in df_team.iterrows():
                t = Team(
                    year=int(sanitize(row['yearID'])),
                    league=row['lgID'],
                    team_id=row['teamID'],
                    franch_id=row['franchID'],
                    division=row['divID'],
                    games=int(sanitize(row['G'])),
                    games_at_home=int(sanitize(row['Ghome'])),
                    wins=int(sanitize(row['W'])),
                    losses=int(sanitize(row['L'])),
                    runs=int(sanitize(row['R'])),
                    at_bats=int(sanitize(row['AB'])),
                    hits=int(sanitize(row['H'])),
                    doubles=int(sanitize(row['2B'])),
                    triples=int(sanitize(row['3B'])),
                    homeruns=int(sanitize(row['HR'])),
                    base_on_balls=int(sanitize(row['BB'])),
                    strikeouts=int(sanitize(row['SO'])),
                    stolen_bases=int(sanitize(row['SB'])),
                    caught_stealing=int(sanitize(row['CS'])),
                    runs_against=int(sanitize(row['RA'])),
                    earned_runs=int(sanitize(row['ER'])),
                    saves=int(sanitize(row['SV'])),
                    ip_outs=int(sanitize(row['IPouts'])),
                    errors=int(sanitize(row['E'])),
                    name=row['name'],
                    park=row['park'],
                    batting_park_factor=int(sanitize(row['BPF'])),
                    pitching_park_factor=int(sanitize(row['PPF'])),
                    team_id_br=row['teamIDBR'],
                    team_id_retro=row['teamIDretro']
                )
                session.add(t)
                pbar.update()
        return Result.Ok()
    except Exception as e:
        error = 'Error: {error}'.format(error=repr(e))
        session.rollback()
        return Result.Fail(error)
