"""Populate team table with initial data."""
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from vigorish.models.team import Team
from vigorish.util.numeric_helpers import sanitize
from vigorish.util.result import Result

TEAM_CSV_FILE_PATH = Path(__file__).parent / "csv" / "Teams.csv"


def populate_teams(db_session):
    """Populate team table with initial data."""
    result = import_teams_csv(db_session)
    if result.failure:
        return result
    db_session.commit()
    return Result.Ok()


def import_teams_csv(db_session):
    try:
        df_team = pd.read_csv(
            TEAM_CSV_FILE_PATH,
            usecols=[
                "yearID",
                "lgID",
                "teamID",
                "franchID",
                "divID",
                "G",
                "Ghome",
                "W",
                "L",
                "R",
                "AB",
                "H",
                "2B",
                "3B",
                "HR",
                "BB",
                "SO",
                "SB",
                "CS",
                "RA",
                "ER",
                "SV",
                "IPouts",
                "E",
                "name",
                "park",
                "BPF",
                "PPF",
                "teamIDBR",
                "teamIDretro",
            ],
        )
        df_team.columns = df_team.columns.str.strip()

        with tqdm(
            total=len(df_team),
            desc="Populating team table........",
            unit="row",
            mininterval=0.12,
            maxinterval=5,
            unit_scale=True,
            ncols=90,
        ) as pbar:
            for _, row in df_team.iterrows():
                t = Team(
                    year=int(sanitize(row["yearID"])),
                    league=row["lgID"],
                    team_id=row["teamID"],
                    franch_id=row["franchID"],
                    division=row["divID"],
                    games=int(sanitize(row["G"])),
                    games_at_home=int(sanitize(row["Ghome"])),
                    wins=int(sanitize(row["W"])),
                    losses=int(sanitize(row["L"])),
                    runs=int(sanitize(row["R"])),
                    at_bats=int(sanitize(row["AB"])),
                    hits=int(sanitize(row["H"])),
                    doubles=int(sanitize(row["2B"])),
                    triples=int(sanitize(row["3B"])),
                    homeruns=int(sanitize(row["HR"])),
                    base_on_balls=int(sanitize(row["BB"])),
                    strikeouts=int(sanitize(row["SO"])),
                    stolen_bases=int(sanitize(row["SB"])),
                    caught_stealing=int(sanitize(row["CS"])),
                    runs_against=int(sanitize(row["RA"])),
                    earned_runs=int(sanitize(row["ER"])),
                    saves=int(sanitize(row["SV"])),
                    ip_outs=int(sanitize(row["IPouts"])),
                    errors=int(sanitize(row["E"])),
                    name=row["name"],
                    park=row["park"],
                    batting_park_factor=int(sanitize(row["BPF"])),
                    pitching_park_factor=int(sanitize(row["PPF"])),
                    team_id_br=row["teamIDBR"],
                    team_id_retro=row["teamIDretro"],
                )
                db_session.add(t)
                pbar.update()
        return Result.Ok()
    except Exception as e:
        error = "Error: {error}".format(error=repr(e))
        db_session.rollback()
        return Result.Fail(error)
