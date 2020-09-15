"""Populate team table with initial data."""
from dataclasses import dataclass

from dataclass_csv import accept_whitespaces, DataclassReader
from tqdm import tqdm

from vigorish.models.team import Team
from vigorish.util.result import Result


@accept_whitespaces
@dataclass
class TeamCsvRow:
    yearID: int = None
    lgID: str = None
    teamID: str = None
    franchID: str = None
    divID: str = None
    Rank: int = None
    G: int = None
    Ghome: int = None
    W: int = None
    L: int = None
    DivWin: str = None
    WCWin: str = None
    LgWin: str = None
    WSWin: str = None
    R: int = None
    AB: int = None
    H: int = None
    doubles: int = None
    triples: int = None
    HR: int = None
    BB: int = None
    SO: int = None
    SB: int = None
    CS: int = None
    HBP: int = None
    SF: int = None
    RA: int = None
    ER: int = None
    ERA: float = None
    CG: int = None
    SHO: int = None
    SV: int = None
    IPouts: int = None
    HA: int = None
    HRA: int = None
    BBA: int = None
    SOA: int = None
    E: int = None
    DP: int = None
    FP: float = None
    name: str = None
    park: str = None
    attendance: int = None
    BPF: int = None
    PPF: int = None
    teamIDBR: str = None
    teamIDlahman45: str = None
    teamIDretro: str = None


TEAMS_CSV_FILENAME = "Teams.csv"


def populate_teams(db_session, csv_folder):
    """Populate team table with initial data."""
    result = import_teams_csv(db_session, csv_folder)
    if result.failure:
        return result
    db_session.commit()
    return Result.Ok()


def import_teams_csv(db_session, csv_folder):
    team_csv_file = csv_folder.joinpath(TEAMS_CSV_FILENAME)
    csv_text = team_csv_file.read_text()
    total_rows = len([row for row in csv_text.split("\n") if row])
    try:
        with open(team_csv_file) as team_csv:
            reader = DataclassReader(team_csv, TeamCsvRow)
            reader.map("2B").to("doubles")
            reader.map("3B").to("triples")
            with tqdm(
                total=total_rows,
                desc="Populating team table........",
                unit="row",
                mininterval=0.12,
                maxinterval=5,
                unit_scale=True,
                ncols=90,
            ) as pbar:
                for row in reader:
                    t = Team(
                        year=row.yearID,
                        league=row.lgID,
                        team_id=row.teamID,
                        franch_id=row.franchID,
                        division=row.divID,
                        games=row.G,
                        games_at_home=row.Ghome,
                        wins=row.W,
                        losses=row.L,
                        runs=row.R,
                        at_bats=row.AB,
                        hits=row.H,
                        doubles=row.doubles,
                        triples=row.triples,
                        homeruns=row.HR,
                        base_on_balls=row.BB,
                        strikeouts=row.SO,
                        stolen_bases=row.SB,
                        caught_stealing=row.CS,
                        runs_against=row.RA,
                        earned_runs=row.ER,
                        saves=row.SV,
                        ip_outs=row.IPouts,
                        errors=row.E,
                        name=row.name,
                        park=row.park,
                        batting_park_factor=row.BPF,
                        pitching_park_factor=row.PPF,
                        team_id_br=row.teamIDBR,
                        team_id_retro=row.teamIDretro,
                    )
                    db_session.add(t)
                    pbar.update()
        return Result.Ok()
    except Exception as e:
        error = "Error: {error}".format(error=repr(e))
        db_session.rollback()
        return Result.Fail(error)
