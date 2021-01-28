"""Populate player_id and player tables with initial data."""
from dataclasses import dataclass
from datetime import datetime

from dataclass_csv import accept_whitespaces, DataclassReader, dateformat
from tqdm import tqdm

from vigorish.database import Assoc_Player_Team, Player, PlayerId, Season, Team
from vigorish.tasks import UpdatePlayerIdMapTask, UpdatePlayerTeamMapTask
from vigorish.util.dt_format_strings import DATE_ONLY
from vigorish.util.result import Result


@accept_whitespaces
@dateformat(DATE_ONLY)
@dataclass
class PlayerCsvRow:
    playerID: str = None
    birthYear: int = None
    birthMonth: int = None
    birthDay: int = None
    birthCountry: str = None
    birthState: str = None
    birthCity: str = None
    deathYear: int = None
    deathMonth: int = None
    deathDay: int = None
    deathCountry: str = None
    deathState: str = None
    deathCity: str = None
    nameFirst: str = None
    nameLast: str = None
    nameGiven: str = None
    weight: int = None
    height: int = None
    bats: str = None
    throws: str = None
    debut: datetime = None
    finalGame: datetime = None
    retroID: str = None
    bbrefID: str = None


PLAYER_ID_MAP_CSV = "bbref_player_id_map.csv"
PLAYER_TEAM_MAP_CSV = "bbref_player_team_map.csv"
PEOPLE_CSV = "People.csv"


def populate_players(app, csv_folder):
    """Populate player_id and player tables with initial data."""
    result = import_id_map_csv(app, csv_folder)
    if result.failure:
        return result
    app.db_session.commit()
    result = import_people_csv(app.db_session, csv_folder)
    if result.failure:
        return result
    app.db_session.commit()
    result = import_team_map_csv(app, csv_folder)
    if result.failure:
        return result
    app.db_session.commit()
    return Result.Ok()


def import_id_map_csv(app, csv_folder):
    try:
        id_map_task = UpdatePlayerIdMapTask(app, csv_folder.joinpath(PLAYER_ID_MAP_CSV))
        player_id_map = id_map_task.read_bbref_player_id_map_from_file()
        with tqdm(
            total=len(player_id_map),
            desc="Populating player_id table.....",
            unit="row",
            mininterval=0.12,
            maxinterval=5,
            unit_scale=True,
            ncols=90,
        ) as pbar:
            for id_map in player_id_map:
                app.db_session.add(
                    PlayerId(
                        mlb_id=int(id_map.mlb_ID),
                        mlb_name=id_map.name_common,
                        bbref_id=id_map.player_ID,
                        bbref_name=None,
                    )
                )
                pbar.update()
        return Result.Ok()
    except Exception as e:
        error = f"Error: {repr(e)}"
        app.db_session.rollback()
        return Result.Fail(error)


def import_people_csv(db_session, csv_folder):
    player_csv_file = csv_folder.joinpath(PEOPLE_CSV)
    csv_text = player_csv_file.read_text()
    total_rows = len([row for row in csv_text.split("\n") if row])
    try:
        with open(player_csv_file) as player_csv:
            reader = DataclassReader(player_csv, PlayerCsvRow)
            with tqdm(
                total=total_rows,
                desc="Populating player table........",
                unit="row",
                mininterval=0.12,
                maxinterval=5,
                unit_scale=True,
                ncols=90,
            ) as pbar:
                for row in reader:
                    if not (row.birthYear or row.birthMonth or row.birthDay or row.debut):
                        pbar.update()
                        continue
                    player_id = PlayerId.find_by_bbref_id(db_session, row.bbrefID)
                    if not player_id:
                        pbar.update()
                        continue
                    p = Player(
                        mlb_id=player_id.mlb_id,
                        bbref_id=row.bbrefID,
                        name_first=row.nameFirst,
                        name_last=row.nameLast,
                        name_given=row.nameGiven,
                        bats=row.bats,
                        throws=row.throws,
                        weight=row.weight,
                        height=row.height,
                        debut=row.debut,
                        birth_year=row.birthYear,
                        birth_month=row.birthMonth,
                        birth_day=row.birthDay,
                        birth_country=row.birthCountry,
                        birth_state=row.birthState,
                        birth_city=row.birthCity,
                        missing_mlb_id=False,
                    )
                    db_session.add(p)
                    db_session.commit()
                    player_id.db_player_id = p.id
                    pbar.update()
        return Result.Ok()
    except Exception as e:
        error = f"Error: {repr(e)}"
        db_session.rollback()
        return Result.Fail(error)


def import_team_map_csv(app, csv_folder):
    try:
        team_map_task = UpdatePlayerTeamMapTask(app, csv_folder.joinpath(PLAYER_TEAM_MAP_CSV))
        player_team_map = team_map_task.read_bbref_player_team_map_from_file()
        with tqdm(
            total=len(player_team_map),
            desc="Populating player_team table...",
            unit="row",
            mininterval=0.12,
            maxinterval=5,
            unit_scale=True,
            ncols=90,
        ) as pbar:
            for team_map in player_team_map:
                player = Player.find_by_bbref_id(app.db_session, team_map.player_ID)
                team = Team.find_by_team_id_and_year(app.db_session, team_map.team_ID, int(team_map.year_ID))
                season = Season.find_by_year(app.db_session, int(team_map.year_ID))
                player_team = Assoc_Player_Team(
                    db_player_id=player.id,
                    db_team_id=team.id,
                    mlb_id=int(team_map.mlb_ID),
                    bbref_id=team_map.player_ID,
                    team_id=team.team_id_br,
                    year=int(team_map.year_ID),
                    stint_number=team_map.stint_ID,
                    season_id=season.id,
                )
                app.db_session.add(player_team)
                pbar.update()
        return Result.Ok()
    except Exception as e:
        error = f"Error: {repr(e)}"
        app.db_session.rollback()
        return Result.Fail(error)
