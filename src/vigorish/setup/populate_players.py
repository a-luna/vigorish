"""Populate player_id and player tables with initial data."""
from dataclasses import dataclass
from datetime import datetime

from dataclass_csv import accept_whitespaces, DataclassReader, dateformat
from tqdm import tqdm

from vigorish.config.database import Player, PlayerId
from vigorish.config.project_paths import PEOPLE_CSV, TEST_PEOPLE_CSV
from vigorish.tasks.update_player_maps import UpdatePlayerIdMap
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


def populate_players(app, is_test=False):
    db_session = app["db_session"]
    """Populate player_id and player tables with initial data."""
    result = import_idmap_csv(db_session, app, is_test)
    if result.failure:
        return result
    db_session.commit()
    result = import_people_csv(db_session, is_test)
    if result.failure:
        return result
    db_session.commit()
    return Result.Ok()


def import_idmap_csv(db_session, app, is_test):
    try:
        update_player_id_map = UpdatePlayerIdMap(app)
        player_id_map = update_player_id_map.read_bbref_player_id_map_from_file(is_test)
        with tqdm(
            total=len(player_id_map),
            desc="Populating player_id table...",
            unit="row",
            mininterval=0.12,
            maxinterval=5,
            unit_scale=True,
            ncols=90,
        ) as pbar:
            for id_map in player_id_map:
                db_session.add(
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
        error = "Error: {error}".format(error=repr(e))
        db_session.rollback()
        return Result.Fail(error)


def import_people_csv(db_session, is_test):
    player_csv_file = TEST_PEOPLE_CSV if is_test else PEOPLE_CSV
    csv_text = player_csv_file.read_text()
    total_rows = len([row for row in csv_text.split("\n") if row])
    try:
        with open(player_csv_file) as player_csv:
            reader = DataclassReader(player_csv, PlayerCsvRow)
            with tqdm(
                total=total_rows,
                desc="Populating player table......",
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
                    setattr(player_id, "db_player_id", p.id)
                    pbar.update()
        return Result.Ok()
    except Exception as e:
        error = "Error: {error}".format(error=repr(e))
        db_session.rollback()
        return Result.Fail(error)
