"""Update player and player_id tables when idmap.csv file is updated."""
import csv
import dataclasses
from pathlib import Path
from pprint import pprint

from dataclass_csv import DataclassReader, accept_whitespaces


@accept_whitespaces
@dataclasses.dataclass
class PlayerIdMap:
    mlb_id: int = dataclasses.field(default=None)
    mlb_name: str = dataclasses.field(default=None)
    mlb_pos: str = dataclasses.field(default=None)
    mlb_team: str = dataclasses.field(default=None)
    mlb_team_long: str = dataclasses.field(default=None)
    bats: str = dataclasses.field(default=None)
    throws: str = dataclasses.field(default=None)
    birth_year: str = dataclasses.field(default=None)
    bp_id: int = dataclasses.field(default=None)
    bref_id: str = dataclasses.field(default=None)
    bref_name: str = dataclasses.field(default=None)
    cbs_id: int = dataclasses.field(default=None)
    cbs_name: str = dataclasses.field(default=None)
    cbs_pos: str = dataclasses.field(default=None)
    espn_id: int = dataclasses.field(default=None)
    espn_name: str = dataclasses.field(default=None)
    espn_pos: str = dataclasses.field(default=None)
    fg_id: str = dataclasses.field(default=None)
    fg_name: str = dataclasses.field(default=None)
    fg_pos: str = dataclasses.field(default=None)
    lahman_id: str = dataclasses.field(default=None)
    nfbc_id: int = dataclasses.field(default=None)
    nfbc_name: str = dataclasses.field(default=None)
    nfbc_pos: str = dataclasses.field(default=None)
    retro_id: str = dataclasses.field(default=None)
    retro_name: str = dataclasses.field(default=None)
    debut: int = dataclasses.field(default=None)
    yahoo_id: int = dataclasses.field(default=None)
    yahoo_name: str = dataclasses.field(default=None)
    yahoo_pos: str = dataclasses.field(default=None)
    mlb_depth: str = dataclasses.field(default=None)
    ottoneu_id: str = dataclasses.field(default=None)
    ottoneu_name: str = dataclasses.field(default=None)
    ottoneu_pos: str = dataclasses.field(default=None)
    rotowire_id: int = dataclasses.field(default=None)
    rotowire_name: str = dataclasses.field(default=None)
    rotowire_pos: str = dataclasses.field(default=None)


def update_player_id_map():
    IDMAP = Path(__file__).parent.parent / "setup/csv/idmap.csv"
    UPDATED_IDMAP = Path(__file__).parent.parent / "setup/csv/idmap_new.csv"
    with open(IDMAP) as f:
        with open(UPDATED_IDMAP) as f_new:
            id_map_csv_reader = DataclassReader(f, PlayerIdMap)
            current_id_map = [id_map for id_map in list(id_map_csv_reader) if id_map.bref_id]
            current_player_ids = set([id_map.mlb_id for id_map in current_id_map])
            new_id_map_csv_reader = DataclassReader(f_new, PlayerIdMap)
            new_id_map = list(new_id_map_csv_reader)
            new_player_ids = set([id_map.mlb_id for id_map in new_id_map])
            ids_to_add = new_player_ids - current_player_ids
            if not ids_to_add:
                return
            updated_id_map = [dataclasses.asdict(id_map) for id_map in current_id_map]
            new_player_log = []
            potential_ids_to_add = [
                dataclasses.asdict(id_map)
                for id_map in new_id_map
                if id_map.mlb_id in list(ids_to_add)
            ]
            for id_map in potential_ids_to_add:
                if id_map["birth_year"]:
                    id_map["birth_year"] = id_map["birth_year"][-4:]
                if id_map["mlb_id"]:
                    new_player_log.append(
                        {
                            "player_name": id_map["mlb_name"],
                            "mlb_id": id_map["mlb_id"],
                            "bbref_id": id_map["bref_id"],
                        }
                    )
                    updated_id_map.append(id_map)
                else:
                    print(
                        f"Error! Failed to retrieve player id map for player: "
                        f'{id_map["mlb_name"]} from idmap_new.csv'
                    )
            updated_id_map.sort(key=lambda x: x["mlb_name"])
            with open("idmap_updated.csv", mode="w") as f_updated:
                fieldnames = [
                    "mlb_id",
                    "mlb_name",
                    "mlb_pos",
                    "mlb_team",
                    "mlb_team_long",
                    "bats",
                    "throws",
                    "birth_year",
                    "bp_id",
                    "bref_id",
                    "bref_name",
                    "cbs_id",
                    "cbs_name",
                    "cbs_pos",
                    "espn_id",
                    "espn_name",
                    "espn_pos",
                    "fg_id",
                    "fg_name",
                    "fg_pos",
                    "lahman_id",
                    "nfbc_id",
                    "nfbc_name",
                    "nfbc_pos",
                    "retro_id",
                    "retro_name",
                    "debut",
                    "yahoo_id",
                    "yahoo_name",
                    "yahoo_pos",
                    "mlb_depth",
                    "ottoneu_id",
                    "ottoneu_name",
                    "ottoneu_pos",
                    "rotowire_id",
                    "rotowire_name",
                    "rotowire_pos",
                ]
                writer = csv.DictWriter(f_updated, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(updated_id_map)
            pprint(new_player_log)
