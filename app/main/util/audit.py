"""One-off functions I used for various auditing/refactoring efforts."""
import csv
import dataclasses
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path
from pprint import pprint

from dataclass_csv import DataclassReader, accept_whitespaces
from halo import Halo
from tqdm import tqdm

from app.main.models.season import Season
from app.main.util.s3_helper import (
    get_all_scraped_brooks_game_ids,
    get_brooks_pitch_logs_for_game_from_s3,
    upload_brooks_pitch_logs_for_game,
    get_all_pitch_app_ids_scraped,
    get_brooks_pitchfx_log_from_s3,
    delete_brooks_pitchfx_log_from_s3,
    rename_brooks_pitchfx_log,
    upload_brooks_pitchfx_log,
)
from app.main.util.result import Result


def audit(db, year):
    season = Season.find_by_year(db["session"], year)
    spinner = Halo(text=f'Gathering all scraped Pitchfx IDs for the {year} MLB season...', color='magenta', spinner='dots3')
    spinner.start()
    result = get_all_pitch_app_ids_scraped(season.year)
    if result.failure:
        return result
    spinner.stop();
    scraped_pitch_app_ids = result.value
    pitchfx_id_tuples = []
    bbref_pitch_app_ids = []
    error_dict = {}
    with tqdm(total=len(scraped_pitch_app_ids), unit="pitch_app", position=0, leave=True) as pbar:
        for pitch_app_id in scraped_pitch_app_ids:
            pbar.set_description(pitch_app_id[:8])
            result = get_brooks_pitchfx_log_from_s3(pitch_app_id)
            if result.failure:
                error_dict[pitch_app_id] = result.error
            pitchfx_log = result.value
            bbref_pitch_app_id = f"{pitchfx_log.bbref_game_id}_{pitchfx_log.pitcher_id_mlb}"
            pitchfx_id_tuples.append((bbref_pitch_app_id, pitch_app_id))
            bbref_pitch_app_ids.append(bbref_pitch_app_id)
            pbar.update()
    histogram = Counter(bbref_pitch_app_ids)
    bbref_pitch_app_ids_set = Counter(list(set(bbref_pitch_app_ids)))
    duplicate_ids = histogram - bbref_pitch_app_ids_set
    total_duplicates = sum(duplicate_ids.values())
    duplicate_map = {}
    for bbref_pitch_app_id in duplicate_ids.keys():
        duplicate_map[bbref_pitch_app_id] = [t[1] for t in pitchfx_id_tuples if t[0] == bbref_pitch_app_id]
    print(f"S3 bucket contains {len(scraped_pitch_app_ids)} PitchFX logs, containing {len(bbref_pitch_app_ids_set)} unique pitching appearances.")
    print(f"{len(duplicate_ids)} pitch appearances were scraped more than once, resulting in {total_duplicates} logs which must be removed from the bucket:\n\n")
    pprint(duplicate_map, indent=4)
    return exit_app_success(db, "Successfully completed audit of pitchfx data.")


def fixdupes(db):
    audit_json = Path("audit_results.json").read_text()
    audit_results = json.loads(audit_json)
    audit_results_copy = json.loads(audit_json)
    error_dict = {}
    with tqdm(total=len(audit_results), unit="pitch_app", position=0, leave=True) as pbar:
        for bbref_pitch_app_id, pitch_app_id_list in audit_results.items():
            pbar.set_description(bbref_pitch_app_id)
            if len(pitch_app_id_list) != 2:
                pbar.update()
                continue
            pa1 = get_brooks_pitchfx_log_from_s3(pitch_app_id_list[0]).value
            pa2 = get_brooks_pitchfx_log_from_s3(pitch_app_id_list[1]).value
            if pa1 != pa2:
                pa1_dict = pa1.as_dict()
                pa1_dict.pop("pitchfx_log")
                pa2_dict = pa2.as_dict()
                pa2_dict.pop("pitchfx_log")
                error_dict[bbref_pitch_app_id] = [pa1_dict, pa2_dict]
                pbar.update()
                continue
            result = delete_brooks_pitchfx_log_from_s3(pitch_app_id_list[1], 2019)
            if result.failure:
                error_dict[bbref_pitch_app_id] = f"Trying to delete '{pitch_app_id_list[1]}':\nERROR: {result.error}"
                pbar.update()
                continue
            audit_results_copy.pop(bbref_pitch_app_id)
            pbar.update()
    print("The following errors occurred:")
    pprint(error_dict, indent=4)
    print("Remaining duplicate PitchFX logs:")
    pprint(audit_results_copy, indent=4)
    return exit_app_success(db, "Successfully removed all duplicate pitchfx data.")


def bulkrename(db, year):
    season = Season.find_by_year(db["session"], year)
    spinner = Halo(text=f'Gathering all scraped Pitchfx IDs for the {year} MLB season...', color='magenta', spinner='dots3')
    spinner.start()
    result = get_all_pitch_app_ids_scraped(season.year)
    if result.failure:
        return result
    scraped_pitch_app_ids = result.value
    scraped_pitch_app_ids = [
        pitch_app_id for pitch_app_id
        in scraped_pitch_app_ids
        if GUID_REGEX.search(pitch_app_id)
    ]
    spinner.stop();
    error_dict = {}
    with tqdm(total=len(scraped_pitch_app_ids), unit="pitch_app", position=0, leave=True) as pbar:
        for pitch_app_id in scraped_pitch_app_ids:
            pbar.set_description(pitch_app_id[:8])
            result = get_brooks_pitchfx_log_from_s3(pitch_app_id)
            if result.failure:
                error_dict[pitch_app_id] = result.error
                pbar.update()
                continue
            pitchfx_log = result.value
            bbref_pitch_app_id = f"{pitchfx_log.bbref_game_id}_{pitchfx_log.pitcher_id_mlb}"
            result = rename_brooks_pitchfx_log(pitch_app_id, bbref_pitch_app_id, year=season.year)
            if result.failure:
                error_dict[pitch_app_id] = result.error
                pbar.update()
                continue
            pbar.update()
    print("The following errors occurred:")
    pprint(error_dict, indent=4)
    return exit_app_success(db, "Successfully removed all duplicate pitchfx data.")


def bulkupdate(db, year):
    season = Season.find_by_year(db["session"], year)
    spinner = Halo(text=f'Gathering all scraped brooksbaseball game ids for the 2019 MLB season...', color='cyan', spinner='dots3')
    spinner.start()
    result = get_all_scraped_brooks_game_ids(season.year)
    if result.failure:
        return result
    scraped_brooks_game_ids = result.value
    spinner.stop();
    game_log_error_dict = {}
    with tqdm(total=len(scraped_brooks_game_ids), unit="game", position=0, leave=True) as pbar_game:
        for bb_game_id in scraped_brooks_game_ids:
            pbar_game.set_description(bb_game_id)
            result = get_brooks_pitch_logs_for_game_from_s3(bb_game_id)
            if result.failure:
                game_log_error_dict[bb_game_id] = result.error
                pbar_game.update()
                continue
            pitch_logs_for_game = result.value
            for pitch_log in pitch_logs_for_game.pitch_logs:
                with tqdm(total=len(pitch_logs_for_game.pitch_logs), unit="pitch_app", position=1, leave=True) as pbar_pitch_app:
                    pbar_pitch_app.set_description(f"{pitch_log.pitcher_id_mlb}")
                    pitch_log.pitch_app_id = f"{pitch_log.bbref_game_id}_{pitch_log.pitcher_id_mlb}"
                    pbar_pitch_app.update()
            result = upload_brooks_pitch_logs_for_game(pitch_logs_for_game)
            if result.failure:
                game_log_error_dict[bb_game_id] = result.error
                pbar_game.update()
                continue
            pbar_game.update()

    spinner = Halo(text=f'Gathering all scraped pitchfx ids for the 2019 MLB season...', color='magenta', spinner='dots3')
    spinner.start()
    result = get_all_pitch_app_ids_scraped(season.year)
    if result.failure:
        return result
    scraped_pitch_app_ids = result.value
    spinner.stop();
    pfx_error_dict = {}
    with tqdm(total=len(scraped_pitch_app_ids), unit="pitch_app", position=0, leave=True) as pbar:
        for pitch_app_id in scraped_pitch_app_ids:
            pbar.set_description(pitch_app_id)
            result = get_brooks_pitchfx_log_from_s3(pitch_app_id)
            if result.failure:
                pfx_error_dict[pitch_app_id] = result.error
                pbar.update()
                continue
            pitchfx_log = result.value
            pitchfx_log.pitch_app_id = f"{pitchfx_log.bbref_game_id}_{pitchfx_log.pitcher_id_mlb}"
            for pitchfx in pitchfx_log.pitchfx_log:
                pitchfx.pitch_app_id = f"{pitchfx_log.bbref_game_id}_{pitchfx_log.pitcher_id_mlb}"
            result = upload_brooks_pitchfx_log(pitchfx_log)
            if result.failure:
                pfx_error_dict[pitch_app_id] = result.error
                pbar.update()
                continue
            pbar.update()
    print("The following errors occurred:")
    pprint(game_log_error_dict, indent=4)
    pprint(pfx_error_dict, indent=4)
    return exit_app_success(db, "Successfully updated all pitch logs with new id format.")


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
    IDMAP_PATH = Path(__file__).resolve().parent.parent / "setup/csv/idmap.csv"
    IDMAP_NEW_PATH = Path(__file__).resolve().parent.parent / "setup/csv/idmap_new.csv"
    with open(IDMAP_PATH) as f:
        with open(IDMAP_NEW_PATH) as f_new:
            id_map = DataclassReader(f, PlayerIdMap)
            id_map_list = list(id_map)
            player_ids = set([id.mlb_id for id in id_map_list])
            id_map_new = DataclassReader(f_new, PlayerIdMap)
            id_map_list_new = list(id_map_new)
            player_ids_new = set([id.mlb_id for id in id_map_list_new])
            net_new_ids = player_ids_new - player_ids
            if not net_new_ids:
                return
            id_map_updated = [dataclasses.asdict(id_map) for id_map in id_map_list]
            for player_id_mlb in list(net_new_ids):
                player_id_matches = [id_map for id_map in id_map_list_new if id_map.mlb_id == player_id_mlb]
                if player_id_matches:
                    player_id_map = player_id_matches[0]
                    if player_id_map.birth_year:
                        player_id_map.birth_year = player_id_map.birth_year[-4:]
                    if player_id_map.mlb_id:
                        id_map_updated.append(dataclasses.asdict(player_id_map))
                else:
                    print(f"Error! Failed to retrieve player id map for mlb_id: {player_id_mlb} from idmap_new.csv")
            id_map_updated.sort(key=lambda x: x["mlb_name"])
            with open("idmap_updated.csv", mode='w') as f_updated:
                fieldnames = ["mlb_id","mlb_name","mlb_pos","mlb_team","mlb_team_long","bats","throws","birth_year","bp_id","bref_id","bref_name","cbs_id","cbs_name","cbs_pos","espn_id","espn_name","espn_pos","fg_id","fg_name","fg_pos","lahman_id","nfbc_id","nfbc_name","nfbc_pos","retro_id","retro_name","debut","yahoo_id","yahoo_name","yahoo_pos","mlb_depth","ottoneu_id","ottoneu_name","ottoneu_pos","rotowire_id","rotowire_name","rotowire_pos"]
                writer = csv.DictWriter(f_updated, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(id_map_updated)
