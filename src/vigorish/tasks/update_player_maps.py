"""Initialize and update the Player ID Map and Player Team Map CSV files."""
from dataclasses import asdict, dataclass, field

import requests
from dacite import from_dict

from vigorish.config.project_paths import PLAYER_ID_MAP_CSV, PLAYER_TEAM_MAP_CSV
from vigorish.tasks.base import Task
from vigorish.util.dataclass_helpers import sanitize_row_dict
from vigorish.util.result import Result

ALL_PITCH_STATS_URL = "https://www.baseball-reference.com/data/war_daily_pitch.txt"
ALL_BAT_STATS_URL = "https://www.baseball-reference.com/data/war_daily_bat.txt"
YEAR_PITCH_STATS_URL = "https://www.baseball-reference.com/data/war_daily_pitch_year.txt"
YEAR_BAT_STATS_URL = "https://www.baseball-reference.com/data/war_daily_bat_year.txt"


@dataclass(eq=True, frozen=True)
class BBRefBatStats:
    name_common: str
    age: str
    mlb_ID: str
    player_ID: str
    year_ID: str
    team_ID: str
    stint_ID: str
    lg_ID: str
    PA: str
    G: str
    Inn: str
    runs_bat: str
    runs_br: str
    runs_dp: str
    runs_field: str
    runs_infield: str
    runs_outfield: str
    runs_catcher: str
    runs_good_plays: str
    runs_defense: str
    runs_position: str
    runs_position_p: str
    runs_replacement: str
    runs_above_rep: str
    runs_above_avg: str
    runs_above_avg_off: str
    runs_above_avg_def: str
    WAA: str
    WAA_off: str
    WAA_def: str
    WAR: str
    WAR_def: str
    WAR_off: str
    WAR_rep: str
    salary: str
    pitcher: str
    teamRpG: str
    oppRpG: str
    oppRpPA_rep: str
    oppRpG_rep: str
    pyth_exponent: str
    pyth_exponent_rep: str
    waa_win_perc: str
    waa_win_perc_off: str
    waa_win_perc_def: str
    waa_win_perc_rep: str
    OPS_plus: str
    TOB_lg: str
    TB_lg: str


@dataclass(eq=True, frozen=True)
class BBRefPitchStats:
    name_common: str
    age: str
    mlb_ID: str
    player_ID: str
    year_ID: str
    team_ID: str
    stint_ID: str
    lg_ID: str
    G: str
    GS: str
    IPouts: str
    IPouts_start: str
    IPouts_relief: str
    RA: str
    xRA: str
    xRA_sprp_adj: str
    xRA_def_pitcher: str
    PPF: str
    PPF_custom: str
    xRA_final: str
    BIP: str
    BIP_perc: str
    RS_def_total: str
    runs_above_avg: str
    runs_above_avg_adj: str
    runs_above_rep: str
    RpO_replacement: str
    GR_leverage_index_avg: str
    WAR: str
    salary: str
    teamRpG: str
    oppRpG: str
    pyth_exponent: str
    waa_win_perc: str
    WAA: str
    WAA_adj: str
    oppRpG_rep: str
    pyth_exponent_rep: str
    waa_win_perc_rep: str
    WAR_rep: str
    ERA_plus: str
    ER_lg: str


@dataclass(eq=True, frozen=True)
class BBRefPlayerIdMap:
    name_common: str = field(compare=False, repr=False)
    mlb_ID: str = field(repr=False)
    player_ID: str


@dataclass(eq=True, frozen=True)
class BBRefPlayerTeamMap:
    name_common: str = field(compare=False, repr=False)
    age: str = field(compare=False, repr=False)
    mlb_ID: str = field(repr=False)
    player_ID: str
    year_ID: str
    team_ID: str
    stint_ID: str = field(repr=False)
    lg_ID: str = field(repr=False)


class UpdatePlayerIdMap(Task):
    def __init__(self, app):
        super().__init__(app)

    def execute(self):
        if not PLAYER_ID_MAP_CSV.exists():
            self.initialize_bbref_player_id_map()
            return
        id_map = self.read_bbref_player_id_map_from_file(PLAYER_ID_MAP_CSV)
        current_id_map = self.get_current_player_id_map()
        new_id_map = current_id_map - id_map
        if new_id_map:
            id_map.update(new_id_map)
        sorted_id_map = sorted(list(id_map), key=lambda x: x.player_ID)
        self.write_bbref_player_id_map_to_file(sorted_id_map)
        new_id_dicts = [asdict(id_map) for id_map in new_id_map] if new_id_map else None
        return Result.Ok(new_id_dicts)

    def initialize_bbref_player_id_map(self):
        if PLAYER_ID_MAP_CSV.exists():
            PLAYER_ID_MAP_CSV.unlink()
        id_map = self.get_full_player_id_map()
        sorted_id_map = sorted(id_map, key=lambda x: x.player_ID)
        self.write_bbref_player_id_map_to_file(sorted_id_map)

    def get_full_player_id_map(self):
        (bat_stat_list, pitch_stat_list) = parse_full_pitch_and_bat_stats()
        return self.get_player_id_map_from_stats_lists(bat_stat_list, pitch_stat_list)

    def get_current_player_id_map(self):
        (bat_stat_list, pitch_stat_list) = parse_current_pitch_and_bat_stats()
        return self.get_player_id_map_from_stats_lists(bat_stat_list, pitch_stat_list)

    def get_player_id_map_from_stats_lists(self, *stats_lists):
        id_map = set()
        for stats_list in stats_lists:
            id_map.update(
                {
                    BBRefPlayerIdMap(stats.name_common, stats.mlb_ID, stats.player_ID)
                    for stats in list(stats_list)
                    if "NULL" not in stats.mlb_ID
                }
            )
        sorted_id_map = sorted(list(id_map), key=lambda x: x.player_ID)
        return set(sorted_id_map)

    def write_bbref_player_id_map_to_file(self, new_id_map):
        csv_text = serialize_data_class_objects(new_id_map)
        PLAYER_ID_MAP_CSV.write_text(csv_text)

    def read_bbref_player_id_map_from_file(self, player_id_map_csv):
        if not player_id_map_csv.exists():
            player_id_map_csv.touch()
            return set()
        id_map_text = player_id_map_csv.read_text()
        if not id_map_text:
            return set()
        id_map = deserialize_data_class_objects(id_map_text, BBRefPlayerIdMap)
        return set(id_map)


class UpdatePlayerTeamMap(Task):
    def __init__(self, app):
        super().__init__(app)

    def execute(self):
        if not PLAYER_TEAM_MAP_CSV.exists():
            self.initialize_bbref_player_team_map()
            return
        team_map = self.read_bbref_player_team_map_from_file()
        current_team_map = self.get_current_team_id_map()
        new_team_map = current_team_map - team_map
        if new_team_map:
            team_map.update(new_team_map)
        sorted_team_map = sorted(list(team_map), key=lambda x: (x.player_ID, x.year_ID, x.stint_ID))
        self.write_bbref_player_team_map_to_file(sorted_team_map)
        new_team_dicts = [asdict(team_map) for team_map in new_team_map] if new_team_map else None
        return Result.Ok(new_team_dicts)

    def initialize_bbref_player_team_map(self):
        if PLAYER_TEAM_MAP_CSV.exists():
            PLAYER_TEAM_MAP_CSV.unlink()
        team_map = self.get_full_team_id_map()
        sorted_team_map = sorted(team_map, key=lambda x: (x.player_ID, x.year_ID, x.stint_ID))
        self.write_bbref_player_team_map_to_file(sorted_team_map)

    def get_full_team_id_map(self):
        (bat_stat_list, pitch_stat_list) = parse_full_pitch_and_bat_stats()
        return self.get_player_team_map_from_stats_lists(bat_stat_list, pitch_stat_list)

    def get_current_team_id_map(self):
        (bat_stat_list, pitch_stat_list) = parse_current_pitch_and_bat_stats()
        return self.get_player_team_map_from_stats_lists(bat_stat_list, pitch_stat_list)

    def get_player_team_map_from_stats_lists(self, *stats_lists):
        team_map = set()
        for stats_list in stats_lists:
            team_map.update(
                {
                    BBRefPlayerTeamMap(
                        stats.name_common,
                        stats.age,
                        stats.mlb_ID,
                        stats.player_ID,
                        stats.year_ID,
                        stats.team_ID,
                        stats.stint_ID,
                        stats.lg_ID,
                    )
                    for stats in list(stats_list)
                }
            )
        sorted_team_map = sorted(list(team_map), key=lambda x: (x.player_ID, x.year_ID, x.stint_ID))
        return set(sorted_team_map)

    def write_bbref_player_team_map_to_file(self, new_id_map):
        csv_text = serialize_data_class_objects(new_id_map)
        PLAYER_TEAM_MAP_CSV.write_text(csv_text)

    def read_bbref_player_team_map_from_file(self):
        if not PLAYER_TEAM_MAP_CSV.exists():
            PLAYER_TEAM_MAP_CSV.touch()
            return set()
        team_map_text = PLAYER_TEAM_MAP_CSV.read_text()
        if not team_map_text:
            return set()
        team_map = deserialize_data_class_objects(team_map_text, BBRefPlayerTeamMap)
        return set(team_map)


def parse_full_pitch_and_bat_stats():
    pitch_stat_list = parse_pitch_stats(ALL_PITCH_STATS_URL)
    bat_stat_list = parse_bat_stats(ALL_BAT_STATS_URL)
    return (pitch_stat_list, bat_stat_list)


def parse_current_pitch_and_bat_stats():
    pitch_stat_list = parse_pitch_stats(YEAR_PITCH_STATS_URL)
    bat_stat_list = parse_bat_stats(YEAR_BAT_STATS_URL)
    return (pitch_stat_list, bat_stat_list)


def parse_pitch_stats(url):
    response = requests.get(url)
    page_text = response.text
    pitch_stat_list = deserialize_data_class_objects(page_text, BBRefPitchStats)
    return set(pitch_stat_list)


def parse_bat_stats(url):
    response = requests.get(url)
    page_text = response.text
    bat_stat_list = deserialize_data_class_objects(page_text, BBRefBatStats)
    return set(bat_stat_list)


def serialize_data_class_objects(data_class_objects):
    dataclass_dicts = [asdict(do) for do in data_class_objects]
    if not dataclass_dicts:
        return None
    col_names = [",".join(list(dataclass_dicts[0].keys()))]
    csv_rows = [",".join(sanitize_row_dict(d)) for d in dataclass_dicts]
    return "\n".join((col_names + csv_rows))


def deserialize_data_class_objects(csv_text, data_class):
    csv_rows = csv_text.split("\n")
    col_names = [col.strip() for col in csv_rows.pop(0).split(",")]
    csv_rows = [row.split(",") for row in csv_rows]
    csv_dict_list = [dict(zip(col_names, row)) for row in csv_rows if row != [""]]
    return [from_dict(data_class=data_class, data=csv_dict) for csv_dict in csv_dict_list]
