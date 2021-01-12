"""Initialize and update the Player ID Map and Player Team Map CSV files."""
from dataclasses import asdict, dataclass, field

import requests

from vigorish.config.project_paths import PLAYER_ID_MAP_CSV, PLAYER_TEAM_MAP_CSV
from vigorish.tasks.base import Task
from vigorish.util.dataclass_helpers import (
    serialize_data_class_to_csv,
    deserialize_data_class_from_csv,
)
from vigorish.util.dt_format_strings import DATE_ONLY
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
        new_id_map = list(set(current_id_map) - set(id_map))
        if not new_id_map:
            return Result.Ok([])
        id_map = id_map + new_id_map
        id_map.sort(key=lambda x: x.player_ID)
        self.write_bbref_player_id_map_to_file(id_map, PLAYER_ID_MAP_CSV)
        new_id_dicts = [asdict(id_map) for id_map in new_id_map] if new_id_map else None
        return Result.Ok(new_id_dicts)

    def initialize_bbref_player_id_map(self):
        if PLAYER_ID_MAP_CSV.exists():
            PLAYER_ID_MAP_CSV.unlink()
        id_map = self.get_full_player_id_map()
        self.write_bbref_player_id_map_to_file(id_map, PLAYER_ID_MAP_CSV)

    def get_full_player_id_map(self):
        (bat_stat_list, pitch_stat_list) = parse_full_pitch_and_bat_stats()
        return self.get_player_id_map_from_stats_lists(bat_stat_list, pitch_stat_list)

    def get_current_player_id_map(self):
        (bat_stat_list, pitch_stat_list) = parse_current_pitch_and_bat_stats()
        return self.get_player_id_map_from_stats_lists(bat_stat_list, pitch_stat_list)

    def get_player_id_map_from_stats_lists(self, *stats_lists):
        id_map = {
            BBRefPlayerIdMap(stats.name_common, stats.mlb_ID, stats.player_ID)
            for stats_list in stats_lists
            for stats in stats_list
            if "NULL" not in stats.mlb_ID
        }
        return sorted(id_map, key=lambda x: x.player_ID)

    def write_bbref_player_id_map_to_file(self, new_id_map, player_id_csv_map):
        csv_text = serialize_data_class_to_csv(new_id_map, date_format=DATE_ONLY)
        player_id_csv_map.write_text(csv_text)

    def read_bbref_player_id_map_from_file(self, player_id_map_csv):
        if not player_id_map_csv.exists():
            player_id_map_csv.touch()
            return []
        id_map_text = player_id_map_csv.read_text()
        if not id_map_text:
            return []
        id_map = deserialize_data_class_from_csv(id_map_text, BBRefPlayerIdMap)
        return list(set(id_map))


class UpdatePlayerTeamMap(Task):
    def __init__(self, app):
        super().__init__(app)

    def execute(self):
        if not PLAYER_TEAM_MAP_CSV.exists():
            self.initialize_bbref_player_team_map()
            return
        team_map = self.read_bbref_player_team_map_from_file(PLAYER_TEAM_MAP_CSV)
        current_team_map = self.get_current_team_id_map()
        new_team_map = list(set(current_team_map) - set(team_map))
        if not new_team_map:
            return Result.Ok([])
        team_map = team_map + new_team_map
        team_map.sort(key=lambda x: (x.player_ID, x.year_ID, x.stint_ID))
        self.write_bbref_player_team_map_to_file(team_map, PLAYER_TEAM_MAP_CSV)
        new_team_dicts = [asdict(team_map) for team_map in new_team_map] if new_team_map else None
        return Result.Ok(new_team_dicts)

    def initialize_bbref_player_team_map(self):
        if PLAYER_TEAM_MAP_CSV.exists():
            PLAYER_TEAM_MAP_CSV.unlink()
        team_map = self.get_full_team_id_map()
        self.write_bbref_player_team_map_to_file(team_map, PLAYER_TEAM_MAP_CSV)

    def get_full_team_id_map(self):
        (bat_stat_list, pitch_stat_list) = parse_full_pitch_and_bat_stats()
        return self.get_player_team_map_from_stats_lists(bat_stat_list, pitch_stat_list)

    def get_current_team_id_map(self):
        (bat_stat_list, pitch_stat_list) = parse_current_pitch_and_bat_stats()
        return self.get_player_team_map_from_stats_lists(bat_stat_list, pitch_stat_list)

    def get_player_team_map_from_stats_lists(self, *stats_lists):
        team_map = {
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
            for stats_list in stats_lists
            for stats in stats_list
        }
        return sorted(team_map, key=lambda x: (x.player_ID, x.year_ID, x.stint_ID))

    def write_bbref_player_team_map_to_file(self, new_team_map, player_team_map_csv):
        csv_text = serialize_data_class_to_csv(new_team_map, date_format=DATE_ONLY)
        player_team_map_csv.write_text(csv_text)

    def read_bbref_player_team_map_from_file(self, player_team_map_csv):
        if not player_team_map_csv.exists():
            player_team_map_csv.touch()
            return []
        team_map_text = player_team_map_csv.read_text()
        if not team_map_text:
            return []
        team_map = deserialize_data_class_from_csv(team_map_text, BBRefPlayerTeamMap)
        return list(set(team_map))


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
    pitch_stat_list = deserialize_data_class_from_csv(page_text, BBRefPitchStats)
    return list(set(pitch_stat_list))


def parse_bat_stats(url):
    response = requests.get(url)
    page_text = response.text
    bat_stat_list = deserialize_data_class_from_csv(page_text, BBRefBatStats)
    return list(set(bat_stat_list))
