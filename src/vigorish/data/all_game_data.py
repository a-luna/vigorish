from collections import defaultdict, OrderedDict
from copy import deepcopy
from datetime import datetime
from functools import lru_cache

from tabulate import tabulate

from vigorish.cli.components.models import DisplayTable
from vigorish.cli.components.table_viewer import TableViewer
from vigorish.config.database import PitchAppScrapeStatus, PlayerId, Team
from vigorish.data.all_player_data import AllPlayerData
from vigorish.enums import DataSet, DefensePosition, VigFile
from vigorish.util.dt_format_strings import DT_AWARE, DT_AWARE_VERBOSE
from vigorish.util.exceptions import ScrapedDataException
from vigorish.util.list_helpers import flatten_list2d, group_and_sort_dict_list, make_chunked_list
from vigorish.util.regex import INNING_LABEL_REGEX
from vigorish.util.result import Result
from vigorish.util.string_helpers import (
    get_bbref_team_id,
    inning_number_to_string,
    validate_at_bat_id,
    validate_bbref_game_id,
    wrap_text,
)

AT_BAT_TABLE_MAX_ROWS = 8
AT_BAT_DESC_MAX_WIDTH = 35


class AllGameData:
    def __init__(self, app, bbref_game_id):
        # TODO: Create method to get pitch type counts for pitch app
        self.app = app
        self.db_engine = app["db_engine"]
        self.db_session = app["db_session"]
        self.scraped_data = app["scraped_data"]
        combined_data = self.scraped_data.get_combined_game_data(bbref_game_id)
        if not combined_data:
            raise ScrapedDataException(
                file_type=VigFile.COMBINED_GAME_DATA, data_set=DataSet.ALL, url_id=bbref_game_id
            )
        self.bbref_game_id = combined_data["bbref_game_id"]
        self.pitchfx_vs_bbref_audit = combined_data["pitchfx_vs_bbref_audit"]
        self.game_meta_info = combined_data["game_meta_info"]
        self.away_team_data = combined_data["away_team_data"]
        self.home_team_data = combined_data["home_team_data"]
        self.innings_list = combined_data["play_by_play_data"]
        self.duplicate_guids = combined_data["duplicate_guids"]
        self.removed_pitchfx = combined_data["removed_pitchfx"]
        self.invalid_pitchfx = combined_data["invalid_pitchfx"]
        self.player_id_dict = combined_data["player_id_dict"]
        self.all_player_mlb_ids = [id_map["mlb_id"] for id_map in self.player_id_dict.values()]
        self.game_date = validate_bbref_game_id(self.bbref_game_id).value["game_date"]

    @property
    def away_team_id(self):
        return self.away_team_data["team_id_br"]

    @property
    def home_team_id(self):
        return self.home_team_data["team_id_br"]

    @property
    def away_team(self):
        return Team.find_by_team_id_and_year(
            self.db_session, self.away_team_id, self.game_date.year
        )

    @property
    def home_team(self):
        return Team.find_by_team_id_and_year(
            self.db_session, self.home_team_id, self.game_date.year
        )

    @property
    def game_datetime(self):
        return datetime.strptime(self.game_meta_info["game_date_time_str"], DT_AWARE)

    @property
    def bat_stats_player_ids(self):
        batter_mlb_ids = [
            int(validate_at_bat_id(at_bat_id).value["batter_mlb_id"])
            for at_bat_id in self.get_at_bat_map().keys()
        ]
        return list(set(batter_mlb_ids))

    @property
    def pitch_stats_player_ids(self):
        return [mlb_id for mlb_id in self.get_pitch_stat_map().keys()]

    @property
    def team_data(self):
        return {
            self.away_team_id: self.away_team_data,
            self.home_team_id: self.home_team_data,
        }

    @property
    def bat_boxscore(self):
        return {
            self.away_team_id: self.create_bat_boxscore(self.away_team_id),
            self.home_team_id: self.create_bat_boxscore(self.home_team_id),
        }

    @lru_cache(maxsize=None)
    def create_bat_boxscore(self, team_id):
        team_data = self.team_data[team_id]
        batter_box = OrderedDict()
        for slot in team_data["starting_lineup"]:
            mlb_id = self.get_player_id_map(bbref_id=slot["player_id_br"]).mlb_id
            batter_box[slot["bat_order"]] = self.get_bat_boxscore_for_player(
                mlb_id, slot["def_position"], team_data
            )
        lineup_player_ids = [bat_boxscore["mlb_id"] for bat_boxscore in batter_box.values()]
        sub_player_ids = [
            mlb_id
            for mlb_id in self.get_all_player_ids_by_team(team_data["team_id_br"])
            if mlb_id in self.bat_stats_player_ids and mlb_id not in lineup_player_ids
        ]
        for num, sub_id in enumerate(sub_player_ids, start=1):
            batter_box[f"BN{num}"] = self.get_bat_boxscore_for_player(sub_id, "BN", team_data)
        return batter_box

    def get_player_id_map(self, mlb_id=None, bbref_id=None):
        if not mlb_id and not bbref_id:
            return None
        return (
            PlayerId.find_by_mlb_id(self.db_session, mlb_id)
            if mlb_id
            else PlayerId.find_by_bbref_id(self.db_session, bbref_id)
        )

    def get_all_player_ids_by_team(self, team_id):
        return [
            player_dict["mlb_id"]
            for player_dict in self.player_id_dict.values()
            if player_dict["team_id_bbref"] == team_id
        ]

    def get_bat_boxscore_for_player(self, mlb_id, def_position, team_data):
        player_id = self.get_player_id_map(mlb_id=mlb_id)
        bat_stats = self.get_bat_stats(mlb_id).value
        (at_bats, details) = parse_bat_stats_for_game(bat_stats)
        return {
            "team_id": team_data["team_id_br"],
            "name": player_id.mlb_name,
            "mlb_id": player_id.mlb_id,
            "bbref_id": player_id.bbref_id,
            "def_position": DefensePosition.from_abbrev(def_position),
            "at_bats": at_bats,
            "bat_stats": details,
            "stats_to_date": parse_bat_stats_to_date(bat_stats),
            "at_bat_viewer": self.view_valid_at_bats_for_batter(mlb_id).value,
        }

    @property
    def pitch_boxscore(self):
        return {
            self.away_team_id: self.create_pitch_boxscore(self.away_team_id),
            self.home_team_id: self.create_pitch_boxscore(self.home_team_id),
        }

    @lru_cache(maxsize=None)
    def create_pitch_boxscore(self, team_id):
        team_data = self.team_data[team_id]
        pitcher_box = OrderedDict()
        pitcher_ids = [
            mlb_id
            for mlb_id in self.get_all_player_ids_by_team(team_data["team_id_br"])
            if mlb_id in self.pitch_stats_player_ids
        ]
        rp_count = 0
        for mlb_id in self.get_pitcher_app_order(pitcher_ids):
            player_box = self.get_pitch_boxscore_for_player(mlb_id, team_data)
            if player_box["pitch_app_type"] == "SP":
                dict_index = "SP"
            else:
                rp_count += 1
                dict_index = f"RP{rp_count}"
            pitcher_box[dict_index] = player_box
        return pitcher_box

    def get_pitcher_app_order(self, pitcher_ids):
        pitcher_app_dict = {}
        for mlb_id in pitcher_ids:
            at_bats = self.get_valid_at_bats_for_pitcher(mlb_id).value
            first_pitch = min(at_bat["pbp_table_row_number"] for at_bat in at_bats)
            pitcher_app_dict[mlb_id] = first_pitch
        return sorted(pitcher_ids, key=lambda x: pitcher_app_dict[x])

    def get_pitch_boxscore_for_player(self, mlb_id, team_data):
        player_id = self.get_player_id_map(mlb_id=mlb_id)
        pitch_stats = self.get_pitch_app_stats(mlb_id).value
        game_stats = parse_pitch_app_stats(pitch_stats)
        is_starter = "GS" in game_stats
        return {
            "team_id": team_data["team_id_br"],
            "name": player_id.mlb_name,
            "mlb_id": mlb_id,
            "bbref_id": player_id.bbref_id,
            "pitch_app_type": "SP" if is_starter else "RP",
            "game_results": game_stats,
            "at_bat_viewer": self.view_valid_at_bats_for_pitcher(mlb_id).value,
        }

    @lru_cache(maxsize=None)
    def get_valid_at_bats(self):
        return [
            at_bat_data
            for inning_data in self.innings_list
            for at_bat_data in inning_data["inning_events"]
        ]

    @lru_cache(maxsize=None)
    def get_invalid_at_bats(self):
        return [
            at_bat_data
            for inning_dict in self.invalid_pitchfx.values()
            for at_bat_data in inning_dict.values()
        ]

    def get_all_at_bats(self):
        return self.get_valid_at_bats() + self.get_invalid_at_bats()

    def get_at_bat_map(self):
        return {at_bat["at_bat_id"]: at_bat for at_bat in self.get_all_at_bats()}

    @lru_cache(maxsize=None)
    def get_all_pitch_stats(self):
        return self.away_team_data["pitching_stats"] + self.home_team_data["pitching_stats"]

    def get_pitch_stat_map(self):
        return {
            pitch_stats["pitcher_id_mlb"]: pitch_stats for pitch_stats in self.get_all_pitch_stats()
        }

    @lru_cache(maxsize=None)
    def get_all_bat_stats(self):
        bat_stats = self.away_team_data["batting_stats"] + self.home_team_data["batting_stats"]
        return [bat_stat for bat_stat in bat_stats if bat_stat["total_plate_appearances"]]

    def get_bat_stat_map(self):
        return {bat_stats["batter_id_mlb"]: bat_stats for bat_stats in self.get_all_bat_stats()}

    def view_valid_at_bats_for_batter(self, mlb_id):
        result = self.validate_mlb_id(mlb_id)
        if result.failure:
            return result
        mlb_id = result.value
        at_bats = self.get_valid_at_bats_for_batter(mlb_id).value
        batter_tables = []
        for num, at_bat in enumerate(at_bats, start=1):
            player_name = self.get_player_id_map(mlb_id=mlb_id).mlb_name
            heading = f"At Bat #{num}/{len(at_bats)} for {player_name} in Game {self.bbref_game_id}"
            table_list = self.create_at_bat_table_list(at_bat["at_bat_id"], heading)
            batter_tables.extend(table_list)
        table_viewer = self.create_table_viewer(batter_tables)
        return Result.Ok(table_viewer)

    def validate_mlb_id(self, mlb_id):
        if not isinstance(mlb_id, int):
            try:
                mlb_id = int(mlb_id)
            except ValueError:
                return Result.Fail(f'"{mlb_id}" could not be parsed as a valid int')
        if mlb_id not in self.all_player_mlb_ids:
            error = f"Error: Player MLB ID: {mlb_id} did not appear in game {self.bbref_game_id}"
            return Result.Fail(error)
        return Result.Ok(mlb_id)

    def get_valid_at_bats_for_batter(self, mlb_id):
        result = self.validate_mlb_id(mlb_id)
        if result.failure:
            return result
        mlb_id = result.value
        at_bats = [ab for ab in self.get_valid_at_bats() if ab["batter_id_mlb"] == mlb_id]
        return Result.Ok(at_bats)

    @lru_cache(maxsize=None)
    def create_at_bat_table_list(self, at_bat_id, heading=None):
        at_bat = self.get_at_bat_map().get(at_bat_id)
        message = self.get_at_bat_details(at_bat)
        pitch_seq_desc = format_pitch_sequence_description(at_bat["pitch_sequence_description"])
        chunked_list = make_chunked_list(pitch_seq_desc, chunk_size=AT_BAT_TABLE_MAX_ROWS)
        return [
            DisplayTable(tabulate(chunk, tablefmt="fancy_grid"), heading, message)
            for chunk in chunked_list
        ]

    def get_at_bat_details(self, at_bat):
        pitch_app_stats = self.get_pitch_app_stats(at_bat["pitcher_id_mlb"]).value
        bat_stats = self.get_bat_stats(at_bat["batter_id_mlb"]).value
        return get_at_bat_details(at_bat, pitch_app_stats, bat_stats)

    def get_pitch_app_stats(self, mlb_id):
        result = self.validate_mlb_id(mlb_id)
        if result.failure:
            return result
        mlb_id = result.value
        pitch_stats = self.get_pitch_stat_map().get(mlb_id)
        return Result.Ok(pitch_stats)

    def get_bat_stats(self, mlb_id):
        result = self.validate_mlb_id(mlb_id)
        if result.failure:
            return result
        mlb_id = result.value
        bat_stats = self.get_bat_stat_map().get(mlb_id)
        return Result.Ok(bat_stats["bbref_data"]) if bat_stats else Result.Ok({})

    def create_table_viewer(self, table_list, table_color="bright_cyan"):
        return TableViewer(
            table_list=table_list,
            prompt="Press Enter to return to previous menu",
            confirm_only=True,
            table_color=table_color,
            heading_color="bright_yellow",
            message_color=None,
        )

    def view_valid_at_bats_for_pitcher(self, mlb_id):
        result = self.validate_mlb_id(mlb_id)
        if result.failure:
            return result
        mlb_id = result.value
        at_bats = self.get_valid_at_bats_for_pitcher(mlb_id).value
        at_bats_by_inning = group_and_sort_dict_list(at_bats, "inning_id", "pbp_table_row_number")
        at_bat_tables_by_inning = {}
        all_at_bat_tables = []
        for inning_id, at_bats in at_bats_by_inning.items():
            inning = inning_id[-5:]
            at_bat_tables = []
            for num, at_bat in enumerate(at_bats, start=1):
                player_name = self.get_player_id_map(mlb_id=mlb_id).mlb_name
                heading = f"Inning: {inning}, At Bat #{num}/{len(at_bats)}, P: {player_name}"
                table_list = self.create_at_bat_table_list(at_bat["at_bat_id"], heading)
                at_bat_tables.extend(table_list)
            at_bat_tables_by_inning[inning] = at_bat_tables
            all_at_bat_tables.extend(at_bat_tables)

        innings_viewer = {"ALL": self.create_table_viewer(all_at_bat_tables)}
        for inning, at_bat_tables in at_bat_tables_by_inning.items():
            innings_viewer[inning] = self.create_table_viewer(at_bat_tables)
        return Result.Ok(innings_viewer)

    def get_valid_at_bats_for_pitcher(self, mlb_id):
        result = self.validate_mlb_id(mlb_id)
        if result.failure:
            return result
        mlb_id = result.value
        at_bats = [
            at_bat for at_bat in self.get_valid_at_bats() if at_bat["pitcher_id_mlb"] == mlb_id
        ]
        return Result.Ok(at_bats)

    def view_at_bats_by_inning(self):
        all_at_bat_tables = []
        at_bat_tables_by_inning = {}
        for inning_dict in self.get_innings_sorted():
            inning = inning_dict["inning_id"][-5:]
            inning_tables = []
            for num, at_bat in enumerate(inning_dict["at_bats"], start=1):
                heading = f"Inning: {inning}, At Bat #{num}/{len(inning_dict['at_bats'])}"
                table_list = self.create_at_bat_table_list(at_bat["at_bat_id"], heading)
                inning_tables.extend(table_list)
            at_bat_tables_by_inning[inning] = inning_tables
            all_at_bat_tables.extend(inning_tables)

        innings_viewer = {"ALL": self.create_table_viewer(all_at_bat_tables)}
        for inning, at_bat_tables in at_bat_tables_by_inning.items():
            innings_viewer[inning] = self.create_table_viewer(at_bat_tables)
        return Result.Ok(innings_viewer)

    def get_innings_sorted(self):
        at_bats_by_inning = group_and_sort_dict_list(
            self.get_valid_at_bats(), "inning_id", "pbp_table_row_number"
        )
        innings_unsorted = [
            {"inning_id": inning_id, "at_bats": at_bats}
            for inning_id, at_bats in at_bats_by_inning.items()
        ]
        return get_innings_sorted(innings_unsorted)

    def view_game_meta_info(self):
        meta_info = deepcopy(self.game_meta_info)
        meta_info.pop("umpires")
        meta_info.pop("game_time_hour")
        meta_info.pop("game_time_minute")
        game_start_time = meta_info.pop("game_date_time_str")
        table_rows = [[name, value] for name, value in meta_info.items()]
        table_rows.insert(0, ["game_start_time", game_start_time])
        table = tabulate(table_rows)
        heading = f"Meta Information for game {self.bbref_game_id}"
        return self.create_table_viewer([DisplayTable(table, heading)])

    def view_invalid_at_bats(self):
        pass

    def compare_removed_pitchfx(self):
        pass

    @lru_cache(maxsize=None)
    def get_all_pitchfx(self):
        pfx_dict = defaultdict(list)
        all_pitchfx = [at_bat["pitchfx"] for at_bat in self.get_all_at_bats()]
        all_pitchfx.extend(self.get_duplicate_guid_pfx())
        all_pitchfx.extend(self.get_removed_pfx())
        for pfx in flatten_list2d(all_pitchfx):
            pfx_dict[pfx["pitch_app_id"]].append(pfx)
        return pfx_dict

    def get_duplicate_guid_pfx(self):
        return [
            at_bat["pitchfx"]
            for inning_dict in self.duplicate_guids.values()
            for at_bat in inning_dict.values()
        ]

    def get_removed_pfx(self):
        return [
            at_bat["pitchfx"]
            for inning_dict in self.removed_pitchfx.values()
            for at_bat in inning_dict.values()
        ]

    def get_pitch_mix(self, mlb_id):
        result = self.validate_mlb_id(mlb_id)
        if result.failure:
            return result
        mlb_id = result.value
        pitch_app_id = f"{self.bbref_game_id}_{mlb_id}"
        pitcher_data = AllPlayerData(self.app, mlb_id)
        pitch_app = PitchAppScrapeStatus.find_by_pitch_app_id(self.db_session, pitch_app_id)
        return {
            "pitch_app": pitch_app.pitch_mix,
            "by_year": pitcher_data.pitch_mix_by_year(),
            "all": pitcher_data.pitch_mix(),
            "bat_r": pitcher_data.pitch_mix_right(),
            "bat_l": pitcher_data.pitch_mix_left(),
        }

    def get_matchup_details(self):
        away_record = get_team_record_for_linescore(self.away_team_data)
        home_record = get_team_record_for_linescore(self.home_team_data)
        matchup = f"{self.away_team.name} ({away_record}) vs {self.home_team.name} ({home_record})"
        game_time = self.game_datetime.strftime(DT_AWARE_VERBOSE)
        return f"{game_time}\n{matchup}\n"

    def get_linescore(self):
        return get_linescore_tables(self.away_team_data, self.home_team_data, self.innings_list)


def get_linescore_tables(away_team_data, home_team_data, innings_list):
    table_list = []
    (away_team_runs_chunked, home_team_runs_chunked) = get_runs_by_inning(innings_list)
    for i in range(len(away_team_runs_chunked)):
        start_inning = i * 9 + 1
        last_chunk = 1 == len(away_team_runs_chunked) - 1
        table = create_linescore_table(
            away_team_data["team_id_br"],
            away_team_runs_chunked[i],
            get_team_totals_for_linescore(away_team_data),
            home_team_data["team_id_br"],
            home_team_runs_chunked[i],
            get_team_totals_for_linescore(home_team_data),
            start_inning,
            last_chunk,
        )
        table_list.append(table)
    return "\n\n".join(table_list)


def get_team_record_for_linescore(team_data):
    team_wins = team_data["total_wins_before_game"]
    team_losses = team_data["total_losses_before_game"]
    return f"{team_wins}-{team_losses}"


def get_team_totals_for_linescore(team_data):
    team_total_runs = team_data["total_runs_scored_by_team"]
    team_hits = team_data["total_hits_by_team"]
    team_errors = team_data["total_errors_by_team"]
    team_totals = [team_total_runs, team_hits, team_errors]
    return team_totals


def get_runs_by_inning(innings_list):
    is_top_half = True
    away_team_runs = []
    home_team_runs = []
    for inning in innings_list:
        total_runs_this_inning = get_total_runs_in_inning(inning["inning_events"])
        if is_top_half:
            away_team_runs.append(total_runs_this_inning)
        else:
            home_team_runs.append(total_runs_this_inning)
        is_top_half = not is_top_half
    if len(away_team_runs) > len(home_team_runs):
        home_team_runs.append("X")
    away_team_runs_chunked = make_chunked_list(away_team_runs, chunk_size=9)
    home_team_runs_chunked = make_chunked_list(home_team_runs, chunk_size=9)
    return (away_team_runs_chunked, home_team_runs_chunked)


def get_total_runs_in_inning(inning_at_bats):
    return sum(ab["runs_outs_result"].count("R") for ab in inning_at_bats)


def create_linescore_table(
    away_team_id,
    away_team_runs_by_inning,
    away_team_totals,
    home_team_id,
    home_team_runs_by_inning,
    home_team_totals,
    start_inning,
    last_chunk,
):
    end_inning = start_inning + len(away_team_runs_by_inning)
    inning_numbers = get_linescore_inning_numbers(start_inning, end_inning)
    if start_inning != 1 and last_chunk:
        away_team_runs_by_inning = add_filler_columns(away_team_runs_by_inning)
        home_team_runs_by_inning = add_filler_columns(home_team_runs_by_inning)
        inning_numbers = add_filler_columns(inning_numbers)
    total_headers = [f"{' '*(4-len(letter))}{letter}" for letter in ["R", "H", "E"]]
    column_headers = [f"{' '*(4-len(str(num)))}{num}" for num in inning_numbers]
    away_numbers = [f"{' '*(4-len(str(num)))}{num}" for num in away_team_runs_by_inning]
    away_totals = [f"{' '*(4-len(str(num)))}{num}" for num in away_team_totals]
    home_numbers = [f"{' '*(4-len(str(num)))}{num}" for num in home_team_runs_by_inning]
    home_totals = [f"{' '*(4-len(str(num)))}{num}" for num in home_team_totals]
    return (
        f"   {''.join(column_headers)}{''.join(total_headers)}\n"
        f"----{'----'*9}{'----'*3}\n"
        f"{away_team_id}{''.join(away_numbers)}{''.join(away_totals)}\n"
        f"{home_team_id}{''.join(home_numbers)}{''.join(home_totals)}"
    )


def add_filler_columns(list_to_pad):
    pad_length = 9 - len(list_to_pad)
    for i in range(pad_length):
        list_to_pad.append("  ")
    return list_to_pad


def get_linescore_inning_numbers(start, end):
    inning_numbers = []
    for i in range(start, end):
        pad_len = 1 if i < 10 else 0
        inning_numbers.append(f"{' '*pad_len}{i}")
    return inning_numbers


def get_innings_sorted(innings_unsorted):
    inning_ids = set([inning["inning_id"] for inning in innings_unsorted])
    inning_weights = {inning_id: get_inning_weight(inning_id) for inning_id in inning_ids}
    return sorted(innings_unsorted, key=lambda x: inning_weights[x["inning_id"]])


def get_inning_weight(inning_id):
    match = INNING_LABEL_REGEX.search(inning_id)
    if not match:
        return 0
    inn_half = match[1]
    inn_number = int(match[2])
    top_or_bottom = -1 if inn_half == "TOP" else 0
    return inn_number * 2 + top_or_bottom


def get_at_bat_details(at_bat, pitch_app_stats, bat_stats):
    pitch_stats = parse_pitch_app_stats(pitch_app_stats)
    (at_bats, details) = parse_bat_stats_for_game(bat_stats)
    inning = inning_number_to_string(at_bat["inning_id"][-5:])
    score = parse_score_for_at_bat(at_bat)
    outs_plural = "out" if at_bat["outs_before_play"] == 1 else "outs"
    outs = f"{at_bat['outs_before_play']} {outs_plural}"
    at_bat_result = parse_at_bat_result(at_bat)
    return (
        f"Pitcher..: {at_bat['pitcher_name']} ({pitch_stats})\n"
        f"Batter...: {at_bat['batter_name']} ({at_bats}, {details})\n"
        f"Score....: {score}\n"
        f"Inning...: {inning}, {outs}\n"
        f"On Base..: {at_bat['runners_on_base']}\n"
        f"Result...: {at_bat_result}"
    )


def parse_score_for_at_bat(at_bat):
    if not at_bat["pitchfx"]:
        return at_bat["score"]
    pfx = at_bat["pitchfx"][0]
    match = INNING_LABEL_REGEX.search(at_bat["inning_id"])
    if not match:
        return 0
    inn_half = match[1]
    batter_team_id = get_bbref_team_id(pfx["opponent_team_id_bb"])
    pitcher_team_id = get_bbref_team_id(pfx["pitcher_team_id_bb"])
    split = at_bat["score"].split("-")
    return (
        f"{batter_team_id} {split[0]} - {pitcher_team_id} {split[1]}"
        if inn_half == "TOP"
        else f"{pitcher_team_id} {split[1]} - {batter_team_id} {split[0]}"
    )


def parse_at_bat_result(at_bat):
    outcome = at_bat["play_description"]
    if at_bat["pitchfx"]:
        outcome = at_bat["pitchfx"][0]["des"]
    total_outs = at_bat["runs_outs_result"].count("O")
    total_runs = at_bat["runs_outs_result"].count("R")
    if not total_outs and not total_runs:
        return outcome
    outs_plural = "out" if total_outs == 1 else "outs"
    outs = f"{total_outs} {outs_plural}" if total_outs else ""
    runs_plural = "run" if total_runs == 1 else "runs"
    runs = f"{total_runs} {runs_plural} scored" if total_runs else ""
    separator = ", " if total_outs and total_runs else ""
    return f"{outcome} ({outs}{separator}{runs})"


def parse_bat_stats_for_game(bat_stats):
    if not bat_stats or not bat_stats["plate_appearances"]:
        return ("0/0", "")
    at_bats = f"{bat_stats['hits']}/{bat_stats['at_bats']}"
    stat_line = get_detailed_bat_stats(bat_stats)
    return (at_bats, stat_line)


def get_detailed_bat_stats(bat_stats):
    stats = homeruns = doubles = triples = stolen_bases = None
    stat_list = []
    if bat_stats["details"]:
        stats = parse_bat_stat_details(bat_stats["details"])
        homeruns = stats.pop("HR", None)
        triples = stats.pop("3B", None)
        doubles = stats.pop("2B", None)
        stolen_bases = stats.pop("SB", None)
    if homeruns:
        stat_list.append(homeruns)
    if triples:
        stat_list.append(triples)
    if doubles:
        stat_list.append(doubles)
    if bat_stats["rbis"]:
        rbis = bat_stats["rbis"] if bat_stats["rbis"] > 1 else ""
        stat_list.append(f"{rbis}RBI")
    if bat_stats["runs_scored"]:
        runs_scored = bat_stats["runs_scored"] if bat_stats["runs_scored"] > 1 else ""
        stat_list.append(f"{runs_scored}R")
    if stolen_bases:
        stat_list.append(stolen_bases)
    if bat_stats["strikeouts"]:
        strikeouts = bat_stats["strikeouts"] if bat_stats["strikeouts"] > 1 else ""
        stat_list.append(f"{strikeouts}K")
    if bat_stats["bases_on_balls"]:
        bases_on_balls = bat_stats["bases_on_balls"] if bat_stats["bases_on_balls"] > 1 else ""
        stat_list.append(f"{bases_on_balls}BB")
    if stats:
        stat_list.extend([remaining_stat for remaining_stat in stats.values()])
    return ", ".join(stat_list)


def parse_bat_stat_details(details):
    return {stat["stat"]: parse_single_bat_stat(stat) for stat in details}


def parse_single_bat_stat(stat):
    return f"{stat['count']}{stat['stat']}" if stat["count"] > 1 else stat["stat"]


def parse_bat_stats_to_date(bat_stats):
    return (
        (
            f"{format_stat_decimal(bat_stats['avg_to_date'])}/"
            f"{format_stat_decimal(bat_stats['obp_to_date'])}/"
            f"{format_stat_decimal(bat_stats['slg_to_date'])}/"
            f"{format_stat_decimal(bat_stats['ops_to_date'])}"
        )
        if bat_stats
        else ""
    )


def format_stat_decimal(stat_value):
    stat_with_leading_zero = f"{stat_value:0.3f}"
    return stat_with_leading_zero[1:]


def parse_pitch_app_stats(pitch_stats):
    if not pitch_stats or not pitch_stats["bbref_data"]:
        return ""
    bbref_stats = pitch_stats["bbref_data"]
    ip = bbref_stats["innings_pitched"]
    r = bbref_stats["runs"]
    er = bbref_stats["earned_runs"]
    runs = f"{er}ER" if er == r else f"{r}R, {er}ER"
    stat_list = []
    if bbref_stats["hits"]:
        h = bbref_stats["hits"] if bbref_stats["hits"] > 1 else ""
        stat_list.append(f"{h}H")
    if bbref_stats["strikeouts"]:
        k = bbref_stats["strikeouts"] if bbref_stats["strikeouts"] > 1 else ""
        stat_list.append(f"{k}K")
    if bbref_stats["bases_on_balls"]:
        bb = bbref_stats["bases_on_balls"] if bbref_stats["bases_on_balls"] > 1 else ""
        stat_list.append(f"{bb}BB")
    details = f", {', '.join(stat_list)}" if stat_list else ""
    game_score = f" (GS: {bbref_stats['game_score']})" if bbref_stats["game_score"] > 0 else ""
    return f"{ip} IP, {runs}{details}{game_score}"


def format_pitch_sequence_description(pitch_seq_desc):
    return [format_event_description(seq) for seq in pitch_seq_desc]


def format_event_description(event_desc):
    if (
        not event_desc[0]
        and not event_desc[2]
        and event_desc[1].startswith("(")
        and event_desc[1].endswith(")")
    ):
        event_desc[1] = event_desc[1][1:]
        event_desc[1] = event_desc[1][:-1]
    desc_lines = [wrap_text(s, max_len=AT_BAT_DESC_MAX_WIDTH) for s in event_desc[1].split("\n")]
    event_desc[1] = "\n".join(desc_lines)
    event_desc[1] = event_desc[1].replace("\n\n", "\n")
    return event_desc
