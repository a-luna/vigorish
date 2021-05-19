from collections import defaultdict
from copy import deepcopy
from datetime import datetime
from functools import cached_property

from tabulate import tabulate

import vigorish.database as db
from vigorish.cli.components.viewers import create_display_table, create_table_viewer, DisplayTable
from vigorish.data.player_data import PlayerData
from vigorish.enums import DataSet, DefensePosition, VigFile
from vigorish.util.dt_format_strings import DT_AWARE, DT_AWARE_VERBOSE
from vigorish.util.exceptions import ScrapedDataException
from vigorish.util.list_helpers import flatten_list2d, group_and_sort_dict_list, make_chunked_list
from vigorish.util.regex import INNING_LABEL_REGEX
from vigorish.util.result import Result
from vigorish.util.string_helpers import (
    get_bbref_team_id,
    inning_number_to_string,
    validate_bbref_game_id,
    wrap_text,
)

AT_BAT_TABLE_MAX_ROWS = 8
AT_BAT_DESC_MAX_WIDTH = 35


class GameData:
    def __init__(self, app, bbref_game_id):

        # TODO: Create views for pitch_type by handedness by year
        # TODO: Create views for batter/pitcher stats to date, career, etc
        # TODO: Create view for invalid pitchfx
        # TODO: Create view to demonstrate how removed pfx is duplicative

        self.app = app
        self.db_engine = app.db_engine
        self.db_session = app.db_session
        self.scraped_data = app.scraped_data
        combined_data = self.scraped_data.get_combined_game_data(bbref_game_id)
        if not combined_data:
            raise ScrapedDataException(file_type=VigFile.COMBINED_GAME_DATA, data_set=DataSet.ALL, url_id=bbref_game_id)
        self.last_modified = combined_data["last_modified"]
        self.bbref_game_id = combined_data["bbref_game_id"]
        self.pitchfx_vs_bbref_audit = combined_data["pitchfx_vs_bbref_audit"]
        self.game_meta_info = combined_data["game_meta_info"]
        self.away_team_data = combined_data["away_team_data"]
        self.home_team_data = combined_data["home_team_data"]
        self.innings_list = combined_data["play_by_play_data"]
        self.removed_pitchfx = combined_data["removed_pitchfx"]
        self.invalid_pitchfx = combined_data["invalid_pitchfx"]
        self.player_id_dict = combined_data["player_id_dict"]
        self.all_player_mlb_ids = [id_map["mlb_id"] for id_map in self.player_id_dict.values()]
        self.game_date = validate_bbref_game_id(self.bbref_game_id).value["game_date"]
        self.player_data_cache = {}

    @property
    def away_team_id(self):
        return self.away_team_data["team_id_br"]

    @property
    def home_team_id(self):
        return self.home_team_data["team_id_br"]

    @property
    def away_team(self):
        return db.Team.find_by_team_id_and_year(self.db_session, self.away_team_id, self.game_date.year)

    @property
    def home_team(self):
        return db.Team.find_by_team_id_and_year(self.db_session, self.home_team_id, self.game_date.year)

    @property
    def game_datetime(self):
        return datetime.strptime(self.game_meta_info["game_date_time_str"], DT_AWARE)

    @property
    def team_name(self):
        return {
            self.away_team_id: self.away_team.name,
            self.home_team_id: self.home_team.name,
        }

    @property
    def team_data(self):
        return {
            self.away_team_id: self.away_team_data,
            self.home_team_id: self.home_team_data,
        }

    @property
    def all_at_bats(self):
        return self.valid_at_bats + self.invalid_at_bats

    @property
    def at_bat_map(self):
        return {at_bat["at_bat_id"]: at_bat for at_bat in self.all_at_bats}

    @property
    def pitch_stats_map(self):
        return {stats["pitcher_id_mlb"]: stats for stats in self.all_pitch_stats}

    @property
    def bat_stats_map(self):
        return {stats["batter_id_mlb"]: stats for stats in self.all_bat_stats}

    @property
    def bat_stats_player_ids(self):
        return list(self.bat_stats_map.keys())

    @property
    def pitch_stats_player_ids(self):
        return list(self.pitch_stats_map.keys())

    @property
    def winning_pitcher(self):
        pitcher_id_bbref = (
            self.away_team_data["pitcher_of_record"]
            if self.away_team_data["team_won"]
            else self.home_team_data["pitcher_of_record"]
        )
        return self.get_player_id_map(bbref_id=pitcher_id_bbref).mlb_id

    @property
    def losing_pitcher(self):
        pitcher_id_bbref = (
            self.away_team_data["pitcher_of_record"]
            if self.home_team_data["team_won"]
            else self.home_team_data["pitcher_of_record"]
        )
        return self.get_player_id_map(bbref_id=pitcher_id_bbref).mlb_id

    @property
    def pitcher_earned_save(self):
        pitcher_id_bbref = (
            self.away_team_data["pitcher_earned_save"]
            if self.away_team_data["team_won"] and self.away_team_data["pitcher_earned_save"]
            else self.home_team_data["pitcher_earned_save"]
            if self.home_team_data["team_won"] and self.home_team_data["pitcher_earned_save"]
            else None
        )
        return self.get_player_id_map(bbref_id=pitcher_id_bbref).mlb_id if pitcher_id_bbref else None

    @cached_property
    def valid_at_bats(self):
        return [at_bat_data for inning_data in self.innings_list for at_bat_data in inning_data["inning_events"]]

    @cached_property
    def invalid_at_bats(self):
        return [at_bat_data for inning_dict in self.invalid_pitchfx.values() for at_bat_data in inning_dict.values()]

    @cached_property
    def player_substitutions(self):
        return [
            event
            for at_bat in self.valid_at_bats
            for event in at_bat["pbp_events"]
            if event["event_type"] == "SUBSTITUTION"
        ]

    @cached_property
    def all_pitch_stats(self):
        return self.away_team_data["pitching_stats"] + self.home_team_data["pitching_stats"]

    @cached_property
    def all_bat_stats(self):
        bat_stats = self.away_team_data["batting_stats"] + self.home_team_data["batting_stats"]
        return [bat_stat for bat_stat in bat_stats if bat_stat["total_plate_appearances"]]

    @cached_property
    def bat_boxscore(self):
        return {
            self.away_team_id: self.create_bat_boxscore(self.away_team_id),
            self.home_team_id: self.create_bat_boxscore(self.home_team_id),
        }

    @cached_property
    def pitch_boxscore(self):
        return {
            self.away_team_id: self.create_pitch_boxscore(self.away_team_id),
            self.home_team_id: self.create_pitch_boxscore(self.home_team_id),
        }

    @cached_property
    def all_pitchfx(self):
        pfx_dict = defaultdict(list)
        all_pitchfx = [at_bat["pitchfx"] for at_bat in self.all_at_bats]
        all_pitchfx.extend(self.get_removed_pfx())
        for pfx in flatten_list2d(all_pitchfx):
            pfx_dict[pfx["pitch_app_id"]].append(self._update_pfx_attributes(pfx))
        return pfx_dict

    @cached_property
    def starting_pitcher_mlb_ids(self):
        return [
            mlb_id
            for mlb_id, pitch_stats in self.pitch_stats_map.items()
            if check_pitch_stats_for_game_score(pitch_stats)
        ]

    @cached_property
    def extra_innings(self):
        return len(self.get_inning_numbers_sorted()) > 9

    def get_game_data(self):
        return {
            "game_id": self.bbref_game_id,
            "linescore": self.get_html_linescore(),
            "pitcher_results": self.get_pitcher_results(),
            "extra_innings": self.extra_innings,
        }

    def get_boxscore_data(self):
        boxscore_data = {
            "last_modified": self.last_modified,
            "game_id": self.bbref_game_id,
            "away_team": self.get_team_data(self.away_team_id),
            "home_team": self.get_team_data(self.home_team_id),
            "extra_innings": self.extra_innings,
            "game_meta": self.get_game_meta_info(),
            "linescore": self.get_html_linescore(),
            "inning_summaries": self._get_inning_summaries(),
        }
        if self.extra_innings:
            boxscore_data["linescore_complete"] = self.get_html_linescore(condensed=False)
        return boxscore_data

    def _get_inning_summaries(self):
        return {
            inning["inning_id"]: {
                "inning_label": inning["inning_label"],
                "begin_inning_summary": inning["begin_inning_summary"],
                "end_inning_summary": inning["end_inning_summary"],
                "inning_totals": inning["inning_totals"],
            }
            for inning in self.innings_list
        }

    def get_team_data(self, team_id):
        (team_bat_boxscore, team_pitch_boxscore) = self._prepare_team_data(team_id)
        return {
            "team_id": team_id,
            "team_name": self.team_name[team_id],
            "total_wins": self.team_data[team_id]["total_wins_before_game"],
            "total_losses": self.team_data[team_id]["total_losses_before_game"],
            "runs_scored_by_team": self.team_data[team_id]["total_runs_scored_by_team"],
            "runs_scored_by_opponent": self.team_data[team_id]["total_runs_scored_by_opponent"],
            "team_won": self.team_data[team_id]["team_won"],
            "pitcher_of_record": self.team_data[team_id]["pitcher_of_record"],
            "pitcher_earned_save": self.team_data[team_id]["pitcher_earned_save"],
            "batting": team_bat_boxscore,
            "pitching": team_pitch_boxscore,
        }

    def _prepare_team_data(self, team_id):
        team_data = deepcopy(self.team_data[team_id])
        team_data.pop("starting_lineup")
        bat_map = {b["batter_id_mlb"]: b for b in team_data.pop("batting_stats")}
        bat_boxscore = self.bat_boxscore[team_id]
        for box in bat_boxscore.values():
            box["def_position"] = int(box["def_position"])
            box["total_pbp_events"] = bat_map[box["mlb_id"]]["total_pbp_events"]
            box["total_incomplete_at_bats"] = bat_map[box["mlb_id"]]["total_incomplete_at_bats"]
            box["total_plate_appearances"] = bat_map[box["mlb_id"]]["total_plate_appearances"]
            box["at_bat_results"] = self._summarize_all_at_bats_for_player(box["mlb_id"])
            box["incomplete_at_bat_ids"] = bat_map[box["mlb_id"]]["incomplete_at_bat_ids"]
            box["substitutions"] = self._find_player_sub_events(box["mlb_id"])
            box["bbref_data"] = bat_map[box["mlb_id"]]["bbref_data"]
        pitch_map = {p["pitcher_id_mlb"]: p for p in team_data.pop("pitching_stats")}
        pitch_boxscore = self.pitch_boxscore[team_id]
        for box in pitch_boxscore.values():
            box["pitch_app_id"] = pitch_map[box["mlb_id"]]["pitch_app_id"]
            box["pitch_count_by_inning"] = pitch_map[box["mlb_id"]]["pitch_count_by_inning"]
            box["pitch_count_by_pitch_type"] = pitch_map[box["mlb_id"]]["pitch_count_by_pitch_type"]
            box["at_bat_ids"] = pitch_map[box["mlb_id"]]["pitch_app_pitchfx_audit"]["at_bat_ids_pitchfx_complete"]
            box["substitutions"] = self._find_player_sub_events(box["mlb_id"])
            box["bbref_data"] = pitch_map[box["mlb_id"]]["bbref_data"]
            box["inning_totals"] = self._summarize_pitch_app(box["mlb_id"])
        return (bat_boxscore, pitch_boxscore)

    def _summarize_all_at_bats_for_player(self, mlb_id):
        return [self._summarize_at_bat(ab) for ab in self.valid_at_bats if ab["batter_id_mlb"] == mlb_id]

    def _summarize_at_bat(self, at_bat):
        pbp_events = [event for event in at_bat["pbp_events"] if event["event_type"] == "AT_BAT"]
        pbp_events.sort(key=lambda x: x["pbp_table_row_number"])
        return {
            "at_bat_id": at_bat["at_bat_id"],
            "pbp_table_row_number": at_bat["pbp_table_row_number"],
            "batter_name": at_bat["batter_name"],
            "pitcher_name": at_bat["pitcher_name"],
            "inning": pbp_events[-1]["inning_label"],
            "runners_on_base": pbp_events[-1]["runners_on_base"],
            "outs": pbp_events[-1]["outs_before_play"],
            "play_description": pbp_events[-1]["play_description"],
            "pitch_sequence": pbp_events[-1]["pitch_sequence"],
        }

    def _find_player_sub_events(self, mlb_id):
        player_id = self.get_player_id_map(mlb_id=mlb_id)
        bbref_id = player_id.bbref_id
        return [
            sub_event
            for sub_event in self.player_substitutions
            if bbref_id in [sub_event["incoming_player_id_br"], sub_event["outgoing_player_id_br"]]
        ]

    def _summarize_pitch_app(self, mlb_id):
        pfx = self.get_pfx_for_pitcher(mlb_id).value
        pfx_by_inning = group_and_sort_dict_list(pfx, "inning", "pitch_id")
        at_bats = [ab for ab in self.valid_at_bats if ab["pitcher_id_mlb"] == mlb_id]
        at_bats_by_inning = group_and_sort_dict_list(at_bats, "inning_id", "pbp_table_row_number")
        inning_totals = []
        for (inning_pfx, inning_at_bats) in zip(pfx_by_inning.values(), at_bats_by_inning.values()):
            totals = {
                "outs": sum(ab["runs_outs_result"].count("O") for ab in inning_at_bats),
                "hits": sum(pfx["ab_result_hit"] for pfx in inning_pfx),
                "runs": sum(ab["runs_outs_result"].count("R") for ab in inning_at_bats),
                "bb": sum(pfx["ab_result_bb"] for pfx in inning_pfx),
                "so": sum(pfx["ab_result_k"] for pfx in inning_pfx),
                "bf": len({pfx["batter_id_mlb"] for pfx in inning_pfx}),
                "pitch_count": len(inning_pfx),
                "strikes": len([pfx for pfx in inning_pfx if pfx["basic_type"] in ["S", "X"]]),
            }
            inning_totals.append(totals)
        return dict(zip(list(pfx_by_inning.keys()), inning_totals))

    def get_all_at_bats_no_pfx(self):
        return list(map(self._prepare_at_bat_for_api_response, deepcopy(self.valid_at_bats)))

    def get_pbp_for_at_bat(self, at_bat_id):
        at_bat_copy = deepcopy(self.at_bat_map[at_bat_id])
        return self._prepare_at_bat_for_api_response(at_bat_copy)

    def _prepare_at_bat_for_api_response(self, at_bat):
        pfx = at_bat.pop("pitchfx")
        pfx_audit = at_bat.pop("at_bat_pitchfx_audit")
        at_bat["inning_label"] = at_bat["pbp_events"][0]["inning_label"]
        at_bat["pitcher_throws"] = pfx[0]["p_throws"] if pfx else ""
        at_bat["batter_stance"] = pfx[0]["stand"] if pfx else ""
        at_bat["total_pitches"] = pfx_audit["pitch_count_bbref"]
        at_bat["pfx_complete"] = pfx_audit["pitch_count_bbref"] == pfx_audit["pitch_count_pitchfx"]
        at_bat["final_count_balls"] = pfx[-1]["balls"] if pfx else 0
        at_bat["final_count_strikes"] = pfx[-1]["strikes"] if pfx else 0
        at_bat["pfx_des"] = pfx[0]["des"] if pfx else ""
        return at_bat

    def get_pfx_for_pitcher(self, mlb_id):
        result = self.validate_mlb_id(mlb_id)
        if result.failure:
            return result
        mlb_id = result.value
        pfx = [at_bat["pitchfx"] for at_bat in self.valid_at_bats if at_bat["pitcher_id_mlb"] == mlb_id]
        return Result.Ok(self._prepare_pfx_for_api_response(flatten_list2d(pfx)))

    def get_pfx_for_at_bat(self, at_bat_id):
        if at_bat_id not in self.at_bat_map:
            return None
        pfx = self.at_bat_map[at_bat_id]["pitchfx"]
        return self._prepare_pfx_for_api_response(pfx)

    def _prepare_pfx_for_api_response(self, pfx):
        pfx = list(map(db.PitchFx.update_pfx_dict, deepcopy(pfx)))
        pfx = list(map(self._update_pfx_attributes, pfx))
        return sorted(pfx, key=lambda x: x["time_pitch_thrown_utc"])

    def _update_pfx_attributes(self, pfx):
        pfx["is_sp"] = pfx["pitcher_id"] in self.starting_pitcher_mlb_ids
        pfx["is_rp"] = pfx["pitcher_id"] not in self.starting_pitcher_mlb_ids
        return pfx

    def get_player_data(self, mlb_id):
        player_data = self.player_data_cache.get(mlb_id)
        if not player_data:
            player_data = PlayerData(self.app, mlb_id)
            self.player_data_cache[mlb_id] = player_data
        return player_data

    def create_bat_boxscore(self, team_id):
        team_data = self.team_data[team_id]
        batter_box = {}
        for slot in team_data["starting_lineup"]:
            mlb_id = self.get_player_id_map(bbref_id=slot["player_id_br"]).mlb_id
            bat_order = slot["bat_order"]
            def_pos = slot["def_position"]
            batter_box[slot["bat_order"]] = self.get_bat_boxscore_for_player(mlb_id, bat_order, def_pos, team_data)
        lineup_player_ids = [bat_boxscore["mlb_id"] for bat_boxscore in batter_box.values()]
        sub_player_ids = [
            mlb_id
            for mlb_id in self.get_all_player_ids_by_team(team_data["team_id_br"])
            if mlb_id in self.bat_stats_player_ids and mlb_id not in lineup_player_ids
        ]
        for num, sub_id in enumerate(sub_player_ids, start=1):
            batter_box[f"BN{num}"] = self.get_bat_boxscore_for_player(sub_id, 0, "BN", team_data)
        return batter_box

    def get_player_id_map(self, mlb_id=None, bbref_id=None):
        if not mlb_id and not bbref_id:
            return None
        return (
            db.PlayerId.find_by_mlb_id(self.db_session, mlb_id)
            if mlb_id
            else db.PlayerId.find_by_bbref_id(self.db_session, bbref_id)
        )

    def get_all_player_ids_by_team(self, team_id):
        return [
            player_dict["mlb_id"]
            for player_dict in self.player_id_dict.values()
            if player_dict["team_id_bbref"] == team_id
        ]

    def get_bat_boxscore_for_player(self, mlb_id, bat_order, def_pos, team_data):
        player_id = self.get_player_id_map(mlb_id=mlb_id)
        bat_stats = self.get_bat_stats(mlb_id).value
        (at_bats, details) = parse_bat_stats_for_game(bat_stats)
        def_position = DefensePosition.from_abbrev(def_pos)
        return {
            "team_id": team_data["team_id_br"],
            "name": player_id.mlb_name,
            "mlb_id": player_id.mlb_id,
            "bbref_id": player_id.bbref_id,
            "is_starter": def_position.is_starter,
            "bat_order": bat_order if def_position.is_starter else 0,
            "def_position": def_position,
            "at_bats": at_bats,
            "bat_stats": details,
            "stats_to_date": parse_bat_stats_to_date(bat_stats),
        }

    def create_pitch_boxscore(self, team_id):
        team_data = self.team_data[team_id]
        pitcher_box = {}
        pitcher_ids = [
            mlb_id
            for mlb_id in self.get_all_player_ids_by_team(team_data["team_id_br"])
            if mlb_id in self.pitch_stats_player_ids
        ]
        rp_count = 0
        for mlb_id in self.get_pitcher_app_order(pitcher_ids):
            player_box = self.get_pitch_boxscore_for_player(mlb_id, team_data)
            if player_box["pitch_app_type"] == "SP":
                pitcher_box["SP"] = player_box
            else:
                rp_count += 1
                pitcher_box[f"RP{rp_count}"] = player_box
        return pitcher_box

    def get_pitcher_app_order(self, pitcher_ids):
        pitcher_app_dict = {
            mlb_id: min(at_bat["pbp_table_row_number"] for at_bat in self.get_valid_at_bats_for_pitcher(mlb_id).value)
            for mlb_id in pitcher_ids
        }
        return sorted(pitcher_ids, key=lambda x: pitcher_app_dict[x])

    def get_pitch_boxscore_for_player(self, mlb_id, team_data):
        player_id = self.get_player_id_map(mlb_id=mlb_id)
        pitch_stats = self.get_pitch_app_stats(mlb_id).value
        game_stats = parse_pitch_app_stats(pitch_stats)
        is_starter = mlb_id in self.starting_pitcher_mlb_ids
        return {
            "team_id": team_data["team_id_br"],
            "name": player_id.mlb_name,
            "mlb_id": mlb_id,
            "bbref_id": player_id.bbref_id,
            "pitch_app_type": "SP" if is_starter else "RP",
            "game_results": game_stats,
        }

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
        table_viewer = create_table_viewer(batter_tables)
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
        at_bats = [ab for ab in self.valid_at_bats if ab["batter_id_mlb"] == mlb_id]
        return Result.Ok(at_bats)

    def create_at_bat_table_list(self, at_bat_id, heading=None):
        at_bat = self.at_bat_map.get(at_bat_id)
        if not at_bat:
            return []
        message = self.get_at_bat_details(at_bat)
        pitch_seq_desc = format_pitch_sequence_description(at_bat["pitch_sequence_description"])
        chunked_list = make_chunked_list(pitch_seq_desc, chunk_size=AT_BAT_TABLE_MAX_ROWS)
        return [create_display_table(chunk, heading, message) for chunk in chunked_list]

    def get_at_bat_details(self, at_bat):
        pitch_app_stats = self.get_pitch_app_stats(at_bat["pitcher_id_mlb"]).value
        bat_stats = self.get_bat_stats(at_bat["batter_id_mlb"]).value
        return get_at_bat_details(at_bat, pitch_app_stats, bat_stats["bbref_data"])

    def get_pitch_app_stats(self, mlb_id):
        result = self.validate_mlb_id(mlb_id)
        if result.failure:
            return result
        mlb_id = result.value
        pitch_stats = self.pitch_stats_map.get(mlb_id, {})
        if pitch_stats:
            sp_mlb_ids = self.starting_pitcher_mlb_ids
            pitch_stats["is_sp"] = pitch_stats["pitcher_id_mlb"] in sp_mlb_ids
            pitch_stats["is_rp"] = pitch_stats["pitcher_id_mlb"] not in sp_mlb_ids
            pitch_stats["is_wp"] = pitch_stats["pitcher_id_mlb"] == self.winning_pitcher
            pitch_stats["is_lp"] = pitch_stats["pitcher_id_mlb"] == self.losing_pitcher
            pitch_stats["is_sv"] = pitch_stats["pitcher_id_mlb"] == self.pitcher_earned_save
        return Result.Ok(deepcopy(pitch_stats))

    def get_bat_stats(self, mlb_id):
        result = self.validate_mlb_id(mlb_id)
        if result.failure:
            return result
        mlb_id = result.value
        bat_stats = self.bat_stats_map.get(mlb_id, {})
        return Result.Ok(deepcopy(bat_stats))

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

        innings_viewer = {"ALL": create_table_viewer(all_at_bat_tables)}
        for inning, at_bat_tables in at_bat_tables_by_inning.items():
            innings_viewer[inning] = create_table_viewer(at_bat_tables)
        return Result.Ok(innings_viewer)

    def get_valid_at_bats_for_pitcher(self, mlb_id):
        result = self.validate_mlb_id(mlb_id)
        if result.failure:
            return result
        mlb_id = result.value
        at_bats = [at_bat for at_bat in self.valid_at_bats if at_bat["pitcher_id_mlb"] == mlb_id]
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

        innings_viewer = {"ALL": create_table_viewer(all_at_bat_tables)}
        for inning, at_bat_tables in at_bat_tables_by_inning.items():
            innings_viewer[inning] = create_table_viewer(at_bat_tables)
        return Result.Ok(innings_viewer)

    def get_innings_sorted(self):
        at_bats_by_inning = group_and_sort_dict_list(self.valid_at_bats, "inning_id", "pbp_table_row_number")
        innings_unsorted = [
            {"inning_id": inning_id, "at_bats": at_bats} for inning_id, at_bats in at_bats_by_inning.items()
        ]
        return get_innings_sorted(innings_unsorted)

    def view_game_meta_info(self):
        meta_info = self.get_game_meta_info()
        meta_info.pop("umpires")
        game_start_time = meta_info.pop("game_start_time")
        table_rows = [[name, value] for name, value in meta_info.items()]
        table_rows.insert(0, ["game_start_time", game_start_time])
        table = tabulate(table_rows)
        heading = f"Meta Information for game {self.bbref_game_id}"
        return create_table_viewer([DisplayTable(table, heading)])

    def get_game_meta_info(self):
        meta_info = deepcopy(self.game_meta_info)
        meta_info.pop("game_time_hour")
        meta_info.pop("game_time_minute")
        meta_info["game_start_time"] = meta_info.pop("game_date_time_str")
        return meta_info

    def get_removed_pfx(self):
        return [at_bat["pitchfx"] for inning_dict in self.removed_pitchfx.values() for at_bat in inning_dict.values()]

    def view_pitch_mix_batter_stance_splits(self, mlb_id):
        result = self.validate_mlb_id(mlb_id)
        if result.failure:
            return result
        mlb_id = result.value
        pitcher_data = self.get_player_data(mlb_id)
        table_viewer = pitcher_data.view_pitch_mix_batter_stance_splits(self.bbref_game_id)
        return Result.Ok(table_viewer)

    def view_pitch_mix_season_splits(self, mlb_id):
        result = self.validate_mlb_id(mlb_id)
        if result.failure:
            return result
        mlb_id = result.value
        pitcher_data = self.get_player_data(mlb_id)
        table_viewer = pitcher_data.view_pitch_mix_season_splits(self.bbref_game_id)
        return Result.Ok(table_viewer)

    def view_pd_pitch_type_splits_for_pitcher(self, mlb_id):
        result = self.validate_mlb_id(mlb_id)
        if result.failure:
            return result
        mlb_id = result.value
        pitcher_data = self.get_player_data(mlb_id)
        table_viewer = pitcher_data.view_plate_discipline_pitch_type_splits(self.bbref_game_id)
        return Result.Ok(table_viewer)

    def view_bb_pitch_type_splits_for_pitcher(self, mlb_id):
        result = self.validate_mlb_id(mlb_id)
        if result.failure:
            return result
        mlb_id = result.value
        pitcher_data = self.get_player_data(mlb_id)
        table_viewer = pitcher_data.view_batted_ball_pitch_type_splits(self.bbref_game_id)
        return Result.Ok(table_viewer)

    def view_bat_stats_pitch_type_splits_for_pitcher(self, mlb_id):
        result = self.validate_mlb_id(mlb_id)
        if result.failure:
            return result
        mlb_id = result.value
        pitcher_data = self.get_player_data(mlb_id)
        table_viewer = pitcher_data.view_bat_stats_pitch_type_splits(self.bbref_game_id)
        return Result.Ok(table_viewer)

    def get_matchup_details(self):
        away_record = get_team_record_for_linescore(self.away_team_data)
        home_record = get_team_record_for_linescore(self.home_team_data)
        matchup = f"{self.away_team.name} ({away_record}) vs {self.home_team.name} ({home_record})"
        game_time = self.game_datetime.strftime(DT_AWARE_VERBOSE)
        w_l_sv = self.get_pitcher_results_str()
        return f"{game_time}\n{matchup}\n{w_l_sv}\n"

    def get_pitcher_results_str(self):
        pitchers = self.get_pitcher_results()
        w_pitcher = f'WP: {pitchers["wp"]["name"]} ({pitchers["wp"]["team_id"]})'
        l_pitcher = f'LP: {pitchers["lp"]["name"]} ({pitchers["lp"]["team_id"]})'
        if "sv" not in pitchers:
            return f"{w_pitcher} {l_pitcher}"
        sv_pitcher = f'SV: {pitchers["sv"]["name"]} ({pitchers["wp"]["team_id"]})'
        return f"{w_pitcher} {l_pitcher} {sv_pitcher}"

    def get_pitcher_results(self):
        pitcher_results = {}
        w_pitcher_bbref_id = (
            self.away_team_data["pitcher_of_record"]
            if self.away_team_data["team_won"]
            else self.home_team_data["pitcher_of_record"]
        )
        w_pitcher_id = self.get_player_id_map(bbref_id=w_pitcher_bbref_id).mlb_id
        w_pitcher_name = self.get_player_id_map(bbref_id=w_pitcher_bbref_id).mlb_name
        w_pitcher_team_id = self.away_team_id if self.away_team_data["team_won"] else self.home_team_id
        pitcher_results["wp"] = {"mlb_id": w_pitcher_id, "name": w_pitcher_name, "team_id": w_pitcher_team_id}
        l_pitcher_bbref_id = (
            self.home_team_data["pitcher_of_record"]
            if self.away_team_data["team_won"]
            else self.away_team_data["pitcher_of_record"]
        )
        l_pitcher_id = self.get_player_id_map(bbref_id=l_pitcher_bbref_id).mlb_id
        l_pitcher_name = self.get_player_id_map(bbref_id=l_pitcher_bbref_id).mlb_name
        l_pitcher_team_id = self.home_team_id if self.away_team_data["team_won"] else self.away_team_id
        pitcher_results["lp"] = {"mlb_id": l_pitcher_id, "name": l_pitcher_name, "team_id": l_pitcher_team_id}
        sv_pitcher_bbref_id = (
            self.away_team_data["pitcher_earned_save"]
            if self.away_team_data["team_won"] and self.away_team_data["pitcher_earned_save"]
            else self.home_team_data["pitcher_earned_save"]
            if self.home_team_data["team_won"] and self.home_team_data["pitcher_earned_save"]
            else 0
        )
        if sv_pitcher_bbref_id:
            sv_pitcher_id = self.get_player_id_map(bbref_id=sv_pitcher_bbref_id).mlb_id
            sv_pitcher_name = self.get_player_id_map(bbref_id=sv_pitcher_bbref_id).mlb_name
            pitcher_results["sv"] = {"mlb_id": sv_pitcher_id, "name": sv_pitcher_name, "team_id": w_pitcher_team_id}
        return pitcher_results

    def get_tui_linescore(self):
        table_list = [
            create_tui_linescore(**html_linescore)
            for html_linescore in get_html_linescore(
                self.away_team_data, self.home_team_data, self.innings_list, chunkify=True
            )
        ]
        return "\n\n".join(table_list)

    def get_html_linescore(self, condensed=True):
        linescore_components = get_html_linescore(
            self.away_team_data, self.home_team_data, self.innings_list, condensed
        )
        if condensed and self.extra_innings:
            linescore_components = condense_html_linescore(linescore_components)
        return linescore_components

    def get_inning_numbers_sorted(self):
        inning_labels = [inning["inning_id"] for inning in self.get_innings_sorted()]
        return sorted({parse_inning_number_from_label(inning) for inning in inning_labels})

    def get_inning_runs_scored_map(self):
        html_linescore = get_html_linescore(self.away_team_data, self.home_team_data, self.innings_list, False)
        return get_inning_runs_scored_map(html_linescore)


def get_html_linescore(away_team_data, home_team_data, innings_list, condensed=False, chunkify=False):
    linescore_tables = []
    (total_chunks, away_team_runs_chunked, home_team_runs_chunked) = get_runs_by_inning(innings_list, chunkify)
    for i in range(total_chunks):
        start_inning = i * 9 + 1
        linescore = create_html_linescore_components(
            away_team_data["team_id_br"],
            away_team_runs_chunked[i],
            get_team_totals_for_linescore(away_team_data),
            home_team_data["team_id_br"],
            home_team_runs_chunked[i],
            get_team_totals_for_linescore(home_team_data),
            start_inning,
            is_last_chunk(total_chunks, i + 1),
            is_extra_innings_game(total_chunks, away_team_runs_chunked[i]),
        )
        if condensed:
            linescore.pop("last_chunk")
        linescore_tables.append(linescore)
    return linescore_tables


def condense_html_linescore(linescore_tables):
    inning_runs_scored_map = get_inning_runs_scored_map(linescore_tables)
    (condensed_linescore_map, removed_inning_map, inning_runs_scored_map) = _condense_linescore(inning_runs_scored_map)
    return {
        "inning_numbers": [x["inning"] for x in condensed_linescore_map],
        "game_totals": ["R", "H", "E"],
        "away_team_id": linescore_tables[0]["away_team_id"],
        "away_team_runs_by_inning": [x["away_runs"] for x in condensed_linescore_map],
        "away_team_totals": linescore_tables[0]["away_team_totals"],
        "home_team_id": linescore_tables[0]["home_team_id"],
        "home_team_runs_by_inning": [x["home_runs"] for x in condensed_linescore_map],
        "home_team_totals": linescore_tables[0]["home_team_totals"],
        "extra_innings": linescore_tables[0]["extra_innings"],
        "removed_innings": removed_inning_map,
        "inning_runs_scored_map": inning_runs_scored_map,
    }


def get_inning_runs_scored_map(linescore_tables):
    inning_runs_scored_map = []
    if isinstance(linescore_tables, list):
        for linescore in linescore_tables:
            inning_map = _get_inning_runs_scored_map(linescore)
            inning_runs_scored_map.extend(inning_map)
    else:
        inning_runs_scored_map = _get_inning_runs_scored_map(linescore_tables)
    (_, _, inning_runs_scored_map) = _condense_linescore(inning_runs_scored_map)
    return inning_runs_scored_map


def _get_inning_runs_scored_map(linescore):
    inning_runs_scored_map = []
    inning_numbers_without_filler = [num for num in linescore["inning_numbers"] if num != ""]
    for i in range(len(inning_numbers_without_filler)):
        inning = linescore["inning_numbers"][i]
        away_team_runs_scored = linescore["away_team_runs_by_inning"][i]
        home_team_runs_scored = linescore["home_team_runs_by_inning"][i]
        inning_somebody_scored = away_team_runs_scored != 0 or home_team_runs_scored != 0
        home_team_runs_int = 0 if isinstance(home_team_runs_scored, str) else home_team_runs_scored
        inning_runs_scored_map.append(
            {
                "runs_scored": inning_somebody_scored,
                "inning": inning,
                "away_runs": away_team_runs_scored,
                "home_runs": home_team_runs_scored,
                "total_runs_scored": away_team_runs_scored + home_team_runs_int,
            }
        )
    return inning_runs_scored_map


def _condense_linescore(inning_runs_scored_map):
    innings_nobody_scored = list(filter(lambda x: not x["runs_scored"], inning_runs_scored_map))
    innings_somebody_scored = list(filter(lambda x: x["runs_scored"], inning_runs_scored_map))
    removed_innings = []
    if len(innings_somebody_scored) < 9:
        missing_column_count = 9 - len(innings_somebody_scored)
        innings_somebody_scored.extend(innings_nobody_scored[:missing_column_count])
        removed_innings = [inn["inning"] for inn in innings_nobody_scored[missing_column_count:]]
    elif len(innings_somebody_scored) > 9:
        remove_column_count = len(innings_somebody_scored) - 9
        innings_somebody_scored.sort(key=lambda x: x["total_runs_scored"])
        innings_somebody_scored = innings_somebody_scored[remove_column_count:]
        removed_innings = [inn["inning"] for inn in innings_nobody_scored[:remove_column_count]]
    removed_inning_map = []
    for inning_map in inning_runs_scored_map:
        removed_inning_map.append(inning_map["inning"] in removed_innings)
        inning_map["removed_inning"] = inning_map["inning"] in removed_innings
    return (sorted(innings_somebody_scored, key=lambda x: x["inning"]), removed_inning_map, inning_runs_scored_map)


def get_runs_by_inning(innings_list, chunkify):
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
    if not chunkify:
        return (1, [away_team_runs], [home_team_runs])
    away_team_runs_chunked = make_chunked_list(away_team_runs, chunk_size=9)
    home_team_runs_chunked = make_chunked_list(home_team_runs, chunk_size=9)
    total_nine_inning_chunks = len(away_team_runs_chunked)
    return (total_nine_inning_chunks, away_team_runs_chunked, home_team_runs_chunked)


def get_total_runs_in_inning(inning_at_bats):
    return sum(ab["runs_outs_result"].count("R") for ab in inning_at_bats)


def is_last_chunk(total_linescore_tables, current_table_number):
    return current_table_number == total_linescore_tables


def is_extra_innings_game(total_linescore_tables, innings_list):
    return total_linescore_tables > 1 or len(innings_list) > 9


def create_html_linescore_components(
    away_team_id,
    away_team_runs_by_inning,
    away_team_totals,
    home_team_id,
    home_team_runs_by_inning,
    home_team_totals,
    start_inning,
    last_chunk,
    extra_innings,
):
    end_inning = start_inning + len(away_team_runs_by_inning)
    inning_numbers = list(range(start_inning, end_inning))
    if last_chunk and extra_innings:
        away_team_runs_by_inning = add_filler_columns(away_team_runs_by_inning)
        home_team_runs_by_inning = add_filler_columns(home_team_runs_by_inning)
        inning_numbers = add_filler_columns(inning_numbers)

    return {
        "inning_numbers": inning_numbers,
        "game_totals": ["R", "H", "E"],
        "away_team_id": away_team_id,
        "away_team_runs_by_inning": away_team_runs_by_inning,
        "away_team_totals": away_team_totals,
        "home_team_id": home_team_id,
        "home_team_runs_by_inning": home_team_runs_by_inning,
        "home_team_totals": home_team_totals,
        "last_chunk": last_chunk,
        "extra_innings": extra_innings,
    }


def create_tui_linescore(
    inning_numbers,
    game_totals,
    away_team_id,
    away_team_runs_by_inning,
    away_team_totals,
    home_team_id,
    home_team_runs_by_inning,
    home_team_totals,
    last_chunk,
    extra_innings,
):
    inning_numbers = [get_tui_inning_number(num) for num in inning_numbers]
    tui_inning_numbers = [f"{' '*(4-len(str(num)))}{num}" for num in inning_numbers]
    tui_game_totals = [f"{' '*(4-len(letter))}{letter}" for letter in game_totals]
    tui_away_inning_runs = [f"{' '*(4-len(str(num)))}{num}" for num in away_team_runs_by_inning]
    tui_away_totals = [f"{' '*(4-len(str(num)))}{num}" for num in away_team_totals]
    tui_home_inning_runs = [f"{' '*(4-len(str(num)))}{num}" for num in home_team_runs_by_inning]
    tui_home_totals = [f"{' '*(4-len(str(num)))}{num}" for num in home_team_totals]
    return format_tui_linescore_components(
        tui_inning_numbers,
        tui_game_totals,
        away_team_id,
        tui_away_inning_runs,
        tui_away_totals,
        home_team_id,
        tui_home_inning_runs,
        tui_home_totals,
        last_chunk,
    )


def format_tui_linescore_components(
    tui_inning_numbers,
    tui_game_totals,
    away_team_id,
    tui_away_inning_runs,
    tui_away_totals,
    home_team_id,
    tui_home_inning_runs,
    tui_home_totals,
    last_chunk,
):
    linescore = f'   {"".join(tui_inning_numbers)}'
    if last_chunk:
        linescore += "".join(tui_game_totals)
    linescore += f"\n----{'----'*9}"
    if last_chunk:
        linescore += f"{'----'*3}"
    linescore += f"\n{away_team_id}{''.join(tui_away_inning_runs)}"
    if last_chunk:
        linescore += "".join(tui_away_totals)
    linescore += f"\n{home_team_id}{''.join(tui_home_inning_runs)}"
    if last_chunk:
        linescore += "".join(tui_home_totals)
    return linescore


def get_tui_inning_number(inning_number):
    if isinstance(inning_number, str):
        return ""
    pad_len = 1 if inning_number < 10 else 0
    return f"{' '*pad_len}{inning_number}"


def add_filler_columns(list_to_pad):
    for _ in range(9 - len(list_to_pad)):
        list_to_pad.append("")
    return list_to_pad


def get_team_record_for_linescore(team_data):
    team_wins = team_data["total_wins_before_game"]
    team_losses = team_data["total_losses_before_game"]
    return f"{team_wins}-{team_losses}"


def get_team_totals_for_linescore(team_data):
    team_total_runs = team_data["total_runs_scored_by_team"]
    team_hits = team_data["total_hits_by_team"]
    team_errors = team_data["total_errors_by_team"]
    return [team_total_runs, team_hits, team_errors]


def convert_linescore_for_html(linescore):
    if isinstance(linescore, list):
        linescore = linescore[0]
    inning_runs_scored_map = get_inning_runs_scored_map(linescore)
    (_, removed_inning_map, _) = _condense_linescore(inning_runs_scored_map)
    html_dict_keys = ("col_index", "col_header", "away_team", "home_team", "css_class", "removed_inning")
    team_id_column = (0, "&nbsp;", linescore["away_team_id"], linescore["home_team_id"], "team-id", False)
    html_linescore = [dict(zip(html_dict_keys, team_id_column))]

    total_innings = len(linescore["inning_numbers"])
    inning_col_indices = list(range(1, total_innings + 1))
    inning_css_classes = ["inning-runs-scored" for _ in range(total_innings)]
    inning_columns = zip(
        inning_col_indices,
        linescore["inning_numbers"],
        linescore["away_team_runs_by_inning"],
        linescore["home_team_runs_by_inning"],
        inning_css_classes,
        removed_inning_map,
    )
    html_linescore.extend([dict(zip(html_dict_keys, column)) for column in inning_columns])

    game_total_col_indices = list(range(total_innings + 1, total_innings + 4))
    game_total_css_classes = ["game-total" for _ in range(len(linescore["game_totals"]))]
    removed_inning_false = [False for _ in range(len(linescore["game_totals"]))]
    game_total_columns = zip(
        game_total_col_indices,
        linescore["game_totals"],
        linescore["away_team_totals"],
        linescore["home_team_totals"],
        game_total_css_classes,
        removed_inning_false,
    )
    html_linescore.extend([dict(zip(html_dict_keys, column)) for column in game_total_columns])
    return html_linescore


def get_innings_sorted(innings_unsorted):
    inning_ids = {inning["inning_id"] for inning in innings_unsorted}
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


def parse_inning_number_from_label(inning_id):
    match = INNING_LABEL_REGEX.search(inning_id)
    if not match:
        return 0
    try:
        return int(match[2])
    except ValueError:
        return 0


def get_at_bat_details(at_bat, pitch_app_stats, bat_stats):
    pitch_stats = parse_pitch_app_stats(pitch_app_stats)
    (at_bats, stat_line) = parse_bat_stats_for_game(bat_stats)
    details = f", {stat_line}" if stat_line else ""
    inning = inning_number_to_string(at_bat["inning_id"][-5:])
    score = parse_score_for_at_bat(at_bat)
    outs_plural = "out" if at_bat["outs_before_play"] == 1 else "outs"
    outs = f"{at_bat['outs_before_play']} {outs_plural}"
    at_bat_result = parse_at_bat_result(at_bat)
    return (
        f"Pitcher..: {at_bat['pitcher_name']} ({pitch_stats})\n"
        f"Batter...: {at_bat['batter_name']} ({at_bats}{details})\n"
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
    if "bbref_data" not in bat_stats or not bat_stats["bbref_data"] or not bat_stats["bbref_data"]["plate_appearances"]:
        return ("0/0", "")
    at_bats = f"{bat_stats['bbref_data']['hits']}/{bat_stats['bbref_data']['at_bats']}"
    stat_line = get_detailed_bat_stats(bat_stats["bbref_data"])
    return (at_bats, stat_line)


def get_detailed_bat_stats(bat_stats):
    stats = homeruns = doubles = triples = stolen_bases = iw = None
    stat_list = []
    if bat_stats["details"]:
        stats = parse_bat_stat_details(bat_stats["details"])
        homeruns = stats.pop("HR", None)
        triples = stats.pop("3B", None)
        doubles = stats.pop("2B", None)
        stolen_bases = stats.pop("SB", None)
        iw = stats.pop("IW", None)
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
        bb = f"{bases_on_balls}BB"
        if iw:
            bb += f" ({iw})"
        stat_list.append(bb)
    if stats:
        stat_list.extend(list(stats.values()))
    return ", ".join(stat_list)


def parse_bat_stat_details(details):
    return {stat["stat"]: parse_single_bat_stat(stat) for stat in details}


def parse_single_bat_stat(stat):
    separator = "-" if stat["stat"] in ["2B", "3B"] else ""
    return f"{stat['count']}{separator}{stat['stat']}" if stat["count"] > 1 else stat["stat"]


def parse_bat_stats_to_date(bat_stats):
    return (
        (
            f"{format_stat_decimal(bat_stats['bbref_data']['avg_to_date'])}/"
            f"{format_stat_decimal(bat_stats['bbref_data']['obp_to_date'])}/"
            f"{format_stat_decimal(bat_stats['bbref_data']['slg_to_date'])}/"
            f"{format_stat_decimal(bat_stats['bbref_data']['ops_to_date'])}"
        )
        if "bbref_data" in bat_stats
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
    game_score = f", GS: {bbref_stats['game_score']}" if bbref_stats["game_score"] > 0 else ""
    return f"{ip} IP ({runs}{details}{game_score})"


def format_pitch_sequence_description(pitch_seq_desc):
    return [format_event_description(seq) for seq in pitch_seq_desc]


def format_event_description(event_desc):
    if not event_desc[0] and not event_desc[2] and event_desc[1].startswith("(") and event_desc[1].endswith(")"):
        event_desc[1] = event_desc[1][1:]
        event_desc[1] = event_desc[1][:-1]
    desc_lines = [wrap_text(s, max_len=AT_BAT_DESC_MAX_WIDTH) for s in event_desc[1].split("\n")]
    event_desc[1] = "\n".join(desc_lines).replace("\n\n", "\n")
    return event_desc


def check_pitch_stats_for_game_score(pitch_stats):
    if not pitch_stats or not pitch_stats["bbref_data"]:
        return False
    return pitch_stats["bbref_data"]["game_score"] > 0
