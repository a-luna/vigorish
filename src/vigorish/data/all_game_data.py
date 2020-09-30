from tabulate import tabulate

from vigorish.constants import VigFile, DataSet
from vigorish.config.database import PlayerId, RunnersOnBase
from vigorish.data.viewers.table_viewer import DisplayTable, TableViewer
from vigorish.util.exceptions import ScrapedDataException
from vigorish.util.list_helpers import group_and_sort_dict_list
from vigorish.util.result import Result
from vigorish.util.string_helpers import inning_number_to_string


class AllGameData:
    _all_at_bats = []
    _at_bat_map = {}
    _all_pitch_stats = []
    _pitch_stat_map = {}
    _all_bat_stats = []
    _bat_stat_map = {}

    def __init__(self, db_session, scraped_data, bbref_game_id):
        self.db_session = db_session
        self.scraped_data = scraped_data
        combined_data = scraped_data.get_json_combined_data(bbref_game_id)
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

    @property
    def all_player_bbref_ids(self):
        return [bbref_id for bbref_id in self.player_id_dict.keys()]

    @property
    def all_player_mlb_ids(self):
        return [id_map["mlb_id"] for id_map in self.player_id_dict.values()]

    @property
    def player_id_map(self):
        return {
            mlb_id: PlayerId.find_by_mlb_id(self.db_session, mlb_id)
            for mlb_id in self.all_player_mlb_ids
        }

    @property
    def bbref_id_to_mlb_id_map(self):
        return {bbref_id: id_map["mlb_id"] for bbref_id, id_map in self.player_id_dict.items()}

    @property
    def mlb_id_to_bbref_id_map(self):
        return {id_map["mlb_id"]: bbref_id for bbref_id, id_map in self.player_id_dict.items()}

    @property
    def all_at_bats(self):
        if self._all_at_bats:
            return self._all_at_bats
        valid_at_bats = [
            at_bat_data
            for inning_data in self.innings_list
            for at_bat_data in inning_data["inning_events"]
        ]
        invalid_at_bats = [
            at_bat_data
            for inning_dict in self.invalid_pitchfx.values()
            for at_bat_data in inning_dict.values()
        ]
        self._all_at_bats = valid_at_bats + invalid_at_bats
        return self._all_at_bats

    @property
    def at_bat_map(self):
        if self._at_bat_map:
            return self._at_bat_map
        self._at_bat_map = {at_bat["at_bat_id"]: at_bat for at_bat in self.all_at_bats}
        return self._at_bat_map

    @property
    def all_pitch_stats(self):
        if self._all_pitch_stats:
            return self._all_pitch_stats
        pitch_stats = self.away_team_data["pitching_stats"] + self.home_team_data["pitching_stats"]
        self._all_pitch_stats = [pitch_app for pitch_app in pitch_stats]
        return self._all_pitch_stats

    @property
    def pitch_stat_map(self):
        if self._pitch_stat_map:
            return self._pitch_stat_map
        self._pitch_stat_map = {
            pitch_stats["pitcher_id_mlb"]: pitch_stats for pitch_stats in self.all_pitch_stats
        }
        return self._pitch_stat_map

    @property
    def all_player_ids_with_pitch_stats(self):
        return [mlb_id for mlb_id in self.pitch_stat_map.keys()]

    @property
    def all_bat_stats(self):
        if self._all_bat_stats:
            return self._all_bat_stats
        bat_stats = self.away_team_data["batting_stats"] + self.home_team_data["batting_stats"]
        self._all_bat_stats = [bat_stat for bat_stat in bat_stats]
        return self._all_bat_stats

    @property
    def bat_stat_map(self):
        if self._bat_stat_map:
            return self._bat_stat_map
        self._bat_stat_map = {
            bat_stats["batter_id_mlb"]: bat_stats for bat_stats in self.all_bat_stats
        }
        return self._bat_stat_map

    @property
    def all_player_ids_with_bat_stats(self):
        return [mlb_id for mlb_id in self.bat_stat_map.keys()]

    def get_all_at_bats_involving_batter(self, mlb_id):
        result = self.validate_mlb_id(mlb_id)
        if result.failure:
            return result
        mlb_id = result.value
        at_bats = [at_bat for at_bat in self.all_at_bats if at_bat["batter_id_mlb"] == mlb_id]
        return Result.Ok(at_bats)

    def get_all_at_bats_involving_pitcher(self, mlb_id):
        result = self.validate_mlb_id(mlb_id)
        if result.failure:
            return result
        mlb_id = result.value
        at_bats = [at_bat for at_bat in self.all_at_bats if at_bat["pitcher_id_mlb"] == mlb_id]
        return Result.Ok(at_bats)

    def view_at_bats_for_batter(self, mlb_id):
        result = self.validate_mlb_id(mlb_id)
        if result.failure:
            return result
        mlb_id = result.value
        at_bats = self.get_all_at_bats_involving_batter(mlb_id).value
        table_list = []
        for num, at_bat in enumerate(at_bats, start=1):
            player_name = self.player_id_map.get(mlb_id).mlb_name
            heading = f"{player_name} At Bat #{num}/{len(at_bats)} in Game {self.bbref_game_id}"
            table = self.create_at_bat_table(at_bat, heading)
            table_list.append(table)
        table_viewer = self.create_table_viewer(table_list)
        return Result.Ok(table_viewer)

    def view_at_bats_for_pitcher(self, mlb_id):
        result = self.validate_mlb_id(mlb_id)
        if result.failure:
            return result
        mlb_id = result.value
        at_bats = self.get_all_at_bats_involving_pitcher(mlb_id).value
        at_bats_by_inning = group_and_sort_dict_list(at_bats, "inning_id", "pbp_table_row_number")
        innings_viewer = {}
        for inning_id, at_bats in at_bats_by_inning.items():
            inning = inning_id[-5:]
            table_list = []
            for num, at_bat in enumerate(at_bats, start=1):
                player_name = self.player_id_map.get(mlb_id).mlb_name
                heading = f"At Bat #{num}/{len(at_bats)}, Inning: {inning}, P: {player_name}"
                table = self.create_at_bat_table(at_bat, heading)
                table_list.append(table)
            table_viewer = self.create_table_viewer(table_list)
            innings_viewer[inning] = table_viewer
        return Result.Ok(innings_viewer)

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

    def create_at_bat_table(self, at_bat, heading=None):
        table = tabulate(at_bat["pitch_sequence_description"])
        message = self.get_at_bat_details(at_bat)
        return DisplayTable(table, heading, message)

    def get_at_bat_details(self, at_bat):
        pitch_app_stats = self.parse_pitch_app_stats(at_bat["pitcher_id_mlb"]).value
        batter_game_stats = self.parse_bat_stats_for_game(at_bat["batter_id_mlb"]).value
        inning = inning_number_to_string(at_bat["inning_id"][-5:])
        runners_on = RunnersOnBase.find_by_notation(self.db_session, at_bat["runners_on_base"])
        outs_plural = "out" if at_bat["outs_before_play"] == 1 else "outs"
        outs = f"{at_bat['outs_before_play']} {outs_plural}"
        at_bat_result = self.parse_at_bat_result(at_bat)
        return (
            f"Pitcher..: {at_bat['pitcher_name']} [{pitch_app_stats}]\n"
            f"Batter...: {at_bat['batter_name']} [{batter_game_stats}]\n"
            f"Score....: {at_bat['score']}, {runners_on}\n"
            f"Inning...: {inning}, {outs}\n"
            f"Result...: {at_bat_result}\n"
        )

    def parse_at_bat_result(self, at_bat):
        total_outs = at_bat["runs_outs_result"].count("O")
        total_runs = at_bat["runs_outs_result"].count("R")
        if not total_outs and not total_runs:
            return "No outs or runs scored this at bat"
        outs = f"{total_outs} outs" if total_outs else ""
        runs = f"{total_runs} runs scored" if total_runs else ""
        separator = ", " if total_outs and total_runs else ""
        return f"{outs}{separator}{runs}"

    def parse_bat_stats_to_date(self, mlb_id):
        result = self.get_bat_stats(mlb_id)
        if result.failure:
            return result
        bat_stats = result.value
        return Result.Ok(
            f"{bat_stats['avg_to_date']}/"
            f"{bat_stats['obp_to_date']}/"
            f"{bat_stats['slg_to_date']}/"
            f"{bat_stats['ops_to_date']}"
        )

    def parse_bat_stats_for_game(self, mlb_id):
        result = self.get_bat_stats(mlb_id)
        if result.failure:
            return result
        bat_stats = result.value
        if not bat_stats["plate_appearances"]:
            return Result.Ok("Batter has no plate appearances")
        stat_list = []
        at_bats = f"{bat_stats['hits']}/{bat_stats['at_bats']}"
        if bat_stats["runs_scored"]:
            stat_list.append(f"{bat_stats['runs_scored']} R")
        if bat_stats["rbis"]:
            stat_list.append(f"{bat_stats['rbis']} RBI")
        if bat_stats["strikeouts"]:
            stat_list.append(f"{bat_stats['strikeouts']} K")
        if bat_stats["bases_on_balls"]:
            stat_list.append(f"{bat_stats['bases_on_balls']} BB")
        if bat_stats["details"]:
            stat_list.append(self.parse_bat_stat_details(bat_stats["details"]))
        stat_line = f'{at_bats} {", ".join(stat_list)}'
        return Result.Ok(stat_line)

    def parse_bat_stat_details(self, details):
        return ", ".join([self.parse_single_bat_stat(stat) for stat in details])

    def parse_single_bat_stat(self, stat):
        if stat["count"] == 1:
            return stat["stat"]
        return f"{stat['count']} {stat['stat']}"

    def parse_pitch_app_stats(self, mlb_id):
        result = self.get_pitch_app_stats(mlb_id)
        if result.failure:
            return result
        pitch_app = result.value
        game_score = (
            f", Game Score: {pitch_app['game_score']}" if pitch_app["game_score"] > 0 else ""
        )
        earned_runs = (
            f"{pitch_app['earned_runs']} ER"
            if pitch_app["earned_runs"] == pitch_app["runs"]
            else f"{pitch_app['runs']} R ({pitch_app['earned_runs']} ER)"
        )
        stat_line = (
            f"{pitch_app['innings_pitched']} IP, {earned_runs}, {pitch_app['hits']} H, "
            f"{pitch_app['strikeouts']} K, {pitch_app['bases_on_balls']} BB{game_score}"
        )
        return Result.Ok(stat_line)

    def create_table_viewer(self, table_list):
        return TableViewer(
            table_list=table_list,
            prompt="Press Enter to return to previous menu",
            confirm_only=True,
            table_color="bright_cyan",
            heading_color="bright_yellow",
            message_color=None,
        )

    def get_bat_stats(self, mlb_id):
        result = self.validate_mlb_id(mlb_id)
        if result.failure:
            return result
        mlb_id = result.value
        bat_stats = self.bat_stat_map.get(mlb_id)
        return Result.Ok(bat_stats["bbref_data"])

    def get_pitch_app_stats(self, mlb_id):
        result = self.validate_mlb_id(mlb_id)
        if result.failure:
            return result
        mlb_id = result.value
        pitch_stats = self.pitch_stat_map.get(mlb_id)
        return Result.Ok(pitch_stats["bbref_data"])
