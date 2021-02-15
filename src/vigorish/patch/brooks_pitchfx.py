from collections import defaultdict, OrderedDict
from copy import deepcopy
from dataclasses import dataclass, field
from typing import List

from dacite import from_dict

import vigorish.database as db
from vigorish.enums import DataSet
from vigorish.patch.base import Patch, PatchList
from vigorish.scrape.brooks_pitchfx.models.pitchfx_log import BrooksPitchFxLog
from vigorish.util.list_helpers import group_and_sort_list
from vigorish.util.result import Result
from vigorish.util.string_helpers import get_brooks_team_id


@dataclass
class BrooksPitchFxPatchList(PatchList):
    original_pfx_logs: List[BrooksPitchFxLog] = field(repr=False, init=False)

    def __post_init__(self):
        self.data_set = DataSet.BROOKS_PITCHFX
        self.patch_list_id = "__brooks_pitchfx_patch_list__"

    def apply(self, data, db_session, boxscore):
        data = self.pre_process_data(data)
        for patch in self.patch_list:
            result = patch.apply(data)
            if result.failure:
                return result
            data = result.value
        result = self.post_process_data(data, db_session, boxscore)
        if result.failure:
            return result
        data = result.value
        return Result.Ok(data)

    def pre_process_data(self, data):
        self.original_pfx_logs = {pfx_log.pitch_app_id: pfx_log for pfx_log in deepcopy(data)}
        return [pfx for pitchfx_log in data for pfx in pitchfx_log.pitchfx_log]

    def post_process_data(self, data, db_session, boxscore=None):
        pfx_grouped = group_and_sort_list(data, "pitch_app_id", "park_sv_id")
        for pa_id, pfx_log in pfx_grouped.items():
            og_pfx_log = self.original_pfx_logs.get(pa_id, None)
            if not og_pfx_log:
                new_pfx_log = self.create_new_pitchfx_log(pa_id, pfx_log, db_session, boxscore)
                self.original_pfx_logs[pa_id] = new_pfx_log
                continue
            og_pfx_log.pitchfx_log = pfx_log
            og_pfx_log.pitch_count_by_inning = self.get_pitch_count_by_inning(pfx_log)
            og_pfx_log.total_pitch_count = len(pfx_log)
        patched_pfx_logs = list(self.original_pfx_logs.values())
        return Result.Ok(patched_pfx_logs)

    def get_pitch_count_by_inning(self, pitchfx_log):
        pitch_count_unordered = defaultdict(int)
        for pfx in pitchfx_log:
            pitch_count_unordered[pfx.inning] += 1
        pitch_count_ordered = OrderedDict()
        for k in sorted(pitch_count_unordered.keys()):
            pitch_count_ordered[k] = pitch_count_unordered[k]
        return pitch_count_ordered

    def create_new_pitchfx_log(self, pitch_app_id, pitchfx_log, db_session, boxscore):
        pitcher_id_mlb = pitch_app_id.split("_")[1]
        player = db.PlayerId.find_by_mlb_id(db_session, pitcher_id_mlb)
        away_team_id = boxscore.away_team_data.team_id_br
        home_team_id = boxscore.home_team_data.team_id_br
        player_team_id = boxscore.player_team_dict[player.bbref_id]
        opponent_team_id = away_team_id if player_team_id == home_team_id else home_team_id
        game_status = db.GameScrapeStatus.find_by_bbref_game_id(db_session, self.url_id)
        pfx_log_dict = {
            "pitchfx_log": pitchfx_log,
            "pitch_count_by_inning": self.get_pitch_count_by_inning(pitchfx_log),
            "total_pitch_count": str(len(pitchfx_log)),
            "pitcher_name": player.mlb_name,
            "pitcher_id_mlb": pitcher_id_mlb,
            "pitch_app_id": pitch_app_id,
            "pitcher_team_id_bb": get_brooks_team_id(player_team_id),
            "opponent_team_id_bb": get_brooks_team_id(opponent_team_id),
            "bb_game_id": game_status.bb_game_id,
            "bbref_game_id": self.url_id,
            "game_date_year": str(game_status.game_date.year),
            "game_date_month": str(game_status.game_date.month),
            "game_date_day": str(game_status.game_date.day),
            "game_date_hour": str(game_status.game_time_hour),
            "game_date_minute": str(game_status.game_time_minute),
            "time_zone_name": game_status.game_time_zone,
            "pitchfx_url": "",
        }
        pfx_log = from_dict(data_class=BrooksPitchFxLog, data=pfx_log_dict)
        pfx_log.pitcher_id_mlb = int(pitcher_id_mlb)
        return pfx_log


@dataclass
class PatchBrooksPitchFxBatterId(Patch):
    bbref_game_id: str
    park_sv_id: str = field(repr=False)
    pitch_app_id: str = field(repr=False)
    current_at_bat_id: str = field(repr=False)
    current_batter_id: int
    current_batter_name: str = field(repr=False)
    new_at_bat_id: str = field(repr=False)
    new_batter_id: int
    new_batter_name: str = field(repr=False)

    def __post_init__(self):
        self.data_set = DataSet.BROOKS_PITCHFX
        self.patch_id = "__patch_brooks_pitchfx_batter_id__"

    def apply(self, data):
        matches = [pfx for pfx in data if pfx.park_sv_id == self.park_sv_id and pfx.pitch_app_id == self.pitch_app_id]
        if not matches:
            error = (
                "Unable to locate the PitchFX reading identified in this patch: "
                f"pitch_app_id: {self.pitch_app_id}, park_sv_id: {self.park_sv_id}"
            )
            return Result.Fail(error)
        if len(matches) > 1:
            error = (
                "More than one PitchFX raeding was found that matches this patch: "
                f"pitch_app_id: {self.pitch_app_id}, park_sv_id: {self.park_sv_id}"
            )
            return Result.Fail(error)
        matches[0].at_bat_id = self.new_at_bat_id
        matches[0].batter_id = self.new_batter_id
        matches[0].batter_name = self.new_batter_name
        matches[0].is_patched = True
        return Result.Ok(data)


@dataclass
class PatchBrooksPitchFxPitcherId(Patch):
    bbref_game_id: str
    park_sv_id: str = field(repr=False)
    current_at_bat_id: str = field(repr=False)
    current_pitch_app_id: str = field(repr=False)
    current_pitcher_id: int
    current_pitcher_name: str = field(repr=False)
    new_at_bat_id: str = field(repr=False)
    new_pitch_app_id: str = field(repr=False)
    new_pitcher_id: int
    new_pitcher_name: str = field(repr=False)

    def __post_init__(self):
        self.data_set = DataSet.BROOKS_PITCHFX
        self.patch_id = "__patch_brooks_pitchfx_pitcher_id__"

    def apply(self, data):
        matches = [pfx for pfx in data if pfx.park_sv_id == self.park_sv_id]
        if not matches:
            error = (
                "Unable to locate the PitchFX reading identified in this patch: "
                f"pitch_app_id: {self.current_pitch_app_id}, park_sv_id: {self.park_sv_id}"
            )
            return Result.Fail(error)
        if len(matches) > 1:
            error = (
                "More than one PitchFX reading was found that matches this patch: "
                f"pitch_app_id: {self.current_pitch_app_id}, park_sv_id: {self.park_sv_id}"
            )
            return Result.Fail(error)
        matches[0].at_bat_id = self.new_at_bat_id
        matches[0].pitch_app_id = self.new_pitch_app_id
        matches[0].pitcher_id = self.new_pitcher_id
        matches[0].pitcher_name = self.new_pitcher_name
        matches[0].is_patched = True
        return Result.Ok(data)


@dataclass
class PatchBrooksPitchFxDeletePitchFx(Patch):
    bbref_game_id: str
    park_sv_id: str
    pitch_app_id: str

    def __post_init__(self):
        self.data_set = DataSet.BROOKS_PITCHFX
        self.patch_id = "__patch_brooks_pitchfx_delete_pitchfx__"

    def apply(self, data):
        pass
