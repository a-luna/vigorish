import json
from dataclasses import dataclass
from typing import Any

from app.main.constants import (
    PITCH_TYPE_DICT, AT_BAT_RESULTS_HIT, AT_BAT_RESULTS_K,
    AT_BAT_RESULTS_BB, AT_BAT_RESULTS_HBP, AT_BAT_RESULTS_OUT,
    AT_BAT_RESULTS_DP
)
from app.main.util.string_functions import validate_bb_game_id
from app.main.util.list_functions import as_dict_list


@dataclass
class BrooksPitchFxLog:
    pitchfx_log: Any
    pitch_count_by_inning: Any
    total_pitch_count: str = "0"
    pitcher_name: str = ""
    pitcher_id_mlb: str = "0"
    pitch_app_id: str = ""
    pitcher_team_id_bb: str = ""
    opponent_team_id_bb: str = ""
    bb_game_id: str = ""
    bbref_game_id: str = ""
    pitchfx_url: str = ""

    @property
    def game_date(self):
        result = validate_bb_game_id(self.bb_game_id)
        if result.failure:
            return None
        game_dict = result.value
        return game_dict["game_date"]

    @property
    def bbref_pitch_app_id(self):
        return f"{self.bbref_game_id}_{self.pitcher_id_mlb}"

    @property
    def pitch_types(self):
        pitch_type_dict = {ptype:0 for ptype in PITCH_TYPE_DICT.keys()}
        for pfx in self.pitchfx_log:
            pitch_type_dict[pfx.mlbam_pitch_name] += 1
        return {k:v for k,v in pitch_type_dict.items() if v > 0}

    @property
    def at_bats(self):
        ab_ids = sorted(list(set([pfx.ab_id for pfx in self.pitchfx_log])))
        at_bats = {}
        for ab_id in ab_ids:
            at_bats[ab_id] = sorted(
                [pfx for pfx in self.pitchfx_log if pfx.ab_id == ab_id],
                key=lambda x: x.ab_count)
        return at_bats

    @property
    def swinging_strike_count(self):
        return len([pfx for pfx in self.pitchfx_log if pfx.pdes == "Swinging Strike"])

    @property
    def called_strike_count(self):
        return len([pfx for pfx in self.pitchfx_log if pfx.pdes == "Called Strike"])

    @property
    def called_plus_swinging_strike_count(self):
        return self.swinging_strike_count + self.called_strike_count

    @property
    def called_plus_swinging_strike_rate(self):
        return float(self.called_plus_swinging_strike_count / self.total_pitch_count)

    @property
    def batters_faced(self):
        ab_ids = sorted(list(set([pfx.ab_id for pfx in self.pitchfx_log])))
        return len(ab_ids)

    @property
    def ab_results(self):
        ab_ids = sorted(list(set([pfx.ab_id for pfx in self.pitchfx_log])))
        results = {}
        for ab_id in ab_ids:
            ab_result = list(set([
                pfx.des for pfx in self.pitchfx_log
                if pfx.ab_id == ab_id]))[0]
            results[ab_id] = ab_result
        return results

    @property
    def innings_pitched(self):
        outs = 0
        for ab_id, ab_result in self.ab_results.items():
            if ab_result in AT_BAT_RESULTS_DP:
                outs += 2
                continue
            if ab_result in AT_BAT_RESULTS_OUT:
                outs += 1
        try:
            (full, partial) = divmod(outs, 3)
            return float(f"{full}.{partial}")
        except ValueError:
            return None

    @property
    def total_hit(self):
        ab_ids = sorted(list(set([pfx.ab_id for pfx in self.pitchfx_log])))
        hits = 0
        for ab_id in ab_ids:
            ab_result = set([
                pfx.des for pfx in self.pitchfx_log
                if pfx.ab_id == ab_id
                and pfx.des in AT_BAT_RESULTS_HIT])
            if ab_result:
                hits += 1
        return hits

    @property
    def total_k(self):
        ab_ids = sorted(list(set([pfx.ab_id for pfx in self.pitchfx_log])))
        ks = 0
        for ab_id in ab_ids:
            ab_result = set([
                pfx.des for pfx in self.pitchfx_log
                if pfx.ab_id == ab_id
                and pfx.des in AT_BAT_RESULTS_K])
            if ab_result:
                ks += 1
        return ks

    @property
    def total_bb(self):
        ab_ids = sorted(list(set([pfx.ab_id for pfx in self.pitchfx_log])))
        bbs = 0
        for ab_id in ab_ids:
            ab_result = set([
                pfx.des for pfx in self.pitchfx_log
                if pfx.ab_id == ab_id
                and pfx.des in AT_BAT_RESULTS_BB])
            if ab_result:
                bbs += 1
        return bbs

    @property
    def total_hbp(self):
        ab_ids = sorted(list(set([pfx.ab_id for pfx in self.pitchfx_log])))
        hbps = 0
        for ab_id in ab_ids:
            ab_result = set([
                pfx.des for pfx in self.pitchfx_log
                if pfx.ab_id == ab_id
                and pfx.des in AT_BAT_RESULTS_HBP])
            if ab_result:
                hbps += 1
        return hbps


    def __eq__(self, other):
        if not isinstance(other, BrooksPitchFxLog):
            return NotImplemented
        if self.bbref_game_id != other.bbref_game_id:
            return False
        if self.pitcher_id_mlb != other.pitcher_id_mlb:
            return False
        if self.total_pitch_count != other.total_pitch_count:
            return False
        if self.pitch_count_by_inning != other.pitch_count_by_inning:
            return False
        return True


    def as_dict(self):
        return dict(
            __brooks_pitchfx_log__=True,
            pitchfx_log=as_dict_list(self.pitchfx_log),
            pitch_count_by_inning=self.pitch_count_by_inning,
            pitcher_name=self.pitcher_name,
            pitcher_id_mlb=int(self.pitcher_id_mlb),
            pitch_app_id=self.pitch_app_id,
            total_pitch_count=int(self.total_pitch_count),
            pitcher_team_id_bb=self.pitcher_team_id_bb,
            opponent_team_id_bb=self.opponent_team_id_bb,
            bb_game_id=self.bb_game_id,
            bbref_game_id=self.bbref_game_id,
            pitchfx_url=self.pitchfx_url)

    def as_json(self):
        """Convert pitchfx log to JSON."""
        return json.dumps(self.as_dict(), indent=2)
