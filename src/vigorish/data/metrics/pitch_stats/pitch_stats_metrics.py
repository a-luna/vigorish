from functools import cached_property
from typing import List

import vigorish.database as db


class PitchStatsMetrics:
    def __init__(
        self,
        pitch_stats: List[db.PitchStats],
        year: int = None,
        player_id_mlb: int = None,
        player_id_bbref: str = None,
        team_id_bbref: str = None,
        player_team_id_bbref: str = None,
        opponent_team_id_bbref: str = None,
        role: str = None,
        stint_number: int = None,
    ) -> None:
        self.year = year
        self.pitch_stats = pitch_stats
        self.mlb_id = player_id_mlb
        self.bbref_id = player_id_bbref
        self.role = role
        self.team_id_bbref = team_id_bbref or player_team_id_bbref
        self.player_team_id_bbref = team_id_bbref or player_team_id_bbref
        self.opponent_team_id_bbref = opponent_team_id_bbref
        self.stint_number = stint_number

    def __repr__(self) -> str:
        splits = []
        if self.bbref_id:
            splits.append(f"player_id={self.bbref_id}")
        if self.player_team_id_bbref or self.team_id_bbref:
            team = self.player_team_id_bbref or self.team_id_bbref
            splits.append(f"team={team}")
        if self.year:
            splits.append(f"year={self.year}")
        if self.role:
            splits.append(f"role={self.role}")
        if self.opponent_team_id_bbref:
            splits.append(f"opponent={self.opponent_team_id_bbref}")
        if self.stint_number:
            splits.append(f"stint={self.stint_number}")
        splits.append(f"total_games={self.total_games}")
        splits.append(f"innings_pitched={self.innings_pitched}")
        return f"PitchStatsMetrics({', '.join(splits)})"

    @cached_property
    def total_games(self) -> int:
        return len(self.pitch_stats)

    @cached_property
    def games_as_sp(self) -> int:
        return sum(stats.is_sp for stats in self.pitch_stats)

    @cached_property
    def games_as_rp(self) -> int:
        return sum(stats.is_rp for stats in self.pitch_stats)

    @cached_property
    def wins(self) -> int:
        return sum(stats.is_wp for stats in self.pitch_stats)

    @cached_property
    def losses(self) -> int:
        return sum(stats.is_lp for stats in self.pitch_stats)

    @cached_property
    def saves(self) -> int:
        return sum(stats.is_sv for stats in self.pitch_stats)

    @cached_property
    def total_outs(self) -> int:
        return sum(stats.total_outs for stats in self.pitch_stats)

    @cached_property
    def innings_pitched(self) -> float:
        if not self.total_outs:
            return 0.0
        (complete_innings, remaining_outs) = divmod(self.total_outs, 3)
        return float(f"{complete_innings}.{remaining_outs}")

    @cached_property
    def batters_faced(self) -> int:
        return sum(stats.batters_faced for stats in self.pitch_stats)

    @cached_property
    def runs(self) -> int:
        return sum(stats.runs for stats in self.pitch_stats)

    @cached_property
    def earned_runs(self) -> int:
        return sum(stats.earned_runs for stats in self.pitch_stats)

    @cached_property
    def hits(self) -> int:
        return sum(stats.hits for stats in self.pitch_stats)

    @cached_property
    def homeruns(self) -> int:
        return sum(stats.homeruns for stats in self.pitch_stats)

    @cached_property
    def strikeouts(self) -> int:
        return sum(stats.strikeouts for stats in self.pitch_stats)

    @cached_property
    def bases_on_balls(self) -> int:
        return sum(stats.bases_on_balls for stats in self.pitch_stats)

    @cached_property
    def era(self) -> float:
        runs_per_out = self.earned_runs / float(self.total_outs) if self.total_outs else 0.0
        return round(runs_per_out * 27, ndigits=2)

    @cached_property
    def whip(self) -> float:
        whip = ((self.bases_on_balls + self.hits) * 3) / float(self.total_outs) if self.total_outs else 0.0
        return round(whip, ndigits=2)

    @cached_property
    def k_per_nine(self) -> float:
        k_per_out = self.strikeouts / float(self.total_outs) if self.total_outs else 0.0
        return round(k_per_out * 27, ndigits=1)

    @cached_property
    def bb_per_nine(self) -> float:
        bb_per_out = self.bases_on_balls / float(self.total_outs) if self.total_outs else 0.0
        return round(bb_per_out * 27, ndigits=1)

    @cached_property
    def hr_per_nine(self) -> float:
        hr_per_out = self.homeruns / float(self.total_outs) if self.total_outs else 0.0
        return round(hr_per_out * 27, ndigits=1)

    @cached_property
    def k_per_bb(self) -> float:
        k_per_bb = self.strikeouts / float(self.bases_on_balls) if self.bases_on_balls else 0.0
        return round(k_per_bb * 27, ndigits=1)

    @cached_property
    def k_rate(self) -> float:
        k_rate = self.strikeouts / float(self.batters_faced) if self.batters_faced else 0.0
        return round(k_rate, ndigits=1)

    @cached_property
    def bb_rate(self) -> float:
        bb_rate = self.bases_on_balls / float(self.batters_faced) if self.batters_faced else 0.0
        return round(bb_rate, ndigits=1)

    @cached_property
    def k_minus_bb(self) -> float:
        return self.k_rate - self.bb_rate

    @cached_property
    def hr_per_fb(self) -> float:
        hr_per_fb = self.homeruns / float(self.fly_balls) if self.fly_balls else 0.0
        return round(hr_per_fb, ndigits=1)

    @cached_property
    def pitch_count(self) -> int:
        return sum(stats.pitch_count for stats in self.pitch_stats)

    @cached_property
    def strikes(self) -> int:
        return sum(stats.strikes for stats in self.pitch_stats)

    @cached_property
    def strikes_contact(self) -> int:
        return sum(stats.strikes_contact for stats in self.pitch_stats)

    @cached_property
    def strikes_swinging(self) -> int:
        return sum(stats.strikes_swinging for stats in self.pitch_stats)

    @cached_property
    def strikes_looking(self) -> int:
        return sum(stats.strikes_looking for stats in self.pitch_stats)

    @cached_property
    def ground_balls(self) -> int:
        return sum(stats.ground_balls for stats in self.pitch_stats)

    @cached_property
    def fly_balls(self) -> int:
        return sum(stats.fly_balls for stats in self.pitch_stats)

    @cached_property
    def line_drives(self) -> int:
        return sum(stats.line_drives for stats in self.pitch_stats)

    @cached_property
    def unknown_type(self) -> int:
        return sum(stats.unknown_type for stats in self.pitch_stats)

    @cached_property
    def inherited_runners(self) -> int:
        return sum(stats.inherited_runners for stats in self.pitch_stats)

    @cached_property
    def inherited_scored(self) -> int:
        return sum(stats.inherited_scored for stats in self.pitch_stats)

    @cached_property
    def wpa_pitch(self) -> float:
        wpa_pitch = sum(stats.wpa_pitch for stats in self.pitch_stats)
        return round(wpa_pitch, ndigits=2)

    @cached_property
    def re24_pitch(self) -> float:
        re24_pitch = sum(stats.re24_pitch for stats in self.pitch_stats)
        return round(re24_pitch, ndigits=1)

    def as_dict(self):
        dict_keys = list(filter(lambda x: not x.startswith(("__", "as_")), dir(self)))
        pitch_stats_metrics_dict = {key: getattr(self, key) for key in dict_keys}
        pitch_stats_metrics_dict.pop("pitch_stats")
        return pitch_stats_metrics_dict
