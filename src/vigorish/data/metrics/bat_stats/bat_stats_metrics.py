from functools import cached_property
from typing import List

import vigorish.database as db
from vigorish.enums import DefensePosition
from vigorish.util.string_helpers import format_decimal_bat_stat


class BatStatsMetrics:
    def __init__(
        self,
        bat_stats: List[db.BatStats],
        year: int = None,
        player_id_mlb: int = None,
        player_id_bbref: str = None,
        team_id_bbref: str = None,
        player_team_id_bbref: str = None,
        opponent_team_id_bbref: str = None,
        is_starter: bool = False,
        bat_order_list: List[int] = None,
        def_position_list: List[DefensePosition] = None,
        stint_number: int = None,
    ) -> None:
        self.year = year
        self.bat_stats = bat_stats
        self.mlb_id = player_id_mlb
        self.bbref_id = player_id_bbref
        self.team_id_bbref = team_id_bbref or player_team_id_bbref
        self.player_team_id_bbref = team_id_bbref or player_team_id_bbref
        self.opponent_team_id_bbref = opponent_team_id_bbref
        self.is_starter = is_starter
        self.bat_order_list = bat_order_list or []
        self.def_position_list = def_position_list or []
        self.stint_number = stint_number
        self.changed_teams_midseason = False
        self.all_stats_for_season = False
        self.all_stats_for_stint = False
        self.career_stats_all_teams = False
        self.career_stats_for_team = False
        self.all_team_stats_for_def_pos = False
        self.all_player_stats_for_def_pos = False
        self.separate_player_stats_for_def_pos = False
        self.all_team_stats_for_bat_order = False
        self.all_player_stats_for_bat_order = False
        self.separate_player_stats_for_bat_order = False

    def __repr__(self) -> str:
        splits = []
        if self.bbref_id:
            splits.append(f"player_id={self.bbref_id}")
        if self.player_team_id_bbref or self.team_id_bbref:
            team = self.player_team_id_bbref or self.team_id_bbref
            splits.append(f"team={team}")
        if self.year:
            splits.append(f"year={self.year}")
        if self.bat_order_list:
            splits.append(f"bat_order={_format_bat_order_list(self.bat_order_list)}")
        if self.def_position_list:
            splits.append(f"def_position={_format_def_position_list(self.def_position_list)}")
        if self.opponent_team_id_bbref:
            splits.append(f"opponent={self.opponent_team_id_bbref}")
        if self.stint_number:
            splits.append(f"stint={self.stint_number}")
        splits.append(f"total_games={self.total_games}")
        return f"BatStatsMetrics({', '.join(splits)})"

    @cached_property
    def total_games(self) -> int:
        return len(self.bat_stats)

    @cached_property
    def avg(self) -> float:
        avg = self.hits / float(self.at_bats) if self.at_bats else 0.0
        return float(format_decimal_bat_stat(avg))

    @cached_property
    def obp(self) -> float:
        obp_denom = self.at_bats + self.bases_on_balls + self.hit_by_pitch
        obp = (self.hits + self.bases_on_balls + self.hit_by_pitch) / float(obp_denom) if obp_denom else 0.0
        return float(format_decimal_bat_stat(obp))

    @cached_property
    def slg(self) -> float:
        singles = self.hits - self.doubles - self.triples - self.homeruns
        total_bases = singles + self.doubles * 2 + self.triples * 3 + self.homeruns * 4
        slg = total_bases / float(self.at_bats) if self.at_bats else 0.0
        return float(format_decimal_bat_stat(slg))

    @cached_property
    def ops(self) -> float:
        ops = self.obp + self.slg
        return float(format_decimal_bat_stat(ops))

    @cached_property
    def iso(self) -> float:
        iso = self.slg - self.avg
        return float(format_decimal_bat_stat(iso))

    @cached_property
    def bb_rate(self) -> float:
        bb_rate = self.bases_on_balls / float(self.plate_appearances) if self.plate_appearances else 0.0
        return round(bb_rate, ndigits=1)

    @cached_property
    def k_rate(self) -> float:
        k_rate = self.strikeouts / float(self.plate_appearances) if self.plate_appearances else 0.0
        return round(k_rate, ndigits=1)

    @cached_property
    def contact_rate(self) -> float:
        cached_property = (self.at_bats - self.strikeouts) / float(self.at_bats) if self.at_bats else 0.0
        return round(cached_property, ndigits=1)

    @cached_property
    def plate_appearances(self) -> int:
        return sum(bs.plate_appearances for bs in self.bat_stats)

    @cached_property
    def at_bats(self) -> int:
        return sum(bs.at_bats for bs in self.bat_stats)

    @cached_property
    def hits(self) -> int:
        return sum(bs.hits for bs in self.bat_stats)

    @cached_property
    def runs_scored(self) -> int:
        return sum(bs.runs_scored for bs in self.bat_stats)

    @cached_property
    def rbis(self) -> int:
        return sum(bs.rbis for bs in self.bat_stats)

    @cached_property
    def bases_on_balls(self) -> int:
        return sum(bs.bases_on_balls for bs in self.bat_stats)

    @cached_property
    def strikeouts(self) -> int:
        return sum(bs.strikeouts for bs in self.bat_stats)

    @cached_property
    def doubles(self) -> int:
        return sum(bs.doubles for bs in self.bat_stats)

    @cached_property
    def triples(self) -> int:
        return sum(bs.triples for bs in self.bat_stats)

    @cached_property
    def homeruns(self) -> int:
        return sum(bs.homeruns for bs in self.bat_stats)

    @cached_property
    def stolen_bases(self) -> int:
        return sum(bs.stolen_bases for bs in self.bat_stats)

    @cached_property
    def caught_stealing(self) -> int:
        return sum(bs.caught_stealing for bs in self.bat_stats)

    @cached_property
    def hit_by_pitch(self) -> int:
        return sum(bs.hit_by_pitch for bs in self.bat_stats)

    @cached_property
    def intentional_bb(self) -> int:
        return sum(bs.intentional_bb for bs in self.bat_stats)

    @cached_property
    def gdp(self) -> int:
        return sum(bs.gdp for bs in self.bat_stats)

    @cached_property
    def sac_fly(self) -> int:
        return sum(bs.sac_fly for bs in self.bat_stats)

    @cached_property
    def sac_hit(self) -> int:
        return sum(bs.sac_hit for bs in self.bat_stats)

    @cached_property
    def total_pitches(self) -> int:
        return sum(bs.total_pitches for bs in self.bat_stats)

    @cached_property
    def total_strikes(self) -> int:
        return sum(bs.total_strikes for bs in self.bat_stats)

    @cached_property
    def wpa_bat(self) -> float:
        wpa_bat = sum(bs.wpa_bat for bs in self.bat_stats)
        return round(wpa_bat, ndigits=2)

    @cached_property
    def wpa_bat_pos(self) -> float:
        wpa_bat_pos = sum(bs.wpa_bat_pos for bs in self.bat_stats)
        return round(wpa_bat_pos, ndigits=2)

    @cached_property
    def wpa_bat_neg(self) -> float:
        wpa_bat_neg = sum(bs.wpa_bat_neg for bs in self.bat_stats)
        return round(wpa_bat_neg, ndigits=2)

    @cached_property
    def re24_bat(self) -> float:
        re24_bat = sum(bs.re24_bat for bs in self.bat_stats)
        return round(re24_bat, ndigits=1)

    def as_dict(self):
        dict_keys = list(filter(lambda x: not x.startswith(("__", "as_")), dir(self)))
        bat_stat_metrics_dict = {key: getattr(self, key) for key in dict_keys}
        bat_stat_metrics_dict.pop("bat_stats")
        bat_stat_metrics_dict["bat_order"] = _format_bat_order_list(bat_stat_metrics_dict["bat_order_list"])
        bat_stat_metrics_dict["def_position"] = _format_def_position_list(bat_stat_metrics_dict["def_position_list"])
        return bat_stat_metrics_dict


def _format_bat_order_list(bat_order_list: List[int]) -> str:
    return ",".join(str(bat_order) for bat_order in bat_order_list)


def _format_def_position_list(def_position_list: List[int]) -> str:
    return ",".join(str(DefensePosition(int(def_pos))) for def_pos in def_position_list)


# from vigorish.app import Vigorish
# from vigorish.enums import DefensePosition
# app = Vigorish()
# def_positions = [DefensePosition.SECOND_BASE, DefensePosition.SHORT_STOP]
# ball = app.scraped_data.get_bat_stats_for_defpos_for_season_for_all_teams(def_positions, 2021)
# bt = app.scraped_data.get_bat_stats_for_defpos_by_player_for_team(def_positions, "TOR", 2021)
