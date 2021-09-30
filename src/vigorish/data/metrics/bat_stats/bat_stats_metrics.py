from functools import cached_property
from typing import Dict, List, Union

import vigorish.database as db
from vigorish.enums import DefensePosition
from vigorish.util.list_helpers import group_and_sort_list
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
    def total_games_started(self) -> int:
        return sum(bs.is_starter for bs in self.bat_stats)

    @property
    def total_games_subbed(self) -> int:
        return self.total_games - self.total_games_started

    @property
    def percent_started(self) -> float:
        percent_started = self.total_games_started / float(self.total_games) if self.total_games else 0.0
        return round(percent_started, ndigits=3)

    @property
    def percent_subbed(self) -> float:
        percent_subbed = self.total_games_subbed / float(self.total_games) if self.total_games else 0.0
        return round(percent_subbed, ndigits=3)

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
        return round(bb_rate, ndigits=3)

    @cached_property
    def k_rate(self) -> float:
        k_rate = self.strikeouts / float(self.plate_appearances) if self.plate_appearances else 0.0
        return round(k_rate, ndigits=3)

    @cached_property
    def contact_rate(self) -> float:
        cached_property = (self.at_bats - self.strikeouts) / float(self.at_bats) if self.at_bats else 0.0
        return round(cached_property, ndigits=3)

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

    @cached_property
    def def_position_list(self) -> List[DefensePosition]:
        return list({DefensePosition(int(bs.def_position)) for bs in self.bat_stats})

    @cached_property
    def def_position_metrics(self) -> List[Dict[str, Union[bool, int, float, DefensePosition]]]:
        bat_stats_grouped = group_and_sort_list(self.bat_stats, "def_position", "date_id")
        pos_counts = [get_pos_metrics(k, v, self.bat_stats) for k, v in bat_stats_grouped.items()]
        return sorted(pos_counts, key=lambda x: x["percent"], reverse=True)

    @cached_property
    def bat_order_list(self) -> List[DefensePosition]:
        return list({bs.bat_order for bs in self.bat_stats}) if self.bat_stats else []

    @cached_property
    def bat_order_metrics(self) -> List[Dict[str, Union[int, float]]]:
        bat_orders_grouped = group_and_sort_list(self.bat_stats, "bat_order", "date_id")
        order_number_counts = [get_bat_order_metrics(k, v, self.bat_stats) for k, v in bat_orders_grouped.items()]
        return sorted(order_number_counts, key=lambda x: x["percent"], reverse=True)

    def as_dict(self):
        dict_keys = list(filter(lambda x: not x.startswith(("__", "as_")), dir(self)))
        bat_stat_metrics_dict = {key: getattr(self, key) for key in dict_keys}
        bat_stat_metrics_dict.pop("bat_stats")
        bat_stat_metrics_dict["bat_order"] = _format_bat_order_list(bat_stat_metrics_dict["bat_order_list"])
        bat_stat_metrics_dict["def_position"] = _format_def_position_list(bat_stat_metrics_dict["def_position_list"])
        return bat_stat_metrics_dict


def get_pos_metrics(
    pos_number: str, pos_stats: List[db.BatStats], all_bat_stats: List[db.BatStats]
) -> Dict[str, Union[bool, int, float, DefensePosition]]:
    def_pos = DefensePosition(int(pos_number))
    return {
        "def_pos": def_pos,
        "is_starter": def_pos.is_starter,
        "total_games": len(pos_stats),
        "percent": round(len(pos_stats) / float(len(all_bat_stats)), 3) * 100,
    }


def get_bat_order_metrics(
    bat_order: str, bat_order_stats: List[db.BatStats], all_bat_stats: List[db.BatStats]
) -> Dict[str, Union[int, float]]:
    return {
        "bat_order": bat_order,
        "total_games": len(bat_order_stats),
        "percent": round(len(bat_order_stats) / float(len(all_bat_stats)), 3) * 100,
    }


def _format_bat_order_list(bat_order_list: List[int]) -> str:
    return ",".join(str(bat_order) for bat_order in bat_order_list)


def _format_def_position_list(def_position_list: List[int]) -> str:
    return ",".join(str(DefensePosition(int(def_pos))) for def_pos in def_position_list)
