from __future__ import annotations

from functools import cached_property
from typing import Dict, List

import vigorish.database as db
from vigorish.data.metrics.constants import PFX_BATTED_BALL_METRICS, PFX_PLATE_DISCIPLINE_METRICS
from vigorish.enums import PitchType
from vigorish.util.string_helpers import format_decimal_bat_stat


class PitchFxMetrics:
    def __init__(self, pfx: List[db.PitchFx], mlb_id: int = None, p_throws: str = None, bat_stand: str = None) -> None:
        self.pfx = pfx
        self.mlb_id = mlb_id
        self.p_throws = p_throws
        self.bat_stand = bat_stand
        self.percent = 0

    def __repr__(self):
        split_type = ""
        if self.p_throws:
            split_type += f"p_throws={self.p_throws}, "
        if self.bat_stand:
            split_type += f"bat_stand={self.bat_stand}, "
        return (
            "PitchFxMetrics("
            f"mlb_id={self.mlb_id}, "
            f"{split_type}"
            f"pitch_type={self.pitch_type.print_name}, "
            f"total_pitches={self.total_pitches}, "
            f"percent={self.percent:.0%})"
        )

    @property
    def pitch_name(self) -> str:
        return self.pitch_type.print_name

    @cached_property
    def pitch_type_int(self):
        return sum({p.pitch_type_int for p in self.pfx})

    @property
    def pitch_type(self) -> PitchType:
        return PitchType(self.pitch_type_int)

    @property
    def pitch_type_abbrevs(self) -> List[str]:
        return [str(pt) for pt in self.pitch_type]

    @property
    def triple_slash(self) -> str:
        return (
            f"{format_decimal_bat_stat(self.avg)}/"
            f"{format_decimal_bat_stat(self.obp)}/"
            f"{format_decimal_bat_stat(self.slg)} ({format_decimal_bat_stat(self.ops)})"
        )

    @cached_property
    def total_pitches(self) -> int:
        return len(self.pfx)

    @cached_property
    def total_inside_strike_zone(self) -> int:
        return sum(p.inside_strike_zone for p in self.pfx)

    @cached_property
    def total_outside_strike_zone(self) -> int:
        return sum(p.outside_strike_zone for p in self.pfx)

    @cached_property
    def total_called_strikes(self) -> int:
        return sum(p.called_strike for p in self.pfx)

    @cached_property
    def total_swinging_strikes(self) -> int:
        return sum(p.swinging_strike for p in self.pfx)

    @cached_property
    def total_swings(self) -> int:
        return sum(p.batter_did_swing for p in self.pfx)

    @cached_property
    def total_swings_inside_zone(self) -> int:
        return sum(p.swing_inside_zone for p in self.pfx)

    @cached_property
    def total_swings_outside_zone(self) -> int:
        return sum(p.swing_outside_zone for p in self.pfx)

    @cached_property
    def total_bad_whiffs(self):
        return len([p for p in self.pfx if p.swing_outside_zone and p.swinging_strike])

    @cached_property
    def total_swings_made_contact(self) -> int:
        return sum(p.batter_made_contact for p in self.pfx)

    @cached_property
    def total_contact_inside_zone(self) -> int:
        return sum(p.contact_inside_zone for p in self.pfx)

    @cached_property
    def total_contact_outside_zone(self) -> int:
        return sum(p.contact_outside_zone for p in self.pfx)

    @cached_property
    def total_balls_in_play(self) -> int:
        return sum(p.is_in_play for p in self.pfx)

    @cached_property
    def total_ground_balls(self) -> int:
        return sum(p.is_ground_ball for p in self.pfx)

    @cached_property
    def total_line_drives(self) -> int:
        return sum(p.is_line_drive for p in self.pfx)

    @cached_property
    def total_fly_balls(self) -> int:
        return sum(p.is_fly_ball for p in self.pfx)

    @cached_property
    def total_popups(self) -> int:
        return sum(p.is_popup for p in self.pfx)

    @cached_property
    def total_pa(self) -> int:
        return sum(p.is_final_pitch_of_ab for p in self.pfx)

    @cached_property
    def total_hits(self) -> int:
        return sum(p.ab_result_hit for p in self.pfx)

    @cached_property
    def total_outs(self) -> int:
        return sum(p.ab_result_out for p in self.pfx)

    @cached_property
    def total_k(self) -> int:
        return sum(p.ab_result_k for p in self.pfx)

    @cached_property
    def total_bb(self) -> int:
        return sum(p.ab_result_bb for p in self.pfx)

    @cached_property
    def total_hbp(self) -> int:
        return sum(p.ab_result_hbp for p in self.pfx)

    @cached_property
    def total_sac_hit(self) -> int:
        return sum(p.ab_result_sac_hit for p in self.pfx)

    @cached_property
    def total_sac_fly(self) -> int:
        return sum(p.ab_result_sac_fly for p in self.pfx)

    @cached_property
    def total_errors(self) -> int:
        return sum(p.ab_result_error for p in self.pfx)

    @cached_property
    def total_at_bats(self) -> int:
        return self.total_hits + self.total_outs + self.total_errors - self.total_sac_hit - self.total_sac_fly

    @cached_property
    def total_singles(self) -> int:
        return sum(p.ab_result_single for p in self.pfx)

    @cached_property
    def total_doubles(self) -> int:
        return sum(p.ab_result_double for p in self.pfx)

    @cached_property
    def total_triples(self) -> int:
        return sum(p.ab_result_triple for p in self.pfx)

    @cached_property
    def total_homeruns(self) -> int:
        return sum(p.ab_result_homerun for p in self.pfx)

    @cached_property
    def total_ibb(self) -> int:
        return sum(p.ab_result_ibb for p in self.pfx)

    @cached_property
    def total_hard_hits(self) -> int:
        return sum(p.is_hard_hit for p in self.pfx)

    @cached_property
    def total_medium_hits(self) -> int:
        return sum(p.is_medium_hit for p in self.pfx)

    @cached_property
    def total_soft_hits(self) -> int:
        return sum(p.is_soft_hit for p in self.pfx)

    @cached_property
    def total_barrels(self) -> int:
        return sum(p.is_barreled for p in self.pfx)

    @cached_property
    def avg_speed(self) -> float:
        return sum(p.start_speed for p in self.pfx) / float(self.total_pitches) if self.total_pitches else 0.0

    @cached_property
    def avg_pfx_x(self) -> float:
        return sum(p.pfx_x for p in self.pfx) / float(self.total_pitches) if self.total_pitches else 0.0

    @cached_property
    def avg_pfx_z(self) -> float:
        return sum(p.pfx_z for p in self.pfx) / float(self.total_pitches) if self.total_pitches else 0.0

    @cached_property
    def avg_px(self) -> float:
        return sum(p.px for p in self.pfx) / float(self.total_pitches) if self.total_pitches else 0.0

    @cached_property
    def avg_pz(self) -> float:
        return sum(p.pz for p in self.pfx) / float(self.total_pitches) if self.total_pitches else 0.0

    @cached_property
    def avg_plate_time(self) -> float:
        return sum(p.plate_time for p in self.pfx) / float(self.total_pitches) if self.total_pitches else 0.0

    @cached_property
    def avg_extension(self) -> float:
        return sum(p.extension for p in self.pfx) / float(self.total_pitches) if self.total_pitches else 0.0

    @cached_property
    def avg_break_angle(self) -> float:
        return sum(p.break_angle for p in self.pfx) / float(self.total_pitches) if self.total_pitches else 0.0

    @cached_property
    def avg_break_length(self) -> float:
        return sum(p.break_length for p in self.pfx) / float(self.total_pitches) if self.total_pitches else 0.0

    @cached_property
    def avg_break_y(self) -> float:
        return sum(p.break_y for p in self.pfx) / float(self.total_pitches) if self.total_pitches else 0.0

    @cached_property
    def avg_spin_rate(self) -> float:
        return sum(p.spin_rate for p in self.pfx) / float(self.total_pitches) if self.total_pitches else 0.0

    @cached_property
    def avg_spin_direction(self) -> float:
        return sum(p.spin_direction for p in self.pfx) / float(self.total_pitches) if self.total_pitches else 0.0

    @cached_property
    def zone_rate(self) -> float:
        return self.total_inside_strike_zone / float(self.total_pitches) if self.total_pitches else 0.0

    @cached_property
    def called_strike_rate(self) -> float:
        return self.total_called_strikes / float(self.total_pitches) if self.total_pitches else 0.0

    @cached_property
    def swinging_strike_rate(self) -> float:
        return self.total_swinging_strikes / float(self.total_pitches) if self.total_pitches else 0.0

    @cached_property
    def whiff_rate(self) -> float:
        return self.total_swinging_strikes / float(self.total_swings) if self.total_swings else 0.0

    @cached_property
    def csw_rate(self) -> float:
        return (
            (self.total_swinging_strikes + self.total_called_strikes) / float(self.total_pitches)
            if self.total_pitches
            else 0.0
        )

    @cached_property
    def bad_whiff_rate(self) -> float:
        return self.total_bad_whiffs / float(self.total_swings) if self.total_swings else 0.0

    @cached_property
    def o_swing_rate(self) -> float:
        return (
            self.total_swings_outside_zone / float(self.total_outside_strike_zone)
            if self.total_outside_strike_zone
            else 0.0
        )

    @cached_property
    def z_swing_rate(self) -> float:
        return (
            self.total_swings_inside_zone / float(self.total_inside_strike_zone)
            if self.total_inside_strike_zone
            else 0.0
        )

    @cached_property
    def swing_rate(self) -> float:
        return self.total_swings / float(self.total_pitches) if self.total_pitches else 0.0

    @cached_property
    def o_contact_rate(self) -> float:
        return (
            self.total_contact_outside_zone / float(self.total_swings_outside_zone)
            if self.total_swings_outside_zone
            else 0.0
        )

    @cached_property
    def z_contact_rate(self) -> float:
        return (
            self.total_contact_inside_zone / float(self.total_swings_inside_zone)
            if self.total_swings_inside_zone
            else 0.0
        )

    @cached_property
    def contact_rate(self) -> float:
        return self.total_swings_made_contact / float(self.total_swings) if self.total_swings else 0.0

    @cached_property
    def ground_ball_rate(self) -> float:
        return self.total_ground_balls / float(self.total_balls_in_play) if self.total_balls_in_play else 0.0

    @cached_property
    def fly_ball_rate(self) -> float:
        return self.total_fly_balls / float(self.total_balls_in_play) if self.total_balls_in_play else 0.0

    @cached_property
    def line_drive_rate(self) -> float:
        return self.total_line_drives / float(self.total_balls_in_play) if self.total_balls_in_play else 0.0

    @cached_property
    def popup_rate(self) -> float:
        return self.total_popups / float(self.total_balls_in_play) if self.total_balls_in_play else 0.0

    @cached_property
    def avg(self) -> float:
        return self.total_hits / float(self.total_at_bats) if self.total_at_bats else 0.0

    @cached_property
    def obp(self) -> float:
        obp_denominator = self.total_at_bats + self.total_bb + self.total_hbp + self.total_sac_fly
        return (self.total_hits + self.total_bb + self.total_hbp) / float(obp_denominator) if obp_denominator else 0.0

    @cached_property
    def slg(self) -> float:
        return (
            (self.total_singles + (self.total_doubles * 2) + (self.total_triples * 3) + (self.total_homeruns * 4))
            / float(self.total_at_bats)
            if self.total_at_bats
            else 0.0
        )

    @cached_property
    def ops(self) -> float:
        return self.obp + self.slg

    @cached_property
    def iso(self) -> float:
        return self.slg - self.avg

    @cached_property
    def bb_rate(self) -> float:
        return self.total_bb / float(self.total_pa) if self.total_pa else 0.0

    @cached_property
    def k_rate(self) -> float:
        return self.total_k / float(self.total_pa) if self.total_pa else 0.0

    @cached_property
    def hr_per_fb(self) -> float:
        return self.total_homeruns / float(self.total_fly_balls) if self.total_fly_balls else 0.0

    @cached_property
    def avg_launch_speed(self) -> float:
        balls_in_play = [p for p in self.pfx if p.launch_speed]
        return sum(p.launch_speed for p in balls_in_play) / len(balls_in_play) if balls_in_play else 0.0

    @cached_property
    def max_launch_speed(self) -> float:
        has_launch_speed = [p.launch_speed for p in self.pfx if p.launch_speed]
        return max(has_launch_speed) if has_launch_speed else 0.0

    @cached_property
    def avg_launch_angle(self) -> float:
        balls_in_play = [p for p in self.pfx if p.launch_angle]
        return sum(p.launch_angle for p in balls_in_play) / len(balls_in_play) if balls_in_play else 0.0

    @cached_property
    def avg_hit_distance(self) -> float:
        balls_in_play = [p for p in self.pfx if p.total_distance]
        return sum(p.total_distance for p in balls_in_play) / len(balls_in_play) if balls_in_play else 0.0

    @cached_property
    def hard_hit_rate(self) -> float:
        return self.total_hard_hits / float(self.total_balls_in_play) if self.total_balls_in_play else 0.0

    @cached_property
    def medium_hit_rate(self) -> float:
        return self.total_medium_hits / float(self.total_balls_in_play) if self.total_balls_in_play else 0.0

    @cached_property
    def soft_hit_rate(self) -> float:
        return self.total_soft_hits / float(self.total_balls_in_play) if self.total_balls_in_play else 0.0

    @cached_property
    def barrel_rate(self) -> float:
        return self.total_barrels / float(self.total_balls_in_play) if self.total_balls_in_play else 0.0

    def as_dict(self):
        dict_keys = list(filter(lambda x: not x.startswith(("__", "for_", "as_", "create_", "get_", "pfx")), dir(self)))
        pfx_metrics_dict = {key: getattr(self, key) for key in dict_keys}
        pfx_metrics_dict.pop("pitch_type")
        pfx_metrics_dict.pop("pitch_name")
        pfx_metrics_dict["pitch_type"] = pfx_metrics_dict.pop("pitch_type_abbrevs")
        return pfx_metrics_dict

    def get_bat_stats(self, include_pa_count: bool = False) -> Dict[str, str]:
        total_pa = f" ({self.total_pa})" if include_pa_count else ""
        return {
            "pitch_type": f"{self.pitch_name}{total_pa}",
            "AVG/OBP/SLG (OPS)": self.triple_slash,
            "K%": f"{self.k_rate:.0%}",
            "BB%": f"{self.bb_rate:.0%}",
            "HR/FB": f"{self.hr_per_fb:.0%}",
        }

    def get_plate_discipline_stats(self, include_pitch_count: bool = False) -> Dict[str, str]:
        total_pitches = f" ({self.total_pitches})" if include_pitch_count else ""
        pd_stats = {"pitch_type": f"{self.pitch_name}{total_pitches}"}
        for metric, (rate, total) in PFX_PLATE_DISCIPLINE_METRICS.items():
            pitch_count = f" ({getattr(self, total)})" if include_pitch_count else ""
            pd_stats[metric] = f"{getattr(self, rate):.0%}{pitch_count}"
        return pd_stats

    def get_batted_ball_stats(self, include_bip_count: bool = False) -> Dict[str, str]:
        total_bip = f" ({self.total_balls_in_play})" if include_bip_count else ""
        bb_stats = {"pitch_type": f"{self.pitch_name}{total_bip}"}
        for metric, (rate, total) in PFX_BATTED_BALL_METRICS.items():
            bip_count = f" ({getattr(self, total)})" if include_bip_count else ""
            bb_stats[metric] = f"{getattr(self, rate):.0%}{bip_count}"
        return bb_stats

    def get_usage_stats(self, include_pitch_count: bool = False) -> str:
        pitch_count = f" ({self.total_pitches})" if include_pitch_count else ""
        return f"{self.percent:.0%}{pitch_count} {self.avg_speed:.1f}mph"
