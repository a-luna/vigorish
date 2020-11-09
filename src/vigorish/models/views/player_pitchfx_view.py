from sqlalchemy import case, func, join, or_, select
from sqlalchemy_utils import create_view

from vigorish.config.database import Base, PitchFx, PlayerId


class Pitch_Mix_All_View(Base):
    __table__ = create_view(
        name="pitch_mix_all",
        selectable=select(
            [
                PlayerId.db_player_id.label("id"),
                PlayerId.mlb_id.label("mlb_id"),
                PlayerId.mlb_name.label("name"),
                func.count(PitchFx.id).label("total_pitches"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "CH", 1)], else_=0)).label("ch_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "CU", 1)], else_=0)).label("cu_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "EP", 1)], else_=0)).label("ep_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "FA", 1)], else_=0)).label("fa_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "FC", 1)], else_=0)).label("fc_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "FF", 1)], else_=0)).label("ff_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "FS", 1)], else_=0)).label("fs_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "FT", 1)], else_=0)).label("ft_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "FO", 1)], else_=0)).label("fo_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "IN", 1)], else_=0)).label("in_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "KC", 1)], else_=0)).label("kc_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "KN", 1)], else_=0)).label("kn_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "PO", 1)], else_=0)).label("po_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "SC", 1)], else_=0)).label("sc_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "SI", 1)], else_=0)).label("si_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "SL", 1)], else_=0)).label("sl_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "UN", 1)], else_=0)).label("un_count"),
            ]
        )
        .where(or_(PitchFx.stand == "L", PitchFx.stand == "R"))
        .select_from(
            join(
                PlayerId,
                PitchFx,
                PlayerId.db_player_id == PitchFx.pitcher_id,
                isouter=True,
            )
        )
        .group_by(PlayerId.db_player_id)
        .order_by(PlayerId.mlb_name),
        metadata=Base.metadata,
        cascade_on_drop=False,
    )


class Pitch_Mix_Left_View(Base):
    __table__ = create_view(
        name="pitch_mix_left",
        selectable=select(
            [
                PlayerId.db_player_id.label("id"),
                PlayerId.mlb_id.label("mlb_id"),
                PlayerId.mlb_name.label("name"),
                func.count(PitchFx.id).label("total_pitches"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "CH", 1)], else_=0)).label("ch_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "CU", 1)], else_=0)).label("cu_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "EP", 1)], else_=0)).label("ep_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "FA", 1)], else_=0)).label("fa_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "FC", 1)], else_=0)).label("fc_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "FF", 1)], else_=0)).label("ff_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "FS", 1)], else_=0)).label("fs_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "FT", 1)], else_=0)).label("ft_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "FO", 1)], else_=0)).label("fo_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "IN", 1)], else_=0)).label("in_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "KC", 1)], else_=0)).label("kc_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "KN", 1)], else_=0)).label("kn_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "PO", 1)], else_=0)).label("po_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "SC", 1)], else_=0)).label("sc_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "SI", 1)], else_=0)).label("si_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "SL", 1)], else_=0)).label("sl_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "UN", 1)], else_=0)).label("un_count"),
            ]
        )
        .where(PitchFx.stand == "L")
        .select_from(
            join(
                PlayerId,
                PitchFx,
                PlayerId.db_player_id == PitchFx.pitcher_id,
                isouter=True,
            )
        )
        .group_by(PlayerId.db_player_id)
        .order_by(PlayerId.mlb_name),
        metadata=Base.metadata,
        cascade_on_drop=False,
    )


class Pitch_Mix_Right_View(Base):
    __table__ = create_view(
        name="pitch_mix_right",
        selectable=select(
            [
                PlayerId.db_player_id.label("id"),
                PlayerId.mlb_id.label("mlb_id"),
                PlayerId.mlb_name.label("name"),
                func.count(PitchFx.id).label("total_pitches"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "CH", 1)], else_=0)).label("ch_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "CU", 1)], else_=0)).label("cu_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "EP", 1)], else_=0)).label("ep_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "FA", 1)], else_=0)).label("fa_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "FC", 1)], else_=0)).label("fc_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "FF", 1)], else_=0)).label("ff_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "FS", 1)], else_=0)).label("fs_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "FT", 1)], else_=0)).label("ft_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "FO", 1)], else_=0)).label("fo_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "IN", 1)], else_=0)).label("in_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "KC", 1)], else_=0)).label("kc_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "KN", 1)], else_=0)).label("kn_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "PO", 1)], else_=0)).label("po_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "SC", 1)], else_=0)).label("sc_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "SI", 1)], else_=0)).label("si_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "SL", 1)], else_=0)).label("sl_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "UN", 1)], else_=0)).label("un_count"),
            ]
        )
        .where(PitchFx.stand == "R")
        .select_from(
            join(
                PlayerId,
                PitchFx,
                PlayerId.db_player_id == PitchFx.pitcher_id,
                isouter=True,
            )
        )
        .group_by(PlayerId.db_player_id)
        .order_by(PlayerId.mlb_name),
        metadata=Base.metadata,
        cascade_on_drop=False,
    )


class Pitch_Mix_By_Year_View(Base):
    __table__ = create_view(
        name="pitch_mix_by_year",
        selectable=select(
            [
                PlayerId.db_player_id.label("id"),
                PitchFx.season_id.label("season_id"),
                PlayerId.mlb_id.label("mlb_id"),
                PlayerId.mlb_name.label("name"),
                func.count(PitchFx.id).label("total_pitches"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "CH", 1)], else_=0)).label("ch_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "CU", 1)], else_=0)).label("cu_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "EP", 1)], else_=0)).label("ep_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "FA", 1)], else_=0)).label("fa_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "FC", 1)], else_=0)).label("fc_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "FF", 1)], else_=0)).label("ff_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "FS", 1)], else_=0)).label("fs_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "FT", 1)], else_=0)).label("ft_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "FO", 1)], else_=0)).label("fo_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "IN", 1)], else_=0)).label("in_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "KC", 1)], else_=0)).label("kc_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "KN", 1)], else_=0)).label("kn_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "PO", 1)], else_=0)).label("po_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "SC", 1)], else_=0)).label("sc_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "SI", 1)], else_=0)).label("si_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "SL", 1)], else_=0)).label("sl_count"),
                func.sum(case([(PitchFx.mlbam_pitch_name == "UN", 1)], else_=0)).label("un_count"),
            ]
        )
        .where(or_(PitchFx.stand == "L", PitchFx.stand == "R"))
        .select_from(
            join(
                PlayerId,
                PitchFx,
                PlayerId.db_player_id == PitchFx.pitcher_id,
                isouter=True,
            )
        )
        .group_by(PitchFx.season_id)
        .group_by(PlayerId.db_player_id)
        .order_by(PlayerId.mlb_name),
        metadata=Base.metadata,
        cascade_on_drop=False,
    )
