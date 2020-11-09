from sqlalchemy import case, func, join, select
from sqlalchemy_utils import create_view

from vigorish.config.database import Base, PitchAppScrapeStatus, PitchFx


class PitchApp_PitchFx_View(Base):
    __table__ = create_view(
        name="pitch_app_pitchfx",
        selectable=select(
            [
                PitchAppScrapeStatus.id.label("id"),
                PitchAppScrapeStatus.pitcher_id_mlb.label("pitcher_id_mlb"),
                PitchAppScrapeStatus.pitch_app_id.label("pitch_app_id"),
                PitchAppScrapeStatus.bbref_game_id.label("bbref_game_id"),
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
        .select_from(
            join(
                PitchAppScrapeStatus,
                PitchFx,
                PitchAppScrapeStatus.id == PitchFx.pitch_app_db_id,
                isouter=True,
            )
        )
        .group_by(PitchAppScrapeStatus.id),
        metadata=Base.metadata,
        cascade_on_drop=False,
    )
