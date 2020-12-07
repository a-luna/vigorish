from sqlalchemy import and_, func, or_, select
from sqlalchemy_utils import create_view

from vigorish.database import Base, PitchFx


class Pitch_Type_All_View(Base):
    __table__ = create_view(
        name="pitch_type_all",
        selectable=select(
            [
                PitchFx.pitcher_id.label("id"),
                PitchFx.pitcher_id_mlb.label("pitcher_id_mlb"),
                PitchFx.mlbam_pitch_name.label("pitch_type"),
                func.count(PitchFx.id).label("total_pitches"),
                func.avg(PitchFx.start_speed).label("avg_speed"),
                func.avg(PitchFx.pfx_x).label("avg_pfx_x"),
                func.avg(PitchFx.pfx_z).label("avg_pfx_z"),
                func.avg(PitchFx.px).label("avg_px"),
                func.avg(PitchFx.pz).label("avg_pz"),
            ]
        )
        .where(
            and_(
                PitchFx.is_duplicate_guid == 0,
                PitchFx.is_duplicate_pitch_number == 0,
                PitchFx.is_invalid_ibb == 0,
                PitchFx.is_out_of_sequence == 0,
                or_(PitchFx.stand == "L", PitchFx.stand == "R"),
            )
        )
        .select_from(PitchFx)
        .group_by(PitchFx.pitcher_id)
        .group_by(PitchFx.mlbam_pitch_name)
        .order_by(PitchFx.pitcher_id_mlb),
        metadata=Base.metadata,
        cascade_on_drop=False,
    )


class Pitch_Type_Right_View(Base):
    __table__ = create_view(
        name="pitch_type_right",
        selectable=select(
            [
                PitchFx.pitcher_id.label("id"),
                PitchFx.pitcher_id_mlb.label("pitcher_id_mlb"),
                PitchFx.mlbam_pitch_name.label("pitch_type"),
                func.count(PitchFx.id).label("total_pitches"),
                func.avg(PitchFx.start_speed).label("avg_speed"),
                func.avg(PitchFx.pfx_x).label("avg_pfx_x"),
                func.avg(PitchFx.pfx_z).label("avg_pfx_z"),
                func.avg(PitchFx.px).label("avg_px"),
                func.avg(PitchFx.pz).label("avg_pz"),
            ]
        )
        .where(
            and_(
                PitchFx.is_duplicate_guid == 0,
                PitchFx.is_duplicate_pitch_number == 0,
                PitchFx.is_invalid_ibb == 0,
                PitchFx.is_out_of_sequence == 0,
                PitchFx.stand == "R",
            )
        )
        .select_from(PitchFx)
        .group_by(PitchFx.pitcher_id)
        .group_by(PitchFx.mlbam_pitch_name)
        .order_by(PitchFx.pitcher_id_mlb),
        metadata=Base.metadata,
        cascade_on_drop=False,
    )


class Pitch_Type_Left_View(Base):
    __table__ = create_view(
        name="pitch_type_left",
        selectable=select(
            [
                PitchFx.pitcher_id.label("id"),
                PitchFx.pitcher_id_mlb.label("pitcher_id_mlb"),
                PitchFx.mlbam_pitch_name.label("pitch_type"),
                func.count(PitchFx.id).label("total_pitches"),
                func.avg(PitchFx.start_speed).label("avg_speed"),
                func.avg(PitchFx.pfx_x).label("avg_pfx_x"),
                func.avg(PitchFx.pfx_z).label("avg_pfx_z"),
                func.avg(PitchFx.px).label("avg_px"),
                func.avg(PitchFx.pz).label("avg_pz"),
            ]
        )
        .where(
            and_(
                PitchFx.is_duplicate_guid == 0,
                PitchFx.is_duplicate_pitch_number == 0,
                PitchFx.is_invalid_ibb == 0,
                PitchFx.is_out_of_sequence == 0,
                PitchFx.stand == "L",
            )
        )
        .select_from(PitchFx)
        .group_by(PitchFx.pitcher_id)
        .group_by(PitchFx.mlbam_pitch_name)
        .order_by(PitchFx.pitcher_id_mlb),
        metadata=Base.metadata,
        cascade_on_drop=False,
    )


class Pitch_Type_By_Year_View(Base):
    __table__ = create_view(
        name="pitch_type_by_year",
        selectable=select(
            [
                PitchFx.pitcher_id.label("id"),
                PitchFx.pitcher_id_mlb.label("pitcher_id_mlb"),
                PitchFx.season_id.label("season_id"),
                PitchFx.mlbam_pitch_name.label("pitch_type"),
                func.count(PitchFx.id).label("total_pitches"),
                func.avg(PitchFx.start_speed).label("avg_speed"),
                func.avg(PitchFx.pfx_x).label("avg_pfx_x"),
                func.avg(PitchFx.pfx_z).label("avg_pfx_z"),
                func.avg(PitchFx.px).label("avg_px"),
                func.avg(PitchFx.pz).label("avg_pz"),
            ]
        )
        .where(
            and_(
                PitchFx.is_duplicate_guid == 0,
                PitchFx.is_duplicate_pitch_number == 0,
                PitchFx.is_invalid_ibb == 0,
                PitchFx.is_out_of_sequence == 0,
                or_(PitchFx.stand == "L", PitchFx.stand == "R"),
            )
        )
        .select_from(PitchFx)
        .group_by(PitchFx.season_id)
        .group_by(PitchFx.pitcher_id)
        .group_by(PitchFx.mlbam_pitch_name)
        .order_by(PitchFx.pitcher_id_mlb),
        metadata=Base.metadata,
        cascade_on_drop=False,
    )
