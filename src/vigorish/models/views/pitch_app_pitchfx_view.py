from sqlalchemy import and_, func, or_, select
from sqlalchemy_utils import create_view

from vigorish.database import Base, PitchFx


class PitchApp_PitchType_View(Base):
    __table__ = create_view(
        name="pitch_app_pitch_type",
        selectable=select(
            [
                PitchFx.pitch_app_db_id.label("id"),
                PitchFx.pitcher_id.label("pitcher_id"),
                PitchFx.pitcher_id_mlb.label("pitcher_id_mlb"),
                PitchFx.pitch_app_id.label("pitch_app_id"),
                PitchFx.bbref_game_id.label("bbref_game_id"),
                PitchFx.team_pitching_id.label("team_pitching_id"),
                PitchFx.team_batting_id.label("team_batting_id"),
                PitchFx.game_status_id.label("game_status_id"),
                PitchFx.date_id.label("date_id"),
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
        .group_by(PitchFx.pitch_app_db_id)
        .group_by(PitchFx.mlbam_pitch_name)
        .order_by(PitchFx.pitch_app_id),
        metadata=Base.metadata,
        cascade_on_drop=False,
    )
