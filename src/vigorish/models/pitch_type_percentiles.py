from sqlalchemy import Boolean, Column, Float, Integer, String

import vigorish.database as db


class PitchTypePercentile(db.Base):

    __tablename__ = "pitch_type_percentiles"
    id = Column(Integer, primary_key=True)
    stat_name = Column(String)
    pitch_type = Column(String)
    thrown_r = Column(Boolean)
    thrown_l = Column(Boolean)
    thrown_both = Column(Boolean)
    percentile = Column(Float)
    stat_value = Column(Float)

    @classmethod
    def get_percentile_for_rhp_pos_stat(cls, db_session, stat_name, pfx_metrics):
        stat_value = getattr(pfx_metrics, stat_name)
        pt_percentile = (
            db_session.query(cls)
            .filter_by(stat_name=stat_name)
            .filter_by(thrown_r=True)
            .filter_by(pitch_type=str(pfx_metrics.pitch_type))
            .filter(cls.stat_value >= stat_value)
            .first()
        )
        return (stat_value, pt_percentile.percentile) if pt_percentile else (stat_value, 100.0)

    @classmethod
    def get_percentile_for_lhp_pos_stat(cls, db_session, stat_name, pfx_metrics):
        stat_value = getattr(pfx_metrics, stat_name)
        pt_percentile = (
            db_session.query(cls)
            .filter_by(stat_name=stat_name)
            .filter_by(thrown_l=True)
            .filter_by(pitch_type=str(pfx_metrics.pitch_type))
            .filter(cls.stat_value >= stat_value)
            .first()
        )
        return (stat_value, pt_percentile.percentile) if pt_percentile else (stat_value, 100.0)

    @classmethod
    def get_percentile_for_rhp_neg_stat(cls, db_session, stat_name, pfx_metrics):
        stat_value = getattr(pfx_metrics, stat_name)
        pt_percentile = (
            db_session.query(cls)
            .filter_by(stat_name=stat_name)
            .filter_by(thrown_r=True)
            .filter_by(pitch_type=str(pfx_metrics.pitch_type))
            .filter(cls.stat_value <= stat_value)
            .first()
        )
        return (stat_value, pt_percentile.percentile) if pt_percentile else (stat_value, 100.0)

    @classmethod
    def get_percentile_for_lhp_neg_stat(cls, db_session, stat_name, pfx_metrics):
        stat_value = getattr(pfx_metrics, stat_name)
        pt_percentile = (
            db_session.query(cls)
            .filter_by(stat_name=stat_name)
            .filter_by(thrown_l=True)
            .filter_by(pitch_type=str(pfx_metrics.pitch_type))
            .filter(cls.stat_value <= stat_value)
            .first()
        )
        return (stat_value, pt_percentile.percentile) if pt_percentile else (stat_value, 100.0)

    @classmethod
    def calculate_pitch_type_percentiles(cls, db_session, p_throws, pfx):
        return (
            cls.calculate_pitch_type_percentiles_for_rhp(db_session, pfx)
            if p_throws == "R"
            else cls.calculate_pitch_type_percentiles_for_lhp(db_session, pfx)
        )

    @classmethod
    def calculate_pitch_type_percentiles_for_rhp(cls, db_session, pfx):
        return {
            "pitch_type": str(pfx.pitch_type),
            "avg_speed": cls.get_percentile_for_rhp_pos_stat(db_session, "avg_speed", pfx),
            "ops": cls.get_percentile_for_rhp_neg_stat(db_session, "ops", pfx),
            "zone_rate": cls.get_percentile_for_rhp_pos_stat(db_session, "zone_rate", pfx),
            "o_swing_rate": cls.get_percentile_for_rhp_pos_stat(db_session, "o_swing_rate", pfx),
            "whiff_rate": cls.get_percentile_for_rhp_pos_stat(db_session, "whiff_rate", pfx),
            "bad_whiff_rate": cls.get_percentile_for_rhp_pos_stat(db_session, "bad_whiff_rate", pfx),
            "contact_rate": cls.get_percentile_for_rhp_neg_stat(db_session, "contact_rate", pfx),
            "ground_ball_rate": cls.get_percentile_for_rhp_pos_stat(db_session, "ground_ball_rate", pfx),
            "barrel_rate": cls.get_percentile_for_rhp_neg_stat(db_session, "barrel_rate", pfx),
            "avg_exit_velocity": cls.get_percentile_for_rhp_neg_stat(db_session, "avg_launch_speed", pfx),
        }

    @classmethod
    def calculate_pitch_type_percentiles_for_lhp(cls, db_session, pfx):
        return {
            "pitch_type": str(pfx.pitch_type),
            "avg_speed": cls.get_percentile_for_lhp_pos_stat(db_session, "avg_speed", pfx),
            "ops": cls.get_percentile_for_lhp_neg_stat(db_session, "ops", pfx),
            "zone_rate": cls.get_percentile_for_lhp_pos_stat(db_session, "zone_rate", pfx),
            "o_swing_rate": cls.get_percentile_for_lhp_pos_stat(db_session, "o_swing_rate", pfx),
            "whiff_rate": cls.get_percentile_for_lhp_pos_stat(db_session, "whiff_rate", pfx),
            "bad_whiff_rate": cls.get_percentile_for_lhp_pos_stat(db_session, "bad_whiff_rate", pfx),
            "contact_rate": cls.get_percentile_for_lhp_neg_stat(db_session, "contact_rate", pfx),
            "ground_ball_rate": cls.get_percentile_for_lhp_pos_stat(db_session, "ground_ball_rate", pfx),
            "barrel_rate": cls.get_percentile_for_lhp_neg_stat(db_session, "barrel_rate", pfx),
            "avg_exit_velocity": cls.get_percentile_for_lhp_neg_stat(db_session, "avg_launch_speed", pfx),
        }
