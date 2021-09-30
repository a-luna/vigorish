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
        percentiles = {
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
        return format_player_stat_values(percentiles)

    @classmethod
    def calculate_pitch_type_percentiles_for_lhp(cls, db_session, pfx):
        percentiles = {
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
        return format_player_stat_values(percentiles)


def format_player_stat_values(p):
    p["avg_speed"] = (round(p["avg_speed"][0], ndigits=1), p["avg_speed"][1])
    p["ops"] = (round(p["ops"][0], ndigits=3), p["ops"][1])
    p["zone_rate"] = (round(p["zone_rate"][0], ndigits=3), p["zone_rate"][1])
    p["o_swing_rate"] = (round(p["o_swing_rate"][0], ndigits=3), p["o_swing_rate"][1])
    p["whiff_rate"] = (round(p["whiff_rate"][0], ndigits=3), p["whiff_rate"][1])
    p["bad_whiff_rate"] = (round(p["bad_whiff_rate"][0], ndigits=3), p["bad_whiff_rate"][1])
    p["contact_rate"] = (round(p["contact_rate"][0], ndigits=3), p["contact_rate"][1])
    p["ground_ball_rate"] = (round(p["ground_ball_rate"][0], ndigits=3), p["ground_ball_rate"][1])
    p["barrel_rate"] = (round(p["barrel_rate"][0], ndigits=3), p["barrel_rate"][1])
    p["avg_exit_velocity"] = (round(p["avg_exit_velocity"][0], ndigits=1), p["avg_exit_velocity"][1])
    return p
