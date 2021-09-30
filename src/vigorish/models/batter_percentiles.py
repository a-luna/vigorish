from sqlalchemy import Column, Float, Integer, String

import vigorish.database as db


class BatterPercentile(db.Base):

    __tablename__ = "batter_percentiles"
    id = Column(Integer, primary_key=True)
    stat_name = Column(String)
    percentile = Column(Float)
    stat_value = Column(Float)

    @classmethod
    def get_percentile_for_pos_stat(cls, db_session, stat_name, pfx_metrics):
        stat_value = getattr(pfx_metrics, stat_name)
        pt_percentile = (
            db_session.query(cls).filter_by(stat_name=stat_name).filter(cls.stat_value >= stat_value).first()
        )
        return (stat_value, pt_percentile.percentile) if pt_percentile else (stat_value, 100.0)

    @classmethod
    def get_percentile_for_neg_stat(cls, db_session, stat_name, pfx_metrics):
        stat_value = getattr(pfx_metrics, stat_name)
        pt_percentile = (
            db_session.query(cls).filter_by(stat_name=stat_name).filter(cls.stat_value <= stat_value).first()
        )
        return (stat_value, pt_percentile.percentile) if pt_percentile else (stat_value, 100.0)

    @classmethod
    def calculate_batter_percentiles(cls, db_session, pfx):
        percentiles = {
            "bb_rate": cls.get_percentile_for_pos_stat(db_session, "bb_rate", pfx),
            "k_rate": cls.get_percentile_for_neg_stat(db_session, "k_rate", pfx),
            "contact_rate": cls.get_percentile_for_pos_stat(db_session, "contact_rate", pfx),
            "o_swing_rate": cls.get_percentile_for_neg_stat(db_session, "o_swing_rate", pfx),
            "whiff_rate": cls.get_percentile_for_neg_stat(db_session, "whiff_rate", pfx),
            "bad_whiff_rate": cls.get_percentile_for_neg_stat(db_session, "bad_whiff_rate", pfx),
            "line_drive_rate": cls.get_percentile_for_pos_stat(db_session, "line_drive_rate", pfx),
            "barrel_rate": cls.get_percentile_for_pos_stat(db_session, "barrel_rate", pfx),
            "avg_launch_speed": cls.get_percentile_for_pos_stat(db_session, "avg_launch_speed", pfx),
            "max_launch_speed": cls.get_percentile_for_pos_stat(db_session, "max_launch_speed", pfx),
        }
        return format_player_stat_values(percentiles)


def format_player_stat_values(p):
    p["bb_rate"] = (round(p["bb_rate"][0], ndigits=3), p["bb_rate"][1])
    p["k_rate"] = (round(p["k_rate"][0], ndigits=3), p["k_rate"][1])
    p["contact_rate"] = (round(p["contact_rate"][0], ndigits=3), p["contact_rate"][1])
    p["o_swing_rate"] = (round(p["o_swing_rate"][0], ndigits=3), p["o_swing_rate"][1])
    p["whiff_rate"] = (round(p["whiff_rate"][0], ndigits=3), p["whiff_rate"][1])
    p["bad_whiff_rate"] = (round(p["bad_whiff_rate"][0], ndigits=3), p["bad_whiff_rate"][1])
    p["line_drive_rate"] = (round(p["line_drive_rate"][0], ndigits=3), p["line_drive_rate"][1])
    p["barrel_rate"] = (round(p["barrel_rate"][0], ndigits=3), p["barrel_rate"][1])
    p["avg_launch_speed"] = (round(p["avg_launch_speed"][0], ndigits=1), p["avg_launch_speed"][1])
    p["max_launch_speed"] = (round(p["max_launch_speed"][0], ndigits=1), p["max_launch_speed"][1])
    return p
