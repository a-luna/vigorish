from sqlalchemy import Boolean, Column, Float, Integer, String

import vigorish.database as db


class BatterPercentile(db.Base):

    __tablename__ = "batter_percentiles"
    id = Column(Integer, primary_key=True)
    stat_name = Column(String)
    bat_r = Column(Boolean)
    bat_l = Column(Boolean)
    bat_both = Column(Boolean)
    percentile = Column(Float)
    stat_value = Column(Float)

    @classmethod
    def get_percentile_for_rhb_pos_stat(cls, db_session, stat_name, pfx_metrics):
        stat_value = getattr(pfx_metrics, stat_name)
        pt_percentile = (
            db_session.query(cls)
            .filter_by(stat_name=stat_name)
            .filter_by(bat_r=True)
            .filter(cls.stat_value >= stat_value)
            .first()
        )
        return (stat_value, pt_percentile.percentile) if pt_percentile else (stat_value, 100.0)

    @classmethod
    def get_percentile_for_lhb_pos_stat(cls, db_session, stat_name, pfx_metrics):
        stat_value = getattr(pfx_metrics, stat_name)
        pt_percentile = (
            db_session.query(cls)
            .filter_by(stat_name=stat_name)
            .filter_by(bat_l=True)
            .filter(cls.stat_value >= stat_value)
            .first()
        )
        return (stat_value, pt_percentile.percentile) if pt_percentile else (stat_value, 100.0)

    @classmethod
    def get_percentile_for_rhb_neg_stat(cls, db_session, stat_name, pfx_metrics):
        stat_value = getattr(pfx_metrics, stat_name)
        pt_percentile = (
            db_session.query(cls)
            .filter_by(stat_name=stat_name)
            .filter_by(bat_r=True)
            .filter(cls.stat_value <= stat_value)
            .first()
        )
        return (stat_value, pt_percentile.percentile) if pt_percentile else (stat_value, 100.0)

    @classmethod
    def get_percentile_for_lhb_neg_stat(cls, db_session, stat_name, pfx_metrics):
        stat_value = getattr(pfx_metrics, stat_name)
        pt_percentile = (
            db_session.query(cls)
            .filter_by(stat_name=stat_name)
            .filter_by(bat_l=True)
            .filter(cls.stat_value <= stat_value)
            .first()
        )
        return (stat_value, pt_percentile.percentile) if pt_percentile else (stat_value, 100.0)

    @classmethod
    def calculate_batter_percentiles(cls, db_session, bat_stand, pfx):
        return (
            cls.calculate_batter_percentiles_for_rhb(db_session, pfx)
            if bat_stand == "R"
            else cls.calculate_batter_percentiles_for_lhb(db_session, pfx)
        )

    @classmethod
    def calculate_batter_percentiles_for_rhb(cls, db_session, pfx):
        return {
            "bb_rate": cls.get_percentile_for_rhb_pos_stat(db_session, "bb_rate", pfx),
            "k_rate": cls.get_percentile_for_rhb_neg_stat(db_session, "k_rate", pfx),
            "whiff_rate": cls.get_percentile_for_rhb_neg_stat(db_session, "whiff_rate", pfx),
            "o_swing_rate": cls.get_percentile_for_rhb_neg_stat(db_session, "o_swing_rate", pfx),
            "contact_rate": cls.get_percentile_for_rhb_pos_stat(db_session, "contact_rate", pfx),
            "soft_hit_rate": cls.get_percentile_for_rhb_neg_stat(db_session, "soft_hit_rate", pfx),
            "barrel_rate": cls.get_percentile_for_rhb_pos_stat(db_session, "barrel_rate", pfx),
            "avg_launch_speed": cls.get_percentile_for_rhb_pos_stat(db_session, "avg_launch_speed", pfx),
            "max_launch_speed": cls.get_percentile_for_rhb_pos_stat(db_session, "max_launch_speed", pfx),
        }

    @classmethod
    def calculate_batter_percentiles_for_lhb(cls, db_session, pfx):
        return {
            "bb_rate": cls.get_percentile_for_lhb_pos_stat(db_session, "bb_rate", pfx),
            "k_rate": cls.get_percentile_for_lhb_neg_stat(db_session, "k_rate", pfx),
            "whiff_rate": cls.get_percentile_for_lhb_neg_stat(db_session, "whiff_rate", pfx),
            "o_swing_rate": cls.get_percentile_for_lhb_neg_stat(db_session, "o_swing_rate", pfx),
            "contact_rate": cls.get_percentile_for_lhb_pos_stat(db_session, "contact_rate", pfx),
            "soft_hit_rate": cls.get_percentile_for_lhb_neg_stat(db_session, "soft_hit_rate", pfx),
            "barrel_rate": cls.get_percentile_for_lhb_pos_stat(db_session, "barrel_rate", pfx),
            "avg_launch_speed": cls.get_percentile_for_lhb_pos_stat(db_session, "avg_launch_speed", pfx),
            "max_launch_speed": cls.get_percentile_for_lhb_pos_stat(db_session, "max_launch_speed", pfx),
        }
