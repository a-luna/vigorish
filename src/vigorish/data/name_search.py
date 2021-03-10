from functools import cached_property

import vigorish.database as db
from vigorish.util.string_helpers import fuzzy_match


class PlayerNameSearch:
    def __init__(self, db_session):
        self.db_session = db_session

    @cached_property
    def player_id_name_map(self):
        pitcher_ids = {ps.player_id_mlb for ps in self.db_session.query(db.PitchStats).all()}
        batter_ids = {bs.player_id_mlb for bs in self.db_session.query(db.BatStats).all()}
        all_ids = set(list(pitcher_ids) + list(batter_ids))
        return {player_id: db.PlayerId.find_by_mlb_id(self.db_session, player_id).mlb_name for player_id in all_ids}

    def fuzzy_match(self, query, score_cutoff=80):
        return fuzzy_match(query, self.player_id_name_map, score_cutoff)
