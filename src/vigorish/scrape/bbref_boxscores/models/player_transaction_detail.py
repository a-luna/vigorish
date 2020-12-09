class BBRefTransactionDetail:
    player_id = 0
    player_id_br = ""
    draft_year = ""
    draft_round = ""
    draft_pick = ""
    old_team_id_br = 0
    new_team_id_br = 0

    def as_dict(self):
        dict = {
            "player_id": f"{self.player_id}",
            "player_id_br": f"{self.player_id_br}",
            "old_team_id_br": f"{self.old_team_id_br}",
            "new_team_id_br": f"{self.new_team_id_br}",
        }

        if self.draft_year and not self.draft_year.isspace():
            dict["draft_year"] = int(self.draft_year)
        if self.draft_round and not self.draft_round.isspace():
            dict["draft_round"] = int(self.draft_round)
        if self.draft_pick and not self.draft_pick.isspace():
            dict["draft_pick"] = int(self.draft_pick)
        return dict
