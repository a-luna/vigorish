from dataclasses import dataclass


@dataclass
class BBRefPitchStats:
    """Statistics for a an individual pitching appearance in a single game."""

    player_id_br: str = "0"
    player_team_id_br: str = "0"
    opponent_team_id_br: str = "0"
    innings_pitched: str = "0"
    hits: str = "0"
    runs: str = "0"
    earned_runs: str = "0"
    bases_on_balls: str = "0"
    strikeouts: str = "0"
    homeruns: str = "0"
    batters_faced: str = "0"
    pitch_count: str = "0"
    strikes: str = "0"
    strikes_contact: str = "0"
    strikes_swinging: str = "0"
    strikes_looking: str = "0"
    ground_balls: str = "0"
    fly_balls: str = "0"
    line_drives: str = "0"
    unknown_type: str = "0"
    game_score: str = "0"
    inherited_runners: str = "0"
    inherited_scored: str = "0"
    wpa_pitch: str = "0"
    avg_lvg_index: str = "0"
    re24_pitch: str = "0"

    def as_dict(self):
        """Convert a single player's pitching appearance to a dictionary."""
        return {
            "player_id_br": f"{self.player_id_br}",
            "player_team_id_br": f"{self.player_team_id_br}",
            "opponent_team_id_br": f"{self.opponent_team_id_br}",
            "innings_pitched": float(self.innings_pitched),
            "hits": int(self.hits),
            "runs": int(self.runs),
            "earned_runs": int(self.earned_runs),
            "bases_on_balls": int(self.bases_on_balls),
            "strikeouts": int(self.strikeouts),
            "homeruns": int(self.homeruns),
            "batters_faced": int(self.batters_faced),
            "pitch_count": int(self.pitch_count),
            "strikes": int(self.strikes),
            "strikes_contact": int(self.strikes_contact),
            "strikes_swinging": int(self.strikes_swinging),
            "strikes_looking": int(self.strikes_looking),
            "ground_balls": int(self.ground_balls),
            "fly_balls": int(self.fly_balls),
            "line_drives": int(self.line_drives),
            "unknown_type": int(self.unknown_type),
            "game_score": int(self.game_score),
            "inherited_runners": int(self.inherited_runners),
            "inherited_scored": int(self.inherited_scored),
            "wpa_pitch": float(self.wpa_pitch),
            "avg_lvg_index": float(self.avg_lvg_index),
            "re24_pitch": float(self.re24_pitch),
        }
