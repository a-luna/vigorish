"""Team statistics for a single season."""
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship

from vigorish.config.database import Base
from vigorish.util.list_helpers import display_dict


class Team(Base):
    """Team statistics for a single season."""

    __tablename__ = "team"
    id = Column(Integer, primary_key=True)
    year = Column(Integer)
    league = Column(String)
    team_id = Column(String)
    franch_id = Column(String)
    division = Column(String)
    games = Column(Integer)
    games_at_home = Column(Integer)
    wins = Column(Integer)
    losses = Column(Integer)
    runs = Column(Integer)
    at_bats = Column(Integer)
    hits = Column(Integer)
    doubles = Column(Integer)
    triples = Column(Integer)
    homeruns = Column(Integer)
    base_on_balls = Column(Integer)
    strikeouts = Column(Integer)
    stolen_bases = Column(Integer)
    caught_stealing = Column(Integer)
    runs_against = Column(Integer)
    earned_runs = Column(Integer)
    saves = Column(Integer)
    ip_outs = Column(Integer)
    errors = Column(Integer)
    name = Column(String)
    park = Column(String)
    batting_park_factor = Column(Integer)
    pitching_park_factor = Column(Integer)
    team_id_br = Column(String)
    team_id_retro = Column(String)
    regular_season_id = Column(Integer, ForeignKey("season.id"))
    post_season_id = Column(Integer, ForeignKey("season.id"))

    regular_season = relationship("Season", foreign_keys=[regular_season_id])
    post_season = relationship("Season", foreign_keys=[post_season_id])

    boxscores = relationship(
        "Boxscore",
        primaryjoin="or_(" "Team.id==Boxscore.away_team_id, " "Team.id==Boxscore.home_team_id)",
    )

    team_pitching_stats = relationship(
        "GamePitchStats",
        primaryjoin="Team.id==GamePitchStats.player_team_id",
        back_populates="player_team",
    )
    opponent_pitching_stats = relationship(
        "GamePitchStats",
        primaryjoin="Team.id==GamePitchStats.opponent_team_id",
        back_populates="opponent_team",
    )

    team_batting_stats = relationship(
        "GameBatStats",
        primaryjoin="Team.id==GameBatStats.player_team_id",
        back_populates="player_team",
    )
    opponent_batting_stats = relationship(
        "GameBatStats",
        primaryjoin="Team.id==GameBatStats.opponent_team_id",
        back_populates="opponent_team",
    )

    # player_transactions_out = (
    #   relationship('PlayerTransactionLink',
    #   primaryjoin='Team.id==PlayerTransactionLink.old_team_id',
    #   back_populates='old_team')
    # )
    # player_transactions_in = (
    #   relationship('PlayerTransactionLink',
    #   primaryjoin='Team.id==PlayerTransactionLink.new_team_id',
    #   back_populates='new_team')
    # )

    def __repr__(self):
        return f"<Team team_id={self.team_id}, year={self.year}>"

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def display(self):
        season_dict = self.as_dict()
        title = f"{self.name} {self.year}"
        display_dict(season_dict, title=title)

    @classmethod
    def find_by_name_and_year(cls, db_session, name, year):
        return db_session.query(cls).filter_by(team_id=name).filter_by(year=year).first()
