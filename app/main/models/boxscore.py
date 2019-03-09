"""Db model that describes a boxscore for an MLB game and tracks data scraping progress."""
from sqlalchemy import Column, Boolean, Index, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import relationship, backref

from app.main.models.base import Base
from app.main.models.game_bat_stats import GameBatStats
from app.main.models.game_pitch_stats import GamePitchStats
from app.main.models.game_starting_lineup import GameStartingLineupSlot
from app.main.models.game_meta import GameMetaInformation
from app.main.models.game_team_totals import GameTeamTotals
from app.main.util.list_functions import display_dict

class Boxscore(Base):
    """Db model that describes a boxscore for an MLB game and tracks data scraping progress."""

    __tablename__ = 'boxscore'
    id = Column(Integer, primary_key=True)
    boxscore_url = Column(String)
    bbref_game_id = Column(String)
    bb_game_id = Column(String)
    away_team_id_br = Column(String)
    home_team_id_br = Column(String)
    away_team_id = Column(Integer, ForeignKey('team.id'))
    home_team_id = Column(Integer, ForeignKey('team.id'))
    scrape_status_date_id = Column(Integer, ForeignKey('scrape_status_date.id'))
    season_id = Column(Integer, ForeignKey('season.id'))

    meta = relationship('GameMetaInformation', backref=backref("boxscore", uselist=False))
    away_team = relationship('Team', foreign_keys=[away_team_id])
    home_team = relationship('Team', foreign_keys=[home_team_id])
    away_team_totals = relationship(
        'GameTeamTotals',
        primaryjoin=('and_('
            'Boxscore.id==GameTeamTotals.boxscore_id, '
            'Boxscore.away_team_id==GameTeamTotals.away_team_id)'
        )
    )
    home_team_totals = relationship(
        'GameTeamTotals',
        primaryjoin=('and_('
            'Boxscore.id==GameTeamTotals.boxscore_id, '
            'Boxscore.home_team_id==GameTeamTotals.home_team_id)'
        )
    )
    starting_lineups = relationship('GameStartingLineupSlot', backref='boxscore')
    batting_stats = relationship('GameBatStats', backref='boxscore')
    pitching_stats = relationship('GamePitchStats', backref='boxscore')

    def __repr__(self):
        return f'<Boxscore(bbref_game_id="{self.bbref_game_id}", id={self.id})>'

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def display(self):
        season_dict = self.as_dict()
        title = f'Details for boxscore: {self.bbref_game_id}'
        display_dict(season_dict, title=title)