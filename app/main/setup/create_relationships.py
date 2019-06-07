from tqdm import tqdm

from app.main.constants import SEASON_TYPE_DICT
from app.main.models.player import Player
from app.main.models.player_id import PlayerId
from app.main.models.season import Season
from app.main.models.team import Team
from app.main.util.result import Result

def create_relationships(session):
    result = __link_player_tables(session)
    if result.failure:
        return result
    result = __link_teams_and_seasons(session)
    if result.failure:
        return result

    session.commit()
    return Result.Ok()

def __link_player_tables(session):
    try:
        for p in tqdm(
            session.query(Player).all(),
            desc='Linking player tables........',
            unit='row',
            mininterval=0.12,
            maxinterval=5,
            unit_scale=True
        ):
            player_id = PlayerId.find_by_mlb_id(session, p.mlb_id)
            if player_id:
                player_id.db_player_id = p.id
        return Result.Ok()
    except Exception as e:
        error = 'Error: {error}'.format(error=repr(e))
        session.rollback()
        return Result.Fail(error)

def __link_teams_and_seasons(session):
    try:
        for team in tqdm(
            session.query(Team).all(),
            desc='Linking team/season tables...',
            unit='row',
            mininterval=0.12,
            maxinterval=5,
            unit_scale=True
        ):
            reg_season = Season.find_by_year(session, team.year)
            post_season = Season.find_by_year(
                session,
                team.year,
                season_type=SEASON_TYPE_DICT['post']
            )
            if reg_season:
                team.regular_season_id = reg_season.id
            if post_season:
                team.post_season_id = post_season.id
        return Result.Ok()
    except Exception as e:
        error = 'Error: {error}'.format(error=repr(e))
        session.rollback()
        return Result.Fail(error)
