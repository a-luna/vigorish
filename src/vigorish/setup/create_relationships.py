from tqdm import tqdm

from vigorish.enums import SeasonType
from vigorish.models.player import Player
from vigorish.models.player_id import PlayerId
from vigorish.models.season import Season
from vigorish.models.team import Team
from vigorish.util.result import Result


def create_relationships(db_session):
    result = link_player_tables(db_session)
    if result.failure:
        return result
    result = link_teams_and_seasons(db_session)
    if result.failure:
        return result

    db_session.commit()
    return Result.Ok()


def link_player_tables(db_session):
    try:
        for p in tqdm(
            db_session.query(Player).all(),
            desc="Linking player tables........",
            unit="row",
            mininterval=0.12,
            maxinterval=5,
            unit_scale=True,
            ncols=90,
        ):
            player_id = PlayerId.find_by_mlb_id(db_session, p.mlb_id)
            if player_id:
                player_id.db_player_id = p.id
        return Result.Ok()
    except Exception as e:
        error = "Error: {error}".format(error=repr(e))
        db_session.rollback()
        return Result.Fail(error)


def link_teams_and_seasons(db_session):
    try:
        for team in tqdm(
            db_session.query(Team).all(),
            desc="Linking team/season tables...",
            unit="row",
            mininterval=0.12,
            maxinterval=5,
            unit_scale=True,
            ncols=90,
        ):
            reg_season = Season.find_by_year(db_session, team.year)
            post_season = Season.find_by_year(
                db_session, team.year, season_type=SeasonType.POST_SEASON
            )
            if reg_season:
                team.regular_season_id = reg_season.id
            if post_season:
                team.post_season_id = post_season.id
        return Result.Ok()
    except Exception as e:
        error = "Error: {error}".format(error=repr(e))
        db_session.rollback()
        return Result.Fail(error)
