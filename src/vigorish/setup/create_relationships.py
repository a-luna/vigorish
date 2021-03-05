from tqdm import tqdm

import vigorish.database as db
from vigorish.enums import SeasonType
from vigorish.util.exceptions import InvalidSeasonException
from vigorish.util.result import Result


def create_relationships(db_session):
    result = link_teams_and_seasons(db_session)
    if result.failure:
        return result

    db_session.commit()
    return Result.Ok()


def link_teams_and_seasons(db_session):
    for team in tqdm(
        db_session.query(db.Team).all(),
        desc="Linking team/season tables.....",
        unit="row",
        mininterval=0.12,
        maxinterval=5,
        unit_scale=True,
        ncols=90,
    ):
        try:
            reg_season = db.Season.find_by_year(db_session, team.year)
            post_season = db.Season.find_by_year(db_session, team.year, season_type=SeasonType.POST_SEASON)
            team.regular_season_id = reg_season.id
            team.post_season_id = post_season.id
        except InvalidSeasonException:
            continue
    return Result.Ok()
