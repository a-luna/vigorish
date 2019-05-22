"""Populate mlb_season table with initial data."""
from tqdm import tqdm

from app.main.constants import SEASON_TYPE_DICT
from app.main.models.season import Season
from app.main.util.result import Result

def populate_seasons(session):
    """Populate mlb_season table with initial data."""
    result = __add_mlb_seasons(session)
    if result.failure:
        return result
    session.commit()
    return Result.Ok()

def __add_mlb_seasons(session):
    try:
        mlb_seasons = [
            Season(
                year=2016,
                start_date='2016-04-03',
                end_date='2016-10-02',
                asg_date='2016-07-12',
                season_type=SEASON_TYPE_DICT['reg']
            ),
            Season(
                year=2016,
                start_date='2016-10-03',
                end_date='2016-11-02',
                season_type=SEASON_TYPE_DICT['post']
            ),
            Season(
                year=2017,
                start_date='2017-04-02',
                end_date='2017-10-01',
                asg_date='2017-07-11',
                season_type=SEASON_TYPE_DICT['reg']
            ),
            Season(
                year=2017,
                start_date='2017-10-03',
                end_date='2017-11-01',
                season_type=SEASON_TYPE_DICT['post']
            ),
            Season(
                year=2018,
                start_date='2018-03-29',
                end_date='2018-10-01',
                asg_date='2018-07-17',
                season_type=SEASON_TYPE_DICT['reg']
            ),
            Season(
                year=2018,
                start_date='2018-10-02',
                end_date='2018-10-31',
                season_type=SEASON_TYPE_DICT['post']
            ),
            Season(
                year=2019,
                start_date='2019-03-28',
                end_date='2019-09-29',
                asg_date='2019-07-09',
                season_type=SEASON_TYPE_DICT['reg']
            ),
            Season(
                year=2019,
                start_date='2019-10-01',
                end_date='2019-10-30',
                season_type=SEASON_TYPE_DICT['post']
            )
        ]

        for season in tqdm(
            mlb_seasons,
            desc='Populating mlb_season table..',
            ncols=100,
            unit='row',
            mininterval=0.12,
            maxinterval=5,
            unit_scale=True
        ):
            session.add(season)
        return Result.Ok()
    except Exception as e:
        error = 'Error: {error}'.format(error=repr(e))
        session.rollback()
        return Result.Fail(error)
