"""Populate mlb_season table with initial data."""
from tqdm import tqdm

from app.main.constants import SEASON_TYPE_DICT
from app.main.models.season import Season

def populate_seasons(session):
    """Populate mlb_season table with initial data."""
    print('\nPopulating mlb_season table:')
    result = __add_mlb_seasons(session)
    if not result['success']:
        return result
    session.commit()
    return dict(success=True)

def __add_mlb_seasons(session):
    try:
        mlb2016season = Season(
            year=2016,
            start_date='2016-04-03',
            end_date='2016-10-02',
            asg_date='2016-07-12',
            season_type=SEASON_TYPE_DICT['reg']
        )
        mlb2016postseason = Season(
            year=2016,
            start_date='2016-10-03',
            end_date='2016-11-02',
            season_type=SEASON_TYPE_DICT['post']
        )
        mlb2017season = Season(
            year=2017,
            start_date='2017-04-02',
            end_date='2017-10-01',
            asg_date='2017-07-11',
            season_type=SEASON_TYPE_DICT['reg']
        )
        mlb2017postseason = Season(
            year=2017,
            start_date='2017-10-03',
            end_date='2017-11-01',
            season_type=SEASON_TYPE_DICT['post']
        )
        mlb2018season = Season(
            year=2018,
            start_date='2018-03-29',
            end_date='2018-10-01',
            asg_date='2018-07-17',
            season_type=SEASON_TYPE_DICT['reg']
        )
        mlb2018postseason = Season(
            year=2018,
            start_date='2018-10-02',
            end_date='2018-10-31',
            season_type=SEASON_TYPE_DICT['post']
        )
        mlb2019season = Season(
            year=2019,
            start_date='2019-03-20',
            end_date='2019-09-29',
            asg_date='2019-07-09',
            season_type=SEASON_TYPE_DICT['reg']
        )
        mlb2019postseason = Season(
            year=2019,
            start_date='2019-10-01',
            end_date='2019-10-30',
            season_type=SEASON_TYPE_DICT['post']
        )

        seasons = []
        seasons.append(mlb2016season)
        seasons.append(mlb2016postseason)
        seasons.append(mlb2017season)
        seasons.append(mlb2017postseason)
        seasons.append(mlb2018season)
        seasons.append(mlb2018postseason)
        seasons.append(mlb2019season)
        seasons.append(mlb2019postseason)

        for season in tqdm(seasons, ncols=100, unit='row'):
            session.add(season)
        return dict(success=True)
    except Exception as e:
        error = 'Error: {error}'.format(error=repr(e))
        session.rollback()
        return dict(success=False, message=error)