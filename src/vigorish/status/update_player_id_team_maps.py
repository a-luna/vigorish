import vigorish.database as db
from vigorish.tasks.scrape_mlb_player_info import ScrapeMlbPlayerInfoTask


def update_player_id_map(app, new_player_ids):
    for id_map in new_player_ids:
        player_id = db.PlayerId(
            mlb_id=int(id_map["mlb_ID"]),
            mlb_name=id_map["name_common"],
            bbref_id=id_map["player_ID"],
            bbref_name=None,
        )
        app.db_session.add(player_id)
        player = db.Player.find_by_bbref_id(app.db_session, player_id.bbref_id)
        if not player:
            scrape_task = ScrapeMlbPlayerInfoTask(app)
            result = scrape_task.find_by_mlb_id(id_map["mlb_ID"], id_map["player_ID"])
            if result.failure:
                continue
            player = result.value
        player_id.db_player_id = player.id
    app.db_session.commit()


def update_player_team_map(app, new_player_teams):
    for team_map in new_player_teams:
        player = db.Player.find_by_bbref_id(app.db_session, team_map["player_ID"])
        if not player:
            scrape_task = ScrapeMlbPlayerInfoTask(app)
            result = scrape_task.find_by_mlb_id(team_map["mlb_ID"], team_map["player_ID"])
            if result.failure:
                continue
            player = result.value
        team = db.Team.find_by_team_id_and_year(app.db_session, team_map["team_ID"], int(team_map["year_ID"]))
        season = db.Season.find_by_year(app.db_session, int(team_map["year_ID"]))
        player_team = db.Assoc_Player_Team(
            db_player_id=player.id,
            db_team_id=team.id,
            mlb_id=int(team_map["mlb_ID"]),
            bbref_id=team_map["player_ID"],
            team_id=team.team_id_br,
            year=int(team_map["year_ID"]),
            stint_number=team_map["stint_ID"],
            season_id=season.id,
        )
        app.db_session.add(player_team)
