import duckdb
from dbnba.nba_db import NBA_Season

season = '2022-23'

NBA_Season.create_db(season=season, force=True)

print(NBA_DB.get_teams())
