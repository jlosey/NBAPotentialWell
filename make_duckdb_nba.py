import duckdb
from dbnba.nba_db import NBA_Season
import pandas as pd

season = '2022-23'

nba22 = NBA_Season(season=season)

teams = nba22._get_teams()
teams_df = pd.DataFrame(teams)

con = duckdb.connect(database='nba.db', read_only=False)
#con.execute('CREATE SEQUENCE IF NOT EXISTS season_seq START 1;')
# Create the seasons table if it doesn't exist and populate with season provided
con.execute("CREATE TABLE IF NOT EXISTS seasons (ID INTEGER PRIMARY KEY, season VARCHAR, season_type VARCHAR)")
if con.execute("SELECT COUNT(*) FROM seasons").fetchone()[0] == 0:
    con.execute("INSERT INTO seasons VALUES (?, ?, ?)", (int('2'+nba22.season[:4]),nba22.season, nba22.season_type))
else:
    print("Season already exists in the database, skipping insert.")
#con.execute("INSERT INTO seasons VALUES (?, ?)", (nba22.season, nba22.season_type))

# Create the teams table if it doesn't exist and populate with teams
con.execute("CREATE TABLE IF NOT EXISTS teams (ID INTEGER, full_name VARCHAR, abbreviation VARCHAR, nickname VARCHAR, city VARCHAR, state VARCHAR)")
[con.execute("INSERT INTO teams VALUES (?, ?, ?, ?, ?, ?)",
             (team['id'], team['full_name'], team['abbreviation'], team['nickname'],
              team['city'], team['state'])) for team in teams]

