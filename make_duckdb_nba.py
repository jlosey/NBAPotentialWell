import duckdb
from dbnba.nba_db import NBA_Season
import pandas as pd
from nba_api.stats.endpoints import leaguegamefinder

season = '2022-23'

nba22 = NBA_Season(season=season)

teams = nba22._get_teams()


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

con.execute("""CREATE TABLE IF NOT EXISTS games (GAME_ID INTEGER PRIMARY KEY, 
            SEASON_ID INTEGER, TEAM_ID_HOME INTEGER, TEAM_ID_AWAY INTEGER, 
            GAME_DATE DATE, MATCHUP VARCHAR, PTS_HOME INTEGER, PTS_AWAY INTEGER, 
            MIN_HOME VARCHAR, MIN_AWAY VARCHAR)""")
#for gid in nba22.game_ids:
games = leaguegamefinder.LeagueGameFinder(#team_id_nullable=gid,
                        season_nullable=nba22.season,
                        season_type_nullable=nba22.season_type)
games_data = games.get_data_frames()[0]
home_data = games_data.loc[games_data['MATCHUP'].str.contains('vs'), ['SEASON_ID', 'GAME_ID', 'TEAM_ID', 'GAME_DATE', 'MATCHUP', 'PTS','MIN']]
away_data = games_data.loc[games_data['MATCHUP'].str.contains('@'), ['SEASON_ID', 'GAME_ID', 'TEAM_ID', 'GAME_DATE', 'MATCHUP', 'PTS','MIN']]
merged_data = pd.merge(right=home_data, left=away_data, right_on='GAME_ID', left_on='GAME_ID', suffixes=('_HOME', '_AWAY'))
for n,row in merged_data.iterrows():
    con.execute("INSERT INTO games VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (row['GAME_ID'], row['SEASON_ID_HOME'], row['TEAM_ID_HOME'], row['TEAM_ID_AWAY'],
                    row['GAME_DATE_HOME'], row['MATCHUP_HOME'], row['PTS_HOME'], row['PTS_AWAY'],
                    row['MIN_HOME'], row['MIN_AWAY']))

