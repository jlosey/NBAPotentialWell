import duckdb
from dbnba.nba_db import NBA_Season
import pandas as pd
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.endpoints import playbyplay
from tqdm import tqdm
from numpy import nan

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
    try :
        con.execute("INSERT INTO games VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (row['GAME_ID'], row['SEASON_ID_HOME'], row['TEAM_ID_HOME'], row['TEAM_ID_AWAY'],
                    row['GAME_DATE_HOME'], row['MATCHUP_HOME'], row['PTS_HOME'], row['PTS_AWAY'],
                    row['MIN_HOME'], row['MIN_AWAY']))
    except duckdb.ConstraintException as e:
        print(f"Error inserting game {row['GAME_ID']}: {e}")
        continue

## Play-by-play data
con.execute("""CREATE TABLE IF NOT EXISTS play_by_play (
            GAME_ID INTEGER REFERENCES games(GAME_ID), 
            EVENTNUM INTEGER, 
            EVENTMSGTYPE INTEGER, 
            EVENTMSGACTIONTYPE INTEGER, 
            PERIOD INTEGER,
            WCTIMESTRING VARCHAR, 
            PCTIMESTRING VARCHAR, 
            HOMEDESCRIPTION VARCHAR, 
            NEUTRALDESCRIPTION VARCHAR,
            VISITORDESCRIPTION VARCHAR, 
            SCORE VARCHAR, 
            SCOREMARGIN INTEGER, 
            PRIMARY KEY (GAME_ID, EVENTNUM))""")
games_data_existing = con.execute("SELECT GAME_ID FROM play_by_play").fetchnumpy()
#games_data_existing = [g[0] for g in games_data_existing]'
games_data_remaining = games_data[~games_data['GAME_ID'].astype('int').isin(games_data_existing['GAME_ID'])]
if games_data_existing['GAME_ID'].size > 0:
    print(f"Games already exist in play-by-play data, fetching remaining {len(games_data_remaining)} games.")
else:
    print("No games found in play-by-play data, fetching all for season.")
for gid in tqdm(games_data_remaining['GAME_ID'].unique()):
    pbp = playbyplay.PlayByPlay(game_id=gid)
    pbp_df = pbp.get_data_frames()[0]
    pbp_df['SCOREMARGIN'] = pbp_df['SCOREMARGIN'].replace('None',nan)
    pbp_df.loc[0, 'SCOREMARGIN'] = 0
    pbp_df.loc[0, 'SCORE'] = '0-0'
    pbp_df.loc[pbp_df['SCOREMARGIN'] == 'TIE','SCOREMARGIN'] = 0
    pbp_df['SCOREMARGIN'] = pbp_df['SCOREMARGIN'].ffill()
    pbp_df['HOMEDESCRIPTION'] = pbp_df['HOMEDESCRIPTION'].replace('"','\"').replace("'","\'")
    pbp_df['VISITORDESCRIPTION'] = pbp_df['VISITORDESCRIPTION'].replace('"','\"').replace("'","\'")

    if not pbp_df.empty:
    #    pbp_df.to_sql('play_by_play', con, if_exists='append', index=False)
        #insert_vals = ""
        #for n,row in pbp_df.iterrows():
            #insert_vals += f"""({row['GAME_ID']}, {row['EVENTNUM']}, {row['EVENTMSGTYPE']}, {row['EVENTMSGACTIONTYPE']},
            #              {row['PERIOD']}, '{row['WCTIMESTRING']}', '{row['PCTIMESTRING']}',
            #              '{row['HOMEDESCRIPTION']}', '{row['NEUTRALDESCRIPTION']}',
            #              '{row['VISITORDESCRIPTION']}', '{row['SCORE']}', {row['SCOREMARGIN']}),"""
        pbp_values = [[row['GAME_ID'], row['EVENTNUM'], row['EVENTMSGTYPE'], row['EVENTMSGACTIONTYPE'],
                          row['PERIOD'], row['WCTIMESTRING'], row['PCTIMESTRING'],
                          row['HOMEDESCRIPTION'], row['NEUTRALDESCRIPTION'],
                          row['VISITORDESCRIPTION'], row['SCORE'], row['SCOREMARGIN']] for n,row in pbp_df.iterrows()]
        try:
            con.executemany("INSERT INTO play_by_play VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,);",pbp_values) 
        except duckdb.ConstraintException as e:
            print(f"Error inserting play-by-play data for game {gid}: {e}")
            continue
# Create indexes for faster querying
con.execute("CREATE INDEX IF NOT EXISTS idx_seasons_id ON seasons (ID)")
con.execute("CREATE INDEX IF NOT EXISTS idx_games_game_id ON games (GAME_ID)")
con.execute("CREATE INDEX IF NOT EXISTS idx_play_by_play_game_id ON play_by_play (GAME_ID)")
con.execute("CREATE INDEX IF NOT EXISTS idx_teams_id ON teams (ID)")

