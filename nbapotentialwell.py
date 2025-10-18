#Classes and methods to pull NBA play-by-play data
#conver it to a Markov transition matrix
#and estimate potential energy wells
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.library.parameters import Season
from nba_api.stats.library.parameters import SeasonType
from nba_api.stats.static import teams
from nba_api.stats.endpoints import playbyplay
import duckdb
con = duckdb.connect(database='nba.db', read_only=False)
nba_teams = con.execute("SELECT * FROM teams;").df()

class NBAPotentialWell:
    def __init__(self,team_name,season):
        assert nba_teams['FULL_NAME'].str.fullmatch(team_name).any(), "Team not found in database"
        self.team_name = team_name
        self.team_id = nba_teams.loc[nba_teams['FULL_NAME'] == team_name, 'TEAM_ID'].values[0]
        # Consider and assertion for season here
        self.season = season
        self.game_ids = self._get_game_ids()
    
    def _get_game_ids(self):
        games = leaguegamefinder.LeagueGameFinder(team_id_nullable=self.team_id,
                            season_nullable=self.season,
                            season_type_nullable=SeasonType.regular)        
        return {g['GAME_DATE'] + " - " + g['MATCHUP']:g['GAME_ID']
                for g in games.get_normalized_dict()['LeagueGameFinderResults']}
        
    def _get_game_str(self):
        games = leaguegamefinder.LeagueGameFinder(team_id_nullable=self.team_id,
                            season_nullable=self.season,
                            season_type_nullable=SeasonType.regular)        
        return [g['GAME_DATE'] + " - " + g['MATCHUP'] for g in games.get_normalized_dict()['LeagueGameFinderResults']]  
        
    def _format_time(self):
        """Convert a time string in the format 'MM:SS' to seconds accounting for the period"""
        df = self.pbp.copy()
        df['TIME_ELAPSED'] = pd.to_timedelta('00:' + (df['PERIOD']*12).astype('str') + ':00') \
            - pd.to_timedelta('00:' + df['PCTIMESTRING'])
        df.loc[:,'TIME_ELAPSED'] = pd.to_timedelta('00:' + (df.loc[:,'PERIOD']*12).astype('str') + ':00')\
            - pd.to_timedelta('00:' + df.loc[:,'PCTIMESTRING']) 
        df.loc[:,'TIME_S'] = df.loc[:,'TIME_ELAPSED'].dt.total_seconds()
        return df

class NBAGameProcessing():
    def __init__(self, game_id,max_differential=30,lag=30):
        self.game_id = int(game_id)
        self.pbp = self._get_play_by_play()
        self.max_differential = max_differential
        self.bins = np.arange(-max_differential, max_differential + 1, 1)
        self.mat = np.zeros((len(self.bins)-1, len(self.bins)-1))
        
    
    def _get_play_by_play(self,format=True):
        """Loop through all plays in a game and return a list of plays"""
        pbp = con.execute("SELECT PERIOD, PCTIMESTRING, SCORE, SCOREMARGIN, EVENTMSGTYPE FROM play_by_play WHERE GAME_ID = ?"
                          , [self.game_id]).df()
        score_id = pbp['EVENTMSGTYPE'].isin([1,3]) # 1 for field goals, 3 for free throws
        return _format_time(pbp.loc[score_id, ['PERIOD', 'PCTIMESTRING', 'SCORE', 'SCOREMARGIN']].reset_index(drop=True))   

    
    def create_transition_matrix(self,lag=30):
        """Create a transition matrix from the play-by-play data"""
        # This method would implement the logic to create a transition matrix
        # based on the play-by-play data.
        df = self.pbp.copy()
        lag_int = int(lag/0.1)  # Convert lag from seconds to number of rows
        margin = df['SCOREMARGIN'].astype(int).values[0:-lag_int] + self.max_differential
        margin_shift = df['SCOREMARGIN'].astype(int).values[lag_int:] + self.max_differential
        np.add.at(self.mat,(margin,margin_shift),1)

        
    def plot_score_margin(self,fig=None):
        """Plot the score margin over time"""
        # This method would implement the logic to plot the score margin.
        # For example, using matplotlib to visualize the score margin over time.
        if not fig:
            fig = plt.figure(figsize=(6,4))
        ax = fig.subplots()
        ax.plot(self.pbp.index, self.pbp['SCOREMARGIN'])
        ax.set_xlabel('Time (seconds)')
        ax.set_ylabel('Score Margin')
        ax.set_ylim(-30,30)
        ax.set_title('Score Margin Over Time')
        #plt.show()
        return ax
        
    
    def plot_transition_matrix(self):
        """Plot the transition matrix"""
        # This method would implement the logic to plot the transition matrix.
        # For example, using matplotlib or seaborn to visualize the matrix.
        fig = plt.figure()
        ax = fig.add_subplot(111)
        m = ax.pcolormesh(self.mat, cmap='hot_r')
        cbar = plt.colorbar(m)
        ticks = ax.get_xticks()
        ax.set_xticks(ticks)
        ax.set_yticks(ticks)
        ax.set_xticklabels([int(t-self.max_differential) for t in ticks])
        ax.set_yticklabels([int(t-self.max_differential) for t in ticks])
        plt.show()

def _format_time(df):
    """Convert a time string in the format 'MM:SS' to seconds accounting for the period"""
    #df = self.pbp.copy()
    df['TIME_ELAPSED'] = pd.to_timedelta('00:' + (df['PERIOD']*12).astype('str') + ':00') \
        - pd.to_timedelta('00:' + df['PCTIMESTRING'])
    df.loc[:,'TIME_ELAPSED'] = pd.to_timedelta('00:' + (df.loc[:,'PERIOD']*12).astype('str') + ':00')\
        - pd.to_timedelta('00:' + df.loc[:,'PCTIMESTRING']) 
    df.loc[:,'TIME_S'] = df.loc[:,'TIME_ELAPSED'].dt.total_seconds()
    df_full_time = pd.DataFrame(data=np.arange(0,2880,0.1),columns=['TIME_S'])
    df_full_time = df_full_time.set_index('TIME_S').join(df[['TIME_S','SCORE','SCOREMARGIN']].set_index('TIME_S'),how='left',lsuffix='_FULL',rsuffix='')
    df_full_time.loc[0,['SCORE']] = '0 - 0'
    df_full_time.loc[0,['SCOREMARGIN']] = 0
    df_full_time = df_full_time.ffill()
    df_full_time['SCORE_HOME'] = df_full_time['SCORE'].str.split(" - ").str[1].astype(int)
    df_full_time['SCORE_AWAY'] = df_full_time['SCORE'].str.split(" - ").str[0].astype(int)
    return df_full_time

if __name__ == "__main__":
    npw = NBAPotentialWell('Chicago Bulls','2022-23')
    games = npw._get_game_ids()
    g = NBAGameProcessing(games[81])
    g.create_transition_matrix(lag=20)
    g.plot_score_margin()
    g.plot_transition_matrix()
    #print(np.sum(g.mat,axis=0))