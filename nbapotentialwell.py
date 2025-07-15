#Classes and methods to pull NBA play-by-play data
#conver it to a Markov transition matrix
#and estimate potential energy wells
import numpy as np
import pandas as pd
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.library.parameters import Season
from nba_api.stats.library.parameters import SeasonType
from nba_api.stats.static import teams
from nba_api.stats.endpoints import playbyplay

nba_teams = teams.get_teams()

class NBAPotentialWell:
    def __init__(self,team_name,season):
        self.team_name = team_name
        self.team_id = [t['id'] for t in nba_teams if t['full_name'] == ('')]
        self.season = season
        self.game_ids = self._get_game_ids()
        self.pbp = self._get_play_by_play()
    
    def _get_game_ids(self):
        games = leaguegamefinder.LeagueGameFinder(team_id_nullable=self.team_id,
                            season_nullable=self.season,
                            season_type_nullable=SeasonType.regular)
        return [g ['GAME_ID'] for g in games.get_normalized_dict()['LeagueGameFinderResults']]
        
    def _get_play_by_play(self,game_id,format=True):
        """Loop through all plays in a game and return a list of plays"""
        #play_d = {}
        pbp = playbyplay.PlayByPlay(game_id=game_id)
        plays = pbp.get_data_frames()[0]
        score_id = plays['EVENTMSGTYPE'].isin([1,3]) # 1 for field goals, 3 for free throws
        #play_data = plays.get_normalized_dict()['LeagueGameFinderResults']
        #play_d[g_id] = plays['SCORE']
        return plays.loc[score_id, ['PERIOD', 'PCTIMESTRING', 'SCORE']].reset_index(drop=True)
    
    def _format_time(self):
        """Convert a time string in the format 'MM:SS' to seconds accounting for the period"""
        df = self.pbp.copy()
        df['TIME_ELAPSED'] = pd.to_timedelta('00:' + (df['PERIOD']*12).astype('str') + ':00') \
            - pd.to_timedelta('00:' + df['PCTIMESTRING'])
        df.loc[:,'TIME_ELAPSED'] = pd.to_timedelta('00:' + (df.loc[:,'PERIOD']*12).astype('str') + ':00')\
            - pd.to_timedelta('00:' + df.loc[:,'PCTIMESTRING']) 
        df.loc[:,'TIME_S'] = df.loc[:,'TIME_ELAPSED'].dt.total_seconds()
        return df

if __name__ == "__main__":
    npw = NBAPotentialWell('Chicago Bulls','2023-24')
   
    print(npw._get_play_by_play('2042000211')._format_time())