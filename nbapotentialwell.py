#Classes and methods to pull NBA play-by-play data
#conver it to a Markov transition matrix
#and estimate potential energy wells
import numpy as np
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
        self.game_ids = self.get_game_ids()
    
    def get_game_ids(self):
        games = leaguegamefinder.LeagueGameFinder(team_id_nullable=self.team_id,
                            season_nullable=self.season,
                            season_type_nullable=SeasonType.regular)
        return [g ['GAME_ID'] for g in games.get_normalized_dict()['LeagueGameFinderResults']]
        
    def get_play_by_play(self,game_id):
        """Loop through all plays in a game and return a list of plays"""
        #play_d = {}
        pbp = playbyplay.PlayByPlay(game_id=game_id)
        plays = pbp.get_normalized_dict()
        #play_data = plays.get_normalized_dict()['LeagueGameFinderResults']
        #play_d[g_id] = plays['SCORE']
        return [p[['TIME','SCOREMARGIN']] for p in plays if p['EVENTMSGACTIONTYPE'] in ['1','2','3']]
            
if __name__ == "__main__":
    npw = NBAPotentialWell('Chicago Bulls','2023-24')
   
    print(npw.get_play_by_play('2042000211'))