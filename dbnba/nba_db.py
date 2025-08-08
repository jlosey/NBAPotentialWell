from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.library.parameters import Season
from nba_api.stats.library.parameters import SeasonType
from nba_api.stats.static import teams, players
from nba_api.stats.endpoints import playbyplay

class NBA_Season():
    def __init__(self, season, season_type=SeasonType.regular):
        self.season = season
        self.season_type = season_type
        self.team_ids = [team['id'] for team in teams.get_teams()]
        self.player_ids = [player['id'] for player in players.get_players()]
        self.game_ids = self._get_game_ids()
    def _get_teams(self):
        """Get a list of NBA teams"""
        return teams.get_teams()
    def _get_players(self):
        """Get a list of NBA players"""
        return players.get_players()
    def _get_game_ids(self):
        games = leaguegamefinder.LeagueGameFinder(season_nullable=self.season,
                                                  season_type_nullable=self.season_type)
        return [g['GAME_ID'] for g in games.get_normalized_dict()['LeagueGameFinderResults']]
    
if __name__ == "__main__":

    nba_season = NBA_Season(season=2022)
    print("Teams:", nba_season._get_teams())
    print("Players:", nba_season._get_players())
    print("Game IDs:", nba_season.game_ids)
    
    # Example of getting play-by-play data for a specific game
    #if nba_season.game_ids:
    #    pbp = playbyplay.PlayByPlay(game_id=nba_season.game_ids[0])
    #    print(pbp.get_data_frames()[0].head())