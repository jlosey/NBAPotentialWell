import panel as pn
import numpy as np
import matplotlib.pyplot as plt
#from matplotlib.figure import Figure
from nbapotentialwell import NBAPotentialWell,NBAGameProcessing,nba_teams

ACCENT = "goldenrod"
LOGO = "https://assets.holoviz.org/panel/tutorials/matplotlib-logo.png"

pn.extension(sizing_mode="stretch_width")




select_team = pn.widgets.Select(name='Teams',options=nba_teams['FULL_NAME'].to_list(),value=nba_teams['FULL_NAME'].to_list()[1])
def get_games(team_id):
    npw = NBAPotentialWell(team_id,'2022-23')
    games = npw._get_game_ids()
    return npw,games
npw,games = pn.bind(get_games,select_team,watch=True)
select_game = pn.widgets.Select(name='Games',options=list(games.keys()),value=list(games.keys())[0])
cl = pn.Column(select_team,select_game)
print('xxx',select_game.value)
g = NBAGameProcessing(games[select_game.value])
g.create_transition_matrix(lag=20)
fig = plt.figure(figsize=(6,4))
ax = g.plot_score_margin(fig=fig)
component = pn.pane.Matplotlib(fig, format='svg', sizing_mode='scale_both')

pn.template.FastListTemplate(
    title="My App", sidebar=[LOGO,cl], main=[component], accent=ACCENT
).servable()