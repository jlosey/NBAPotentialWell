import panel as pn
import numpy as np
import matplotlib.pyplot as plt
#from matplotlib.figure import Figure
from nbapotentialwell import NBAPotentialWell,NBAGameProcessing,nba_teams

ACCENT = "goldenrod"
LOGO = "https://assets.holoviz.org/panel/tutorials/matplotlib-logo.png"

pn.extension(sizing_mode="stretch_width")

npw = NBAPotentialWell('Chicago Bulls','2022-23')
games = npw._get_game_ids()
g = NBAGameProcessing(games[81])
g.create_transition_matrix(lag=20)
fig = plt.figure(figsize=(6,4))
ax = g.plot_score_margin(fig=fig)

select = pn.widgets.Select(name='Teams',options=nba_teams['FULL_NAME'].to_list())
ddown = pn.Row(select.controls(jslink=True),select)
component = pn.pane.Matplotlib(fig, format='svg', sizing_mode='scale_both')

pn.template.FastListTemplate(
    title="My App", sidebar=[LOGO], main=[ddown,component], accent=ACCENT
).servable()