#!/usr/bin/env python3
import requests
import re
from datetime import datetime
from bs4 import BeautifulSoup

url = 'https://www.basketball-reference.com/leagues/NBA_2023_games-october.html'
headers = {'User-Agent': 'Mozilla/5.0'}

print(f"Fetching {url}...")
response = requests.get(url, headers=headers, timeout=30)
print(f"Status: {response.status_code}")

soup = BeautifulSoup(response.content, 'html.parser')
table = soup.find('table', {'id': 'schedule'})

games = []
current_date = None

for row in table.find_all('tr'):
    # Check for date header row
    th = row.find('th')
    if th:
        th_class = th.get('class', [])
        th_text = th.get_text(strip=True)
        print(f"TH: class={th_class}, text={th_text[:40]}")
        
        if 'left' in th_class:
            print(f"  -> Found date header!")
            try:
                current_date = datetime.strptime(th_text, "%a, %b %d, %Y")
                print(f"  -> Date set to: {current_date}")
            except Exception as e:
                print(f"  -> Parse error: {e}")
            continue
    
    # Parse game row
    cells = row.find_all('td')
    if len(cells) >= 6:
        print(f"Game row: {len(cells)} cells, current_date={current_date}")
        visitor = cells[1].get_text(strip=True)
        home = cells[3].get_text(strip=True)
        print(f"  Teams: {visitor} @ {home}")
        
        # Find boxscore link
        boxscore_link = None
        for i, cell in enumerate(cells):
            link = cell.find('a', href=re.compile(r'/boxscores/\d{9}'))
            if link:
                boxscore_link = link
                print(f"  Found boxscore link in cell {i}: {link['href']}")
                break
        
        if boxscore_link and current_date:
            game_id = boxscore_link['href'].split('/')[-1].replace('.html', '')
            print(f"  -> ADDING GAME: {game_id}")
            games.append({
                'game_id': game_id, 
                'game_date': current_date.strftime('%Y-%m-%d'), 
                'home_team': home, 
                'visitor_team': visitor
            })
        elif not boxscore_link:
            print(f"  -> No boxscore link!")
        elif not current_date:
            print(f"  -> No current date!")
    
    if len(games) >= 3:
        print("\nStopping after 3 games for debug...")
        break

print(f"\nTotal games: {len(games)}")
for g in games:
    print(f"  {g}")
