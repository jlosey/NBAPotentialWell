#!/usr/bin/env python3
"""
NBA data scraper using Basketball-Reference with NORMALIZED tables.
"""
import sys
import time
import random
import logging
import re
import requests
from datetime import datetime
from typing import Optional, List, Dict
from bs4 import BeautifulSoup
import duckdb
import pandas as pd
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class RateLimiter:
    def __init__(self, min_delay: float = 3.0, max_delay: float = 5.0):
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.last_call = 0
    
    def wait(self):
        elapsed = time.time() - self.last_call
        delay = random.uniform(self.min_delay, self.max_delay)
        if elapsed < delay:
            time.sleep(delay - elapsed)
        self.last_call = time.time()


class BasketballReferenceScraper:
    BASE_URL = "https://www.basketball-reference.com"
    
    def __init__(self, season: str):
        self.season = season
        self.season_end = int(season.split('-')[0]) + 1
        self.rate_limiter = RateLimiter(min_delay=3.0, max_delay=5.0)
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
        self.failed_games: List[str] = []
        self.success_count = 0
    
    def _get(self, url: str):
        self.rate_limiter.wait()
        try:
            response = self.session.get(url, timeout=30)
            if "Rate Limit Exceeded" in response.text:
                logger.warning("Rate limited, waiting 5 minutes...")
                time.sleep(300)
                return None
            return BeautifulSoup(response.content, 'html.parser')
        except Exception as e:
            logger.error(f"Request failed: {e}")
            return None
    
    def fetch_game_list(self) -> pd.DataFrame:
        logger.info(f"Fetching games for {self.season}...")
        games = []
        months = ['october', 'november', 'december', 'january', 'february', 'march', 'april']
        
        for month in months:
            url = f"{self.BASE_URL}/leagues/NBA_{self.season_end}_games-{month}.html"
            logger.info(f"  Fetching {month}...")
            
            soup = self._get(url)
            if not soup:
                continue
            
            table = soup.find('table', {'id': 'schedule'})
            if not table:
                continue
            
            current_date = None
            for row in table.find_all('tr'):
                th = row.find('th')
                if th and 'left' in th.get('class', []):
                    date_text = th.get_text(strip=True)
                    try:
                        current_date = datetime.strptime(date_text, "%a, %b %d, %Y")
                    except:
                        continue
                
                cells = row.find_all('td')
                if len(cells) < 6:
                    continue
                
                try:
                    visitor = cells[1].get_text(strip=True)
                    home = cells[3].get_text(strip=True)
                    
                    boxscore_link = None
                    for cell in cells:
                        link = cell.find('a', href=re.compile(r'/boxscores/\d{9}'))
                        if link:
                            boxscore_link = link
                            break
                    
                    if boxscore_link and current_date:
                        game_id = boxscore_link['href'].split('/')[-1].replace('.html', '')
                        games.append({
                            'game_id': game_id, 
                            'game_date': current_date.strftime('%Y-%m-%d'),
                            'season': self.season,
                            'home_team': home, 
                            'visitor_team': visitor
                        })
                except:
                    continue
        
        df = pd.DataFrame(games)
        logger.info(f"Found {len(df)} games")
        return df
    
    def fetch_play_by_play(self, game_id: str):
        url = f"{self.BASE_URL}/boxscores/pbp/{game_id}.html"
        soup = self._get(url)
        if not soup:
            return None
        
        table = soup.find('table', {'id': 'pbp'})
        if not table:
            return None
        
        events = []
        period = 1
        
        for row in table.find_all('tr'):
            # Check for period header (TH element with '1st Q', '2nd Q', etc.)
            th = row.find('th')
            if th and len(row.find_all('td')) == 0:
                th_text = th.get_text(strip=True)
                if '1st' in th_text:
                    period = 1
                    logger.debug(f"Period 1 detected")
                elif '2nd' in th_text:
                    period = 2
                    logger.debug(f"Period 2 detected")
                elif '3rd' in th_text:
                    period = 3
                    logger.debug(f"Period 3 detected")
                elif '4th' in th_text:
                    period = 4
                    logger.debug(f"Period 4 detected")
                elif 'OT' in th_text or 'overtime' in th_text.lower():
                    period = 5
                    logger.debug(f"OT (Period 5) detected")
                continue
            
            cells = row.find_all('td')
            num_cells = len(cells)
            
            if num_cells < 6:
                continue
            
            time_text = cells[0].get_text(strip=True)
            
            if ':' not in time_text:
                continue
            
            # Correct column mapping based on debug
            away_desc = cells[1].get_text(strip=True) if num_cells > 1 else None
            away_pts_change = cells[2].get_text(strip=True) if num_cells > 2 else None
            score_text = cells[3].get_text(strip=True) if num_cells > 3 else None
            home_pts_change = cells[4].get_text(strip=True) if num_cells > 4 else None
            home_desc = cells[5].get_text(strip=True) if num_cells > 5 else None
            
            if not away_desc and not home_desc:
                continue
            
            events.append({
                'GAME_ID': game_id, 
                'EVENTNUM': len(events) + 1, 
                'PERIOD': period, 
                'PCTIMESTRING': time_text, 
                'SCORE': score_text if score_text and '-' in score_text else None,
                'AWAY_PTS_CHANGE': away_pts_change if away_pts_change else None,
                'HOME_PTS_CHANGE': home_pts_change if home_pts_change else None,
                'HOMEDESCRIPTION': home_desc if home_desc else None, 
                'VISITORDESCRIPTION': away_desc if away_desc else None
            })
        
        return pd.DataFrame(events) if events else None


def setup_normalized_database(con: duckdb.DuckDBPyConnection, season: str):
    """Setup normalized database schema (star schema)."""
    logger.info("Setting up normalized database schema...")
    
    # Dimension: Seasons
    con.execute("""
        CREATE TABLE IF NOT EXISTS dim_seasons (
            season_id INTEGER PRIMARY KEY,
            season_label VARCHAR,
            season_type VARCHAR
        )
    """)
    
    season_id = int('2' + season[:4])
    con.execute("""
        INSERT OR IGNORE INTO dim_seasons VALUES (?, ?, ?)
    """, (season_id, season, 'Regular Season'))
    
    # Dimension: Teams
    con.execute("""
        CREATE TABLE IF NOT EXISTS dim_teams (
            team_id INTEGER PRIMARY KEY,
            team_name VARCHAR UNIQUE,
            team_city VARCHAR,
            team_abbr VARCHAR
        )
    """)
    
    # Dimension: Games
    con.execute("""
        CREATE TABLE IF NOT EXISTS dim_games (
            game_id VARCHAR PRIMARY KEY,
            season_id INTEGER REFERENCES dim_seasons(season_id),
            game_date DATE,
            home_team_id INTEGER REFERENCES dim_teams(team_id),
            away_team_id INTEGER REFERENCES dim_teams(team_id),
            home_team_name VARCHAR,
            away_team_name VARCHAR
        )
    """)
    
    # Fact: Play by Play
    con.execute("""
        CREATE TABLE IF NOT EXISTS fact_play_by_play (
            GAME_ID VARCHAR REFERENCES dim_games(game_id),
            EVENTNUM INTEGER,
            PERIOD INTEGER,
            PCTIMESTRING VARCHAR,
            SCORE VARCHAR,
            AWAY_PTS_CHANGE VARCHAR,
            HOME_PTS_CHANGE VARCHAR,
            HOMEDESCRIPTION VARCHAR,
            VISITORDESCRIPTION VARCHAR,
            PRIMARY KEY (GAME_ID, EVENTNUM)
        )
    """)
    
    # Indexes
    con.execute("CREATE INDEX IF NOT EXISTS idx_games_season ON dim_games (season_id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_games_date ON dim_games (game_date)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_pbp_game ON fact_play_by_play (GAME_ID)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_pbp_period ON fact_play_by_play (GAME_ID, PERIOD)")
    
    logger.info("Database setup complete")
    return season_id


def get_or_create_team(con, team_name: str) -> int:
    """Get team ID or create if not exists."""
    result = con.execute("SELECT team_id FROM dim_teams WHERE team_name = ?", [team_name]).fetchone()
    if result:
        return result[0]
    
    # Generate ID and insert
    max_id = con.execute("SELECT COALESCE(MAX(team_id), 0) FROM dim_teams").fetchone()[0]
    new_id = max_id + 1
    con.execute("INSERT INTO dim_teams (team_id, team_name) VALUES (?, ?)", [new_id, team_name])
    return new_id


def main():
    season = sys.argv[1] if len(sys.argv) > 1 else '2022-23'
    con = duckdb.connect(database='nba_bbr_normalized.db', read_only=False)
    
    try:
        # Setup database
        season_id = setup_normalized_database(con, season)
        
        # Initialize scraper
        scraper = BasketballReferenceScraper(season=season)
        games_df = scraper.fetch_game_list()
        
        if games_df.empty:
            logger.error("No games found")
            sys.exit(1)
        
        # Insert teams and games
        logger.info("Inserting teams and games...")
        for _, row in games_df.iterrows():
            # Get or create teams
            home_team_id = get_or_create_team(con, row['home_team'])
            away_team_id = get_or_create_team(con, row['visitor_team'])
            
            # Insert game
            try:
                con.execute("""
                    INSERT OR IGNORE INTO dim_games 
                    (game_id, season_id, game_date, home_team_id, away_team_id, home_team_name, away_team_name)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (row['game_id'], season_id, row['game_date'], home_team_id, away_team_id, 
                      row['home_team'], row['visitor_team']))
            except Exception as e:
                logger.warning(f"Error inserting game {row['game_id']}: {e}")
        
        # Process play-by-play
        logger.info(f"Fetching play-by-play for {len(games_df)} games...")
        for idx, (_, row) in enumerate(tqdm(games_df.iterrows(), total=len(games_df)), 1):
            game_id = row['game_id']
            
            existing = con.execute("SELECT COUNT(*) FROM fact_play_by_play WHERE GAME_ID = ?", [game_id]).fetchone()[0]
            if existing > 0:
                continue
            
            pbp_df = scraper.fetch_play_by_play(game_id)
            if pbp_df is not None and not pbp_df.empty:
                try:
                    con.executemany("""
                        INSERT INTO fact_play_by_play 
                        (GAME_ID, EVENTNUM, PERIOD, PCTIMESTRING, SCORE, AWAY_PTS_CHANGE, HOME_PTS_CHANGE, HOMEDESCRIPTION, VISITORDESCRIPTION)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, pbp_df[['GAME_ID', 'EVENTNUM', 'PERIOD', 'PCTIMESTRING', 'SCORE', 
                                  'AWAY_PTS_CHANGE', 'HOME_PTS_CHANGE', 'HOMEDESCRIPTION', 'VISITORDESCRIPTION']].values.tolist())
                    scraper.success_count += 1
                except Exception as e:
                    logger.error(f"Error inserting PBP for {game_id}: {e}")
                    scraper.failed_games.append(game_id)
            else:
                scraper.failed_games.append(game_id)
        
        logger.info("=" * 60)
        logger.info("SCRAPING COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Games: {len(games_df)}")
        logger.info(f"Play-by-play: {scraper.success_count} successful")
        if scraper.failed_games:
            logger.warning(f"Failed: {len(scraper.failed_games)} games")
        
        # Show sample query
        logger.info("\nSample data:")
        sample = con.execute("""
            SELECT g.game_date, t1.team_name as home, t2.team_name as away, 
                   f.period, f.pctimestring, f.score, f.homedescription
            FROM fact_play_by_play f
            JOIN dim_games g ON f.game_id = g.game_id
            JOIN dim_teams t1 ON g.home_team_id = t1.team_id
            JOIN dim_teams t2 ON g.away_team_id = t2.team_id
            WHERE f.homedescription IS NOT NULL
            LIMIT 5
        """).fetchall()
        for row in sample:
            logger.info(f"  {row}")
        
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        con.close()
        logger.info("Database connection closed")


if __name__ == "__main__":
    main()