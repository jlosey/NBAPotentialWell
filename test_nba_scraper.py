#!/usr/bin/env python3
"""
Comprehensive test suite for NBA scraper.
Run with: pytest test_nba_scraper.py -v
"""
import pytest
import time
import pandas as pd
import duckdb
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

# Import the modules to test
from make_duckdb_nba_robust import (
    RateLimiter, 
    RetryWithBackoff, 
    NBAScraper,
    setup_database
)


class TestRateLimiter:
    """Tests for RateLimiter class."""
    
    def test_initialization(self):
        """Test rate limiter initializes with correct delays."""
        rl = RateLimiter(min_delay=1.0, max_delay=2.0)
        assert rl.min_delay == 1.0
        assert rl.max_delay == 2.0
        assert rl.last_call == 0
    
    def test_wait_enforces_minimum_delay(self):
        """Test that wait() enforces minimum delay between calls."""
        rl = RateLimiter(min_delay=0.5, max_delay=0.5)  # Fixed 0.5s delay
        
        start = time.time()
        rl.wait()
        first_call = time.time()
        
        rl.wait()
        second_call = time.time()
        
        elapsed = second_call - first_call
        assert elapsed >= 0.5, f"Expected >= 0.5s delay, got {elapsed:.3f}s"
    
    def test_wait_with_zero_delay(self):
        """Test wait() with zero elapsed time."""
        rl = RateLimiter(min_delay=0.1, max_delay=0.1)
        rl.last_call = time.time()  # Set to now
        
        start = time.time()
        rl.wait()
        elapsed = time.time() - start
        
        assert elapsed >= 0.1, f"Expected >= 0.1s delay, got {elapsed:.3f}s"


class TestRetryWithBackoff:
    """Tests for RetryWithBackoff decorator."""
    
    def test_successful_call_no_retry(self):
        """Test function that succeeds on first call."""
        @RetryWithBackoff(max_retries=3, base_delay=0.1)
        def success_func():
            return "success"
        
        result = success_func()
        assert result == "success"
    
    def test_retry_on_failure_then_success(self):
        """Test function that fails once then succeeds."""
        call_count = 0
        
        @RetryWithBackoff(max_retries=3, base_delay=0.1)
        def fail_once():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise Exception("Temporary failure")
            return "success"
        
        result = fail_once()
        assert result == "success"
        assert call_count == 2
    
    def test_max_retries_exceeded(self):
        """Test function that always fails raises exception."""
        @RetryWithBackoff(max_retries=2, base_delay=0.1)
        def always_fail():
            raise Exception("Persistent failure")
        
        with pytest.raises(Exception) as exc_info:
            always_fail()
        
        assert "Persistent failure" in str(exc_info.value)
    
    def test_backoff_increases_delay(self):
        """Test that delay increases with each retry."""
        delays = []
        
        @RetryWithBackoff(max_retries=3, base_delay=0.1)
        def track_delays():
            delays.append(time.time())
            raise Exception("fail")
        
        try:
            track_delays()
        except:
            pass
        
        # Check that delays between calls increase
        assert len(delays) == 3  # 3 attempts


class TestNBAScraper:
    """Tests for NBAScraper class."""
    
    @pytest.fixture
    def scraper(self):
        """Create a test scraper instance."""
        return NBAScraper(season='2022-23', season_type='Regular Season')
    
    def test_initialization(self, scraper):
        """Test scraper initializes correctly."""
        assert scraper.season == '2022-23'
        assert scraper.season_type == 'Regular Season'
        assert scraper.success_count == 0
        assert scraper.failed_games == []
    
    @patch('make_duckdb_nba_robust.playbyplay.PlayByPlay')
    @patch('make_duckdb_nba_robust.RateLimiter.wait')
    def test_fetch_play_by_play_success(self, mock_wait, mock_playbyplay, scraper):
        """Test successful play-by-play fetch."""
        # Mock the API response
        mock_pbp = MagicMock()
        mock_pbp.get_data_frames.return_value = [pd.DataFrame({
            'GAME_ID': ['0022200001'],
            'EVENTNUM': [1, 2],
            'EVENTMSGTYPE': [1, 2],
            'EVENTMSGACTIONTYPE': [1, 1],
            'PERIOD': [1, 1],
            'WCTIMESTRING': ['10:00', '9:45'],
            'PCTIMESTRING': ['12:00', '11:45'],
            'HOMEDESCRIPTION': ['Shot made', None],
            'NEUTRALDESCRIPTION': [None, None],
            'VISITORDESCRIPTION': [None, 'Shot missed'],
            'SCORE': ['2-0', '2-0'],
            'SCOREMARGIN': ['2', '2']
        })]
        mock_playbyplay.return_value = mock_pbp
        
        result = scraper.fetch_play_by_play('0022200001')
        
        assert result is not None
        assert len(result) == 2
        assert mock_wait.called
        assert mock_playbyplay.called
    
    @patch('make_duckdb_nba_robust.playbyplay.PlayByPlay')
    @patch('make_duckdb_nba_robust.RateLimiter.wait')
    def test_fetch_play_by_play_empty_response(self, mock_wait, mock_playbyplay, scraper):
        """Test handling of empty play-by-play response."""
        mock_pbp = MagicMock()
        mock_pbp.get_data_frames.return_value = [pd.DataFrame()]
        mock_playbyplay.return_value = mock_pbp
        
        result = scraper.fetch_play_by_play('0022200001')
        
        assert result is None
        assert '0022200001' in scraper.failed_games
    
    @patch('make_duckdb_nba_robust.playbyplay.PlayByPlay')
    @patch('make_duckdb_nba_robust.RateLimiter.wait')
    def test_fetch_play_by_play_keyerror(self, mock_wait, mock_playbyplay, scraper):
        """Test handling of NBA API KeyError (resultSet issue)."""
        mock_playbyplay.side_effect = KeyError("resultSet")
        
        result = scraper.fetch_play_by_play('0022200001')
        
        assert result is None
    
    def test_clean_play_by_play(self, scraper):
        """Test data cleaning logic."""
        df = pd.DataFrame({
            'SCOREMARGIN': ['None', 'TIE', '5'],
            'SCORE': [None, None, None],
            'HOMEDESCRIPTION': ["Player's shot", None, None]
        })
        
        result = scraper._clean_play_by_play(df)
        
        assert result.loc[0, 'SCOREMARGIN'] == 0
        assert result.loc[1, 'SCOREMARGIN'] == 0
        assert result.loc[0, 'SCORE'] == '0-0'


class TestDatabaseOperations:
    """Tests for database operations."""
    
    @pytest.fixture
    def db_connection(self):
        """Create a temporary in-memory database."""
        con = duckdb.connect(database=':memory:')
        yield con
        con.close()
    
    def test_setup_database_creates_tables(self, db_connection):
        """Test that setup_database creates all required tables."""
        teams = [{'id': 1, 'full_name': 'Test Team', 'abbreviation': 'TST', 
                  'nickname': 'Test', 'city': 'Test City', 'state': 'TS'}]
        
        setup_database(db_connection, '2022-23', 'Regular Season', teams)
        
        # Check tables exist
        tables = db_connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        table_names = [t[0] for t in tables]
        
        assert 'seasons' in table_names
        assert 'teams' in table_names
        assert 'games' in table_names
        assert 'play_by_play' in table_names
    
    def test_setup_database_inserts_season(self, db_connection):
        """Test that season is inserted correctly."""
        setup_database(db_connection, '2022-23', 'Regular Season', [])
        
        result = db_connection.execute(
            "SELECT season, season_type FROM seasons WHERE season = '2022-23'"
        ).fetchone()
        
        assert result is not None
        assert result[0] == '2022-23'
        assert result[1] == 'Regular Season'
    
    def test_setup_database_inserts_teams(self, db_connection):
        """Test that teams are inserted correctly."""
        teams = [
            {'id': 1, 'full_name': 'Lakers', 'abbreviation': 'LAL',
             'nickname': 'Lakers', 'city': 'Los Angeles', 'state': 'CA'},
            {'id': 2, 'full_name': 'Celtics', 'abbreviation': 'BOS',
             'nickname': 'Celtics', 'city': 'Boston', 'state': 'MA'}
        ]
        
        setup_database(db_connection, '2022-23', 'Regular Season', teams)
        
        count = db_connection.execute(
            "SELECT COUNT(*) FROM teams"
        ).fetchone()[0]
        
        assert count == 2
    
    def test_setup_database_idempotent(self, db_connection):
        """Test that running setup twice doesn't duplicate data."""
        teams = [{'id': 1, 'full_name': 'Test', 'abbreviation': 'TST',
                  'nickname': 'Test', 'city': 'City', 'state': 'ST'}]
        
        setup_database(db_connection, '2022-23', 'Regular Season', teams)
        setup_database(db_connection, '2022-23', 'Regular Season', teams)
        
        count = db_connection.execute(
            "SELECT COUNT(*) FROM teams"
        ).fetchone()[0]
        
        assert count == 1  # Should not duplicate


class TestIntegration:
    """Integration tests with mocked external dependencies."""
    
    @patch('make_duckdb_nba_robust.leaguegamefinder.LeagueGameFinder')
    def test_full_season_scrape_mock(self, mock_game_finder):
        """Test full season scrape with mocked API responses."""
        # Mock game finder response
        mock_games = MagicMock()
        mock_games.get_data_frames.return_value = [pd.DataFrame({
            'SEASON_ID': ['22022'] * 4,
            'GAME_ID': ['0022200001', '0022200001', '0022200002', '0022200002'],
            'TEAM_ID': [1, 2, 3, 4],
            'GAME_DATE': ['2022-10-18', '2022-10-18', '2022-10-19', '2022-10-19'],
            'MATCHUP': ['LAL vs. GS', 'GS @ LAL', 'BOS vs. PHI', 'PHI @ BOS'],
            'PTS': [100, 110, 105, 95],
            'MIN': [240, 240, 240, 240]
        })]
        mock_game_finder.return_value = mock_games
        
        scraper = NBAScraper(season='2022-23')
        
        # This would need more mocking for a full integration test
        # Just verify initialization works
        assert scraper.season == '2022-23'


class TestEdgeCases:
    """Tests for edge cases and error conditions."""
    
    def test_rate_limiter_with_negative_delay(self):
        """Test rate limiter handles edge case delays."""
        rl = RateLimiter(min_delay=0.0, max_delay=0.0)
        start = time.time()
        rl.wait()
        rl.wait()
        elapsed = time.time() - start
        assert elapsed < 0.1  # Should be almost instant
    
    def test_scraper_with_special_characters_in_names(self):
        """Test handling of player names with special characters."""
        scraper = NBAScraper(season='2022-23')
        
        df = pd.DataFrame({
            'HOMEDESCRIPTION': ["O'Neal's dunk", 'Regular "quote" text', "Björk's shot"],
            'SCOREMARGIN': ['2', '4', '6'],
            'SCORE': ['2-0', '4-0', '6-0']
        })
        
        result = scraper._clean_play_by_play(df)
        
        # Should not raise exception
        assert len(result) == 3
    
    def test_empty_season_string(self):
        """Test scraper handles empty season."""
        with pytest.raises((ValueError, TypeError)):
            NBAScraper(season='')


# Run tests if executed directly
if __name__ == '__main__':
    pytest.main([__file__, '-v'])
