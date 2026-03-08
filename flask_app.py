#!/usr/bin/env python3
"""
NBA Game Viewer Web Application
Simple Flask app to display games by team and season
"""
from flask import Flask, render_template, jsonify, request, send_file
import duckdb
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import io
import base64
import logging
import numpy as np

from nbapotentialwell import NBAGameProcessing

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Database path (using normalized database)
DB_PATH = 'nba_bbr_normalized.db'


def get_db_connection():
    """Create a database connection."""
    return duckdb.connect(DB_PATH, read_only=True)


@app.route('/')
def index():
    """Main page with dropdowns."""
    return render_template('index.html')


@app.route('/api/seasons')
def get_seasons():
    """Get list of available seasons."""
    conn = get_db_connection()
    try:
        seasons = conn.execute("""
            SELECT DISTINCT season_label FROM dim_seasons ORDER BY season_label DESC
        """).fetchall()
        return jsonify([s[0] for s in seasons])
    finally:
        conn.close()


@app.route('/api/teams')
def get_teams():
    """Get list of all teams."""
    conn = get_db_connection()
    try:
        teams = conn.execute("""
            SELECT team_name FROM dim_teams ORDER BY team_name
        """).fetchall()
        return jsonify([t[0] for t in teams])
    finally:
        conn.close()


@app.route('/api/games')
def get_games():
    """Get games filtered by season and/or team."""
    season = request.args.get('season', '')
    team = request.args.get('team', '')
    
    conn = get_db_connection()
    try:
        query = """
            SELECT 
                g.game_id,
                g.game_date,
                s.season_label,
                t1.team_name as home_team,
                t2.team_name as away_team,
                g.home_team_name || ' vs ' || g.away_team_name as matchup
            FROM dim_games g
            JOIN dim_seasons s ON g.season_id = s.season_id
            JOIN dim_teams t1 ON g.home_team_id = t1.team_id
            JOIN dim_teams t2 ON g.away_team_id = t2.team_id
            WHERE 1=1
        """
        params = []
        
        if season:
            query += " AND s.season_label = ?"
            params.append(season)
        
        if team:
            query += " AND (t1.team_name = ? OR t2.team_name = ?)"
            params.extend([team, team])
        
        query += " ORDER BY g.game_date DESC"
        
        games = conn.execute(query, params).fetchall()
        
        result = []
        for game in games:
            result.append({
                'game_id': game[0],
                'game_date': game[1],
                'season': game[2],
                'home_team': game[3],
                'away_team': game[4],
                'matchup': game[5]
            })
        
        return jsonify(result)
    finally:
        conn.close()


@app.route('/api/game/<game_id>')
def get_game_details(game_id):
    """Get play-by-play details for a specific game."""
    conn = get_db_connection()
    try:
        # Get game info
        game = conn.execute("""
            SELECT g.game_id, g.game_date, s.season_label, t1.team_name, t2.team_name
            FROM dim_games g
            JOIN dim_seasons s ON g.season_id = s.season_id
            JOIN dim_teams t1 ON g.home_team_id = t1.team_id
            JOIN dim_teams t2 ON g.away_team_id = t2.team_id
            WHERE g.game_id = ?
        """, [game_id]).fetchone()
        
        if not game:
            return jsonify({'error': 'Game not found'}), 404
        
        # Get play-by-play
        pbp = conn.execute("""
            SELECT 
                eventnum,
                period,
                pctimestring,
                score,
                homedescription,
                visitordescription
            FROM fact_play_by_play
            WHERE game_id = ?
            ORDER BY eventnum
        """, [game_id]).fetchall()
        
        return jsonify({
            'game': {
                'game_id': game[0],
                'game_date': game[1],
                'season': game[2],
                'home_team': game[3],
                'away_team': game[4]
            },
            'play_by_play': [
                {
                    'eventnum': p[0],
                    'period': p[1],
                    'time': p[2],
                    'score': p[3],
                    'home_description': p[4],
                    'away_description': p[5]
                }
                for p in pbp
            ]
        })
    finally:
        conn.close()


@app.route('/api/game/<game_id>/plot')
def get_game_plot(game_id):
    """Generate a score vs time plot for a game."""
    conn = get_db_connection()
    try:
        # Get game info
        game_info = conn.execute("""
            SELECT g.game_date, t1.team_name, t2.team_name
            FROM dim_games g
            JOIN dim_teams t1 ON g.home_team_id = t1.team_id
            JOIN dim_teams t2 ON g.away_team_id = t2.team_id
            WHERE g.game_id = ?
        """, [game_id]).fetchone()
        
        if not game_info:
            return jsonify({'error': 'Game not found'}), 404
        
        game_date, home_team, away_team = game_info
        
        # Get play-by-play data with scores
        pbp = conn.execute("""
            SELECT period, pctimestring, score, homedescription, visitordescription
            FROM fact_play_by_play
            WHERE game_id = ? AND score IS NOT NULL
            ORDER BY eventnum
        """, [game_id]).fetchall()
        
        if not pbp:
            return jsonify({'error': 'No play-by-play data found'}), 404
        
        # Get available periods for info
        periods_available = conn.execute("""
            SELECT DISTINCT period FROM fact_play_by_play
            WHERE game_id = ? AND score IS NOT NULL
            ORDER BY period
        """, [game_id]).fetchall()
        available_periods = [p[0] for p in periods_available]
        logger.info(f"Game {game_id}: Periods available: {available_periods}")
        
        # Parse scores and create time series (in seconds)
        times_seconds = []
        home_scores = []
        away_scores = []
        
        for row in pbp:
            period, time_str, score, home_desc, away_desc = row
            if score and '-' in score:
                try:
                    away_score, home_score = map(int, score.split('-'))
                    
                    # Convert time to SECONDS elapsed
                    # Period 1-4 are 12 minutes (720 seconds) each, OT is 5 minutes (300 seconds)
                    if period <= 4:
                        period_start_seconds = (period - 1) * 720
                        period_duration = 720
                    else:
                        period_start_seconds = 2880 + (period - 5) * 300  # 48 minutes in seconds
                        period_duration = 300
                    # Parse time string (MM:SS.s)
                    if ':' in time_str:
                        minutes, seconds = time_str.split(':')
                        time_remaining_seconds = int(minutes) * 60 + float(seconds)
                        elapsed_seconds = period_start_seconds + period_duration - time_remaining_seconds
                        
                        times_seconds.append(elapsed_seconds)
                        home_scores.append(home_score)
                        away_scores.append(away_score)
                except:
                    continue
        
        if not times_seconds:
            return jsonify({'error': 'Could not parse score data'}), 404
        
        # Calculate score differential
        score_diff = [h - a for h, a in zip(home_scores, away_scores)]
        
        # Create side-by-side plots
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6), dpi=100)
        
        # Helper function to format seconds as MM:SS
        def format_time(seconds):
            minutes = int(seconds // 60)
            secs = int(seconds % 60)
            return f"{minutes}:{secs:02d}"
        
        # Determine max period and generate period boundaries
        max_period = max(available_periods) if available_periods else 4
        
        # Calculate period boundaries in seconds
        # Periods 1-4: 720 sec (12 min) each
        # OT periods (>4): 300 sec (5 min) each
        period_boundaries = []
        cumulative_time = 0
        
        for p in range(1, max_period + 1):
            if p <= 4:
                period_duration = 720  # 12 minutes
            else:
                period_duration = 300  # 5 minutes for OT
            cumulative_time += period_duration
            period_boundaries.append(cumulative_time)
        
        # Quarter/period marks for vertical lines (all except the last)
        period_marks = period_boundaries[:-1] if len(period_boundaries) > 1 else []
        
        # Generate tick positions and labels
        tick_positions = [0] + period_boundaries
        tick_labels = []
        for i, pos in enumerate(tick_positions):
            if i == 0:
                tick_labels.append('0:00')
            elif i <= 4:
                tick_labels.append(f'{i*12}:00')  # End of Q1, Q2, Q3, Q4
            else:
                ot_num = i - 4
                tick_labels.append(f'OT{ot_num}')  # OT1, OT2, etc.
        
        max_time = max(times_seconds) if times_seconds else period_boundaries[-1] if period_boundaries else 2880
        game_duration = max(max_time, period_boundaries[-1] if period_boundaries else 2880)
        
        # ===== LEFT PLOT: Score Timeline =====
        ax1.plot(times_seconds, home_scores, label=home_team, linewidth=2, color='#1f77b4')
        ax1.plot(times_seconds, away_scores, label=away_team, linewidth=2, color='#ff7f0e')
        
        # Fill area between lines (simplified)
        ax1.fill_between(times_seconds, home_scores, away_scores, alpha=0.2, color='gray')
        
        # Add period lines
        for p_sec in period_marks:
            ax1.axvline(x=p_sec, color='gray', linestyle='--', alpha=0.3, linewidth=0.8)
        
        ax1.set_xlabel('Game Time', fontsize=11)
        ax1.set_ylabel('Score', fontsize=11)
        ax1.set_title('Score Timeline', fontsize=12, fontweight='bold')
        ax1.legend(loc='upper left', fontsize=9)
        ax1.grid(True, alpha=0.2)
        ax1.set_xlim(0, game_duration)
        ax1.set_xticks(tick_positions)
        ax1.set_xticklabels(tick_labels)
        
        # ===== RIGHT PLOT: Score Differential =====
        ax2.plot(times_seconds, score_diff, linewidth=2, color='#333')
        ax2.fill_between(times_seconds, score_diff, 0, alpha=0.3, color='gray')
        ax2.axhline(y=0, color='black', linestyle='-', linewidth=1)
        
        # Add period lines
        for p_sec in period_marks:
            ax2.axvline(x=p_sec, color='gray', linestyle='--', alpha=0.3, linewidth=0.8)
        
        ax2.set_xlabel('Game Time', fontsize=11)
        ax2.set_ylabel('Score Diff (Home - Away)', fontsize=11)
        ax2.set_title('Score Differential', fontsize=12, fontweight='bold')
        ax2.grid(True, alpha=0.2)
        ax2.set_xlim(0, game_duration)
        max_diff = max([abs(s) for s in score_diff])
        ax2.set_ylim(-max_diff, max_diff)
        ax2.set_xticks(tick_positions)
        ax2.set_xticklabels(tick_labels)
        # Main title
        fig.suptitle(f'{away_team} @ {home_team} - {game_date}', 
                     fontsize=13, fontweight='bold')
        
        # Save to buffer
        buf = io.BytesIO()
        plt.tight_layout()
        plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
        buf.seek(0)
        plt.close(fig)
        
        return send_file(buf, mimetype='image/png')
        
    finally:
        conn.close()


@app.route('/api/game/<game_id>/autocorr')
def get_autocorr_plot(game_id):
    """Generate autocorrelation plot for score differential with user-specified lag."""
    lag = request.args.get('lag', 1, type=int)
    
    if lag < 1 or lag > 100:
        return jsonify({'error': 'Lag must be between 1 and 100'}), 400
    
    conn = get_db_connection()
    try:
        # Get game info
        game_info = conn.execute("""
            SELECT g.game_date, t1.team_name, t2.team_name
            FROM dim_games g
            JOIN dim_teams t1 ON g.home_team_id = t1.team_id
            JOIN dim_teams t2 ON g.away_team_id = t2.team_id
            WHERE g.game_id = ?
        """, [game_id]).fetchone()
        
        if not game_info:
            return jsonify({'error': 'Game not found'}), 404
        
        game_date, home_team, away_team = game_info
        
        # Get play-by-play data with scores
        pbp = conn.execute("""
            SELECT period, pctimestring, score
            FROM fact_play_by_play
            WHERE game_id = ? AND score IS NOT NULL
            ORDER BY eventnum
        """, [game_id]).fetchall()
        
        if not pbp:
            return jsonify({'error': 'No play-by-play data found'}), 404

        # Get available periods for this game
        periods_available = sorted(set([row[0] for row in pbp]))
        max_period = max(periods_available) if periods_available else 4

        # Calculate period boundaries in seconds
        period_boundaries = []
        cumulative_time = 0
        for p in range(1, max_period + 1):
            if p <= 4:
                period_duration = 720
            else:
                period_duration = 300
            cumulative_time += period_duration
            period_boundaries.append(cumulative_time)

        # Period marks for vertical lines
        period_marks = period_boundaries[:-1] if len(period_boundaries) > 1 else []

        # Generate tick positions and labels
        tick_positions = [0] + period_boundaries
        tick_labels = []
        for i, pos in enumerate(tick_positions):
            if i == 0:
                tick_labels.append('0:00')
            elif i <= 4:
                tick_labels.append(f'{i*12}:00')
            else:
                ot_num = i - 4
                tick_labels.append(f'OT{ot_num}')

        # Parse scores and create time series (in seconds)
        times_seconds = []
        score_diff = []

        for row in pbp:
            period, time_str, score = row
            if score and '-' in score:
                try:
                    away_score, home_score = map(int, score.split('-'))

                    # Convert time to SECONDS elapsed
                    if period <= 4:
                        period_start_seconds = (period - 1) * 720
                        period_duration = 720
                    else:
                        period_start_seconds = 2880 + (period - 5) * 300
                        period_duration = 300

                    if ':' in time_str:
                        minutes, seconds = time_str.split(':')
                        time_remaining_seconds = int(minutes) * 60 + float(seconds)
                        elapsed_seconds = period_start_seconds + period_duration - time_remaining_seconds

                        times_seconds.append(elapsed_seconds)
                        score_diff.append(home_score - away_score)
                except:
                    continue

        if len(times_seconds) < lag + 10:
            return jsonify({'error': 'Not enough data points for autocorrelation'}), 400

        # Interpolate to 1-second intervals
        max_time = int(max(times_seconds))
        time_grid = np.arange(0, max_time + 1)
        score_diff_interp = np.interp(time_grid, times_seconds, score_diff)

        # Calculate autocorrelation for the specified lag
        n = len(score_diff_interp)
        x = score_diff_interp

        # Normalize the data
        x = x - np.mean(x)

        # Calculate autocorrelation at specified lag
        if lag >= n:
            lag = n - 1

        # Calculate autocorrelation for lags 1 to user-specified lag
        max_lag_to_show = min(lag, n // 4)  # Show up to user-specified lag
        lags = np.arange(1, max_lag_to_show + 1)
        autocorrs = []

        for l in lags:
            if l < n:
                corr = np.corrcoef(x[:-l], x[l:])[0, 1]
                if np.isnan(corr):
                    corr = 0
                autocorrs.append(corr)
            else:
                autocorrs.append(0)

        # Create the plot
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), dpi=100)
        
        game_duration = max(max_time, period_boundaries[-1] if period_boundaries else 2880)

        # ===== TOP PLOT: Score Differential (1-second resolution) =====
        ax1.plot(time_grid, score_diff_interp, linewidth=1, color='#333', alpha=0.8)
        ax1.fill_between(time_grid, score_diff_interp, 0, alpha=0.3, color='gray')
        ax1.axhline(y=0, color='black', linestyle='-', linewidth=1)

        # Add period lines
        for p_sec in period_marks:
            ax1.axvline(x=p_sec, color='gray', linestyle='--', alpha=0.3, linewidth=0.8)

        ax1.set_xlabel('Game Time', fontsize=11)
        ax1.set_ylabel('Score Diff (Home - Away)', fontsize=11)
        ax1.set_title('Score Differential (1-second resolution)', fontsize=12, fontweight='bold')
        ax1.grid(True, alpha=0.2)
        ax1.set_xlim(0, game_duration)
        max_diff = max(abs(np.min(score_diff_interp)), abs(np.max(score_diff_interp)))
        ax1.set_ylim(-max_diff, max_diff)
        ax1.set_xticks(tick_positions)
        ax1.set_xticklabels(tick_labels)
        
        # ===== BOTTOM PLOT: Autocorrelation =====
        ax2.bar(lags, autocorrs, width=0.8, color='#667eea', edgecolor='#764ba2', alpha=0.7)
        ax2.axhline(y=0, color='black', linestyle='-', linewidth=1)
        
        # Highlight significant correlations
        significance_threshold = 2 / np.sqrt(n)  # Approximate 95% confidence
        ax2.axhline(y=significance_threshold, color='red', linestyle='--', alpha=0.5, label=f'95% CI ({significance_threshold:.3f})')
        ax2.axhline(y=-significance_threshold, color='red', linestyle='--', alpha=0.5)
        
        ax2.set_xlabel('Lag (seconds)', fontsize=11)
        ax2.set_ylabel('Autocorrelation', fontsize=11)
        ax2.set_title(f'Score Differential Autocorrelation (lag = {lag})', fontsize=12, fontweight='bold')
        ax2.grid(True, alpha=0.2, axis='y')
        ax2.set_xlim(0, max_lag_to_show + 1)
        ax2.legend(loc='upper right', fontsize=9)
        
        # Main title
        fig.suptitle(f'{away_team} @ {home_team} - {game_date}', 
                     fontsize=13, fontweight='bold')
        
        # Save to buffer
        buf = io.BytesIO()
        plt.tight_layout()
        plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
        buf.seek(0)
        plt.close(fig)
        
        return send_file(buf, mimetype='image/png')
        
    finally:
        conn.close()


if __name__ == '__main__':
    print("Starting NBA Game Viewer on http://localhost:5000")
    app.run(debug=True, host='0.0.0.0', port=5000)
