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
        
        # Parse scores and create time series
        times = []
        home_scores = []
        away_scores = []
        
        for row in pbp:
            period, time_str, score, home_desc, away_desc = row
            if score and '-' in score:
                try:
                    away_score, home_score = map(int, score.split('-'))
                    
                    # Convert time to minutes elapsed
                    # Period 1-4 are 12 minutes each, OT is 5 minutes
                    if period <= 4:
                        period_start = (period - 1) * 12
                    else:
                        period_start = 48 + (period - 5) * 5
                    # Parse time string (MM:SS.s)
                    if ':' in time_str:
                        minutes, seconds = time_str.split(':')
                        time_remaining = int(minutes) + float(seconds) / 60
                        elapsed = period_start + (12 if period <= 4 else 5) - time_remaining
                        
                        times.append(elapsed)
                        home_scores.append(home_score)
                        away_scores.append(away_score)
                except:
                    continue
        
        if not times:
            return jsonify({'error': 'Could not parse score data'}), 404
        
        # Calculate score differential
        score_diff = [h - a for h, a in zip(home_scores, away_scores)]
        
        # Create side-by-side plots (optimized for speed)
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6), dpi=100)
        
        # ===== LEFT PLOT: Score Timeline =====
        ax1.plot(times, home_scores, label=home_team, linewidth=2, color='#1f77b4')
        ax1.plot(times, away_scores, label=away_team, linewidth=2, color='#ff7f0e')
        
        # Fill area between lines (simplified)
        ax1.fill_between(times, home_scores, away_scores, alpha=0.2, color='gray')
        
        # Add quarter lines only (no labels for speed)
        for q in range(1, 5):
            ax1.axvline(x=q*12, color='gray', linestyle='--', alpha=0.3, linewidth=0.8)
        
        ax1.set_xlabel('Game Time (Minutes)', fontsize=11)
        ax1.set_ylabel('Score', fontsize=11)
        ax1.set_title('Score Timeline', fontsize=12, fontweight='bold')
        ax1.legend(loc='upper left', fontsize=9)
        ax1.grid(True, alpha=0.2)
        
        max_time = max(times) if times else 48
        ax1.set_xlim(0, max(max_time, 48))
        
        # ===== RIGHT PLOT: Score Differential =====
        ax2.plot(times, score_diff, linewidth=2, color='#333')
        ax2.fill_between(times, score_diff, 0, alpha=0.3, color='gray')
        ax2.axhline(y=0, color='black', linestyle='-', linewidth=1)
        
        # Add quarter lines
        for q in range(1, 5):
            ax2.axvline(x=q*12, color='gray', linestyle='--', alpha=0.3, linewidth=0.8)
        
        ax2.set_xlabel('Game Time (Minutes)', fontsize=11)
        ax2.set_ylabel('Score Diff (Home - Away)', fontsize=11)
        ax2.set_title('Score Differential', fontsize=12, fontweight='bold')
        ax2.grid(True, alpha=0.2)
        ax2.set_xlim(0, max(max_time, 48))
        max_diff = max([abs(s) for s in score_diff])
        ax2.set_ylim(-max_diff,max_diff)
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
