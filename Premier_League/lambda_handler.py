import json
import pandas as pd
from collections import defaultdict
from datetime import datetime
import os
import time
import io
import requests


auth_token = "0d1a4376aa1c463c8952abef2008f1b3"

def extract_data(auth_token):
    """Extract match data from football-data.org API"""
    url = "https://api.football-data.org/v4/competitions/PL/matches"
    
    headers = {
        "X-Auth-Token": auth_token
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        print("Data extracted successfully")
    else:
        print(f"Error occurred. Status Code: {response.status_code}")
        print(response.text)
        return None
    
    matches = data.get("matches", [])
    
    # Save raw matches data to /tmp directory
    file_path = '/tmp/matches.json'
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(matches, f, indent=4, ensure_ascii=False)
    print("Match stats are extracted to '/tmp/matches.json'")
    
    return matches


def transform_match_data(json_data):
    # Initialize a defaultdict to store team statistics
    team_stats = defaultdict(lambda: {
        'team_id': 0,
        'team_name': '',
        'goals_scored': 0,
        'goals_conceded': 0,
        'wins': 0,
        'losses': 0,
        'draws': 0,
        'home_games': 0,
        'away_games': 0,
        'home_goals_scored': 0,
        'away_goals_scored': 0
    })
    print("Transforming data...")
    # Process each match
    for match in json_data:
        if match['status'] != 'FINISHED':
            continue
            
        home_team = match['homeTeam']
        away_team = match['awayTeam']
        home_score = match['score']['fullTime']['home']
        away_score = match['score']['fullTime']['away']
        
        # Update home team stats
        team_stats[home_team['id']].update({
            'team_id': home_team['id'],
            'team_name': home_team['name'],
            'goals_scored': team_stats[home_team['id']]['goals_scored'] + home_score,
            'goals_conceded': team_stats[home_team['id']]['goals_conceded'] + away_score,
            'home_games': team_stats[home_team['id']]['home_games'] + 1,
            'home_goals_scored': team_stats[home_team['id']]['home_goals_scored'] + home_score
        })
        
        # Update away team stats
        team_stats[away_team['id']].update({
            'team_id': away_team['id'],
            'team_name': away_team['name'],
            'goals_scored': team_stats[away_team['id']]['goals_scored'] + away_score,
            'goals_conceded': team_stats[away_team['id']]['goals_conceded'] + home_score,
            'away_games': team_stats[away_team['id']]['away_games'] + 1,
            'away_goals_scored': team_stats[away_team['id']]['away_goals_scored'] + away_score
        })
        
        # Update win/loss/draw stats
        if home_score > away_score:
            team_stats[home_team['id']]['wins'] += 1
            team_stats[away_team['id']]['losses'] += 1
        elif away_score > home_score:
            team_stats[away_team['id']]['wins'] += 1
            team_stats[home_team['id']]['losses'] += 1
        else:
            team_stats[home_team['id']]['draws'] += 1
            team_stats[away_team['id']]['draws'] += 1
    
    # Convert to DataFrame
    df = pd.DataFrame.from_dict(team_stats, orient='index')
    
    # Calculate total games and win/loss percentages
    df['total_games'] = df['home_games'] + df['away_games']
    df['win_percentage'] = (df['wins'] / df['total_games'] * 100).round(2)
    
    # Calculate points (3 for win, 1 for draw, 0 for loss)
    df['points'] = (df['wins'] * 3) + (df['draws'] * 1)
    
    # Reorder columns
    columns = [
        'team_name', 'total_games', 'points',
        'goals_scored', 'goals_conceded',
        'wins', 'draws', 'losses', 
        'win_percentage', 'home_games', 'away_games',
        'home_goals_scored', 'away_goals_scored'
    ]
    
    return df[columns]


def lambda_handler(event, context):
    try:
        # Authentication token (consider using environment variables for security)
        auth_token = "0d1a4376aa1c463c8952abef2008f1b3"
        
        # Extract data from the external API
        matches_data = extract_data(auth_token=auth_token)
        
        # Transform data
        team_performance_df = transform_match_data(matches_data)
        
        # Sort DataFrame by points in descending order
        team_performance_df = team_performance_df.sort_values('points', ascending=False)
        
        # Convert DataFrame to CSV format (as string)
        csv_buffer = io.StringIO()
        team_performance_df.to_csv(csv_buffer, index=False)
        csv_content = csv_buffer.getvalue()
        
        # Return CSV as the response
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "text/csv",
                "Access-Control-Allow-Origin": "*"
            },
            "body": csv_content
        }
    except Exception as e:
        # Handle any errors and return an appropriate error response
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps({"error": str(e)})
        }
        