# Sixth experiment: Transform match data into team performance statistics and loading to database

import json
import pandas as pd
from collections import defaultdict
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os
import schedule
import time
import requests
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import io

with open("config.json") as f:
    config = json.load(f)

auth_token = config["X-Auth-Token"]
aws_access_key = config["aws_access_key_id"]
aws_secret_key = config["aws_secret_access_key"]

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
    
    # Save raw matches data
    with open('matches.json', 'w', encoding='utf-8') as f:
        json.dump(matches, f, indent=4, ensure_ascii=False)
    print("Match stats are extracted to 'matches.json' folder")
    
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
    df['home_performance'] = (df['home_goals_scored'] / df['home_games']).round(2)
    df['away_performance'] = (df['away_goals_scored'] / df['away_games']).round(2)
    
    # Calculate points (3 for win, 1 for draw, 0 for loss)
    df['points'] = (df['wins'] * 3) + (df['draws'] * 1)
    
    # Reorder columns
    columns = [
        'team_name', 'total_games', 'points',
        'goals_scored', 'goals_conceded',
        'wins', 'draws', 'losses', 
        'win_percentage', 'home_games', 'away_games',
        'home_goals_scored', 'away_goals_scored',
        'home_performance', 'away_performance'
    ]
    
    return df[columns]


import sqlite3


# Insert data into the database
def load_data_to_db(df):
    for index, row in df.iterrows():
        cursor.execute('''
            INSERT OR REPLACE INTO league_table (
                team_name, total_games, points, goals_scored, goals_conceded,
                wins, losses, draws, win_percentage, home_games,
                away_games, home_goals_scored, away_goals_scored,
                home_performance, away_performance
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', tuple(row))
    
    # Commit the changes
    conn.commit()



if __name__ == "__main__":


    # Create a connection to SQLite (or PostgreSQL if you choose)
    conn = sqlite3.connect('league_table.db')
    cursor = conn.cursor()

    # Create the table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS league_table (
            team_name TEXT,
            total_games INTEGER,
            points INTEGER,
            goals_scored INTEGER,
            goals_conceded INTEGER,
            wins INTEGER,
            losses INTEGER,
            draws INTEGER,
            win_percentage REAL,
            home_games INTEGER,
            away_games INTEGER,
            home_goals_scored INTEGER,
            away_goals_scored INTEGER,
            home_performance REAL,
            away_performance REAL
        )
    ''')

    matches_data = extract_data(auth_token=auth_token)
    # Transform data
    team_performance_df = transform_match_data(matches_data)
    # Sort DataFrame by points in descending order
    team_performance_df = team_performance_df.sort_values('points', ascending=False)
    # Save to CSV
    team_performance_df.to_csv('team_performance.csv', index=False)
    
    # Example usage
    load_data_to_db(team_performance_df)

    # Query the database
    query = "SELECT * FROM league_table"
    df = pd.read_sql_query(query, conn)

    # Display the table as a DataFrame
    print(df)

    # Close the connection
    conn.close()
