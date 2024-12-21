# it is not finalized yet

import json
import pandas as pd
from collections import defaultdict
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Create SQLAlchemy Base
Base = declarative_base()

# Define the Team Performance table model
class TeamPerformance(Base):
    __tablename__ = 'team_performance'
    
    team_id = Column(Integer, primary_key=True)
    team_name = Column(String(100))
    total_games = Column(Integer)
    goals_scored = Column(Integer)
    goals_conceded = Column(Integer)
    wins = Column(Integer)
    losses = Column(Integer)
    draws = Column(Integer)
    points = Column(Integer)
    win_percentage = Column(Float)
    home_games = Column(Integer)
    away_games = Column(Integer)
    home_goals_scored = Column(Integer)
    away_goals_scored = Column(Integer)
    home_performance = Column(Float)
    away_performance = Column(Float)

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
        'team_id', 'team_name', 'total_games',
        'goals_scored', 'goals_conceded',
        'wins', 'losses', 'draws', 'points',
        'win_percentage', 'home_games', 'away_games',
        'home_goals_scored', 'away_goals_scored',
        'home_performance', 'away_performance'
    ]
    
    return df[columns]

def main():
    # Database connection settings
    DATABASE_URL = "postgresql://username:password@localhost:5432/football_db"  # Replace with your database credentials
    
    try:
        # Create database engine
        engine = create_engine(DATABASE_URL)
        
        # Create tables
        Base.metadata.create_all(engine)
        
        # Read JSON file
        with open('matches.json', 'r') as file:
            json_data = json.load(file)
        
        # Transform data
        team_performance_df = transform_match_data(json_data)
        
        # Load data into database
        team_performance_df.to_sql(
            'team_performance',
            engine,
            if_exists='replace',  # Options: 'fail', 'replace', 'append'
            index=False
        )
        
        print("Data successfully loaded into the database!")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()