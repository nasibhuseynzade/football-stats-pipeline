import pandas as pd
import sqlite3
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Date, Time, Float, ForeignKey, Boolean, DateTime
from sqlalchemy.orm import declarative_base
from sqlalchemy.exc import SQLAlchemyError

Base = declarative_base()

class League(Base):
    __tablename__ = 'leagues'
    
    league_id = Column(Integer, primary_key=True)
    name = Column(String)
    country = Column(String)
    season = Column(Integer)

class Team(Base):
    __tablename__ = 'teams'
    
    team_id = Column(Integer, primary_key=True)
    name = Column(String)

class Venue(Base):
    __tablename__ = 'venues'
    
    venue_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)
    city = Column(String)

class Standings(Base):
    __tablename__ = 'standings'
    
    team_id = Column(Integer, ForeignKey('teams.team_id'), primary_key=True)
    rank = Column(Integer)
    team_logo = Column(String)
    points = Column(Integer)
    goal_difference = Column(Integer)
    group = Column(String)
    form = Column(String)
    status = Column(String)
    description = Column(String)
    
    # All matches
    all_played = Column(Integer)
    all_wins = Column(Integer)
    all_draws = Column(Integer)
    all_losses = Column(Integer)
    all_goals_for = Column(Integer)
    all_goals_against = Column(Integer)
    
    # Home matches
    home_played = Column(Integer)
    home_wins = Column(Integer)
    home_draws = Column(Integer)
    home_losses = Column(Integer)
    home_goals_for = Column(Integer)
    home_goals_against = Column(Integer)
    
    # Away matches
    away_played = Column(Integer)
    away_wins = Column(Integer)
    away_draws = Column(Integer)
    away_losses = Column(Integer)
    away_goals_for = Column(Integer)
    away_goals_against = Column(Integer)
    
    last_update = Column(DateTime)

class Fixture(Base):
    __tablename__ = 'fixtures'
    
    fixture_id = Column(Integer, primary_key=True)
    date = Column(Date)
    time = Column(Time)
    status = Column(String)
    elapsed_time = Column(Integer)
    
    # Foreign Keys
    league_id = Column(Integer, ForeignKey('leagues.league_id'))
    venue_id = Column(Integer, ForeignKey('venues.venue_id'))
    home_team_id = Column(Integer, ForeignKey('teams.team_id'))
    away_team_id = Column(Integer, ForeignKey('teams.team_id'))
    
    # Match Details
    referee = Column(String)
    round = Column(String)
    
    # Scores
    home_score = Column(Integer)
    away_score = Column(Integer)
    score_halftime_home = Column(Integer)
    score_halftime_away = Column(Integer)
    score_fulltime_home = Column(Integer)
    score_fulltime_away = Column(Integer)
    score_extratime_home = Column(Integer)
    score_extratime_away = Column(Integer)
    score_penalty_home = Column(Integer)
    score_penalty_away = Column(Integer)
    
    # Results
    home_team_winner = Column(Boolean)
    away_team_winner = Column(Boolean)

def load_data_to_db(fixtures_df=None, standings_df=None, db_url='sqlite:///football_data.db'):
    """
    Load fixtures and standings data into a relational database
    
    Args:
        fixtures_df (pd.DataFrame): Normalized fixtures DataFrame
        standings_df (pd.DataFrame): Normalized standings DataFrame
        db_url (str): Database connection URL
    """
    try:
        # Create engine
        engine = create_engine(db_url)
        
        # Drop existing tables (if any) to start fresh
        Base.metadata.drop_all(engine)
        
        # Create all tables
        Base.metadata.create_all(engine)
        
        with engine.begin() as conn:
            if fixtures_df is not None:
                # Process fixtures data (keeping existing code...)
                leagues_df = fixtures_df[['league_id', 'league_name', 'league_country', 'season']].drop_duplicates()
                leagues_df.columns = ['league_id', 'name', 'country', 'season']
                
                teams_df = pd.concat([
                    fixtures_df[['home_team_id', 'home_team_name']].rename(columns={'home_team_id': 'team_id', 'home_team_name': 'name'}),
                    fixtures_df[['away_team_id', 'away_team_name']].rename(columns={'away_team_id': 'team_id', 'away_team_name': 'name'})
                ]).drop_duplicates()
                
                venues_df = fixtures_df[['venue_name', 'venue_city']].drop_duplicates()
                venues_df.columns = ['name', 'city']
                venues_df['venue_id'] = range(1, len(venues_df) + 1)
                
                fixtures_df = fixtures_df.merge(venues_df, 
                                             left_on=['venue_name', 'venue_city'],
                                             right_on=['name', 'city'])
                
                # Load fixtures-related tables
                leagues_df.to_sql('leagues', conn, if_exists='append', index=False)
                teams_df.to_sql('teams', conn, if_exists='append', index=False)
                venues_df.to_sql('venues', conn, if_exists='append', index=False)
                
                fixtures_columns = [
                    'fixture_id', 'date', 'time', 'status', 'elapsed_time',
                    'league_id', 'venue_id', 'home_team_id', 'away_team_id',
                    'referee', 'round',
                    'home_score', 'away_score',
                    'score_halftime_home', 'score_halftime_away',
                    'score_fulltime_home', 'score_fulltime_away',
                    'score_extratime_home', 'score_extratime_away',
                    'score_penalty_home', 'score_penalty_away',
                    'home_team_winner', 'away_team_winner'
                ]
                fixtures_df[fixtures_columns].to_sql('fixtures', conn, if_exists='append', index=False)
            
            if standings_df is not None:
                # Process standings data - only include columns that match our schema
                standings_columns = [
                    'team_id', 'rank', 'team_logo', 'points', 
                    'goal_difference', 'group', 'form', 'status', 
                    'description', 'all_played', 'all_wins', 
                    'all_draws', 'all_losses', 'all_goals_for', 
                    'all_goals_against', 'home_played', 'home_wins', 
                    'home_draws', 'home_losses', 'home_goals_for', 
                    'home_goals_against', 'away_played', 'away_wins', 
                    'away_draws', 'away_losses', 'away_goals_for', 
                    'away_goals_against', 'last_update'
                ]
                
                # Convert last_update to datetime if it's not already
                if 'last_update' in standings_df.columns:
                    standings_df['last_update'] = pd.to_datetime(standings_df['last_update'])
                
                # Only include the columns that match our schema
                filtered_standings_df = standings_df[standings_columns]
                
                # Load standings data
                filtered_standings_df.to_sql('standings', conn, if_exists='append', index=False)
        
        print("Data successfully loaded into the database")
        
    except SQLAlchemyError as e:
        print(f"Database error occurred: {e}")
        raise  # Re-raise the exception to see the full error message
    except Exception as e:
        print(f"An error occurred: {e}")
        raise  # Re-raise the exception to see the full error message

if __name__ == "__main__":
    # Load both fixtures and standings data
    fixtures_df = ...
    standings_df = ...
    load_data_to_db(fixtures_df, standings_df)
    