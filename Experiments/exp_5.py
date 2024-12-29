#loading to aws s3 with airflow (not done yet)

import json
import pandas as pd
import os
import schedule
import time
import requests
from collections import defaultdict
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import io
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

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

import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

def load_to_s3(dataframe, bucket_name, file_name, aws_access_key=None, aws_secret_key=None, region_name='us-east-1'):
    """
    Upload a DataFrame to an S3 bucket as a CSV file.
    
    Args:
        dataframe (pd.DataFrame): The DataFrame to upload.
        bucket_name (str): The name of the S3 bucket.
        file_name (str): The key name for the file in the S3 bucket.
        aws_access_key (str, optional): AWS access key ID. If None, environment credentials will be used.
        aws_secret_key (str, optional): AWS secret access key. If None, environment credentials will be used.
        region_name (str): The AWS region where the bucket is located. Default is 'us-east-1'.
    
    Raises:
        ValueError: If the DataFrame is empty.
        Exception: For other unexpected issues during upload.
    """
    

    # Validate that the DataFrame is not empty
    if dataframe.empty:
        raise ValueError("The DataFrame is empty and cannot be uploaded to S3.")
    
        
    # Generate the dynamic file name
    today = datetime.now().strftime('%Y%m%d')
    file_name = f'standings_snapshots/standings_{today}.csv'

    # Create an S3 client
    try:
        if aws_access_key and aws_secret_key:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=region_name
            )
        else:
            s3_client = boto3.client('s3', region_name=region_name)
        
        # Convert DataFrame to CSV in memory
        csv_buffer = io.StringIO()
        dataframe.to_csv(csv_buffer, index=False)
        
        # Upload the CSV file to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=csv_buffer.getvalue()
        )
        print(f"File '{file_name}' successfully uploaded to bucket '{bucket_name}'.")
    
    except NoCredentialsError:
        print("AWS credentials not found.")
        raise
    except PartialCredentialsError:
        print("Incomplete AWS credentials provided.")
        raise
    except Exception as e:
        print(f"An error occurred: {e}")
        raise



if __name__ == "__main__":

    matches_data = extract_data(auth_token=auth_token)
    # Transform data
    team_performance_df = transform_match_data(matches_data)
    # Sort DataFrame by points in descending order
    team_performance_df = team_performance_df.sort_values('points', ascending=False)
    # Save to CSV
    team_performance_df.to_csv('team_performance.csv', index=False)
    
    # Upload DataFrame to S3
    load_to_s3(
        dataframe=team_performance_df,
        bucket_name='pl-standing',
        file_name='team_performance.csv',
        aws_access_key=aws_access_key,  
        aws_secret_key=aws_secret_key   
    )



# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'email': 'nasib.huseynzade@gmail.com',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
with DAG(
    'daily_data_pipeline',
    default_args=default_args,
    description='Daily snapshot pipeline',
    schedule_interval=timedelta(days=1),  # Runs daily
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    # Create tasks
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id='load_data_to_s3',
        python_callable=load_data_to_s3,
    )

    # Set task dependencies
    extract_task >> transform_task >> load_task
