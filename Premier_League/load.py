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
import sqlite3

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


def manage_league_table(df, db_path='league_table.db', verify=False):
    """
    Handles database creation, data insertion, and optional verification for league table.

    Args:
        df (pd.DataFrame): DataFrame containing team performance data.
        db_path (str): Path to the SQLite database file.
        verify (bool): Whether to verify the number of rows inserted into the database.
    """
    # Create a connection to SQLite
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # Drop existing table if you want to start fresh
        cursor.execute('DROP TABLE IF EXISTS league_table')

        # Create the table with team_name as PRIMARY KEY
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS league_table (
                team_name TEXT PRIMARY KEY,
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

        # Insert data into the database
        for _, row in df.iterrows():
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

        # Optional: Verify the data
        if verify:
            cursor.execute('SELECT COUNT(*) FROM league_table')
            print(f"Total rows in database: {cursor.fetchone()[0]}")

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    finally:
        # Close the connection
        conn.close()