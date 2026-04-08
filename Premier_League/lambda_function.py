import json
import pandas as pd
from collections import defaultdict
import io
import requests
import boto3

# Initialize S3 client
s3_client = boto3.client('s3')

BUCKET_NAME = "new-pl" 
STATE_FILE = "last_date.txt"
DATA_FILE = "league_standings.csv"

def get_last_processed_date():
    """Reads the last processed date from S3. Returns a very old date if missing (for initial setup)."""
    try:
        obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=STATE_FILE)
        return obj['Body'].read().decode('utf-8')
    except:
        return "2000-01-01T00:00:00Z"

def save_last_processed_date(date_str):
    """Saves the newly processed date to S3."""
    s3_client.put_object(Bucket=BUCKET_NAME, Key=STATE_FILE, Body=date_str)

def get_old_standings():
    """Reads current standings from S3 and converts them to a dictionary."""
    team_stats = defaultdict(lambda: {
        'team_id': 0, 'team_name': '', 'goals_scored': 0, 'goals_conceded': 0,
        'wins': 0, 'losses': 0, 'draws': 0, 'goal_difference': 0,
        'home_games': 0, 'away_games': 0
    })
    
    try:
        obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=DATA_FILE)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        
        # Convert DataFrame to a dictionary (with team_id as key)
        stats_dict = df.set_index('team_id').to_dict('index')
        for k, v in stats_dict.items():
            team_stats[k].update(v)
            team_stats[k]['team_id'] = k # Write ID back into each record
    except Exception as e:
        print("Eski veri bulunamadı, sıfırdan tablo oluşturuluyor...")
        
    return team_stats

def extract_data(auth_token):
    """Fetches matches from the API."""
    url = "https://api.football-data.org/v4/competitions/PL/matches"
    headers = {"X-Auth-Token": auth_token}
    response = requests.get(url, headers=headers)
    return response.json().get("matches", [])

def transform_match_data(matches, last_date, team_stats):
    newest_match_date = last_date
    processed_count = 0

    print("Veriler işleniyor...")
    for match in matches:
        match_date = match['utcDate']
        
        # Process only FINISHED matches newer than our saved date (incremental logic)
        if match['status'] != 'FINISHED' or match_date <= last_date:
            continue
            
        processed_count += 1
        home_team = match['homeTeam']
        away_team = match['awayTeam']
        home_score = match['score']['fullTime']['home']
        away_score = match['score']['fullTime']['away']
        
        # Update stats (adds on top of existing values)
        team_stats[home_team['id']].update({
            'team_id': home_team['id'],
            'team_name': home_team['name'],
            'goals_scored': team_stats[home_team['id']]['goals_scored'] + home_score,
            'goals_conceded': team_stats[home_team['id']]['goals_conceded'] + away_score,
            'home_games': team_stats[home_team['id']]['home_games'] + 1
        })
        
        team_stats[away_team['id']].update({
            'team_id': away_team['id'],
            'team_name': away_team['name'],
            'goals_scored': team_stats[away_team['id']]['goals_scored'] + away_score,
            'goals_conceded': team_stats[away_team['id']]['goals_conceded'] + home_score,
            'away_games': team_stats[away_team['id']]['away_games'] + 1
        })
        
        if home_score > away_score:
            team_stats[home_team['id']]['wins'] += 1
            team_stats[away_team['id']]['losses'] += 1
        elif away_score > home_score:
            team_stats[away_team['id']]['wins'] += 1
            team_stats[home_team['id']]['losses'] += 1
        else:
            team_stats[home_team['id']]['draws'] += 1
            team_stats[away_team['id']]['draws'] += 1

        # Keep the date of the most recently processed match in memory
        if match_date > newest_match_date:
            newest_match_date = match_date
            
    print(f"Toplam {processed_count} yeni maç işlendi.")
    
    # Build DataFrame and compute derived fields
    df = pd.DataFrame.from_dict(team_stats, orient='index')
    
    if not df.empty:
        df['total_games'] = df['wins'] + df['draws'] + df['losses']
        df['win_percentage'] = (df['wins'] / df['total_games'] * 100).round(2)
        df['points'] = (df['wins'] * 3) + (df['draws'] * 1)
        df['goal_difference'] = df['goals_scored'] - df['goals_conceded']
            
        # NOTE: we added the team_id column
        columns = [
            'team_id', 'team_name', 'total_games', 'points',
            'goals_scored', 'goals_conceded','goal_difference',
            'wins', 'draws', 'losses', 'win_percentage'
        ]
        df = df[columns]
        
    return df, newest_match_date

def lambda_handler(event, context):
    try:
        auth_token = "our_api_token_here" # Replace with your actual API token
        
        # 1. Read state (memory)
        last_date = get_last_processed_date()
        print(f"En son işlenen tarih: {last_date}")
        
        # 2. Read previous table from S3
        team_stats = get_old_standings()
        
        # 3. Fetch data from API
        matches_data = extract_data(auth_token)
        
        # 4. Process only new matches (incremental)
        team_performance_df, newest_date = transform_match_data(matches_data, last_date, team_stats)
        
        if not team_performance_df.empty:
            # Sort by points
            team_performance_df = team_performance_df.sort_values(
                by=['points', 'goal_difference'], 
                ascending=[False, False]
            )
            
            # 5. Convert the new table to CSV and write to S3
            csv_buffer = io.StringIO()
            team_performance_df.to_csv(csv_buffer, index=False)
            
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=DATA_FILE,
                Body=csv_buffer.getvalue(),
                ContentType='text/csv',
                ACL='public-read' # Make it public so the website can read it
            )
            
            # 6. Save the new date to S3
            if newest_date > last_date:
                save_last_processed_date(newest_date)
                
            msg = f"Başarılı! {newest_date} tarihine kadar güncellendi."
        else:
            msg = "Yeni oynanmış maç bulunamadı. Tablo güncel."

        return {
            "statusCode": 200,
            "headers": {"Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"message": msg})
        }
        
    except Exception as e:
        print(f"Hata detayı: {str(e)}")
        return {
            "statusCode": 500,
            "headers": {"Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"error": str(e)})
        }