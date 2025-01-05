import json
import pandas as pd
import requests
import requests
from datetime import datetime

auth_token="d77b6c777bmsh11749babe461d18p13a86fjsn14f5ae360f4c"


def extract_standings(auth_token):

    url = "https://api-football-v1.p.rapidapi.com/v3/standings"

    querystring = {"season":"2023","league":"203"}

    headers = {
        "x-rapidapi-key": auth_token,
        "x-rapidapi-host": "api-football-v1.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)

    print(response.json())
    # 4. Yanıtı Kontrol Et
    if response.status_code == 200:  # Başarılı istek
        data = response.json()  # JSON verisini al
        print("Veri çekildi:")
    else:
        print(f"Bir hata oluştu. Durum Kodu: {response.status_code}")
        print(response.text)
    standings = data['response'][0]['league']['standings']

    with open('standings_2023.json', 'w', encoding='utf-8') as f:
        json.dump(standings, f, indent=4, ensure_ascii=False)
    print("Standings saved to 'data2.json'")
    
    return standings

def normalize_standings(json_data):
    # List to hold the normalized standings
    standings_normalized = []

    # Iterate through the standings (each group may have multiple standings arrays)
    for group_standings in json_data:
        for team in group_standings:
            normalized = {
                "rank": team["rank"],
                "team_id": team["team"]["id"],
                "team_name": team["team"]["name"],
                "team_logo": team["team"]["logo"],
                "points": team["points"],
                "goal_difference": team["goalsDiff"],
                "group": team["group"],
                "form": team["form"],
                "status": team["status"],
                "description": team["description"],
                "all_played": team["all"]["played"],
                "all_wins": team["all"]["win"],
                "all_draws": team["all"]["draw"],
                "all_losses": team["all"]["lose"],
                "all_goals_for": team["all"]["goals"]["for"],
                "all_goals_against": team["all"]["goals"]["against"],
                "home_played": team["home"]["played"],
                "home_wins": team["home"]["win"],
                "home_draws": team["home"]["draw"],
                "home_losses": team["home"]["lose"],
                "home_goals_for": team["home"]["goals"]["for"],
                "home_goals_against": team["home"]["goals"]["against"],
                "away_played": team["away"]["played"],
                "away_wins": team["away"]["win"],
                "away_draws": team["away"]["draw"],
                "away_losses": team["away"]["lose"],
                "away_goals_for": team["away"]["goals"]["for"],
                "away_goals_against": team["away"]["goals"]["against"],
                "last_update": team["update"]
            }
            standings_normalized.append(normalized)

    # Convert to DataFrame
    df = pd.DataFrame(standings_normalized)

    return df

def extract_fixture(auth_token):

    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures"
    
    querystring = {"season": "2023", "team": "611"}
    
    headers = {
        "x-rapidapi-key": auth_token,
        "x-rapidapi-host": "api-football-v1.p.rapidapi.com"
    }
    
    try:
        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status()  # Raise exception for bad status codes
        
        data = response.json()
        print("Data successfully extracted")
        
        # Extract fixtures from response
        fixtures = data.get("response", [])
        
        # Save raw data
        with open('data2.json', 'w', encoding='utf-8') as f:
            json.dump(fixtures, f, indent=4, ensure_ascii=False)
        print("Raw fixtures saved to 'data2.json'")
        
        return fixtures
        
    except requests.exceptions.RequestException as e:
        print(f"Error during API request: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON response: {e}")
        return None

def normalize_fixtures(json_data):

    if not json_data:
        print("No data to normalize")
        return None
        
    fixtures = []
    
    for fixture in json_data:
        # Convert UTC timestamp to datetime
        match_date = datetime.fromisoformat(fixture['fixture']['date'].replace('Z', '+00:00'))
        
        normalized = {
            # Fixture information
            'fixture_id': fixture['fixture']['id'],
            'date': match_date.date(),
            'time': match_date.time(),
            'status': fixture['fixture']['status']['long'],
            'elapsed_time': fixture['fixture']['status']['elapsed'],
            
            # Venue information
            'venue_name': fixture['fixture']['venue']['name'],
            'venue_city': fixture['fixture']['venue']['city'],
            'referee': fixture['fixture']['referee'],
            
            # League information
            'league_id': fixture['league']['id'],
            'league_name': fixture['league']['name'],
            'league_country': fixture['league']['country'],
            'season': fixture['league']['season'],
            'round': fixture['league']['round'],
            
            # Team information
            'home_team_id': fixture['teams']['home']['id'],
            'home_team_name': fixture['teams']['home']['name'],
            'away_team_id': fixture['teams']['away']['id'],
            'away_team_name': fixture['teams']['away']['name'],
            
            # Score information
            'home_score': fixture['goals']['home'],
            'away_score': fixture['goals']['away'],
            'score_halftime_home': fixture['score']['halftime']['home'],
            'score_halftime_away': fixture['score']['halftime']['away'],
            'score_fulltime_home': fixture['score']['fulltime']['home'],
            'score_fulltime_away': fixture['score']['fulltime']['away'],
            'score_extratime_home': fixture['score']['extratime']['home'],
            'score_extratime_away': fixture['score']['extratime']['away'],
            'score_penalty_home': fixture['score']['penalty']['home'],
            'score_penalty_away': fixture['score']['penalty']['away'],
            
            # Winner information
            'home_team_winner': fixture['teams']['home'].get('winner'),
            'away_team_winner': fixture['teams']['away'].get('winner'),
        }
        
        fixtures.append(normalized)
    
    # Create DataFrame
    df = pd.DataFrame(fixtures)
    
    # Add derived columns
    df['full_score'] = df['home_score'].astype(str) + '-' + df['away_score'].astype(str)
    
    # Handle datetime sorting
    df['datetime'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['time'].astype(str))
    df = df.sort_values('datetime')
    
    # Save to CSV
    output_filename = f'fixtures_{df["season"].iloc[0]}.csv'
    df.to_csv(output_filename, index=False)
    print(f"Saved normalized fixtures to {output_filename}")
    
    return df