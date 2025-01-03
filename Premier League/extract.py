import json
import pandas as pd
from datetime import datetime
import os
import schedule
import time
import requests
from collections import defaultdict


def extract_data():
    """Extract match data from football-data.org API"""
    url = "https://api.football-data.org/v4/competitions/PL/matches"
    
    with open("config.json") as f:
        config = json.load(f)

    auth_token = config["X-Auth-Token"]
    
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