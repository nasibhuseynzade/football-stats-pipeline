# extracting data from API and saving all match scores to a CSV file

import json
import requests
import io
import pandas as pd

# API URL
url= "https://api.football-data.org/v4/competitions/PL/matches"



# Get the X-Auth-Token from user input
auth_token = "0d1a4376aa1c463c8952abef2008f1b3"

# Headers (Authentication key)
headers = {
    "X-Auth-Token": auth_token 
}

# API request
response = requests.get(url, headers=headers)

# Control the response
if response.status_code == 200:  
    data = response.json()  # get JSON data
    print("Data extracted succesfully:")
else:
    print(f"Bir hata oluştu. Durum Kodu: {response.status_code}")
    print(response.text)

matches = data.get("matches", [])  # "matches" listesine ulaş
df = pd.DataFrame(matches)  # DataFrame'e dönüştür

with open('matches.json', 'w', encoding='utf-8') as f:
    json.dump(matches, f, indent=4, ensure_ascii=False)
print("Match Stats ae extracted to 'matches.json' folder")


# Transform to DataFrame
matches_data = []
for match in matches:
    score_home = match.get("score", {}).get("fullTime", {}).get("home")
    score_away = match.get("score", {}).get("fullTime", {}).get("away")

    matches_data.append({
        "Matchday": match.get("matchday"),  # Matchday
        "Home Team": match.get("homeTeam", {}).get("shortName"),  # Home Team
        "Away Team": match.get("awayTeam", {}).get("shortName"),  # Away Team
        "Score": f"{score_home}-{score_away}",
        "Date": match.get("utcDate")  # Maç tarihi
    })
df = pd.DataFrame(matches_data)
# Group matches by matchday
grouped_matches = df.groupby("Matchday").apply(
    lambda x: x[["Home Team", "Away Team", "Score", "Date"]].to_dict(orient="records")
).to_dict()

# Prepare JSON output
json_output = json.dumps(grouped_matches)


print(json_output)
# Loading the data to CSV
#df.to_csv("premier_league_matches.csv", index=False)
#print("Mathc stats are extracted to 'premier_league_matches.csv' folder")