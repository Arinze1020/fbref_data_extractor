import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
from random import randint
from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem
import re

# Configure user agent rotator
software_names = [SoftwareName.CHROME.value]
operating_systems = [OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value]
user_agent_rotator = UserAgent(software_names=software_names, operating_systems=operating_systems, limit=10000)

# Define the index ranges for each team's data
team1_indices = {
    "summary": 3,
    "passing": 4,
    "pass_type": 5,
    "defensive_action": 6,
    "possession": 7,
    "miscellaneous_stats": 8,
    "goalkeeper": 9
}

team2_indices = {
    "summary": 10,
    "passing": 11,
    "pass_type": 12,
    "defensive_action": 13,
    "possession": 14,
    "miscellaneous_stats": 15,
    "goalkeeper": 16
}

# Function to scrape match report links
# def scrape_match_report_links(url):
#     response = requests.get(url)
#     response.raise_for_status()
#     soup = BeautifulSoup(response.content, 'html.parser')
#     base_url = "https://fbref.com"
#     match_report_cells = soup.find_all('td', {'data-stat': 'match_report'})
#     href_links = [base_url + a['href'] for cell in match_report_cells for a in cell.find_all('a', href=True)]
#     return href_links

# # URL to scrape match report links
# schedule_url = "https://fbref.com/en/comps/9/2023-2024/schedule/2023-2024-Premier-League-Scores-and-Fixtures"

# # Scrape match report links
# urls = scrape_match_report_links(schedule_url)

# for testing use this two urls it makes it easy to make sure the code is running before loading the 308 urls
urls = [
    'https://fbref.com/en/matches/3a6836b4/Burnley-Manchester-City-August-11-2023-Premier-League',
    'https://fbref.com/en/matches/26a7f90c/Arsenal-Nottingham-Forest-August-12-2023-Premier-Leagu',
    # Add more URLs here
]
total_urls = len(urls)
print(f"Number of URLs: {total_urls}")

# Data cleaning functions

# Function to check if a cell contains the pattern "number followed by 'Players'"
def contains_role(text):
    return bool(pd.Series(text).str.contains(r'\d+\s+Players', regex=True).any())

# Function to clean the 'match_time' column
def extract_date(text):
    if '2023' in text:
        return text.split('2023')[0] + '2023'
    elif '2024' in text:
        return text.split('2024')[0] + '2024'
    else:
        return text  # Return original text if no date found

# Initialize a dictionary to store dataframes for each index
index_sheets = {index: [] for index in team1_indices.keys()}

# Loop through each URL and extract the tables
for idx, url in enumerate(urls):
    print(f"Processing URL {idx + 1}/{total_urls}: {url}")

    # Rotate user agent
    user_agent = user_agent_rotator.get_random_user_agent()
    headers = {'User-Agent': user_agent}

    # Send a GET request to the URL
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Extract tables
    tables = pd.read_html(response.content)

    # Extract match info
    match_report = soup.find('h1').text
    teams = match_report.split(' Match Report')[0]
    home_team, away_team = teams.split(' vs. ')

    # Extract match time from the match report
    match_time = match_report.split(' – ')[1]

    # Extract scores (to check for the result)
    home_score = soup.find_all('div', class_='score')[0].text
    away_score = soup.find_all('div', class_='score')[1].text

    # Determine the result for both teams
    if int(home_score) > int(away_score):
        home_result, away_result = 'Win', 'Lose'
    elif int(home_score) < int(away_score):
        home_result, away_result = 'Lose', 'Win'
    else:
        home_result, away_result = 'Draw', 'Draw'

    # Loop over each index to create a combined sheet for both teams
    for index in team1_indices:
        team1_df = tables[team1_indices[index]].copy()  # Extract data for team 1
        team2_df = tables[team2_indices[index]].copy()  # Extract data for team 2

        # Add relevant information for both teams
        team1_df['Team'] = home_team
        team1_df['Result'] = home_result
        team1_df['Match Time'] = match_time
        team2_df['Team'] = away_team
        team2_df['Result'] = away_result
        team2_df['Match Time'] = match_time

        # Clean 'Match Time' column for both teams
        team1_df['Match Time'] = team1_df['Match Time'].apply(extract_date)
        team2_df['Match Time'] = team2_df['Match Time'].apply(extract_date)

        # Add matchup info
        matchup = f"{home_team} vs {away_team}"
        team1_df['Matchup'] = matchup
        team2_df['Matchup'] = matchup

        # Combine both teams' data for this index
        combined_df = pd.concat([team1_df, team2_df], ignore_index=True)

        # Check if the dataframe has a MultiIndex and flatten it if necessary
        if isinstance(combined_df.columns, pd.MultiIndex):
            combined_df.columns = ['_'.join(col).strip() for col in combined_df.columns.values]

        # Clean the dataset: Remove rows with "Players" in any cell
        mask = combined_df.applymap(lambda x: contains_role(x) if isinstance(x, str) else False)
        combined_df_cleaned = combined_df[~mask.any(axis=1)]

        # Append to the respective sheet in the dictionary
        index_sheets[index].append(combined_df_cleaned)

    # Add a random delay between requests
    time.sleep(randint(1, 20))

# Save all the sheets to a single Excel file
with pd.ExcelWriter('data.xlsx') as writer:
    for index, dfs in index_sheets.items():
        combined_index_df = pd.concat(dfs, ignore_index=True)
        combined_index_df.to_excel(writer, sheet_name=index.capitalize(), index=False)

print("All cleaned data saved to 'data.xlsx'")
