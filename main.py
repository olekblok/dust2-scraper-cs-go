import requests
import logging
import pandas as pd
from bs4 import BeautifulSoup
import re
import random
from concurrent.futures import ThreadPoolExecutor


SCRAPE_FROM = 2367000
SCRAPE_TO = 2367001


def load_user_agents(file_path='./lists/ua_list.txt'):
    """Load user agents from a file."""
    try:
        with open(file_path, 'r') as f:
            return f.read().splitlines()
    except Exception as e:
        logging.exception('Failed to load user agents:', exc_info=e)
        return []


def get_page(user_agents, page_url):
    """Send a GET request to the page."""
    user_agent = random.choice(user_agents)
    headers = {'User-Agent': user_agent}
    try:
        page = requests.get(page_url, headers=headers)
        page.raise_for_status()
        logging.info("Successful request for %s", page_url)
        return page_url, page.content
    except requests.RequestException as e:
        logging.exception("Connection error: %s - URL: %s", e, page_url)
        return page_url, None


def normalize_date(match_date):
    """Return date in correct format."""
    if match_date.startswith("May"):
        match = re.search(r'May \d{1,2}, \d{4}', match_date)
        if match:
            return match.group(0)
    else:
        pattern = (
            r'(?:Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Oct|Nov|Dec) '
            r'\d{1,2}, \d{4}'
        )
        match = re.search(pattern, match_date)
        if match:
            return match.group(0)
    return match_date.strip()


def parse_string(string):
    """Parse the match result string into separate scores."""
    parts = string.split(" - ")
    part_1 = int(parts[0].strip())
    part_2 = int(parts[1].strip())
    return part_1, part_2


def fetch_players_data(soup, match_id):
    """Scrape and return data about players."""
    players_data = []
    tabs = soup.find_all(
        "div", class_="tabs-content match-result-wrapper-tab not-active")
    for tab in tabs:
        map_number = tab.get("data-tab-content-id")
        map_id = str(match_id) + str(map_number)
        player_rows = tab.find_all("tr")
        for row in player_rows:
            player_data = [item.text.strip() for item in row.find_all("td")]
            if len(player_data) == 6:
                players_data.append([map_id] + player_data)

    return players_data


def fetch_matches(soup, match_id):
    """Scrape and return data about matches."""
    match_result = soup.find("div", class_="match-page-header-time").text.strip()
    match_date = normalize_date(
        soup.find("div", class_="match-page-header-day").text.strip())
    match_title = soup.find("h1", class_="matchpage-header-title").text.strip()
    score_1, score_2 = parse_string(match_result)
    # Checking if it is Bo1 
    if score_1 > 5 and score_1 > score_2:
        score_1 = 1
        score_2 = 0
    elif score_2 > 5 and score_2 > score_1:
        score_1 = 0
        score_2 = 1
    
    match_title = match_title.replace(" tips & odds", "")
    teams = re.findall(r'(.+?)\s+vs.\s+(.+)', match_title)

    return {
        'MatchID': match_id,
        'Team1': teams[0][0],
        'Team2': teams[0][1],
        'T1Score': score_1,
        'T2Score': score_2,
        'Date': match_date
    }


def fetch_maps(soup, match_id):
    """Scrape and return data about maps."""
    maps_names = []
    map_ids = []
    scores_team_1 = []
    scores_team_2 = []

    picked_maps = soup.find_all(
        "div", class_="standard-box map-container map-picked")
    leftover_map = soup.find(
        "div", class_="standard-box map-container map-leftover")

    for picked_map in picked_maps:
        scores_team_1.append(
            picked_map.find("div", class_="map-container-score-left").text.strip())
        scores_team_2.append(
            picked_map.find("div", class_="map-container-score-right").text.strip())
        maps_names.append(
            picked_map.find("div", class_="map-container-map-name").text.strip())

    if leftover_map:
        if leftover_map.find("div", class_="map-container-score-left").text.strip() != '':
            maps_names.append(
                leftover_map.find("div", class_="map-container-map-name").text.strip())
            scores_team_1.append(
                leftover_map.find("div", class_="map-container-score-left").text.strip())
            scores_team_2.append(
                leftover_map.find("div", class_="map-container-score-right").text.strip())

    map_ids = [str(match_id) + str(i + 1) for i in range(len(maps_names))]

    return {
        'MapID': map_ids,
        'T1Score': scores_team_1,
        'T2Score': scores_team_2,
        'MapName': maps_names,
        'MatchID': [match_id] * len(map_ids)
    }


def fetch_data(page_url, content):
    """Process page content to extract match, map, and player data."""
    if content is None:
        return None, None, None
    try:
        match_id = re.search(r'/matches/(\d+)/', page_url).group(1)
        soup = BeautifulSoup(content, 'html.parser')
        map_data = fetch_maps(soup, match_id)
        player_data = fetch_players_data(soup, match_id)
        match_data = fetch_matches(soup, match_id)
        return map_data, player_data, match_data
    except Exception as e:
        logging.exception("Error processing data: %s - URL: %s", e, page_url)
        return None, None, None


def save_to_csv(data, filename):
    """Save the data to a CSV file."""
    try:
        df = pd.DataFrame(data)
        df.to_csv(filename, index=False, sep=',', encoding='utf-8-sig')
        logging.info('File %s was saved successfully', filename)
    except Exception as e:
        logging.error("Error while saving file to CSV: %s", e)


def main():
    """Main function to orchestrate the scraping and saving of data."""
    page_urls = [
        f"http://www.dust2.us/matches/{i}/b8-vs-enterprise"
        for i in range(SCRAPE_FROM, SCRAPE_TO)
    ]
    user_agents = load_user_agents()
    all_map_data = []
    all_player_data = []
    all_match_data = []

    with ThreadPoolExecutor(max_workers=20) as executor:
        results = executor.map(lambda url: get_page(user_agents, url), page_urls)
        for page_url, content in results:
            map_data, player_data, match_data = fetch_data(page_url, content)
            if map_data:
                all_map_data.append(map_data)
            if player_data:
                all_player_data.extend(player_data)
            if match_data:
                all_match_data.append(match_data)

    if all_map_data:
        flat_map_data = {
            key: [item for sublist in [d[key] for d in all_map_data] for item in sublist]
            for key in all_map_data[0]
        }
        save_to_csv(flat_map_data, './data/maps.csv')

    if all_player_data:
        player_columns = ['MapID', 'Player', 'K - D', 'K-D', 'ADR', 'KAST', 'Rating']
        df_players = pd.DataFrame(all_player_data, columns=player_columns)
        df_players[['Kills', 'Deaths']] = df_players['K - D'].apply(lambda x: pd.Series(parse_string(x)))
        df_players['K-D'] = df_players['K-D'].str.replace('+', '').astype(int)
        df_players = df_players[['MapID', 'Player', 'Kills', 'Deaths', 'K-D', 'ADR', 'KAST', 'Rating']]
        save_to_csv(df_players, './data/player_stats.csv')

    if all_match_data:
        df_matches = pd.DataFrame(all_match_data)
        save_to_csv(df_matches, './data/matches.csv')


if __name__ == "__main__":
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
    main()
