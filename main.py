import requests
import logging
import pandas as pd
from bs4 import BeautifulSoup
import re
import random
from concurrent.futures import ThreadPoolExecutor


def load_user_agents(file_path='./lists/ua_list.txt'):
    """Returning the user agent for request"""
    try:
        with open(file_path, 'r') as f:
            return f.read().splitlines()
    except Exception as e:
        logging.exception('Failed to load user agents:', exc_info=e)


def get_page(user_agents, page_url):
    """Sending get request to the page"""
    user_agent = random.choice(user_agents)
    headers = {'User-Agent': user_agent}
    try:
        page = requests.get(page_url, headers=headers)
        page.raise_for_status()
        logging.info("Successful request for " + page_url)
        return page_url, page.content
    except requests.RequestException as e:
        logging.exception(f"Connection error: {e} - URL: {page_url}")
        return page_url, None


def normalize_date(match_date):
    """Returns correct date format."""
    if match_date.startswith("May"):
        match = re.search(r'May \d{1,2}, \d{4}', match_date)
        if match:
            return match.group(0)
    else:
        pattern = r'(?:Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{1,2}, \d{4}'
        match = re.search(pattern, match_date)
        if match:
            return match.group(0)
    return match_date.strip()


def fetch_matches():
    """
    Function which scrapes and returns 
    data as a list about the data about matches
    """
    hello = None


def fetch_maps():
    """
    Function which scrapes and returns 
    data as a list about the data about maps
    """
    hello = None


def fetch_player_stats():
    """
    Function which scrapes and returns 
    data as a list about the data about player stats
    """
    hello = None


def fetch_data(page_url, content):
    if content is None:
        return
    try:
        match_id = re.search(r'/matches/(\d+)/', page_url).group(1)
        soup = BeautifulSoup(content, 'html.parser')
    except Exception as e:
        logging.exception(f"Error processing data: {e} - URL: {page_url}")


def main():
    page_urls = [f"http://www.dust2.us/matches/{i}/b8-vs-enterprise"
                 for i in range(2372719, 2372720)]
    user_agents = load_user_agents()

    with ThreadPoolExecutor(max_workers=10) as executor:
        results = executor.map(lambda url: get_page(user_agents, url), page_urls)

        # for page_url, content in results:
        #     fetch_data(page_url, content)


if __name__ == "__main__":
    main()
