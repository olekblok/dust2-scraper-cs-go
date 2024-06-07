
# Match Data Scraper

This Python web scraper extracts match, map, and player statistics from `dust2.us`, saving the data into CSV files for analysis.

## Features

- Fetches match details, including teams, scores, and dates.
- Gathers map statistics, such as map names and scores.
- Extracts player data, including kills, deaths, assists, ADR, KAST, and rating.
- Utilizes threading for parallel requests, enhancing performance.
- Normalizes dates and scores for consistency.

## Requirements

- Python 3.8+
- `requests`
- `beautifulsoup4`
- `pandas`

## Usage

1. Clone the repository and navigate to it.
2. Create a virtual environment: `python3 -m venv venv` (or `venv\Scripts\activate` on Windows).
3. Install dependencies: `pip install -r requirements.txt`.
4. Ensure a `ua_list.txt` file exists with user agent strings in `./lists`.
5. Run the scraper: `python main.py`.

## Configuration

- Adjust the match ID range in `main()` to scrape different matches.

## File Structure

- `main.py`: Main script for running the scraper.
- `lists/ua_list.txt`: Text file containing user agent strings.
- `maps.csv`: Map data output file.
- `player_stats.csv`: Player statistics output file.
- `matches.csv`: Match details output file.

## Contributing

Feel free to fork the repository and submit pull requests.

## License

MIT License. See `LICENSE` for more details.
