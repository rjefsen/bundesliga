#!/usr/bin/env python3
"""
Parse Champions League match results from openfootball text files.
Downloads data from GitHub and outputs CSV.
"""

import re
import csv
import urllib.request

URLS = {
    "2024-25": "https://raw.githubusercontent.com/openfootball/champions-league/master/2024-25/cl.txt",
    "2023-24": "https://raw.githubusercontent.com/openfootball/champions-league/master/2023-24/cl.txt",
    "2022-23": "https://raw.githubusercontent.com/openfootball/champions-league/master/2022-23/cl.txt",
    "2021-22": "https://raw.githubusercontent.com/openfootball/champions-league/master/2021-22/cl.txt",
    "2020-21": "https://raw.githubusercontent.com/openfootball/champions-league/master/2020-21/cl.txt",
    "2019-20": "https://raw.githubusercontent.com/openfootball/champions-league/master/2019-20/cl.txt",
    "2018-19": "https://raw.githubusercontent.com/openfootball/champions-league/master/2018-19/cl.txt",
}

MONTH_MAP = {
    "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
    "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
    "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
}

# Match result pattern:
#   TeamA (COUNTRY)  v  TeamB (COUNTRY)   3-1  (halftime)
# or with extra time / penalties info
MATCH_PATTERN = re.compile(
    r'^\s+'
    r'(?:\d{1,2}\.\d{2}\s+)?'        # optional time like 18.45 or 21.00
    r'(.+?)\s+'                        # home team (greedy but we'll trim)
    r'v\s+'                            # 'v' separator
    r'(.+?)\s+'                        # away team
    r'(\d+)-(\d+)'                     # score home-away
    r'(?:\s+.*)?$'                     # optional rest (halftime, aet, pen)
)

# More precise pattern that handles team names with country codes
MATCH_PATTERN2 = re.compile(
    r'^\s+'
    r'(?:\d{1,2}\.\d{2}\s+)?'            # optional time
    r'(.*?\([A-Z]{3}\))\s+'              # home team ending with (XXX)
    r'v\s+'                               # 'v' separator
    r'(.*?\([A-Z]{3}\))\s+'              # away team ending with (XXX)
    r'(\d+)-(\d+)'                        # score
    r'(?:\s+.*)?$'                        # rest of line
)

# Date patterns
# "  Tue Sep/17 2024" or "  Wed Sep/18" or "  [Tue Sep/17]"
DATE_WITH_YEAR = re.compile(
    r'^\s+(?:\[)?(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+([A-Z][a-z]{2})/(\d{1,2})(?:\s+(\d{4}))?(?:\])?'
)


def parse_file(season, text):
    """Parse a single season's text file and return list of match dicts."""
    matches = []
    lines = text.split('\n')

    current_date = None
    current_year = None

    # Determine the base year from the season string
    # e.g., "2024-25" -> starts in 2024
    base_year = int(season.split('-')[0])

    for line in lines:
        # Check for date lines
        date_match = DATE_WITH_YEAR.match(line)
        if date_match:
            month_str = date_match.group(1)
            day = int(date_match.group(2))
            explicit_year = date_match.group(3)

            month_num = MONTH_MAP.get(month_str)
            if month_num is None:
                continue

            if explicit_year:
                current_year = int(explicit_year)
            else:
                # Infer year from month: Aug-Dec = base_year, Jan-Jul = base_year+1
                month_int = int(month_num)
                if month_int >= 7:  # Jul onwards = first year of season
                    current_year = base_year
                else:  # Jan-Jun = second year
                    current_year = base_year + 1

            current_date = f"{current_year}-{month_num}-{day:02d}"
            continue

        # Check for match lines - try the more precise pattern first
        m = MATCH_PATTERN2.match(line)
        if m and current_date:
            home_team = m.group(1).strip()
            away_team = m.group(2).strip()
            home_goals = int(m.group(3))
            away_goals = int(m.group(4))

            matches.append({
                'season': season,
                'date': current_date,
                'home_team': home_team,
                'away_team': away_team,
                'home_goals': home_goals,
                'away_goals': away_goals,
            })

    return matches


def main():
    all_matches = []

    for season, url in sorted(URLS.items()):
        print(f"Downloading {season}...")
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req) as response:
            text = response.read().decode('utf-8')

        print(f"  Parsing {season}...")
        matches = parse_file(season, text)
        print(f"  Found {len(matches)} matches")
        all_matches.extend(matches)

    # Sort by season, then date
    all_matches.sort(key=lambda m: (m['season'], m['date']))

    # Write CSV
    output_path = "/home/user/bundesliga/src/data/cl/cl_results_history.csv"
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['season', 'date', 'home_team', 'away_team', 'home_goals', 'away_goals'])
        for m in all_matches:
            writer.writerow([m['season'], m['date'], m['home_team'], m['away_team'], m['home_goals'], m['away_goals']])

    print(f"\nTotal matches: {len(all_matches)}")
    print(f"Written to: {output_path}")

    # Print per-season counts
    from collections import Counter
    season_counts = Counter(m['season'] for m in all_matches)
    for s in sorted(season_counts):
        print(f"  {s}: {season_counts[s]} matches")


if __name__ == '__main__':
    main()
