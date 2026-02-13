"""Fetch F1 historical data from Jolpica API (Ergast replacement) and save as CSVs."""
import requests
import pandas as pd
import time
import os

BASE_URL = "https://api.jolpi.ca/ergast/f1"
DATA_DIR = "data/f1"
os.makedirs(DATA_DIR, exist_ok=True)

def fetch_json(url, retries=4):
    """Fetch JSON with exponential backoff retry."""
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, ValueError) as e:
            if attempt < retries - 1:
                wait = 2 ** (attempt + 1)
                print(f"  Retry {attempt+1} after {wait}s: {e}")
                time.sleep(wait)
            else:
                print(f"  FAILED after {retries} attempts: {e}")
                return None

def fetch_race_results(years):
    """Fetch race results for given years."""
    all_rows = []
    for year in years:
        print(f"Fetching {year} race results...")
        data = fetch_json(f"{BASE_URL}/{year}/results.json?limit=1000")
        if not data:
            continue
        races = data['MRData']['RaceTable']['Races']
        for race in races:
            rnd = int(race['round'])
            circuit_id = race['Circuit']['circuitId']
            circuit_name = race['Circuit']['circuitName']
            race_name = race['raceName']
            date = race['date']
            for r in race['Results']:
                row = {
                    'year': year, 'round': rnd, 'race_name': race_name,
                    'circuit_id': circuit_id, 'circuit_name': circuit_name, 'date': date,
                    'driver_id': r['Driver']['driverId'],
                    'driver': f"{r['Driver']['givenName']} {r['Driver']['familyName']}",
                    'driver_code': r['Driver'].get('code', ''),
                    'constructor_id': r['Constructor']['constructorId'],
                    'constructor': r['Constructor']['name'],
                    'grid': int(r['grid']),
                    'position': int(r['position']) if r['position'].isdigit() else None,
                    'position_text': r['positionText'],
                    'points': float(r['points']),
                    'laps': int(r['laps']),
                    'status': r['status'],
                    'fastest_lap_rank': r.get('FastestLap', {}).get('rank', ''),
                }
                all_rows.append(row)
        time.sleep(0.5)  # Rate limiting
    return pd.DataFrame(all_rows)

def fetch_qualifying(years):
    """Fetch qualifying results for given years."""
    all_rows = []
    for year in years:
        print(f"Fetching {year} qualifying...")
        data = fetch_json(f"{BASE_URL}/{year}/qualifying.json?limit=1000")
        if not data:
            continue
        races = data['MRData']['RaceTable']['Races']
        for race in races:
            rnd = int(race['round'])
            circuit_id = race['Circuit']['circuitId']
            for q in race['QualifyingResults']:
                row = {
                    'year': year, 'round': rnd, 'circuit_id': circuit_id,
                    'driver_id': q['Driver']['driverId'],
                    'constructor_id': q['Constructor']['constructorId'],
                    'quali_pos': int(q['position']),
                    'q1': q.get('Q1', ''),
                    'q2': q.get('Q2', ''),
                    'q3': q.get('Q3', ''),
                }
                all_rows.append(row)
        time.sleep(0.5)
    return pd.DataFrame(all_rows)

def fetch_constructors():
    """Fetch all constructors."""
    print("Fetching constructors...")
    data = fetch_json(f"{BASE_URL}/constructors.json?limit=500")
    if not data:
        return pd.DataFrame()
    constructors = data['MRData']['ConstructorTable']['Constructors']
    return pd.DataFrame([{
        'constructor_id': c['constructorId'],
        'constructor': c['name'],
        'nationality': c['nationality'],
    } for c in constructors])

def fetch_circuits():
    """Fetch all circuits."""
    print("Fetching circuits...")
    data = fetch_json(f"{BASE_URL}/circuits.json?limit=200")
    if not data:
        return pd.DataFrame()
    circuits = data['MRData']['CircuitTable']['Circuits']
    return pd.DataFrame([{
        'circuit_id': c['circuitId'],
        'circuit_name': c['circuitName'],
        'locality': c['Location']['locality'],
        'country': c['Location']['country'],
    } for c in circuits])

if __name__ == '__main__':
    years = list(range(2014, 2026))

    # Fetch all data
    results_df = fetch_race_results(years)
    quali_df = fetch_qualifying(years)
    constructors_df = fetch_constructors()
    circuits_df = fetch_circuits()

    # Save to CSV
    results_df.to_csv(f"{DATA_DIR}/race_results.csv", index=False)
    quali_df.to_csv(f"{DATA_DIR}/qualifying.csv", index=False)
    constructors_df.to_csv(f"{DATA_DIR}/constructors.csv", index=False)
    circuits_df.to_csv(f"{DATA_DIR}/circuits.csv", index=False)

    print(f"\n=== Data Summary ===")
    print(f"Race results: {len(results_df)} rows, {results_df['year'].nunique()} years")
    print(f"Qualifying:   {len(quali_df)} rows")
    print(f"Constructors: {len(constructors_df)}")
    print(f"Circuits:     {len(circuits_df)}")
    print(f"\nFiles saved to {DATA_DIR}/")
