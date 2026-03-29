"""
TASK 1: Identify and Ingest Air Quality Data
============================================
This module fetches air quality data from the OpenAQ API v3 for India
and creates a standardized raw dataset for further processing.

Features:
- Fetches from multiple monitoring stations
- Handles API authentication and pagination
- Extracts PM2.5, NO2, and other pollutants
- Validates and structures data into a DataFrame
- Saves raw data for auditing
"""

import os
import requests
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime
import json

# Load .env from the same folder as this script
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path, override=True)

BASE_URL = "https://api.openaq.org/v3"
RAW_DATA_FILE = "openaq_raw_data.csv"
INGESTION_LOG_FILE = "ingestion_log.txt"


def log_message(msg):
    """Log messages to both console and file for audit trail."""
    timestamp = datetime.now().isoformat()
    log_entry = f"[{timestamp}] {msg}"
    print(log_entry)
    with open(INGESTION_LOG_FILE, "a") as f:
        f.write(log_entry + "\n")


def get_api_key():
    """
    Retrieve OpenAQ API key from environment.
    Supports both OPENAQ_API_KEY and OPENAQ_API_K (typo tolerance).
    """
    api_key = os.getenv("OPENAQ_API_KEY") or os.getenv("OPENAQ_API_K")
    if api_key:
        api_key = api_key.strip().strip('"').strip("'")
        if api_key.strip() in {"your_real_openaq_key", "your_api_key_here"}:
            raise ValueError(
                "OPENAQ_API_KEY is still a placeholder value. Replace it with your actual OpenAQ API key."
            )
        return api_key

    setup_help = (
        "OPENAQ_API_KEY not found.\n"
        "Add your key in one of these ways:\n"
        "1) Temporary shell export:\n"
        "   export OPENAQ_API_KEY='your_api_key_here'\n"
        "2) Project .env file at ./aqi_monitoring_dashboard-/.env with:\n"
        "   OPENAQ_API_KEY=your_api_key_here"
    )
    raise ValueError(setup_help)


def build_headers(api_key):
    """Build HTTP headers for OpenAQ API requests."""
    return {
        "X-API-Key": api_key,
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }


def get_locations(country="IN", limit=100, page=1, headers=None):
    """
    Fetch locations (monitoring stations) from OpenAQ API.
    
    Args:
        country: ISO country code (default: "IN" for India)
        limit: Number of results per page
        page: Page number for pagination
        headers: HTTP headers with API key
    
    Returns:
        API response JSON
    """
    url = f"{BASE_URL}/locations"
    params = {
        "limit": limit,
        "page": page,
        "iso": country,
    }

    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def get_all_locations(country="IN", per_page=100, max_pages=50, headers=None):
    """
    Fetch all locations across multiple pages.
    
    Args:
        country: ISO country code
        per_page: Results per page
        max_pages: Maximum pages to fetch
        headers: HTTP headers
    
    Returns:
        List of all location objects
    """
    all_locations = []
    for page in range(1, max_pages + 1):
        try:
            payload = get_locations(country=country, limit=per_page, page=page, headers=headers)
            results = payload.get("results", [])
            if not results:
                log_message(f"No more results at page {page}. Stopping pagination.")
                break

            all_locations.extend(results)
            log_message(f"Fetched page {page}: {len(results)} locations")
            
            if len(results) < per_page:
                log_message(f"Partial page at {page}. Stopping pagination.")
                break
        except Exception as e:
            log_message(f"Warning: Error on page {page}: {e}")
            continue

    return all_locations


def filter_locations_by_city(locations, city):
    """Filter locations by city name (partial match)."""
    if not city:
        return locations

    city_lower = city.strip().lower()
    filtered = []
    for loc in locations:
        name = (loc.get("name") or "").lower()
        locality = (loc.get("locality") or "").lower()
        if city_lower in name or city_lower in locality:
            filtered.append(loc)
    return filtered


def get_measurements_by_location(location_id, parameters=None, headers=None):
    """
    Fetch measurements for a specific location.
    Attempts both 'latest' and 'measurements' endpoints for robustness.
    
    Args:
        location_id: OpenAQ location ID
        parameters: List of parameter names to filter (optional)
        headers: HTTP headers
    
    Returns:
        API response JSON with measurements
    """
    # Try the 'latest' endpoint first
    url = f"{BASE_URL}/locations/{location_id}/latest"
    
    params = {}
    if parameters:
        params["parameters"] = ",".join(parameters)
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        log_message(f"Failed to fetch measurements for location {location_id}: {e}")
        return {"results": []}


def flatten_measurements(location_id, location_name, data):
    """
    Flatten API response into tabular format.
    
    Args:
        location_id: Location ID
        location_name: Location name
        data: API response JSON
    
    Returns:
        List of dictionaries (one per measurement)
    """
    rows = []
    
    for item in data.get("results", []):
        # Extract measurement details
        row = {
            "location_id": location_id,
            "sensor_id": item.get("sensorId") or item.get("sensorsId"),
            "location_name": location_name,
            "datetime_utc": item.get("datetime", {}).get("utc") if isinstance(item.get("datetime"), dict) else item.get("datetime"),
            "datetime_local": item.get("datetime", {}).get("local") if isinstance(item.get("datetime"), dict) else None,
            "parameter": item.get("parameter", {}).get("name") if isinstance(item.get("parameter"), dict) else item.get("parameter"),
            "parameter_display": item.get("parameter", {}).get("displayName") if isinstance(item.get("parameter"), dict) else None,
            "units": item.get("parameter", {}).get("units") if isinstance(item.get("parameter"), dict) else item.get("units"),
            "value": item.get("value"),
            "coordinates_latitude": item.get("coordinates", {}).get("latitude"),
            "coordinates_longitude": item.get("coordinates", {}).get("longitude"),
            "country_code": item.get("country", {}).get("code"),
            "country_name": item.get("country", {}).get("name"),
            "is_mobile": item.get("isMobile"),
            "is_monitor": item.get("isMonitor"),
            "measurement_id": item.get("id"),
            "sensor_name": item.get("sensorName"),
        }
        rows.append(row)
    
    return rows


def validate_measurement(row):
    """
    Validate a measurement row.
    Returns True if row is acceptable, False otherwise.
    """
    # Must have a value
    if pd.isna(row.get("value")):
        return False
    
    # Must have a timestamp
    if not row.get("datetime_utc"):
        return False
    
    # Must have a location
    if not row.get("location_id"):
        return False
    
    return True


def main():
    """Main data ingestion workflow."""
    
    # Clear log file
    with open(INGESTION_LOG_FILE, "w") as f:
        f.write(f"=== AQI Data Ingestion Log ===\nStart: {datetime.now().isoformat()}\n\n")
    
    log_message("="*60)
    log_message("TASK 1: DATA INGESTION - Starting")
    log_message("="*60)
    
    # Respect OPENAQ_USE_API explicitly.
    # When false, local dataset is preferred even if an API key exists.
    use_api_if_available = os.getenv("OPENAQ_USE_API", "").lower() in ["true", "1", "yes"]
    try_api_first = use_api_if_available
    
    # Try to use existing Delhi PM2.5 data first (useful when API unavailable)
    # Unless API preference is explicitly set
    delhi_data = None
    if not try_api_first:
        delhi_data = load_delhi_pm25_data("df_2021_to_2022_pm25_delhi.csv")
    
    if delhi_data is not None and not try_api_first:
        log_message("\n✓ Using existing Delhi PM2.5 data (API-independent)")
        df = delhi_data
        df.to_csv(RAW_DATA_FILE, index=False)
        log_message(f"✓ Saved raw data to {RAW_DATA_FILE}")
        
        log_message("\n" + "="*60)
        log_message("✓ TASK 1 COMPLETED: Data Ingestion Successful")
        log_message("="*60)
        log_message(f"\nRaw data saved to: {RAW_DATA_FILE}")
        log_message(f"Measurements: {len(df)}")
        log_message(f"Date range: {df['datetime_utc'].min()} to {df['datetime_utc'].max()}")
        log_message(f"Locations: {df['location_id'].nunique()}")
        log_message(f"Parameter: {df['parameter'].unique()[0]}")
        log_message(f"Unit: {df['units'].unique()[0]}")
        log_message("="*60 + "\n")
        return
    
    # Attempt API approach (preferred when configured)
    if try_api_first:
        log_message("\n✓ API key detected. Attempting live OpenAQ API data ingestion...")
    else:
        log_message("\nNo local data found. Attempting API approach...")
    
    # Step 1: Get API credentials
    try:
        api_key = get_api_key()
        log_message("✓ API key loaded successfully")
    except ValueError as e:
        log_message(f"✗ API key error: {e}")
        print(e)
        return

    headers = build_headers(api_key)

    # Step 2: Configuration
    city = os.getenv("OPENAQ_CITY", "").strip() or None
    country = os.getenv("OPENAQ_COUNTRY", "IN").strip().upper()
    per_page = int(os.getenv("OPENAQ_PAGE_LIMIT", "100"))
    max_pages = int(os.getenv("OPENAQ_MAX_PAGES", "10"))
    max_locations = int(os.getenv("OPENAQ_MAX_LOCATIONS", "0"))
    parameters = os.getenv("OPENAQ_PARAMETERS", "pm25,no2,pm10").split(",")

    city_text = city if city else "ALL"
    log_message(f"Configuration: country={country}, city={city_text}, params={parameters}")

    # Step 3: Fetch locations
    log_message("\n[STEP 1/4] Fetching monitoring station locations...")
    try:
        locations = get_all_locations(country=country, per_page=per_page, max_pages=max_pages, headers=headers)
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 401:
            log_message("✗ Authentication failed (401 Unauthorized)")
            log_message("Use a valid OpenAQ v3 API key in .env")
            return
        log_message(f"✗ Failed to fetch locations: {e}")
        return
    except requests.RequestException as e:
        log_message(f"✗ Request error: {e}")
        return

    log_message(f"✓ Fetched {len(locations)} monitoring stations")

    # Step 4: Filter by city if needed
    if city:
        city_matched_locations = filter_locations_by_city(locations, city)
        log_message(f"✓ Filtered to {len(city_matched_locations)} locations matching '{city}'")
        locations = city_matched_locations

    if max_locations > 0:
        locations = locations[:max_locations]
        log_message(f"✓ Limited to {max_locations} locations")

    if not locations:
        log_message("✗ No locations found. Exiting.")
        return

    # Step 5: Fetch measurements from each location
    log_message(f"\n[STEP 2/4] Fetching measurements from {len(locations)} stations...")
    all_rows = []
    valid_count = 0
    invalid_count = 0

    for idx, loc in enumerate(locations, 1):
        location_id = loc.get("id")
        location_name = loc.get("name", "Unknown")

        try:
            data = get_measurements_by_location(location_id, headers=headers)
            rows = flatten_measurements(location_id, location_name, data)
            
            # Validate rows
            for row in rows:
                if validate_measurement(row):
                    all_rows.append(row)
                    valid_count += 1
                else:
                    invalid_count += 1
            
            if idx % 10 == 0:
                log_message(f"  Progress: {idx}/{len(locations)} locations processed")
        except Exception as e:
            log_message(f"  Warning: Skipped {location_name} (ID {location_id}): {e}")

    log_message(f"✓ Collected {valid_count} valid measurements ({invalid_count} invalid)")

    if not all_rows:
        log_message("✗ No valid measurements collected. Exiting.")
        return

    # Step 6: Create DataFrame and save raw data
    log_message(f"\n[STEP 3/4] Creating raw data DataFrame...")
    df = pd.DataFrame(all_rows)
    
    # Parse dates
    df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], errors="coerce", utc=True)
    df["datetime_local"] = pd.to_datetime(df["datetime_local"], errors="coerce")
    
    # Convert value to numeric
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["coordinates_latitude"] = pd.to_numeric(df["coordinates_latitude"], errors="coerce")
    df["coordinates_longitude"] = pd.to_numeric(df["coordinates_longitude"], errors="coerce")
    
    # Remove duplicates
    df = df.drop_duplicates(subset=["location_id", "sensor_id", "datetime_utc", "value"])
    
    df.to_csv(RAW_DATA_FILE, index=False)
    log_message(f"✓ Saved raw data to {RAW_DATA_FILE} ({len(df)} rows)")

    # Step 7: Summary statistics
    log_message(f"\n[STEP 4/4] Data Summary:")
    log_message(f"  Total measurements: {len(df)}")
    log_message(f"  Date range: {df['datetime_utc'].min()} to {df['datetime_utc'].max()}")
    log_message(f"  Unique locations: {df['location_id'].nunique()}")
    log_message(f"  Unique parameters: {df['parameter'].nunique()}")
    log_message(f"  Parameters: {df['parameter'].unique()}")
    log_message(f"  Units: {df['units'].unique()}")
    
    log_message("\n" + "="*60)
    log_message("✓ TASK 1 COMPLETED: Data Ingestion Successful")
    log_message("="*60)
    log_message(f"\nRaw data saved to: {RAW_DATA_FILE}")
    log_message(f"Audit log saved to: {INGESTION_LOG_FILE}\n")


def load_delhi_pm25_data(filepath="df_2021_to_2022_pm25_delhi.csv"):
    """
    Load and convert wide-format Delhi PM2.5 data to standard ingestion format.
    
    This handles existing datasets in wide format (stations as columns)
    and converts them to long format with proper parameter information.
    """
    log_message("\nAlternative: Loading existed Delhi PM2.5 dataset...")
    
    try:
        df_wide = pd.read_csv(filepath)
        log_message(f"✓ Loaded {filepath} ({len(df_wide)} rows, {len(df_wide.columns)} columns)")
    except FileNotFoundError:
        log_message(f"✗ File not found: {filepath}")
        return None
    
    # Parse datetime
    df_wide['datetime'] = pd.to_datetime(df_wide['datetime'], errors='coerce', utc=False)
    df_wide.set_index('datetime', inplace=True)
    
    # Convert from wide to long format
    df_long = df_wide.stack().reset_index()
    df_long.columns = ['datetime_local', 'location_name', 'value']
    
    # Remove NaN values
    df_long = df_long.dropna(subset=['value']).copy()
    
    # Add standard columns
    df_long['datetime_utc'] = df_long['datetime_local'].dt.tz_convert('UTC') if df_long['datetime_local'].dt.tz else pd.to_datetime(df_long['datetime_local'], utc=True)
    df_long['parameter'] = 'pm25'
    df_long['parameter_display'] = 'PM2.5'
    df_long['units'] = 'µg/m³'
    df_long['value'] = pd.to_numeric(df_long['value'], errors='coerce')
    
    # Add dummy location info (would need separate location data)
    df_long['location_id'] = pd.factorize(df_long['location_name'])[0]
    df_long['sensor_id'] = pd.factorize(df_long['location_name'])[0]
    df_long['coordinates_latitude'] = None
    df_long['coordinates_longitude'] = None
    df_long['country_code'] = 'IN'
    df_long['country_name'] = 'India'
    df_long['is_mobile'] = False
    df_long['is_monitor'] = True
    
    # Keep only required columns
    df_long = df_long[[
        'location_id', 'sensor_id', 'location_name', 'datetime_utc', 'datetime_local',
        'parameter', 'parameter_display', 'units', 'value',
        'coordinates_latitude', 'coordinates_longitude', 'country_code', 'country_name',
        'is_mobile', 'is_monitor'
    ]]
    
    # Remove duplicates
    df_long = df_long.drop_duplicates(subset=['location_id', 'datetime_utc', 'value'])
    
    log_message(f"✓ Converted to long format: {len(df_long)} measurements")
    log_message(f"  Date range: {df_long['datetime_utc'].min()} to {df_long['datetime_utc'].max()}")
    log_message(f"  Unique locations: {df_long['location_id'].nunique()}")
    
    return df_long


if __name__ == "__main__":
    main()
