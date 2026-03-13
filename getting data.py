import os
import requests
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path

# Load .env from the same folder as this script
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path, override=True)

BASE_URL = "https://api.openaq.org/v3"


def get_api_key():
    # Accept a common typo as fallback to reduce setup friction.
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
    return {
        "X-API-Key": api_key,
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }

def get_locations(country="IN", limit=100, page=1, headers=None):
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
    all_locations = []
    for page in range(1, max_pages + 1):
        payload = get_locations(country=country, limit=per_page, page=page, headers=headers)
        results = payload.get("results", [])
        if not results:
            break

        all_locations.extend(results)
        if len(results) < per_page:
            break

    return all_locations


def filter_locations_by_city(locations, city):
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

def get_latest_by_location(location_id, headers=None):
    url = f"{BASE_URL}/locations/{location_id}/latest"
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()

def flatten_latest_results(location_name, data):
    rows = []
    for item in data.get("results", []):
        rows.append({
            "location_id": item.get("locationId") or item.get("locationsId"),
            "sensor_id": item.get("sensorsId"),
            "location_name": location_name,
            "datetime_utc": item.get("datetime", {}).get("utc"),
            "datetime_local": item.get("datetime", {}).get("local"),
            "parameter": item.get("parameter", {}).get("name"),
            "parameter_display": item.get("parameter", {}).get("displayName"),
            "units": item.get("parameter", {}).get("units"),
            "value": item.get("value"),
            "coordinates_latitude": item.get("coordinates", {}).get("latitude"),
            "coordinates_longitude": item.get("coordinates", {}).get("longitude"),
            "country_code": item.get("country", {}).get("code"),
            "country_name": item.get("country", {}).get("name"),
            "is_mobile": item.get("isMobile"),
            "is_monitor": item.get("isMonitor")
        })
    return rows


def clean_and_preprocess_data(df):
    cleaned = df.copy()

    # Normalize string fields to reduce category noise.
    text_columns = [
        "location_name",
        "parameter",
        "parameter_display",
        "units",
        "country_code",
        "country_name",
    ]
    for col in text_columns:
        if col in cleaned.columns:
            cleaned[col] = cleaned[col].astype("string").str.strip()

    # Parse numeric fields and keep them as nullable dtypes.
    numeric_columns = [
        "location_id",
        "sensor_id",
        "value",
        "coordinates_latitude",
        "coordinates_longitude",
    ]
    for col in numeric_columns:
        if col in cleaned.columns:
            cleaned[col] = pd.to_numeric(cleaned[col], errors="coerce")

    if "location_id" in cleaned.columns:
        cleaned["location_id"] = cleaned["location_id"].astype("Int64")
    if "sensor_id" in cleaned.columns:
        cleaned["sensor_id"] = cleaned["sensor_id"].astype("Int64")

    # Parse datetimes for downstream time-series analysis.
    if "datetime_utc" in cleaned.columns:
        cleaned["datetime_utc"] = pd.to_datetime(cleaned["datetime_utc"], errors="coerce", utc=True)
    if "datetime_local" in cleaned.columns:
        cleaned["datetime_local"] = pd.to_datetime(cleaned["datetime_local"], errors="coerce", utc=False)

    # Remove unusable rows.
    cleaned = cleaned.dropna(subset=["value", "datetime_utc", "location_id"])

    # Remove invalid negatives for parameters that should not be below zero.
    non_negative_params = {
        "pm1",
        "pm10",
        "pm25",
        "o3",
        "co",
        "no",
        "no2",
        "nox",
        "so2",
        "bc",
        "co2",
        "relativehumidity",
        "wind_speed",
    }
    if "parameter" in cleaned.columns:
        param_lower = cleaned["parameter"].str.lower()
        invalid_negative = param_lower.isin(non_negative_params) & (cleaned["value"] < 0)
        cleaned = cleaned.loc[~invalid_negative].copy()

    # Keep coordinate ranges valid when available.
    if "coordinates_latitude" in cleaned.columns:
        cleaned = cleaned[
            cleaned["coordinates_latitude"].isna()
            | cleaned["coordinates_latitude"].between(-90, 90)
        ]
    if "coordinates_longitude" in cleaned.columns:
        cleaned = cleaned[
            cleaned["coordinates_longitude"].isna()
            | cleaned["coordinates_longitude"].between(-180, 180)
        ]

    # Add reusable time features.
    cleaned["date"] = cleaned["datetime_utc"].dt.date
    cleaned["hour_utc"] = cleaned["datetime_utc"].dt.hour
    cleaned["month"] = cleaned["datetime_utc"].dt.to_period("M").astype("string")
    cleaned["day_of_week"] = cleaned["datetime_utc"].dt.day_name()

    cleaned["has_coordinates"] = cleaned[["coordinates_latitude", "coordinates_longitude"]].notna().all(axis=1)

    cleaned = cleaned.drop_duplicates(subset=["location_id", "sensor_id", "datetime_utc", "value"]).reset_index(drop=True)

    return cleaned

def main():
    try:
        api_key = get_api_key()
    except ValueError as e:
        print(e)
        return

    headers = build_headers(api_key)

    city = os.getenv("OPENAQ_CITY", "").strip() or None
    country = os.getenv("OPENAQ_COUNTRY", "IN").strip().upper()
    per_page = int(os.getenv("OPENAQ_PAGE_LIMIT", "100"))
    max_pages = int(os.getenv("OPENAQ_MAX_PAGES", "50"))
    max_locations = int(os.getenv("OPENAQ_MAX_LOCATIONS", "0"))

    city_text = city if city else "ALL"
    print(f"Fetching locations for city={city_text}, country={country} ...")

    try:
        # OpenAQ v3 reliably supports `iso` for country filtering.
        locations = get_all_locations(country=country, per_page=per_page, max_pages=max_pages, headers=headers)
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 401:
            details = ""
            try:
                details = e.response.text.strip()
            except Exception:
                details = ""
            print(
                "Authentication failed (401 Unauthorized). "
                "OpenAQ rejected the key in X-API-Key. "
                "Use a valid active OpenAQ v3 key in .env as OPENAQ_API_KEY=<key>, "
                "then run again."
            )
            if details:
                print(f"API response: {details}")
            return
        print(f"Failed to fetch locations: {e}")
        return
    except requests.RequestException as e:
        print(f"Failed to fetch locations: {e}")
        return

    city_matched_locations = filter_locations_by_city(locations, city)
    if city:
        locations = city_matched_locations

    if max_locations > 0:
        locations = locations[:max_locations]

    print(f"Resolved {len(locations)} location(s) for processing.")

    if not locations:
        print("No locations found.")
        return

    all_rows = []

    for loc in locations:
        location_id = loc.get("id")
        location_name = loc.get("name", "Unknown")

        print(f"Fetching latest measurements for location {location_name} (ID: {location_id})")

        try:
            latest_json = get_latest_by_location(location_id, headers=headers)
            rows = flatten_latest_results(location_name, latest_json)
            all_rows.extend(rows)
        except requests.RequestException as e:
            print(f"Skipping {location_name}: {e}")

    df = pd.DataFrame(all_rows)

    if df.empty:
        print("No measurement data collected.")
        return

    df.to_csv("openaq_raw_data.csv", index=False)
    print("\nSaved raw data to openaq_raw_data.csv")

    cleaned_df = clean_and_preprocess_data(df)
    cleaned_df.to_csv("openaq_cleaned_data.csv", index=False)
    print("Saved cleaned data to openaq_cleaned_data.csv")
    print(f"Raw rows: {len(df)} | Cleaned rows: {len(cleaned_df)}")
    print(cleaned_df.head())

if __name__ == "__main__":
    main()