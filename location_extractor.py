import pandas as pd
from openaq import OpenAQ as aq

# -----------------------------
# API setup
# -----------------------------

#Create an OpenAQ account and use your API key here
api = aq(api_key="7your api key")

locations = api.locations.list(
    countries_id=9,
    limit=1000
)

# Convert to DataFrame
df = pd.DataFrame([loc for loc in locations.results])

# -----------------------------
# Required sensor filter
# -----------------------------
required = {"co", "no2", "pm10", "pm25", "o3", "so2"}

def has_all_required(sensor_list):
    try:
        params = {s["parameter"]["name"] for s in sensor_list}
        return required.issubset(params)
    except:
        return False

# -----------------------------
# Time thresholds
# -----------------------------
now_utc = pd.Timestamp.now(tz="UTC")
recent_threshold = now_utc - pd.Timedelta(days=1)   # data available today
history_threshold = now_utc - pd.DateOffset(years=1) # change data availability threshold here

# -----------------------------
# Filtering
# -----------------------------
filtered_df = df[df["sensors"].apply(has_all_required)]
filtered_df = filtered_df[filtered_df["is_monitor"] == True]

# Convert datetime columns
filtered_df["datetime_last_utc"] = pd.to_datetime(
    filtered_df["datetime_last"].str["utc"],
    errors="coerce",
    utc=True
)

filtered_df["datetime_first_utc"] = pd.to_datetime(
    filtered_df["datetime_first"].str["utc"],
    errors="coerce",
    utc=True
)

# Apply time filters
filtered_df = filtered_df[
    (filtered_df["datetime_last_utc"] >= recent_threshold) &
    (filtered_df["datetime_first_utc"] <= history_threshold)
]

# -----------------------------
# Save full dataframe
# -----------------------------
print(filtered_df.shape)
filtered_df.to_csv("full_location_info.csv", index=False)

# -----------------------------
# Create reduced dataframe
# -----------------------------
location_df = filtered_df[["name", "coordinates", "sensors"]].copy()

# Optional: flatten coordinates into lat/lon columns (recommended)
location_df["latitude"] = location_df["coordinates"].apply(lambda x: x.get("latitude") if isinstance(x, dict) else None)
location_df["longitude"] = location_df["coordinates"].apply(lambda x: x.get("longitude") if isinstance(x, dict) else None)

# Save reduced dataframe
print(location_df.shape)
location_df.to_csv("location.csv", index=False)
