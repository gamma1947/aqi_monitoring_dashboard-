import pandas as pd
from openaq import OpenAQ
import time

# -----------------------------
# API Setup
# -----------------------------
api = OpenAQ(api_key="703567a9b637449ffb21acf96659f4a58d69761753e4e1082ab6fb84232652de")

# -----------------------------
# Step 1: Get locations (India)
# -----------------------------
locations = api.locations.list(
    countries_id=9,
    limit=1000
)

df = pd.DataFrame([loc for loc in locations.results])

# Keep only monitors with coordinates
df = df[df["is_monitor"] == True]
df = df.dropna(subset=["coordinates"])

# Extract lat/lon
df["latitude"] = df["coordinates"].apply(lambda x: x.get("latitude"))
df["longitude"] = df["coordinates"].apply(lambda x: x.get("longitude"))

# -----------------------------
# Pollutants to fetch
# -----------------------------
parameters = ["pm25", "pm10", "no2", "so2", "co", "o3"]

# -----------------------------
# Step 2: Fetch measurements
# -----------------------------
data = []

for i, row in df.iterrows():
    location_name = row["name"]

    entry = {
        "name": location_name,
        "latitude": row["latitude"],
        "longitude": row["longitude"]
    }

    for param in parameters:
        try:
            res = api.measurements.list(
                location=location_name,
                parameter=param,
                limit=1,
                sort="desc"
            )

            if res.results:
                entry[param] = res.results[0]["value"]
            else:
                entry[param] = None

        except:
            entry[param] = None

        time.sleep(0.2)  # avoid API rate limits

    data.append(entry)

    print(f"Processed: {location_name}")

# -----------------------------
# Step 3: Create DataFrame
# -----------------------------
final_df = pd.DataFrame(data)

# -----------------------------
# Step 4: Clean data
# -----------------------------
# Keep rows with at least one pollutant
final_df = final_df.dropna(
    subset=["pm25", "pm10", "no2", "so2", "co", "o3"],
    how="all"
)

# -----------------------------
# Save
# -----------------------------
final_df.to_csv("india_aqi_data.csv", index=False)

print("Saved → india_aqi_data.csv")