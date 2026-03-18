import json

with open("india_states.geojson") as f:
    data = json.load(f)

print(len(data["features"]))