import geopandas as gpd
import folium

# Read shapefile
gdf = gpd.read_file("Admin2.shp")

# Convert to GeoJSON
gdf.to_file("india_states.geojson", driver="GeoJSON")

# Create base map
m = folium.Map(
    location=[22.5, 79],
    zoom_start=5,
    tiles="CartoDB positron"
)

# Add state layer
folium.GeoJson(
    "india_states.geojson",
    name="States",
    zoom_on_click=True,
    style_function=lambda x: {
        "fillOpacity": 0,
        "color": "black",
        "weight": 1
    },
    highlight_function=lambda x: {
        "fillColor": "yellow",
        "fillOpacity": 0.4
    },
    tooltip=folium.GeoJsonTooltip(
        fields=["ST_NM"],
        aliases=["State:"]
    )
).add_to(m)

# Save map
m.save("india_state_zoom_map.html")

print("Map created successfully → india_state_zoom_map.html")