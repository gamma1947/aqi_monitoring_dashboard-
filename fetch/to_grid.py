import numpy as np
import pandas as pd
import xarray as xr
from scipy.spatial import cKDTree

# =========================================================
# 1. CONFIG
# =========================================================
DATA_DIR = "data/"
VARIABLES = ["pm25", "pm10", "o3"]

lat_min, lat_max = 28.05, 29.18
lon_min, lon_max = 76.5, 78.00
RES = 0.05

# =========================================================
# 2. GRID SETUP
# =========================================================
lat_grid = np.arange(lat_min, lat_max, RES)
lon_grid = np.arange(lon_min, lon_max, RES)

lon_mesh, lat_mesh = np.meshgrid(lon_grid, lat_grid)
xy_grid = np.column_stack([lat_mesh.ravel(), lon_mesh.ravel()])

# =========================================================
# 3. LOAD STATION METADATA
# =========================================================
delhi_location = pd.read_csv(f"{DATA_DIR}/delhi_location.csv")
delhi_location['id'] = delhi_location['id'].astype(str)

# =========================================================
# 4. IDW FUNCTION
# =========================================================
def idw_interpolation(xy_stations, values, xy_grid, k=4, p=2):
    n_points = len(xy_stations)

    if n_points == 0:
        return np.full(len(xy_grid), np.nan)

    k = min(k, n_points)

    tree = cKDTree(xy_stations)
    dists, idxs = tree.query(xy_grid, k=k)

    if k == 1:
        dists = dists[:, None]
        idxs = idxs[:, None]

    weights = 1 / (dists**p + 1e-12)
    weighted_vals = np.sum(weights * values[idxs], axis=1)
    norm = np.sum(weights, axis=1)

    return weighted_vals / norm

# =========================================================
# 5. PROCESS FUNCTION
# =========================================================
def process_variable(var_name):
    print(f"\nProcessing {var_name}...")

    # -----------------------------
    # Load data
    # -----------------------------
    df = pd.read_csv(f"{DATA_DIR}/{var_name}.csv")

    # -----------------------------
    # Clean datetime
    # -----------------------------
    df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
    df = df.dropna(subset=['datetime'])
    df = df.drop_duplicates(subset=['datetime'])

    # -----------------------------
    # Convert to numeric
    # -----------------------------
    station_cols = [c for c in df.columns if c != 'datetime']
    df[station_cols] = df[station_cols].apply(pd.to_numeric, errors='coerce')

    # -----------------------------
    # Set index
    # -----------------------------
    df = df.set_index('datetime').sort_index()

    # -----------------------------
    # Time binning (1h)
    # -----------------------------
    df_hourly = df.resample('1h').mean()

    # -----------------------------
    # Align stations (IMPORTANT FIX)
    # -----------------------------
    common_ids = [c for c in df_hourly.columns if c in delhi_location['id'].values]

    df_hourly = df_hourly[common_ids]
    meta = delhi_location.set_index('id').loc[common_ids]

    xy_stations_all = meta[['lat', 'lon']].values

    # -----------------------------
    # Interpolation loop
    # -----------------------------
    times = df_hourly.index
    nt = len(times)
    nlat = len(lat_grid)
    nlon = len(lon_grid)

    grid_data = np.zeros((nt, nlat, nlon), dtype=np.float32)

    for t_idx, t in enumerate(times):
        values = df_hourly.loc[t].values.astype(float)

        mask = ~np.isnan(values)

        xy_valid = xy_stations_all[mask]
        val_valid = values[mask]

        if len(val_valid) == 0:
            grid_data[t_idx] = np.nan
            continue

        interp = idw_interpolation(xy_valid, val_valid, xy_grid)
        grid_data[t_idx] = interp.reshape(nlat, nlon)

        if t_idx % 100 == 0:
            print(f"{var_name}: timestep {t_idx}/{nt}")

    # -----------------------------
    # Fix time (remove timezone)
    # -----------------------------
    if times.tz is not None:
        time_vals = times.tz_convert(None).to_numpy()
    else:
        time_vals = times.to_numpy()

    # -----------------------------
    # Create xarray
    # -----------------------------
    ds = xr.DataArray(
        data=grid_data,
        dims=("time", "lat", "lon"),
        coords={
            "time": ("time", time_vals),
            "lat": ("lat", lat_grid.astype(np.float32)),
            "lon": ("lon", lon_grid.astype(np.float32)),
        },
        name=var_name
    )

    # -----------------------------
    # Save
    # -----------------------------
    out_path = f"data/{var_name}_grid.nc"
    ds.to_netcdf(out_path)

    print(f"Saved → {out_path}")

    return ds

# =========================================================
# 6. RUN ALL VARIABLES → COMBINE INTO ONE FILE
# =========================================================
if __name__ == "__main__":
    data_vars = {}

    for var in VARIABLES:
        ds = process_variable(var)
        data_vars[var] = ds

    # -----------------------------
    # Combine into one Dataset
    # -----------------------------
    combined_ds = xr.Dataset(data_vars)

    # -----------------------------
    # Save single file
    # -----------------------------
    combined_ds.to_netcdf("data/pollution_grid.nc")

    print("\nSaved combined dataset → pollution_grid.nc")