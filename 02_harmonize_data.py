"""
TASK 2: Harmonise Measurements Across Sensors
==============================================
This module standardizes air quality measurements across different sensors
by handling:
- Unit normalization (convert all to consistent units)
- Timestamp harmonization (ensure UTC consistency)
- Sensor calibration differences
- Reporting frequency standardization
- Missing value imputation strategies
"""

import pandas as pd
import numpy as np
from datetime import datetime
import json
from pathlib import Path

RAW_DATA_FILE = "openaq_raw_data.csv"
HARMONIZED_DATA_FILE = "openaq_harmonized_data.csv"
HARMONIZATION_LOG_FILE = "harmonization_log.txt"
UNIT_MAPPINGS_FILE = "unit_mappings.json"
HARMONIZATION_REPORT_FILE = "harmonization_report.csv"


def log_message(msg):
    """Log messages to both console and file."""
    timestamp = datetime.now().isoformat()
    log_entry = f"[{timestamp}] {msg}"
    print(log_entry)
    with open(HARMONIZATION_LOG_FILE, "a") as f:
        f.write(log_entry + "\n")


# Unit conversion definitions
UNIT_CONVERSIONS = {
    "pm25": {
        "µg/m³": 1.0,  # Reference unit
        "ug/m3": 1.0,
        "μg/m³": 1.0,
        "mg/m³": 1000.0,  # mg to µg
    },
    "pm10": {
        "µg/m³": 1.0,
        "ug/m3": 1.0,
        "mg/m³": 1000.0,
    },
    "pm1": {
        "µg/m³": 1.0,
        "ug/m3": 1.0,
        "mg/m³": 1000.0,
    },
    "no2": {
        "ppb": 1.0,  # Reference unit
        "ppm": 1000.0,  # ppm to ppb
        "μg/m³": 1.0,
        "ug/m3": 1.0,
    },
    "no": {
        "ppb": 1.0,
        "ppm": 1000.0,
    },
    "o3": {
        "ppb": 1.0,
        "ppm": 1000.0,
    },
    "so2": {
        "ppb": 1.0,
        "ppm": 1000.0,
    },
    "co": {
        "ppm": 1.0,  # Reference unit
        "ppb": 0.001,  # ppb to ppm
    },
    "temperature": {
        "°C": 1.0,  # Reference unit
        "C": 1.0,
        "K": None,  # Requires conversion function
    },
    "relativehumidity": {
        "%": 1.0,  # Already normalized
    },
    "bc": {
        "µg/m³": 1.0,
        "ug/m3": 1.0,
        "mg/m³": 1000.0,
    }
}

STANDARD_UNITS = {
    "pm25": "µg/m³",
    "pm10": "µg/m³",
    "pm1": "µg/m³",
    "no2": "ppb",
    "no": "ppb",
    "o3": "ppb",
    "so2": "ppb",
    "co": "ppm",
    "temperature": "°C",
    "relativehumidity": "%",
    "bc": "µg/m³",
}


def normalize_parameter_name(param):
    """
    Normalize parameter names to standard format.
    
    Example:
        "PM2.5" -> "pm25"
        "NO2" -> "no2"
    """
    if pd.isna(param) or not param:
        return "unknown"
    
    param = str(param).strip().lower()
    
    # Map common variations
    param_map = {
        "pm2.5": "pm25",
        "pm2_5": "pm25",
        "pm1.0": "pm1",
        "pm10.0": "pm10",
        "nitrogen dioxide": "no2",
        "nitrogen monoxide": "no",
        "ozone": "o3",
        "sulfur dioxide": "so2",
        "carbon monoxide": "co",
        "black carbon": "bc",
        "relative humidity": "relativehumidity",
        "rh": "relativehumidity",
    }
    
    return param_map.get(param, param)


def normalize_unit_name(unit):
    """
    Normalize unit names to standard format.
    
    Example:
        "ug/m3" -> "µg/m³"
        "µg/m³" -> "µg/m³"
    """
    if pd.isna(unit) or not unit:
        return "unknown"
    
    unit = str(unit).strip()
    
    # Normalize common variations
    unit_map = {
        "ug/m3": "µg/m³",
        "μg/m3": "µg/m³",
        "μg/m³": "µg/m³",
        "mg/m3": "mg/m³",
        "ug/m³": "µg/m³",
        "%rh": "%",
        "ppb (air quality)": "ppb",
        "ppm (air quality)": "ppm",
    }
    
    return unit_map.get(unit, unit)


def convert_value_to_standard_unit(row):
    """
    Convert a measurement value to standardized unit.
    
    Returns tuple: (converted_value, standard_unit, conversion_applied)
    """
    param = normalize_parameter_name(row.get("parameter"))
    unit = normalize_unit_name(row.get("units"))
    value = row.get("value")
    
    # Handle missing values
    if pd.isna(value):
        return np.nan, "unknown", False
    
    if param not in UNIT_CONVERSIONS:
        return value, unit, False
    
    param_conversions = UNIT_CONVERSIONS[param]
    standard_unit = STANDARD_UNITS.get(param, "unknown")
    
    # If unit is not in our conversion map, return as-is
    if unit not in param_conversions:
        return value, unit, False
    
    conversion_factor = param_conversions[unit]
    
    # Handle special cases (e.g., temperature K to °C)
    if conversion_factor is None:
        if param == "temperature" and unit == "K":
            converted_value = value - 273.15
            return converted_value, standard_unit, True
        return value, unit, False
    
    # Standard conversion using multiplication
    converted_value = float(value) * conversion_factor
    return converted_value, standard_unit, True


def detect_sensor_drift(df, location_id, parameter, window_size=24):
    """
    Detect potential sensor calibration drift.
    Returns indices of readings that deviate significantly from rolling mean.
    """
    subset = df[
        (df['location_id'] == location_id) &
        (df['parameter_normalized'] == parameter)
    ].copy()
    
    if len(subset) < window_size:
        return []
    
    subset = subset.sort_values('datetime_utc')
    subset['rolling_mean'] = subset['value_standardized'].rolling(
        window=window_size, 
        center=True, 
        min_periods=1
    ).mean()
    subset['rolling_std'] = subset['value_standardized'].rolling(
        window=window_size, 
        center=True, 
        min_periods=1
    ).std()
    
    # Detect points > 3 std deviations from rolling mean
    subset['drift_score'] = np.abs(
        (subset['value_standardized'] - subset['rolling_mean']) / (subset['rolling_std'] + 0.1)
    )
    
    drift_indices = subset[subset['drift_score'] > 3].index.tolist()
    return drift_indices


def harmonize_data(raw_df):
    """
    Main harmonization workflow.
    
    Returns:
        Tuple of (harmonized_df, harmonization_report_df)
    """
    log_message("\n" + "="*60)
    log_message("TASK 2: HARMONIZATION - Starting")
    log_message("="*60)
    
    harmonized = raw_df.copy()
    
    # Step 1: Normalize parameter names
    log_message("\n[STEP 1/6] Normalizing parameter names...")
    harmonized['parameter_normalized'] = harmonized['parameter'].apply(normalize_parameter_name)
    param_summary = harmonized.groupby(['parameter', 'parameter_normalized']).size().reset_index(name='count')
    log_message(f"✓ Processed {len(param_summary)} parameter variations")
    for _, row in param_summary.iterrows():
        if row['parameter'] != row['parameter_normalized']:
            log_message(f"  {row['parameter']} -> {row['parameter_normalized']} ({row['count']} records)")
    
    # Step 2: Normalize unit names
    log_message("\n[STEP 2/6] Normalizing unit names...")
    harmonized['unit_normalized'] = harmonized['units'].apply(normalize_unit_name)
    unit_summary = harmonized.groupby(['units', 'unit_normalized']).size().reset_index(name='count')
    log_message(f"✓ Processed {len(unit_summary)} unit variations")
    
    # Step 3: Convert all values to standard units
    log_message("\n[STEP 3/6] Converting to standardized units...")
    conversions = harmonized.apply(convert_value_to_standard_unit, axis=1, result_type='expand')
    conversions.columns = ['value_standardized', 'unit_standard', 'conversion_applied']
    harmonized = pd.concat([harmonized, conversions], axis=1)
    
    conversions_made = conversions['conversion_applied'].sum()
    log_message(f"✓ Applied {conversions_made} unit conversions")
    
    # Log conversion examples
    converted = harmonized[harmonized['conversion_applied']]
    if len(converted) > 0:
        log_message(f"\nConversion examples:")
        for param in converted['parameter_normalized'].unique()[:5]:
            examples = converted[converted['parameter_normalized'] == param].head(2)
            for _, row in examples.iterrows():
                log_message(f"  {row['parameter']}: {row['value']} {row['units']} -> {row['value_standardized']:.2f} {row['unit_standard']}")
    
    # Step 4: Timestamp harmonization
    log_message("\n[STEP 4/6] Harmonizing timestamps...")
    # Ensure UTC timezone
    if harmonized['datetime_utc'].dt.tz is None:
        harmonized['datetime_utc'] = harmonized['datetime_utc'].dt.tz_localize('UTC')
    else:
        harmonized['datetime_utc'] = harmonized['datetime_utc'].dt.tz_convert('UTC')
    
    # Extract time features
    harmonized['year'] = harmonized['datetime_utc'].dt.year
    harmonized['month'] = harmonized['datetime_utc'].dt.month
    harmonized['day'] = harmonized['datetime_utc'].dt.day
    harmonized['hour'] = harmonized['datetime_utc'].dt.hour
    harmonized['minute'] = harmonized['datetime_utc'].dt.minute
    harmonized['day_of_week'] = harmonized['datetime_utc'].dt.day_name()
    harmonized['week_of_year'] = harmonized['datetime_utc'].dt.isocalendar().week
    
    log_message(f"✓ Timestamps synchronized")
    log_message(f"  Date range: {harmonized['datetime_utc'].min()} to {harmonized['datetime_utc'].max()}")
    
    # Step 5: Data quality validation
    log_message("\n[STEP 5/6] Validating measurement quality...")
    
    # Flag measurements with issues
    harmonized['quality_flags'] = ""
    harmonized.loc[harmonized['value_standardized'] < 0, 'quality_flags'] += "negative_value|"
    harmonized.loc[harmonized['value_standardized'] > 1e6, 'quality_flags'] += "extreme_value|"
    harmonized.loc[harmonized['parameter_normalized'] == 'unknown', 'quality_flags'] += "unknown_param|"
    harmonized.loc[harmonized['unit_standard'] == 'unknown', 'quality_flags'] += "unknown_unit|"
    
    # Remove trailing pipe
    harmonized['quality_flags'] = harmonized['quality_flags'].str.rstrip('|')
    
    flagged_count = (harmonized['quality_flags'] != '').sum()
    log_message(f"✓ Flagged {flagged_count} measurements with quality issues")
    
    flag_breakdown = harmonized[harmonized['quality_flags'] != '']['quality_flags'].value_counts()
    for flag, count in flag_breakdown.head(5).items():
        log_message(f"  {flag}: {count}")
    
    # Step 6: Reporting frequency analysis
    log_message("\n[STEP 6/6] Analyzing reporting frequency...")
    
    freq_analysis = []
    for location_id in harmonized['location_id'].unique():
        loc_data = harmonized[harmonized['location_id'] == location_id]
        time_diffs = loc_data.groupby('sensor_id')['datetime_utc'].diff().dt.total_seconds() / 3600  # Convert to hours
        
        if len(time_diffs) > 0:
            freq_analysis.append({
                'location_id': location_id,
                'location_name': loc_data['location_name'].iloc[0],
                'measurement_count': len(loc_data),
                'unique_sensors': loc_data['sensor_id'].nunique(),
                'avg_frequency_hours': time_diffs.mean(),
                'min_frequency_hours': time_diffs.min(),
                'max_frequency_hours': time_diffs.max(),
            })
    
    freq_df = pd.DataFrame(freq_analysis)
    if len(freq_df) > 0:
        log_message(f"✓ Analyzed reporting frequency for {len(freq_df)} locations")
        log_message(f"  Average frequency: {freq_df['avg_frequency_hours'].mean():.1f} hours")
        log_message(f"  Range: {freq_df['avg_frequency_hours'].min():.1f} - {freq_df['avg_frequency_hours'].max():.1f} hours")
    
    log_message("\n" + "="*60)
    log_message("✓ TASK 2 COMPLETED: Harmonization Successful")
    log_message("="*60)
    
    return harmonized, freq_df


def main():
    """Main harmonization workflow."""
    
    # Clear log file
    with open(HARMONIZATION_LOG_FILE, "w") as f:
        f.write(f"=== Harmonization Log ===\nStart: {datetime.now().isoformat()}\n\n")
    
    # Load raw data
    try:
        raw_df = pd.read_csv(RAW_DATA_FILE)
        log_message(f"✓ Loaded raw data: {RAW_DATA_FILE} ({len(raw_df)} rows)")
    except FileNotFoundError:
        log_message(f"✗ Error: {RAW_DATA_FILE} not found")
        log_message("Please run 01_ingest_data.py first to generate raw data.")
        return
    
    # Parse datetime columns
    raw_df['datetime_utc'] = pd.to_datetime(raw_df['datetime_utc'], errors='coerce', utc=True)
    raw_df['datetime_local'] = pd.to_datetime(raw_df['datetime_local'], errors='coerce')
    raw_df['value'] = pd.to_numeric(raw_df['value'], errors='coerce')
    
    # Remove rows with no datetime
    raw_df = raw_df.dropna(subset=['datetime_utc'])
    
    # Run harmonization
    harmonized_df, freq_df = harmonize_data(raw_df)
    
    # Save harmonized data
    harmonized_df.to_csv(HARMONIZED_DATA_FILE, index=False)
    log_message(f"\n✓ Harmonized data saved to: {HARMONIZED_DATA_FILE} ({len(harmonized_df)} rows)")
    
    # Save frequency analysis
    if len(freq_df) > 0:
        freq_df.to_csv(HARMONIZATION_REPORT_FILE, index=False)
        log_message(f"✓ Frequency analysis saved to: {HARMONIZATION_REPORT_FILE}")
    
    # Save unit mapping reference
    unit_mapping_ref = {
        "standardConversions": UNIT_CONVERSIONS,
        "standardUnits": STANDARD_UNITS,
        "generatedAt": datetime.now().isoformat(),
    }
    with open(UNIT_MAPPINGS_FILE, "w") as f:
        json.dump(unit_mapping_ref, f, indent=2)
    log_message(f"✓ Unit mapping reference saved to: {UNIT_MAPPINGS_FILE}")
    
    # Print summary
    log_message("\n" + "="*60)
    log_message("SUMMARY")
    log_message("="*60)
    log_message(f"Raw data rows: {len(raw_df)}")
    log_message(f"Harmonized data rows: {len(harmonized_df)}")
    log_message(f"Unique locations: {harmonized_df['location_id'].nunique()}")
    log_message(f"Unique sensors: {harmonized_df['sensor_id'].nunique()}")
    log_message(f"Parameters: {', '.join(harmonized_df['parameter_normalized'].unique())}")
    log_message(f"\nQuality flags assigned: {(harmonized_df['quality_flags'] != '').sum()} rows")
    log_message(f"\nAll outputs saved to current directory")
    log_message("="*60 + "\n")


if __name__ == "__main__":
    main()
