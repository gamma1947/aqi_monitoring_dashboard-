import numpy as np
import pandas as pd


RAW_FILE = "openaq_raw_data.csv"
NULL_REPORT_FILE = "report_null_values.csv"
MESSY_REPORT_FILE = "report_messy_data.csv"
OUTLIER_REPORT_FILE = "report_outliers.csv"
NOISE_REPORT_FILE = "report_noise.csv"
CLEAN_FILE = "openaq_cleaned_data.csv"


def load_raw_data(path=RAW_FILE):
	return pd.read_csv(path)


def analyze_nulls(df):
	null_count = df.isna().sum()
	null_pct = (null_count / len(df) * 100).round(2)
	report = pd.DataFrame({
		"column": null_count.index,
		"null_count": null_count.values,
		"null_percent": null_pct.values,
	}).sort_values(["null_count", "column"], ascending=[False, True])
	report.to_csv(NULL_REPORT_FILE, index=False)
	return report


def detect_messy_data(df):
	working = df.copy()

	working["datetime_utc_parsed"] = pd.to_datetime(working["datetime_utc"], errors="coerce", utc=True)
	working["datetime_local_parsed"] = pd.to_datetime(working["datetime_local"], errors="coerce")

	messy_mask = pd.Series(False, index=working.index)

	messy_mask |= working["datetime_utc_parsed"].isna()
	messy_mask |= working["location_name"].astype("string").str.strip().isin(["", "<NA>"])

	if "coordinates_latitude" in working.columns:
		lat = pd.to_numeric(working["coordinates_latitude"], errors="coerce")
		messy_mask |= lat.notna() & ~lat.between(-90, 90)
	if "coordinates_longitude" in working.columns:
		lon = pd.to_numeric(working["coordinates_longitude"], errors="coerce")
		messy_mask |= lon.notna() & ~lon.between(-180, 180)

	messy = working.loc[messy_mask].copy()
	messy.to_csv(MESSY_REPORT_FILE, index=False)
	return messy


def detect_outliers(df):
	outliers = df.copy()
	outliers["value_num"] = pd.to_numeric(outliers["value"], errors="coerce")
	outliers = outliers.dropna(subset=["value_num"]).copy()

	q1 = outliers["value_num"].quantile(0.25)
	q3 = outliers["value_num"].quantile(0.75)
	iqr = q3 - q1

	if iqr == 0:
		outlier_mask = pd.Series(False, index=outliers.index)
		lower_bound = q1
		upper_bound = q3
	else:
		lower_bound = q1 - 1.5 * iqr
		upper_bound = q3 + 1.5 * iqr
		outlier_mask = (outliers["value_num"] < lower_bound) | (outliers["value_num"] > upper_bound)

	outlier_rows = outliers.loc[outlier_mask].copy()
	outlier_rows["outlier_rule"] = "IQR"
	outlier_rows["lower_bound"] = lower_bound
	outlier_rows["upper_bound"] = upper_bound
	outlier_rows.to_csv(OUTLIER_REPORT_FILE, index=False)
	return outlier_rows


def detect_noise(df):
	noise = df.copy()
	noise["value_num"] = pd.to_numeric(noise["value"], errors="coerce")
	noise["datetime_utc_parsed"] = pd.to_datetime(noise["datetime_utc"], errors="coerce", utc=True)
	noise = noise.dropna(subset=["value_num", "datetime_utc_parsed", "sensor_id"]).copy()

	noise = noise.sort_values(["sensor_id", "datetime_utc_parsed"]).copy()
	noise["value_diff"] = noise.groupby("sensor_id")["value_num"].diff().abs()

	valid_diffs = noise["value_diff"].dropna()
	if valid_diffs.empty:
		empty_noise = noise.iloc[0:0].copy()
		empty_noise["noise_rule"] = pd.Series(dtype="string")
		empty_noise["jump_threshold"] = pd.Series(dtype="float")
		empty_noise.to_csv(NOISE_REPORT_FILE, index=False)
		return empty_noise

	diff_q1 = valid_diffs.quantile(0.25)
	diff_q3 = valid_diffs.quantile(0.75)
	diff_iqr = diff_q3 - diff_q1

	if pd.isna(diff_iqr) or diff_iqr == 0:
		threshold = valid_diffs.median()
	else:
		threshold = diff_q3 + 1.5 * diff_iqr

	noise_rows = noise[noise["value_diff"] > threshold].copy()
	noise_rows["noise_rule"] = "sudden_jump"
	noise_rows["jump_threshold"] = threshold
	noise_rows.to_csv(NOISE_REPORT_FILE, index=False)
	return noise_rows


def clean_and_preprocess(df):
	cleaned = df.copy()

	text_columns = ["location_name", "parameter", "parameter_display", "units", "country_code", "country_name"]
	for col in text_columns:
		if col in cleaned.columns:
			cleaned[col] = cleaned[col].astype("string").str.strip()

	numeric_columns = ["location_id", "sensor_id", "value", "coordinates_latitude", "coordinates_longitude"]
	for col in numeric_columns:
		if col in cleaned.columns:
			cleaned[col] = pd.to_numeric(cleaned[col], errors="coerce")

	cleaned["datetime_utc"] = pd.to_datetime(cleaned["datetime_utc"], errors="coerce", utc=True)
	cleaned["datetime_local"] = pd.to_datetime(cleaned["datetime_local"], errors="coerce")

	# Drop records that cannot be used for analysis.
	cleaned = cleaned.dropna(subset=["location_id", "sensor_id", "datetime_utc", "value"]).copy()

	# Remove obviously invalid values and coordinate noise.
	cleaned = cleaned[cleaned["value"] >= 0].copy()
	cleaned = cleaned[
		cleaned["coordinates_latitude"].isna() | cleaned["coordinates_latitude"].between(-90, 90)
	]
	cleaned = cleaned[
		cleaned["coordinates_longitude"].isna() | cleaned["coordinates_longitude"].between(-180, 180)
	]

	# Remove extreme outliers using IQR on value.
	q1 = cleaned["value"].quantile(0.25)
	q3 = cleaned["value"].quantile(0.75)
	iqr = q3 - q1
	if iqr > 0:
		lower_bound = q1 - 1.5 * iqr
		upper_bound = q3 + 1.5 * iqr
		cleaned = cleaned[(cleaned["value"] >= lower_bound) & (cleaned["value"] <= upper_bound)].copy()

	# Fill categorical nulls after cleaning.
	for col in text_columns:
		if col in cleaned.columns:
			cleaned[col] = cleaned[col].fillna("unknown")

	cleaned["location_id"] = cleaned["location_id"].astype("Int64")
	cleaned["sensor_id"] = cleaned["sensor_id"].astype("Int64")

	# Feature engineering for model/dashboard use.
	cleaned["date"] = cleaned["datetime_utc"].dt.date
	cleaned["hour_utc"] = cleaned["datetime_utc"].dt.hour
	cleaned["day_of_week"] = cleaned["datetime_utc"].dt.day_name()
	cleaned["month"] = cleaned["datetime_utc"].dt.strftime("%Y-%m")
	cleaned["has_coordinates"] = cleaned[["coordinates_latitude", "coordinates_longitude"]].notna().all(axis=1)

	cleaned = cleaned.drop_duplicates(subset=["location_id", "sensor_id", "datetime_utc", "value"])
	cleaned = cleaned.sort_values(["location_id", "sensor_id", "datetime_utc"]).reset_index(drop=True)
	cleaned.to_csv(CLEAN_FILE, index=False)
	return cleaned


def print_summary(df, null_report, messy, outliers, noise, cleaned):
	print("Data quality analysis complete")
	print(f"Input rows: {len(df)}")
	print(f"Columns with nulls: {(null_report['null_count'] > 0).sum()}")
	print(f"Messy rows: {len(messy)}")
	print(f"Outlier rows: {len(outliers)}")
	print(f"Noise rows: {len(noise)}")
	print(f"Cleaned rows: {len(cleaned)}")
	print("Generated files:")
	print(f"- {NULL_REPORT_FILE}")
	print(f"- {MESSY_REPORT_FILE}")
	print(f"- {OUTLIER_REPORT_FILE}")
	print(f"- {NOISE_REPORT_FILE}")
	print(f"- {CLEAN_FILE}")


def main():
	df = load_raw_data(RAW_FILE)
	null_report = analyze_nulls(df)
	messy = detect_messy_data(df)
	outliers = detect_outliers(df)
	noise = detect_noise(df)
	cleaned = clean_and_preprocess(df)
	print_summary(df, null_report, messy, outliers, noise, cleaned)


if __name__ == "__main__":
	main()
