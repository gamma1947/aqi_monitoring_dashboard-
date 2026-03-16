# 🌍 Air Quality Monitoring Dashboard

An interactive **Air Quality Monitoring Dashboard** built using real-world environmental sensor data.  
This project was developed as part of the **Data Science Practices course** taught by Prof. Bedarth Goswami at IISER Pune.

The dashboard ingests data from **OpenAQ**, processes it through a structured data-cleaning pipeline, and visualizes pollution trends using an interactive web interface built with **Streamlit**.

The goal of the project is to understand **temporal pollution patterns, spatial variation across monitoring stations, and the impact of imperfect sensor data on environmental interpretation.**

---

# 📊 Project Overview

Urban air quality monitoring relies heavily on distributed sensor networks that collect environmental data across different locations and time intervals. However, such real-world data often contains issues such as:

- Missing observations  
- Sensor noise  
- Calibration drift  
- Inconsistent reporting frequencies  

This project develops a **reproducible data pipeline and visualization dashboard** that:

1. Collects air quality data from public sensor networks  
2. Cleans and harmonizes the data  
3. Handles missing and noisy readings  
4. Aggregates pollution data across different time scales  
5. Provides interactive visualizations for exploration and analysis  

---

# ⚙️ Technologies Used

- Python  
- Streamlit — interactive dashboard interface  
- OpenAQ API — air quality data source  
- Pandas — data cleaning and manipulation  
- NumPy — numerical operations  
- Matplotlib / Plotly — data visualization  

---

# 🌐 Data Source: OpenAQ API

Air quality observations used in this project were obtained from the **OpenAQ platform**, an open-source initiative that aggregates air quality measurements from governmental and research-grade monitoring networks worldwide.

The OpenAQ API provides programmatic access to environmental measurements including:

- Particulate matter concentrations
- Gaseous pollutants
- Sensor metadata
- Monitoring station locations
- Measurement timestamps

For this project, the API was used to retrieve data from monitoring stations across **India**, including information about pollutant concentration, reporting intervals, and station metadata.

API requests were implemented using Python and structured into a reproducible ingestion pipeline that automatically retrieves and formats the data for downstream processing.

More information: https://docs.openaq.org/

# Variable and Location Selection

### 1. Pollutant Variables Selected

Air Quality Index (AQI) calculations typically rely on a subset of key atmospheric pollutants that are known to have strong health impacts. For this project, six commonly monitored pollutants were selected from the OpenAQ dataset:

| Pollutant | Description |
|-----------|-------------|
| PM2.5 | Fine particulate matter smaller than 2.5 µm |
| PM10 | Particulate matter smaller than 10 µm |
| NO₂ | Nitrogen dioxide |
| SO₂ | Sulfur dioxide |
| CO | Carbon monoxide |
| O₃ | Ozone |

These pollutants were selected because they are the **primary components used in most national AQI frameworks**, including those used by environmental agencies such as the Central Pollution Control Board (CPCB).

Filtering the dataset to these variables ensures consistency in comparing pollution trends across monitoring stations.

### 2. Monitoring station selection

Montoring stations with all 6 sensors available are only selected.
Some cities contain multiple monitoring stations, while others may only have a single reporting sensor.

To ensure consistent city-level comparisons, the following strategy was used:

- **Cities with a single monitoring station**  
  The station measurements are used directly as the representative air quality record for that location.

- **Cities with multiple monitoring stations**  
  Pollutant concentrations are aggregated across stations using the **mean value** to obtain a city-level estimate.

This aggregation helps reduce the influence of **sensor-specific noise or calibration differences** while still allowing smaller urban centers with limited monitoring infrastructure to be included in the analysis.

# 🧠 System Architecture

The project follows a modular pipeline architecture:

OpenAQ API  
↓  
Data Ingestion  
↓  
Data Cleaning & Preprocessing  
- Missing value handling  
- Outlier detection  
- Sensor noise filtering  
↓  
Data Aggregation  
- Hourly  
- Daily  
- Monthly  
↓  
Visualization Dashboard (Streamlit)

---

# 🧹 Data Processing Pipeline

### 1. Data Ingestion
Air quality data is fetched using the **OpenAQ API**, covering multiple monitoring stations.

### 2. Data Harmonization
Sensor data is standardized to ensure consistency in:

- Units  
- Timestamp formats  
- Reporting frequency

### 3. Data Cleaning
The cleaning pipeline addresses:

- Missing sensor readings  
- Outliers in pollution measurements  
- Noise in sensor signals  
- Inconsistent or duplicated timestamps  

### 4. Missing Data Strategies

Two different strategies were explored:

1. **Interpolation-based approach**
2. **Model-based imputation**

These methods were compared to understand how data-repair strategies affect environmental interpretation.

---

# 📈 Dashboard Features

The dashboard provides:

### Interactive Visualizations
- Temporal pollution trends  
- Station-wise comparison  
- Pollutant concentration over time  

### Multi-scale Analysis
Data is aggregated across:

- Hourly averages  
- Daily averages  
- Monthly trends  

### Extreme Pollution Events
The system identifies and visualizes:

- Pollution spikes  
- Episode duration  
- Relative intensity across stations  

---

# 🖥️ Running the Dashboard

### Clone the Repository

```bash
git clone https://github.com/yourusername/air-quality-dashboard.git
cd air-quality-dashboard
```

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Run the Streamlit App

```bash
streamlit run app.py
```

The dashboard will open in your browser.

---

# 📂 Project Structure

```
air-quality-dashboard
│
├── data/                 # Raw and processed datasets
├── api/                  # API fetching scripts
├── cleaning/             # Data cleaning pipeline
├── dashboard/            # Streamlit dashboard
├── utils/                # Helper functions
│
├── app.py                # Main Streamlit application
├── requirements.txt
└── README.md
```

---

# 🔬 Key Insights Explored

- Temporal trends in air pollution levels  
- Spatial variation across monitoring stations  
- Impact of missing-data strategies on environmental analysis  
- Identification of extreme pollution episodes  

---

# ⚠️ Limitations

- Sensor networks may have **calibration drift** over time  
- Data availability varies across monitoring stations  
- Imputation methods can introduce bias in interpretation  
- Some sensors report at irregular time intervals  

These limitations highlight the importance of **careful preprocessing and transparency in environmental analytics.**

---

# 👥 Contributors

| Name | Contribution |
|-----|-------------|
| Ajay Kasaudhan | GUI development and dashboard design |
| Ashik | API integration and data ingestion |
| Hitesh CK | System architecture design |
| Sahil Rajput | Data cleaning and interpolation methods |

---

# 📚 Course Information

This project was completed as a **group project for the Data Science Practices course** at **IISER Pune**.

Instructor: **Prof. Bedarth Goswami**
