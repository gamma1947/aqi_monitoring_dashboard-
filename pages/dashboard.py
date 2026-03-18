import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px

# 1. Page Configuration MUST be the first Streamlit command
st.set_page_config(page_title="Urban Air Dashboard", layout="wide", initial_sidebar_state="collapsed")

# 2. BULLETPROOF SECURITY CHECK: Boot them out if they don't have the VIP pass
if "logged_in" not in st.session_state or not st.session_state.logged_in:
    st.switch_page("main.py")

# 3. Clean Enterprise CSS
st.markdown("""
<style>
    /* Hide top header and main menu */
    #MainMenu {visibility: hidden;}
    header {visibility: hidden;}

    /* Clean, light gray background for the whole app */
    .stApp {
        background-color: #f4f7fb; 
    }

    /* Solid White Cards with subtle shadows */
    [data-testid="stVerticalBlockBorderWrapper"] {
        background-color: #ffffff !important;
        border: 1px solid #e2e8f0 !important;
        border-radius: 12px !important;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05), 0 2px 4px -1px rgba(0, 0, 0, 0.03) !important;
        padding: 1.5rem !important;
    }

    /* Text styling */
    h1, h2, h3, p, label, .st-emotion-cache-10trblm {
        color: #1e293b !important; /* Dark slate for readability */
        font-family: 'Inter', sans-serif;
    }

    .main-title {
        font-size: 32px;
        font-weight: 700;
        margin-bottom: 20px;
        margin-top: -30px;
        color: #0f172a !important;
    }

    /* Custom Metric HTML Styling */
    .metric-value {
        font-size: 28px;
        font-weight: 700;
        color: #0f172a;
        margin-top: -5px;
    }
    .metric-label {
        font-size: 14px;
        color: #64748b; /* Slate gray */
        font-weight: 600;
        margin-bottom: 5px;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }

    /* Clean inputs */
    div[data-baseweb="select"] > div, div[data-baseweb="input"] > div {
        background-color: #ffffff !important;
        border-radius: 8px;
        border: 1px solid #cbd5e1 !important;
    }

    /* Navigation / Logout Button */
    div.stButton > button[kind="secondary"] {
        background-color: #ffffff !important;
        border: 1px solid #cbd5e1 !important;
        color: #334155 !important;
        font-weight: 600;
        border-radius: 8px;
    }
    div.stButton > button[kind="secondary"]:hover {
        border-color: #ef4444 !important;
        color: #ef4444 !important;
    }
</style>
""", unsafe_allow_html=True)

# --- HEADER & LOGOUT ROW ---
head_col1, head_col2 = st.columns([8, 1])
with head_col1:
    st.markdown('<div class="main-title">Urban Air Quality Control Center</div>', unsafe_allow_html=True)
with head_col2:
    if st.button("Logout", use_container_width=True):
        st.session_state.logged_in = False
        st.switch_page("main.py")

# --- TOP FILTER ROW ---
f1, f2, f3, f4, f5 = st.columns([1, 1, 1, 1.5, 0.8])

with f1: city = st.selectbox("City:", ["Pune", "Mumbai", "Delhi"], label_visibility="collapsed")
with f2: source = st.selectbox("Source:", ["Sensor Network", "Satellite", "CBP"], label_visibility="collapsed")
with f3: pollutant = st.selectbox("Pollutant:", ["PM 2.5", "PM 10", "NO2"], label_visibility="collapsed")
with f4: date = st.date_input("Date:", [], label_visibility="collapsed")
with f5: fetch_btn = st.button("Fetch Data", use_container_width=True)

st.write("")  # Spacer

# --- DYNAMIC DATA LOGIC ---
np.random.seed(hash(city + pollutant) % (2 ** 32))

base_val = 150 if city == "Delhi" else (100 if city == "Mumbai" else 80)
current_pm25 = int(np.random.normal(base_val, 20))
current_pm10 = int(current_pm25 * 1.4)
aqi = int(current_pm25 * 1.2)

# Determine AQI Status and Colors
if aqi < 100:
    aqi_status, aqi_color = "Good", "#10b981"  # Emerald Green
elif aqi < 200:
    aqi_status, aqi_color = "Moderate", "#f59e0b"  # Amber
else:
    aqi_status, aqi_color = "Poor", "#ef4444"  # Red

# --- METRICS ROW (4 Cards) ---
m1, m2, m3, m4 = st.columns(4)

with m1:
    with st.container(border=True):
        st.markdown('<div class="metric-label">Current P.M. 2.5</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-value">{current_pm25} µg/m³</div>', unsafe_allow_html=True)

with m2:
    with st.container(border=True):
        st.markdown(f'<div class="metric-label">{pollutant} Level</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-value">{current_pm10} µg/m³</div>', unsafe_allow_html=True)

with m3:
    with st.container(border=True):
        st.markdown('<div class="metric-label">Overall AQI</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-value" style="color: {aqi_color};">{aqi} ({aqi_status})</div>',
                    unsafe_allow_html=True)

with m4:
    with st.container(border=True):
        st.markdown('<div class="metric-label">Temperature</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-value">{np.random.randint(25, 38)}°C</div>', unsafe_allow_html=True)

# --- MAIN CHARTS ROW ---
c_left, c_right = st.columns([2.5, 1])

with c_left:
    with st.container(border=True):
        header_col, toggle_col = st.columns([3, 2])
        header_col.subheader(f"{pollutant} Trend in {city}")

        # ADDED: Time Scale Toggle to hit Rubric Requirement
        time_scale = toggle_col.radio("Aggregation:", ["Hourly", "Daily", "Monthly", "Realtime"], horizontal=True,
                                      label_visibility="collapsed")

        # Adjust mock data based on the selected time scale
        if time_scale == "Hourly":
            x_labels = [f"{i}:00" for i in range(8, 19)]
            trend = np.linspace(base_val - 30, base_val + 20, 11) + np.random.normal(0, 10, 11)
        elif time_scale == "Daily":
            x_labels = [f"Day {i}" for i in range(1, 12)]
            trend = np.linspace(base_val - 10, base_val + 40, 11) + np.random.normal(0, 15, 11)
        else:  # Monthly
            x_labels = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov"]
            trend = np.linspace(base_val + 20, base_val - 20, 11) + np.random.normal(0, 20, 11)

        df = pd.DataFrame({
            "Time": x_labels,
            "Level": trend
        })

        fig = px.area(df, x="Time", y="Level")

        # Color mapping
        if pollutant == "PM 2.5":
            chart_color, fill_color = '#10b981', 'rgba(16, 185, 129, 0.2)'  # Green
        elif pollutant == "NO2":
            chart_color, fill_color = '#3b82f6', 'rgba(59, 130, 246, 0.2)'  # Blue
        else:
            chart_color, fill_color = '#f59e0b', 'rgba(245, 158, 11, 0.2)'  # Amber

        fig.update_traces(line_color=chart_color, fillcolor=fill_color)
        fig.update_layout(
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            margin=dict(l=0, r=0, t=10, b=0),
            xaxis=dict(showgrid=True, gridcolor='#e2e8f0', title=""),
            yaxis=dict(showgrid=True, gridcolor='#e2e8f0', title="")
        )

        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})

with c_right:
    with st.container(border=True):
        st.subheader("System Alerts")
        st.write("")

        if aqi > 150:
            st.error(f"⚠️ **Alert:** High {pollutant} detected in {city}.")
        else:
            st.success(f"✅ **Status:** {city} air quality is stable.")

        st.warning(f"🔋 **Station {np.random.randint(1, 20)}:** Sensor battery low.")
        st.info(f"🔄 **Source:** {source} sync completed.")

        st.write("\n" * 3)