import streamlit as st
import time

# 1. Page Configuration MUST be the first Streamlit command
st.set_page_config(
    page_title="UrbanAir OS | Portal",
    page_icon="🍃",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# 2. BULLETPROOF ROUTING: Initialize state before anything else renders
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

if "auth_flow" not in st.session_state:
    st.session_state.auth_flow = "login"

# If already logged in, send them straight to the dashboard immediately
if st.session_state.logged_in:
    st.switch_page("pages/dashboard.py")

# 3. Advanced CSS to Target Streamlit Elements
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap');

    /* Hide default Streamlit headers */
    #MainMenu {visibility: hidden;}
    header {visibility: hidden;}
    footer {visibility: hidden;}

    /* Global Typography */
    html, body, [class*="css"] {
        font-family: 'Plus Jakarta Sans', sans-serif;
    }

    /* The Background Gradient */
    .stApp {
        background: linear-gradient(135deg, #e0f2fe 0%, #f0fdf4 50%, #dcfce7 100%);
    }

    /* Vertically center everything on the screen */
    [data-testid="stAppViewBlockContainer"] {
        display: flex;
        flex-direction: column;
        justify-content: center;
        min-height: 100vh;
        padding-top: 0rem;
        padding-bottom: 0rem;
    }

    /* Styling Streamlit's native container to be the Glass Card */
    [data-testid="stVerticalBlockBorderWrapper"] {
        background: rgba(255, 255, 255, 0.95) !important;
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255, 255, 255, 0.6) !important;
        border-radius: 24px !important;
        box-shadow: 0 20px 40px rgba(0, 0, 0, 0.08), 0 1px 3px rgba(0,0,0,0.05) !important;
        padding: 2.5rem 2rem !important;
    }

    /* Typography inside the card */
    .brand-title {
        font-size: 32px;
        font-weight: 800;
        color: #0f172a;
        text-align: center;
        margin-bottom: 0.2rem;
        margin-top: 1rem;
        letter-spacing: -0.5px;
    }

    .brand-subtitle {
        font-size: 15px;
        font-weight: 500;
        color: #64748b;
        text-align: center;
        margin-bottom: 2rem;
    }

    /* Input Fields Styling */
    div[data-baseweb="input"] > div {
        border-radius: 12px !important;
        border: 1px solid #cbd5e1 !important;
        background-color: #f8fafc !important;
    }
    div[data-baseweb="input"] > div:focus-within {
        border-color: #059669 !important;
        box-shadow: 0 0 0 3px rgba(5, 150, 105, 0.1) !important;
    }

    /* Primary Button Styling */
    div.stButton > button[kind="primary"] {
        background: linear-gradient(135deg, #059669 0%, #047857 100%);
        color: white;
        border: none;
        border-radius: 12px;
        font-weight: 600;
        height: 3rem;
        transition: all 0.3s ease;
        margin-top: 10px;
    }
    div.stButton > button[kind="primary"]:hover {
        transform: translateY(-2px);
        box-shadow: 0 10px 15px -3px rgba(5, 150, 105, 0.3);
    }
</style>
""", unsafe_allow_html=True)

# 4. Layout: Centering the card horizontally
_, center_col, _ = st.columns([1, 1.2, 1])

with center_col:
    with st.container(border=True):

        st.markdown('<div style="text-align:center; font-size:50px;">🍃</div>', unsafe_allow_html=True)
        st.markdown('<div class="brand-title">UrbanAir OS</div>', unsafe_allow_html=True)

        # --- LOGIN FLOW ---
        if st.session_state.auth_flow == "login":
            st.markdown('<div class="brand-subtitle">Secure access to live telemetry</div>', unsafe_allow_html=True)

            username = st.text_input("Work Email or ID")
            password = st.text_input("Password", type="password")

            if st.button("Authenticate", type="primary", use_container_width=True):
                if username == "admin" and password == "1234":
                    # Lock in the state memory
                    st.session_state.logged_in = True
                    # Rerun the script so the top-level routing catches it and safely switches
                    st.rerun()
                else:
                    st.error("Invalid credentials.")

            st.write("---")
            col1, col2 = st.columns(2)
            with col1:
                if st.button("Request Access", use_container_width=True):
                    st.session_state.auth_flow = "signup"
                    st.rerun()
            with col2:
                if st.button("Recover Password", use_container_width=True):
                    st.session_state.auth_flow = "forgot"
                    st.rerun()

        # --- SIGNUP FLOW ---
        elif st.session_state.auth_flow == "signup":
            st.markdown('<div class="brand-subtitle">Request workspace access</div>', unsafe_allow_html=True)
            first_name = st.text_input("First Name")
            email = st.text_input("Corporate Email")

            if st.button("Submit Request", type="primary", use_container_width=True):
                st.success("Request sent!")

            st.write("---")
            if st.button("Return to Sign In", use_container_width=True):
                st.session_state.auth_flow = "login"
                st.rerun()

        # --- FORGOT PASSWORD FLOW ---
        elif st.session_state.auth_flow == "forgot":
            st.markdown('<div class="brand-subtitle">Reset identity credentials</div>', unsafe_allow_html=True)
            email_reset = st.text_input("Corporate Email Address")

            if st.button("Send Recovery Link", type="primary", use_container_width=True):
                st.info("Recovery link sent.")

            st.write("---")
            if st.button("Return to Sign In", use_container_width=True):
                st.session_state.auth_flow = "login"
                st.rerun()

# Footer outside the card
st.markdown("""
    <div style='text-align: center; color: #64748b; font-size: 12px; margin-top: 2rem;'>
        Protected by TLS 1.3 • UrbanAir OS v3.2.0
    </div>
""", unsafe_allow_html=True)