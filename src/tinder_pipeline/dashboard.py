import os

import pandas as pd
import redis
import streamlit as st

# Page Config
st.set_page_config(page_title="Tinder Trust & Safety", layout="wide")
st.title("🔥 Tinder Trust & Safety: Real-Time Monitor")

# Connect to Redis
# We use 'redis' as hostname because this will run inside Docker
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
r = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)

# Auto-refresh mechanism
if "sleep_time" not in st.session_state:
    st.session_state.sleep_time = 2


def get_data():
    """Fetch all user risk profiles from Redis"""
    keys = r.keys("user_risk:*")
    if not keys:
        return []

    # Batch get all data
    data = []
    for key in keys:
        record = r.hgetall(key)
        record["user_id"] = key.split(":")[1]  # Extract ID from key
        data.append(record)
    return data


# Main UI
st.subheader("Live Feed of User Analysis")

# Fetch Data
raw_data = get_data()

if raw_data:
    df = pd.DataFrame(raw_data)

    # Reorder columns for readability
    if "risky" in df.columns and "reason" in df.columns:
        df = df[["user_id", "risky", "reason"]]

        # Color coding function
        def highlight_risk(val):
            color = "red" if val == "True" else "green"
            return f"background-color: {color}"

        # Display the table
        st.dataframe(
            df.style.applymap(highlight_risk, subset=["risky"]),
            use_container_width=True,
        )

        # Metrics at the top
        col1, col2, col3 = st.columns(3)
        total = len(df)
        risky_count = len(df[df["risky"] == "True"])
        col1.metric("Total Processed", total)
        col2.metric("Threats Detected", risky_count)
        col3.metric("Safety Score", f"{((total - risky_count) / total) * 100:.1f}%")

else:
    st.info("Waiting for data stream...")

# Auto-rerun button for live updates
if st.button("Refresh Data"):
    st.rerun()
