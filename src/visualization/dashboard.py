import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Load environment variables
load_dotenv()

# Database connection
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Create database connection
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

def load_traffic_data():
    """Load traffic data from database."""
    query = """
    SELECT tr.*, l.name as location_name
    FROM traffic_readings tr
    JOIN locations l ON tr.location_id = l.id
    WHERE tr.timestamp >= NOW() - INTERVAL '24 hours'
    """
    return pd.read_sql(query, engine)

def load_weather_data():
    """Load weather data from database."""
    query = """
    SELECT wd.*, l.name as location_name
    FROM weather_data wd
    JOIN locations l ON wd.location_id = l.id
    WHERE wd.timestamp >= NOW() - INTERVAL '24 hours'
    """
    return pd.read_sql(query, engine)

def load_metrics():
    """Load aggregated metrics from database."""
    query = """
    SELECT *
    FROM traffic_metrics
    WHERE timestamp >= NOW() - INTERVAL '24 hours'
    """
    return pd.read_sql(query, engine)

def main():
    st.set_page_config(page_title="Traffic Analytics Dashboard", layout="wide")
    st.title("Smart City Traffic Analytics Dashboard")

    # Sidebar filters
    st.sidebar.header("Filters")
    time_range = st.sidebar.selectbox(
        "Time Range",
        ["Last 24 Hours", "Last 7 Days", "Last 30 Days"]
    )

    # Load data
    traffic_df = load_traffic_data()
    weather_df = load_weather_data()
    metrics_df = load_metrics()

    # Main metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(
            "Average Traffic Speed",
            f"{traffic_df['average_speed'].mean():.1f} km/h"
        )
    with col2:
        st.metric(
            "Average Congestion Level",
            f"{traffic_df['congestion_level'].mean():.2f}"
        )
    with col3:
        st.metric(
            "Total Vehicle Count",
            f"{traffic_df['vehicle_count'].sum():,}"
        )
    with col4:
        st.metric(
            "Active Locations",
            f"{traffic_df['location_id'].nunique()}"
        )

    # Traffic patterns
    st.header("Traffic Patterns")
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.line(
            traffic_df,
            x='timestamp',
            y='average_speed',
            color='location_name',
            title='Average Speed Over Time'
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = px.line(
            traffic_df,
            x='timestamp',
            y='congestion_level',
            color='location_name',
            title='Congestion Level Over Time'
        )
        st.plotly_chart(fig, use_container_width=True)

    # Weather impact
    st.header("Weather Impact")
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.scatter(
            pd.merge(traffic_df, weather_df, on=['location_id', 'timestamp']),
            x='temperature',
            y='average_speed',
            color='precipitation',
            title='Speed vs Temperature (color: precipitation)'
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = px.scatter(
            pd.merge(traffic_df, weather_df, on=['location_id', 'timestamp']),
            x='precipitation',
            y='congestion_level',
            color='visibility',
            title='Congestion vs Precipitation (color: visibility)'
        )
        st.plotly_chart(fig, use_container_width=True)

    # Location analysis
    st.header("Location Analysis")
    location_stats = traffic_df.groupby('location_name').agg({
        'average_speed': 'mean',
        'congestion_level': 'mean',
        'vehicle_count': 'sum'
    }).reset_index()
    
    fig = go.Figure(data=[
        go.Bar(name='Average Speed', x=location_stats['location_name'], y=location_stats['average_speed']),
        go.Bar(name='Congestion Level', x=location_stats['location_name'], y=location_stats['congestion_level'])
    ])
    fig.update_layout(title='Location Performance Metrics')
    st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    main() 