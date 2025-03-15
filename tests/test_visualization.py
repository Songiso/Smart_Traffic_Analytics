import pytest
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
from src.visualization.dashboard import (
    load_traffic_data,
    load_weather_data,
    load_metrics
)

@pytest.fixture
def sample_traffic_df():
    """Create sample traffic DataFrame for testing."""
    return pd.DataFrame({
        'location_id': [1, 1, 2, 2],
        'location_name': ['Location A', 'Location A', 'Location B', 'Location B'],
        'timestamp': [
            datetime.now(),
            datetime.now() + timedelta(hours=1),
            datetime.now(),
            datetime.now() + timedelta(hours=1)
        ],
        'vehicle_count': [100, 120, 80, 90],
        'average_speed': [65.5, 60.0, 70.0, 68.0],
        'congestion_level': [0.3, 0.4, 0.2, 0.25]
    })

@pytest.fixture
def sample_weather_df():
    """Create sample weather DataFrame for testing."""
    return pd.DataFrame({
        'location_id': [1, 1, 2, 2],
        'location_name': ['Location A', 'Location A', 'Location B', 'Location B'],
        'timestamp': [
            datetime.now(),
            datetime.now() + timedelta(hours=1),
            datetime.now(),
            datetime.now() + timedelta(hours=1)
        ],
        'temperature': [25.5, 26.0, 24.0, 24.5],
        'precipitation': [0.0, 0.2, 0.5, 0.3],
        'visibility': [10.0, 9.5, 8.0, 8.5],
        'wind_speed': [5.5, 6.0, 7.0, 6.5]
    })

@patch('src.visualization.dashboard.engine')
def test_load_traffic_data(mock_engine, sample_traffic_df):
    """Test loading traffic data."""
    # Mock the database query
    mock_engine.execute.return_value = sample_traffic_df
    
    # Load the data
    result = load_traffic_data()
    
    # Check the results
    assert isinstance(result, pd.DataFrame)
    assert 'location_id' in result.columns
    assert 'timestamp' in result.columns
    assert 'vehicle_count' in result.columns
    assert 'average_speed' in result.columns
    assert 'congestion_level' in result.columns

@patch('src.visualization.dashboard.engine')
def test_load_weather_data(mock_engine, sample_weather_df):
    """Test loading weather data."""
    # Mock the database query
    mock_engine.execute.return_value = sample_weather_df
    
    # Load the data
    result = load_weather_data()
    
    # Check the results
    assert isinstance(result, pd.DataFrame)
    assert 'location_id' in result.columns
    assert 'timestamp' in result.columns
    assert 'temperature' in result.columns
    assert 'precipitation' in result.columns
    assert 'visibility' in result.columns
    assert 'wind_speed' in result.columns

def test_create_traffic_plot(sample_traffic_df):
    """Test creating traffic visualization."""
    # Create plot
    fig = px.line(
        sample_traffic_df,
        x='timestamp',
        y='average_speed',
        color='location_name',
        title='Average Speed Over Time'
    )
    
    # Check plot properties
    assert isinstance(fig, go.Figure)
    assert fig.layout.title.text == 'Average Speed Over Time'
    assert fig.layout.xaxis.title.text == 'timestamp'
    assert fig.layout.yaxis.title.text == 'average_speed'

def test_create_weather_plot(sample_weather_df):
    """Test creating weather visualization."""
    # Create plot
    fig = px.line(
        sample_weather_df,
        x='timestamp',
        y='temperature',
        color='location_name',
        title='Temperature Over Time'
    )
    
    # Check plot properties
    assert isinstance(fig, go.Figure)
    assert fig.layout.title.text == 'Temperature Over Time'
    assert fig.layout.xaxis.title.text == 'timestamp'
    assert fig.layout.yaxis.title.text == 'temperature'

def test_create_correlation_plot(sample_traffic_df, sample_weather_df):
    """Test creating correlation visualization."""
    # Merge data
    merged_df = pd.merge(
        sample_traffic_df,
        sample_weather_df,
        on=['location_id', 'timestamp', 'location_name']
    )
    
    # Create plot
    fig = px.scatter(
        merged_df,
        x='temperature',
        y='average_speed',
        color='precipitation',
        title='Speed vs Temperature'
    )
    
    # Check plot properties
    assert isinstance(fig, go.Figure)
    assert fig.layout.title.text == 'Speed vs Temperature'
    assert fig.layout.xaxis.title.text == 'temperature'
    assert fig.layout.yaxis.title.text == 'average_speed' 