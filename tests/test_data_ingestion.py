import pytest
import json
from datetime import datetime
from unittest.mock import Mock, patch
from src.data_ingestion.producer import (
    generate_traffic_data,
    generate_weather_data,
    generate_event_data
)

def test_generate_traffic_data():
    """Test traffic data generation."""
    data = generate_traffic_data()
    
    # Check required fields
    assert isinstance(data, dict)
    assert 'location_id' in data
    assert 'timestamp' in data
    assert 'vehicle_count' in data
    assert 'average_speed' in data
    assert 'congestion_level' in data
    assert 'vehicle_types' in data
    
    # Check data types
    assert isinstance(data['location_id'], int)
    assert isinstance(data['vehicle_count'], int)
    assert isinstance(data['average_speed'], float)
    assert isinstance(data['congestion_level'], float)
    assert isinstance(data['vehicle_types'], dict)
    
    # Check value ranges
    assert 1 <= data['location_id'] <= 10
    assert 10 <= data['vehicle_count'] <= 1000
    assert 20 <= data['average_speed'] <= 120
    assert 0 <= data['congestion_level'] <= 1

def test_generate_weather_data():
    """Test weather data generation."""
    data = generate_weather_data()
    
    # Check required fields
    assert isinstance(data, dict)
    assert 'location_id' in data
    assert 'timestamp' in data
    assert 'temperature' in data
    assert 'precipitation' in data
    assert 'visibility' in data
    assert 'wind_speed' in data
    
    # Check data types
    assert isinstance(data['location_id'], int)
    assert isinstance(data['temperature'], float)
    assert isinstance(data['precipitation'], float)
    assert isinstance(data['visibility'], float)
    assert isinstance(data['wind_speed'], float)
    
    # Check value ranges
    assert 1 <= data['location_id'] <= 10
    assert 10 <= data['temperature'] <= 35
    assert 0 <= data['precipitation'] <= 10
    assert 0 <= data['visibility'] <= 10
    assert 0 <= data['wind_speed'] <= 30

@patch('kafka.KafkaProducer')
def test_kafka_producer(mock_producer):
    """Test Kafka producer functionality."""
    # Create mock producer instance
    mock_producer_instance = Mock()
    mock_producer.return_value = mock_producer_instance
    
    # Import producer module (after mocking)
    from src.data_ingestion.producer import produce_data
    
    # Mock the send method
    mock_producer_instance.send = Mock()
    mock_producer_instance.flush = Mock()
    
    # Call produce_data (it will use our mock)
    with pytest.raises(KeyboardInterrupt):
        produce_data()
    
    # Verify that send was called
    assert mock_producer_instance.send.called
    assert mock_producer_instance.flush.called 