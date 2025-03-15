import pytest
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.models.schema import (
    Base,
    Location,
    TrafficReading,
    WeatherData,
    SpecialEvent,
    TrafficMetrics
)

@pytest.fixture(scope="function")
def test_db():
    """Create a test database."""
    # Use SQLite in-memory database for testing
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    yield session
    
    session.close()
    Base.metadata.drop_all(engine)

@pytest.fixture
def sample_location(test_db):
    """Create a sample location."""
    location = Location(
        name="Test Location",
        latitude=40.7128,
        longitude=-74.0060,
        road_type="highway"
    )
    test_db.add(location)
    test_db.commit()
    return location

def test_location_creation(test_db):
    """Test creating a location."""
    location = Location(
        name="Test Location",
        latitude=40.7128,
        longitude=-74.0060,
        road_type="highway"
    )
    test_db.add(location)
    test_db.commit()
    
    assert location.id is not None
    assert location.name == "Test Location"
    assert location.latitude == 40.7128
    assert location.longitude == -74.0060
    assert location.road_type == "highway"

def test_traffic_reading_creation(test_db, sample_location):
    """Test creating a traffic reading."""
    reading = TrafficReading(
        location_id=sample_location.id,
        vehicle_count=100,
        average_speed=65.5,
        congestion_level=0.3,
        vehicle_types='{"cars": 80, "trucks": 15, "buses": 5}'
    )
    test_db.add(reading)
    test_db.commit()
    
    assert reading.id is not None
    assert reading.location_id == sample_location.id
    assert reading.vehicle_count == 100
    assert reading.average_speed == 65.5
    assert reading.congestion_level == 0.3
    assert reading.vehicle_types == '{"cars": 80, "trucks": 15, "buses": 5}'

def test_weather_data_creation(test_db, sample_location):
    """Test creating weather data."""
    weather = WeatherData(
        location_id=sample_location.id,
        temperature=25.5,
        precipitation=0.5,
        visibility=8.0,
        wind_speed=15.2
    )
    test_db.add(weather)
    test_db.commit()
    
    assert weather.id is not None
    assert weather.location_id == sample_location.id
    assert weather.temperature == 25.5
    assert weather.precipitation == 0.5
    assert weather.visibility == 8.0
    assert weather.wind_speed == 15.2

def test_special_event_creation(test_db):
    """Test creating a special event."""
    event = SpecialEvent(
        name="Test Event",
        start_time=datetime.utcnow(),
        end_time=datetime.utcnow(),
        event_type="concert",
        description="Test description",
        impact_level=0.7,
        affected_locations="[1, 2, 3]"
    )
    test_db.add(event)
    test_db.commit()
    
    assert event.id is not None
    assert event.name == "Test Event"
    assert event.event_type == "concert"
    assert event.impact_level == 0.7
    assert event.affected_locations == "[1, 2, 3]"

def test_traffic_metrics_creation(test_db, sample_location):
    """Test creating traffic metrics."""
    metrics = TrafficMetrics(
        metric_type="peak_hour",
        value=85.5,
        location_id=sample_location.id,
        period_start=datetime.utcnow(),
        period_end=datetime.utcnow()
    )
    test_db.add(metrics)
    test_db.commit()
    
    assert metrics.id is not None
    assert metrics.metric_type == "peak_hour"
    assert metrics.value == 85.5
    assert metrics.location_id == sample_location.id

def test_relationships(test_db, sample_location):
    """Test relationships between models."""
    # Create related records
    reading = TrafficReading(
        location_id=sample_location.id,
        vehicle_count=100,
        average_speed=65.5,
        congestion_level=0.3
    )
    weather = WeatherData(
        location_id=sample_location.id,
        temperature=25.5,
        precipitation=0.5,
        visibility=8.0,
        wind_speed=15.2
    )
    
    test_db.add_all([reading, weather])
    test_db.commit()
    
    # Test relationships
    assert reading in sample_location.traffic_readings
    assert weather in sample_location.weather_data
    assert reading.location == sample_location
    assert weather.location == sample_location 