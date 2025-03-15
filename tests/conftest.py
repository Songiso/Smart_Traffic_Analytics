import pytest
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.models.schema import Base

# Load environment variables for testing
load_dotenv()

@pytest.fixture(scope="session")
def test_db_url():
    """Create a test database URL."""
    return "sqlite:///:memory:"

@pytest.fixture(scope="session")
def test_engine(test_db_url):
    """Create a test database engine."""
    engine = create_engine(test_db_url)
    Base.metadata.create_all(engine)
    yield engine
    Base.metadata.drop_all(engine)

@pytest.fixture(scope="function")
def test_session(test_engine):
    """Create a new database session for a test."""
    Session = sessionmaker(bind=test_engine)
    session = Session()
    yield session
    session.rollback()
    session.close()

@pytest.fixture(scope="session")
def test_kafka_config():
    """Create test Kafka configuration."""
    return {
        'bootstrap_servers': 'localhost:9092',
        'topic_traffic_data': 'test_traffic_data',
        'topic_weather_data': 'test_weather_data',
        'topic_events_data': 'test_events_data'
    }

@pytest.fixture(scope="session")
def test_spark_config():
    """Create test Spark configuration."""
    return {
        'master': 'local[2]',
        'app_name': 'TestTrafficAnalytics',
        'shuffle_partitions': '1',
        'default_parallelism': '1'
    } 