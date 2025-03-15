import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from datetime import datetime
from src.processing.spark_job import (
    create_spark_session,
    process_traffic_data,
    process_weather_data
)

@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return (SparkSession.builder
            .appName("TestTrafficAnalytics")
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.default.parallelism", "1")
            .getOrCreate())

@pytest.fixture(scope="function")
def sample_traffic_data(spark):
    """Create sample traffic data for testing."""
    schema = StructType([
        StructField("location_id", IntegerType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("vehicle_count", IntegerType(), True),
        StructField("average_speed", DoubleType(), True),
        StructField("congestion_level", DoubleType(), True),
        StructField("vehicle_types", StringType(), True)
    ])
    
    data = [
        (1, datetime.now(), 100, 65.5, 0.3, '{"cars": 80, "trucks": 15, "buses": 5}'),
        (1, datetime.now(), 120, 60.0, 0.4, '{"cars": 90, "trucks": 20, "buses": 10}'),
        (2, datetime.now(), 80, 70.0, 0.2, '{"cars": 70, "trucks": 8, "buses": 2}')
    ]
    
    return spark.createDataFrame(data, schema)

@pytest.fixture(scope="function")
def sample_weather_data(spark):
    """Create sample weather data for testing."""
    schema = StructType([
        StructField("location_id", IntegerType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("visibility", DoubleType(), True),
        StructField("wind_speed", DoubleType(), True)
    ])
    
    data = [
        (1, datetime.now(), 25.5, 0.0, 10.0, 5.5),
        (1, datetime.now(), 26.0, 0.2, 9.5, 6.0),
        (2, datetime.now(), 24.0, 0.5, 8.0, 7.0)
    ]
    
    return spark.createDataFrame(data, schema)

def test_create_spark_session():
    """Test Spark session creation."""
    spark = create_spark_session()
    assert spark is not None
    assert spark.version is not None
    spark.stop()

def test_process_traffic_data(spark, sample_traffic_data):
    """Test traffic data processing."""
    # Process the sample data
    result = sample_traffic_data.groupBy("location_id").agg({
        "average_speed": "avg",
        "congestion_level": "avg",
        "vehicle_count": "sum"
    })
    
    # Check the results
    row = result.filter(result.location_id == 1).collect()[0]
    assert row["avg(average_speed)"] == pytest.approx(62.75, 0.1)
    assert row["avg(congestion_level)"] == pytest.approx(0.35, 0.1)
    assert row["sum(vehicle_count)"] == 220

def test_process_weather_data(spark, sample_weather_data):
    """Test weather data processing."""
    # Process the sample data
    result = sample_weather_data.groupBy("location_id").agg({
        "temperature": "avg",
        "precipitation": "avg",
        "visibility": "avg"
    })
    
    # Check the results
    row = result.filter(result.location_id == 1).collect()[0]
    assert row["avg(temperature)"] == pytest.approx(25.75, 0.1)
    assert row["avg(precipitation)"] == pytest.approx(0.1, 0.1)
    assert row["avg(visibility)"] == pytest.approx(9.75, 0.1)

def test_join_traffic_weather_data(spark, sample_traffic_data, sample_weather_data):
    """Test joining traffic and weather data."""
    # Join the datasets
    joined = sample_traffic_data.join(
        sample_weather_data,
        ["location_id", "timestamp"],
        "inner"
    )
    
    # Check the joined data
    assert joined.count() > 0
    assert all(col in joined.columns for col in [
        "location_id", "timestamp", "vehicle_count", "average_speed",
        "temperature", "precipitation", "visibility"
    ]) 