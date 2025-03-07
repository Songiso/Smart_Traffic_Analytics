from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_TOPIC_TRAFFIC_DATA = os.getenv('KAFKA_TOPIC_TRAFFIC_DATA')
KAFKA_TOPIC_WEATHER_DATA = os.getenv('KAFKA_TOPIC_WEATHER_DATA')
KAFKA_TOPIC_EVENTS_DATA = os.getenv('KAFKA_TOPIC_EVENTS_DATA')

# Define schemas
traffic_schema = StructType([
    StructField("location_id", IntegerType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("vehicle_count", IntegerType(), True),
    StructField("average_speed", DoubleType(), True),
    StructField("congestion_level", DoubleType(), True),
    StructField("vehicle_types", StringType(), True)
])

weather_schema = StructType([
    StructField("location_id", IntegerType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("precipitation", DoubleType(), True),
    StructField("visibility", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True)
])

def create_spark_session():
    """Create and configure Spark session."""
    return (SparkSession.builder
            .appName("TrafficAnalytics")
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .config("spark.sql.streaming.schemaInference", "true")
            .getOrCreate())

def process_traffic_data(spark):
    """Process traffic data stream."""
    # Read from Kafka
    traffic_df = (spark.readStream
                 .format("kafka")
                 .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                 .option("subscribe", KAFKA_TOPIC_TRAFFIC_DATA)
                 .load()
                 .select(from_json(col("value").cast("string"), traffic_schema).alias("data"))
                 .select("data.*"))

    # Calculate metrics
    traffic_metrics = (traffic_df
                      .withWatermark("timestamp", "1 minute")
                      .groupBy(window("timestamp", "5 minutes"), "location_id")
                      .agg(
                          avg("average_speed").alias("avg_speed"),
                          avg("congestion_level").alias("avg_congestion"),
                          count("*").alias("reading_count")
                      ))

    # Write to console (for development)
    query = traffic_metrics.writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    return query

def process_weather_data(spark):
    """Process weather data stream."""
    # Read from Kafka
    weather_df = (spark.readStream
                 .format("kafka")
                 .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                 .option("subscribe", KAFKA_TOPIC_WEATHER_DATA)
                 .load()
                 .select(from_json(col("value").cast("string"), weather_schema).alias("data"))
                 .select("data.*"))

    # Calculate metrics
    weather_metrics = (weather_df
                      .withWatermark("timestamp", "1 minute")
                      .groupBy(window("timestamp", "5 minutes"), "location_id")
                      .agg(
                          avg("temperature").alias("avg_temperature"),
                          avg("precipitation").alias("avg_precipitation"),
                          avg("visibility").alias("avg_visibility")
                      ))

    # Write to console (for development)
    query = weather_metrics.writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    return query

def main():
    """Main function to run the Spark processing job."""
    spark = create_spark_session()
    
    try:
        # Start processing streams
        traffic_query = process_traffic_data(spark)
        weather_query = process_weather_data(spark)
        
        # Wait for termination
        spark.streams.awaitAnyTermination()
        
    except Exception as e:
        print(f"Error in Spark processing: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 