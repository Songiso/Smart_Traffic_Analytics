import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_TOPIC_TRAFFIC_DATA = os.getenv('KAFKA_TOPIC_TRAFFIC_DATA')
KAFKA_TOPIC_WEATHER_DATA = os.getenv('KAFKA_TOPIC_WEATHER_DATA')
KAFKA_TOPIC_EVENTS_DATA = os.getenv('KAFKA_TOPIC_EVENTS_DATA')

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def generate_traffic_data():
    """Generate simulated traffic data."""
    return {
        'location_id': random.randint(1, 10),
        'timestamp': datetime.utcnow().isoformat(),
        'vehicle_count': random.randint(10, 1000),
        'average_speed': random.uniform(20, 120),
        'congestion_level': random.uniform(0, 1),
        'vehicle_types': {
            'cars': random.randint(5, 800),
            'trucks': random.randint(1, 100),
            'buses': random.randint(1, 50)
        }
    }

def generate_weather_data():
    """Generate simulated weather data."""
    return {
        'location_id': random.randint(1, 10),
        'timestamp': datetime.utcnow().isoformat(),
        'temperature': random.uniform(10, 35),
        'precipitation': random.uniform(0, 10),
        'visibility': random.uniform(0, 10),
        'wind_speed': random.uniform(0, 30)
    }

def generate_event_data():
    """Generate simulated special event data."""
    event_types = ['sports', 'concert', 'festival', 'protest', 'construction']
    return {
        'name': f"Event_{random.randint(1, 1000)}",
        'start_time': datetime.utcnow().isoformat(),
        'end_time': (datetime.utcnow() + timedelta(hours=random.randint(1, 24))).isoformat(),
        'event_type': random.choice(event_types),
        'description': f"Description for event {random.randint(1, 1000)}",
        'impact_level': random.uniform(0, 1),
        'affected_locations': [random.randint(1, 10) for _ in range(random.randint(1, 5))]
    }

def produce_data():
    """Produce data to Kafka topics."""
    try:
        while True:
            # Produce traffic data
            traffic_data = generate_traffic_data()
            producer.send(KAFKA_TOPIC_TRAFFIC_DATA, traffic_data)
            
            # Produce weather data
            weather_data = generate_weather_data()
            producer.send(KAFKA_TOPIC_WEATHER_DATA, weather_data)
            
            # Produce event data (less frequently)
            if random.random() < 0.1:  # 10% chance to generate event data
                event_data = generate_event_data()
                producer.send(KAFKA_TOPIC_EVENTS_DATA, event_data)
            
            # Flush to ensure messages are sent
            producer.flush()
            
            # Wait before next iteration
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("Stopping data production...")
    finally:
        producer.close()

if __name__ == "__main__":
    print("Starting data production...")
    produce_data() 