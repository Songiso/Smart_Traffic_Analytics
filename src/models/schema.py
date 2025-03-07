from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()

class Location(Base):
    __tablename__ = 'locations'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    road_type = Column(String(50))
    
    traffic_readings = relationship("TrafficReading", back_populates="location")
    weather_data = relationship("WeatherData", back_populates="location")

class TrafficReading(Base):
    __tablename__ = 'traffic_readings'
    
    id = Column(Integer, primary_key=True)
    location_id = Column(Integer, ForeignKey('locations.id'))
    timestamp = Column(DateTime, default=datetime.utcnow)
    vehicle_count = Column(Integer)
    average_speed = Column(Float)
    congestion_level = Column(Float)  # 0-1 scale
    vehicle_types = Column(String(200))  # JSON string of vehicle type counts
    
    location = relationship("Location", back_populates="traffic_readings")

class WeatherData(Base):
    __tablename__ = 'weather_data'
    
    id = Column(Integer, primary_key=True)
    location_id = Column(Integer, ForeignKey('locations.id'))
    timestamp = Column(DateTime, default=datetime.utcnow)
    temperature = Column(Float)
    precipitation = Column(Float)
    visibility = Column(Float)
    wind_speed = Column(Float)
    
    location = relationship("Location", back_populates="weather_data")

class SpecialEvent(Base):
    __tablename__ = 'special_events'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False)
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime, nullable=False)
    event_type = Column(String(50))
    description = Column(String(500))
    impact_level = Column(Float)  # 0-1 scale
    affected_locations = Column(String(200))  # JSON string of affected location IDs

class TrafficMetrics(Base):
    __tablename__ = 'traffic_metrics'
    
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    metric_type = Column(String(50))  # e.g., 'peak_hour', 'daily_average'
    value = Column(Float)
    location_id = Column(Integer, ForeignKey('locations.id'))
    period_start = Column(DateTime)
    period_end = Column(DateTime) 